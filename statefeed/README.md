# Committed state feed (fixed-subscription preview)

This opt-in module exports final state changes to a local consumer such as
flashloan. It uses Geth's existing `OnStateUpdate` commit hook, without modifying
the go-ethereum submodule. It does not decode pools, quote swaps, or send trades.

## Integration and correctness

`cmd/nitro` installs a hook containing only `OnStateUpdate`. `appendBlock` begins
capture only for a real block append and keeps this hook on the normal
`WriteBlockAndSetHeadWithTime` path. The generic live-tracer `InsertChain` path
would re-execute the block and is deliberately not used for this commit-only hook.
Other live tracers retain their original behavior; simultaneous VM tracing and
state feed are rejected at configuration time.

Geth supplies the finalized write set after internal-call/transaction rollbacks.
The module filters it by a precomputed address/slot lookup, including account
balance/nonce, code, storage writes and deletion. Pre-Cancun hashed slot keys and
post-Cancun raw keys are handled separately. Publication happens only after the
canonical append succeeds. Missing/mismatched capture invalidates the epoch and
forces bootstrap rather than publishing a falsely complete delta.

This removes RPC polling of already subscribed state. Geth still executes the
chain and materializes its tracing update (including unselected changes/trie
metadata) before filtering. This overhead must be measured before production use.

## Enable

Default is disabled. Export a subscription from flashloan, then add these options
to the node's existing configuration:

```
--execution.state-feed.enable
--execution.state-feed.subscription=/path/to/subscription.json
--execution.state-feed.addr=127.0.0.1
--execution.state-feed.port=9647
```

The listener is loopback-only. The file format is:

```json
{"schema":1,"accounts":[{"address":"0x0000000000000000000000000000000000001234","slots":["0x0000000000000000000000000000000000000000000000000000000000000001"]}]}
```

Addresses/slots are deduplicated and sorted. The version is SHA-256 of compact
normalized JSON. Limits: 4,096 accounts, 65,536 distinct slots, 256 retained
updates and 64 MiB retained encoded updates by default. History bounds are
configurable. Subscription changes require a node restart in this preview.

## Protocol v1

- `GET /v1/snapshot`: a complete fixed-block snapshot of every subscribed field.
  Includes proven absent accounts/zero slots explicitly. Reads a separate StateDB
  at the captured root; checks epoch and retained continuity before returning.
- `GET /v1/updates?epoch=...&subscription_version=...&after=N`: up to 64 complete
  block deltas after sequence N, with a 10-second long poll when caught up.
- `GET /v1/health`: epoch/version/cursor and bounded-history counters.

Every envelope carries schema, chain ID, random process/reset epoch, subscription
version, sequence, kind, `complete`, block hash/parent hash/state root, L2 and L1
numbers, timestamp, gas limit, coinbase, difficulty/base fee/mix digest. The raw
header mix digest is not itself Arbitrum's EVM PREVRANDAO; consumers must preserve
their chain-specific execution-environment rules.

A snapshot includes code, native account balance, nonce, existence and all
subscribed slots. An omitted field in a delta means unchanged only within a
continuous complete stream. Zero writes are explicit. Account deletion explicitly
clears every subscribed slot plus code/balance/nonce. ERC20 `balanceOf` values are
contract-computed values, not the native account balance field.

Empty blocks advance sequence and environment. On reorg (even without replacement
blocks), restart, capture failure or oversized update, the epoch changes. Requests
with a wrong/expired cursor get HTTP 409 and must bootstrap again. History is
bounded, without silent gaps or an unbounded slow-client queue. Initial snapshot
reads can race a reorg/history expiry; those return 409 too.

## Scope and tests

This is a shadow-validation preview, not a production activation. It has fixed
subscriptions, in-memory history and full resnapshot recovery. It does not yet
support live subscription expansion, persistent replay, or a full-chain
performance/capacity claim. A missing new dependency remains an error; receiving
writes does not discover every future read path.

`go test -race ./statefeed` exercises real Geth commits, reverted internal writes,
code/balance/nonce changes, filtered slots, explicit zero, empty blocks, both key
encodings, bounded replay and reorg resets. Set `STATEFEED_FIXTURE_OUT` during the
explicit-zero test to regenerate the Go JSON fixture used by flashloan's Rust
receiver tests. The full Nitro command must also compile with generated contract
bindings and the normal native dependencies.

## Performance

See [PERFORMANCE.md](PERFORMANCE.md) for the measured export costs, bounded
ordering optimization, reproducible microbenchmarks and remaining pipeline work.
As of the 2026-10-28 audit, the sparse ordering change has passed package tests but
has not been deployed to the running node; the documented live HTTP samples and
local before/after microbenchmarks have separate measurement boundaries.
