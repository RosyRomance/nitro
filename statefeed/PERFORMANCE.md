# State-feed performance audit

These measurements isolate Nitro's committed-state export from flashloan's
state application, dependency discovery, local quotation and graph publication.
They do not establish a 50 ms end-to-end update SLA for 1,121 pools.

Status on 2026-10-28: the sparse capture change is implemented and package-tested
in this checkout, but has **not been deployed to the running Nitro node**. The
live HTTP measurements below were collected from the existing deployment, not
from a before/after deployment experiment. Baseline microbenchmarks used revision
`d165f4f8c`; optimized microbenchmarks used this change with the same fixture.

## Deployment and measurements

The measured node has 16 logical CPUs / 8 physical cores (Xeon Platinum 8375C)
and approximately 124 GiB RAM. Its Docker CPU quota, CPU set and memory limit are
unset. GOMAXPROCS is not explicitly overridden; the previous node startup log
recorded 16. Raising it cannot turn serial JSON-RPC batch execution into parallel
calls or accelerate CPU work running in flashloan.

The early fixed subscription sampled here contained 1,075 accounts and 36,522
slots. Later consumer testing extended the subscription to 40,919 slots; the
earlier payload sizes are not a guarantee for that later configuration. A
read-only sample of 63 consecutive retained updates had these sizes:

| Per-block measurement | Median | P95 | Maximum |
| --- | ---: | ---: | ---: |
| Encoded envelope bytes | 1,913 | 3,471 | 4,076 |
| Changed accounts | 4 | 8 | 12 |
| Changed subscribed slots | 5 | 13 | 15 |

A full bootstrap snapshot was 26,842,633 bytes and took approximately 210 ms,
including process startup and transfer. This cold-start measurement must not be
compared with incremental per-block costs. It includes contract bytecode and all
subscribed values.

A separate HTTP keep-alive measurement ran inside the container's network
namespace, excluding shell, nsenter and curl startup from each request:

| Request | Samples | Payload | Median | P95 |
| --- | ---: | ---: | ---: | ---: |
| Health | 100 | 214 B | 0.095 ms | 0.123 ms |
| One already-retained delta | 100 | 1,002 B | 0.106 ms | 0.127 ms |
| 63 already-retained deltas | 30 | 131,728 B | 0.795 ms | 1.398 ms |

These measure already-available data. A long poll waiting for the next block
includes block arrival time and is not a measure of feed processing latency.
Neither measurement establishes the delay from canonical commit to consumer
receipt. No node restart, deployment change or transaction submission was needed.

## Capture optimization and repeatable microbenchmarks

`capture` previously walked the entire sorted subscription after filtering each
block, merely to order the touched accounts. It now preallocates the exact output
size and sorts only the touched entries when fewer than one sixteenth of
subscribed accounts changed. Denser updates retain the existing ordered scan:
benchmarking unconditional sorting showed a substantial dense-block regression.
Filtering, zero writes, code-only changes and complete account deletion semantics
are unchanged. Empty accounts remain an empty JSON array, not null.

The synthetic benchmark uses 1,075 accounts, 34 subscribed slots per account and
two changed slots per touched account. It isolates feed work from execution and
storage access. `BenchmarkCapture` times `Begin` (including header copy and
locking) plus `capture`; initial subscription lookup construction is excluded.
It does not time whole-chain `ToTracingUpdate`, canonical commit or publication.
The table shows medians of three runs, one Go execution thread, 100 ms minimum
benchmark duration per case:

| Changed accounts | Capture before | Capture after |
| --- | ---: | ---: |
| 0 | 5.995 us | 2.770 us |
| 1 | 19.836 us | 3.485 us |
| 4 | 22.075 us | 5.324 us |
| 12 | 32.891 us | 11.663 us |
| 128 | 106.805 us | 109.835 us |
| 1,075 | 940.785 us | 932.744 us |

The four-account case improved 4.15 times but saves only about 17 microseconds.
The roughly 3% difference in the 128-account case is not evidence of a material
end-to-end regression or improvement from this small sample.

Other unchanged stages in the baseline fixture:

- Encoding a four-account delta: 8.298 us; 12 accounts: 20.583 us.
- Encoding 1,075 changed accounts / 2,150 slots: 1.625 ms.
- Reading the latest retained delta through the HTTP handler with 256 history
  entries, including response encoding and recorder allocation: 16.223 us.

The handler benchmark uses immutable repeated payloads solely to time the
256-entry scan and response handling; it is not a consecutive-block correctness
replay. It overlaps response serialization and the live HTTP metric, so these
timings must not be added together as independent end-to-end stages.

`go test -race ./statefeed` passed after implementation. Coverage includes sparse
and dense deterministic ordering, non-subscribed changes, code-only changes,
explicit zero storage, and complete deleted-account fields. The implementation
only changes output allocation and ordering after existing filtering; it does
not change root/parent checks, subscription identity, final-value extraction, or
commit-before-publication semantics. No full Nitro rebuild, deployment speedup,
or end-to-end 50 ms result is claimed by this package-level validation.

Run with the repository's Go toolchain:

```
GOMAXPROCS=1 go test ./statefeed -run '^$' \
  -bench 'Benchmark(Capture|EncodeDelta|ReadLatestDelta)$' \
  -benchmem -benchtime=100ms -count=3
go test -race ./statefeed
```

## Pipeline review and remaining opportunities

| Stage | Current implementation | Opportunity and qualification |
| --- | --- | --- |
| Sequencer input and local execution | Existing Nitro execution engine | Measure canonical-commit arrival separately from consumer update work. No evidence here justifies additional DB-owning Nitro processes. |
| Commit-hook integration | `execution/gethexec/executionengine.go` begins capture before the real append and publishes only after successful canonical append | The commit-only hook already avoids the generic tracer path that re-executes the entire block. Preserve this distinction. |
| Geth update materialization | `go-ethereum/core/state/stateupdate.go:ToTracingUpdate` constructs all account, storage, code and trie-node changes before filtering | A dedicated filtered commit callback could omit unneeded changes and trie metadata. This is unmeasured upstream/submodule work, not included in the capture benchmark; preserve final-state and rollback semantics. |
| Subscription lookup | Raw/hashed slot lookup maps are cached across blocks | Already amortized. Live subscription replacement could remove future restart/bootstrap costs, but requires versioned cutover and exact-block dependency alignment. |
| Changed-account ordering | Sparse touched sort; dense subscription scan | Implemented and measured above. |
| Delta encoding and publication | JSON is encoded while holding the feed mutex, then retained in bounded history | Usually tiny in the sampled workload. Encoding outside the mutex or using a bounded ordered encoder queue needs epoch/sequence validation and cannot publish before canonical commit. Benchmark dense bursts before adding this complexity. |
| History read | At most the configured history is scanned; JSON RawMessage envelopes are encoded into the response | A direct sequence offset and writing existing validated raw JSON could reduce scan/re-encoding costs. Current 256-entry read cost is tens of microseconds; not a primary target. |
| Initial snapshot | Reads each subscribed account/slot at one captured state root; includes code | Content-addressed code caching and paged snapshots could reduce bootstrap bandwidth. A snapshot cursor must remain within the epoch/history window; omitted data must never become zero. |
| RPC backfill | `go-ethereum/rpc/handler.go:handleBatch` processes calls serially in one batch goroutine | Smaller batches over bounded concurrent HTTP requests reduce head-of-line blocking, while overly small batches increase transport overhead. Prefer eliminating warm-state backfills in the consumer and measure the remaining real requests. |
| Consumer state update and quotation | Outside Nitro | Apply a complete block delta once into immutable shared state, deduplicate shared dependencies, schedule bounded CPU work separately from asynchronous I/O, and publish independently timed groups with explicit block identity. These larger consumer costs require their own end-to-end validation. |

A plain higher GOMAXPROCS setting does not address serial work, a barrier waiting
for slow pools, redundant local Hook execution, or millions of individual
consumer cache lookups. Current evidence places the sampled incremental Nitro
HTTP export well below 1 ms; it does not quantify full-chain commit conversion
or guarantee performance during larger future state changes. The 50 ms target
must include the exact chosen start/end boundaries and the slowest required
pool, not only the first independently published group.
