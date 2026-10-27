### Added
- Add an opt-in loopback committed state feed with bounded replay, fixed dependency subscriptions, complete snapshots and reorg/reset epochs. Reuse the Geth state-commit hook without re-executing blocks; pool/quote logic remains in consumers.
