### Changed
- Avoid scanning the complete state-feed subscription to order sparse account changes; retain the ordered scan for dense updates and preserve existing final-state, zero-value, deletion and canonical-publication semantics. Add isolated capture/encoding/history-HTTP benchmarks and ordering correctness tests; measured savings are microseconds, not an end-to-end pool update SLA.
