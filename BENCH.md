# Benchmark log

Scorer: ~20M events, linux/amd64, 8 vCPU, 24 GB. Local Mac is ARM — its
numbers rank builds only. Authoritative: bench.yml x86 A/B + track score.

## Scores
| commit | score | notes |
|---|---|---|
| 62b394e | 16.74s | first submission |
| 1f6d0b4 | 12.47s | perf pass: mimalloc, lock-free interner, flat scatter |

## Experiments
| change | bench | kept? |
|---|---|---|
| v2 rewrite: u64 packed IDs, memchr3 tokenizer, open-addr dedup, dense records, shard-sort | local ~12s @20M gen20m | kept (d5e4537) |
| U64Map flat maps for canon/prod/camp | local — | kept (d5e4537) |
| synth.rs + bench.yml CI A/B | infra | kept (e6ba32f) |
| Bloom prefilter before canon lookups | local gate pass; CI pending | kept (23623cf) |
| **BUG**: fx64 used low product bits → hash clustering on packed keys; ev_filter 19.6s→52.7ms; synth9-20M 692s→17.9s local | local | kept (c8083bd) |
| Dense aggregation in sessionize (per-person day cells + camp×day array, no hashmaps) | synth9-20M: sessionize 10.2s→0.76s, total ~17.9s→~5.3s real | kept (617d086) |
| x86 CI A/B run 44891525945: c8083bd vs 1f6d0b4 @20M synth9 NCPU=4 | base 28.9-31.1s → head 15.5-16.8s (~1.8x); spot-check script bug only | head faster |
| uid StringArray direct offsets+values (no per-row String) | parquet byte-identical; output_rows ~same (sort dominates) | kept (this commit) |
| Group-sort ud output: sort ~2M person-groups + gather, not ~10M rows | synth9-20M output_rows 2.03s→1.14s | kept (7e24f7c) |
| Parallel part-N.parquet writes (NCPU slices, rayon, both tables overlapped) | write_user_parquet split: cols 303ms / writer 764ms; synth9-20M output_rows 1.14s→0.575s; content verified identical | kept (this commit) |
