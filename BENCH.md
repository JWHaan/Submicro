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
| Dense aggregation in sessionize (per-person day cells + camp×day array, no hashmaps) | synth9-20M: sessionize 10.2s→0.76s, total ~17.9s→~5.3s real | kept (this commit) |
