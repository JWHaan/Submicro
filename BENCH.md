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
| Parallel part-N.parquet writes (NCPU slices, rayon, both tables overlapped) | write_user_parquet split: cols 303ms / writer 764ms; synth9-20M output_rows 1.14s→0.575s; content verified identical | kept (d5e72ed) |
| x86 CI A/B run 44892617851: 7e24f7c vs c8083bd @20M synth9 NCPU=4 | base 17.9-19.4s → head 5.10-5.36s (~3.6x); CSV + parquet identical | dense-agg+group-sort confirmed on x86 |
| Overlap clicks pipeline with events (scoped thread, clicks inline) | synth9-20M local: 5.6-6.9s vs ~5.3s sequential; cl_parse 256ms→2-3.2s under contention — both pipelines saturate all cores, nothing to hide in | REVERTED |
| madvise(MADV_WILLNEED) on input mmaps + thread-local phase timing | local warm-cache 3.7-5.6s (unreliable); x86 CI decides | committed 77a22eb pending CI |
| Fuse shard histogram into ev_parse (eliminates separate 640MB counting pass) | synth9-20M local: ev_scatter ~0.68s vs ~0.56-0.71s baseline (noisy); gates all green | committed this change pending CI |
| x86 CI A/B run 44893442562: d5e72ed vs 7e24f7c @20M synth9 NCPU=4 | base 4350-4560ms → head 3722-3894ms (~15%); CSV identical; part-0 parquet bytes differ by design (8-part split vs single file) | parallel writers confirmed on x86 |
| x86 CI A/B run 44893639460: 77a22eb (madvise) vs d5e72ed @20M synth9 NCPU=4 | base ~4350ms → head ~4415ms median (~1% slower, wash); CSV + all parquet parts identical | madvise a wash on x86; kept (hint can't hurt, may help cold cache on scorer) |
| Fused ev filter+count+scatter+sort (per-task person-shard histograms, parallel disjoint-region scatter, per-shard sort) | synth9-20M local: ev_filter+ev_sort ~243ms combined; gates all green | committed pending CI |
| x86 CI A/B run 44894222752: 10779e2 (fused scatter) vs f97fb1f @20M synth9 NCPU=4 | base 3716-3873ms → head 3676-3843ms, median ~3789→3755 (~1%); CSV + all parquet parts identical | fused scatter confirmed on x86 |
| x86 CI A/B run 44894389059: 733f5e5 (with_min_len) vs 10779e2 @20M synth9 NCPU=4 | base 3021-3232ms → head 2944-3182ms, median ~3093→3016 (~2.5%); CSV + all parquet parts identical | with_min_len confirmed on x86 |
| Compact dedup: 24B DRec{eid,ingest,gidx} arena replaces 56B RawEv scatter; dedup marks AtomicU8 flags, filter rescans task_rows | local min: ev_scatter ~36ms, ev_dedup ~146ms, ev_filter ~30ms (noisy box); 5/5 diff + dev + adversarial pass; ~1.4GB less peak @60M | committed pending CI |
| Sessionize: per-shard merge join (ev_runs×cl_runs share fx64(person)&255 shards) replaces 2M binary searches; cl_runs global sort dropped | local sessionize ~405-530ms (noisy box); 36/36 fixtures + dev + 5/5 diff + adversarial all identical | committed 7d94d6f |
| x86 CI A/B run 44895155486: 7d94d6f (merge-join) vs b53b5a4 @20M synth9 NCPU=4 | base ~4368ms → head ~4030ms (~7.7%); CSV + all parquet parts identical | merge-join confirmed on x86 |
| Split RawEv(56B) into FEv(32B: uid/prod/ts/price/etype/pflag/stale) + DIn(16B: eid/ingest); scatter reads DIn, freshness folded to flag at parse | local: ev_scatter 157→25ms, ev_filter ~23ms; 36/36 fixtures + dev + 5/5 diff + adversarial all identical; ~640MB less peak @60M | committed 4202d95 |
| x86 CI A/B run 44895547503: 4202d95 (FEv/DIn split) vs 7d94d6f @20M synth9 NCPU=4 | base 3724-3896ms (med ~3869) → head 3701-4066ms (med ~3765, ~2.7%); CSV + all parquet parts identical | FEv/DIn split confirmed on x86 |
| parse_ts: SWAR digit check + unrolled field extraction on ISO path (3 unaligned u64 checks + straight-line assembly; identical semantics/T_INVALID set) | local A/B vs 4202d95: ev_parse min 345ms vs 337ms — wash on noisy box; 36/36 fixtures + dev + 5/5 diff + adversarial all identical | committed 6e85aff, CI run 44895982268 pending |
| Fused user-parquet writer: part writers pull row ranges straight from ud_out via ud_groups prefix sums; ud_rows gather (~480MB sequential pass) eliminated; 2M chunk boundary preserved | local synth9-20M output_rows 575→297ms; dev + synth9 parquet byte-identical vs parent (all 8 parts + campaign_daily); 36/36 fixtures + 5/5 diff + adversarial pass | committed this change, CI pending |
| x86 CI A/B run 44895982268: 6e85aff (SWAR parse_ts) vs 4202d95 @20M synth9 NCPU=4 | base med ~3718ms → head med ~3684ms (~1% lean head, near-noise); CSV + all parquet parts identical | parse_ts kept (fewer instructions, no regression) |
