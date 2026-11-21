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
| x86 CI A/B run 44896494150: fb2f810 (fused user-parquet writer) vs 3dd79da @20M synth9 NCPU=4 | base 3269-3400ms (med ~3330) → head 3134-3349ms (med ~3187, ~4.3%); CSV + all parquet parts identical | fused writer confirmed on x86 |
| packable charset lookup via const-built 256B PACK_TAB (match -> L1 load) + #[inline(always)] | local ev_parse min ~268ms (noisy); 36/36 fixtures + dev parity + 5/5 diff + adversarial all identical | committed 6db309e, CI run 44896828545 pending |
## parallel teardown v1 — FAILED on x86 (segfault), reverted design
- e3b1f71 scanned /proc/self/maps and MADV_DONTNEED'd every VMA ≥64MiB across threads before exit(0). Included the glibc heap arena + thread stacks → head zeroed its own allocator metadata → SIGSEGV.
- x86 CI run 44897173563: base run1 3541ms then head crashed, exit 139. Run 44897501276 (5b37ae0 inheriting it): also exit 139. Both runs invalid.
- Local gate was inert: #[cfg(linux)] never compiled the path on the ARM Mac — lesson: Linux-only perf code must be exercised in CI or cfg'd so local tests actually run it.

## parallel teardown v2 — buffer-scoped, cross-platform
- Change: register only interior pages of known app-owned buffers ≥1MiB (evs, clicks, ud_out, cd, ud_rows, cd_rows, ud_groups); round to page-aligned interior so malloc headers/footers and neighbor allocs are never touched; par_iter madvise; NO_TEARDOWN env opt-out; #[cfg(unix)] so macOS gate exercises it for real.
- Gate: fixtures 36/36, dev parity OK, diff 5/5, adversarial OK; clean exit rc=0 on every local run (madvise path actually executed on macOS).
- x86 CI A/B run 44898107917: d629351 (teardown v2 + event-balanced chunks) vs 68abd99 @20M NCPU=4 | base 2931-3087ms (med ~2959) → head 3043-3148ms (med ~3080, ~4% SLOWER); outputs identical | combined change REGRESSED — reverted 5b37ae0 (balanced chunks) to isolate; teardown v2 retest pending |
| x86 CI A/B run 44896828545: 6db309e (packable 256B LUT) vs fb2f810 @20M synth9 NCPU=4 | base 3579-4187ms (med ~3622) → head 3338-3453ms (med ~3381, ~6.7%); CSV + all parquet parts identical | packable LUT confirmed on x86 — KEEP |

## event-balanced sessionize chunks — REVERTED (x86 regression)
- 5b37ae0 replaced with_min_len(1<<16) run-count split with ~96 event-mass chunks to fix whale-run serialization on skewed data.
- Combined A/B (above) showed ~4% regression; reverted to isolate — teardown v2 alone re-benched next.
| Correctness: pending-field-at-EOF quote validation + saturating atoi (price overflow wrap) | adversarial EOF-quote + huge-price cases added to make_adv.py; fixtures 36/36, dev parity, diff 5/5, adversarial identical | committed d5fc335 (correctness, not perf) |

## parquet ZSTD-1 compression — experiment
- Enabled parquet `zstd` feature; both writers UNCOMPRESSED → ZSTD level 1.
- Motivation: uncompressed output leaves large dirty pages flushed serially at exit (competitor-measured ~0.15s wall at 20M); spec accepts arbitrary Parquet compression.
- Added src/bin/pqcmp.rs — semantic parquet comparator (schema + sorted row multiset) since outputs differ byte-wise by design.
- Gate: fixtures 36/36, dev CSV parity, diff 5/5, adversarial match, pqcmp MATCH (user_daily 117038 rows, campaign_daily 27400 rows vs uncompressed build; ~1.75x smaller files).
- x86 CI A/B pending vs parent.

## teardown v2 — x86 verdict (run 44898714635)
- d629351-era teardown v2 (+ parser fixes) vs 68abd99 @20M NCPU=4: base 4211-4360ms (med ~4236) → head 4125-4352ms (med ~4241). Wash within noise; outputs identical; **no crash** — buffer-scoped madvise is safe on x86.
- KEEP as neutral: zero regression, exits cleanly; benefit plausibly larger at 60M (more pages to unmap). NO_TEARDOWN opt-out retained.

## parquet ZSTD-1 — REVERTED (x86 regression)
- x86 CI A/B run 44899018969: 7815c70 (ZSTD-1) vs 2620447 @20M NCPU=4 | base 3370-3539ms (med ~3471) → head 3536-3649ms (med ~3607, ~4% SLOWER); CSV identical, parquet DIFFERS as designed (semantic MATCH proven via pqcmp locally).
- Verdict: zstd-1 CPU cost exceeds dirty-page flush savings at this scale. Reverted compression to UNCOMPRESSED + dropped zstd feature; kept pqcmp.rs (useful comparator, no runtime cost).

## mimalloc env tuning (PURGE_DELAY=-1, THP=1) — KEPT
- be9c190: run.sh exports MIMALLOC_PURGE_DELAY=-1 MIMALLOC_THP=1 — musl port of the competitor GLIBC_TUNABLES trick (arnabxero purge-free + kaedeng hugetlb).
- Gate: fixtures 36/36, dev CSV parity identical (env-only change, binary unchanged).
- x86 CI A/B run 44907976203 vs be363ff @20M NCPU=4: base 2991-3167ms (med ~3040) → head 2930-3177ms (med ~3001, ~1.3%). Small but consistent win; identical outputs. KEEP.

## in-Docker PGO — KEPT (wash at 20M)
- 11749be: Dockerfile gains synth training run — build synth, gen 3M events/750k clicks, instrumented -Cprofile-generate build, run inside build container, llvm-profdata merge, -Cprofile-use -Cprofile-correction rebuild (hansliu-dev recipe).
- x86 CI A/B run 44908070010 vs be9c190 @20M NCPU=4: base 2898-3113ms (med ~3018) → head 2932-3166ms (med ~3007). Wash within noise; identical outputs. Competitors measured ~5%; not reproduced at 20M here.
- KEEP as neutral: zero runtime cost, profile is real training data; plausible upside at 60M scale.

## date-prefix cache in parse_ts — KEPT (~2.3%)
- aa038e4: thread-local (u64,u16) -> day_ms cache on the 10-byte YYYY-MM-DD prefix; hits skip w0 SWAR check, month/leap/day validation, days_from_civil. HH:MM:SS + separators still verified per call (hit requires byte-identical prefix).
- Gate: fixtures 36/36, dev CSV parity identical, diff 5/5 vs ref.py.
- x86 CI A/B run 44908796491 vs a2f502c @20M NCPU=4: base 3339-3814ms (med ~3446) -> head 3344-3424ms (med ~3366, ~2.3%). All 7 head runs below base median; parquet byte-identical. KEEP.
- Known caveat (corpus): single-entry caches miss ~93% on shuffled input (joch44 v10). Upgrade path: small day-indexed table if hidden data proves unordered.

## in-Docker PGO — REVERTED (build failure under emulation)
- 11749be's instrumented training run exited 101 inside docker build on an ARM Mac building linux/amd64 (qemu-user): the synth layer runs fine, the profile-generate run panics under emulation. x86 CI had built it fine (run 44908070010) and measured it a wash (~0.4%, noise) — zero measured upside to justify a build-time failure mode.
- Reverted the Dockerfile to the single cargo-build shape of be363ff. mimalloc env (be9c190) and date-prefix cache (aa038e4) unaffected. If PGO is revisited, gate the training run behind a fallback so a failed profile capture degrades to a non-PGO build rather than failing the image.

## track score: 2.54s (submitted commit)
- User submitted the post-revert lineage (master tip 3014a7a / 3d25bf7-family: mimalloc env + date-prefix cache, PGO reverted out of Dockerfile). Track page returned **2.54s** vs the previous 2.77s.
- Consistent with the CI A/Bs (mimalloc ~1.3%, datecache ~2.3% medians at 20M NCPU=4) plus scorer variance.

## snappy parquet compression — KEPT (~1.1%, mechanism confirmed)
- 162a73c: Compression::UNCOMPRESSED -> SNAPPY in both writers + `snap` feature on parquet 56.
- Gate: diff 5/5 vs ref.py; pqcmp semantic IDENTICAL at 62M scale (6,675,187 user_daily + 28,014 campaign_daily rows); output 93M -> 41M.
- x86 CI A/B run 44914275576 vs 4f53ba9 @20M NCPU=4: base 3854-3996ms (med ~3965) -> head 3827-4012ms (med ~3922, ~1.1%, head wins 5/7). CSV identical; parquet bytes DIFFER (expected codec change — spot-check flags DIFFERS across codec boundaries, semantic equality via pqcmp).
- Phase evidence: head output_rows +73ms (snappy CPU) but total -43ms => exit-flush savings ~115ms > codec cost. Should scale up at 60M (output ~3x).
- KEEP. Note: workflow's `cmp` parquet check flags DIFFERS whenever base/head codecs differ; CSV diff + pqcmp are the real gates.
- x86 phase shape @20M NCPU=4 (from this run): ev_parse ~950ms, output_rows ~700ms, sessionize ~615ms, ev_dedup ~480ms, ev_sort ~255ms, cl_* ~620ms, links ~15ms. => prefetch canon_of dead (links trivial); next levers: hand-rolled parquet (output_rows), SIMD tokenizer (ev_parse), sessionize internals, fp dedup (ev_dedup).

## parquet PLAIN encoding + no statistics — KEPT (~2.2%)
- 3f9ad4a: WriterProperties -> Encoding::PLAIN + EnabledStatistics::None in both
  writers. Defaults dictionary-encode every column (hash table per part per
  column, ~2M distinct user_id strings) and compute min/max stats (full extra
  column pass). Neither survives as a win vs snappy.
- Gate: fixtures 36/36, dev parity, diff 5/5; pqcmp all 16 dev parts
  semantically identical. Parquet bytes DIFFER (expected — different encoding).
- x86 CI A/B run 44915112543 vs 9439a5f @20M NCPU=4: base 2746-2792ms (med
  ~2756) -> head 2670-2755ms (med ~2694, ~2.2%, head wins 6/7).
- Phase evidence: output_rows 514ms -> 469ms (-45ms, ~9% of phase) — the
  dict+stats cost isolated. All other phases noise. KEEP.
- Note: this run's machine was ~20% faster across the board than the last A/B
  (runner CPU varies between runs — only compare within a run). At the last
  run's CPU this is probably ~60-90ms.
- output_rows still ~470-700ms depending on CPU: remaining fat is Arrow
  RecordBatch materialization + parquet-rs encode pipeline => hand-rolled
  writer is still the lever there.

## WS-B direct Parquet writer — experiment, timing pending
- Branch `exp/hot-writer` from `73dd703`. `PARQUET_WRITER=snappy` (also the
  default) writes raw Snappy blocks, `uncompressed` writes verbatim pages,
  `arrow` uses the original Arrow writer. No image or CI script change.
- Both tables use PLAIN data pages v1 and compact Thrift metadata. Pages target
  256 KiB; a single long string may exceed that but not the signed page-size
  limit. The direct path consumes accumulator order, since row order does not
  matter. `WRITE_CSV=1` retains the baseline's CSV ordering.
- Local writer gate: 5 Rust tests pass. PyArrow 21 decoded all three modes from
  the same native binary on `synth` seed 13 (160k events/22k clicks, NCPU=3):
  both direct modes matched Arrow data and schemas (84,569 user rows and 20,628
  campaign rows, three parts each). Header-only events/clicks produced one
  schema-matching empty part per table in each mode.
- No performance result or keep decision yet. Full fixtures, official dev
  answer, differential oracle and paired x86 timings are coordinator gates.
