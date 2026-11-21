# Submicro: 310 ms practice target

Baseline `73dd703` (2.64 s on the supplied track-page PDF); final entry
at the time of that snapshot `3014a7a` (2.54 s). Two teams show 0.31 s.
No timing ties the local hansliu checkout (`efa72c0`) to a scored commit.
Neither the phase times below nor an estimated speedup are a track score.

## Where the time goes

| Existing x86 proxy, 20M/4 threads | Experiment | What to measure |
|---|---|---|
| Event parse ~810 ms | A: classify structural bytes once per 64 bytes, consume separator bits, validate wrapper quotes; no reads past mmap end | ev_parse and cl_parse, scalar vs AVX2 parity |
| Output ~470 ms | B: write plain column pages directly for both schemas, batch by actual byte size, compare raw vs Snappy | output_rows, total including exit, bytes, reader schema |
| Dedup ~350 ms; filter/scatter ~125 ms | D/E: compare exact packed IDs and shard-local fingerprint slots; fuse claim creation into parsing | ev_scatter + ev_dedup + ev_filter, total, peak RSS |
| Sessionize ~355 ms; ev_sort ~190 ms | E: consume complete person groups; start largest independent buckets first | phase total and slowest bucket; never split a person naively |
| Clicks ~345 ms combined | Reuse A, compare compact per-person click runs | click phases; byte-smallest click on ties |
| Links ~7 ms | Overlap only if useful on official generated input; join before first canonical lookup | critical path, not sum of worker CPU |
| Allocation/exit: unquantified | C: owned Linux mappings, request huge pages, reuse only exclusively owned dead buffers | system CPU, minor faults, RSS, wall; THP is not guaranteed |

The existing CI times native glibc binaries, 4 threads, warm files and the
in-repo `synth`, not the published generator or Docker entrypoint. In particular
it does not set the Dockerfile's x86-64-v3 flags. Use identical flags on both
sides or dispatch SIMD at runtime. Also measure the production image with the
official generator on x86 before predicting a score. Cutting just the 810 ms
phase cannot bridge an eightfold end-to-end gap.

## Layout and ownership at 60M

- Today: FEv 32 + DIn 16 + scattered DRec 24 bytes = 4.32 GB for 60M
  parsed rows while all three coexist, excluding capacity, input and scratch.
  Later Ev is 24 B/kept row. The kept marker is **one byte** per row.
- Reference: `parse_event` writes to **eid** buckets, not person buckets.
  A per-bucket 8-byte fingerprint/index slot checks full keys on a tag match,
  and a winner bitset records valid survivors. Person scatter follows global
  event-ID arbitration. A person-first variant MUST also emit a globally
  eid-partitioned claim, even for invalid and cross-person copies.
- Candidate wide keys: two 16 B keys + 8 B ingest + 8 B packed payload =
  48 B/row with 8 B struct alignment (56 B wide-payload fallback). Surviving
  person + packed payload: 24 B. At 60M that's 2.88 + up to 1.44 GB before
  clicks, input pages, tables, output and allocator slack. Check actual Rust
  sizes; keep compact keys if widening memory traffic outweighs parsing gains.
- Start with 128-record blocks; choose power-of-two eid/person buckets from
  actual volume (~40k/20k rows per bucket, ~2048/4096 buckets at 60M).
  At 8 workers, 127 unused tail slots x 2048 x 8 x 48 B is up to 100 MB
  when blocks are worker-local; do not replicate these tails per small task.
  Reservation model `2 * (event_bytes + click_bytes) + workers * 1 GiB` is
  virtual only. Check arithmetic, allocation failure, row-index bounds and
  actual cgroup peak below 24 GB, without relying on overcommit for safety.
  Reserve/commit in slabs; never donate mimalloc storage or live slices.

## Execution and gates

1. A, B, C, D: separate worktrees rooted at `73dd703`, original code (do not
   vendor competitor sources). Isolate writer codecs. Dense IDs only with
   exact reversible recognition, bounded allocation and generic fallback;
   no assumptions about generator constants. Never alter the oracle to bless
   a speed patch.
2. Run 36 hand-authored cases, public dev answer, 5 established differential
   cases plus >=20 generated seeds; compare decoded Parquet for both whole
   tables and inspect schemas even if empty. Gate the actual candidate binary:
   sibling shell scripts hard-code `solution/target/release` and fixed `/tmp`
   outputs; their final echo can mask failure. Use private dirs per candidate.
3. Interleave >=5 repeats per side, same host, log SHA, flags, medians,
   phases, correctness and verdict. Recheck marginal gains. Keep layout
   changes only after 60M memory/time check. Rebase/re-gate each combination.
4. E after A/C decisions, not before. F after E: separate PGO, prefetch,
   scheduling and exit experiments. No changes to master on absent evidence.
   A final candidate needs a clean amd64 image, uid 1000, network off,
   read-only input/root, empty stdout and 3 complete official-generator 60M
   runs. Only the team submits.

| Risk | Blocking cases |
|---|---|
| CSV wrappers, malformed row vs empty ID, literal CR, EOF and task split | E13, E15, E19, E20, L3; add 31/32/63/64/65-byte seams and missing LF |
| Dedup across users/files, invalid earliest copy | E4, E5; force a fingerprint collision and split copies across workers |
| Calendar/offset, late cutoff, epoch overflow | E6, E7, E12, E17, E18 |
| Session boundary, same instant, midnight, category tie | E1-E3, E8-E10, E14, E16 |
| Person name including unused linked ID; byte order | L1-L4, E11, E19; add long IDs and embedded NUL |
| Inclusive credit/assist window, lexical tie, assist-only rows | A1-A12, L4 |
| Writer schema/null/date/empty, multi-part, large pages | decode both tables against baseline; separately check zero-row schema and page limits |
