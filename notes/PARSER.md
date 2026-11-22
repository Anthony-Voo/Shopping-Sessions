# Parser work log (branch parse/*, from v59 = 5b5058b022d0905aa151228e6f8b8d7c6f97cbf0)

All timings: this VM (4 vCPU Firecracker, AVX-512, 15 GB), data/p60m (seed 77, 60M events + 15M clicks),
NCPU=8, Dockerfile glibc tunables, warm page cache, ws/ab60.sh (interleaved A/B, medians).
Parse lap = `events parsed` - `read` (it includes the key partition).

## Measurement caveats (Nov 22, ~02:30 UTC)
- **No hardware counters**: `perf stat` reports instructions, cycles, branch-misses as "not supported" in this
  VM; valgrind cannot run AVX-512 code. Instructions/line, IPC and misses/line could not be measured.
  Software counters (page-faults, task-clock, cpu-clock sampling) work.
- **Free-page reporting** is on in this guest (kernel cmdline `page_reporting.page_reporting_order=11`,
  `/sys/module/page_reporting/parameters/min_free_ms` = 0): memory a run frees goes back to the host, and the next
  run's first touch costs host faults. System time per 60M run varies 2-20 s; same-binary parse laps vary
  1.2-3.0 s. Raising min_free_ms (to keep pages in the guest) was refused by the session's permission policy, and
  pre-touching memory before each run did not help. So: more rounds, medians, interleaving.
- v59's own output is not byte-deterministic at NCPU=8 (user_daily part files differ run to run: row order
  across parts depends on thread timing). Exactness checks used instead: (1) `ws/canon.py` hash of every table,
  all rows sorted, all columns; (2) NCPU=1 runs are byte-identical (cmp) run to run.

## Parse breakdown at 60M, v59 (medians of 12-24 runs)
| part | seconds of parse lap | how measured |
|---|---|---|
| compute | 0.95 | 58.7 ns/line single thread (PARSESTAGE=4, 20M, warm memory) x 64.8M lines / 4 cores |
| fresh memory (page zeroing of the 3.5 GB DKey+PEv) | ~0.28 | 1.1 core-s: 4 x `parse lines` (1.445 s) = 5.8 core-s minus compute 3.8 minus links 0.8 |
| links contention | 0.11 | same binary, links thread deferred until after parse (LINKS_DEFER test build): `parse lines` 1.492 -> 1.381 (12 rounds) |
| key partition (inside the lap) | 0.29 | `events parsed` - `parse lines` |
| other (tail imbalance, products) | ~0.05 | remainder |
| **parse lap** | **~1.70-1.75** | |

Single-thread stage costs (PARSESTAGE, 20M, ns/line): structure 13.0 | + field bounds/quotes 10.8 (23.0) |
+ values 20.4 (44.2) | + hash + record write 14.5 (58.7).
Values split (each alone on top of stage 2): event ts +10.7, ingest +4.3, type/null flags +2.0, event-id hash +6.9;
price/product/ok logic ~+5.6. Page faults per single-thread run ~28k (all levels).

Links contention, settled: real but small. Deferring the links thread cuts `parse lines` by 0.11 s (-7.5%), but the
total gets 1.9% worse (links becomes the critical path at `links joined`). The thread holds ~0.78 core-s; moving
it elsewhere only helps if that CPU shrinks. Rank: last.

Input shapes (p20m sample): ts F1 `...Z` 57%, 13-digit epoch 33%, `+hh:mm` 9%, other <1%; ingest always F1;
~80% of lines contain a quote; ids 9 or 33 bytes.

## Ranking of open directions (seconds they could remove at 60M)
1. Key partition fused into the parser (keys straight into shard blocks): up to 0.29 s of lap, plus 1.56 GB less fresh memory.
2. Cross-line SIMD timestamp decode: ts+ingest = 15 ns/line = 0.24 s compute; realistic 0.1 s.
3. Fresh memory via smaller records: 26 B PEv / 20 B DKey ~0.05 s each.
4. Links off the parser's cores: 0.11 s of lap, ~0 of total.
5. Vector id hashes: 64x64->128 multiplies emulated with 4 vpmuludq + ~10 ops per 8 lanes: no fewer uops than scalar mulx; skipped.

## Attempts
| # | change | result | verdict |
|---|---|---|---|
| A1 | v6a: cross-line ts/ingest decode, 8 lanes, AVX-512 gathers | 64-67 vs 58-60 ns/line (PARSESTAGE, PGO and non-PGO) | slower |
| A2 | v6b: same, words via scalar loads into arrays (no gathers) | 63.9 vs 62-63 | tie; profile: 7% on the store-forwarding stall of the vector reload |
| A3 | v6c: lanes built in registers (masked vpbroadcastq) | 63.6-64.4 vs 60-62 | tie. Decode alone: v6 33 vs v5 40.4 ns/line (vector saves 7.4 ns) but the per-line tail ran 12 ns slower inside a closure (Vec len/ptr reloaded through pointers) |
| A4 | v6d: one loop nest (fill 8 lines, decode, tail) instead of a closure | 56.9 vs 58.5 ns/line (-2.7%); 60M same binary PARSER=5 vs 6, 12 rounds: total +0.79% (7/12), parse -1.06% | tie (exact: SELFTEST v6==v5 on 3480 chunk variants incl. 400k synthetic lines with every ts form, quotes, all truncations; mutation tests caught planted F2/F3 bugs; 20M canonical hash == v59) |
| A5 | double prefetch (+64 B) in v6 | 56.2-58.3 vs 57.1-58.8 | tie, dropped |
| B1 | v6kb: DKeys written by the parser into per-(chunk, shard) 128-key blocks, no key partition; dedup copies a shard's blocks into scratch | total 5.50 vs 4.28 (0/12); parse lines +0.32 | much worse: 256 direct store streams per thread = unprefetched RFO misses |
| B2 | v6kb2: + 8-key write-combining buffer per shard, flushed as 3 aligned 64 B streaming stores | parse lap 1.52 vs 2.01 (5/6), total -1.1% (2/6); dedup +0.21 (scratch copy) | partition gone, cost moved to dedup |
| B3 | v6kb3: dedup walks the blocks in place (index = block * 128 + offset) | 8 rounds: total 4.58 vs 5.43 (6/8), parse 8/8, peak RSS 10.94 vs 11.93 GB; dedup still +0.13 | promising, noisy machine |
| B4 | v6kb4: + prefetch two blocks ahead in dedup | dedup 0.61 vs 0.46, 0/6 | worse, reverted |
| B5 | v6kb5: block writer with regular stores instead of streaming | vs NT: total -2.25% (3/12), parse -10% | NT kept |
| B6 | v6kb3 vs v59, 12 rounds | total 4.61 vs 5.32 (7/12), parse 1.55 vs 2.35 (12/12), RSS -1 GB. But on quiet pairs (both sys < 2.5 s, 6 pairs over two sets) B lost 6/6: 4.66 vs 4.37 | the apparent win is v59 paying this VM's host-fault penalty on its fresh key_all |
| B7 | v6kb6: one global key region (sized like key_all) so dedup frees one block for the user shuffle, as in v59 | 10 rounds: 6/10, but quiet pairs 1/3; same binary PARSER=5 vs 6 (4 rounds, quiet): totals 4.22-4.38 vs 4.58-4.81, sys 1.7-1.8 vs 2.4-2.6 | **loss: dropped**. Parse lap does reach 1.32-1.42 s (partition gone; parse lines unchanged), but dedup +0.05-0.2 (now overlaps the links thread), user shuffle+clicks +0.1, regroup +0.3, sessionize +0.15. A dummy per-chunk key buffer (freed at events parsed, like v5's key Vecs) gives most of the later-stage time back: v5's key vectors and key_all pre-fault memory that the later stages recycle, so removing them moves page faults into later, less parallel stages. Code: /tmp/claude-0/main_kb6_final.rs (not committed) |

Design note (B): to make the fused key partition pay, the pipeline's later stages must need less fresh memory too
(or reuse the key blocks for their own buffers); on its own it only moves fault cost downstream. A machine without
free-page reporting is needed to measure fault costs reliably; on this VM they swing 2-20 s of sys per run.
