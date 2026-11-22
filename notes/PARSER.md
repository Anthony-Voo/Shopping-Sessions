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
| B8 | v6kb7: per-chunk key arenas again + an untouched placeholder block of the payload size, freed just before the user shuffle (the shuffle best-fits into it, as into v5's key_all; the arenas stay free for later per-chunk buffers, as v5's key Vecs) | same binary PARSER=5 vs 6, 16 rounds (very noisy period: 2 quiet pairs): total 8/16, median -3.8%; quiet pairs 4.248 vs 4.276 (parse -0.23, dedup +0.09, clicks +0.08, sessionize +0.10). Peak RSS 12.01 vs 11.94 GB | tie before PGO regen. The recycler's best fit never splits blocks: that is why B6/B7 starved the later stages |
| B9 | v6kb7 + PGO profile regenerated for this source (ws/pgo.sh: p20m x2 + fresh 2M seed 9101) | single thread: v6 48.5-50.2 vs v59 52.8-56.3 ns/line (-9%). 60M vs v59, 16 rounds: all runs 8/16, median -6.5% (runs are bimodal: clean ~3.8-4.4 s with sys ~2 s, or hit by host faults ~5.0-5.4 s with sys 4.4-5.4 s; B was hit 7x, A 4x). Clean runs (ws/clean.py, sys < 3 s): total 4.037 -> 3.969 (+1.7%), parse lap 1.578 -> 1.355 (-14%), clean pairs 4/6 | tie. Clean stages: parse -0.23, dedup +0.105, clicks +0.078. links thread now done at 1.65 s, i.e. during dedup, where time-slicing 9 threads on 4 CPUs thrashes the 4 MB per-thread dedup tables |
| B10 | v6kb8 = B9 + rayon workers at nice 10 (WORKER_NICE; links thread stays at 0) | links thread done at 0.80 s instead of 1.52-1.72. 60M vs v59, 16 rounds: total +1.03% (7/16); clean: 4.076 -> 4.027 (+1.2%), clean pairs 3/7; parse lap clean 1.576 -> 1.373 (-12.9%). Clean stages: parse -0.20, dedup +0.066, clicks (user shuffle now faults the placeholder) +0.054, sessionize +0.017 | tie at NCPU=8 |
| B11 | v6kb8 at NCPU=4 (no oversubscription), 8 rounds | clean medians 3.899 vs 3.897 (tie) | tie |
| B12 | PGO profile regenerated for the exact kb8 source | single thread 55.9-59.1 vs 47.9-56.2 ns/line with the kb7 profile | worse/noisy: kb7 profile kept (the binary A/B-tested in B10) |

kb8 exactness (commit on this branch): SELFTEST on p20m (v6 == v5 on 3930 chunk variants + 400k synthetic lines;
all older checks), gate PASS with the kit-hash line skipped (the dev answer was re-issued Nov 21 19:55, after
gate.sha256 was recorded; v59 and kb8 both MATCH it; 36/36 fixtures, edge MATCH, oracle seeds 41-44 MATCH),
difftest seeds 601 and 602 MATCH (2M), p20m canonical table hashes == v59, NCPU=1 p20m Parquet files cmp-identical to v59.
| B13 | links thread profiled (LINKSLAP=1, NCPU=1, 60M): scan 0.10, hash 0.05, intern + union-find 0.48-0.51, roots 0.04, person ids 0.04, root names 0.02, lks 0.1 s (~60% of the thread's samples are kernel: fresh-memory faults) | | |
| B14 | v6kb9 = kb8 + links: union-find in a second pass over the interned id pairs with parent[] prefetched 16 pairs ahead; pairs / id vectors pre-sized | intern 0.33 + union 0.05 (was 0.48-0.51 together); thread 0.73-0.80 vs 0.82-0.88 s. 60M vs kb8, 12 rounds: +0.66% (6/12), clean +2.2% (2 of 3 clean pairs) | small gain, kept in the stack |
| B15 | single thread (NCPU=1) PARSER=5 vs 6: key partition 2.2-2.4 s removed, but dedup +0.4-0.6 s and user shuffle +2.3 s (all sys: first touch of the 1.85 GB placeholder costs ~1.2 s/GB single-threaded on this VM; v5 paid the same inside the key partition) | | explains the ties: fault cost moves, the copy saving is eaten by dedup reading scattered blocks |
| B16 | 512-key blocks (12 KB) instead of 128 | single-thread dedup gap 0.4-0.6 -> 0.21-0.27 s; prefetching the next block: no change (reverted) | kept (kb10) |
| B17 | v6kb10 = kb9 (links) + 512-key blocks, vs v59, 16 rounds | all: -1.54% (7/16); clean: 3.810 vs 3.989 (-4.7%, 1 of 4 clean pairs); parse lap clean 1.512 -> 1.316; peak RSS 12.15 vs 11.94 GB | tie/loss; not committed (patch: /tmp/claude-0/kb10_over_kb8.patch) |

Pooled over the three key-bucket candidates vs v59 (kb7+PGO, kb8, kb10), clean-run total medians: +1.7%, +1.2%, -4.7%.
Same-binary clean medians move by +-3-5% between sets on this VM, so effects under ~3% cannot be resolved here.

## Hand-over (Nov 22 ~04:30 UTC)

### Parse breakdown at 60M (seconds of the parse lap, clean runs, NCPU=8, this VM)
| | v59 | kb8 (branch HEAD code) |
|---|---|---|
| compute | 0.95 (58.7 ns/line single thread) | 0.79 (48.5-50.2 ns/line, PGO regenerated) |
| fresh memory (DKey + PEv first touch) | ~0.28 | ~0.28 in parse (same bytes), but v59's key_all faults (1.85 GB) move to the user shuffle |
| links contention | 0.11 (thread done at ~1.6 s, overlaps parse + partition) | ~0.2 (thread at nice 0 vs workers at nice 10: done at 0.66-0.80 s, all inside parse) |
| key partition | 0.27-0.29 | 0 |
| other | ~0.05 | ~0.05 |
| **parse lap** | **1.51-1.58** | **1.32-1.37** |
| total | 3.81-4.08 | 3.97-4.03 (tie) |
Instructions/line, IPC, misses/line: not measurable (no PMU in this VM; see caveats).

### Changes
| Change | Branch / full hash | Parse lap v59 -> new (clean) | Total 60M median (rounds won) | Peak RSS | Exact | Verdict |
|---|---|---|---|---|---|---|
| v6d cross-line AVX-512 ts/ingest decode | parse/xsimd 564b2555e195f485d9c546a07087e055d5ec0d09 | 1.663 -> 1.671 (same binary P5/P6) | +0.79% (7/12) | 11.91 -> 11.92 | yes | tie |
| kb3 key blocks, no partition | not committed | 1.717 -> 1.552 | clean -3.9% (1/8 clean pairs) | 11.93 -> 10.94 | yes | loss |
| kb7 + placeholder + PGO | not committed | 1.578 -> 1.355 | all -6.5% (8/16), clean +1.7% (4/6) | 11.93 -> 11.98 | yes | tie |
| kb8 = kb7 + workers at nice 10 | parse/xsimd 0ff446e39f9278d52d890e78f5740d6bd48e2a0f | 1.576 -> 1.373 | all +1.03% (7/16), clean +1.2% (3/7) | 11.92 -> 11.95 | yes (SELFTEST, gate*, difftests 601/602, 20M == v59, clean offline build) | tie |
| kb10 = kb8 + links UF prefetch/pre-size + 512-key blocks | not committed | 1.512 -> 1.316 | all -1.54% (7/16), clean -4.7% (1/4) | 11.94 -> 12.15 | SELFTEST only | tie/loss |
(*) gate with the kit-hash line skipped: the dev answer was re-issued after gate.sha256 was recorded; v59 and kb8 both MATCH it.

### Submit
**keep 5b5058b022d0905aa151228e6f8b8d7c6f97cbf0.** No parser change met the bar (>= 8/12 rounds and > 2.5% median at 60M).
kb8 reaches the parse target (lap 1.32-1.37 s < 1.4 s, exact) but its total is within noise of v59 (+1.0-1.2%):
the parse saving is paid back downstream (user shuffle now first-touches the memory the key partition used to fault,
dedup reads scattered key blocks). Expected board change if kb8 were submitted anyway: 0 +- 3%.

Docker: no daemon in this VM (/var/run/docker.sock missing). Clean build instead: `git archive HEAD | tar -x -C /tmp/claude-0/clean`,
then in it `RUSTFLAGS="-C target-cpu=cascadelake -C relocation-model=static -Cprofile-use=/tmp/claude-0/clean/pgo/merged.profdata" cargo +1.90 build --release --locked --offline`:
dev answer MATCH, p20m canonical hashes == v59, NCPU=1 p20m Parquet files cmp-identical to the native build and to v59.
