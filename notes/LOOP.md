# Shopping Sessions self-improvement loop

Goal: board time ≈ 0.5 s (local best ≈ 0.5 s at 20M; board ≈ 0.9–1.0 × local). Current #1 on the board: 0.65 s.
Deadline: Sun Nov 22, 16:00 UTC (11:00 ET). Final ranking = 60M new events, so memory footprint matters.

## State (update every iteration)
- Workspace (from Nov 21 13:00 UTC): edit /home/user/shopping-sessions/src only; never write in koillea. Gate: scratchpad/ws/check.sh [BIN]; difftest: ws/difftest.sh N SEED -- BIN; A/B: ws/abm.sh A B N OUT. Notes live here (notes/ on dev).
- Current best: v59 (v58 + non-PIE static relocation model; dev; main = v58 pending 60M). Before: v58 (v57 + PGO; dev). Before: v57 (v56 + dedup table 2x capacity; dev). Before: v56 (v55 + click equal-ts check folded into the fix-up; dev). Before: v55 (v54 + INT32 counts + faster links + fused resolve partition; dev). Before: v54 (v53 + one-pass sessions; dev). Before: v53 (v52 + no 4 KB-page churn in resolve/sessionize: pre-sized buffers, per-thread reuse; dev). Before: v52 (v51 + lazy assists + 2-pass radix; dev). Before: v51 (v50 + whole-shard radix sort by ts, no per-person sorts; main.v51rx2.rs, binary s_v51rx2; dev b89e1be). Before: v50 (v49 + per-person u64 key sorts + 4-byte click refs; main.v50st.rs, binary s_v50st; dev 6b47f08). Before: v49 (v48 + 26 B event record + 40 B rows + click scanner; main.v49stack.rs, binary s_v49stack). Before: v48 (v47 + 24 B campaign accumulator + 2x interner table; main.v48s23.rs, binary s_v48s23). Before: v47 (v46 + 128-bit multiply id hash; main.v47hash.rs + recycle.v46remap.rs, binary s_v47hash). Before: v46 (v45 + mremap gathering in the allocator; recycle.v46remap.rs, main.v45acc.rs, binary s_v46remap). Before: v45 (v44 + largest-first sessionize, per-thread accumulators) — source `…/scratchpad/main.v45acc.rs`, binary `…/scratchpad/s_v45acc`. Before: v44 (v43 + final person ids at resolve, no remap) — source `…/scratchpad/main.v44b.rs`, binary `…/scratchpad/s_v44b`. Before: v43 (v42 + PR 14 B) — source `…/scratchpad/main.v43pr14.rs`, binary `…/scratchpad/s_v43pr14`. Before: v42 (v39 + packed UCl/CR + hash product table) — source `…/scratchpad/main.v42stack.rs`, binary `…/scratchpad/s_v42stack`. Before: v39 (v37 + PEv packed to 30 B) — source `…/scratchpad/main.v39pev30.rs`, binary `…/scratchpad/s_v39pev30`. Before: v37 (v36 + prefetched links interner) — source `…/scratchpad/main.v37links.rs`, binary `…/scratchpad/s_v37links`. Judge parse changes on the PARSE LAP (mean of 6 interleaved runs), not only total. Before: v36 (v35 + pext field bounds; PARSESTAGE=1..4 staged parser bench) — source `…/scratchpad/main.v36pext.rs`, binary `…/scratchpad/s_v36`. USER FOCUS: PARSER SPEED ONLY. Before: v35 (v30 + lazy input mapping) — source `…/scratchpad/main.v35lazy.rs`, binary `…/scratchpad/s_v35`, on `dev`. Warm = v30; cold −0.5 s. ALSO TIME COLD RUNS (python3 -I scratchpad/evict.py data/p20m before a run). Before: v30 Local ~1.30–1.32 s. Before: v29 (SH_BITS=8). Previous: v26 — source `/tmp/claude-0/-home-user/7093f2af-7756-5848-9566-55372d396f64/scratchpad/main.v26bench.rs` (= v26 + PARSE5BENCH=1 single-thread parser benchmark; same pipeline),
  binary `.../scratchpad/s_v26`, on Anthony-Voo/Shopping-Sessions `dev` and `main` at be891fc. Board 1.49 s (submission 3).
- Local best (20M, `tools/ab.sh`): ~1.45–1.54 s depending on machine noise.

## One iteration (do exactly this)
1. Read this file. Pick the highest-ranked idea in the backlog whose status is `todo`.
2. Start from the current best source: `cp <best source> fast/src/main.rs` (work dir: `tracks/shopping-sessions`).
3. Implement ONE idea. `cd fast && cargo build --release`.
4. Correctness (all must pass, else fix or abandon):
   - `SELFTEST=1 fast/target/release/sessions data/p20m out/p20m` if the parser changed
   - `tools/check.sh` → `GATE PASS`
   - `tools/difftest.sh 2000000 <fresh seed>` → `MATCH` (use a seed not used before: next = 110, 111, ...)
   - test-only knobs for rare paths when the change has one (e.g. `KBTINY=1`)
5. Speed: `tools/ab.sh <best binary> fast/target/release/sessions 8` plus 2–3 runs of stage laps.
   Keep only if B beats A by more than ~3% AND the relevant stage laps agree. Otherwise it is a tie: revert (save the source as `scratchpad/main.vNN_<idea>_tie.rs`).
6. If kept: save `scratchpad/main.vNN_<idea>.rs` and `scratchpad/s_vNN`, update State above, copy the source to
   `/home/user/shopping-sessions/src/main.rs`, build with `cargo +1.90 build --release --locked --offline` (as the Dockerfile),
   commit on `dev` with the measured numbers, and `git push origin dev`. NEVER push `main` (the user submits; ask first).
   NEVER push to koillea/goonies-buildathon.
7. Log the result in the table at the bottom (version, idea, A/B numbers, verdict). Mark the idea `done`/`tie`/`failed`.
8. If 3 ideas in a row tie, re-profile (`perf record -e cpu-clock` on a line-tables build) and add new ideas from the profile.
9. When a kept version is ≥10% faster than what is on the board, tell the user it is worth a submission.

## REDESIGN backlog (user chose: redesign the pipeline; target is ~1 s total savings, so only structural changes)
| # | Idea | Expected | Status |
|---|---|---|---|
| R1 | Drop the user grouping: after dedup, look up each winner's person in a global linked-user table (built by the links thread), emit person records straight into person shards (one partition instead of uid partition + resolve + remap + person partition). Assign dense per-shard person indexes inside sessionize. Measure stage times; keep v42 path for comparison. | −0.15 to −0.3 s | rejected on analysis: bytes moved per event 170 B today vs 144–166 B, and cache-local per-shard lookups become random global lookups (20–40 MB table) |
| R2 | Sessionize in one pass: radix-sort each person shard by (person, ts) once instead of counting sort + per-person sorts | −0.1 s | done (v50 + v51): 3-pass LSD radix by ts over the shard + stable group by person; per-person sorts gone |
| R3 | Clicks: same direct-to-person routing as R1 | −0.05 s | dropped with R1 |
| S4a | Stage 4: dedicated click parser (128-byte fast path, prefetch, local month cache) | clicks −30% | tie: 1-core stage4 0.857→0.773, 4-core 0.182→0.183, best-of-8 +2.5%/−1.5%; stage 4 is memory-bound with 4 cores (main.v46clk.rs) |

## Memory reuse (measured Nov 21 10:30 UTC): writing never-touched memory costs ~3x reusing it
(4 threads, 2 MB pages: fresh 1 GB 0.100 s vs rewrite 0.032 s; 4 KB pages fresh 0.165 s). Pipeline touches ~2.6 GB fresh.
| # | Idea | Expected | Status |
|---|---|---|---|
| M1 | Measure fresh MB per stage | diagnostic | done (FRESHLOG via RssAnon): parse 1417, key shuffle 479, clicks+uid 263, resolve+person 260, sessionize+write 61 MB. Avoidable ~450-520 MB = ~0.03-0.04 s (3%) |
| M2 | Reuse freed memory for later big outputs | −0.05 to −0.1 s | done via allocator mremap gathering (v46): median 1.438→1.354, best 1.395→1.280; +256 KB pool threshold adds nothing |
| M3 | Person stage still takes ~260 MB fresh | −1-2% | split oversized free blocks: worse (median 1.348→1.381, 1/12 pairs), regroup still +272 MB fresh; cause not the whole-block hand-out. Needs a per-allocation log to find. |

## Sessionize (profile Nov 21 11:30: ~35% of the stage is the counting sort: count pass reads + eg/cg scatter)
| # | Idea | Expected | Status |
|---|---|---|---|
| S1 | In-place counting sort (cycle leader) | sessionize −10%? | worse: sessionize 0.280→0.315, total 1.356→1.448 (3/12); reverted (main.v48inplace.rs) |
| S2 | Campaign accumulator 24 B | −1% | done with S3 (v48) |
| S3 | Links interner table 2x | −1% | done with S2 (v48): 24 runs median 1.401→1.345, 20/24 |

## Next ideas (after v45)
| # | Idea | Expected | Status |
|---|---|---|---|
| H1 | One 128-bit multiply hash chain | parser −3-5% | DONE v47: parser 75.5→65.9 ns/line, total median −2.7% over 24 runs (17/24) |
| H2 | Event record 30 → 26 B (chunk-relative uid offset, lazy names) | −2% | tie over 24 runs (13/24; first set −2.1% 8/12, second 5/12); reverted (main.v48pev26.rs) |

## Parse-stage backlog (user focus)
| # | Idea | Expected | Status |
|---|---|---|---|
| P3 | Pack the other records too: CR (24 B), UCl (40 B), PR (16 B) — check each for padding | −1–3% | todo |
| P1 | (tie) Dedup key 24 → 20 B (packed, 32-bit loc = chunk<<21 \| idx; loser's user shard from its payload): parser writes 83 MB less, key shuffle moves 17% less | parse −2%, keys −17% | todo |
| P2 | Event payload: drop 2 B padding by moving cat into price's top bits only if all category ids < 256 at runtime (fallback layout otherwise) | parse −3% | todo |

## Backlog (ranked by expected wall-time gain at 20M; also weigh 60M)
| # | Idea | Expected | Status |
|---|---|---|---|
| A1 | Autotune SH_BITS (shard count 2^9 / 2^10 / 2^11): one build per value, ab.sh vs best | ±5% | done: 2^8 wins (1.488 vs 1.559) |
| A2 | Autotune event chunk size (chunks(): try 0.5x / 2x current) | ±3% | done: 32 MB (kept with A3) |
| A3 | Autotune write-combining burst (>=128 / 192 / 384 B) | ±2% | done: 384 B (kept with A2) |
| A4 | Autotune click chunk size (7<<19 now: try 2x / 0.5x) | ±1% | tie (4x: 1.383 vs 1.402; 16x slower). Re-sweep around v30: SH 7/9 tie, CHUNK 64 MB slower |
| 1 | (deferred: needs 64-bit-only event identity, ~1e-4 wrong-answer risk at 60M, for ~3%) Dedup key 24 B → 16 B (eh u64, ing i32, loc u32 = chunk<<20 \| idx; loser's user shard from its payload). Keep 96-bit identity by storing eh2 in place of… (if impossible, use eh only only if exact compare of event_id bytes is added for equal eh) | −5% (bytes in 3 passes + zeroing) | todo |
| 2 | Event payload PEv 32 B → 24 B (offset-in-input ref instead of 48-bit pointer, price/cat packed) | −4%, more at 60M | deferred: 209 bits needed > 192 |
| 3 | Parser field stage: quote/field bounds from the 128-bit quote mask instead of 14 byte loads | −3% | failed (slower) |
| 4 | Parser value stage: cheaper ts/ingest parse (common F1 path first), branchless kind/price | −4% | todo |
| 5 | Links thread: open-addressing interner with prefetch (1.8M random lookups) | −3% (gives parse a core back) | tie: links thread 0.30→0.23 s uncontended, total 1.430 vs 1.427 |
| 6 | Sessionize: fuse counting-sort count pass into the person partition (counts by local person) | −3% | todo |
| 7 | Fuse dense-id remap into the person partition (map during scatter) | −2% | todo |
| 8 | Clicks: campaign lookup via perfect-hash/array on campaign id bytes instead of FxHashMap<&[u8]> + memcmp | −2% | todo |
| 9 | Reduce fresh-memory zeroing: allocate stage outputs into freed blocks (segment outputs ≤ freed block size) | −5% | todo |
| 10 | Parquet writer: start writing user parts while sessionize finishes (overlap), dictionary for top_category | −2% | todo |

## GOAL 0.35 s (user, Nov 21 13:30 UTC: run nonstop until 0.35; checkpoints -> dev; main only if >=5% better than v51 over 24 runs)
Board: v51 = 1.19 s. Floor test (scratchpad/floor): touching all 1.5 GB = 0.06 s, counting lines = 0.10 s (4 threads, warm), so all cost is per-line CPU.
Per-line parse (1 core, PARSESTAGE): structure 12.5, fields/quotes 13, values (ts/ingest/kind/price/product) 23, hash+records 15, misc 7 = ~70 ns.
Stage walls (warm, 4T): parse 0.51, key tail 0.08, dedup 0.07, user shuffle 0.05, click parse 0.09, click shuffle 0.02, regroup 0.15, sessionize 0.24, write 0.05.
| # | Idea | Expected | Status |
|---|---|---|---|
| G1 | Parse structure: sliding 3-block mask window (each block scanned once) | parse -5% | tie (stage 1 slower 16 vs 12.5 ns, total ns/line flat), reverted |
| G2 | Values stage: profile sub-costs (event ts forms, ingest month cache, kind, price, product lookup) and cut the largest | parse -10% | G2a (month cache for event ts F1) tie; remaining sub-costs todo |
| G3 | Hash+records: write PEv/DKey with non-temporal or batched stores; skip h12 for losers impossible (need key) -> try cheaper eid hash (h64 only + 32-bit check from len/bytes) | parse -5% | todo |
| G4 | Sessionize attribution: lazy assisted counting | sess -15% | near-tie (sess -2%), in stack |
| G5 | Regroup: profile resolve vs person shuffle; fuse dense remap into shuffle (backlog 7) | regroup -20% | todo |
| G6 | Key tail + dedup (0.15): dedup directly from parse-time key shards without the extra partition pass | -0.05 s | todo |
| G7 | Structural: parse writes person-ready records once links are known (links thread first, ~0.3 s on 1 core: make it parallel so it finishes before parse needs it) | big | todo (hard) |

## Results log
| Version | Idea | A (best) | B (new) | Verdict |
|---|---|---|---|---|
| v20 | dedup clears losers only | 1.772 | 1.568 | kept |
| v21 | inline 64 B bursts | 6.35 cpu-s | 6.04 cpu-s | kept |
| v22 | per-line parser | 1.545 | 1.462 | kept |
| v23 | person-shard locality | 1.531 | 1.588 | reverted |
| v23b | fused keys in parser (WC) | 1.547 | 1.554 | tie |
| v24 | parser prefetch | 1.536 | 1.481 | kept |
| v25 | heap-stream prefetch | — | — | tie |
| v26 | 128 B line fast path + links lists | 1.521 | 1.454 | kept |
| v27 | open-addressing resolve table | 1.482 | 1.553 | tie |
| v28 | quote-mask field bounds | parser 1-thread 1.46–1.64 | 1.60–1.63 | failed (slower) |
| v29 | SH_BITS 10 → 8 (sweep 7..11) | 1.559 | 1.488 | kept, pushed to dev |
| v30 | CHUNK 8→32 MB + burst 192→384 B | 1.429 | 1.321 | kept, pushed to dev |
| v31 | click chunk 4x / 16x; SH 7, 9; CHUNK 64 MB (around v30) | 1.402 | 1.383 best | tie, not kept |
| v32 | separate person-shard count PSH 2^10/2^11/2^12 (with NSH 2^8) | 1.380 | 1.394 (2^10) | tie, not kept: sessionize time does not depend on shard size |
| v33 | prefetched open-addressing links interner | 1.427 | 1.430 | tie, not kept (stage −25%, total unchanged) |
| v34 | fused key bucketing in parser (sampled capacity, 384 B bursts) | 1.346 | 1.378 | tie/slower, retired |
| v35 | lazy input mapping (no MAP_POPULATE) | cold 2.16–2.32 / warm 1.380 | cold 1.57–1.73 / warm 1.370 | kept, pushed to dev |
| v36 | pext field bounds (quote check for all 7 fields from bit masks) | parser 70.9 ns/line | 68.2 ns/line (5/5 pairs) | kept (parser −3.8%) |
| v36x | event-time month cache local; unchecked quote reads | parser 70–72 | tie / slower | reverted |
| v37x | branch-free event-time parser (all formats computed, selected) | parser 67–73 ns | 81–86 ns | reverted (exact, slower) |
| v37y | links before parse (diagnostic) | parse 0.54–0.66 concurrent | links 0.3 s + parse 0.48–0.57 | concurrent stays (links costs parse ~0.05–0.1 s) |
| v37z | MADV_POPULATE_WRITE on allocator blocks | parse 0.54–0.66 | 0.65–0.71 | reverted (zeroes unused capacity) |
| v37 | prefetched links interner (re-test of v33 on v36, judged on parse lap) | parse 0.573 / total 1.463 (means of 6) | parse 0.525 / total 1.420 | kept, pushed to dev |
| v38x | MADV_POPULATE_READ per input chunk | parse 0.568 warm / total 1.68 cold | 0.548 warm / 1.85 cold | rejected (cold regression) |
| v38y | per-line scanner for links pairs | links thread 0.21–0.25 s | 0.21–0.25 s | tie, reverted |
| v38 | 20-byte packed dedup keys | 1.402 / 1.354 (best of 8, x2) | 1.417 / 1.357 | tie, reverted (parse −6% offset by slower loser path) |
| v39 | PEv packed(2): 30 B instead of 32 | total mean 1.456, best 1.428 | 1.381, best 1.393 (5/6 pairs) | kept, pushed to dev |
| v40x | ts_any: branch-light event time with month cache + cmov selects | parser 65–71 ns | 74–79 ns | reverted (exact, slower; ts dispatch idea closed) |
| v40y | hash-keyed product table (no memcmp) | parse mean 0.577 / total 1.481 | 0.583 / 1.481 | tie, reverted (1-thread parser −2%) |
| v40z | pack UCl/CR | — | — | dropped: not parsing (user focus), and build needed a sort rewrite |
| v41x | links thread at nice 19 | parse 0.581 / total 1.466 | parse 0.523 / total 1.464 | tie, reverted: moving work between stages does not change the total (CPU-bound) |
| v41 | pack UCl (36 B) and CR (20 B) | mean 1.462, best 1.353 | 1.431, best 1.332 | ~2%, under threshold: parked (main.v41pack_small.rs), stack later with other small wins |
| v42x | raw-pointer record writes in parser | 69.1 ns/line mean | 67.9 | tie, reverted |
| v42 | stack: packed click records + hash product table | 1.388 / 1.402 / 1.412 (best of 8) | 1.291 / 1.355 / 1.374 | kept, pushed to dev |
| v43x | profile: kernel page zeroing = 18% of single-thread CPU; fresh 2.6 GB (parse outputs ~1.1 GB inevitable, key array 580 MB) | — | — | finding |
| v43y | fused keys with 4 KB-page buckets (to avoid zeroing slack) | v30 1.564 / huge-page fused 1.551 | 1.629 | worse, reverted (4 KB faults cost more) |
| v43 | PR packed to 14 B (u16 category) | best 1.385 / 1.378 | 1.346 / 1.320 | kept, pushed to dev |
| v44x | per-person sort on 32-bit ts | sessionize 1-core 1.153 | 1.112 (overlapping spread) | tie, reverted |
| v44y | hash-keyed campaign table (+ v44x) | clicks stage 1-core 0.769, total 5.375 | 0.764, 5.340 | tie, reverted |
| v44 | final person ids at resolve (no remap pass) + contiguous root names | stage3 0.202, sessionize 0.284, total 1.458 | 0.153, 0.310, 1.431 | tie, kept as simpler; pushed |
| v45 | sessionize largest-first + per-thread campaign accumulators (whale shard 407K vs 76K mean) | sessionize 0.320, best 1.355 (v44) / 1.383 (v43) | 0.289, best 1.340 / 1.323 | kept, pushed |
| v46x | largest-first resolve | stage3 0.148 | 0.159 | worse, reverted (no imbalance in uid shards) |
| v46y | 40-byte output rows (u32 counts) | sessionize 0.292, mean 1.451, best 1.300/1.350 | 0.275, 1.401, 1.342/1.333 | tie (mixed), reverted (main.v46row.rs) |
| v46z | stack: 40 B rows + click scanner + 32-bit sort key (14 interleaved runs, median) | median 1.390, mean 1.397 | 1.385, 1.380 (11/14 pairs) | real but ~1%: not kept (main.v46stack.rs) |
| v46 | allocator gathers freed blocks via mremap | median 1.438 / best 1.395 | 1.354 / 1.280 (9/12 pairs) | kept, pushed to dev |
| v47x | allocator splits oversized free blocks | median 1.348 | 1.381 | worse, reverted |
| M3 diag | per-stage fresh: pool maps 0 MB after parse; later fresh = untouched tails of reused blocks (~300 MB slack) + glibc small buffers; ~0.03 s total | — | — | memory reuse exhausted at v46 |
| v47 | 128-bit multiply id hash | parser 75.5 ns, median 1.418 (24 runs) | 65.9 ns, 1.380 (17/24 pairs) | kept, pushed to dev |
| v48x | event record 26 B | median 1.488 (24 runs) | 1.464, 13/24 pairs | tie, reverted |
| v48y | batched parser, event times grouped by format | parser 65-79 ns/line | 71-82 ns/line (0/5 pairs) | slower, reverted (main.v48batch.rs) |
| v48z | Parquet writer streams pages (no page buffers) | write stage 0.080 | 0.080 | tie, reverted (pq.v48.rs) |
| v48w | thread count sweep NCPU 4/5/6/8 (v47) | 4: 1.492 | 5: 1.516, 6: 1.543, 8: 1.549 | 4 stays (scorer 8 HT can't be tested here) |
| v48 | 24 B campaign accumulator + 2x links interner table | median 1.401 (24 runs) | 1.345 (20/24 pairs) | kept, pushed to dev |
| v49 | stack on v48: 26 B event record + 40 B rows + click scanner | median 1.475 (24 runs) | 1.421 (17/24 pairs) | kept, pushed to dev |
| v50x | u32 dedup table (8-bit tag) | dedup 0.0715, total 1.325 | 0.0750, 1.351 (5/12) | tie, reverted |
| v50y | click records with 4-byte chunk refs (UCl 36->28 B, CR 20->16 B) | median 1.390 (24 runs) | 1.394 (13/24) | tie, PARKED for a later stack (main.v50clk.rs, exact: gate + seeds 168/169) |
| v50 | stack: per-person sorts on u64 (ts<<32\|idx) keys gathered into the free input buffer (diag: sorting 14/20 B packed records was 1.0 of 2.2 Gcyc in the stage) + parked 4-byte click refs | median 1.418 (24 runs), sess lap 0.289 | 1.335 (18/24 pairs), sess 0.262 | kept, pushed to dev 6b47f08 (gate + seeds 170-174; Rust 1.90 build) |
| v51 | whole-shard 3-pass radix sort by ts + stable group by person (person count fused into the histogram pass); no per-person sorts (diag: sorts were 1.06 of 2.9 Gcyc in the stage) | sess lap 0.269, median 1.431 (24 runs) | 0.244, 1.386 (16/24) | kept, pushed to dev b89e1be (gate + seeds 175-177; Rust 1.90 build) |
| v52x | assisted counters in a separate u32 table (diag: 36.5M random RMWs/run, ~11 per session) | sess lap 0.249, median 1.353 (16 runs) | 0.238, 1.369 (6/16) | tie (stage −4%, total flat), reverted (main.v52as.rs, exact: gate + seed 178) |
| G1 | sliding 3-block mask window in the parser | 62-73 ns/line | 66-67 ns/line, stage 1 16 vs 12.5 | tie, reverted |
| G2a | month cache for event ts in F1 form (as ingest_ts) | 64-77 ns/line | 70-78 ns/line | tie, reverted (main.g2a_tsmc.rs, exact: gate + seed 179) |
| G4 | lazy assisted counting (flush per window exit / day change) | sess 0.233, median 1.340 (16) | 0.228, 1.293 (10/16) | near-tie, exact (gate + seed 180); KEPT IN STACK (main.g4lazy.rs), evaluate with next ideas |
| G8 | sessionize radix 2 passes on ts>>9 + per-person insertion fix | sess 0.236 (vs G4) | 0.225, total flat 10/16 | exact (gate + seed 181), in stack |
| v52 | stack G4 + G8 vs v51 | median 1.337 (24 runs), sess 0.234 | 1.298 (20/24), sess 0.225 | kept, checkpoint on dev (-2.9%, below the 5% bar for main) |
| G9 | regroup split: resolve 0.10 s, person partition 0.03 s. Flat open-addressing resolve table (retry of v27) | regroup 0.146, total 1.310 (16) | 0.191, 1.360 (2/16) | worse, reverted (main.g9rt.rs) |
| G10 | campaign lookup by number (array) when all ids are canonical c_<n> | clicks stage 0.165, total 1.288 (16) | 0.176, 1.304 (8/16) | tie, reverted (main.g10camp.rs, exact: gate + seed 183) |

Observation (Nov 21 ~14:00): micro-level CPU cuts (sliding masks, ts cache, campaign array, flat resolve table) all tie or lose;
byte cuts (P1 20 B keys, H2 26 B PEv) also tied alone but won when stacked. Remaining wins come from stacking exact near-ties.
Ideas next: G11 PR/PEv price+cat in one u32 when ncat < 255 (fallback: current layout via const generic) ; G12 dedup key 20 B (P1) restacked on v52 ;
G13 sessionize: skip the click radix entirely for persons with 0/1 clicks (most), sort clicks per person only when >1 ; G14 write stage overlap with sessionize.
| G12 | 20 B dedup key restacked on v52 | median (24 runs) | 12/24 pairs | tie, reverted (main.g12k20.rs, exact) |
| G13 | researcher #1: atomic bitset dedup filter (microbench, 20.8M ids, 4 threads) | key tail + dedup 0.145 s | insert 0.12 s (prefetched) + test 0.06 s, 8-13% candidates left | rejected: costs more than it replaces |
| G14 | researcher #4: shard count at 60M (SH_BITS 8 vs 10, 8 interleaved runs on p60m) | 4.524 s | 4.521 s (6/8) | tie: 256 shards are fine for the final |
| G15 | fork-and-report fast exit (parent exits when outputs are written; child tears down 2 GB) | native: written->exit gap 0.04-0.05 s (20M), 0.07-0.10 s (60M); docker run median 1.918 (20 rounds) | 1.914 (9/20) | tie under docker (container end waits for the child): reverted (main.g15fx.rs) |
Note: docker works here (start dockerd; base images from local shopping-sessions:latest, Docker Hub rate-limited). Docker-level timing: scratchpad/dimg.
| G16 | sessionize radix 3x8-bit (256 buckets) instead of 2x11-bit | sess 0.239, total 1.465 (16) | sess 0.248, total 1.357 (11/16) | stage lap worse (total = noise): reverted (main.g16r8.rs, exact: gate + seed 186) |
| diag | parse value costs (PARSESTAGE 7/8/9 knock-outs, 1 core): stage 3 48-59 ns; without event ts 35-36 (ts = 12-23 ns), without ingest 43 (5-15), without kind 47-52 (0-7) | — | — | event ts is the largest value cost (3 random formats -> mispredicts) |
| G17 | ingest_ts decoded without re-validation once its month is cached (rules: ingest_ts is always valid F1; any non-F1 shape / new month -> validating f1) | parse 1-core 65.5-70.0 ns; parse lap 0.555, total 1.316 (20) | 62.7-67.8 ns; 0.518, 1.329 (11/20) | stage lap -7%, total noise; exact (gate, SELFTEST, seeds 187-188): IN STACK (main.g17ing.rs) |
| G17b | G17 over 24 runs (wall) and 16 runs (cpu) vs v52 | wall 1.341 / cpu 5.104 | 1.335 (14/24) / 5.150 (8/16) | tie: reverted (rule shortcut not worth keeping). Note: wall ~ total CPU / 4; per-stage laps shift with the links thread, judge by totals |
| diag | page faults per stage (minflt via /proc): parse 28K (file mapping), clicks 4K, regroup 60K (!), sessionize 4K; regroup's per-shard hash tables/vectors fault in 4 KB pages (240 MB) | — | — | 97K faults/run |
| G18 | (a) resolve writes into two pre-sized buffers + per-thread reused hash tables; (b) sessionize per-thread reused scratch; (c) per-thread output row buffers | faults 97K; wall 1.453 / cpu 5.404 (24) | faults 35.5K; wall 1.403 / cpu 5.197 (wall 15/24, cpu 14/24) | KEPT as v53 (exact: gate + seeds 189-192) |
| v53 vs v51 | direct 24-run A/B | wall 1.439 / cpu 5.284 | 1.407 (13/24) / 5.275 (14/24) | -2.2% wall: below the 5% bar, main stays v51 |
| diag | huge-page faults per run (vmstat thp_fault_alloc): 948 x 2 MB = 1.9 GB zeroed = anon peak -> allocator reuse is complete; zeroing (6% of CPU) only shrinks with peak memory. Peak is at "events parsed" (payloads + per-chunk keys + partitioned keys); the fused-key fix was tried 3x (v23b, v34, v43y): tie/worse | — | — | — |
| G19 | Parquet pages pre-sized exactly (no regrowth) | write lap 0.0665 | 0.0705 (7/16) | tie, reverted |
| G20 | user_daily sessions/events as INT32 (rules: any integer width; counts < 2^31); output 93 -> 76 MB | write lap 0.0640, total 1.339 (16) | 0.0595 (10/16), total noise | exact (gate + seed 194): IN STACK |
| diag | links thread uncontended (NCPU=1): parse 0.045, hash 0.015, intern+union-find 0.133, rootv 0.012, roots 0.016, lks 0.04 = 0.26 s CPU | — | — | — |
| G21 | links: fast line scanner + 8-byte big-endian name prefixes for the union-find order (equal prefixes -> full compare) | links thread 0.272 s | 0.226-0.267 s | exact (gate + seeds 195-196) |
| stack | G20 + G21 vs v53, 24 runs | wall 1.372 / cpu 5.120 | 1.376 / 5.130 (14/24) | tie: parked (scratchpad/stack_g20_g21.patch), src back to v53 |
| 60M check | v53 vs v51 at 60M (10 interleaved pairs) | wall 4.170 / cpu 16.04 | 4.165 / 15.95 (6/10, 5/10) | tie at the final's size: v52/v53 gains do not show at 60M either. 60M stage walls: parse 1.65-1.97, key tail 0.23-0.28, dedup 0.25-0.29, clicks 0.49-0.54, regroup 0.38-0.45, sessionize 0.72-0.78, write 0.17-0.21; links thread 1.03 s (concurrent) |
| G22 | click parse in rayon::join with key partition + dedup (fill barrier tails) | wall 1.370 / cpu 5.086 (16) | 1.466 / 5.373 (5/16) | worse: clicks lose the freed-block reuse (fresh memory) — reverted |
| G23 | links as rayon::join with event parsing (no 5th OS thread) | wall 1.364 / cpu 5.100 (16) | 1.429 / 5.303 (5/16) | worse: parse runs a worker short; reverted |
| G24 | sessionize: one pass per session (day/row first, category totals inside the boundary loop) | sess 0.237, total 1.329 (16, abm) | 0.231, 1.324 (7/16) | 24-run abc vs v53: wall 1.334 -> 1.282 (20/24), cpu 4.923 -> 4.719 (22/24): KEPT as v54 (exact: gate + seeds 199-200) |
| v54 vs v51 | direct 24-run A/B at 20M | wall 1.444 / cpu 5.398 | 1.353 (18/24) / 5.025 (18/24) | -6.3% wall: MAIN fast-forwarded to 850b4d3 (Rust 1.90 build: gate + seeds 201-202 pass). 60M (8 pairs): 4.604 -> 4.569 (5/8), output identical to v51 |
| G25 | sessionize: drop the 2 radix passes; group by person then insertion sort (<=32) / library sort per person | sess 0.247 (12) | 0.327 | much worse (most events belong to large persons): reverted |
| G26 | resolve writes u32 pids; partition_map builds PR/CR from (UEv/UCl, pid) straight into person-shard order (no PR/CR intermediate) | regroup 0.141, total 1.292 (12); abc 24: wall 1.353 / cpu 5.077 | regroup 0.129 (9/12), 1.256 (8/12); abc 24: 1.385 (11/24) / 5.014 (13/24) | stage -8.5%, total tie: parked (scratchpad/g26_partition_map.patch, exact: gate + seeds 204-205) |
| v55 | stack on v54: G20 (INT32 counts) + G21 (links scanner + name prefixes) + G26 (fused resolve partition) | abc 24: wall 1.293 / cpu 4.749 | 1.259 (16/24) / 4.627 (19/24) | KEPT as v55 on dev (exact: gate + seed 206); <5% vs v54 so main stays v54 |
| G27 | build with -C target-feature=-prefer-256-bit (let LLVM auto-vectorize at 512 bits) | wall 1.417 / cpu 5.204 (16) | 1.406 (7/16) / 5.143 (9/16) | tie: build flags unchanged |
| G28 | sessionize radix 1 pass on ts>>20 (17-min buckets) + insertion fix | sess 0.246, total 1.357 (16) | 0.346, 1.528 (1/16) | much worse (fix-up cost explodes): reverted; 2 passes on ts>>9 is the sweet spot |
| G29 | clicks: equal-ts detection folded into the insertion fix-up (one pass less over clicks) | sess 0.245, total 1.434 (16) | 0.241, 1.393 (10/16) | near-tie, exact (gate + seeds 209-210): IN WORKING STACK (scratchpad/g29_eq.patch) |
| G30 | Parquet string columns read rows through a boxed closure (no Vec<Option<&[u8]>> copies) | write lap 0.0535 (12) | 0.0660 (3/12) | worse (per-row dyn calls): reverted |
| v56 | G29 alone vs v55, 24 runs | wall 1.378 / cpu 5.040 | 1.302 (16/24) / 4.811 (16/24) | KEPT as v56 on dev (exact: gate + seeds 209-210) |
| noise check | v56 vs v54 twice (24 each) and v55 vs v54 | set 1: 1.317 vs 1.366 (5/24); set 2: 1.356 vs 1.303 (15/24); v55: 1.365 vs 1.346 (16/24) | pooled v56 vs v54: wall 20/48, cpu 23/48 | MEASUREMENT LIMIT: two 24-run sets of the same pair differ by +-4% on this VM. Changes < ~5% are not resolvable; v55/v56 are ties with v54 (exact, kept on dev). main stays v54. From now on: confirm any candidate with two independent 24-run sets (or 48 pooled) before keeping. |
| 60M check | v56 vs v54 at 60M (10 pairs) | wall 4.196 / cpu 15.94 | 4.329 (4/10) / 16.24 (5/10) | no gain at the final size either: main stays v54 |
| diag | links at 60M (NCPU=1): 2.66M pairs, intern+union-find 0.52 s (195 ns/pair vs 140 at 20M: 128 MB table), whole thread 0.87 s CPU = ~5% of run CPU | — | — | sort-based interning would save ~2-3%: below what this VM can measure |
| diag | perf cycles/instructions not supported in this VM; task-clock of the SAME binary varies 10% run to run (5.10-5.59 s) | — | — | no lower-noise measure available |
| G31 | links interning by radix sort (4 x 16-bit digits) instead of the hash table | links thread (NCPU=1) 0.261 s at 20M, 0.885 at 60M | 0.340, 1.190 | slower (65536-way scatter has worse locality than the prefetched table); exact (gate, seeds 212-213, 60M output identical): reverted (main.g31ls.rs) |
| HW | lscpu: this VM = 4 cores, 1 thread/core, L1d 48 KB, L2 2 MB/core, L3 260 MB (Sapphire/Emerald Rapids class). Scorer = GCP n2 Cascade Lake+ (1-1.25 MB L2/core, much smaller L3), 8 vCPU = 4 cores x HT | — | — | cache-size tuning done here may not transfer. Board can score a given SHA: use it for scorer-specific tests |
| exp-sh10 | branch exp-sh10 = v54 with SH_BITS 10 (1024 shards: ~0.8 MB sessionize shards at 60M instead of 3.2 MB) | 60M local v54 4.298 | 4.358 (1/6) | output identical; locally a slight loss; OPTIONAL board test by SHA 7d7abd0 (user decides) |
| exp-sh10 @20M | v54 vs v54+SH_BITS 10, 16 runs | wall 1.417 / cpu 5.292 | 1.378 (10/16) / 5.117 (11/16) | tie/slightly better at 20M: a fair optional board test (SHA 7d7abd0) |
| robustness | fresh 60M dataset (generate seed 7781, 5.2 GB): v54, v56, v51, exp-sh10 | — | all exit 0; outputs identical (compare.py MATCH x3); warm wall 3.9-4.9 s each (noise); peak RSS 11.3 GB incl. the 5.2 GB file mapping (anon ~6 GB) vs 24 GB container | final-size safety confirmed for main v54 |
| diag | parse tail balance (20M warm): 4 threads finish within ~20 ms of each other (~1% idle) | — | — | not a lever |
| diag | parser line profile (PARSESTAGE=4, 62.5 ns/line): flat — comma extraction + u128 helpers ~12%, event ts ~8.5%, field bounds/quotes ~5%, two id hashes ~5.5%, nothing > 6% alone | — | — | — |
| G32 | comma positions via BMI2 pdep per k (parallel) instead of the clear-lowest-bit chain on u128 | 62.6-65.4 ns/line | 67.3-75.0 | slower: reverted |
| G33 | AES-NI id hash (aesenc rounds) instead of the 128-bit multiply chain, everywhere | 63.6-68.3 ns/line | 68.2-73.0 | not faster: reverted (exact: SELFTEST, gate, seeds 215-216) |

USER DIRECTION (Nov 21 ~16:00 UTC): stop creating new designs/experiments; keep refining the current best code (v54 = main; dev v56 = v54 + exact ties) with small incremental edits, keep only confirmed improvements, fewer tests.
| R1 | refine best: parser PF_DIST 1024/2048/4096/8192 (PARSESTAGE, 3 rounds) | 2048: 64-76 ns | 1024: 66-68, 4096: 63-72, 8192: 63-68 | flat: unchanged |
| R2 | refine best: CHUNK 32 -> 16 MB (24 runs) | wall 1.394 / cpu 5.168 | 1.412 (10/24) / 5.332 (9/24) | not better: unchanged |
| R3 | refine best: write-combining burst 384 -> 512 B (24 runs) | wall 1.284 / cpu 4.738 | 1.333 (7/24) / 4.902 (7/24) | worse: unchanged |
| R4 | refine best: write-combining burst 384 -> 256 B (24 runs) | wall 1.371 / cpu 5.103 | 1.400 (10/24) / 5.169 (10/24) | not better: 384 B stays (both neighbours lose) |
| R5 | refine best: links interner prefetch AHEAD 12 -> 24 / 48 (links thread, NCPU=1, 3 runs each at 20M and 60M) | 12: 0.263-0.267 / 0.70-0.88 s | 24: 0.235-0.284 / 0.70-0.78; 48: 0.209-0.295 / 0.76-0.88 | within noise: unchanged |
| R6 | refine best: dedup table capacity (n + n/4) -> 2n, next power of two | dedup lap 0.0715, total 1.303 (16); 24-run sets: wall 1.288 / 1.312 | 0.0535 (16/16), 1.220; sets: 1.236 (19/24) / 1.216 (21/24), cpu 19/24, 22/24 | CONFIRMED in two sets: KEPT as v57 (exact: gate + seed 217) |
| v57 vs v54 | direct, two 24-run sets | wall 1.311 / 1.282 | 1.341 (7/24) / 1.242 (20/24) | inconsistent: main stays v54 (dedup stage gain itself is solid, 16/16) |
| R7 | refine best: links interner table 2x -> 4x pairs (links thread NCPU=1, 4 runs each) | 20M 0.213-0.248, 60M 0.611-0.758 | 0.193-0.248, 0.635-0.883 | within noise: unchanged |
| R8 | refine best: dedup table 2x -> 4x | dedup lap 0.0535, total 1.212 (16) | 0.0570 (1/16), 1.270 | worse (table outgrows L2): 2x is the sweet spot |
| R9 | refine best: resolve map reserve lkm*2+1024 -> lkm*4+8192 | regroup 0.1240, total 1.198 (16) | 0.1805 (0/16), 1.210 | much worse (bigger reused map costs more to clear/rehash): try smaller next |
| R10 | refine best: resolve map reserve lkm*2+1024 -> lkm+256 | regroup 0.1705 (16); totals 1.282 / 1.256 | regroup 0.1525 (12/16); totals 1.359 (5/24) / 1.327 (5/24) | stage lap better but both totals lose: rejected; current reserve sits between two worse settings |
| R11 | refine best: sessionize campaign table day-major instead of campaign-major | sess 0.225, total 1.290 (16); sets 1.296 / 1.247 | 0.222, 1.221 (13/16); sets 1.289 (10/24) / 1.277 (6/24) | not confirmed: reverted (exact: gate + seed 219) |
| R12 | refine best: sessionize radix key ts>>9 -> ts>>10 | sess 0.225, total 1.302 (16) | 0.230, 1.353 (6/16) | worse: unchanged. ProdTab (already 2x, purchases only), click Vec capacity, BufWriter size: not on a hot path, skipped |
| R13 | refine best: prefetch the dedup slot of key i+8 | dedup 0.0505, total 1.196 (16) | 0.0525 (5/16), 1.215 | not better (table L2-resident here): reverted |
| R14 | refine best: remaining local candidates reviewed — same-user reuse in resolve (rows are shuffled: consecutive same-user records are rare), skip click-window loops (already exit on the first compare) | — | — | no measurable refinement left on this VM; loop to 30-min heartbeat |
| REDESIGN | direct resolve (user approved "go"): global lock-free user->person table (linked pre-filled, unlinked claimed by CAS), one pass from per-chunk records straight into person shards, no user shuffle; old path kept as exact fallback | old: user shuffle 0.05 + resolve/partition 0.13 = ~0.18 s | direct 0.35-0.37 s (prefill 0.075, event lookups 0.18-0.21, clicks 0.035, names 0.01-0.05, partitions 0.09-0.12); exact (gate, seeds 220-221, 20M output = fallback path) | ~2x slower: random lookups into one 58 MB table cannot match per-shard L2-resident tables; reverted (scratchpad/redesign_direct_resolve.patch). Distinct users: 1.48M at 20M, 4.45M at 60M (~17 records/user) |
| R15 | refine best: click chunk 3.5 MB -> 1.75 / 7 MB | clicks lap 0.1535 (12) | 1.75 MB: 0.1555 (5/12); 7 MB: CRASHES on 20M (click refs need chunks < 4 MB, assert) | unchanged; note: 3.5 MB is near the 4 MB click-ref limit by design |
| R16 | refine best: identity (rotated) hasher for the resolve map instead of FxHash | regroup 0.1235, total 1.175 (16) | 0.1780 (0/16), 1.221 | much worse: reverted (exact: gate + seed 222) |
| R17 | parse: skip a lane? no — raw-pointer pushes rejected on analysis (a key-emitting line can be 8 bytes, so the n/64 pre-size is not a bound; push's capacity branch is predictable) | — | — | not attempted |
| noise | same binary, 16 runs: plain cv 3.8%, nice -n -20 cv 6.8% (worse) | — | — | set-to-set drift comes from VM neighbours; interleaved pair counts stay the measure |
| R18 | G17 (ingest_ts decode without re-validation once the month is cached) re-tested on v57, two 24-run sets | 1.277 / 1.227 | 1.238 (18/24) / 1.234 (9/24); pooled 27/48 | tie: reverted (exact: gate + seed 223) |
| R19 | refine best: URow 40 -> 32 B (person id instead of name slice; name looked up at write time) | sess+write lap 0.2650, total 1.264 (16) | 0.2770 (4/16), 1.217 | lap worse (random name lookups at write time): reverted (exact: gate + seed 224) |
| R20 | refine best: dedup table zero only the used slots (track list) instead of a full memset per shard | dedup 0.0535, total 1.216 (16) | 0.0750 (0/16), 1.250 | worse (scattered zeroing + tracking > sequential memset): reverted (exact: gate + seed 225) |
| 60M | v57 vs v54 at 60M (12 interleaved pairs) | wall 4.212 / cpu 16.17 | 4.079 (7/12) / 15.45 (8/12) | leans better (-3% wall, -4% cpu) but not decisive; main stays v54. R21 (links lks lists by counting pass) skipped: ~0.04 s on a concurrent thread, unmeasurable |
| 60M #2 | v57 vs v54 at 60M, second 12-pair set | wall 4.306 / cpu 16.46 | 4.234 (7/12) / 16.05 (8/12) | pooled 14/24 wall, 16/24 cpu: consistent ~2-3% gain, below the 5% bar; v57 is exact and never worse at 60M (low-risk to promote if the user wants) |
| R22 | remaining micro-candidates reviewed: winner-bitmap word test in the user shuffle (1-2 cycles on a memory-bound copy), PRODTAB src check (purchases only, 3%), finish() category Vec (few entries per row) | — | — | each < 0.1%: not run; loop back to 30-min heartbeat |
| R23 | parse: lazy bounds for product/price fields — rejected on analysis (product_id is required on every event; only price is purchase-only: 1 of 7 fields) | — | — | not run |
| R24 | parse: hoist ws/1000 out of the per-line ingest conversion | 66.4 ns/line median (5 rounds) | 65.9 (3/5) | tie (compiler already hoists it): reverted |
PARSER STATUS: ~62-66 ns/line single-thread, flat profile (no item > 6%). Tried on the parser: sliding mask window, pdep comma positions, AES hash, event-ts month cache, ingest no-revalidation, raw-pointer pushes, lazy fields, ws hoist, PF_DIST 1k-8k, CHUNK 16 MB, 512-bit autovectorization: all ties or losses.
| R25 | parse: test the F1 timestamp shape first (58% of lines), F3/F2 after | 63.5-67.3 ns/line (6 rounds) | 63.3-67.0 (3/6) | tie: reverted (exact: gate, SELFTEST) |
| R26 | parse: branch-free event_kind (one masked 8-byte load, cmov select) | 62.6-66.5 ns/line (6 rounds) | 63.1-68.2 (1/6) | slower: reverted (exact: gate + seed 226) |
| R27 | parse: drop the legacy per-line uid len/ptr assert (uoff is chunk-relative, chunk < 2^31 asserted) | 62.4-69.1 ns/line (6 rounds) | 62.6-66.9 (2/6) | tie (predictable branch): reverted (exact: gate + seed 227) |
| R28 | parse: hash the event id right after extracting it (multiply chain overlaps ts parsing) | parse 64.3-65.5 ns median; totals 1.309 / 1.362 / 1.268 | parse 63.0-63.2 (10/14 rounds); totals 1.271 (19/24) / 1.357 (12/24) / 1.282 (4/24) | parse -2% but total tie (35/72): reverted (exact: gate + seed 228) |
| R29 | parse: drop per-line kcount/ucount (count in the partitions) — rejected on analysis (an extra counting pass over 20M keys costs more than L1-resident increments); DKey/PEv store order — rejected (plain stores, no traffic change) | — | — | parser candidates exhausted; loop to 30-min heartbeat |
| R30 | parse: software-pipelined parser (stage 1 structure of line i+1 issued before stage 2 of line i; parse_event_chunk6) | 64.8 ns/line median (8 rounds, same binary) | 67.0 (2/8) | slower: the parser is branch-mispredict bound (random ts formats/quotes), mispredicts flush the overlapped work; descriptor copies add cost. Exact (SELFTEST chunk6 == chunk5 on 90 variants, gate, seeds 229-230). Reverted (scratchpad/pipelined_parser.patch). Manual 2-line interleave ruled out for the same reason |
| R31 / v58 | PGO: profile from an instrumented rustc 1.90 build of v57 run on p20m x2 + a fresh 2M seed; pgo/merged.profdata committed, Dockerfile RUSTFLAGS adds -Cprofile-use | plain 1.90 v57: parse 64.9 ns/line; wall 1.238 / 1.252 (24-run sets) | PGO: 62.8 ns/line (5/6); wall 1.184 (21/24) / 1.187 (18/24), cpu 20/24, 18/24 | CONFIRMED -4.4 / -5.2%: KEPT as v58. Docker image builds from the repo (rust:1.90-slim), dev answer MATCH, 20M output == v57. Regenerate the profile with notes/pgo.sh after any source change |
| MAIN | main fast-forwarded to v58 (b8cf964) on the user's "just do it if it improves" | v54 (1.90 build) | 20M: 1.215 (14/24) / 1.187 (22/24), pooled wall 36/48, cpu 38/48; 60M: 4.245 -> 4.162 (7/12 wall, 9/12 cpu), output identical | gate + seeds 232-233 on the 1.90 PGO build; Docker image builds from the repo and matches the dev answer |
| R32 | PGO profile from broader training (p20m + p60m + 3 fresh 2M seeds) | v58 profile: wall 1.220 / 1.205 | broad: 1.236 (5/24) / 1.251 (6/24) | worse (60M run dominates the counts?): v58 profile kept |
| R33 / v59 | non-PIE build: -C relocation-model=static in RUSTFLAGS (with PGO) | v58: wall 1.246 / 1.236 (24-run sets); 60M 4.229 | 1.204 (19/24) / 1.204 (16/24), cpu 18/24, 16/24; 60M 4.241 (3/10) | 20M CONFIRMED (-3.4 / -2.6%%): v59 on dev; 60M tie-to-slightly-worse: main stays v58 pending a second 60M set. Exact: gate + seed 234; Docker image builds, dev answer MATCH |
| v59 @60M #2 | second 60M set vs v58 | 4.228 / cpu 15.88 | 4.260 (4/10) / 16.07 (3/10) | 60M pooled 7/20: slightly worse at the final size -> main stays v58 (v59 only on dev) |
| R34 | PGO + -C llvm-args=-enable-ext-tsp-block-placement=1 | v58: wall 1.234 / 1.232 | 1.285 (5/24) / 1.264 (4/24) | worse (~3-4%): not adopted |
| R35 | PGO profile weighted toward 60M (current p20m profile x3 + one p60m run x1) | v58: 60M 4.337 / 4.245; 20M 1.269 | 60M 4.153 (7/10) / 4.215 (7/12, cpu 4/12); 20M 1.317 (4/24) | 60M pooled ~tie (wall 14/22, cpu 11/22), 20M worse: not adopted |
| R36 | -C target-cpu=skylake-avx512 instead of cascadelake (same ISA; PGO) | v58: 20M 1.274 / 1.286; 60M 4.240 | 1.214 (15/24) / 1.266 (16/24); 60M 4.373 (3/10) | 20M borderline (31/48), 60M worse: not adopted |
| R37 | NCPU 4/5/6 at 60M — skipped (the scorer forces NCPU=8 on 4 cores x HT; this VM has 4 plain vCPUs, so the result would not transfer); radix histogram reuse — skipped (512 x 16 KB allocations, unmeasurable) | — | — | build and source levers exhausted; loop to 30-min heartbeat |

## LANG — should we leave Rust? (measured, no)
- Rust fixed cost: v58 on 48 KB input 6.4 ms vs empty C main 1.1 ms -> ~5 ms of 1190.
- Kernel A (naive byte loops: scan to user_id, FNV hash, open-addr count, parse last field), 20.8M lines / 1.6 GB, 1 thread, 3 runs each, identical outputs:
  gcc C 2.62-2.93 s | Go 2.90-3.00 | Rust 3.64-3.84 | clang C 3.66-3.94 | Zig 0.17 3.59-3.97 | OCaml 4.14 4.76-5.00.
  LLVM (Rust=clang=Zig) loses ~28% to gcc only on naive per-byte loops (extra induction vars in scan loops); clang -fno-vectorize/-fno-unroll/-O2 don't close it.
- Kernel B (our style: AVX-512 64B separator masks + tzcnt walk + crc hash): gcc 1.65-1.88 s vs clang 1.76-1.89 -> tie.
- Conclusion: compiler/language is not the gap; our hot code is mask/intrinsic based. MoonBit not installed (no SIMD intrinsics; would be slower). Hand asm: no expected win over intrinsics.
- Kernel C (sessionize-like: 4-pass 14-bit LSD radix of 20M (uid,ts) u64 + gap scan) and Kernel D (resolve-like: 20M open-addressing lookups, 1.5M users), 1 thread, 3 runs, identical checksums:
  sort+scan ms: gcc 834-919 | clang 893-979 | Rust 859-953 | Go 862-929 -> tie.
  hash ms:      gcc 275-293 | clang 273-286 | Rust 288-376 (first run outlier, then 305/288) | Go 276-299 -> tie.
  Both memory-bound; language irrelevant.

## Nov 21 evening plan (BUDGET.md): scorer timer + big changes
- TIMER (ws/stimer.py) calibrated on board v26 1.49 / v45 1.22 / v51 1.19 at 20M, 6 interleaved rounds:
  warm NCPU4 +25.0% / -7.6%; warm NCPU8 +25.6% / -11.1%; cold NCPU4 +33.8% / -7.1%; cold NCPU8 +53.4% / -1.4% (board +22.1% / -2.5%).
  Cold cache overstates disk-heavy versions: the scorer reads from page cache. Judge = warm NCPU8 at 60M (board ~0.87 x local warm); guard = cold NCPU8 not worse.
- Parser branch-order upper bound: p20m lines pre-sorted by shape (quotes, ts form, type) -> parse lap 0.443 -> 0.431 (-3% of parse), output MATCH. Not a lever.
- Links thread: 0.78 s alone (NCPU=1), looks 1.6-1.9 s at NCPU8 only from sharing cores; ~5% of CPU, off the critical path. Not a lever.
- BIG2 sessionize by person counting + per-person sort (insertion <=32, pdq <=4096, radix above) instead of shard radix_22 + person scatter:
  exact (gate PASS) but sessionize lap 0.707-0.787 -> 0.807-1.135 at 60M (6 pairs, all worse). Random small sorts are branchy; reverted.
- BIG1 parse into per-thread scratch + per-chunk WC grouping by shard (keys by event shard, payloads by user shard), dedup/resolve/person-partition over (chunk, shard) slices: removes the key partition and the user shuffle. Exact (gate PASS; patch in scratchpad g2_parse_grouped.patch).
  Per-stage CPU at 60M (4 interleaved runs, NCPU8): base parse 6.55 + key part 1.00 + dedup 0.97 + u+clicks 1.88 + resolve/pm 1.76 = 12.16 core-s;
  g2 parse+group 8.55 + dedup 1.34 + u+clicks 1.28 + resolve/pm 2.05 = 13.22. Total 15.67 -> 16.69 core-s; THP faults 3035 -> 2124 (sys -0.5) but user +1.5.
  Grouping both streams per chunk costs 2.0 core-s vs 1.0 + ~0.6 for the two global partitions; 155 small slices per shard slow dedup/resolve. Wall: stimer warm 12 rounds +4.5% (3/12 faster). Rejected.
- New tool: lap lines can carry process CPU (scratchpad cpulap.py patch) + ws/cpulaps.py for per-stage CPU medians.
- Sessionize split (rdtsc, 60M): event radix+person scatter 39%, click sort 10%, per-person scan 50%; ~5 ns per record per scatter pass: at the scatter floor.
- Parser stages (PARSESTAGE, 1 thread, 20M): structure 12.4 ns/line, +field bounds/quotes 12.3, +values (ts/ing/type/price/product) 26.4, +hash+write 10.7 = 61.8.
- C3 campaign lookup through the 96-bit ProdTab table instead of the byte-keyed FxHashMap: exact (gate PASS), u+clicks CPU 1.88 -> 1.88 core-s: no effect, reverted.

## Nov 22 ~03:00 UTC: v60 two-way month caches; v5 data (scorer rescored onto v5 IDs)
- Found: event ts costs 13.5 ns/line of 62 (PARSESTAGE nots variant); 7 ns with lines shape-sorted, so ~6.5 ns is form-dispatch mispredicts.
  Branch-free 3-form ts parser: exact (SELFTEST 14M) but 65.8 -> 73 ns/line; F3-branch + merged F1/F2: tie. Both dropped.
- Found: both month caches (MonthCache for ingest/click ts, thread-local MONTH for event ts) held ONE month; a 14-day window
  spanning two months (p60m Sep 20-Oct 4; ~45% of seeds) thrashes them on random-order lines.
- v60 = two-way caches (branch-free pick of the matching entry). SELFTEST 14,013,728 timestamps identical (new fuzz with random
  months on one shared cache); gate PASS; oracle seeds 239, 240, 9202 (month-spanning) MATCH; v5 generator: dev answer MATCH, seeds 501-503 MATCH.
  PARSESTAGE 1 thread: month-spanning 20M (seed 9202) 1.62-1.63 -> 1.44-1.49 s (-10%); p20m tie.
  stimer warm NCPU8 12 rounds vs v59: p60m (spanning) -2.4% (10/12 faster, 6/12 >3%); x20 (spanning) -1.9% (7/12);
  p20m -0.5% (5/12); v5 n20 (single month) +1.9% (7/12 faster); v5 n60 (single month) -0.0% (4/12). Win where it applies, tie elsewhere.
- PGO profile retrained on v5 data: n60 +1.5% vs v60 (0/12 >3%): not adopted (v60 keeps the profile made from its source on old-format data).
- v5 data shape: lines avg 82 B (was 77), 0.49% over the 128 B fast path; v59 and v60 exact on it.

## Nov 22 ~03:40 UTC: board #19 (0.9663; leader 0.5987); stage polish easiest -> hardest (v60, v5 20M: parse 0.533 s / sess 0.228 / u+clicks 0.183 / resolve+pm 0.133 / key part 0.076 / dedup 0.070 / write 0.055)
- write: exact-size Parquet pages -> 0.051 vs 0.053 s, no effect, reverted.
- dedup: prefetch the slot of the key 8 ahead (table > L2 at 60M): dedup 0.234 -> 0.219 s, cpu 0.92 -> 0.86 (n60, 4 runs). kept.
- resolve: FlatTab (open addressing on the uniform uid h64, multiply slot, 0 = empty -> side table) instead of FxHashMap:
  resolve+pm 0.488 -> 0.472 s, cpu 1.75 -> 1.70. kept.
- clicks: 13-digit ts through parse_ts_over (SWAR), uid hash via h12_over when 8 chunk bytes follow: click parse cpu 1.08-1.31 -> 1.02-1.09. kept.
- v61 = v60 + those three, PGO regenerated. Gate PASS, seeds 241 242 (old gen) + 504 505 (v5 gen) MATCH, v5 dev answer MATCH.
  stimer 12 rounds: v5 n60 +0.5% (5/12), v5 n20 +0.6% (4/12); cpuab 8 rounds n60: cpu 15.19 -> 14.93 core-s (-1.7%), wall -2.0% (4/8). Small, real in CPU, inside wall noise.

## Nov 22 ~04:20 UTC: different designs (user: "try a different design")
- DIRECT: parser writes keys/payloads straight into shard-grouped arenas (per-thread BLK=2048 blocks per shard,
  64 MB segments on demand), dedup by arena index, resolve/person partition over block lists: no key/user shuffle.
  Exact (gate, seeds 243-245 + 506-508, v5 dev). Direct stores: total cpu 15.31 -> 16.84 core-s (parse +2.0);
  with per-shard WC staging: 15.77 -> 15.90 (parse +1.2, partitions -1.5, resolve/pm +0.4). Tie: the scatter cost is in the
  staging, not the extra pass. Reverted (scratchpad direct_arena.patch).
- Shard count: SH_BITS 7 +5.7% (0/8), 9 -0.4% (6/8) at v5 n60: 256 stays.
- v62 SIMD batched timestamps: lines wait in batches of 8; ts8 (AVX-512 F/BW/DQ, no VBMI) parses 8 ts (F1/F2/13-digit F3, all
  computed and selected by mask) and 8 ingest_ts (F1-only variant) at once, fields gathered straight from the chunk
  (copy path within 32 B of a chunk end); other forms -> scalar parse_ts. Magic divisions verified exhaustively.
  SELFTEST: ts8 decided 8,321,249 fuzz lanes identically (both variants); found+fixed a month-17 alias in the table lookup.
  Gate PASS, seeds 246-248, 9202, 509-511 MATCH, v5 dev MATCH, full 20M outputs identical to v61 (p20m, n20).
  PARSESTAGE 1 thread p20m: 64.3 -> 58.6 ns/line (-8.9%, 6/6). stimer 12 rounds: v5 n60 -3.8% (8/12, 6/12 >3%), v5 n20 -0.8% (7/12);
  cpuab 8 rounds n60: cpu 15.34 -> 15.11 (-1.5%), wall -0.5% (5/8).
- v62 clean build: docker --network=none --no-cache of 18f8a5a OK, container output MATCH v5 dev answer.
- Hash upper bound (h12_over -> trivial values in the batch tail): p20m 59.8 -> 53.0, n20 65.8 -> 55.8 ns/line, but flawed
  (trivial hashes put every record in one shard). wyhash-style 16 B per multiply + parallel finish multiplies: exact (all
  20M outputs identical, gate, seeds 249 512 513) but p20m 58.7 -> 60.3, n20 66.2 -> 65.6: no gain (OoO already hides the chain). Reverted.
