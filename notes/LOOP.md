# Shopping Sessions self-improvement loop

Goal: board time ≈ 0.5 s (local best ≈ 0.5 s at 20M; board ≈ 0.9–1.0 × local). Current #1 on the board: 0.65 s.
Deadline: Sun Nov 22, 16:00 UTC (11:00 ET). Final ranking = 60M new events, so memory footprint matters.

## State (update every iteration)
- Workspace (from Nov 21 13:00 UTC): edit /home/user/shopping-sessions/src only; never write in koillea. Gate: scratchpad/ws/check.sh [BIN]; difftest: ws/difftest.sh N SEED -- BIN; A/B: ws/abm.sh A B N OUT. Notes live here (notes/ on dev).
- Current best: v52 (v51 + lazy assists + 2-pass radix; dev). Before: v51 (v50 + whole-shard radix sort by ts, no per-person sorts; main.v51rx2.rs, binary s_v51rx2; dev b89e1be). Before: v50 (v49 + per-person u64 key sorts + 4-byte click refs; main.v50st.rs, binary s_v50st; dev 6b47f08). Before: v49 (v48 + 26 B event record + 40 B rows + click scanner; main.v49stack.rs, binary s_v49stack). Before: v48 (v47 + 24 B campaign accumulator + 2x interner table; main.v48s23.rs, binary s_v48s23). Before: v47 (v46 + 128-bit multiply id hash; main.v47hash.rs + recycle.v46remap.rs, binary s_v47hash). Before: v46 (v45 + mremap gathering in the allocator; recycle.v46remap.rs, main.v45acc.rs, binary s_v46remap). Before: v45 (v44 + largest-first sessionize, per-thread accumulators) — source `…/scratchpad/main.v45acc.rs`, binary `…/scratchpad/s_v45acc`. Before: v44 (v43 + final person ids at resolve, no remap) — source `…/scratchpad/main.v44b.rs`, binary `…/scratchpad/s_v44b`. Before: v43 (v42 + PR 14 B) — source `…/scratchpad/main.v43pr14.rs`, binary `…/scratchpad/s_v43pr14`. Before: v42 (v39 + packed UCl/CR + hash product table) — source `…/scratchpad/main.v42stack.rs`, binary `…/scratchpad/s_v42stack`. Before: v39 (v37 + PEv packed to 30 B) — source `…/scratchpad/main.v39pev30.rs`, binary `…/scratchpad/s_v39pev30`. Before: v37 (v36 + prefetched links interner) — source `…/scratchpad/main.v37links.rs`, binary `…/scratchpad/s_v37links`. Judge parse changes on the PARSE LAP (mean of 6 interleaved runs), not only total. Before: v36 (v35 + pext field bounds; PARSESTAGE=1..4 staged parser bench) — source `…/scratchpad/main.v36pext.rs`, binary `…/scratchpad/s_v36`. USER FOCUS: PARSER SPEED ONLY. Before: v35 (v30 + lazy input mapping) — source `…/scratchpad/main.v35lazy.rs`, binary `…/scratchpad/s_v35`, on `dev`. Warm = v30; cold −0.5 s. ALSO TIME COLD RUNS (python3 -I scratchpad/evict.py data/p20m before a run). Before: v30 Local ~1.30–1.32 s. Before: v29 (SH_BITS=8). Previous: v26 — source `/tmp/claude-0/-home-user/7093f2af-7756-5848-9566-55372d396f64/scratchpad/main.v26bench.rs` (= v26 + PARSE5BENCH=1 single-thread parser benchmark; same pipeline),
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
