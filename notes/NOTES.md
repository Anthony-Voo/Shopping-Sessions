# Shopping Sessions — submission log and calibration

Board = practice scorer (~20M events, median of 3). Final = ~60M new events, 3 runs (top 10 re-timed 7x on an idle machine).

## Submissions (made by the user on the event site)
| # | When (UTC) | Version | Local best (20M) | Board |
|---|---|---|---|---|
| 1 | Nov 20 | early pipeline | — | 2.97 s (#29 at the time) |
| 2 | Nov 21 ~01:00 | Anthony-Voo/Shopping-Sessions `main` 039a9a2 | 1.984 s (re-measured Nov 21 05:45 interleaved with v24) | 1.79 s (#55) |
| 3 | Nov 21 (pushed ~06:30 UTC; user submits) | `main` be891fc (v26) | 1.45–1.54 s; Rust 1.90 build (as Dockerfile) passes gate + seed 107 | **1.49 s** (below top 50 of 155; #50 was 1.27 s) |
| 4 | Nov 21 (pushed to main; user submits) | `main` 32426ed (v30: 256 shards, 32 MB chunks, 384 B bursts) | 1.396 s vs v26 1.589 s in the same A/B (Rust 1.90 build; gate + seed 112 pass) | pending (predicted ~1.3 s) |
| 5 | Nov 21 ~10:35 UTC (pushed to main; user submits) | `main` a23aa9b (v45) | ~1.29-1.34 s (vs v26 ~1.60 s); 60M 4.84-5.25 s; Rust 1.90 build passes gate + seed 147 | **1.22 s** (user report) |
| 6 | Nov 21 ~11:10 UTC (pushed to main; user submits) | `main` 378d2a2 (v47: v45 + mremap gathering + 128-bit id hash) | ~9-10% faster than v45 locally; fresh Rust 1.90 build passes gate + seed 157 | pending (predicted ~1.10-1.12 s) |
| 7 | Nov 21 ~12:40 UTC (pushed to main; user submits) | `main` b89e1be (v51: v49 + u64/radix per-shard time sort + 4-byte click refs; sessionize 0.29→0.24 s) | 24-run A/B chain: v49 1.418 → v50 1.335 → v51 1.386 vs v50 1.431 (≈ −9% over v49, ≈ −22% over v45); Rust 1.90 build passes gate + seeds 170-178 | **1.19 s** (user report; predicted 0.95-1.0 — gain mostly did not transfer) |

## Calibration
board ≈ 0.90 × local best (submission 2: 1.79 / 1.984), but submission 3 gave 1.49 / ~1.48 ≈ 1.0.
Treat board as 0.9–1.0 × local (submission 5: 1.22 / ~1.31 ≈ 0.93); practice runs vary ~20% across scoring machines (organizer note).

## Candidates (not submitted)
| Version | Where | Local best | Predicted board |
|---|---|---|---|
| v22 per-line parser | dev cf7953c | ~1.46–1.55 | ~1.35 |
| v24 + parser prefetch | local (scratchpad main.v24pf.rs) | 1.478 (A/B vs main 1.984) | ~1.33 |

## Final-round reminder (Nov 21 10:10 UTC)
The final rebuilds each team's chosen commit (best-scoring submission by default) on ~60M new events.
Best board submission today = v26 (1.49 s). Unless a better one is submitted before Sun Nov 22 11:00 ET,
the final runs v26. At 60M locally: v26 5.85-7.72 s (one 118 s run under memory pressure), v45 4.84-5.25 s,
identical output. User chose "keep improving, submit later" (asked 10:10 UTC).

## Submission 7 post-mortem (Nov 21 ~13:00 UTC)
v51 board 1.19 vs v45 1.22 (−2.5%) though local A/B said ≈ −20%. Re-check here: at NCPU=4 warm, v45 ~1.75 vs v51 ~1.49 (−15%);
at NCPU=8 (scorer's setting, oversubscribed on this 4-vCPU VM) gaps shrink to ~0-15%; cold page cache: v45 ~2.0 vs v51 ~1.78.
Most local wins were CPU-stage wins on 4 dedicated threads; the scorer is 8 hyperthreads (4 cores) and likely bound elsewhere
(I/O or memory bandwidth). Local A/B is not a reliable predictor of the board any more.

## Nov 21 ~14:45 UTC: main fast-forwarded to 850b4d3 (v54)
v54 vs v51 (24 interleaved runs, 20M): wall 1.444 -> 1.353 s (-6.3%), CPU -6.9%; at 60M a tie (4.60 -> 4.57). Not yet submitted; predicted board ~1.10-1.15 s (v51 scored 1.19).

## Optional board experiment (prepared Nov 21 ~15:35 UTC)
Branch exp-sh10, SHA 7d7abd0 = v54 with 1024 shards. Same output as v54. This VM has 2 MB L2 / 260 MB L3, the scorer ~1 MB L2 and a small L3, so smaller shards may help there (locally a slight loss at 60M). Submit by SHA only if a spare submission is available; compare with v54 (main 850b4d3).

## Nov 21 ~22:20 UTC: main fast-forwarded to b8cf964 (v58 = v57 + PGO)
vs v54 (both rustc 1.90): 20M pooled 36/48 wall, 38/48 cpu faster (sets -0.4% / -4.9%); 60M -2% wall, -3% cpu; output identical. Docker image (rust:1.90-slim + pgo/merged.profdata) builds and matches the dev answer. Not yet submitted.

## Nov 22 ~00:20 UTC: FREEZE = 5b5058b022d0905aa151228e6f8b8d7c6f97cbf0 (dev; code = v59 b05f61a)
- No big change won (BUDGET.md / LOOP.md): parse-grouped fusion +1.0 core-s, person-sort sessionize slower, campaign table no effect; parser branch-order upper bound only -3% of parse.
- Refreshed PGO profile tested and NOT adopted (60M +2.0%, 3/12; 20M +3.0%, 6/12): the committed v59 profile stays.
- v59 vs v58 on the calibrated timer (warm, NCPU8, 60M, 12 rounds): +0.9%, 6/12 -> tie; v59 was -3% at 20M.
- Clean build: git archive of 5b5058b, docker build --network=none --no-cache OK; container output MATCH dev answer and MATCH native v59 at 20M.
- main still = v58 b8cf964 (untouched). Submit by the full hash above. Not submitted (submissions are made by the user).

## Board after the scorer fix (scorer version shopping-sessions-5), read Nov 22 02:34 UTC 
Goonies #5 at 0.9663 s (user report + board). Leader "today is yesterday's tomorrow" 0.7257, #2 Fraun 0.8661, #3 hillclimbers 0.9224, #4 Jitesh Rao 0.9619, #6 electrum 0.9688, #7 BingBU 0.9730.
Assumed to be the frozen commit 5b5058b (v59). Local warm NCPU8 20M median 1.223 s -> board/local ~0.79 (was ~0.87 before the fix).

## Nov 22 ~03:20 UTC: candidate v60 = bf91689482893c7f9d84fbd7fa7428e46f27d77d (dev)
- Organizer change (Sat 9:08 PM ET): v5 IDs (per-seed keyed bijection, 1 in 8 ids with 32-char hex suffix); best entries rescored onto v5;
  finals = top 30, 3 fresh 60M datasets, 9 interleaved runs. New rule: entries must be the team's own work (ours is).
- v59 and v60 both exact on v5 data: v5 dev answer MATCH, oracle seeds 501-503 MATCH.
- v60 vs v59: win where a window spans two months (60M -2.4%, 10/12 faster), tie otherwise. Clean build of bf91689 with
  docker --network=none --no-cache OK; container output MATCH v5 dev answer. Not submitted (user submits).

## Nov 22 ~06:15 UTC: board 0.99 s (user report; corrected from 0.90)
Assumed to be v63 (73501a31afb2b7131a3311387f7df5131e32cbbb), the last commit handed over; v59 scored 0.9663 s.
v63 local warm NCPU8 on v5 20M: median 1.276 s (v59 ~1.30) -> board/local ~0.78. +2.5% vs v59 on one board run: inside the
organizers' stated ~20% machine-to-machine spread; locally v63 is 2% faster at 20M and ~5-8% at 60M.
- Nov 22: v63 resubmitted: board 0.9795 s (user report). v63 board runs so far: 0.99, 0.9795; v59: 0.9663 (one run, likely a fast machine).
  Local head-to-head v59 vs v63 (12 interleaved runs, warm, NCPU8): v5 20M 1.249 -> 1.203 (-3.8%, 9/12); v5 60M 3.894 -> 3.720 (-4.5%, 12/12).

## Nov 22 ~10:00 UTC: v64 = v63 + links thread changes (candidate)
- Rayon workers start at nice 10 (env WORKER_NICE, 0 = off); the links thread stays at nice 0. On v63 the links thread
  finished at ~1.76-1.95 s, i.e. at or after the end of parsing, time-slicing with the parse/dedup workers; now it is done at ~0.76 s.
- Links union-find in a second pass over the interned id pairs, parent[] prefetched 16 pairs ahead (intern + union-find
  0.48-0.51 s -> ~0.38 s on the links thread); `pairs` and the per-id vectors pre-sized. Roots do not depend on union order.
- 60M (v5 data, warm, NCPU=8, ws/ab60.sh interleaved, all 24 runs clean): v63 4.252 -> v64 4.037 s median (-5.0%), 15/24
  rounds (sets: 7/12 -3.5%, 8/12 -6.6%); parse lap 2.140 -> 1.791. 20M: tie (1.167 -> 1.182, 5/12).
- PGO: profile regenerated for this source lost (-3.7%, 5/12 at 60M): v63's pgo/merged.profdata kept (same as every earlier retrain).
- Exact: SELFTEST ok; gate PASS (with the dev-answer hash line skipped: the kit's dev answer was re-issued Nov 21 19:55, after
  gate.sha256; v63 and v64 MATCH the current one); difftest seeds 611 and 612 (window Apr 22 -> May 6) MATCH;
  20M NCPU=1 Parquet cmp-identical to v63, NCPU=8 canonical table hashes identical.

## Nov 22 ~11:00 UTC: v65 = v64 + branch-free id hash + one gather less for ingest_ts (parser)
- Upper bounds measured on v64's parser (1 thread, p20m, pieces stubbed out, not exact): full 60.3 ns/line; the two id
  hashes replaced by one multiply 44.6; timestamps skipped 42.6-46.3; record pushes skipped ~57; event type skipped ~59.5.
  So the id hash was the largest single piece (~16 ns), not the timestamps. Earlier wyhash/AES/XXH3 variants tied because they
  kept a length-dependent loop: 1 in 8 ids is 33 bytes (4 words) and the rest 9 bytes (1 word), so the loop's exit branch
  mispredicts on about a quarter of the lines (the "lines sorted by shape" test never sorted by id length).
- New h12: ids up to 40 bytes are read with one zero-padded AVX-512 masked load (masked-off bytes cannot fault, so nothing
  past the slice is read) and hashed with a fixed sequence of four 128-bit multiplies (two independent, chain depth 3); longer
  inputs fold further word pairs (h12_long). The same function everywhere (events, clicks, links, products); h12_over == h12.
  SELFTEST: h12 == scalar reference on every event id of the input and on 20k fuzz slices (lengths 0..100, all alignments);
  no 96-bit collision among the sampled event ids.
- ts8g for the F1-only form (ingest_ts) no longer gathers the unused fourth word.
- Output: the hash moves users/events between shards, so part files are no longer byte-identical to v63 (row order inside
  parts); the tables as sorted sets are identical (ws/canon.py: user_daily 2221326 rows 196b836f85761f1f941dac91,
  campaign_daily 28014 rows 357f8931a73b4718889f740e, same as v63/v64), v5 dev answer MATCH, gate PASS (hash line skipped),
  difftests 621/622 MATCH (hash alone), 612 (month-crossing) and 631 MATCH (final).
- 60M (v5, warm, NCPU=8, ws/ab60.sh): hash alone vs v64: +3.8% (7/12) and a noisy second set; pooled clean-run medians
  4.023 -> 3.848 (+4.4%), 4/5 clean pairs; 20M +5.4% (6/12). Ingest gather tweak on top: +3.1% (7/12), parse lines 1.433 -> 1.332.
- PGO retrain for this source: inconclusive/noisy (one set, 1 clean pair); v63's profile kept, as every earlier retrain.
- Final v65 binary vs v64, 60M 12 rounds: 4.585 -> 4.091 (+10.8%, 7/12), clean medians 4.076 -> 3.941 (+3.3%).
- 20 B DKey, second form (chunk id dropped from loc and recovered from the key's position in the shard slice via the
  per-chunk counts; user shard kept in loc, so no loser payload reads): exact (SELFTEST, 20M tables == v63), peak RSS
  11.77 -> 11.45 GB, but 60M vs v65: 4/12, dedup 0.245 -> 0.302, parse lines 1.36 -> 1.43 (misaligned 20 B records),
  clean total -1.4%. Dropped (patch: /tmp/claude-0/dkey20_pos.patch on this VM). A 16 B key is not reachable exactly:
  ing is needed for invalid copies (no payload) and eh2 keeps the 96-bit identity.

## Nov 22 ~11:10 UTC: v66 = v65 + smaller Parquet output + no serial zero fill before resolve
- Per-stage CPU utilisation on v65 (60M, NCPU=8, 4 vCPU): every stage 97-100% except "pid alloc" (two vec![0; n] of
  300 MB zeroed on the main thread: 0.047 s at 27%) and "written" (0.34 s wall, 0.84 s sys: the page-cache copy of 247 MB).
- epid_all / cpid_all are allocated uninitialised (resolve writes every slot): regroup 0.556 -> 0.503 s.
- user_daily was 38.5 B/row, all PLAIN: day, sessions, events now RLE_DICTIONARY (dictionary page = the group's value range in
  order, one bit-packed run of indexes; plain INT32 if a range exceeds 2^16) and revenue_cents INT32 when every row fits
  (one choice for the whole table). 26 B/row: 242 -> 162 MB at 60M; written 0.354 -> 0.315 s.
  Readers checked: pyarrow (file + dataset), polars, duckdb; kit compare MATCH.
- 60M vs v65, 12 rounds: all runs +6.8% (8/12); clean medians within noise (-1.6%, 3/4 clean pairs).
- Exact: gate PASS (hash line skipped), SELFTEST, difftests 616 (Apr -> May window) and 641 MATCH, v5 dev MATCH, 20M and 60M
  outputs MATCH v65 (kit compare; the canonical-hash script now differs only by revenue's int32 type).

## Nov 22 ~11:35 UTC: v67 = v66 + sessionize without sorting events
- Rule used: with time buckets no wider than the 30-min gap (2^20 ms = 17.5 min of ts - window_start), two events of a person
  in one bucket are always in the same session, and two buckets with nothing between them are one session exactly when
  (earliest ts of the later) - (latest ts of the earlier) <= GAP. A session's start, event count, revenue and purchases
  are minima / sums over its buckets, so no order among events is needed.
- Per person shard: count events per person (purchases, ~3%, set aside and sorted by (person, ts)), scatter only the 4-byte
  times by person (was: 2 radix scatters by time + 1 by person of 14-byte records + an insertion fix-up pass), then per person
  a branch-free min / max / count per bucket in a 4096 x 12 B table (13.8 KB used for a 14-day window, L1) with a two-level
  bitmap, walked in time order to emit sessions; each session takes its purchases from the sorted list. Click handling,
  attribution, assists and rows are unchanged. Whales cost the same per event as anyone (no per-person sort).
- 60M vs v66 (12 rounds): sessionize 0.980 -> 0.674 s (-31%); total all runs +5.9% (7/12), clean medians 4.210 -> 3.833
  (+9.0%, 5/5 clean pairs). 20M: sessionize ~0.23 -> ~0.19 s.
- What is left in sessionize (cycle counters, 20M): per-session body (attribution window, campaign accumulators, rows) ~45%,
  click sort + prep ~22%, event count/scatter/aggregate/walk ~30%. Counts at 20M: 667k persons with events (65% with clicks),
  3.36M sessions, 2.22M rows, ~11 campaigns in the window per session, 9.2M assisted updates at day changes.
- Exact: gate PASS (hash line skipped), SELFTEST, difftests 612 (month-crossing) and 651 MATCH, 20M and 60M outputs MATCH v66,
  and an adversarial set (data/edge-bucket: 211k events on bucket/day/window edges, gaps of GAP-1 / GAP / GAP+1, dense
  buckets, whales, equal timestamps, links) MATCH the Python oracle at NCPU=8 and NCPU=1.
- Tried on top of v67, dropped (tie): attribution without sorting clicks. For people with <= 64 sessions a click serves the
  sessions j..k (j = sessions starting before it, k = sessions starting within a week after it), so credit is the latest click
  per j carried forward and assists are per-campaign 64-bit session masks intersected with per-day masks; clicks then need only a
  person scatter (no radix, no fix-up), people with > 64 sessions keep the sorted sliding window. Plus assisted counts in their own
  compact array and pre-sized row buffers. Exact (gate-level checks: dev, adversarial set vs oracle, 20M == v66).
  60M vs v67, 12 rounds: sessionize 0.702 -> 0.693 s, clean total +1.6% (2 of 3 clean pairs), all runs -14% (noise hits): tie.
  Counters (20M): the click sort it removes (~22% of the stage) is replaced by about as much per-click and per-(campaign, day)
  work; the 2,926 people with > 64 sessions hold 10% of sessions and 19% of clicks. Patch: /tmp/claude-0/attr_masks_v68c.patch.

## Nov 22 ~11:50 UTC: v68 = v67 + 8-byte keys through the person partition, purchases on a side stream
- Since v67 sessionize needs only (person, time) per event, the person partition writes u64 keys (person << 32 | ts) instead
  of 14-byte PR records; purchases (~3% of events) are pushed as PR records to one buffer per worker thread inside the same
  pass, partitioned by person shard, and sorted per shard by (person, ts) in sessionize. Row buffers are pre-sized.
- 60M vs v67 (12 rounds): 9/12; regroup 0.637 -> 0.518 s, sessionize 0.704 -> 0.606 s; clean total 4.266 -> 4.018 (+5.8%,
  2 of 3 clean pairs); peak RSS 11.76 -> 11.64 GB.
- Exact: gate PASS (hash line skipped), SELFTEST, difftests 616 (Apr -> May) and 661 MATCH, 20M and 60M MATCH v66,
  adversarial set (data/edge-bucket) MATCH the oracle at NCPU=8 and 1, dev MATCH at NCPU=3.
