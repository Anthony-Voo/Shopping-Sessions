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
