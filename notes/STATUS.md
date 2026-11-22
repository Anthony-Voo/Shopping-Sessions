# Shopping Sessions: status at a glance (Nov 22, 04:13 UTC)

Built only from `notes/LOOP.md`, `notes/BUDGET.md`, `notes/NOTES.md` (main + dev), `notes/PARSER.md` (branch
`parse/xsimd`) and `git log` on main, dev, exp-sh10, parse/xsimd. No `notes/RUNTIME.md` exists. "n/m" = not measured.
Times are local wall seconds unless marked board; "x/y" = rounds won by the new version out of y interleaved rounds.

## 1. Current state

| | |
|---|---|
| Current best version | **v60** `bf91689482893c7f9d84fbd7fa7428e46f27d77d` (main and dev; the NOTES.md candidate). Newer **v61** `5312d035491f2357d6de4af9eee158360394ed32` (dev) is a wall-time tie: +0.5% (5/12) at 60M, +0.6% (4/12) at 20M; CPU -1.7% |
| Last submission + board | Board 0.9663 s (#5 at 02:34 UTC, #19 at 03:40 UTC). NOTES.md only *assumes* this is the frozen v59 `5b5058b022d0905aa151228e6f8b8d7c6f97cbf0`. Last submission with a recorded hash: #7 `b89e1be` (v51), 1.19 s |
| Board #1 | 0.5987 s (LOOP.md, ~03:40 UTC). Earlier at 02:34 UTC: 0.7257 s |
| Time left | Deadline Sun Nov 22 16:00 UTC: **11 h 47 min** from 04:13 UTC |

## 2. Stage breakdown

At 60M, v59 source, NCPU=8, warm cache, non-PGO build (BUDGET.md pass ledger). The 20M column is a different
version and dataset (v60 on v5 data, LOOP.md Nov 22 03:40); it splits stages differently.

| Stage | 60M s | Share | 20M s (v60, v5 data) | Share |
|---|---|---|---|---|
| parse (links thread runs alongside) | 1.96 | 43.7% | 0.533 | 41.7% |
| sessionize | 0.81 | 18.0% | 0.228 | 17.8% |
| click parse + partition | 0.37 (0.28 + 0.09) | 8.2% | 0.183 (user shuffle + clicks) | 14.3% |
| resolve | 0.35 | 7.8% | 0.133 (resolve + person partition) | 10.4% |
| key partition | 0.26 | 5.8% | 0.076 | 5.9% |
| dedup | 0.25 | 5.6% | 0.070 | 5.5% |
| person partition | 0.17 | 3.8% | (in resolve) | |
| user shuffle | 0.16 | 3.6% | (in clicks) | |
| write | 0.16 | 3.6% | 0.055 | 4.3% |
| **total** | **4.49** (PGO build 3.95-4.2) | | 1.278 (sum of laps) | |

Other facts from BUDGET.md:
- Bandwidth alone would be ~0.6 s at 60M, so the pipeline is CPU-bound.
- About 16% of all CPU is the kernel zeroing fresh pages (~6 GB fresh anonymous memory, 12 GB peak RSS).

## 3. Main table

### parse
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Per-line structural parser | v22 `cf7953c0e41adb787d7703653ec026a876886274` | 1.545 → 1.462 | 20M | ✅ | kept since v22 |
| Software prefetch in the parser | v24 `8577fd29d67f169a36d7d8e44004be7d72081e98` | 1.536 → 1.481 | 20M | ✅ | kept |
| 128 B branch-light line fast path | v26 `be891fc7cea375b0955c3b59ea363d2268733b70` | 1.521 → 1.454; board 1.49 | 20M | ✅ | kept; first big board drop |
| pext quote check for all 7 fields | v36 `623b779ed875185f842786d6f2dbad12ecbc6b53` | 70.9 → 68.2 ns/line (5/5) | 20M, 1 thread | ✅ | parser -3.8% |
| Event record packed 32 → 30 B | v39 `4e7f5387cb0bbdb701a0546629d82d42a33aef20` | mean 1.456 → 1.381 (5/6) | 20M | ✅ | fewer fresh bytes |
| 128-bit multiply id hash | v47 `378d2a24c576b4811df4af03e3f36b37ccb8b98f` | 75.5 → 65.9 ns/line; 1.418 → 1.380 (17/24) | 20M | ✅ | the last clear parser win |
| Two-way month caches for timestamps | v60 `bf91689482893c7f9d84fbd7fa7428e46f27d77d` | month-spanning 60M -2.4% (10/12); v5 n60 -0.0% (4/12), n20 +1.9% | both | ✅ | wins only when the 14-day window spans two months (~45% of seeds); tie otherwise |
| ingest_ts without re-validation (G17, R18) | not kept | parse lap -7%; total 14/24, pooled 27/48 | 20M | 🟡 | stage win, total flat |
| Hash the event id early (R28) | not kept | parse -2%; total 35/72 | 20M | 🟡 | stage win, total flat |
| Cross-line AVX-512 ts decode (A2-A4) | `564b2555e195f485d9c546a07087e055d5ec0d09` (parse/xsimd) | 58.5 → 56.9 ns/line; 60M total +0.79% (7/12) | both | 🟡 | decode -7.4 ns, the per-line tail gives it back |
| Small parser tweaks: sliding mask (G1), ts month cache (G2a, v36x), raw-pointer writes (v42x), PF_DIST 1k-8k (R1), ws hoist (R24), F1 first (R25), drop assert (R27), double prefetch (A5), F3 branch + merged F1/F2 | not kept | each within noise (e.g. R25 3/6, R27 2/6, v42x 69.1 → 67.9 ns) | 20M | 🟡 | flat profile: nothing > 6% alone |
| Hash-keyed product table alone | v40y, not kept | parse 0.577 → 0.583 | 20M | 🟡 | tie alone; won inside the v42 stack |
| CHUNK 32 → 16 MB (R2) | not kept | 1.394 → 1.412 (10/24) | 20M | 🟡 | not better |
| Quote-mask field bounds | v28, not kept | 1 thread 1.46-1.64 → 1.60-1.63 | 20M | ❌ | slower |
| Branch-free timestamp parsers (v37x, v40x, Nov 22 3-form) | not kept | 67-73 → 81-86; 65-71 → 74-79; 65.8 → 73 ns/line | 20M | ❌ | computing all forms costs more than the mispredicts |
| Batched parser by ts format | v48y, not kept | 65-79 → 71-82 ns (0/5) | 20M | ❌ | slower |
| pdep comma positions | G32, not kept | 62.6-65.4 → 67.3-75.0 ns | 20M | ❌ | slower |
| AES-NI id hash | G33, not kept | 63.6-68.3 → 68.2-73.0 ns | 20M | ❌ | not faster than mulx |
| Branch-free event_kind | R26, not kept | 1/6 | 20M | ❌ | slower |
| Software-pipelined parser | R30, not kept | 64.8 → 67.0 ns (2/8) | 20M | ❌ | mispredicts flush the overlapped work |
| AVX-512 gather ts decode | A1, not kept | 58-60 → 64-67 ns | 20M | ❌ | gathers are slow |
| Vector id hashes; raw-pointer pushes (R17); lazy fields (R23); drop kcount (R29) | not attempted | n/m | — | ❌ | rejected on analysis |
| Mispredicts as a lever | — | BUDGET: shape-sorted lines, parse lap 0.443 → 0.431 (-3%); Nov 22: event ts 13.5 → 7 ns/line sorted (~6.5 of 62 ns); R30: "mispredict-bound" | 20M | ⚠️ | conflicting: "not a lever" vs ~10% of per-line cost |
| Event record 26 B | v49 says kept (`1b2f8190ed0d17d0e7b3d383d0c8820da0e42b08`) | stack 1.475 → 1.421 (17/24); alone (H2, v48x) 13/24 | 20M | ⚠️ | conflicting: BUDGET (v59 source) still counts PEv 30 B and PARSER.md lists 26 B as open |
| price + cat in one u32 (P2 / G11) | — | n/m (expected parse -3%) | — | ⏳ | todo in LOOP.md |
| Cheaper event-id hash / batched stores (G3) | — | n/m (expected parse -5%) | — | ⏳ | todo; hash = 6.9 ns/line of 58.7 |

### dedup
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Clear losers only, compact table | v20 `b559b0cbbadd81f4492e8599fe397fe9f20182ad` | 1.772 → 1.568 | 20M | ✅ | kept |
| Table capacity 2x | v57 `6188b72e689758eab2e07cceca1a1044c15c8a47` | dedup 0.0715 → 0.0535 (16/16); 60M pooled 14/24 wall, 16/24 cpu | both | ✅ | stage gain solid; ~2-3% total at 60M |
| Prefetch the slot 8 keys ahead | v61 `5312d035491f2357d6de4af9eee158360394ed32` | 60M dedup 0.234 → 0.219 (4 runs); same idea at 20M (R13) 5/16 | both | 🟡 | helps only once the table exceeds L2 (60M); v61 total ties |
| 20 B dedup keys (v38, G12) | not kept | 1.402 → 1.417; G12 12/24 | 20M | 🟡 | parse -6% offset by a slower loser path |
| u32 table with 8-bit tag | v50x, not kept | 5/12 | 20M | 🟡 | tie |
| Table 4x | R8, not kept | dedup 0.0535 → 0.0570 (1/16) | 20M | ❌ | outgrows L2 |
| Zero only used slots | R20, not kept | 0.0535 → 0.0750 (0/16) | 20M | ❌ | scattered zeroing > one memset |
| Atomic bitset pre-filter | G13 (microbench) | 0.12 + 0.06 s vs 0.145 replaced | 20M | ❌ | costs more than it replaces |
| 16 B dedup key | — | n/m (expected -5%) | — | ⚠️ | not exact: ~1e-4 wrong-answer risk at 60M |

### clicks
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Packed click records + hash product table | v42 `a69983acb199a729140dbb3c7ce4e130343219b8` | best of 8: 1.388/1.402/1.412 → 1.291/1.355/1.374 | 20M | ✅ | stacked byte cuts won |
| Faster click ts (SWAR) + uid hash | v61 `5312d035491f2357d6de4af9eee158360394ed32` | click parse cpu 1.08-1.31 → 1.02-1.09 | 60M | 🟡 | CPU win; v61 total ties |
| Dedicated click parser (S4a) | not kept | 4 cores 0.182 → 0.183 | 20M | 🟡 | memory-bound with 4 cores |
| Campaign lookup by array / hash / ProdTab (G10, v44y, C3) | not kept | G10 8/16; C3 1.88 → 1.88 core-s | both | 🟡 | lookup is not the cost |
| Click parse joined with key partition | G22, not kept | 1.370 → 1.466 (5/16) | 20M | ❌ | loses freed-block reuse |
| Click chunk 7 MB | R15 | crashes on 20M (1.75 MB: 5/12) | 20M | ⚠️ | click refs need chunks < 4 MB |
| Sort clicks per person instead of the whole-shard click radix (insertion ≤ 32, stable sort above) | `8a5a4ab63d6065a21ce3740b8de77055ee774670` (easy/click-person-sort, off v60) | vs v60: total 4.154 → 4.338 (+4.45%, 4/12), sess lap 0.656 → 0.690; same binary old/new path: sess lap 0.661 → 0.703 (+6.4%, 2/12), total -1.5% (8/12, noise); RSS 11.32 → 11.32 GB | 60M | ❌ | exact (gate, seeds 701/702, 20M NCPU=1 byte-identical) but per-person sorts on unsorted clicks cost more than 2 cache-resident radix passes |

### links
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Prefetched open-addressing interner | v37 `0f70f1dcbb29269cf1261c91b9a02529362e1a75` | parse 0.573 → 0.525, total 1.463 → 1.420 (means of 6) | 20M | ✅ | judged on the parse lap |
| Interner table 2x (with 24 B campaign accumulator) | v48 `c6702ed9c01004ae23d44fe440af4350c278cf69` | 1.401 → 1.345 (20/24) | 20M | ✅ | kept |
| Links line scanner + 8 B name prefixes (G21) | in v55 `6c5c0a5af207de742c6efbac17d8c6ba94a67e35` | links thread 0.272 → 0.226-0.267 | 20M | ✅ | in the v55 stack |
| Same interner, first try | v33, not kept | 1.427 → 1.430 | 20M | 🟡 | stage -25%, total flat |
| Per-line pair scanner; prefetch AHEAD 24/48; 4x table; nice 19 (v38y, R5, R7, v41x) | not kept | all within noise (v41x 1.466 → 1.464) | both | 🟡 | moving work between stages doesn't change the total |
| Links as rayon::join with parse | G23, not kept | 1.364 → 1.429 (5/16) | 20M | ❌ | parse runs a worker short |
| Interning by radix sort | G31, not kept | links 0.885 → 1.190 | 60M | ❌ | 65536-way scatter: worse locality |
| How much the links thread slows parse | — | BUDGET: ~25% of parse (+0.4 s); LOOP: "~5% of CPU, not a lever"; PARSER.md: 0.11 s of lap, total +1.9% when deferred; v37y: 0.05-0.1 s | both | ⚠️ | conflicting |
| Parallel hash-sharded interning + integer union-find | — | n/m (BUDGET expects ~0.25 s at 60M) | — | ⏳ | depends on the conflict above |
| Build lks lists with a counting pass (R21) | — | n/m (~0.04 s) | — | ⏳ | skipped: unmeasurable on this VM |

### regroup (key partition, user shuffle, resolve, person partition)
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| 256 shards instead of 1024 | v29 `214b43b6fee5e012ca38ce69be1c15733bbab796` | 1.559 → 1.488 | 20M | ✅ | sweep 7..11 |
| 32 MB chunks + 384 B bursts | v30 `32426ed6f6c732a8186422455be9f2968b7da0b0` | 1.429 → 1.321 | 20M | ✅ | neighbours (R2-R4) all lose |
| Inline 64 B burst flushes | v21 (`cf7953c0e41adb787d7703653ec026a876886274`) | 6.35 → 6.04 cpu-s | 20M | ✅ | kept |
| No per-shard allocations (G18) | v53 `68b4b2d14ea216575aa423bc3e53183e635d9e64` | faults 97K → 35.5K; 1.453 → 1.403 (15/24); 60M tie vs v51 | both | ✅ | fewer 4 KB faults |
| Fused resolve partition (G26) | in v55 `6c5c0a5af207de742c6efbac17d8c6ba94a67e35` | regroup 0.141 → 0.129 (9/12); v55 stack 1.293 → 1.259 (16/24) | 20M | ✅ | stage win; alone a total tie |
| Final person ids at resolve | v44 `77c101378b2333cbd3410798bb2547cb2951408c` | stage3 0.202 → 0.153; total 1.458 → 1.431 | 20M | 🟡 | tie, kept as simpler |
| FlatTab resolve table | v61 `5312d035491f2357d6de4af9eee158360394ed32` | resolve + pm 0.488 → 0.472 | 60M | 🟡 | v61 total ties; older flat tables (v27, G9) lost |
| 1024 shards | exp-sh10 `7d7abd010714bde88c1ffb5e269e0fd20073cccb` | 60M 4.298 → 4.358 (1/6); G14 4.524 → 4.521 (6/8); 20M 10/16 | both | 🟡 | never board-tested (smaller-L2 scorer) |
| Shard/chunk sweeps (v31, v32) | not kept | 1.402 → 1.383 best; PSH 1.380 → 1.394 | 20M | 🟡 | sessionize doesn't depend on shard size |
| Fused keys in parser, first try | v23b, not kept | 1.547 → 1.554 | 20M | 🟡 | tie |
| Keys into shard blocks + placeholder + PGO (B9) / + workers nice 10 (B10) | `0ff446e39f9278d52d890e78f5740d6bd48e2a0f` (parse/xsimd) | B9 8/16, clean +1.7%; B10 +1.03% (7/16); parse lap -0.20 s | 60M | 🟡 | parse gain handed back in dedup, clicks, sessionize |
| Fused keys in parser (v34, v43y, B1, B7) | not kept | v34 1.346 → 1.378; v43y 1.564 → 1.629; B1 4.28 → 5.50 (0/12); B7 loss | both | ❌ | page faults move to later, less parallel stages |
| Parse-grouped fusion (BIG1) | not kept | +4.5% (3/12); 15.67 → 16.69 core-s | 60M | ❌ | grouping both streams per chunk costs more |
| Direct resolve via a global table | not kept | ~0.18 → 0.35-0.37 s | 20M | ❌ | random lookups into one 58 MB table |
| Drop the user grouping (R1) | not attempted | n/m | — | ❌ | rejected on analysis |
| Open-addressing / identity-hash resolve (v27, G9, R16) | not kept | v27 1.482 → 1.553; G9 2/16; R16 0/16 | 20M | ❌ | worse |
| Resolve reserve bigger / smaller (R9, R10) | not kept | R9 0/16; R10 totals 5/24 twice | 20M | ❌ | current reserve sits between two worse settings |
| Person-shard locality; largest-first resolve (v23, v46x) | not kept | 1.531 → 1.588; 0.148 → 0.159 | 20M | ❌ | no imbalance to fix |
| Parse writes person-ready records once links are known (G7) | — | n/m | — | ⏳ | "big, hard"; the direct-resolve variant lost |

### sessionize
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Largest shards first + per-thread accumulators | v45 `a23aa9b4e3a87a01836004d369d0b4deaa63bb2b` | sess 0.320 → 0.289; board 1.22 | 20M | ✅ | whale shard 407K vs 76K mean |
| u64 per-person sort keys + 4 B click refs | v50 `6b47f081bf291eb95179495cb1f25c495c71ecdf` | 1.418 → 1.335 (18/24) | 20M | ✅ | sorts were 1.0 of 2.2 Gcyc |
| Whole-shard radix by ts | v51 `b89e1be0e1ef6af0ea2e3e17bf7561d006e33228` | sess 0.269 → 0.244; 1.431 → 1.386 (16/24); board 1.22 → 1.19 | 20M | ✅ | local -20%, board only -2.5% |
| Lazy assists + 2-pass radix (G4 + G8) | v52 `b816de0bc720598f8ca014e6ba3a6c298c4245f0` | 1.337 → 1.298 (20/24) | 20M | ✅ | near-ties that won stacked |
| One pass per session | v54 `850b4d30e64a93c68882d5ea96c272150a3eab27` | 1.334 → 1.282 (20/24); 60M 4.60 → 4.57 | both | ✅ | 20M win, 60M tie |
| Equal-ts check inside the fix-up | v56 `733533f9ed01c7c7e3afd03377943b5223456a10` | 1.378 → 1.302 (16/24); vs v54 pooled 20/48; 60M 4.196 → 4.329 (4/10) | both | 🟡 | in code, but a tie under re-measurement |
| Separate assist table; 32-bit ts sort; day-major campaign table (v52x, v44x, R11) | not kept | v52x 6/16; R11 not confirmed | 20M | 🟡 | stage gains, total flat |
| In-place counting sort | S1, not kept | 0.280 → 0.315 | 20M | ❌ | worse |
| Radix 3×8-bit / 1 pass / none / ts>>10 (G16, G28, G25, R12) | not kept | 0.239 → 0.248; 0.246 → 0.346; 0.247 → 0.327; 6/16 | 20M | ❌ | 2 passes on ts>>9 is the sweet spot |
| Person count + per-person sort (BIG2) | not kept | 0.707-0.787 → 0.807-1.135 (6/6 worse) | 60M | ❌ | small random sorts are branchy |
| 32 B output rows | R19, not kept | 4/16 | 20M | ❌ | random name lookups at write |

### write
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| INT32 counts (G20) | in v55 `6c5c0a5af207de742c6efbac17d8c6ba94a67e35` | write 0.0640 → 0.0595 (10/16); output 93 → 76 MB | 20M | ✅ | in the v55 stack |
| Streaming / exact-size pages (v48z, G19, Nov 22) | not kept | 0.080 → 0.080; 0.0665 → 0.0705 (7/16); 0.053 → 0.051 | 20M | 🟡 | no effect |
| Fork-and-report fast exit | G15, not kept | docker 1.918 → 1.914 (9/20) | both | 🟡 | the container waits for the child |
| Boxed-closure string columns | G30, not kept | 0.0535 → 0.0660 (3/12) | 20M | ❌ | per-row dyn calls |
| Overlap the writer with sessionize; dictionary for top_category | — | n/m (expected -2%) | — | ⏳ | backlog #10 |

### memory
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Lazy input mapping (no MAP_POPULATE) | v35 `9cc7c7bbc39d550c1177409c39b983d9b1ba165b` | cold 2.16-2.32 → 1.57-1.73; warm 1.380 → 1.370 | 20M | ✅ | the cold-run win |
| Allocator gathers freed blocks with mremap | v46 `2887895a317b63e9e6c1c3877b3c4e517dd6099c` | median 1.438 → 1.354 (9/12) | 20M | ✅ | memory reuse done here |
| +256 KB pool threshold | M2, not kept | adds nothing | 20M | 🟡 | tie |
| Split oversized free blocks | v47x, not kept | 1.348 → 1.381 | 20M | ❌ | worse |
| MADV_POPULATE_WRITE / _READ (v37z, v38x) | not kept | parse 0.54-0.66 → 0.65-0.71; cold 1.68 → 1.85 | 20M | ❌ | zeroes unused capacity; cold regression |
| Find the person stage's ~260 MB fresh (M3) | — | n/m (expected -1-2%) | — | ⏳ | needs a per-allocation log |

### build (PGO, flags)
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| PGO | v58 `b8cf9644ef230597aaeda27c1137374ef0dd98d5` | 1.238/1.252 → 1.184 (21/24) / 1.187 (18/24); 60M 4.245 → 4.162 (7/12) | both | ✅ | the only build change that won |
| Non-PIE static relocation | v59 `b05f61a85c93fb5589f81c408027165a55c7dc58` | 20M -3.4/-2.6% (19/24, 16/24); 60M pooled 7/20, timer +0.9% (6/12) | both | 🟡 | in code; gain at 20M vanished at 60M |
| Allow 512-bit auto-vectorization | G27, not kept | 7/16 | 20M | 🟡 | tie |
| PGO profile variants: broad, 60M-weighted, refreshed, v5-data, kb8 (R32, R35, freeze, Nov 22, B12) | not kept | 5/24 + 6/24; 20M 4/24; 60M +2.0% (3/12); +1.5% (0/12); worse | both | ❌ | the v58-style profile stays best |
| ext-tsp block placement | R34, not kept | 5/24, 4/24 (~3-4% worse) | 20M | ❌ | worse |
| target-cpu=skylake-avx512 | R36, not kept | 20M 31/48; 60M 3/10 | both | ❌ | 60M worse |
| Thread count 5/6/8 | v48w, not kept | 4: 1.492 vs 8: 1.549 | 20M | ❌ | scorer forces NCPU=8 anyway |

### language
| Idea | Version / full hash | Measured result | Size | Status | Why |
|---|---|---|---|---|---|
| Leave Rust (C/gcc, clang, Zig, Go, OCaml) | kernels only | our-style SIMD kernel gcc 1.65-1.88 vs clang 1.76-1.89 s; sort/hash kernels tie | 20M kernels | ❌ | gcc only wins on naive byte loops; hot code is intrinsics, memory-bound |

## 4. Patterns
- **Parser micro-tweaks since v47:** 24 tries, 1 win (v60), and that win only shows on month-spanning data.
  The single-thread profile is flat: nothing is over 6% on its own.
- **Stage wins that don't move the total:** G17, R28, G26, R10, v33, v52x and v61 each sped up their own stage, but the total stayed flat.
  The pipeline is CPU-bound on 4 cores, so cutting one stage mostly hands its time to the others.
- **Byte cuts and near-ties:** they tie alone but win stacked (v42, v49, v52, v55). That only showed at 20M.
- **Structural redesigns:** 0 wins in 10 measured tries (direct resolve, BIG1, BIG2, fused key partition ×7). The usual cause is
  page-fault cost moving into later, less parallel stages.
- **20M gains don't carry to 60M or the board:** v53, v54, v56 and v59 tie or lose at 60M. v51 was -20% locally but only -2.5% on the board.

## 5. Top 3 next moves (expected seconds at 60M)
1. **Finish the fused key partition** (B10 `0ff446e39f9278d52d890e78f5740d6bd48e2a0f`) by having dedup, clicks and
   sessionize reuse the key blocks. **Up to ~0.29 s** (the whole key partition, PARSER.md rank 1).
   - Its parse lap already drops 0.20 s; today that gain is handed back downstream.
   - Of the three, it's the only one aimed at a stage this large.
2. **Cheaper event-id hash / batched record stores (G3).** **~0.1 s** (LOOP.md expects -5% of parse; parse is 1.96 s).
   - Upper bound ~0.11 s: the hash is 6.9 of 58.7 ns/line, i.e. 12% of the 0.95 s parse compute.
   - Risk: it is a parser micro-change, the kind that keeps tying.
3. ~~Skip the click radix for persons with 0/1 clicks~~: tried Nov 22 ~05:00 UTC (`8a5a4ab`), exact but sessionize
   +6.4% at 60M: ❌. Next easy candidate: price + category in one u32 (P2, ~0.06 s, M effort, medium risk).
