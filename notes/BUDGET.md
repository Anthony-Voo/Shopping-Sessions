# Where the time has to go (Nov 21, v59 source; 60M = the final's size)

## Machine constants (this VM, 4 vCPU, measured with notes-free C microbenchmarks)
| Operation | Rate (4 threads) |
|---|---|
| streaming read | 49 GB/s |
| copy into already-touched memory | 30 GB/s |
| copy into fresh THP memory | 7.7 GB/s (fresh 4K pages: 4.2 GB/s) |
| first touch of 6 GB anon THP | 0.54 s wall, 1.94 core-s sys |
| mmap + touch the 5.8 GB input (page cache warm) | 0.08 s wall, 0.14 core-s sys |
| independent random 8 B reads (DRAM) | 107 M/s (37 ns each per thread) |

Fresh memory is the expensive kind: ~3x the cost of a reused byte. The input mapping is cheap.

## Data at 60M
events 64.8M lines / 4.98 GB, clicks 15.5M / 0.73 GB, links 2.67M / 55 MB. Peak RSS 12 GB (5.8 GB of it the mapped input): ~6 GB of fresh anon memory, ~3 core-s of kernel time (16% of all CPU).

## Pass ledger (per event unless noted) vs measured wall at 60M (NCPU=8, warm, non-PGO lap build)
| Stage | Passes / bytes | Paper estimate | Measured |
|---|---|---|---|
| parse (+ links on a side thread) | read 77 B, write DKey 24 + PEv 30 = 54 B fresh (3.5 GB) | 62 ns/line compute = 1.0 s on 4 cores; + 3.5 GB fresh ~0.3 s; links thread holds 1 core for ~1.6 s (-25% capacity) ~+0.4 s | 1.96 s |
| key partition | read 24, write 24 fresh (1.56 GB) | 0.05 s bandwidth + 0.14 s fresh | 0.26 s |
| dedup | read 24 + 1 in-L2 probe | 65M x ~8 ns / 4 = 0.13 s | 0.25 s |
| user shuffle | read 30 + bitmap, write 30 (1.85 GB) | 0.06 s bw + recycled/fresh | 0.16 s |
| click parse + partition | read 47 B/click, write 28 + 28 | | 0.28 + 0.09 s |
| resolve | read 30, 1 hash lookup (shard table in L2), write 4 | 80M x ~15 ns / 4 = 0.3 s | 0.35 s |
| person partition | read 34, write PR 14 (0.87 GB) | 0.1 s | 0.17 s |
| sessionize | radix sort of PR (2-3 scatter passes of 14 B) + scan + attribution | single-thread kernel: 20M radix+scan 0.9 s -> 60M/4 = 0.7 s | 0.81 s |
| write | small | | 0.16 s |
| total | ~415 B moved per event (~27 GB), ~9 passes | | 4.49 s (PGO build: 3.95-4.2 s) |

Bandwidth alone would be ~0.6 s at 60M: the pipeline is CPU-bound, and of that CPU ~16% is the kernel
zeroing fresh pages, ~25% of parse time is lost to the links thread, and ~0.5 s is two copy-only
partitions (key, user) that exist only to regroup data.

## What a 0.6 s board pipeline has to look like
Board ~= 0.87 x local warm (calibrated on v26/v45/v51). 0.6 s board = ~0.69 s local at 20M = ~2.0 s at 60M
= 8 core-s for 65M events = ~123 ns per event all-in (today ~236 ns). The budget:
| Stage | Budget at 60M | How |
|---|---|---|
| parse | 1.0 s | the compute floor, with links off the parse's cores and no separate partition pass |
| dedup + both regroupings | 0.25 s | parse writes records straight into shard buckets (no key or user shuffle copy) |
| clicks | 0.2 s | |
| resolve | 0.25 s | |
| sessionize | 0.3 s | one counting pass by person, per-person ts sort in cache (no 2-3 pass radix) |
| write | 0.1 s | |
| fresh memory | halved | fewer intermediate buffers (each removed partition removes 1.5-1.9 GB) |
Every row has to land: no single change gets there. Parser branch prediction is NOT a lever:
with lines pre-sorted by shape (quotes, ts form, type) the parse lap moved only 0.443 -> 0.431 s at 20M.

## Ranked big changes (expected saving at 60M)
1. Parse writes DKey and PEv straight into per-(chunk, shard) buckets; dedup and resolve read the buckets.
   Removes the key partition (0.26 s) and the user shuffle (0.16 s) and ~3.4 GB of fresh memory (~0.3 s). Expect 0.45-0.6 s (11-14%).
2. Sessionize by person-count scatter + in-cache per-person ts sort instead of radix. Expect 0.2-0.3 s.
3. Links: parallel interning (hash-sharded) + integer union-find. Frees ~1.1 core-s during parse: expect ~0.25 s.
