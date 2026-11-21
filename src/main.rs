//! Shopping Sessions: fast exact pipeline.
//!
//! Design: read every CSV into memory in parallel, parse chunks in parallel with
//! hand-written parsers that follow rules.md exactly, then shard records by hash so
//! each worker owns a disjoint set of keys (event_id for dedup, person for
//! sessionizing) and needs no locks. Verified against oracle/reference.py.

mod pq;
mod recycle;

#[global_allocator]
static GLOBAL: recycle::Recycler = recycle::Recycler;

use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const BAD: i64 = i64::MIN; // unparseable timestamp
const HUGE: i64 = i64::MAX; // F3 too long for i64: valid form, outside every window
const SH_BITS: u32 = 10; // shard count = 2^SH_BITS
const NSH: usize = 1 << SH_BITS;
const CHUNK: usize = 8 << 20; // bytes per parse task
const ORGANIC: u32 = u32::MAX;
const UNKNOWN_CAT: u32 = u32::MAX;

type B = &'static [u8];

// ------------------------------------------------------------------ helpers
/// Fast 64-bit hash of a short byte string. Collisions are always checked against the
/// original bytes by the callers, so this only has to be fast and well mixed.
#[inline]
fn h64(b: &[u8]) -> u64 {
    h64s(b, 0x9E37_79B9_7F4A_7C15)
}

/// h12 for a field that is followed by at least 8 more bytes of the same line (event_id and
/// user_id are fields 0 and 1, so ts, ingest_ts, ... always follow): the tail is read with one
/// masked 8-byte load instead of a byte loop. Identical results to h12 (checked by SELFTEST).
#[inline(always)]
fn h12_over(b: &[u8]) -> (u64, u64) {
    let len = (b.len() as u64).wrapping_mul(0xA076_1D64_78BD_642F);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ len;
    let mut y: u64 = 0xD6E8_FEB8_6659_FD93 ^ len;
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        x = (x ^ w).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 32;
        y = (y ^ w).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        y ^= y >> 32;
        i += 8;
    }
    let rem = b.len() - i;
    // SAFETY: callers guarantee >= 8 readable bytes after the field (same mapped line)
    let raw = unsafe { std::ptr::read_unaligned(b.as_ptr().add(i) as *const u64) };
    let t = if rem == 0 { 0 } else { raw & (u64::MAX >> (64 - 8 * rem)) };
    let fin = |mut h: u64| {
        h = (h ^ t).wrapping_mul(0xC4CE_B9FE_1A85_EC53);
        h ^= h >> 29;
        h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 32)
    };
    (fin(x), fin(y))
}

/// Both hashes in one pass over the bytes: (h64(b), h2(b)).
#[inline]
fn h12(b: &[u8]) -> (u64, u64) {
    let len = (b.len() as u64).wrapping_mul(0xA076_1D64_78BD_642F);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ len;
    let mut y: u64 = 0xD6E8_FEB8_6659_FD93 ^ len;
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        x = (x ^ w).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 32;
        y = (y ^ w).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        y ^= y >> 32;
        i += 8;
    }
    let mut t = 0u64;
    for (k, &c) in b[i..].iter().enumerate() {
        t |= (c as u64) << (8 * k);
    }
    let fin = |mut h: u64| {
        h = (h ^ t).wrapping_mul(0xC4CE_B9FE_1A85_EC53);
        h ^= h >> 29;
        h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 32)
    };
    (fin(x), fin(y))
}

/// Independent second hash: (h64, h2) together identify a string (2^-128 collision odds).
#[inline]
fn h2(b: &[u8]) -> u64 {
    h64s(b, 0xD6E8_FEB8_6659_FD93)
}

#[inline]
fn h64s(b: &[u8], seed: u64) -> u64 {
    let mut h: u64 = seed ^ (b.len() as u64).wrapping_mul(0xA076_1D64_78BD_642F);
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        h = (h ^ w).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 32;
        i += 8;
    }
    let mut t = 0u64;
    for (k, &c) in b[i..].iter().enumerate() {
        t |= (c as u64) << (8 * k);
    }
    h = (h ^ t).wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    h ^= h >> 29;
    h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 32)
}

#[inline]
fn shard_h(h: u64) -> usize {
    (h >> (64 - SH_BITS)) as usize
}

#[inline]
fn shard_of(b: &[u8]) -> usize {
    let mut h = FxHasher::default();
    b.hash(&mut h);
    ((h.finish().wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize) % NSH
}

/// One CSV field: Err = malformed; Ok(None) = null.
#[inline]
fn field(f: B) -> Result<Option<B>, ()> {
    let v = if memchr::memchr(b'"', f).is_some() {
        let n = f.len();
        if n < 2 || f[0] != b'"' || f[n - 1] != b'"' {
            return Err(());
        }
        let inner = &f[1..n - 1];
        if memchr::memchr(b'"', inner).is_some() {
            return Err(());
        }
        inner
    } else {
        f
    };
    Ok(if v.is_empty() { None } else { Some(v) })
}

/// One field, given whether it contains a double quote: Err = malformed; Ok(None) = null.
#[inline(always)]
fn field_q(f: B, quoted: bool) -> Result<Option<B>, ()> {
    if !quoted {
        return Ok(if f.is_empty() { None } else { Some(f) });
    }
    let n = f.len();
    if n < 2 || f[0] != b'"' || f[n - 1] != b'"' {
        return Err(());
    }
    let inner = &f[1..n - 1];
    if inner.contains(&b'"') {
        return Err(());
    }
    Ok(if inner.is_empty() { None } else { Some(inner) })
}

/// Exact match of event_type against view / cart / purchase with integer compares.
#[inline(always)]
fn event_kind(et: Option<&[u8]>) -> u8 {
    match et {
        Some(b) if b.len() == 4 => {
            let w = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            if w == u32::from_le_bytes(*b"view") {
                0
            } else if w == u32::from_le_bytes(*b"cart") {
                1
            } else {
                3
            }
        }
        Some(b) if b.len() == 8 && u64::from_le_bytes(b.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") => 2,
        _ => 3,
    }
}

/// 64-byte block masks: (commas, newlines, quotes), bit i = byte i.
#[inline(always)]
fn block_masks(p: &[u8]) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx512bw")]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm512_loadu_si512(p.as_ptr() as *const _);
        return (
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
        );
    }
    #[cfg(all(target_feature = "avx2", not(target_feature = "avx512bw")))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |v: __m256i, c: u8| -> u32 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 };
        let j = |lo: u32, hi: u32| -> u64 { lo as u64 | ((hi as u64) << 32) };
        return (j(m(a, b','), m(b, b',')), j(m(a, b'\n'), m(b, b'\n')), j(m(a, b'"'), m(b, b'"')));
    }
    #[allow(unreachable_code)]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for (i, &x) in p[..64].iter().enumerate() {
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

/// Finish one line given its comma positions and quote count (rules.md CSV dialect).
#[inline(always)]
fn finish_line<'a, const N: usize>(chunk: &'a [u8], start: usize, end: usize, commas: &[usize], nq: u32) -> Option<[Option<&'a [u8]>; N]> {
    if end == start || commas.len() != N - 1 {
        return None; // empty line or wrong field count
    }
    let mut out = [None; N];
    let mut fs = start;
    let mut quoted_pairs = 0u32;
    for k in 0..N {
        let fe = if k < N - 1 { commas[k] } else { end };
        let len = fe - fs;
        // quoted = field is at least 2 bytes and starts and ends with '"' (computed without branches)
        // both reads stay inside the buffer even for an empty last field with no trailing newline
        let first = *chunk.get(fs).unwrap_or(&0);
        let last = *chunk.get(fe.wrapping_sub(1).max(fs)).unwrap_or(&0);
        let isq = ((nq > 0) & (len >= 2) & (first == b'"') & (last == b'"')) as usize;
        quoted_pairs += isq as u32;
        let v = &chunk[fs + isq..fe - isq];
        out[k] = if v.is_empty() { None } else { Some(v) };
        fs = fe + 1;
    }
    // every quote must be one of the two around a whole field
    if nq != 2 * quoted_pairs {
        return None;
    }
    Some(out)
}

/// Scan a chunk of lines (each ending in '\n', the last one may not) and call `f` with each
/// line's N fields, or None for a malformed line. Equivalent to lines() + fields::<N>().
#[inline]
fn scan_lines<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(Option<[Option<&'a [u8]>; N]>)) {
    let n = chunk.len();
    let mut start = 0usize;
    let mut commas: [usize; 16] = [0; 16];
    let mut nc = 0usize; // commas seen in the current line (capped)
    let mut nq = 0u32;
    let mut base = 0usize;
    let mut pad = [0u8; 64];
    while base < n {
        let (c, nl, q) = if base + 64 <= n {
            block_masks(&chunk[base..base + 64])
        } else {
            pad = [0u8; 64];
            pad[..n - base].copy_from_slice(&chunk[base..]);
            block_masks(&pad)
        };
        let mut m = c | nl;
        let mut qleft = q; // quotes in this block not yet assigned to a line
        while m != 0 {
            let b = m.trailing_zeros();
            m &= m - 1;
            let bit = 1u64 << b;
            let pos = base + b as usize;
            if nl & bit != 0 {
                let before = qleft & (bit - 1);
                nq += before.count_ones();
                qleft &= !(bit | (bit - 1));
                f(if nc <= 15 { finish_line::<N>(chunk, start, pos, &commas[..nc], nq) } else { None });
                start = pos + 1;
                nc = 0;
                nq = 0;
            } else {
                if nc < 16 {
                    commas[nc] = pos;
                }
                nc += 1;
            }
        }
        nq += qleft.count_ones();
        base += 64;
    }
    let _ = pad;
    if start < n {
        f(if nc <= 15 { finish_line::<N>(chunk, start, n, &commas[..nc], nq) } else { None });
    }
}

/// (commas, quotes) masks for a 64-byte window.
#[inline(always)]
fn cq_masks(p: &[u8]) -> (u64, u64) {
    #[cfg(target_feature = "avx512bw")]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm512_loadu_si512(p.as_ptr() as *const _);
        return (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)), _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)));
    }
    #[allow(unreachable_code)]
    {
        let (c, _, q) = block_masks(p);
        (c, q)
    }
}

/// Per-line scanner: line ends from one streaming newline search, then each line's commas and
/// quotes from two 64-byte compares. Same results as scan_lines (checked by SELFTEST).
#[inline]
fn scan_lines2<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(Option<[Option<&'a [u8]>; N]>)) {
    let n = chunk.len();
    let mut start = 0usize;
    let mut it = memchr::memchr_iter(b'\n', chunk);
    let mut pad = [0u8; 128];
    let mut commas = [0usize; 16];
    loop {
        if start >= n {
            return;
        }
        let end = it.next().unwrap_or(n);
        let len = end - start;
        if len <= 128 {
            // two 64-byte windows; copy to a padded buffer when they would run past the chunk
            let w: &[u8] = if start + 128 <= n {
                &chunk[start..start + 128]
            } else {
                pad = [0u8; 128];
                pad[..n - start].copy_from_slice(&chunk[start..]);
                &pad
            };
            let (c0, q0) = cq_masks(&w[..64]);
            let (c1, q1) = cq_masks(&w[64..128]);
            let keep0 = if len >= 64 { u64::MAX } else { (1u64 << len) - 1 };
            let keep1 = if len >= 128 { u64::MAX } else if len > 64 { (1u64 << (len - 64)) - 1 } else { 0 };
            let (c0, c1) = (c0 & keep0, c1 & keep1);
            let nq = (q0 & keep0).count_ones() + (q1 & keep1).count_ones();
            let nc = (c0.count_ones() + c1.count_ones()) as usize;
            if nc != N - 1 {
                f(if len == 0 { None } else { None });
            } else {
                let mut m = c0;
                let mut k = 0;
                while m != 0 {
                    commas[k] = start + m.trailing_zeros() as usize;
                    m &= m - 1;
                    k += 1;
                }
                let mut m = c1;
                while m != 0 {
                    commas[k] = start + 64 + m.trailing_zeros() as usize;
                    m &= m - 1;
                    k += 1;
                }
                f(finish_line::<N>(chunk, start, end, &commas[..k], nq));
            }
        } else {
            // long line: count commas/quotes directly
            let line = &chunk[start..end];
            let mut k = 0;
            let mut nq = 0u32;
            for (i, &b) in line.iter().enumerate() {
                if b == b',' {
                    if k < 16 {
                        commas[k] = start + i;
                    }
                    k += 1;
                } else if b == b'"' {
                    nq += 1;
                }
            }
            f(if k == N - 1 { finish_line::<N>(chunk, start, end, &commas[..k], nq) } else { None });
        }
        start = end + 1;
    }
}

/// Dedicated event-chunk parser: per-line SIMD comma/quote masks, field extraction and
/// validation in one loop (same semantics as scan_lines::<7> + the generic event closure).
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn parse_event_chunk(
    chunk: &'static [u8],
    ci: usize,
    ws: i64,
    we: i64,
    products: &FxHashMap<B, u32>,
) -> (Vec<PEv>, Vec<DKey>, Vec<u32>) {
    let cap = chunk.len() / 64 + 16;
    let mut pay: Vec<PEv> = Vec::with_capacity(cap);
    let mut keys: Vec<DKey> = Vec::with_capacity(cap);
    let mut kcount = vec![0u32; NSH];
    let n = chunk.len();
    let mut start = 0usize;
    let mut it = memchr::memchr_iter(b'\n', chunk);
    let mut pad = [0u8; 128];
    let mut cm = [0usize; 8];
    while start < n {
        let end = it.next().unwrap_or(n);
        let len = end - start;
        let line_start = start;
        start = end + 1;
        // comma positions (exactly 6) and quote count
        let nq;
        if len <= 128 {
            let w: &[u8] = if line_start + 128 <= n {
                &chunk[line_start..line_start + 128]
            } else {
                pad = [0u8; 128];
                pad[..n - line_start].copy_from_slice(&chunk[line_start..]);
                &pad
            };
            let (c0, q0) = cq_masks(&w[..64]);
            let (c1, q1) = cq_masks(&w[64..128]);
            let keep0 = if len >= 64 { u64::MAX } else { (1u64 << len) - 1 };
            let keep1 = if len >= 128 { u64::MAX } else if len > 64 { (1u64 << (len - 64)) - 1 } else { 0 };
            let (c0, c1) = (c0 & keep0, c1 & keep1);
            if c0.count_ones() + c1.count_ones() != 6 {
                continue; // wrong field count (also empty lines)
            }
            nq = (q0 & keep0).count_ones() + (q1 & keep1).count_ones();
            let mut k = 0;
            let mut m = c0;
            while m != 0 {
                cm[k] = line_start + m.trailing_zeros() as usize;
                m &= m - 1;
                k += 1;
            }
            let mut m = c1;
            while m != 0 {
                cm[k] = line_start + 64 + m.trailing_zeros() as usize;
                m &= m - 1;
                k += 1;
            }
        } else {
            let mut k = 0;
            let mut q = 0u32;
            for (i, &b) in chunk[line_start..end].iter().enumerate() {
                if b == b',' {
                    if k < 8 {
                        cm[k] = line_start + i;
                    }
                    k += 1;
                } else if b == b'"' {
                    q += 1;
                }
            }
            if k != 6 {
                continue;
            }
            nq = q;
        }
        // field k spans [fs_k, fe_k); strip quotes around whole fields; validate the quote count
        let mut fb = [(0usize, 0usize); 7];
        let mut pairs = 0u32;
        let mut fs = line_start;
        for k in 0..7 {
            let fe = if k < 6 { cm[k] } else { end };
            let flen = fe - fs;
            let first = *chunk.get(fs).unwrap_or(&0);
            let last = *chunk.get(fe.wrapping_sub(1).max(fs)).unwrap_or(&0);
            let isq = ((nq > 0) & (flen >= 2) & (first == b'"') & (last == b'"')) as usize;
            pairs += isq as u32;
            fb[k] = (fs + isq, fe - isq);
            fs = fe + 1;
        }
        if nq != 2 * pairs {
            continue; // a quote that is not around a whole field
        }
        let field = |k: usize| -> Option<B> {
            let (a, b) = fb[k];
            if a < b { Some(&chunk[a..b]) } else { None }
        };
        let Some(eid) = field(0) else { continue };
        let ing = parse_ts(field(3));
        let ts = parse_ts_over(field(2));
        let uid = field(1);
        let kind = event_kind(field(4));
        let pid = field(5);
        let mut ok = uid.is_some() && pid.is_some() && kind < 3 && ts != BAD && ts >= ws && ts < we;
        let mut p = 0u32;
        let mut cat = 0u16;
        if ok && kind == 2 {
            match parse_price(field(6)) {
                Some(v) => {
                    p = v as u32;
                    cat = match products.get(pid.unwrap()) {
                        Some(&c) => c as u16,
                        None => u16::MAX,
                    };
                }
                None => ok = false,
            }
        }
        if ok && ing - ts > DAY {
            ok = false;
        }
        let mut idx = u32::MAX as u64;
        let mut ush = 0u64;
        if ok {
            let u = uid.unwrap();
            idx = pay.len() as u64;
            let (uh, uh2) = h12_over(u);
            ush = shard_h(uh) as u64;
            assert!(u.len() <= 0xFFFF && (u.as_ptr() as u64) < (1 << 48), "user_id reference does not pack");
            pay.push(PEv {
                pk: uh,
                nref: (u.as_ptr() as u64) | ((u.len() as u64) << 48),
                pk2: uh2 as u32,
                ts: (ts - ws) as u32,
                price: p,
                cat,
            });
        }
        let (eh, eh2) = h12_over(eid);
        kcount[shard_h(eh)] += 1;
        // ingest_ts is always F1 (whole seconds); +-68 years around the window fits in i32
        let ing_s = (ing.div_euclid(1000) - ws.div_euclid(1000)).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        keys.push(DKey { eh, eh2: eh2 as u32, ing: ing_s, loc: (ush << 48) | ((ci as u64) << 32) | idx });
    }
    let _ = pad;
    (pay, keys, kcount)
}
static STAGE1_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Two-stage event parser. Stage 1 writes the position of every comma and newline in the
/// chunk into one index (newlines tagged with the top bit) plus per-newline quote prefix
/// counts, with no per-character branches. Stage 2 reads 7 index entries per line. Produces
/// exactly the same records as parse_event_chunk (checked by SELFTEST).
#[inline(never)]
fn parse_event_chunk3(
    chunk: &'static [u8],
    ci: usize,
    ws: i64,
    we: i64,
    products: &FxHashMap<B, u32>,
) -> (Vec<PEv>, Vec<DKey>, Vec<u32>) {
    const NL: u32 = 1 << 31;
    let n = chunk.len();
    assert!(n < (1 << 31), "chunk too large for the structural index");
    let t_stage = Instant::now();
    // stage 1a: count structural characters to size the index exactly
    let mut pad = [0u8; 64];
    let mut total = 0usize;
    let mut nlines = 0usize;
    let mut base = 0usize;
    while base < n {
        let blk: &[u8] = if base + 64 <= n {
            &chunk[base..base + 64]
        } else {
            pad = [0u8; 64];
            pad[..n - base].copy_from_slice(&chunk[base..]);
            &pad
        };
        let (c, nl, _) = block_masks(blk);
        total += (c | nl).count_ones() as usize;
        nlines += nl.count_ones() as usize;
        base += 64;
    }
    // stage 1b: flatten bit positions (branch-free tag) and per-newline quote prefixes
    let mut idx: Vec<u32> = Vec::with_capacity(total + 8);
    let mut nlq: Vec<u32> = Vec::with_capacity(nlines + 1);
    let ip = idx.as_mut_ptr();
    let mut w = 0usize;
    let mut qprefix = 0u32;
    base = 0;
    while base < n {
        let blk: &[u8] = if base + 64 <= n {
            &chunk[base..base + 64]
        } else {
            pad = [0u8; 64];
            pad[..n - base].copy_from_slice(&chunk[base..]);
            &pad
        };
        let (c, nl, q) = block_masks(blk);
        let mut m = c | nl;
        while m != 0 {
            let b = m.trailing_zeros();
            m &= m - 1;
            let tag = (((nl >> b) & 1) as u32) << 31;
            // SAFETY: w < total, the exact count from stage 1a
            unsafe { ip.add(w).write((base as u32 + b) | tag) };
            w += 1;
        }
        let mut mm = nl;
        while mm != 0 {
            let b = mm.trailing_zeros();
            mm &= mm - 1;
            nlq.push(qprefix + (q & ((1u64 << b) - 1)).count_ones());
        }
        qprefix += q.count_ones();
        base += 64;
    }
    // SAFETY: exactly `total` entries were written
    unsafe { idx.set_len(w) };
    let _ = pad;
    let qtotal = qprefix;
    STAGE1_NS.fetch_add(t_stage.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    // stage 2
    let cap = n / 64 + 16;
    let mut pay: Vec<PEv> = Vec::with_capacity(cap);
    let mut keys: Vec<DKey> = Vec::with_capacity(cap);
    let mut kcount = vec![0u32; NSH];
    let m = idx.len();
    let mut e = 0usize;
    let mut li = 0usize; // newlines consumed so far
    let mut next_start = 0usize;
    let mut cm = [0usize; 8];
    let mut mc = MonthCache::new();
    while next_start < n {
        let line_start = next_start;
        let end;
        let ok;
        if e + 7 <= m
            && idx[e + 6] & NL != 0
            && (idx[e] | idx[e + 1] | idx[e + 2] | idx[e + 3] | idx[e + 4] | idx[e + 5]) & NL == 0
        {
            for k in 0..6 {
                cm[k] = idx[e + k] as usize;
            }
            end = (idx[e + 6] & !NL) as usize;
            e += 7;
            ok = true;
        } else {
            let mut k = 0usize;
            let mut found = None;
            while e < m {
                let v = idx[e];
                e += 1;
                if v & NL != 0 {
                    found = Some((v & !NL) as usize);
                    break;
                }
                if k < 8 {
                    cm[k] = v as usize;
                }
                k += 1;
            }
            end = found.unwrap_or(n);
            ok = k == 6;
        }
        // quotes in this line: prefix at its newline minus prefix at the previous newline
        let before = if li == 0 { 0 } else { nlq[li - 1] };
        let nq = if end < n { nlq[li] - before } else { qtotal - before };
        if end < n {
            li += 1;
        }
        next_start = end + 1;
        if !ok {
            continue;
        }
        // field k spans [fs_k, fe_k); strip quotes around whole fields; validate the quote count
        let mut fb = [(0usize, 0usize); 7];
        let mut pairs = 0u32;
        let mut fs = line_start;
        for k in 0..7 {
            let fe = if k < 6 { cm[k] } else { end };
            let flen = fe - fs;
            let first = *chunk.get(fs).unwrap_or(&0);
            let last = *chunk.get(fe.wrapping_sub(1).max(fs)).unwrap_or(&0);
            let isq = ((nq > 0) & (flen >= 2) & (first == b'"') & (last == b'"')) as usize;
            pairs += isq as u32;
            fb[k] = (fs + isq, fe - isq);
            fs = fe + 1;
        }
        if nq != 2 * pairs {
            continue; // a quote that is not around a whole field
        }
        let field = |k: usize| -> Option<B> {
            let (a, b) = fb[k];
            if a < b { Some(&chunk[a..b]) } else { None }
        };
        let Some(eid) = field(0) else { continue };
        let ing = mc.f1(field(3));
        let ts = parse_ts_over(field(2));
        let uid = field(1);
        let kind = event_kind(field(4));
        let pid = field(5);
        let mut ok = uid.is_some() && pid.is_some() && kind < 3 && ts != BAD && ts >= ws && ts < we;
        let mut p = 0u32;
        let mut cat = 0u16;
        if ok && kind == 2 {
            match parse_price(field(6)) {
                Some(v) => {
                    p = v as u32;
                    cat = match products.get(pid.unwrap()) {
                        Some(&c) => c as u16,
                        None => u16::MAX,
                    };
                }
                None => ok = false,
            }
        }
        if ok && ing - ts > DAY {
            ok = false;
        }
        let mut idx = u32::MAX as u64;
        let mut ush = 0u64;
        if ok {
            let u = uid.unwrap();
            idx = pay.len() as u64;
            let (uh, uh2) = h12_over(u);
            ush = shard_h(uh) as u64;
            assert!(u.len() <= 0xFFFF && (u.as_ptr() as u64) < (1 << 48), "user_id reference does not pack");
            pay.push(PEv {
                pk: uh,
                nref: (u.as_ptr() as u64) | ((u.len() as u64) << 48),
                pk2: uh2 as u32,
                ts: (ts - ws) as u32,
                price: p,
                cat,
            });
        }
        let (eh, eh2) = h12_over(eid);
        kcount[shard_h(eh)] += 1;
        // ingest_ts is always F1 (whole seconds); +-68 years around the window fits in i32
        let ing_s = (ing.div_euclid(1000) - ws.div_euclid(1000)).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        keys.push(DKey { eh, eh2: eh2 as u32, ing: ing_s, loc: (ush << 48) | ((ci as u64) << 32) | idx });
    }
    (pay, keys, kcount)
}
/// Cache-blocked two-stage event parser: each ~64 KB window (cut at a line end) is indexed and
/// then parsed while it is still in L2, so the input is read from memory once. Same records as
/// parse_event_chunk (checked by SELFTEST).
#[inline(never)]
fn parse_event_chunk4(
    chunk: &'static [u8],
    ci: usize,
    ws: i64,
    we: i64,
    products: &FxHashMap<B, u32>,
) -> (Vec<PEv>, Vec<DKey>, Vec<u32>, Vec<u32>) {
    const NL: u32 = 1 << 31;
    const WIN: usize = 64 << 10;
    let n = chunk.len();
    assert!(n < (1 << 31), "chunk too large for the structural index");
    let cap = n / 64 + 16;
    let mut pay: Vec<PEv> = Vec::with_capacity(cap);
    let mut keys: Vec<DKey> = Vec::with_capacity(cap);
    let mut kcount = vec![0u32; NSH];
    let mut ucount = vec![0u32; NSH]; // valid payloads per user shard
    // a window never holds more separators than bytes: size the index once
    let mut idx: Vec<u32> = Vec::with_capacity(WIN + 1024);
    let mut nlq: Vec<u32> = Vec::with_capacity(WIN + 1024);
    let mut cm = [0usize; 8];
    let mut mc = MonthCache::new();
    let mut pad = [0u8; 64];
    let mut wstart = 0usize;
    while wstart < n {
        // window [wstart, wend): ends just after a newline (or at the chunk end)
        let wend = if wstart + WIN >= n {
            n
        } else {
            match memchr::memchr(b'\n', &chunk[wstart + WIN..]) {
                Some(k) => wstart + WIN + k + 1,
                None => n,
            }
        };
        let wlen = wend - wstart;
        if idx.capacity() < wlen + 64 {
            idx.reserve(wlen + 64 - idx.len());
            nlq.reserve(wlen + 64 - nlq.len());
        }
        idx.clear();
        nlq.clear();
        // stage 1 on the window (absolute positions)
        let ip = idx.as_mut_ptr();
        let mut w = 0usize;
        let mut qprefix = 0u32;
        let mut base = wstart;
        while base < wend {
            let blk: &[u8] = if base + 64 <= wend {
                &chunk[base..base + 64]
            } else {
                pad = [0u8; 64];
                pad[..wend - base].copy_from_slice(&chunk[base..wend]);
                &pad
            };
            let (c, nl, q) = block_masks(blk);
            let mut m = c | nl;
            while m != 0 {
                let b = m.trailing_zeros();
                m &= m - 1;
                let tag = (((nl >> b) & 1) as u32) << 31;
                // SAFETY: at most wlen entries (one per byte) and capacity >= wlen + 64
                unsafe { ip.add(w).write((base as u32 + b) | tag) };
                w += 1;
            }
            let mut mm = nl;
            while mm != 0 {
                let b = mm.trailing_zeros();
                mm &= mm - 1;
                nlq.push(qprefix + (q & ((1u64 << b) - 1)).count_ones());
            }
            qprefix += q.count_ones();
            base += 64;
        }
        // SAFETY: exactly w entries were written
        unsafe { idx.set_len(w) };
        let qtotal = qprefix;
        // stage 2 on the window's lines
        let m = idx.len();
        let mut e = 0usize;
        let mut li = 0usize;
        let mut next_start = wstart;
        while next_start < wend {
            let line_start = next_start;
            let end;
            let ok;
            if e + 7 <= m
                && idx[e + 6] & NL != 0
                && (idx[e] | idx[e + 1] | idx[e + 2] | idx[e + 3] | idx[e + 4] | idx[e + 5]) & NL == 0
            {
                for k in 0..6 {
                    cm[k] = idx[e + k] as usize;
                }
                end = (idx[e + 6] & !NL) as usize;
                e += 7;
                ok = true;
            } else {
                let mut k = 0usize;
                let mut found = None;
                while e < m {
                    let v = idx[e];
                    e += 1;
                    if v & NL != 0 {
                        found = Some((v & !NL) as usize);
                        break;
                    }
                    if k < 8 {
                        cm[k] = v as usize;
                    }
                    k += 1;
                }
                end = found.unwrap_or(wend);
                ok = k == 6;
            }
            let before = if li == 0 { 0 } else { nlq[li - 1] };
            let nq = if end < wend { nlq[li] - before } else { qtotal - before };
            if end < wend {
                li += 1;
            }
            next_start = end + 1;
            if !ok {
                continue;
            }
        // field k spans [fs_k, fe_k); strip quotes around whole fields; validate the quote count
            let mut fb = [(0usize, 0usize); 7];
            let mut pairs = 0u32;
            let mut fs = line_start;
            for k in 0..7 {
                let fe = if k < 6 { cm[k] } else { end };
                let flen = fe - fs;
                let first = *chunk.get(fs).unwrap_or(&0);
                let last = *chunk.get(fe.wrapping_sub(1).max(fs)).unwrap_or(&0);
                let isq = ((nq > 0) & (flen >= 2) & (first == b'"') & (last == b'"')) as usize;
                pairs += isq as u32;
                fb[k] = (fs + isq, fe - isq);
                fs = fe + 1;
            }
            if nq != 2 * pairs {
                continue; // a quote that is not around a whole field
            }
            let field = |k: usize| -> Option<B> {
                let (a, b) = fb[k];
                if a < b { Some(&chunk[a..b]) } else { None }
            };
            let Some(eid) = field(0) else { continue };
            let ing = mc.f1(field(3));
            let ts = parse_ts_over(field(2));
            let uid = field(1);
            let kind = event_kind(field(4));
            let pid = field(5);
            let mut ok = uid.is_some() && pid.is_some() && kind < 3 && ts != BAD && ts >= ws && ts < we;
            let mut p = 0u32;
            let mut cat = 0u16;
            if ok && kind == 2 {
                match parse_price(field(6)) {
                    Some(v) => {
                        p = v as u32;
                        cat = match products.get(pid.unwrap()) {
                            Some(&c) => c as u16,
                            None => u16::MAX,
                        };
                    }
                    None => ok = false,
                }
            }
            if ok && ing - ts > DAY {
                ok = false;
            }
            let mut idx = u32::MAX as u64;
            let mut ush = 0u64;
            if ok {
                let u = uid.unwrap();
                idx = pay.len() as u64;
                let (uh, uh2) = h12_over(u);
                ush = shard_h(uh) as u64;
                ucount[ush as usize] += 1;
                assert!(u.len() <= 0xFFFF && (u.as_ptr() as u64) < (1 << 48), "user_id reference does not pack");
                pay.push(PEv {
                    pk: uh,
                    nref: (u.as_ptr() as u64) | ((u.len() as u64) << 48),
                    pk2: uh2 as u32,
                    ts: (ts - ws) as u32,
                    price: p,
                    cat,
                });
            }
            let (eh, eh2) = h12_over(eid);
            kcount[shard_h(eh)] += 1;
            // ingest_ts is always F1 (whole seconds); +-68 years around the window fits in i32
            let ing_s = (ing.div_euclid(1000) - ws.div_euclid(1000)).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
            keys.push(DKey { eh, eh2: eh2 as u32, ing: ing_s, loc: (ush << 48) | ((ci as u64) << 32) | idx });
        }
        let _ = pad;
        wstart = wend;
    }
    (pay, keys, kcount, ucount)
}



/// Split a line into exactly N fields; None if malformed. One pass over the bytes.
#[inline]
fn fields<const N: usize>(line: B) -> Option<[Option<B>; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [None; N];
    let mut idx = 0;
    let mut start = 0;
    let mut quoted = false;
    // SWAR: find ',' and '"' 8 bytes at a time
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    let n = line.len();
    let mut base = 0;
    while base < n {
        let w = if base + 8 <= n {
            u64::from_le_bytes(line[base..base + 8].try_into().unwrap())
        } else {
            let mut t = [0u8; 8];
            t[..n - base].copy_from_slice(&line[base..]);
            u64::from_le_bytes(t)
        };
        let xc = w ^ (LO * b',' as u64);
        let xq = w ^ (LO * b'"' as u64);
        // exact zero-byte masks (no false positives from borrows)
        let zc = !(((xc & !HI) + !HI) | xc | !HI);
        let zq = !(((xq & !HI) + !HI) | xq | !HI);
        let mut m = zc | zq;
        if base + 8 > n {
            m &= (1u64 << ((n - base) * 8)) - 1;
        }
        while m != 0 {
            let i = base + (m.trailing_zeros() / 8) as usize;
            m &= m - 1;
            if line[i] == b',' {
                if idx >= N - 1 {
                    return None;
                }
                out[idx] = field_q(&line[start..i], quoted).ok()?;
                idx += 1;
                start = i + 1;
                quoted = false;
            } else {
                quoted = true;
            }
        }
        base += 8;
    }
    if idx != N - 1 {
        return None;
    }
    out[N - 1] = field_q(&line[start..], quoted).ok()?;
    Some(out)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
}

/// "YYYY-MM-DD" -> days since epoch, with full calendar validation (None if impossible).
fn date_days(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let (mo, d) = (d2(b, 5)?, d2(b, 8)?);
    if !(1..=12).contains(&mo) {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let md = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(mo - 1) as usize];
    if d < 1 || d > md {
        return None;
    }
    Some(days_from_civil(y, mo, d))
}

thread_local! {
    // direct-mapped cache: exact 10-byte date key -> days (i64::MIN = invalid date)
    static DATES: std::cell::RefCell<[(u64, u16, i64); 64]> = const { std::cell::RefCell::new([(u64::MAX, 0, 0); 64]) };
}

#[inline]
fn cached_days(b: &[u8]) -> Option<i64> {
    let k1 = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let k2 = u16::from_le_bytes(b[8..10].try_into().unwrap());
    let slot = ((k1 ^ ((k2 as u64) << 48)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
    DATES.with(|c| {
        let mut c = c.borrow_mut();
        let e = c[slot];
        if e.0 == k1 && e.1 == k2 {
            return if e.2 == i64::MIN { None } else { Some(e.2) };
        }
        let r = date_days(b);
        c[slot] = (k1, k2, r.unwrap_or(i64::MIN));
        r
    })
}

/// "YYYY-MM-DDTHH:MM:SS" prefix -> epoch ms, validating the calendar.
#[inline]
fn civil(b: &[u8]) -> Option<i64> {
    if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (h, mi, s) = (d2(b, 11)?, d2(b, 14)?, d2(b, 17)?);
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = cached_days(b)?;
    Some(((days * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

thread_local! {
    // one-entry cache: exact "YYYY-MM-" bytes -> (days at day 1, days in month); month 0 = invalid
    static MONTH: std::cell::Cell<(u64, i64, i64)> = const { std::cell::Cell::new((u64::MAX, 0, 0)) };
}

/// Fast exact "YYYY-MM-DDTHH:MM:SS" prefix -> epoch ms (same results as civil()).
#[inline(always)]
fn civil_fast(b: &[u8]) -> Option<i64> {
    // bytes 8..16 = "DDTHH:MM": XOR with the template turns digits into 0..9 and the separators
    // into 0; one check validates all six digits and both separators
    let w1 = u64::from_le_bytes(b[8..16].try_into().unwrap());
    let x = w1 ^ u64::from_le_bytes(*b"00T00:00");
    const SEP: u64 = 0x0000_FF00_00FF_0000; // bytes 2 ('T') and 5 (':')
    if x & SEP != 0 || (x | x.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let by = |k: u32| ((x >> (8 * k)) & 0xFF) as i64;
    let (d, h, mi) = (by(0) * 10 + by(1), by(3) * 10 + by(4), by(6) * 10 + by(7));
    // bytes 16..19 = ":SS"
    if b[16] != b':' {
        return None;
    }
    let s = d2(b, 17)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let key = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let (k, base, mlen) = MONTH.with(|c| c.get());
    let (base, mlen) = if k == key {
        (base, mlen)
    } else {
        let r = (|| {
            if b[4] != b'-' || b[7] != b'-' {
                return None;
            }
            let y = d2(b, 0)? * 100 + d2(b, 2)?;
            let mo = d2(b, 5)?;
            if !(1..=12).contains(&mo) {
                return None;
            }
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            let ml = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(mo - 1) as usize];
            Some((days_from_civil(y, mo, 1), ml))
        })();
        let (bs, ml) = r.unwrap_or((0, 0));
        MONTH.with(|c| c.set((key, bs, ml)));
        (bs, ml)
    };
    if d < 1 || d > mlen {
        return None;
    }
    Some((((base + d - 1) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// Per-chunk cache for F1 decoding: exact "YYYY-MM-" bytes -> (days at day 1, month length).
struct MonthCache {
    key: u64,
    base: i64,
    mlen: i64,
}

impl MonthCache {
    fn new() -> Self {
        MonthCache { key: u64::MAX, base: 0, mlen: 0 }
    }

    /// Exact F1 "YYYY-MM-DDTHH:MM:SSZ" -> epoch ms; anything else -> parse_ts (identical results).
    #[inline(always)]
    fn f1(&mut self, v: Option<B>) -> i64 {
        let Some(b) = v else { return BAD };
        if b.len() != 20 || b[19] != b'Z' {
            return parse_ts(v);
        }
        let w1 = u64::from_le_bytes(b[8..16].try_into().unwrap());
        let x = w1 ^ u64::from_le_bytes(*b"00T00:00");
        const SEP: u64 = 0x0000_FF00_00FF_0000;
        if x & SEP != 0 || (x | x.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080 != 0 || b[16] != b':' {
            return BAD;
        }
        let by = |k: u32| ((x >> (8 * k)) & 0xFF) as i64;
        let (d, h, mi) = (by(0) * 10 + by(1), by(3) * 10 + by(4), by(6) * 10 + by(7));
        let Some(s) = d2(b, 17) else { return BAD };
        if h > 23 || mi > 59 || s > 59 {
            return BAD;
        }
        let key = u64::from_le_bytes(b[0..8].try_into().unwrap());
        if key != self.key {
            let r = (|| {
                if b[4] != b'-' || b[7] != b'-' {
                    return None;
                }
                let y = d2(b, 0)? * 100 + d2(b, 2)?;
                let mo = d2(b, 5)?;
                if !(1..=12).contains(&mo) {
                    return None;
                }
                let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
                let ml = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(mo - 1) as usize];
                Some((days_from_civil(y, mo, 1), ml))
            })();
            let (bs, ml) = r.unwrap_or((0, 0));
            self.key = key;
            self.base = bs;
            self.mlen = ml;
        }
        if d < 1 || d > self.mlen {
            return BAD;
        }
        (((self.base + d - 1) * 24 + h) * 60 + mi) * 60_000 + s * 1000
    }
}

/// 8 ASCII digits (first byte most significant) -> value, if all are '0'..='9'.
#[inline(always)]
fn swar8(w: u64) -> Option<u64> {
    let x = w.wrapping_sub(0x3030_3030_3030_3030);
    // every byte must be 0..=9: no borrow (high nibble 0) and x + 6 stays below 16
    if (x | x.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0 != 0 || w & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let x = (x.wrapping_mul(10)).wrapping_add(x >> 8);
    let x = (((x & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32)))
        .wrapping_add(((x >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    Some(x)
}

/// parse_ts for a field followed by >= 16 readable bytes of the same line: SWAR fast path for
/// 13-digit epoch milliseconds, everything else through parse_ts. Identical results (SELFTEST).
#[inline(always)]
fn parse_ts_over(v: Option<B>) -> i64 {
    if let Some(b) = v {
        if b.len() == 13 {
            // SAFETY: the caller guarantees bytes after the field are readable
            let w0 = unsafe { std::ptr::read_unaligned(b.as_ptr() as *const u64) };
            let w1 = unsafe { std::ptr::read_unaligned(b.as_ptr().add(5) as *const u64) };
            if let (Some(hi), Some(lo)) = (swar8(w0), swar8(w1)) {
                // digits 0..8 and 5..13 overlap; the low five digits come from the second word
                return (hi * 100_000 + lo % 100_000) as i64;
            }
            return parse_ts(v);
        }
    }
    parse_ts(v)
}

/// Fast parse_ts: identical results to parse_ts_ref (fuzz-checked by SELFTEST=1).
#[inline(always)]
fn parse_ts(v: Option<B>) -> i64 {
    let Some(b) = v else { return BAD };
    let n = b.len();
    if n == 20 && b[19] == b'Z' {
        return civil_fast(b).unwrap_or(BAD);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let (Some(base), Some(oh), Some(om)) = (civil_fast(b), d2(b, 20), d2(b, 23)) else { return BAD };
        if oh > 23 || om > 59 {
            return BAD;
        }
        let off = (oh * 60 + om) * 60_000;
        return if b[19] == b'+' { base - off } else { base + off };
    }
    // F3: digits only; leading zeros allowed; more than 18 significant digits cannot fit
    let mut i = 0;
    while i < n && b[i] == b'0' {
        i += 1;
    }
    if n - i > 18 {
        return if b.iter().all(|c| c.is_ascii_digit()) { parse_ts_ref(Some(b)) } else { BAD };
    }
    let mut x: i64 = 0;
    for &c in &b[i..] {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return BAD;
        }
        x = x * 10 + d as i64;
    }
    x
}

#[inline]
fn parse_ts_ref(v: Option<B>) -> i64 {
    let Some(b) = v else { return BAD };
    if b.len() == 20 && b[19] == b'Z' {
        return civil(b).unwrap_or(BAD);
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let (Some(base), Some(oh), Some(om)) = (civil(b), d2(b, 20), d2(b, 23)) else { return BAD };
        if oh > 23 || om > 59 {
            return BAD;
        }
        let off = (oh * 60 + om) * 60_000;
        return if b[19] == b'+' { base - off } else { base + off };
    }
    let mut v: i64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return BAD;
        }
        v = match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) {
            Some(x) => x,
            None => {
                // keep validating that the rest are digits; the value is simply huge
                return if b.iter().all(|c| c.is_ascii_digit()) { HUGE } else { BAD };
            }
        };
    }
    v
}

/// Digits-only price in 1..=10_000_000, else None.
#[inline]
fn parse_price(v: Option<B>) -> Option<i64> {
    let b = v?;
    let mut x: i64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        x = x * 10 + d as i64;
        if x > 10_000_000 {
            // still must be all digits to be "bad price" vs anything else: both drop
            return None;
        }
    }
    if x >= 1 { Some(x) } else { None }
}

// ------------------------------------------------------------------ input
fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
}

/// Map a file read-only (no copy, no allocation); falls back to reading it.
fn map_file(p: &Path) -> B {
    use std::os::fd::AsRawFd;
    let f = std::fs::File::open(p).expect("open");
    let len = f.metadata().expect("stat").len() as usize;
    if len == 0 {
        return &[];
    }
    // PROT_READ=1, MAP_PRIVATE=2, MAP_POPULATE=0x8000
    let ptr = unsafe { mmap(std::ptr::null_mut(), len, 1, 2 | 0x8000, f.as_raw_fd(), 0) };
    if ptr as isize == -1 || ptr.is_null() {
        return Box::leak(std::fs::read(p).expect("read").into_boxed_slice());
    }
    // the mapping outlives the File handle and is never unmapped (process exits)
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

fn load(paths: &[PathBuf]) -> Vec<B> {
    paths.par_iter().map(|p| map_file(p)).collect()
}

/// Data region of each file (after the header line), cut into chunks that end on '\n'.
fn chunks(bufs: &[B]) -> Vec<B> {
    chunks_of(bufs, CHUNK)
}

/// Data regions cut into chunks of about `size` bytes that end on '\n'.
fn chunks_of(bufs: &[B], size: usize) -> Vec<B> {
    let mut out = vec![];
    for &buf in bufs {
        let Some(h) = memchr::memchr(b'\n', buf) else { continue };
        let mut rest = &buf[h + 1..];
        while rest.len() > size {
            let cut = match memchr::memchr(b'\n', &rest[size..]) {
                Some(i) => size + i + 1,
                None => rest.len(),
            };
            out.push(&rest[..cut]);
            rest = &rest[cut..];
        }
        if !rest.is_empty() {
            out.push(rest);
        }
    }
    out
}

/// Iterate the lines of a chunk ('\n' only; a final empty piece is not a line).
#[inline]
fn lines(chunk: B) -> impl Iterator<Item = B> {
    let n = chunk.len();
    let mut start = 0;
    let mut it = memchr::memchr_iter(b'\n', chunk);
    std::iter::from_fn(move || {
        if start >= n {
            return None;
        }
        let end = it.next().unwrap_or(n);
        let line = &chunk[start..end];
        start = end + 1;
        Some(line)
    })
}

fn small_table(path: &Path) -> Vec<(B, B)> {
    let Ok(buf) = std::fs::read(path) else { return vec![] };
    let buf: B = Box::leak(buf.into_boxed_slice());
    let Some(h) = memchr::memchr(b'\n', buf) else { return vec![] };
    lines(&buf[h + 1..])
        .filter_map(|l| fields::<2>(l))
        .filter_map(|[a, b]| Some((a?, b.unwrap_or(b""))))
        .collect()
}

// ------------------------------------------------------------------ records
#[derive(Clone, Copy)]
struct DKey {
    eh: u64,  // h64(event_id); with eh2 a 96-bit identity for dedup
    eh2: u32, // low 32 bits of h2(event_id)
    ing: i32, // ingest_ts in whole seconds relative to window_start (F1 has whole seconds)
    loc: u64, // uid shard << 48 | chunk << 32 | payload index (u32::MAX if the copy is invalid)
}

/// Before person resolution (pk, pk2, name) hold the user_id's hashes and bytes.
type UEv = PEv;

#[derive(Clone, Copy)]
struct UCl {
    uh: u64,
    uref: u64, // user_id: pointer | length << 48
    cref: u64, // click_id: pointer | length << 48
    uh2: u32,
    ts: u32, // ts - (window_start - 7 days)
    camp: u32,
}

/// Pack a slice of the (never unmapped) input mapping into pointer | length << 48.
#[inline(always)]
fn pack_ref(b: B) -> u64 {
    assert!(b.len() <= 0xFFFF && (b.as_ptr() as u64) < (1 << 48), "reference does not pack");
    (b.as_ptr() as u64) | ((b.len() as u64) << 48)
}

#[inline(always)]
fn unpack_ref(r: u64) -> B {
    // SAFETY: references point into input mappings that live for the whole run
    unsafe { std::slice::from_raw_parts((r & 0xFFFF_FFFF_FFFF) as *const u8, (r >> 48) as usize) }
}

#[derive(Clone, Copy)]
struct PEv {
    pk: u64,    // h64(user_id); with pk2 a 96-bit user identity
    nref: u64,  // user_id bytes in the input mapping: pointer (low 48 bits) | length << 48
    pk2: u32,   // low 32 bits of h2(user_id)
    ts: u32,    // ts - window_start (valid events lie inside the window)
    price: u32, // > 0 exactly for purchases
    cat: u16,
}

impl PEv {
    #[inline]
    fn name(&self) -> B {
        // SAFETY: points into an input mapping that is never unmapped
        unsafe { std::slice::from_raw_parts((self.nref & 0xFFFF_FFFF_FFFF) as *const u8, (self.nref >> 48) as usize) }
    }
}

/// Event after person resolution: key = person id << 32 | (ts - window_start).
#[derive(Clone, Copy)]
struct PR {
    key: u64,
    price: u32, // > 0 exactly for purchases
    cat: u32,
}

/// Click after person resolution: key = person id << 32 | (ts - (window_start - 7 days)).
#[derive(Clone, Copy)]
struct CR {
    key: u64,
    cref: u64, // click_id reference (pack_ref)
    camp: u32,
}

#[inline]
fn shard_pid(pid: u32) -> usize {
    ((pid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - SH_BITS)) as usize
}

#[derive(Clone, Copy)]
struct PCl {
    pk: u64,
    pk2: u64,
    name: B,
    ts: i64,
    cid: B,
    camp: u32,
}

/// Sort records by (pk, ts) through packed 128-bit keys (pk | ts-base | index): one integer
/// compare per step and 16-byte moves, then a single gather into order.
fn sorted_by_pk_ts<T: Copy>(v: &[T], base: i64, pk: impl Fn(&T) -> u64, ts: impl Fn(&T) -> i64) -> Vec<T> {
    let mut k: Vec<u128> = v
        .iter()
        .enumerate()
        .map(|(i, x)| ((pk(x) as u128) << 64) | (((ts(x) - base) as u32 as u128) << 32) | i as u128)
        .collect();
    k.sort_unstable();
    k.iter().map(|&x| v[x as u32 as usize]).collect()
}

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

/// Radix partition: every record is written exactly once into one contiguous array,
/// grouped by shard. Returns the array and NSH+1 shard offsets.
fn partition<T: Copy + Send + Sync>(parts: Vec<Vec<T>>, key: impl Fn(&T) -> usize + Sync) -> (Vec<T>, Vec<usize>) {
    let refs: Vec<&[T]> = parts.iter().map(|v| v.as_slice()).collect();
    partition_keep(&refs, |_, _| true, key)
}

/// Radix partition of the records of several parts for which keep(part, index) holds.
fn partition_keep<T: Copy + Send + Sync>(
    parts: &[&[T]],
    keep: impl Fn(usize, usize) -> bool + Sync,
    key: impl Fn(&T) -> usize + Sync,
) -> (Vec<T>, Vec<usize>) {
    partition_counted(parts, None, keep, key)
}

/// Like partition_keep; `known` supplies per-part shard counts already gathered by the producer.
fn partition_counted<T: Copy + Send + Sync>(
    parts: &[&[T]],
    known: Option<Vec<Vec<u32>>>,
    keep: impl Fn(usize, usize) -> bool + Sync,
    key: impl Fn(&T) -> usize + Sync,
) -> (Vec<T>, Vec<usize>) {
    partition_sized(parts, known, 0, keep, key)
}

/// Like partition_counted; the output's allocation is at least `min_bytes` so that a later
/// stage can reuse the same (already faulted-in) block through the recycling allocator.
fn partition_sized<T: Copy + Send + Sync>(
    parts: &[&[T]],
    known: Option<Vec<Vec<u32>>>,
    min_bytes: usize,
    keep: impl Fn(usize, usize) -> bool + Sync,
    key: impl Fn(&T) -> usize + Sync,
) -> (Vec<T>, Vec<usize>) {
    let counts: Vec<Vec<usize>> = if let Some(k) = known {
        k.into_iter().map(|v| v.into_iter().map(|x| x as usize).collect()).collect()
    } else {
        parts
        .par_iter()
        .enumerate()
        .map(|(pi, v)| {
            let mut c = vec![0usize; NSH];
            for (i, x) in v.iter().enumerate() {
                if keep(pi, i) {
                    c[key(x)] += 1;
                }
            }
            c
        })
        .collect()
    };
    let mut starts: Vec<Vec<usize>> = vec![vec![0; NSH]; parts.len()];
    let mut off = vec![0usize; NSH + 1];
    let mut run = 0;
    for sh in 0..NSH {
        off[sh] = run;
        for c in 0..parts.len() {
            starts[c][sh] = run;
            run += counts[c][sh];
        }
    }
    off[NSH] = run;
    let mut out: Vec<T> = Vec::with_capacity(run.max(min_bytes.div_ceil(std::mem::size_of::<T>().max(1))));
    let ptr = SendPtr(out.as_mut_ptr());
    let ptr = &ptr;
    // software write-combining: stage each shard's elements in a ~256 B buffer (all buffers
    // together ~256 KB, L2-resident) and flush full buffers as one contiguous burst, so the
    // 1024-way scatter costs one TLB/cache miss per burst instead of one per element
    let e = (256 / std::mem::size_of::<T>().max(1)).max(1);
    parts.par_iter().zip(starts.into_par_iter()).enumerate().for_each_init(
        || (Vec::<T>::with_capacity(NSH * e), vec![0u16; NSH]),
        |(buf, fill), (pi, (v, mut pos))| {
            let bp = buf.as_mut_ptr();
            for (i, x) in v.iter().enumerate() {
                if !keep(pi, i) {
                    continue;
                }
                let k = key(x);
                let f = fill[k] as usize;
                // SAFETY: k*e + f < NSH*e = buf capacity (T: Copy, slots are only read after a write)
                unsafe { bp.add(k * e + f).write(*x) };
                if f + 1 == e {
                    // SAFETY: each (chunk, shard) owns the disjoint range [starts, starts+count)
                    unsafe { std::ptr::copy_nonoverlapping(bp.add(k * e), ptr.0.add(pos[k]), e) };
                    pos[k] += e;
                    fill[k] = 0;
                } else {
                    fill[k] = (f + 1) as u16;
                }
            }
            for k in 0..NSH {
                let f = fill[k] as usize;
                if f > 0 {
                    // SAFETY: as above; the tail fits the shard's remaining range
                    unsafe { std::ptr::copy_nonoverlapping(bp.add(k * e), ptr.0.add(pos[k]), f) };
                    pos[k] += f;
                    fill[k] = 0;
                }
            }
        },
    );
    // SAFETY: all `run` slots were written above
    unsafe { out.set_len(run) };
    (out, off)
}

/// Split a partitioned array into disjoint mutable shard slices.
fn shard_slices<'a, T>(mut all: &'a mut [T], off: &[usize]) -> Vec<&'a mut [T]> {
    let mut v = Vec::with_capacity(NSH);
    for sh in 0..NSH {
        let (a, b) = all.split_at_mut(off[sh + 1] - off[sh]);
        v.push(a);
        all = b;
    }
    v
}

/// per_chunk[c][s] -> shards[s] as a list of the chunks' vectors (no copying).
fn transpose<T: Send>(per_chunk: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut cols: Vec<Vec<Vec<T>>> = (0..NSH).map(|_| Vec::with_capacity(per_chunk.len())).collect();
    for chunk in per_chunk {
        for (s, v) in chunk.into_iter().enumerate() {
            if !v.is_empty() {
                cols[s].push(v);
            }
        }
    }
    cols
}

fn scatter<T: Send>(per_chunk: Vec<Vec<Vec<T>>>) -> Vec<Vec<T>> {
    // per_chunk[c][s] -> shards[s]
    let mut cols: Vec<Vec<Vec<T>>> = (0..NSH).map(|_| Vec::with_capacity(per_chunk.len())).collect();
    for chunk in per_chunk {
        for (s, v) in chunk.into_iter().enumerate() {
            cols[s].push(v);
        }
    }
    cols.into_par_iter()
        .map(|vs| {
            let mut out = Vec::with_capacity(vs.iter().map(|v| v.len()).sum());
            for v in vs {
                out.extend(v);
            }
            out
        })
        .collect()
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let (data, out) = if args.len() >= 3 {
        (PathBuf::from(&args[1]), PathBuf::from(&args[2]))
    } else {
        (PathBuf::from(env("DATA_DIR", "/data")), PathBuf::from(env("OUTPUT_DIR", "/output")))
    };
    let ncpu: usize = env("NCPU", "0").parse().unwrap_or(0);
    let ncpu = if ncpu == 0 { std::thread::available_parallelism().map_or(8, |n| n.get()) } else { ncpu };
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();
    let lap = |what: &str| eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());

    // manifest (tiny JSON): pull the two ISO strings out
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let grab = |key: &str| -> i64 {
        let i = man.find(key).expect("key") + key.len();
        let s = &man[i..];
        let a = s.find('"').unwrap() + 1;
        let s = &s[a..];
        let b = s.find('"').unwrap();
        let ts = parse_ts(Some(Box::leak(s[..b].as_bytes().to_vec().into_boxed_slice())));
        assert!(ts != BAD, "manifest timestamp");
        ts
    };
    let (ws, we) = (grab("\"window_start\""), grab("\"window_end\""));
    for t in [&b""[..], b"a", b"u1", b"e42huhoug", b"0123456789abcdefXYZ"] {
        assert_eq!(h12(t), (h64(t), h2(t)), "hash self-check");
    }

    // read everything in parallel
    let (ev_files, cl_files, ln_files) =
        (csv_files(&data.join("events")), csv_files(&data.join("clicks")), csv_files(&data.join("links")));
    let ev_bufs = load(&ev_files);
    let cl_bufs = load(&cl_files);
    let ln_bufs = load(&ln_files);
    lap("read");

    // products / campaigns
    let mut cat_names: Vec<B> = vec![];
    let mut cat_idx: FxHashMap<B, u32> = FxHashMap::default();
    let mut products: FxHashMap<B, u32> = FxHashMap::default();
    for (pid, cat) in small_table(&data.join("products.csv")) {
        let n = cat_names.len() as u32;
        let ci = *cat_idx.entry(cat).or_insert_with(|| {
            cat_names.push(cat);
            n
        });
        products.insert(pid, ci);
    }
    assert!(cat_names.len() < u16::MAX as usize, "more than 65534 product categories");
    let mut camp_ids: Vec<B> = vec![];
    let mut camp_chan: Vec<B> = vec![];
    let mut campaigns: FxHashMap<B, u32> = FxHashMap::default();
    for (cid, ch) in small_table(&data.join("campaigns.csv")) {
        campaigns.insert(cid, camp_ids.len() as u32);
        camp_ids.push(cid);
        camp_chan.push(ch);
    }

    // links run on their own thread, overlapping event parsing and dedup (nothing there needs them)
    let ln_bufs2 = ln_bufs.clone();
    let links_job = std::thread::spawn(move || {
    // links: union-find, root kept as byte-wise smallest ID
    let pairs: Vec<(B, B)> = chunks(&ln_bufs2)
        .iter()
        .flat_map(|&c| {
            lines(c).filter_map(|l| match fields::<2>(l) {
                Some([Some(a), Some(b)]) => Some((a, b)),
                _ => None,
            })
        })
        .collect();
    // intern link IDs by 64-bit hash; if two different IDs ever share a hash, switch to exact string interning
    struct Intern {
        ih: FxHashMap<u64, u32>,
        names: Vec<B>,
        hashes: Vec<u64>,
        parent: Vec<u32>,
        clash: bool,
        slow: FxHashMap<B, u32>,
    }
    impl Intern {
        fn new_id(&mut self, s: B, h: u64) -> u32 {
            let i = self.names.len() as u32;
            self.names.push(s);
            self.hashes.push(h);
            self.parent.push(i);
            if self.clash {
                self.slow.insert(s, i);
            }
            i
        }
        fn id(&mut self, s: B) -> u32 {
            let h = h64(s);
            if self.clash {
                if let Some(&i) = self.slow.get(s) {
                    return i;
                }
            }
            match self.ih.get(&h) {
                None => {
                    let i = self.new_id(s, h);
                    self.ih.insert(h, i);
                    i
                }
                Some(&i) if self.names[i as usize] == s => i,
                Some(_) => {
                    if !self.clash {
                        self.clash = true;
                        self.slow = self.names.iter().enumerate().map(|(i, &n)| (n, i as u32)).collect();
                    }
                    match self.slow.get(s) {
                        Some(&j) => j,
                        None => self.new_id(s, h),
                    }
                }
            }
        }
        fn get(&self, s: B, h: u64) -> Option<u32> {
            if self.clash {
                return self.slow.get(s).copied();
            }
            self.ih.get(&h).copied().filter(|&i| self.names[i as usize] == s)
        }
        fn find(&mut self, mut x: u32) -> u32 {
            let p = &mut self.parent;
            let mut r = x;
            while p[r as usize] != r {
                r = p[r as usize];
            }
            while p[x as usize] != r {
                let n = p[x as usize];
                p[x as usize] = r;
                x = n;
            }
            r
        }
    }
    let mut it = Intern {
        ih: FxHashMap::default(),
        names: vec![],
        hashes: vec![],
        parent: vec![],
        clash: false,
        slow: FxHashMap::default(),
    };
    it.ih.reserve(pairs.len());
    for &(a, b) in &pairs {
        let (ia, ib) = (it.id(a), it.id(b));
        let (ra, rb) = (it.find(ia), it.find(ib));
        if ra != rb {
            let (lo, hi) = if it.names[ra as usize] <= it.names[rb as usize] { (ra, rb) } else { (rb, ra) };
            it.parent[hi as usize] = lo;
        }
    }
    let rootv: Vec<u32> = (0..it.names.len() as u32).map(|i| it.find(i)).collect();
    let it = it;
    // linked uid h64 -> (uid h2, root h64, root h2, root index): one cache miss per lookup
    let mut lk: FxHashMap<u64, (u64, u32)> = FxHashMap::default();
    lk.reserve(it.names.len());
    let mut lk_clash = it.clash;
    for (i, &n) in it.names.iter().enumerate() {
        let r = rootv[i] as usize;
        let v = (h2(n), r as u32);
        if lk.insert(it.hashes[i], v).is_some() {
            lk_clash = true; // two linked IDs share a 64-bit hash: use the exact path
        }
    }
    // the same table split by uid-hash shard, so each shard's lookups stay in cache
    let lks: Vec<FxHashMap<u64, (u64, u32)>> = {
        let mut parts: Vec<Vec<(u64, (u64, u32))>> = (0..NSH).map(|_| Vec::new()).collect();
        for (&k, &v) in &lk {
            parts[shard_h(k)].push((k, v));
        }
        parts.into_iter().map(|v| v.into_iter().collect()).collect()
    };
    drop(lk);
        (it, rootv, lk_clash, lks)
    });

    // events: parse + validate each copy, scatter by event_id
    let ev_chunks = chunks(&ev_bufs);
    if std::env::var("SELFTEST").is_ok() {
        let mut seed: u64 = 0x1234_5678_9ABC_DEF0;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = b"0123456789-+:TZtz .";
        let templates: [&[u8]; 6] = [
            b"2026-10-03T23:58:10Z",
            b"2028-02-29T00:00:00Z",
            b"2026-10-03T19:58:10-04:00",
            b"1791071890000",
            b"0001791071890000",
            b"2027-12-31T23:59:59+23:59",
        ];
        let mut checked = 0u64;
        for _ in 0..3_000_000 {
            let mut t: Vec<u8> = templates[(rnd() % 6) as usize].to_vec();
            // mutate 0-3 random positions, sometimes change length
            for _ in 0..(rnd() % 4) {
                let i = (rnd() as usize) % t.len();
                t[i] = alphabet[(rnd() as usize) % alphabet.len()];
            }
            if rnd() % 10 == 0 {
                t.truncate((rnd() as usize) % t.len() + 1);
            }
            if rnd() % 20 == 0 {
                let k = (rnd() % 12) as usize;
                t = std::iter::repeat_n(b'0', k).chain(t).collect();
            }
            let tb: &'static [u8] = Box::leak(t.into_boxed_slice());
            assert_eq!(parse_ts(Some(tb)), parse_ts_ref(Some(tb)), "mismatch on {:?}", String::from_utf8_lossy(tb));
            checked += 1;
        }
        // every calendar day across leap and non-leap years, at both ends of the day
        for y in [1999i64, 2000, 2024, 2025, 2026, 2027, 2028, 2100] {
            for mo in 1..=13 {
                for d in 0..=32 {
                    for hms in ["00:00:00", "23:59:59", "24:00:00", "12:60:00"] {
                        let t = format!("{y:04}-{mo:02}-{d:02}T{hms}Z");
                        let tb: &'static [u8] = Box::leak(t.into_bytes().into_boxed_slice());
                        assert_eq!(parse_ts(Some(tb)), parse_ts_ref(Some(tb)), "mismatch on {:?}", String::from_utf8_lossy(tb));
                        checked += 1;
                    }
                }
            }
        }
        for _ in 0..2_000_000 {
            let mut t: Vec<u8> = (0..13).map(|_| b"0123456789"[(rnd() % 10) as usize]).collect();
            if rnd() % 5 == 0 {
                let i = (rnd() as usize) % 13;
                t[i] = b"0123456789-+:TZtz .\xff/"[(rnd() as usize) % 21];
            }
            let mut padded = t.clone();
            padded.extend_from_slice(b",2026-10-03T23:58:10Z,view");
            let pb: &'static [u8] = Box::leak(padded.into_boxed_slice());
            let tb = &pb[..13];
            assert_eq!(parse_ts_over(Some(tb)), parse_ts_ref(Some(tb)), "mismatch on {:?}", String::from_utf8_lossy(tb));
            checked += 1;
        }
        let mut mc = MonthCache::new();
        for _ in 0..3_000_000 {
            let mut t: Vec<u8> = templates[(rnd() % 6) as usize].to_vec();
            for _ in 0..(rnd() % 3) {
                let i = (rnd() as usize) % t.len();
                t[i] = alphabet[(rnd() as usize) % alphabet.len()];
            }
            let tb: &'static [u8] = Box::leak(t.into_boxed_slice());
            assert_eq!(mc.f1(Some(tb)), parse_ts_ref(Some(tb)), "f1 mismatch on {:?}", String::from_utf8_lossy(tb));
            checked += 1;
        }
        eprintln!("SELFTEST ok: {checked} timestamps identical");
        // scanners must agree on every line (all event, click and link chunks, plus synthetic lines)
        let synth: &'static [u8] = Box::leak(
            br#"a,b,c,d,e,f,g

"a","",c,d,e,f,g
x,"y"z,c,d,e,f,g
""",b,c,d,e,f,g
a,b,c,d,e,f
a,b,c,d,e,f,g,h
"
,,,,,,
a,b,c,d,e,f,"g"
last,b,c,d,e,f,g"#
                .to_vec()
                .into_boxed_slice(),
        );
        let mut lines_checked = 0u64;
        let mut bad = 0u64;
        let all: Vec<B> = chunks(&ev_bufs).into_iter().chain(chunks(&cl_bufs)).chain(chunks(&ln_bufs)).chain([synth]).collect();
        for c in all {
            for len_cut in [c.len(), c.len().saturating_sub(1), c.len().saturating_sub(37)] {
                let c = &c[..len_cut];
                let (mut a7, mut b7): (Vec<_>, Vec<_>) = (vec![], vec![]);
                scan_lines::<7>(c, |r| a7.push(r));
                scan_lines2::<7>(c, |r| b7.push(r));
                let (mut a4, mut b4): (Vec<_>, Vec<_>) = (vec![], vec![]);
                scan_lines::<4>(c, |r| a4.push(r));
                scan_lines2::<4>(c, |r| b4.push(r));
                lines_checked += a7.len() as u64;
                if a7 != b7 || a4 != b4 {
                    bad += 1;
                }
            }
        }
        assert_eq!(bad, 0, "scanner mismatch");
        eprintln!("SELFTEST ok: scanners agree on {lines_checked} lines (x2 widths, 3 truncations per chunk)");
        let mut hashed = 0u64;
        for c in chunks(&ev_bufs) {
            scan_lines::<7>(c, |r| {
                if let Some(f) = r {
                    for x in [f[0], f[1]].into_iter().flatten() {
                        assert_eq!(h12_over(x), h12(x), "hash mismatch");
                        hashed += 1;
                    }
                }
            });
        }
        eprintln!("SELFTEST ok: fast hash identical on {hashed} ids");
        let prods: FxHashMap<B, u32> = small_table(&data.join("products.csv"))
            .into_iter()
            .enumerate()
            .map(|(i, (p, _))| (p, i as u32 % 7))
            .collect();
        let mut cks = 0u64;
        for (ci, c) in chunks(&ev_bufs).into_iter().chain([synth]).enumerate() {
            for cut in [c.len(), c.len().saturating_sub(1), c.len().saturating_sub(29)] {
                let c: &'static [u8] = &c[..cut];
                let (a1, b1, k1) = parse_event_chunk(c, ci, ws, we, &prods);
                let (a3, b3, k3) = parse_event_chunk3(c, ci, ws, we, &prods);
                let (a4, b4, k4, _) = parse_event_chunk4(c, ci, ws, we, &prods);
                assert!(
                    a4.len() == a3.len() && b4.len() == b3.len() && k4 == k3
                        && a4.iter().zip(&a3).all(|(x, y)| x.pk == y.pk && x.nref == y.nref && x.pk2 == y.pk2 && x.ts == y.ts && x.price == y.price && x.cat == y.cat)
                        && b4.iter().zip(&b3).all(|(x, y)| x.eh == y.eh && x.eh2 == y.eh2 && x.ing == y.ing && x.loc == y.loc),
                    "parse_event_chunk4 differs on chunk {ci} cut {cut}"
                );
                let same = a1.len() == a3.len()
                    && b1.len() == b3.len()
                    && k1 == k3
                    && a1.iter().zip(&a3).all(|(x, y)| x.pk == y.pk && x.nref == y.nref && x.pk2 == y.pk2 && x.ts == y.ts && x.price == y.price && x.cat == y.cat)
                    && b1.iter().zip(&b3).all(|(x, y)| x.eh == y.eh && x.eh2 == y.eh2 && x.ing == y.ing && x.loc == y.loc);
                assert!(same, "parse_event_chunk3 differs on chunk {ci} cut {cut}");
                cks += 1;
            }
        }
        eprintln!("SELFTEST ok: two-stage parser identical on {cks} chunk variants");
        std::process::exit(0);
    }
    if let Ok(level) = std::env::var("PARSEBENCH") {
        // single-threaded cost ladder over all event lines; prints ns/line per level
        let level: u32 = level.parse().unwrap();
        let ev_chunks: Vec<B> = if std::env::var("COPYIN").is_ok() {
            // same bytes, but in anonymous (huge-page eligible) heap memory instead of the file mapping
            ev_chunks.iter().map(|c| &*Box::leak(c.to_vec().into_boxed_slice())).collect()
        } else {
            ev_chunks.clone()
        };
        let t = Instant::now();
        let (mut n, mut acc) = (0u64, 0u64);
        let mut pay: Vec<(u64, u64, i64, i64)> = Vec::with_capacity(1 << 20);
        if level >= 5 {
            for &c in &ev_chunks {
                scan_lines::<7>(c, |r| {
                    n += 1;
                    if let Some([eid, uid, ts, ing, _, _, _]) = r {
                        acc += eid.map_or(0, |x| x.len()) as u64;
                        if level >= 6 {
                            acc = acc.wrapping_add((parse_ts(ts) ^ parse_ts(ing)) as u64);
                        }
                        if level >= 7 {
                            acc = acc.wrapping_add(h12(eid.unwrap_or(b"")).0 ^ h12(uid.unwrap_or(b"")).1);
                        }
                    }
                });
            }
            let el = t.elapsed().as_secs_f64();
            eprintln!("PARSEBENCH level {level}: {n} lines, {:.1} ns/line (acc {acc})", el * 1e9 / n as f64);
            std::process::exit(0);
        }
        for &c in &ev_chunks {
            for l in lines(c) {
                n += 1;
                if level == 0 {
                    acc += l.len() as u64;
                    continue;
                }
                let Some([eid, uid, ts, ing, et, _pid, _price]) = fields::<7>(l) else { continue };
                if level == 1 {
                    acc += eid.map_or(0, |x| x.len()) as u64 + et.map_or(0, |x| x[0]) as u64;
                    continue;
                }
                let (tsv, ingv) = (parse_ts(ts), parse_ts(ing));
                if level == 2 {
                    acc = acc.wrapping_add(tsv as u64 ^ ingv as u64);
                    continue;
                }
                let (a, b) = h12(eid.unwrap_or(b""));
                let (c2, d) = h12(uid.unwrap_or(b""));
                if level == 3 {
                    acc = acc.wrapping_add(a ^ b ^ c2 ^ d ^ tsv as u64);
                    continue;
                }
                pay.push((a, c2, tsv, ingv));
                if pay.len() == pay.capacity() {
                    acc += pay.len() as u64;
                    pay.clear();
                }
                let _ = (b, d);
            }
        }
        let el = t.elapsed().as_secs_f64();
        eprintln!("PARSEBENCH level {level}: {n} lines, {:.1} ns/line, {:.2}s (acc {acc})", el * 1e9 / n as f64, el);
        std::process::exit(0);
    }
    let per_chunk: Vec<(Vec<UEv>, Vec<DKey>, Vec<u32>, Vec<u32>)> = ev_chunks
        .par_iter()
        .enumerate()
        .map(|(ci, &c)| parse_event_chunk4(c, ci, ws, we, &products))
        .collect();
    lap("  parse lines");
    let mut pays: Vec<Vec<UEv>> = Vec::with_capacity(per_chunk.len());
    let mut keys: Vec<Vec<DKey>> = Vec::with_capacity(per_chunk.len());
    let mut kcounts: Vec<Vec<u32>> = Vec::with_capacity(per_chunk.len());
    let mut ucounts: Vec<Vec<u32>> = Vec::with_capacity(per_chunk.len());
    for (a, b, c, d) in per_chunk {
        pays.push(a);
        keys.push(b);
        kcounts.push(c);
        ucounts.push(d);
    }
    let key_refs: Vec<&[DKey]> = keys.iter().map(|v| v.as_slice()).collect();
    let pay_bytes: usize = pays.iter().map(|v| v.len()).sum::<usize>() * std::mem::size_of::<UEv>();
    let (key_all, key_off) = partition_sized(&key_refs, Some(kcounts), pay_bytes, |_, _| true, |k: &DKey| shard_h(k.eh));
    drop(key_refs);
    drop(keys);
    lap("events parsed");

    // dedup within each event shard, keep surviving events, re-scatter by person
    lap("  pre-bitmaps");
    // winner bitmaps, one per chunk, over that chunk's valid payloads. Every valid payload starts
    // as a winner; dedup clears only the losers (a few % of events), so atomics are rare.
    let wcount: Vec<Vec<std::sync::atomic::AtomicU32>> =
        ucounts.into_iter().map(|v| v.into_iter().map(std::sync::atomic::AtomicU32::new).collect()).collect();
    let bits: Vec<Vec<std::sync::atomic::AtomicU64>> =
        pays.iter().map(|v| (0..v.len() / 64 + 1).map(|_| std::sync::atomic::AtomicU64::new(u64::MAX)).collect()).collect();
    let lose = |loc: u64| {
        let idx = (loc & 0xFFFF_FFFF) as usize;
        if idx != u32::MAX as usize {
            let chunk = ((loc >> 32) & 0xFFFF) as usize;
            bits[chunk][idx / 64].fetch_and(!(1u64 << (idx % 64)), std::sync::atomic::Ordering::Relaxed);
            wcount[chunk][(loc >> 48) as usize].fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    };
    // per-thread open-addressing table (reused across shards): one u64 per slot holding the
    // hash's high 40 bits and (index + 1) in the low 24 bits; ~62% load keeps it L2-resident
    (0..NSH).into_par_iter().for_each_init(
        || Vec::<u64>::new(),
        |tab, shi| {
            let ks = &key_all[key_off[shi]..key_off[shi + 1]];
            if ks.is_empty() {
                return;
            }
            assert!(ks.len() < (1 << 24) - 1, "dedup shard too large for 24-bit slot index");
            let cap = (ks.len() + ks.len() / 4 + 1).next_power_of_two();
            let mask = cap - 1;
            const LO: u64 = (1 << 24) - 1;
            tab.clear();
            tab.resize(cap, 0);
            for (i, k) in ks.iter().enumerate() {
                let hi = k.eh & !LO;
                let mut slot = (k.eh.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & mask;
                loop {
                    let t = tab[slot];
                    if t == 0 {
                        tab[slot] = hi | (i as u64 + 1);
                        break;
                    }
                    if t & !LO == hi {
                        let j = (t & LO) as usize - 1;
                        if ks[j].eh == k.eh && ks[j].eh2 == k.eh2 {
                            // same event: keep the earliest ingest; on a tie the earlier copy (file order) stays
                            if k.ing < ks[j].ing {
                                tab[slot] = hi | (i as u64 + 1);
                                lose(ks[j].loc);
                            } else {
                                lose(k.loc);
                            }
                            break;
                        }
                    }
                    slot = (slot + 1) & mask;
                }
            }
        },
    );
    lap("  dedup table");
    drop(key_all);
    lap("  drop keys");
    lap("events deduped");
    let pay_refs: Vec<&[UEv]> = pays.iter().map(|v| v.as_slice()).collect();
    let is_winner = |c: usize, i: usize| bits[c][i / 64].load(std::sync::atomic::Ordering::Relaxed) >> (i % 64) & 1 == 1;
    let wc: Vec<Vec<u32>> = wcount.into_iter().map(|v| v.into_iter().map(|a| a.into_inner()).collect()).collect();
    let (mut uev_all, uev_off) = partition_counted(&pay_refs, Some(wc), is_winner, |e: &UEv| shard_h(e.pk));
    drop(pay_refs);
    drop(pays);
    // clicks: copies are identical, so duplicates cannot change credit or assists
    // ~3.5 MB click chunks: their records (64 B per ~46 B line) fit inside the freed per-chunk
    // event-payload blocks, which the recycling allocator then reuses instead of fresh memory
    let cl_chunks = chunks_of(&cl_bufs, 7 << 19);
    let per_chunk: Vec<(Vec<UCl>, Vec<u32>)> = cl_chunks
        .par_iter()
        .map(|&c| {
            let mut sh: Vec<UCl> = Vec::with_capacity(c.len() / 40 + 16);
            let mut cc = vec![0u32; NSH];
            scan_lines2::<4>(c, |r| {
                let Some([Some(cid), Some(uid), Some(camp), ts]) = r else { return };
                let Some(&camp) = campaigns.get(camp) else { return };
                let ts = parse_ts(ts);
                if ts == BAD || ts < ws - WEEK || ts >= we {
                    return;
                }
                let (uh, uh2) = h12(uid);
                cc[shard_h(uh)] += 1;
                sh.push(UCl { uh, uref: pack_ref(uid), cref: pack_ref(cid), uh2: uh2 as u32, ts: (ts - (ws - WEEK)) as u32, camp });
            });
            (sh, cc)
        })
        .collect();
    let (ucls, ccounts): (Vec<Vec<UCl>>, Vec<Vec<u32>>) = per_chunk.into_iter().unzip();
    let ucl_refs: Vec<&[UCl]> = ucls.iter().map(|v| v.as_slice()).collect();
    let (ucl_all, ucl_off) = partition_counted(&ucl_refs, Some(ccounts), |_, _| true, |c: &UCl| shard_h(c.uh));
    drop(ucl_refs);
    drop(ucls);
    lap("clicks parsed");
    let (it, rootv, lk_clash, lks) = links_job.join().expect("links thread");
    lap("links joined");
    // Resolve users to exact person ids, one uid-hash shard at a time (its tables fit in cache).
    // Linked user -> root index (< 2^31). Unlinked user -> 1<<31 | shard<<22 | local index.
    let resolved: Vec<(Vec<PR>, Vec<CR>, Vec<B>, Vec<u32>, Vec<u32>)> = (0..NSH)
        .into_par_iter()
        .map(|shi| {
            let lkm = &lks[shi];
            // one table per shard: uid h64 -> (uid h2, pid), pre-filled with the shard's linked
            // users and extended with unlinked ones as they appear (one lookup per record)
            let mut tab: FxHashMap<u64, (u64, u32)> = FxHashMap::default();
            if !lk_clash {
                tab.reserve(lkm.len() * 2 + 1024);
                for (&k, &v) in lkm.iter() {
                    tab.insert(k, v);
                }
            }
            let mut side: FxHashMap<(u64, u64), u32> = FxHashMap::default(); // 64-bit hash clashes
            let mut unl_names: Vec<B> = Vec::new();
            let mut new_unlinked = |u: B, unl_names: &mut Vec<B>| -> u32 {
                let local = unl_names.len() as u32;
                unl_names.push(u);
                assert!(local < (1 << (31 - SH_BITS)), "too many unlinked users in one shard");
                (1u32 << 31) | ((shi as u32) << (31 - SH_BITS)) | local
            };
            let mut pid_of = |u: B, uh: u64, u2: u64| -> u32 {
                let u2 = u2 as u32 as u64; // 96-bit user identity: h64 + low 32 bits of h2
                if lk_clash {
                    if let Some(i) = it.get(u, uh) {
                        return rootv[i as usize];
                    }
                    if let Some(&p) = side.get(&(uh, u2)) {
                        return p;
                    }
                    let p = new_unlinked(u, &mut unl_names);
                    side.insert((uh, u2), p);
                    return p;
                }
                match tab.entry(uh) {
                    std::collections::hash_map::Entry::Occupied(o) => {
                        let (t2, p) = *o.get();
                        if t2 as u32 as u64 == u2 {
                            return p;
                        }
                        // same 64-bit hash, different user: exact side table
                        if let Some(&p) = side.get(&(uh, u2)) {
                            return p;
                        }
                        let p = new_unlinked(u, &mut unl_names);
                        side.insert((uh, u2), p);
                        p
                    }
                    std::collections::hash_map::Entry::Vacant(v) => {
                        let p = new_unlinked(u, &mut unl_names);
                        v.insert((u2, p));
                        p
                    }
                }
            };
            let evs = &uev_all[uev_off[shi]..uev_off[shi + 1]];
            let prs: Vec<PR> = evs
                .iter()
                .map(|e| PR {
                    key: ((pid_of(e.name(), e.pk, e.pk2 as u64) as u64) << 32) | e.ts as u64,
                    price: e.price,
                    cat: if e.cat == u16::MAX { UNKNOWN_CAT } else { e.cat as u32 },
                })
                .collect();
            let cls = &ucl_all[ucl_off[shi]..ucl_off[shi + 1]];
            let crs: Vec<CR> = cls
                .iter()
                .map(|c| CR {
                    key: ((pid_of(unpack_ref(c.uref), c.uh, c.uh2 as u64) as u64) << 32) | c.ts as u64,
                    camp: c.camp,
                    cref: c.cref,
                })
                .collect();
            let mut pc = vec![0u32; NSH];
            for r in &prs {
                pc[shard_pid((r.key >> 32) as u32)] += 1;
            }
            let mut cc = vec![0u32; NSH];
            for c in &crs {
                cc[shard_pid((c.key >> 32) as u32)] += 1;
            }
            (prs, crs, unl_names, pc, cc)
        })
        .collect();
    drop(uev_all);
    drop(ucl_all);
    let mut prs_parts = Vec::with_capacity(NSH);
    let mut crs_parts = Vec::with_capacity(NSH);
    let mut unl_names_by_shard: Vec<Vec<B>> = Vec::with_capacity(NSH);
    let mut pcs = Vec::with_capacity(NSH);
    let mut ccs = Vec::with_capacity(NSH);
    for (a, b, c, d, e) in resolved {
        prs_parts.push(a);
        crs_parts.push(b);
        unl_names_by_shard.push(c);
        pcs.push(d);
        ccs.push(e);
    }
    drop(pcs);
    drop(ccs);
    // Dense person ids: linked people keep their root index (< R); unlinked users get
    // R + (their uid shard's offset) + local index. Person shards own contiguous id ranges,
    // so sessionizing can index people with arithmetic instead of hashing.
    let r_count = it.names.len() as u32;
    let mut uprefix = vec![0u32; NSH];
    let mut run = r_count;
    for sh in 0..NSH {
        uprefix[sh] = run;
        run += unl_names_by_shard[sh].len() as u32;
    }
    let total_p = run.max(1);
    let span = (total_p as usize).div_ceil(NSH) as u32;
    let dense = |pid: u32| -> u32 {
        if pid < (1 << 31) {
            pid
        } else {
            uprefix[((pid >> (31 - SH_BITS)) as usize) & (NSH - 1)] + (pid & ((1 << (31 - SH_BITS)) - 1))
        }
    };
    let pshard = |d: u32| -> usize { (d / span) as usize };
    let pcs: Vec<Vec<u32>> = prs_parts
        .par_iter_mut()
        .map(|v| {
            let mut c = vec![0u32; NSH];
            for r in v.iter_mut() {
                let d = dense((r.key >> 32) as u32);
                r.key = ((d as u64) << 32) | (r.key & 0xFFFF_FFFF);
                c[pshard(d)] += 1;
            }
            c
        })
        .collect();
    let ccs: Vec<Vec<u32>> = crs_parts
        .par_iter_mut()
        .map(|v| {
            let mut c = vec![0u32; NSH];
            for x in v.iter_mut() {
                let d = dense((x.key >> 32) as u32);
                x.key = ((d as u64) << 32) | (x.key & 0xFFFF_FFFF);
                c[pshard(d)] += 1;
            }
            c
        })
        .collect();
    let unl_flat: Vec<B> = unl_names_by_shard.into_iter().flatten().collect();
    let name_of_pid = |d: u32| -> B { if d < r_count { it.names[d as usize] } else { unl_flat[(d - r_count) as usize] } };
    let (mut pev_all, pev_off) = {
        let refs: Vec<&[PR]> = prs_parts.iter().map(|v| v.as_slice()).collect();
        partition_counted(&refs, Some(pcs), |_, _| true, |r: &PR| pshard((r.key >> 32) as u32))
    };
    drop(prs_parts);
    let (mut pcl_all, pcl_off) = {
        let refs: Vec<&[CR]> = crs_parts.iter().map(|v| v.as_slice()).collect();
        partition_counted(&refs, Some(ccs), |_, _| true, |c: &CR| pshard((c.key >> 32) as u32))
    };
    drop(crs_parts);
    lap("events regrouped");
    // per person-shard: sessionize, attribute, aggregate
    struct URow {
        name: B,
        day: i32,
        sessions: i64,
        events: i64,
        revenue: i64,
        top: Option<u32>,
    }
    let pev_slices = shard_slices(&mut pev_all, &pev_off);
    let pcl_slices = shard_slices(&mut pcl_all, &pcl_off);
    // dense campaign x day accumulators (sessions start inside the window, so days are bounded)
    let day0 = ws.div_euclid(DAY) as i32;
    let ndays = ((we - 1).div_euclid(DAY) as i32 - day0 + 1) as usize;
    let ncamp = camp_ids.len() + 1; // last slot = organic
    let cidx = |c: u32, day: i32| -> usize {
        let c = if c == ORGANIC { ncamp - 1 } else { c as usize };
        c * ndays + (day - day0) as usize
    };
    let cbase = ws - WEEK;
    let results: Vec<(Vec<URow>, Vec<[i64; 4]>)> = pev_slices
        .into_par_iter()
        .zip(pcl_slices.into_par_iter())
        .enumerate()
        .fold(|| (Vec::<URow>::new(), vec![[0i64; 4]; ncamp * ndays]), |(mut urows, mut camp), (sh, (evs, cls))| {
            // group by person with a counting sort over the shard's contiguous id range, then sort
            // each person's (few) events by time; clicks are grouped the same way
            let base = sh as u32 * span;
            let np = span as usize;
            let li = |key: u64| -> usize { ((key >> 32) as u32 - base) as usize };
            let mut eoff = vec![0u32; np + 1];
            for r in evs.iter() {
                eoff[li(r.key) + 1] += 1;
            }
            for p in 0..np {
                eoff[p + 1] += eoff[p];
            }
            let mut eg: Vec<PR> = Vec::with_capacity(evs.len());
            let mut pos = eoff.clone();
            // SAFETY: every slot in 0..evs.len() is written exactly once below
            unsafe { eg.set_len(evs.len()) };
            for r in evs.iter() {
                let l = li(r.key);
                eg[pos[l] as usize] = *r;
                pos[l] += 1;
            }
            let mut coff = vec![0u32; np + 1];
            for c in cls.iter() {
                coff[li(c.key) + 1] += 1;
            }
            for p in 0..np {
                coff[p + 1] += coff[p];
            }
            let ncl = cls.len();
            let mut cg: Vec<CR> = Vec::with_capacity(ncl);
            // SAFETY: every slot in 0..ncl is written exactly once below
            unsafe { cg.set_len(ncl) };
            let mut cpos = coff.clone();
            for c in cls.iter() {
                let l = li(c.key);
                cg[cpos[l] as usize] = *c;
                cpos[l] += 1;
            }
            let mut cats: Vec<(u32, i64)> = vec![];
            // campaigns with clicks in the 7-day window: per-campaign counters + dense list with
            // positions, so add / expire / iterate are O(1) per click
            let mut acnt: Vec<u32> = vec![0; ncamp];
            let mut apos: Vec<u32> = vec![0; ncamp];
            let mut alist: Vec<u32> = Vec::new();
            for p in 0..np {
                if eoff[p] == eoff[p + 1] {
                    continue; // no events: this person has no sessions
                }
                let evs = &mut eg[eoff[p] as usize..eoff[p + 1] as usize];
                evs.sort_unstable_by_key(|r| r.key);
                let pcl = &mut cg[coff[p] as usize..coff[p + 1] as usize];
                // by ts ascending; equal ts ordered by click_id DESCENDING so the last eligible
                // click is the winner (latest ts, then smallest click_id)
                pcl.sort_unstable_by(|x, y| x.key.cmp(&y.key).then_with(|| unpack_ref(y.cref).cmp(unpack_ref(x.cref))));
                let pcl = &*pcl;
                let pid = base + p as u32;
                let (i, j) = (0usize, evs.len());
                let name = name_of_pid(pid);
                let ets = |x: &PR| ws + (x.key & 0xFFFF_FFFF) as i64;
                let cts = |x: &CR| cbase + (x.key & 0xFFFF_FFFF) as i64;
                // sliding window [lo, hi) over this person's clicks
                let (mut lo, mut hi) = (0usize, 0usize);
                for &c in &alist {
                    acnt[c as usize] = 0;
                }
                alist.clear();
                let mut cur_day = i32::MIN;
                let mut row: Option<URow> = None;
                let mut k = i;
                while k < j {
                    // one session: extend while gap between distinct timestamps <= GAP
                    let start = ets(&evs[k]);
                    let mut prev = start;
                    let mut e = k;
                    let (mut n, mut rev, mut purch) = (0i64, 0i64, false);
                    while e < j {
                        let t = ets(&evs[e]);
                        if !(t == prev || t - prev <= GAP) {
                            break;
                        }
                        prev = t;
                        n += 1;
                        if evs[e].price > 0 {
                            purch = true;
                            rev += evs[e].price as i64;
                        }
                        e += 1;
                    }
                    let day = start.div_euclid(DAY) as i32;
                    if day != cur_day {
                        if let Some(r) = row.take() {
                            urows.push(finish(r, &mut cats, &cat_names));
                        }
                        cur_day = day;
                        row = Some(URow { name, day, sessions: 0, events: 0, revenue: 0, top: None });
                    }
                    let r = row.as_mut().unwrap();
                    r.sessions += 1;
                    r.events += n;
                    r.revenue += rev;
                    for x in &evs[k..e] {
                        if x.price > 0 {
                            match cats.iter_mut().find(|c| c.0 == x.cat) {
                                Some(c) => c.1 += x.price as i64,
                                None => cats.push((x.cat, x.price as i64)),
                            }
                        }
                    }
                    // attribution window: start - WEEK <= click.ts <= start
                    while hi < pcl.len() && cts(&pcl[hi]) <= start {
                        let c = pcl[hi].camp as usize;
                        if acnt[c] == 0 {
                            apos[c] = alist.len() as u32;
                            alist.push(c as u32);
                        }
                        acnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && cts(&pcl[lo]) < start - WEEK {
                        let c = pcl[lo].camp as usize;
                        acnt[c] -= 1;
                        if acnt[c] == 0 {
                            let k = apos[c] as usize;
                            let last = *alist.last().unwrap();
                            alist[k] = last;
                            apos[last as usize] = k as u32;
                            alist.pop();
                        }
                        lo += 1;
                    }
                    let credit = if hi > lo { pcl[hi - 1].camp } else { ORGANIC };
                    let a = &mut camp[cidx(credit, day)];
                    a[0] += 1;
                    a[1] += purch as i64;
                    a[2] += rev;
                    for &c in &alist {
                        camp[cidx(c, day)][3] += 1;
                    }
                    k = e;
                }
                if let Some(r) = row.take() {
                    urows.push(finish(r, &mut cats, &cat_names));
                }
            }
            (urows, camp)
        })
        .collect();
    fn finish(mut r: URow, cats: &mut Vec<(u32, i64)>, cat_names: &[B]) -> URow {
        let name_of = |c: u32| -> &[u8] { if c == UNKNOWN_CAT { b"unknown" } else { cat_names[c as usize] } };
        let mut best: Option<(u32, i64)> = None;
        for &(c, v) in cats.iter() {
            best = match best {
                None => Some((c, v)),
                Some((bc, bv)) => {
                    if v > bv || (v == bv && name_of(c) < name_of(bc)) { Some((c, v)) } else { Some((bc, bv)) }
                }
            };
        }
        r.top = best.map(|b| b.0);
        cats.clear();
        r
    }
    lap("sessionized");

    // merge campaign partials
    let mut dense = vec![[0i64; 4]; ncamp * ndays];
    let mut user_parts: Vec<Vec<URow>> = vec![];
    for (u, c) in results {
        user_parts.push(u);
        for (a, v) in dense.iter_mut().zip(c) {
            for t in 0..4 {
                a[t] += v[t];
            }
        }
    }
    // a (campaign, day) row exists when it was credited or assisted at least once
    let mut camp_all: FxHashMap<(u32, i32), [i64; 4]> = FxHashMap::default();
    for (i, v) in dense.iter().enumerate() {
        if v[0] > 0 || v[3] > 0 {
            let c = i / ndays;
            let c = if c == ncamp - 1 { ORGANIC } else { c as u32 };
            camp_all.insert((c, day0 + (i % ndays) as i32), *v);
        }
    }

    // write Parquet (minimal writer in pq.rs)
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let cat_name = |c: u32| -> B { if c == UNKNOWN_CAT { b"unknown" } else { cat_names[c as usize] } };
    let groups: Vec<Vec<Vec<URow>>> = {
        let mut g: Vec<Vec<Vec<URow>>> = (0..ncpu).map(|_| vec![]).collect();
        for (i, p) in user_parts.into_iter().enumerate() {
            g[i % ncpu].push(p);
        }
        g
    };
    groups.into_par_iter().enumerate().for_each(|(gi, parts)| {
        let rows: Vec<&URow> = parts.iter().flatten().collect();
        pq::write(
            &ud.join(format!("part-{gi}.parquet")),
            &[
                ("user_id", pq::Col::Str(rows.iter().map(|r| Some(r.name)).collect(), false)),
                ("day", pq::Col::Date(rows.iter().map(|r| r.day).collect())),
                ("sessions", pq::Col::I64(rows.iter().map(|r| r.sessions).collect())),
                ("events", pq::Col::I64(rows.iter().map(|r| r.events).collect())),
                ("revenue_cents", pq::Col::I64(rows.iter().map(|r| r.revenue).collect())),
                ("top_category", pq::Col::Str(rows.iter().map(|r| r.top.map(cat_name)).collect(), true)),
            ],
        )
        .expect("write user_daily");
    });
    let crows: Vec<((u32, i32), [i64; 4])> = camp_all.into_iter().collect();
    let cid = |cp: u32| -> B { if cp == ORGANIC { b"organic" } else { camp_ids[cp as usize] } };
    let cch = |cp: u32| -> B { if cp == ORGANIC { b"organic" } else { camp_chan[cp as usize] } };
    pq::write(
        &cd.join("part-0.parquet"),
        &[
            ("campaign_id", pq::Col::Str(crows.iter().map(|((c, _), _)| Some(cid(*c))).collect(), false)),
            ("channel", pq::Col::Str(crows.iter().map(|((c, _), _)| Some(cch(*c))).collect(), false)),
            ("day", pq::Col::Date(crows.iter().map(|((_, d), _)| *d).collect())),
            ("sessions", pq::Col::I64(crows.iter().map(|(_, a)| a[0]).collect())),
            ("purchasing_sessions", pq::Col::I64(crows.iter().map(|(_, a)| a[1]).collect())),
            ("revenue_cents", pq::Col::I64(crows.iter().map(|(_, a)| a[2]).collect())),
            ("assisted_sessions", pq::Col::I64(crows.iter().map(|(_, a)| a[3]).collect())),
        ],
    )
    .expect("write campaign_daily");
    lap("written");
    // skip freeing gigabytes of memory on the way out
    std::process::exit(0);
}
