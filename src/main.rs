//! Shopping Sessions: fast exact pipeline.
//!
//! Design: read every CSV into memory in parallel, parse chunks in parallel with
//! hand-written parsers that follow rules.md exactly, then shard records by hash so
//! each worker owns a disjoint set of keys (event_id for dedup, person for
//! sessionizing) and needs no locks. Verified against oracle/reference.py.

mod pq;
mod probe;
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
const SH_BITS: u32 = 8; // shard count = 2^SH_BITS
const NSH: usize = 1 << SH_BITS;
const PL_BITS: u32 = 32 - SH_BITS; // person id = person shard << PL_BITS | index within the shard
const PL_MASK: u32 = (1 << PL_BITS) - 1;
const PF_DIST: usize = 2048; // parser prefetch distance (bytes)
const CHUNK: usize = 32 << 20; // bytes per parse task
const ORGANIC: u32 = u32::MAX;
const UNKNOWN_CAT: u32 = u32::MAX;

type B = &'static [u8];

// ------------------------------------------------------------------ helpers
/// Fast 64-bit hash of a short byte string. Collisions are always checked against the
/// original bytes by the callers, so this only has to be fast and well mixed.
#[inline]
fn h64(b: &[u8]) -> u64 {
    h12(b).0
}

/// 64x64 -> 128-bit multiply, folded (wyhash's "mum"): one multiply mixes all input bits.
#[inline(always)]
fn mum(a: u64, b: u64) -> (u64, u64) {
    let r = (a as u128) * (b as u128);
    (r as u64, (r >> 64) as u64)
}

const HS: [u64; 8] = [
    0xA076_1D64_78BD_642F,
    0xE703_7ED1_A0B4_28DB,
    0x8EBC_6AF0_9C88_C6E3,
    0x5899_65CC_7537_4CC3,
    0x1D8E_4E27_C47D_124F,
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
];

/// Core of h12 for inputs of at most 40 bytes, given their five zero-padded little-endian words:
/// a fixed sequence of four 128-bit multiplies (the first two independent), no loop and no
/// length-dependent branch: ids of 9 and 33 bytes take exactly the same instructions, so their mix
/// in the input costs no branch mispredicts. (Mixing: words 0-1 and 2-3 each become a 128-bit
/// product, the two products and word 4 + length become a third, and a fourth finishes.)
#[inline(always)]
fn h12_words(q: [u64; 5], len: u64) -> (u64, u64) {
    let (l1, h1) = mum(q[0] ^ HS[0], q[1] ^ HS[1]);
    let (l2, h2) = mum(q[2] ^ HS[2], q[3] ^ HS[3]);
    let (l3, h3) = mum(l1 ^ l2 ^ HS[4], h1 ^ h2 ^ q[4] ^ len.wrapping_mul(HS[5]));
    let (l4, h4) = mum(l3 ^ HS[6], h3 ^ HS[7]);
    (l4 ^ h4, l4)
}

/// Little-endian word i (bytes 8i..8i+8) of b, zero padded; 0 beyond the end.
#[inline(always)]
fn word_at(b: &[u8], i: usize) -> u64 {
    let s = 8 * i;
    if s >= b.len() {
        return 0;
    }
    let n = (b.len() - s).min(8);
    let mut t = [0u8; 8];
    t[..n].copy_from_slice(&b[s..s + n]);
    u64::from_le_bytes(t)
}

/// Reference h12: plain scalar code for every length (SELFTEST checks h12 against it).
fn h12_ref(b: &[u8]) -> (u64, u64) {
    let len = b.len();
    if len <= 40 {
        return h12_words([word_at(b, 0), word_at(b, 1), word_at(b, 2), word_at(b, 3), word_at(b, 4)], len as u64);
    }
    h12_long(b)
}

/// h12 for inputs over 40 bytes: words 0-3 as in h12_words, then every further pair of words
/// folded into the second product, word 4's slot empty.
#[inline(never)]
fn h12_long(b: &[u8]) -> (u64, u64) {
    let len = b.len() as u64;
    let (l1, h1) = mum(word_at(b, 0) ^ HS[0], word_at(b, 1) ^ HS[1]);
    let (mut l2, mut h2) = mum(word_at(b, 2) ^ HS[2], word_at(b, 3) ^ HS[3]);
    let mut k = 4;
    while 8 * k < b.len() {
        let (a, c) = mum(l2 ^ word_at(b, k) ^ HS[2], h2 ^ word_at(b, k + 1) ^ HS[3]);
        l2 = a;
        h2 = c;
        k += 2;
    }
    let (l3, h3) = mum(l1 ^ l2 ^ HS[4], h1 ^ h2 ^ len.wrapping_mul(HS[5]));
    let (l4, h4) = mum(l3 ^ HS[6], h3 ^ HS[7]);
    (l4 ^ h4, l4)
}

/// Both hashes of a byte string: (h64(b), h2(b)). Up to 40 bytes: one zero-padded masked
/// 64-byte load (masked-off bytes cannot fault, so nothing past the slice is touched) and
/// h12_words; longer: h12_long. Identical to h12_ref (SELFTEST).
#[inline(always)]
fn h12(b: &[u8]) -> (u64, u64) {
    let len = b.len();
    if len > 40 {
        return h12_long(b);
    }
    #[cfg(all(target_feature = "avx512f", target_feature = "avx512bw", target_feature = "bmi2"))]
    // SAFETY: the masked load reads only the first len bytes (all inside b); the store target is
    // a 64-byte local
    unsafe {
        use std::arch::x86_64::*;
        let k: __mmask64 = _bzhi_u64(u64::MAX, len as u32);
        let v = _mm512_maskz_loadu_epi8(k, b.as_ptr() as *const i8);
        let mut q = [0u64; 8];
        _mm512_storeu_si512(q.as_mut_ptr() as *mut _, v);
        return h12_words([q[0], q[1], q[2], q[3], q[4]], len as u64);
    }
    #[allow(unreachable_code)]
    h12_words([word_at(b, 0), word_at(b, 1), word_at(b, 2), word_at(b, 3), word_at(b, 4)], len as u64)
}

/// Same as h12 (kept for the call sites that used the over-reading variant; h12 no longer
/// reads past the slice).
#[inline(always)]
fn h12_over(b: &[u8]) -> (u64, u64) {
    h12(b)
}

/// Independent second hash: (h64, h2) together identify a string (2^-128 collision odds).
#[inline]
fn h2(b: &[u8]) -> u64 {
    h12(b).1
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

fn scan_lines3<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(Option<[Option<&'a [u8]>; N]>)) {
    let n = chunk.len();
    let mut start = 0usize;

    let mut pad = [0u8; 128];
    let mut commas = [0usize; 16];
    loop {
        if start >= n {
            return;
        }
        #[cfg(target_feature = "sse")]
        if start + PF_DIST < n {
            // SAFETY: in-bounds address; prefetch has no architectural effect
            unsafe { std::arch::x86_64::_mm_prefetch(chunk.as_ptr().add(start + PF_DIST) as *const i8, std::arch::x86_64::_MM_HINT_T0) };
        }
        // fast path: newline, commas and quotes of a line ending within 128 bytes from one load
        if start + 128 <= n {
            let (c0, n0, q0) = block_masks(&chunk[start..start + 64]);
            let (c1, n1, q1) = block_masks(&chunk[start + 64..start + 128]);
            let nl = (n0 as u128) | ((n1 as u128) << 64);
            if nl != 0 {
                let e = nl.trailing_zeros() as usize;
                let below = (1u128 << e) - 1;
                let cm = ((c0 as u128) | ((c1 as u128) << 64)) & below;
                if cm.count_ones() as usize != N - 1 {
                    f(None);
                } else {
                    let nq = (((q0 as u128) | ((q1 as u128) << 64)) & below).count_ones();
                    let mut m = cm;
                    for slot in commas.iter_mut().take(N - 1) {
                        *slot = start + m.trailing_zeros() as usize;
                        m &= m - 1;
                    }
                    f(finish_line::<N>(chunk, start, start + e, &commas[..N - 1], nq));
                }
                start += e + 1;
                continue;
            }
        }
        let end = memchr::memchr(b'\n', &chunk[start..]).map_or(n, |k| start + k);
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
                uoff: (u.as_ptr() as usize - chunk.as_ptr() as usize) as u32,
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
                uoff: (u.as_ptr() as usize - chunk.as_ptr() as usize) as u32,
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
                    uoff: (u.as_ptr() as usize - chunk.as_ptr() as usize) as u32,
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

fn parse_event_chunk5(
    chunk: &'static [u8],
    ci: usize,
    ws: i64,
    we: i64,
    products: &FxHashMap<B, u32>,
) -> (Vec<PEv>, Vec<DKey>, Vec<u32>, Vec<u32>) {
    let n = chunk.len();
    assert!(n < (1 << 31), "chunk too large for the structural index");
    let cap = n / 64 + 16;
    let mut pay: Vec<PEv> = Vec::with_capacity(cap);
    let mut keys: Vec<DKey> = Vec::with_capacity(cap);
    let mut kcount = vec![0u32; NSH];
    let mut ucount = vec![0u32; NSH]; // valid payloads per user shard
    let mut cm = [0usize; 8];
    let mut mc = MonthCache::new();
    // lines wait in batches of 8 so their ts and ingest_ts are parsed eight at a time (ts8)
    let mut bfb = [[(0usize, 0usize); 7]; 8];
    let mut bn = 0usize;
    let mut pad = [0u8; 64];
    // per-line structural scan: from each line start, load 64 B blocks and take the newline,
    // comma and quote positions straight from the masks (no separator index array)
    let mut next_start = 0usize;
    while next_start < n {
        let line_start = next_start;
        // software prefetch: the file mapping is 4 KB pages, where the hardware prefetcher stops
        #[cfg(target_feature = "sse")]
        {
            let pf = line_start + PF_DIST;
            if pf < n {
                // SAFETY: in-bounds address; prefetch has no architectural effect
                unsafe { std::arch::x86_64::_mm_prefetch(chunk.as_ptr().add(pf) as *const i8, std::arch::x86_64::_MM_HINT_T0) };
            }
        }
        let mut k = 0usize;
        let mut nq = 0u32;
        // fast path only: line-relative comma and quote masks and the newline offset
        let mut fastm: Option<(u128, u128, u32)> = None;
        let mut off = line_start;
        let end;
        // fast path: the whole line (and its newline) within the next 128 bytes
        if line_start + 128 <= n {
            let (c0, n0, q0) = block_masks(&chunk[line_start..line_start + 64]);
            let (c1, n1, q1) = block_masks(&chunk[line_start + 64..line_start + 128]);
            let nl = (n0 as u128) | ((n1 as u128) << 64);
            if nl != 0 {
                let e = nl.trailing_zeros();
                let below = (1u128 << e) - 1;
                let cm128 = ((c0 as u128) | ((c1 as u128) << 64)) & below;
                let q128 = ((q0 as u128) | ((q1 as u128) << 64)) & below;
                end = line_start + e as usize;
                next_start = end + 1;
                if cm128.count_ones() != 6 {
                    continue;
                }
                nq = q128.count_ones();
                fastm = Some((cm128, q128, e));
                let mut m = cm128;
                for slot in cm.iter_mut().take(6) {
                    *slot = line_start + m.trailing_zeros() as usize;
                    m &= m - 1;
                }
                k = 6;
            } else {
                end = usize::MAX; // no newline within 128 bytes: fall back to the block loop
            }
        } else {
            end = usize::MAX;
        }
        let end = if end != usize::MAX {
            end
        } else {
            k = 0;
            nq = 0;
            let end;
            loop {
            let blk: &[u8] = if off + 64 <= n {
                &chunk[off..off + 64]
            } else {
                pad = [0u8; 64];
                pad[..n - off].copy_from_slice(&chunk[off..n]);
                &pad
            };
            let (c, nl, q) = block_masks(blk);
            let below = if nl != 0 { (1u64 << nl.trailing_zeros()) - 1 } else { u64::MAX };
            let mut cmask = c & below;
            nq += (q & below).count_ones();
            while cmask != 0 {
                if k < 8 {
                    cm[k] = off + cmask.trailing_zeros() as usize;
                }
                k += 1;
                cmask &= cmask - 1;
            }
            if nl != 0 {
                end = off + nl.trailing_zeros() as usize;
                break;
            }
            off += 64;
            if off >= n {
                end = n;
                break;
            }
            }
            next_start = end + 1;
            if k != 6 {
                continue;
            }
            end
        };
        // field k spans [fs_k, fe_k); strip quotes around whole fields; validate the quote count
            let mut fb = [(0usize, 0usize); 7];
            let mut pairs = 0u32;
            let mut fs = line_start;
            if let Some((cm128, q128, e)) = fastm {
                // all 7 fields at once from the masks (same rule as the loop below): field k is
                // quoted iff its first and last characters are quotes and it is >= 2 long
                let starts = 1u128 | (cm128 << 1); // first character of each field
                let seps = cm128 | (1u128 << e); // the separator after each field
                let qs = pext128(q128, starts); // bit k: field k starts with a quote
                let qe = pext128(q128 << 1, seps); // bit k: field k ends with a quote
                let one = pext128((starts << 1) & seps, seps); // bit k: field k is 1 long
                let qb = (qs & qe & !one) as u32 & 0x7F;
                pairs = qb.count_ones();
                for k in 0..7 {
                    let fe = if k < 6 { cm[k] } else { end };
                    let isq = ((qb >> k) & 1) as usize;
                    fb[k] = (fs + isq, fe - isq);
                    fs = fe + 1;
                }
            } else {
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
            }
            if nq != 2 * pairs {
                continue; // a quote that is not around a whole field
            }
            if fb[0].0 >= fb[0].1 {
                continue; // no event_id
            }
            bfb[bn] = fb;
            bn += 1;
            if bn == 8 {
                ev_batch(chunk, ci, ws, we, products, &bfb, bn, &mut mc, &mut pay, &mut keys, &mut kcount, &mut ucount);
                bn = 0;
            }
    }
    if bn > 0 {
        ev_batch(chunk, ci, ws, we, products, &bfb, bn, &mut mc, &mut pay, &mut keys, &mut kcount, &mut ucount);
    }
    (pay, keys, kcount, ucount)
}

/// Finish a batch of up to 8 parsed lines (field bounds in bfb): both timestamps of all lines at
/// once through ts8 (scalar parse for any form it does not decide), then each line in order.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn ev_batch(
    chunk: &'static [u8],
    ci: usize,
    ws: i64,
    we: i64,
    products: &FxHashMap<B, u32>,
    bfb: &[[(usize, usize); 7]; 8],
    bn: usize,
    mc: &mut MonthCache,
    pay: &mut Vec<PEv>,
    keys: &mut Vec<DKey>,
    kcount: &mut [u32],
    ucount: &mut [u32],
) {
    let get = |l: usize, k: usize| -> Option<B> {
        let (a, b) = bfb[l][k];
        if a < b { Some(&chunk[a..b]) } else { None }
    };
    let mut tsv = [0i64; 8];
    let mut ingv = [0i64; 8];
    #[cfg(all(target_feature = "avx512bw", target_feature = "avx512dq"))]
    {
        let (mut o, mut n, mut oi, mut ni) = ([0i64; 8], [0u64; 8], [0i64; 8], [0u64; 8]);
        let mut hi = 0usize;
        for l in 0..bn {
            let (a, b) = bfb[l][2];
            let (c, d) = bfb[l][3];
            o[l] = a as i64;
            oi[l] = c as i64;
            n[l] = b.saturating_sub(a) as u64;
            ni[l] = d.saturating_sub(c) as u64;
            hi = hi.max(a).max(c);
        }
        let (dt, di) = if hi + 32 <= chunk.len() {
            (ts8g::<true>(chunk.as_ptr(), &o, &n, &mut tsv), ts8g::<false>(chunk.as_ptr(), &oi, &ni, &mut ingv))
        } else {
            // the chunk ends within 32 bytes of a field: copy the words (zero padded)
            let (mut w, mut wi) = ([[0u64; 8]; 4], [[0u64; 8]; 4]);
            for l in 0..bn {
                let x = ts_words(chunk, o[l] as usize);
                let y = ts_words(chunk, oi[l] as usize);
                for k in 0..4 {
                    w[k][l] = x[k];
                    wi[k][l] = y[k];
                }
            }
            (ts8::<true>(&w, &n, &mut tsv), ts8::<false>(&wi, &ni, &mut ingv))
        };
        for l in 0..bn {
            if (dt >> l) & 1 == 0 {
                tsv[l] = parse_ts_over(get(l, 2));
            }
            if (di >> l) & 1 == 0 {
                ingv[l] = mc.f1(get(l, 3));
            }
        }
    }
    #[cfg(not(all(target_feature = "avx512bw", target_feature = "avx512dq")))]
    for l in 0..bn {
        tsv[l] = parse_ts_over(get(l, 2));
        ingv[l] = mc.f1(get(l, 3));
    }
    for l in 0..bn {
            let fb = bfb[l];
            let field = |k: usize| -> Option<B> {
                let (a, b) = fb[k];
                if a < b { Some(&chunk[a..b]) } else { None }
            };
            let Some(eid) = field(0) else { continue };
            let (ts, ing) = (tsv[l], ingv[l]);
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
                        let pidb = pid.unwrap();
                        let c = match PRODTAB.get() {
                            Some(Some(t)) if t.src == products as *const _ as usize => t.get(pidb),
                            _ => products.get(pidb).copied(),
                        };
                        cat = match c {
                            Some(c) => c as u16,
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
                    uoff: (u.as_ptr() as usize - chunk.as_ptr() as usize) as u32,
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
}

#[inline(never)]
fn parse5_stage<const S: u8>(
    chunk: &'static [u8],
    ci: usize,
    ws: i64,
    we: i64,
    products: &FxHashMap<B, u32>,
) -> (Vec<PEv>, Vec<DKey>, Vec<u32>, Vec<u32>) {
    let n = chunk.len();
    assert!(n < (1 << 31), "chunk too large for the structural index");
    let cap = n / 64 + 16;
    let mut pay: Vec<PEv> = Vec::with_capacity(cap);
    let mut keys: Vec<DKey> = Vec::with_capacity(cap);
    let mut kcount = vec![0u32; NSH];
    let mut ucount = vec![0u32; NSH]; // valid payloads per user shard
    let mut cm = [0usize; 8];
    let mut mc = MonthCache::new();
    let mut pad = [0u8; 64];
    // per-line structural scan: from each line start, load 64 B blocks and take the newline,
    // comma and quote positions straight from the masks (no separator index array)
    let mut next_start = 0usize;
    while next_start < n {
        let line_start = next_start;
        // software prefetch: the file mapping is 4 KB pages, where the hardware prefetcher stops
        #[cfg(target_feature = "sse")]
        {
            let pf = line_start + PF_DIST;
            if pf < n {
                // SAFETY: in-bounds address; prefetch has no architectural effect
                unsafe { std::arch::x86_64::_mm_prefetch(chunk.as_ptr().add(pf) as *const i8, std::arch::x86_64::_MM_HINT_T0) };
            }
        }
        let mut k = 0usize;
        let mut nq = 0u32;
        // fast path only: line-relative comma and quote masks and the newline offset
        let mut fastm: Option<(u128, u128, u32)> = None;
        let mut off = line_start;
        let end;
        // fast path: the whole line (and its newline) within the next 128 bytes
        if line_start + 128 <= n {
            let (c0, n0, q0) = block_masks(&chunk[line_start..line_start + 64]);
            let (c1, n1, q1) = block_masks(&chunk[line_start + 64..line_start + 128]);
            let nl = (n0 as u128) | ((n1 as u128) << 64);
            if nl != 0 {
                let e = nl.trailing_zeros();
                let below = (1u128 << e) - 1;
                let cm128 = ((c0 as u128) | ((c1 as u128) << 64)) & below;
                let q128 = ((q0 as u128) | ((q1 as u128) << 64)) & below;
                end = line_start + e as usize;
                next_start = end + 1;
                if cm128.count_ones() != 6 {
                    continue;
                }
                nq = q128.count_ones();
                fastm = Some((cm128, q128, e));
                let mut m = cm128;
                for slot in cm.iter_mut().take(6) {
                    *slot = line_start + m.trailing_zeros() as usize;
                    m &= m - 1;
                }
                k = 6;
            } else {
                end = usize::MAX; // no newline within 128 bytes: fall back to the block loop
            }
        } else {
            end = usize::MAX;
        }
        let end = if end != usize::MAX {
            end
        } else {
            k = 0;
            nq = 0;
            let end;
            loop {
            let blk: &[u8] = if off + 64 <= n {
                &chunk[off..off + 64]
            } else {
                pad = [0u8; 64];
                pad[..n - off].copy_from_slice(&chunk[off..n]);
                &pad
            };
            let (c, nl, q) = block_masks(blk);
            let below = if nl != 0 { (1u64 << nl.trailing_zeros()) - 1 } else { u64::MAX };
            let mut cmask = c & below;
            nq += (q & below).count_ones();
            while cmask != 0 {
                if k < 8 {
                    cm[k] = off + cmask.trailing_zeros() as usize;
                }
                k += 1;
                cmask &= cmask - 1;
            }
            if nl != 0 {
                end = off + nl.trailing_zeros() as usize;
                break;
            }
            off += 64;
            if off >= n {
                end = n;
                break;
            }
            }
            next_start = end + 1;
            if k != 6 {
                continue;
            }
            end
        };
        if S == 1 {
            std::hint::black_box((end, nq, cm[5]));
            continue;
        }
        // field k spans [fs_k, fe_k); strip quotes around whole fields; validate the quote count
            let mut fb = [(0usize, 0usize); 7];
            let mut pairs = 0u32;
            let mut fs = line_start;
            if let Some((cm128, q128, e)) = fastm {
                // all 7 fields at once from the masks (same rule as the loop below): field k is
                // quoted iff its first and last characters are quotes and it is >= 2 long
                let starts = 1u128 | (cm128 << 1); // first character of each field
                let seps = cm128 | (1u128 << e); // the separator after each field
                let qs = pext128(q128, starts); // bit k: field k starts with a quote
                let qe = pext128(q128 << 1, seps); // bit k: field k ends with a quote
                let one = pext128((starts << 1) & seps, seps); // bit k: field k is 1 long
                let qb = (qs & qe & !one) as u32 & 0x7F;
                pairs = qb.count_ones();
                for k in 0..7 {
                    let fe = if k < 6 { cm[k] } else { end };
                    let isq = ((qb >> k) & 1) as usize;
                    fb[k] = (fs + isq, fe - isq);
                    fs = fe + 1;
                }
            } else {
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
            }
            if nq != 2 * pairs {
                continue; // a quote that is not around a whole field
            }
            if S == 2 {
                std::hint::black_box(fb);
                continue;
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
                        let pidb = pid.unwrap();
                        let c = match PRODTAB.get() {
                            Some(Some(t)) if t.src == products as *const _ as usize => t.get(pidb),
                            _ => products.get(pidb).copied(),
                        };
                        cat = match c {
                            Some(c) => c as u16,
                            None => u16::MAX,
                        };
                    }
                    None => ok = false,
                }
            }
            if ok && ing - ts > DAY {
                ok = false;
            }
            if S == 3 {
                std::hint::black_box((ok, p, cat, ing));
                continue;
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
                    uoff: (u.as_ptr() as usize - chunk.as_ptr() as usize) as u32,
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
    // two entries: a 14-day window can span two months, and lines arrive in random time order
    static MONTH: std::cell::Cell<[(u64, i64, i64); 2]> = const { std::cell::Cell::new([(u64::MAX, 0, 0); 2]) };
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
    let m = MONTH.with(|c| c.get());
    let (base, mlen) = if m[0].0 == key {
        (m[0].1, m[0].2)
    } else if m[1].0 == key {
        (m[1].1, m[1].2)
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
        MONTH.with(|c| c.set([(key, bs, ml), m[0]]));
        (bs, ml)
    };
    if d < 1 || d > mlen {
        return None;
    }
    Some((((base + d - 1) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// Byte mask -> qword with 0xFF in each selected byte (compile-time constant at every use).
const fn bytes_of(pat: u8) -> i64 {
    let mut q = 0u64;
    let mut k = 0;
    while k < 8 {
        if (pat >> k) & 1 == 1 {
            q |= 0xFF << (8 * k);
        }
        k += 1;
    }
    q as i64
}

/// Qword holding byte `c` at each byte position selected by `pat`.
const fn tmpl(pat: u8, c: u8) -> i64 {
    let mut q = 0u64;
    let mut k = 0;
    while k < 8 {
        if (pat >> k) & 1 == 1 {
            q |= (c as u64) << (8 * k);
        }
        k += 1;
    }
    q as i64
}

/// Replicate a per-lane byte pattern (bit k = byte k) across the 8 lanes of a 64-bit byte mask.
const fn rep8(pat: u8) -> u64 {
    (pat as u64) * 0x0101_0101_0101_0101
}

/// Eight timestamp fields at once (AVX-512, no branch on the form). Lane l holds the field's bytes
/// 0..8, 8..16, 16..24, 24..32 in w[0..4][l] (bytes past the field are whatever follows it; the
/// caller only promises they are readable) and its length in n[l]. For every lane shaped like a
/// 13-digit F3, a 20-byte "...Z" F1 or a 25-byte "...+HH:MM" F2 it writes out[l] = parse_ts of the
/// field (BAD when invalid) and sets bit l of the result; other lanes must use parse_ts.
/// ALL = false decides only F1 lanes (ingest_ts, which is F1 by the rules).
#[cfg(all(target_feature = "avx512bw", target_feature = "avx512dq"))]
#[inline(always)]
fn ts8<const ALL: bool>(w: &[[u64; 8]; 4], n: &[u64; 8], out: &mut [i64; 8]) -> u8 {
    use std::arch::x86_64::*;
    // SAFETY: avx512 enabled; loads from the arrays
    unsafe {
        let ld = |a: &[u64; 8]| _mm512_loadu_si512(a.as_ptr() as *const __m512i);
        ts8v::<ALL>(ld(&w[0]), ld(&w[1]), ld(&w[2]), ld(&w[3]), ld(n), out)
    }
}

/// ts8 with the fields gathered straight from `base + off[l]` (every base + off[l] + 32 readable).
#[cfg(all(target_feature = "avx512bw", target_feature = "avx512dq"))]
#[inline(always)]
fn ts8g<const ALL: bool>(base: *const u8, off: &[i64; 8], n: &[u64; 8], out: &mut [i64; 8]) -> u8 {
    use std::arch::x86_64::*;
    // SAFETY: avx512 enabled; the caller guarantees base + off[l] + 32 is readable for every lane
    unsafe {
        let o = _mm512_loadu_si512(off.as_ptr() as *const __m512i);
        let g = |k: usize| _mm512_i64gather_epi64::<1>(o, base.add(8 * k) as *const i64);
        // the F1-only form (ingest_ts) never reads bytes 24..32: no fourth gather for it
        let v3 = if ALL { g(3) } else { _mm512_setzero_si512() };
        ts8v::<ALL>(g(0), g(1), g(2), v3, _mm512_loadu_si512(n.as_ptr() as *const __m512i), out)
    }
}

#[cfg(all(target_feature = "avx512bw", target_feature = "avx512dq"))]
#[inline(always)]
fn ts8v<const ALL: bool>(v0: std::arch::x86_64::__m512i, v1: std::arch::x86_64::__m512i, v2: std::arch::x86_64::__m512i, v3: std::arch::x86_64::__m512i, vn: std::arch::x86_64::__m512i, out: &mut [i64; 8]) -> u8 {
    use std::arch::x86_64::*;
    // SAFETY: avx512bw/dq are enabled for this build; the only store is to `out`
    unsafe {
        let set = |x: i64| _mm512_set1_epi64(x);
        let z = _mm512_set1_epi8(b'0' as i8);
        let ten = _mm512_set1_epi8(10);
        let (d0, d1, d2, d3) = (_mm512_sub_epi8(v0, z), _mm512_sub_epi8(v1, z), _mm512_sub_epi8(v2, z), _mm512_sub_epi8(v3, z));
        let (g0, g1, g2) = (_mm512_cmplt_epu8_mask(d0, ten), _mm512_cmplt_epu8_mask(d1, ten), _mm512_cmplt_epu8_mask(d2, ten));
        // lanes with no set bit in a 64-bit byte mask
        let clean = |bad: u64| -> u8 {
            let v = _mm512_movm_epi8(bad);
            !_mm512_test_epi64_mask(v, v)
        };
        let eq = |v: __m512i, t: i64| _mm512_cmpeq_epi8_mask(v, set(t));
        // separators: '-' at 4 and 7, 'T' at 10, ':' at 13 and 16; F1 'Z' at 19
        let e0 = eq(v0, tmpl(0x90, b'-'));
        let e1 = eq(v1, tmpl(0x04, b'T') | tmpl(0x20, b':'));
        let e2 = eq(v2, tmpl(0x01, b':') | tmpl(0x08, b'Z'));
        // date-time digits at 0-3, 5-6 | 8-9, 11-12, 14-15 | 17-18
        let bad_dt = (rep8(0x6F) & !g0) | (rep8(0x90) & !e0) | ((rep8(0xDB) & !g1) | (rep8(0x24) & !e1)) | (rep8(0x06) & !g2) | (rep8(0x01) & !e2);
        let shape_dt = clean(bad_dt);
        let n20 = _mm512_cmpeq_epi64_mask(vn, set(20));
        let is1 = n20 & clean(rep8(0x08) & !e2); // 20 bytes with 'Z' at 19
        let w10 = _mm512_set1_epi16(0x010A); // byte weights [10, 1]
        let wd = |p: __m512i, k: u32| _mm512_and_si512(_mm512_srlv_epi64(p, set(16 * k as i64)), set(0xFFFF));
        let p0 = _mm512_maddubs_epi16(d0, w10); // (0,1) (2,3)
        let p0s = _mm512_maddubs_epi16(_mm512_srli_epi64(d0, 8), w10); // (5,6) in word 2
        let p1 = _mm512_maddubs_epi16(d1, w10); // (8,9) word 0, (14,15) word 3
        let p1s = _mm512_maddubs_epi16(_mm512_srli_epi64(d1, 8), w10); // (11,12) word 1
        let p2s = _mm512_maddubs_epi16(_mm512_srli_epi64(d2, 8), w10); // (17,18) word 0
        let y = _mm512_add_epi64(_mm512_mul_epu32(wd(p0, 0), set(100)), wd(p0, 1));
        let mo = wd(p0s, 2);
        let dd = wd(p1, 0);
        let h = wd(p1s, 1);
        let mi = wd(p1, 3);
        let sec = wd(p2s, 0);
        let le = |a: __m512i, k: i64| _mm512_cmple_epu64_mask(a, set(k));
        // month length with leap years (y in 0..=9999; magic divisions verified over the range)
        let q100 = _mm512_srli_epi64(_mm512_mul_epu32(y, set(5243)), 19);
        let q400 = _mm512_srli_epi64(q100, 2);
        let r100 = _mm512_sub_epi64(y, _mm512_mul_epu32(q100, set(100)));
        let r400 = _mm512_sub_epi64(y, _mm512_mul_epu32(q400, set(400)));
        let div4 = _mm512_cmpeq_epi64_mask(_mm512_and_si512(y, set(3)), set(0));
        let leap = div4 & (_mm512_cmpneq_epi64_mask(r100, set(0)) | _mm512_cmpeq_epi64_mask(r400, set(0)));
        let t0 = _mm512_setr_epi64(0, 31, 28, 31, 30, 31, 30, 31);
        let t1 = _mm512_setr_epi64(31, 30, 31, 30, 31, 0, 0, 0);
        let mlen = _mm512_permutex2var_epi64(t0, mo, t1);
        let mlen = _mm512_mask_add_epi64(mlen, leap & _mm512_cmpeq_epi64_mask(mo, set(2)), mlen, set(1));
        // month 1..=12 (the table lookup uses only the index's low 4 bits; mlen = 0 for 0, 13..=15), day 1..=mlen
        let rng = le(mo, 12) & _mm512_cmpgt_epu64_mask(mlen, set(0)) & le(h, 23) & le(mi, 59) & le(sec, 59) & _mm512_cmpgt_epu64_mask(dd, set(0)) & _mm512_cmple_epu64_mask(dd, mlen);
        let ok_dt = shape_dt & rng;
        // days_from_civil(y, mo, 1) + 719468 + 146097 (one era up so everything stays non-negative)
        let feb = le(mo, 2);
        let yy = _mm512_add_epi64(y, set(400));
        let yy = _mm512_mask_sub_epi64(yy, feb, yy, set(1));
        let era1 = _mm512_srli_epi64(_mm512_mul_epu32(yy, set(5243)), 21);
        let yoe = _mm512_sub_epi64(yy, _mm512_mul_epu32(era1, set(400)));
        let mp = _mm512_mask_blend_epi64(feb, _mm512_sub_epi64(mo, set(3)), _mm512_add_epi64(mo, set(9)));
        let mp = _mm512_and_si512(mp, set(0xF)); // garbage months stay small (their lanes are invalid)
        let doy = _mm512_srli_epi64(_mm512_mul_epu32(_mm512_add_epi64(_mm512_mul_epu32(mp, set(153)), set(2)), set(13108)), 16);
        let doe = _mm512_add_epi64(
            _mm512_sub_epi64(_mm512_add_epi64(_mm512_mul_epu32(yoe, set(365)), _mm512_srli_epi64(yoe, 2)), _mm512_srli_epi64(_mm512_mul_epu32(yoe, set(1311)), 17)),
            doy,
        );
        let dpos = _mm512_add_epi64(_mm512_mul_epu32(era1, set(146_097)), doe); // < 2^23
        // minutes since the shifted epoch (< 2^32), then ms; the shift comes off as one constant
        let t = _mm512_add_epi64(_mm512_mul_epu32(_mm512_add_epi64(dpos, _mm512_sub_epi64(dd, set(1))), set(24)), h);
        let t = _mm512_add_epi64(_mm512_mul_epu32(t, set(60)), mi);
        const SHIFT_MS: i64 = (719_468 + 146_097) * 86_400_000;
        let dt = _mm512_sub_epi64(_mm512_add_epi64(_mm512_mullo_epi64(t, set(60_000)), _mm512_mul_epu32(sec, set(1000))), set(SHIFT_MS));
        let bad = set(BAD);
        let r1 = _mm512_mask_blend_epi64(ok_dt, bad, dt);
        if !ALL {
            _mm512_storeu_si512(out.as_mut_ptr() as *mut __m512i, r1);
            return is1;
        }
        // F2: sign at 19, ':' at 22, offset digits at 20-21 and 23-24
        let g3 = _mm512_cmplt_epu8_mask(d3, ten);
        let plus = clean(rep8(0x08) & !eq(v2, tmpl(0x08, b'+')));
        let minus = clean(rep8(0x08) & !eq(v2, tmpl(0x08, b'-')));
        let c22 = clean(rep8(0x40) & !eq(v2, tmpl(0x40, b':')));
        let n25 = _mm512_cmpeq_epi64_mask(vn, set(25));
        let is2 = n25 & (plus | minus) & c22;
        let p2 = _mm512_maddubs_epi16(d2, w10); // (20,21) word 2
        let oh = wd(p2, 2);
        let om = _mm512_add_epi64(
            _mm512_mul_epu32(_mm512_and_si512(_mm512_srli_epi64(d2, 56), set(0xFF)), set(10)),
            _mm512_and_si512(d3, set(0xFF)),
        );
        let off = _mm512_mul_epu32(_mm512_add_epi64(_mm512_mul_epu32(oh, set(60)), om), set(60_000));
        let f2 = _mm512_mask_blend_epi64(plus, _mm512_add_epi64(dt, off), _mm512_sub_epi64(dt, off));
        let ok2 = ok_dt & clean((rep8(0xB0) & !g2) | (rep8(0x01) & !g3)) & le(oh, 23) & le(om, 59);
        // F3: 13 digits = 8 (bytes 0..8) and 5 (bytes 8..13, moved to the top of the qword)
        let w100 = _mm512_set1_epi32(0x0001_0064); // 16-bit weights [100, 1]
        let num8 = |d: __m512i| {
            let u = _mm512_madd_epi16(_mm512_maddubs_epi16(d, w10), w100);
            _mm512_add_epi64(_mm512_mul_epu32(u, set(10_000)), _mm512_srli_epi64(u, 32))
        };
        let f3 = _mm512_add_epi64(_mm512_mul_epu32(num8(d0), set(100_000)), num8(_mm512_slli_epi64(d1, 24)));
        let ok3 = clean(!g0 | (rep8(0x1F) & !g1));
        let is3 = _mm512_cmpeq_epi64_mask(vn, set(13));
        let r2 = _mm512_mask_blend_epi64(ok2, bad, f2);
        let r3 = _mm512_mask_blend_epi64(ok3, bad, f3);
        let r = _mm512_mask_blend_epi64(is3, _mm512_mask_blend_epi64(is2, r1, r2), r3);
        _mm512_storeu_si512(out.as_mut_ptr() as *mut __m512i, r);
        is1 | is2 | is3
    }
}

/// Load a field's first 32 bytes for ts8 (zero-padded when the chunk ends sooner).
#[inline(always)]
fn ts_words(chunk: &[u8], a: usize) -> [u64; 4] {
    if a + 32 <= chunk.len() {
        let p = chunk.as_ptr();
        // SAFETY: a + 32 <= chunk.len()
        unsafe { [0, 8, 16, 24].map(|o| std::ptr::read_unaligned(p.add(a + o) as *const u64)) }
    } else {
        let mut b = [0u8; 32];
        let e = chunk.len().min(a + 32);
        b[..e - a].copy_from_slice(&chunk[a..e]);
        [0, 8, 16, 24].map(|o| u64::from_le_bytes(b[o..o + 8].try_into().unwrap()))
    }
}

/// Per-chunk cache for F1 decoding: exact "YYYY-MM-" bytes -> (days at day 1, month length).
struct MonthCache {
    key: u64,
    base: i64,
    mlen: i64,
    // second entry: a 14-day window can span two months, and lines arrive in random time order
    key2: u64,
    base2: i64,
    mlen2: i64,
}

impl MonthCache {
    fn new() -> Self {
        MonthCache { key: u64::MAX, base: 0, mlen: 0, key2: u64::MAX, base2: 0, mlen2: 0 }
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
        let (base, mlen) = self.month(b, key);
        if d < 1 || d > mlen {
            return BAD;
        }
        (((base + d - 1) * 24 + h) * 60 + mi) * 60_000 + s * 1000
    }
}

impl MonthCache {
    /// Make the cache hold the month of the 8 bytes `key` (= "YYYY-MM-" of `b`), as f1 does.
    #[inline(never)]
    fn load(&mut self, b: &[u8], key: u64) {
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
        (self.key2, self.base2, self.mlen2) = (self.key, self.base, self.mlen);
        self.key = key;
        self.base = bs;
        self.mlen = ml;
    }

    /// (first day of the month as days since epoch, month length; 0 if "YYYY-MM-" is invalid) for
    /// the 8 bytes `key` of `b`: either cached entry is picked without a branch.
    #[inline(always)]
    fn month(&mut self, b: &[u8], key: u64) -> (i64, i64) {
        if (key != self.key) & (key != self.key2) {
            self.load(b, key);
        }
        let a = key == self.key;
        (sel(a, self.base, self.base2), sel(a, self.mlen, self.mlen2))
    }
}

/// Branch-free select: c ? a : b (a data dependency, never a mispredicted branch).
#[inline(always)]
fn sel(c: bool, a: i64, b: i64) -> i64 {
    let m = -(c as i64);
    (a & m) | (b & !m)
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
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

/// How input files are brought into memory (MAPMODE env; default below).
fn map_mode() -> u8 {
    match std::env::var("MAPMODE").as_deref() {
        Ok("populate") => 0,
        Ok("willneed") => 1,
        Ok("seq") => 2,
        Ok("none") => 3,
        // default: lazy faulting, so reading the (possibly cold) input overlaps with parsing
        // on all threads instead of one up-front populate (cold 20M: 2.24 s -> 1.72 s; warm: tie)
        _ => 3,
    }
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
    let mode = map_mode();
    let flags = if mode == 0 { 2 | 0x8000 } else { 2 };
    let ptr = unsafe { mmap(std::ptr::null_mut(), len, 1, flags, f.as_raw_fd(), 0) };
    if ptr as isize == -1 || ptr.is_null() {
        return Box::leak(std::fs::read(p).expect("read").into_boxed_slice());
    }
    // MADV_SEQUENTIAL=2, MADV_WILLNEED=3: start read-ahead without waiting for it
    match mode {
        1 => unsafe {
            madvise(ptr, len, 3);
        },
        2 => unsafe {
            madvise(ptr, len, 2);
        },
        _ => {}
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
#[repr(C, packed(4))]
struct UCl { // 28 bytes, no padding
    uh: u64,
    uref: u32, // user_id: click-chunk << 22 | offset in that chunk (CREF_*)
    cref: u32, // click_id: same encoding
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
#[repr(C, packed(2))]
struct PEv { // 30 bytes: no padding (unaligned field loads are free on x86)
    pk: u64,    // h64(user_id); with pk2 a 96-bit user identity
    uoff: u32,  // user_id start, as a byte offset into this record's parse chunk (see name_at)
    pk2: u32,   // low 32 bits of h2(user_id)
    ts: u32,    // ts - window_start (valid events lie inside the window)
    price: u32, // > 0 exactly for purchases
    cat: u16,
}


/// Event after person resolution: key = person id << 32 | (ts - window_start).
#[derive(Clone, Copy)]
#[repr(C, packed(2))]
struct PR { // 14 bytes, no padding
    key: u64,
    price: u32, // > 0 exactly for purchases
    cat: u16,   // category index; u16::MAX = unknown product (categories are < 65535)
}

/// Click after person resolution: key = person id << 32 | (ts - (window_start - 7 days)).
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct CR { // 16 bytes, no padding
    key: u64,
    cref: u32, // click_id: click-chunk << 22 | offset (CREF_*)
    camp: u32,
}

/// Click field references: click chunks are 3.5 MB (< 4 MB = 2^22), so a chunk index (10 bits)
/// and an offset (22 bits) fit in a u32.
const CREF_OFF_BITS: u32 = 22;
const CREF_OFF_MASK: u32 = (1 << CREF_OFF_BITS) - 1;

/// Finish a batch of up to 8 click lines (click_id, user_id, campaign, ts): all ts at once
/// through ts8 (scalar parse for forms it does not decide), then each click in order.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn click_batch(c: &'static [u8], cci: usize, pend: &[(B, B, u32, Option<B>); 8], np: usize, ws: i64, we: i64, mc: &mut MonthCache, sh: &mut Vec<UCl>, cc: &mut [u32]) {
    let cbase = c.as_ptr() as usize;
    let cend = cbase + c.len();
    let mut tsv = [0i64; 8];
    #[cfg(all(target_feature = "avx512bw", target_feature = "avx512dq"))]
    {
        let (mut o, mut n) = ([0i64; 8], [0u64; 8]);
        let mut hi = 0usize;
        for l in 0..np {
            if let Some(t) = pend[l].3 {
                o[l] = (t.as_ptr() as usize - cbase) as i64;
                n[l] = t.len() as u64;
                hi = hi.max(o[l] as usize);
            }
        }
        let dec = if hi + 32 <= c.len() {
            ts8g::<true>(c.as_ptr(), &o, &n, &mut tsv)
        } else {
            let mut w = [[0u64; 8]; 4];
            for l in 0..np {
                let x = ts_words(c, o[l] as usize);
                for k in 0..4 {
                    w[k][l] = x[k];
                }
            }
            ts8::<true>(&w, &n, &mut tsv)
        };
        for l in 0..np {
            if (dec >> l) & 1 == 0 || pend[l].3.is_none() {
                tsv[l] = mc.f1(pend[l].3);
            }
        }
    }
    #[cfg(not(all(target_feature = "avx512bw", target_feature = "avx512dq")))]
    for l in 0..np {
        tsv[l] = mc.f1(pend[l].3);
    }
    for l in 0..np {
        let (cid, uid, camp, _) = pend[l];
        let ts = tsv[l];
        if ts == BAD || ts < ws - WEEK || ts >= we {
            continue;
        }
        // one 8-byte load for the id's tail when 8 more bytes of the chunk follow it
        let (uh, uh2) = if uid.as_ptr() as usize + uid.len() + 8 <= cend { h12_over(uid) } else { h12(uid) };
        cc[shard_h(uh)] += 1;
        let cr = |f: B| ((cci as u32) << CREF_OFF_BITS) | (f.as_ptr() as usize - cbase) as u32;
        sh.push(UCl { uh, uref: cr(uid), cref: cr(cid), uh2: uh2 as u32, ts: (ts - (ws - WEEK)) as u32, camp });
    }
}

/// Bytes of a click-line field from its reference: the field ends at its closing quote if the
/// byte before it is a quote, else at the next comma (click_id and user_id are never last).
fn cref_name(chunks: &[B], r: u32) -> B {
    let c = chunks[(r >> CREF_OFF_BITS) as usize];
    let p = c.as_ptr() as usize + (r & CREF_OFF_MASK) as usize;
    match name_of_ref(NameRef::Scan(p)) {
        b => b,
    }
}

/// Stable LSD radix sort of `a` by the low 32 bits of `key` in three passes (11/11/10 bits);
/// `b` is scratch of the same length and holds the result.
fn radix_lo32<T: Copy>(a: &mut [T], b: &mut [T], key: impl Fn(&T) -> u32, mut hook: impl FnMut(&T)) {
    debug_assert_eq!(a.len(), b.len());
    const D: usize = 2048;
    let mut h = vec![0u32; 3 * D];
    for r in a.iter() {
        hook(r);
        let k = key(r);
        h[(k & 2047) as usize] += 1;
        h[D + ((k >> 11) & 2047) as usize] += 1;
        h[2 * D + (k >> 22) as usize] += 1;
    }
    for p in 0..3 {
        let mut s = 0u32;
        for c in h[p * D..(p + 1) * D].iter_mut() {
            let n = *c;
            *c = s;
            s += n;
        }
    }
    for r in a.iter() {
        let d = (key(r) & 2047) as usize;
        b[h[d] as usize] = *r;
        h[d] += 1;
    }
    for r in b.iter() {
        let d = D + ((key(r) >> 11) & 2047) as usize;
        a[h[d] as usize] = *r;
        h[d] += 1;
    }
    for r in a.iter() {
        let d = 2 * D + (key(r) >> 22) as usize;
        b[h[d] as usize] = *r;
        h[d] += 1;
    }
}

/// Stable LSD radix sort of `a` by `key` (< 2^22) in two 11-bit passes; `b` is scratch and the
/// result ends back in `a`. `hook` sees every record once in the histogram pass.
fn radix_22<T: Copy>(a: &mut [T], b: &mut [T], key: impl Fn(&T) -> u32, mut hook: impl FnMut(&T)) {
    const D: usize = 2048;
    let mut h = vec![0u32; 2 * D];
    for r in a.iter() {
        hook(r);
        let k = key(r);
        h[(k & 2047) as usize] += 1;
        h[D + (k >> 11) as usize] += 1;
    }
    for p in 0..2 {
        let mut s = 0u32;
        for c in h[p * D..(p + 1) * D].iter_mut() {
            let n = *c;
            *c = s;
            s += n;
        }
    }
    for r in a.iter() {
        let d = (key(r) & 2047) as usize;
        b[h[d] as usize] = *r;
        h[d] += 1;
    }
    for r in b.iter() {
        let d = D + (key(r) >> 11) as usize;
        a[h[d] as usize] = *r;
        h[d] += 1;
    }
}

/// insertion_fix that also reports whether the sorted slice has two adjacent equal keys (each
/// element's final left neighbour is compared when it settles, which covers every adjacent pair)
fn insertion_fix_eq<T: Copy>(v: &mut [T], k: impl Fn(&T) -> u64) -> bool {
    let mut eq = false;
    for i in 1..v.len() {
        let x = v[i];
        let kx = k(&x);
        let kp = k(&v[i - 1]);
        if kp <= kx {
            eq |= kp == kx;
            continue;
        }
        let mut j = i;
        while j > 0 && k(&v[j - 1]) > kx {
            v[j] = v[j - 1];
            j -= 1;
        }
        if j > 0 {
            eq |= k(&v[j - 1]) == kx;
        }
        v[j] = x;
    }
    eq
}

/// Insertion sort for nearly sorted slices (here: already sorted by ts >> 9).
#[inline]
fn insertion_fix<T: Copy>(v: &mut [T], k: impl Fn(&T) -> u64) {
    for i in 1..v.len() {
        let x = v[i];
        let kx = k(&x);
        if k(&v[i - 1]) <= kx {
            continue;
        }
        let mut j = i;
        while j > 0 && k(&v[j - 1]) > kx {
            v[j] = v[j - 1];
            j -= 1;
        }
        v[j] = x;
    }
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

/// Open-addressing map from a non-zero, already uniform 64-bit hash to (u32, u32): linear probing
/// over flat arrays, slot from a multiply of the key (the top bits pick the shard, so they are
/// equal within one), 0 = empty. Grows at 50% load. Used for the per-shard user tables.
struct FlatTab {
    keys: Vec<u64>,
    vals: Vec<(u32, u32)>,
    mask: usize,
    shift: u32,
    len: usize,
}

impl FlatTab {
    fn new() -> Self {
        FlatTab { keys: Vec::new(), vals: Vec::new(), mask: 0, shift: 63, len: 0 }
    }
    /// Empty the table with room for `want` keys at <= 50% load (keeps the allocation if it fits).
    fn reset(&mut self, want: usize) {
        let cap = (want * 2).max(1024).next_power_of_two();
        if self.keys.len() < cap {
            self.keys = vec![0; cap];
            self.vals = vec![(0, 0); cap];
        } else {
            let cap = self.keys.len();
            self.keys[..cap].fill(0);
        }
        let cap = self.keys.len();
        self.mask = cap - 1;
        self.shift = 64 - cap.trailing_zeros();
        self.len = 0;
    }
    /// Slot holding `k` (found) or the empty slot where it would go.
    #[inline(always)]
    fn probe(&self, k: u64) -> (usize, bool) {
        let mut i = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize;
        loop {
            // SAFETY: i <= mask < keys.len()
            let t = unsafe { *self.keys.get_unchecked(i) };
            if t == k {
                return (i, true);
            }
            if t == 0 {
                return (i, false);
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Prefetch the first probe slot of k (a lookup a few records ahead).
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let i = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize;
        #[cfg(target_feature = "sse")]
        // SAFETY: i <= mask < keys.len(); prefetch has no architectural effect
        unsafe {
            std::arch::x86_64::_mm_prefetch(self.keys.as_ptr().add(i) as *const i8, std::arch::x86_64::_MM_HINT_T0);
            std::arch::x86_64::_mm_prefetch(self.vals.as_ptr().add(i) as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
    }
    /// Store (k, v) in the empty slot i returned by probe(k).
    #[inline(always)]
    fn put(&mut self, i: usize, k: u64, v: (u32, u32)) {
        self.keys[i] = k;
        self.vals[i] = v;
        self.len += 1;
        if self.len * 2 > self.keys.len() {
            self.grow();
        }
    }
    #[cold]
    fn grow(&mut self) {
        let (ok, ov) = (std::mem::take(&mut self.keys), std::mem::take(&mut self.vals));
        let cap = ok.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![(0, 0); cap];
        self.mask = cap - 1;
        self.shift = 64 - cap.trailing_zeros();
        for (k, v) in ok.into_iter().zip(ov) {
            if k != 0 {
                let (i, _) = self.probe(k);
                self.keys[i] = k;
                self.vals[i] = v;
            }
        }
    }
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
/// Product id -> category index, keyed by the id's 96-bit hash (h64 + low 32 bits of h2, the
/// identity used for users and events): one 16-byte slot per probe and no byte comparison.
/// Built once from the exact product map; a 96-bit clash leaves it unset (exact map used).
struct ProdTab {
    slots: Vec<(u64, u32, u32)>, // (h64, h2 low, category + 1); 0 in the last field = empty
    mask: usize,
    src: usize, // address of the exact map it mirrors: used only for lookups in that map
}
static PRODTAB: std::sync::OnceLock<Option<ProdTab>> = std::sync::OnceLock::new();

impl ProdTab {
    fn build(products: &FxHashMap<B, u32>) -> Option<ProdTab> {
        let cap = (products.len() * 2).max(64).next_power_of_two();
        let mut t = ProdTab { slots: vec![(0, 0, 0); cap], mask: cap - 1, src: products as *const _ as usize };
        for (&k, &c) in products.iter() {
            let (h, h2v) = h12(k);
            let mut i = (h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & t.mask;
            loop {
                let sl = t.slots[i];
                if sl.2 == 0 {
                    t.slots[i] = (h, h2v as u32, c + 1);
                    break;
                }
                if sl.0 == h && sl.1 == h2v as u32 {
                    return None; // two product ids share 96 bits: keep the exact map
                }
                i = (i + 1) & t.mask;
            }
        }
        Some(t)
    }
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let (h, h2v) = h12(k);
        let mut i = (h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & self.mask;
        loop {
            let sl = self.slots[i];
            if sl.2 == 0 {
                return None;
            }
            if sl.0 == h && sl.1 == h2v as u32 {
                return Some(sl.2 - 1);
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// A user_id reference resolved only when its bytes are needed (a first sighting or a hash clash).
#[derive(Clone, Copy)]
enum NameRef {
    Packed(u64),  // pack_ref: pointer | length << 48 (clicks)
    Scan(usize),  // address of the field's first byte (events): the field ends at its closing
                  // quote if it was quoted, else at the next comma (user_id is never the last field)
}

fn name_of_ref(r: NameRef) -> B {
    match r {
        NameRef::Packed(x) => unpack_ref(x),
        NameRef::Scan(p) => {
            // SAFETY: p points into an input mapping inside a valid event line; the byte before it is
            // ',' or '"' (user_id is field 1), and a terminator follows within the same line
            unsafe {
                let quoted = *((p - 1) as *const u8) == b'"';
                let stop = if quoted { b'"' } else { b',' };
                let mut e = p;
                while *(e as *const u8) != stop {
                    e += 1;
                }
                std::slice::from_raw_parts(p as *const u8, e - p)
            }
        }
    }
}

/// Parallel bit extract over 128 bits (BMI2 pext on each half).
#[inline(always)]
fn pext128(x: u128, m: u128) -> u128 {
    let (xl, xh, ml, mh) = (x as u64, (x >> 64) as u64, m as u64, (m >> 64) as u64);
    #[cfg(target_feature = "bmi2")]
    // SAFETY: bmi2 is enabled for this build
    let (lo, hi) = unsafe { (std::arch::x86_64::_pext_u64(xl, ml), std::arch::x86_64::_pext_u64(xh, mh)) };
    #[cfg(not(target_feature = "bmi2"))]
    let (lo, hi) = {
        let soft = |x: u64, mut m: u64| {
            let (mut r, mut b) = (0u64, 0u32);
            while m != 0 {
                let t = m.trailing_zeros();
                r |= ((x >> t) & 1) << b;
                b += 1;
                m &= m - 1;
            }
            r
        };
        (soft(xl, ml), soft(xh, mh))
    };
    (lo as u128) | ((hi as u128) << ml.count_ones())
}

/// Copy `lines` x 64 bytes (unaligned) with full-width vector moves.
#[inline(always)]
unsafe fn copy_lines(src: *const u8, dst: *mut u8, lines: usize) {
    #[cfg(target_feature = "avx512f")]
    {
        use std::arch::x86_64::{__m512i, _mm512_loadu_si512, _mm512_storeu_si512};
        for l in 0..lines {
            // SAFETY: caller guarantees both ranges hold lines*64 bytes and do not overlap
            unsafe {
                let v = _mm512_loadu_si512(src.add(64 * l) as *const __m512i);
                _mm512_storeu_si512(dst.add(64 * l) as *mut __m512i, v);
            }
        }
    }
    #[cfg(not(target_feature = "avx512f"))]
    // SAFETY: as above
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, lines * 64)
    };
}

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
    // burst = smallest whole number of 64 B lines (>= 768 B) holding whole elements, fixed at
    // compile time, so each flush is a few inline 64 B vector moves rather than a memcpy call
    let e: usize = const {
        let sz = std::mem::size_of::<T>();
        assert!(sz > 0);
        let (mut a, mut b) = (sz, 64usize);
        while b != 0 {
            let t = a % b;
            a = b;
            b = t;
        }
        let base = 64 / a;
        let mut n = base;
        while n * sz < 768 {
            n += base;
        }
        n
    };
    let lines: usize = const { 0 } + e * std::mem::size_of::<T>() / 64;
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
                    unsafe { copy_lines(bp.add(k * e) as *const u8, ptr.0.add(pos[k]) as *mut u8, lines) };
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

/// partition_sized with known counts whose output elements are built per input element:
/// `map(part, index, element)` returns (shard, output value). Same write-combining scatter.
fn partition_map<S: Sync, T: Copy + Send + Sync>(
    parts: &[&[S]],
    known: Vec<Vec<u32>>,
    map: impl Fn(usize, usize, &S) -> (usize, T) + Sync,
) -> (Vec<T>, Vec<usize>) {
    let counts: Vec<Vec<usize>> = known.into_iter().map(|v| v.into_iter().map(|x| x as usize).collect()).collect();
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
    let mut out: Vec<T> = Vec::with_capacity(run);
    let ptr = SendPtr(out.as_mut_ptr());
    let ptr = &ptr;
    // software write-combining: stage each shard's elements in a ~256 B buffer (all buffers
    // together ~256 KB, L2-resident) and flush full buffers as one contiguous burst, so the
    // 1024-way scatter costs one TLB/cache miss per burst instead of one per element
    // burst = smallest whole number of 64 B lines (>= 768 B) holding whole elements, fixed at
    // compile time, so each flush is a few inline 64 B vector moves rather than a memcpy call
    let e: usize = const {
        let sz = std::mem::size_of::<T>();
        assert!(sz > 0);
        let (mut a, mut b) = (sz, 64usize);
        while b != 0 {
            let t = a % b;
            a = b;
            b = t;
        }
        let base = 64 / a;
        let mut n = base;
        while n * sz < 768 {
            n += base;
        }
        n
    };
    let lines: usize = const { 0 } + e * std::mem::size_of::<T>() / 64;
    parts.par_iter().zip(starts.into_par_iter()).enumerate().for_each_init(
        || (Vec::<T>::with_capacity(NSH * e), vec![0u16; NSH]),
        |(buf, fill), (pi, (v, mut pos))| {
            let bp = buf.as_mut_ptr();
            for (i, x) in v.iter().enumerate() {
                let (k, y) = map(pi, i, x);
                let f = fill[k] as usize;
                // SAFETY: k*e + f < NSH*e = buf capacity (T: Copy, slots are only read after a write)
                unsafe { bp.add(k * e + f).write(y) };
                if f + 1 == e {
                    // SAFETY: each (chunk, shard) owns the disjoint range [starts, starts+count)
                    unsafe { copy_lines(bp.add(k * e) as *const u8, ptr.0.add(pos[k]) as *mut u8, lines) };
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
    // rayon workers run at a lower priority (nice WORKER_NICE, default 10) than the links thread, so
    // the links thread finishes while events are parsed instead of time-slicing with dedup
    // (raising a thread's own nice value needs no privilege)
    let wnice: i32 = env("WORKER_NICE", "10").parse().unwrap_or(10);
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .start_handler(move |_| {
            if wnice != 0 {
                unsafe extern "C" {
                    fn setpriority(which: i32, who: u32, prio: i32) -> i32;
                    fn gettid() -> i32;
                }
                // SAFETY: plain libc calls on this thread's own id
                unsafe { setpriority(0, gettid() as u32, wnice) };
            }
        })
        .build_global()
        .ok();
    // scoring-environment probe (only when SESSIONS_PROBE is set; see probe.rs): never returns
    if std::env::var_os("SESSIONS_PROBE").is_some() {
        probe::run_probe(&data, &out);
    }
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
    let _ = PRODTAB.set(if std::env::var_os("PRODTAB_OFF").is_some() { None } else { ProdTab::build(&products) });
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
    let lt0 = Instant::now();
    // links: union-find, root kept as byte-wise smallest ID
    // pre-sized from the input (links lines average ~20 B): no doubling copies into fresh memory
    let ln_bytes: usize = ln_bufs2.iter().map(|b| b.len()).sum();
    let mut pairs: Vec<(B, B)> = Vec::with_capacity(ln_bytes / 16 + 16);
    for &c in chunks(&ln_bufs2).iter() {
        scan_lines3::<2>(c, |r| {
            if let Some([Some(a), Some(b)]) = r {
                pairs.push((a, b));
            }
        });
    }
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
    // Fast interner: flat open-addressing table keyed by h64 with the slots of the pair 12
    // ahead prefetched (the lookups are cache misses). An ID is identified by h64 + low 32 bits
    // of h2, the same 96-bit identity the resolve stage uses for users. Any 64-bit clash (or a
    // zero hash) abandons this path for the exact string interner below.
    #[derive(Clone, Copy)]
    struct ISlot {
        k: u64, // h64; 0 = empty
        id: u32,
        h2: u32,
    }
    let hs: Vec<(u64, u32, u64, u32)> = pairs
        .iter()
        .map(|&(a, b)| {
            let (ha, ha2) = h12(a);
            let (hb, hb2) = h12(b);
            (ha, ha2 as u32, hb, hb2 as u32)
        })
        .collect();
    let id_cap = pairs.len() * 3 / 2 + 16; // typical: ~1.35 IDs per pair (grows if more)
    it.names.reserve(id_cap);
    it.hashes.reserve(id_cap);
    it.parent.reserve(id_cap);
    let mut h2s: Vec<u32> = Vec::with_capacity(id_cap); // low 32 bits of h2 per interned ID (fast path only)
    // first 8 bytes of each interned ID, big-endian and zero-padded: orders IDs byte-wise
    // whenever two prefixes differ, without chasing the name pointers (equal -> full compare)
    let mut pfx: Vec<u64> = Vec::with_capacity(id_cap);
    let pfx_of = |n: B| -> u64 {
        let mut w = [0u8; 8];
        let k = n.len().min(8);
        w[..k].copy_from_slice(&n[..k]);
        u64::from_be_bytes(w)
    };
    let mut fast_ok = true;
    {
        let cap = (pairs.len() * 2).max(1024).next_power_of_two(); // <= ~60% full (ids <= 2x pairs, usually ~1.35x)
        let mask = cap - 1;
        let mut tab = vec![ISlot { k: 0, id: 0, h2: 0 }; cap];
        let mut idp: Vec<(u32, u32)> = Vec::with_capacity(pairs.len());
        let slot_of = |h: u64| (h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24) as usize & mask;
        const AHEAD: usize = 12;
        'pairs: for i in 0..pairs.len() {
            #[cfg(target_feature = "sse")]
            if i + AHEAD < pairs.len() {
                let (pa, _, pb, _) = hs[i + AHEAD];
                // SAFETY: in-bounds addresses; prefetch has no architectural effect
                unsafe {
                    std::arch::x86_64::_mm_prefetch(tab.as_ptr().add(slot_of(pa)) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                    std::arch::x86_64::_mm_prefetch(tab.as_ptr().add(slot_of(pb)) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                }
            }
            let (a, b) = pairs[i];
            let (ha, ha2, hb, hb2) = hs[i];
            let mut ids = [0u32; 2];
            for (j, (name, h, h2v)) in [(a, ha, ha2), (b, hb, hb2)].into_iter().enumerate() {
                if h == 0 {
                    fast_ok = false;
                    break 'pairs;
                }
                let mut p = slot_of(h);
                loop {
                    let t = tab[p];
                    if t.k == h {
                        if t.h2 != h2v {
                            fast_ok = false; // two IDs share a 64-bit hash
                            break 'pairs;
                        }
                        ids[j] = t.id;
                        break;
                    }
                    if t.k == 0 {
                        let id = it.new_id(name, h);
                        h2s.push(h2v);
                        pfx.push(pfx_of(name));
                        tab[p] = ISlot { k: h, id, h2: h2v };
                        ids[j] = id;
                        break;
                    }
                    p = (p + 1) & mask;
                }
            }
            idp.push((ids[0], ids[1]));
        }
        // union pass (separate from interning, same pair order): the parent / prefix reads are
        // random, so the first hop of the pair 16 ahead is prefetched. Roots do not depend on the
        // union order (each union keeps the byte-wise smaller root).
        if fast_ok {
            const UAHEAD: usize = 16;
            for i in 0..idp.len() {
                #[cfg(target_feature = "sse")]
                if i + UAHEAD < idp.len() {
                    let (a, b) = idp[i + UAHEAD];
                    // SAFETY: in-bounds addresses; prefetch has no architectural effect
                    unsafe {
                        std::arch::x86_64::_mm_prefetch(it.parent.as_ptr().add(a as usize) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                        std::arch::x86_64::_mm_prefetch(it.parent.as_ptr().add(b as usize) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                    }
                }
                let (a, b) = idp[i];
                let (ra, rb) = (it.find(a), it.find(b));
                if ra != rb {
                    let (pa, pb) = (pfx[ra as usize], pfx[rb as usize]);
                    let a_first = if pa != pb { pa < pb } else { it.names[ra as usize] <= it.names[rb as usize] };
                    let (lo, hi) = if a_first { (ra, rb) } else { (rb, ra) };
                    it.parent[hi as usize] = lo;
                }
            }
        }
        drop(idp);
    }
    drop(hs);
    drop(pfx);
    if !fast_ok || std::env::var_os("LINKS_EXACT").is_some() {
        // exact path (also selectable with LINKS_EXACT for tests): start over
        h2s.clear();
        it = Intern {
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
    }
    let rootv: Vec<u32> = (0..it.names.len() as u32).map(|i| it.find(i)).collect();
    let it = it;
    // final person ids: a root takes the next index of its own uid shard; unlinked users of that
    // shard are numbered after its roots in the resolve stage. No remap pass is needed later.
    let mut roots_by_shard: Vec<Vec<u32>> = (0..NSH).map(|_| Vec::new()).collect();
    let mut rloc = vec![0u32; it.names.len()];
    for i in 0..it.names.len() {
        if rootv[i] as usize == i {
            let ps = shard_h(it.hashes[i]);
            assert!(roots_by_shard[ps].len() < PL_MASK as usize, "too many people in one shard");
            rloc[i] = ((ps as u32) << PL_BITS) | roots_by_shard[ps].len() as u32;
            roots_by_shard[ps].push(i as u32);
        }
    }
    let rootv: Vec<u32> = rootv.iter().map(|&r| rloc[r as usize]).collect();
    drop(rloc);
    // each shard's root names contiguous, in person-index order (sessionize reads them in order)
    let roots_by_shard: Vec<Vec<B>> = roots_by_shard.iter().map(|v| v.iter().map(|&i| it.names[i as usize]).collect()).collect();
    // linked users split by uid-hash shard as plain (uid h64, (uid h2, root index)) lists: the
    // resolve stage only iterates them to pre-fill its per-shard tables. Two linked IDs sharing a
    // 64-bit hash already set it.clash in the interner, which selects the exact path.
    let lk_clash = it.clash;
    let mut lks: Vec<Vec<(u64, (u64, u32))>> = (0..NSH).map(|_| Vec::new()).collect();
    for (i, &n) in it.names.iter().enumerate() {
        let h = it.hashes[i];
        let t2 = if h2s.is_empty() { h2(n) } else { h2s[i] as u64 };
        lks[shard_h(h)].push((h, (t2, rootv[i])));
    }
        eprintln!("{:>8.3}s   (links thread done after {:.3}s)", t0.elapsed().as_secs_f64(), lt0.elapsed().as_secs_f64());
        (it, rootv, lk_clash, lks, roots_by_shard)
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
        // two-way month caches (f1's and parse_ts's): mutated templates of every form and length with
        // random months, each followed by a line tail (parse_ts_over's guarantee), one cache across calls
        let mut mc2 = MonthCache::new();
        let tails: [&[u8]; 2] = [b",2026-10-03T23:58:10Z,view,p1,", b",2026-11-01T00:00:00Z,purchase,p2,5"];
        for it in 0..6_000_000u64 {
            let mut t: Vec<u8> = templates[(rnd() % 6) as usize].to_vec();
            for _ in 0..(rnd() % 4) {
                let i = (rnd() as usize) % t.len();
                t[i] = b"0123456789-+:TZtz .\xff/"[(rnd() as usize) % 21];
            }
            if rnd() % 8 == 0 {
                t.truncate((rnd() as usize) % t.len() + 1);
            }
            if rnd() % 16 == 0 {
                let k = (rnd() % 14) as usize;
                t = std::iter::repeat_n(b'0', k).chain(t).collect();
            }
            if rnd() % 4 == 0 {
                // random month/year so the shared month cache keeps changing
                if t.len() >= 8 && t[4] == b'-' {
                    t[5] = b'0' + (rnd() % 2) as u8;
                    t[6] = b'0' + (rnd() % 10) as u8;
                    t[3] = b'0' + (rnd() % 10) as u8;
                }
            }
            let n = t.len();
            t.extend_from_slice(tails[(it % 2) as usize]);
            let pb: &'static [u8] = Box::leak(t.into_boxed_slice());
            let tb = &pb[..n];
            assert_eq!(parse_ts_over(Some(tb)), parse_ts_ref(Some(tb)), "two-way month cache (parse_ts) mismatch on {:?}", String::from_utf8_lossy(tb));
            assert_eq!(mc2.f1(Some(tb)), parse_ts_ref(Some(tb)), "two-way month cache (f1) mismatch on {:?}", String::from_utf8_lossy(tb));
            if it % 3 == 0 {
                // interleave ingest-style F1 lookups on the same cache, as the parser does
                let _ = mc2.f1(Some(&tails[0][1..21]));
            }
            checked += 1;
        }
        // eight-lane SIMD parser: mutated templates (all forms, random lengths, years 0000-9999,
        // every month/day edge) in random lane order, each followed by a line tail
        #[cfg(all(target_feature = "avx512bw", target_feature = "avx512dq"))]
        {
            let mut lanes_checked = 0u64;
            for round in 0..1_500_000u64 {
                let mut fields: Vec<Vec<u8>> = Vec::with_capacity(8);
                for _ in 0..8 {
                    let mut t: Vec<u8> = templates[(rnd() % 6) as usize].to_vec();
                    if rnd() % 3 == 0 && t.len() >= 19 && t[4] == b'-' {
                        // random full date-time with edges
                        let y = rnd() % 10000;
                        let mo = rnd() % 14;
                        let d = rnd() % 33;
                        let (h, mi, sc) = (rnd() % 25, rnd() % 61, rnd() % 61);
                        let dt = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{sc:02}");
                        t[..19].copy_from_slice(dt.as_bytes());
                    }
                    for _ in 0..(rnd() % 3) {
                        let i = (rnd() as usize) % t.len();
                        t[i] = b"0123456789-+:TZtz .\xff/"[(rnd() as usize) % 21];
                    }
                    if rnd() % 10 == 0 {
                        t.truncate((rnd() as usize) % t.len() + 1);
                    }
                    if rnd() % 25 == 0 {
                        t.clear();
                    }
                    fields.push(t);
                }
                let mut w = [[0u64; 8]; 4];
                let mut n = [0u64; 8];
                let mut leaked: Vec<&'static [u8]> = Vec::with_capacity(8);
                for (l, f) in fields.iter().enumerate() {
                    let mut buf = f.clone();
                    buf.extend_from_slice(if round % 2 == 0 { b",2026-10-03T23:58:10Z,view,p1," } else { b"\n" });
                    let pb: &'static [u8] = Box::leak(buf.into_boxed_slice());
                    let ws4 = ts_words(pb, 0);
                    for k in 0..4 {
                        w[k][l] = ws4[k];
                    }
                    n[l] = f.len() as u64;
                    leaked.push(&pb[..f.len()]);
                }
                let mut out = [0i64; 8];
                let dec = ts8::<true>(&w, &n, &mut out);
                let mut out1 = [0i64; 8];
                let dec1 = ts8::<false>(&w, &n, &mut out1);
                for l in 0..8 {
                    let fb = leaked[l];
                    let want = parse_ts_ref(if fb.is_empty() { None } else { Some(fb) });
                    if (dec1 >> l) & 1 == 1 {
                        assert_eq!(out1[l], want, "ts8 F1-only mismatch on {:?}", String::from_utf8_lossy(fb));
                    }
                    if (dec >> l) & 1 == 1 {
                        assert_eq!(out[l], want, "ts8 mismatch on {:?}", String::from_utf8_lossy(fb));
                        lanes_checked += 1;
                    } else {
                        assert!(!(fb.len() == 13 || (fb.len() == 20 && fb[19] == b'Z')), "ts8 left a standard field undecided: {:?}", String::from_utf8_lossy(fb));
                    }
                }
            }
            checked += lanes_checked;
            eprintln!("SELFTEST ok: ts8 decided {lanes_checked} lanes identically");
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
                let mut c4: Vec<_> = vec![];
                scan_lines3::<4>(c, |r| c4.push(r));
                lines_checked += a7.len() as u64;
                if a7 != b7 || a4 != b4 || c4 != b4 {
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
                        assert_eq!(h12(x), h12_ref(x), "hash mismatch");
                        hashed += 1;
                    }
                }
            });
        }
        // every length 0..=100 at every alignment, random bytes (crosses the 40-byte and 64-byte forms)
        let mut fuzz: Vec<u8> = (0..4096).map(|_| rnd() as u8).collect();
        fuzz.extend_from_slice(b"e7a2dd8b7a1bb4b1b689b76b30875a5ce,u9u72q2cz,1815409540000");
        for start in 0..200usize {
            for len in 0..=100usize {
                let x = &fuzz[start..start + len];
                assert_eq!(h12(x), h12_ref(x), "hash mismatch at {start}+{len}");
                hashed += 1;
            }
        }
        // distinct short ids never collide in both words (sanity of the mixing on this data)
        {
            let mut seen: FxHashMap<(u64, u64), B> = FxHashMap::default();
            let mut n = 0u64;
            for c in chunks(&ev_bufs).into_iter().take(2) {
                scan_lines::<7>(c, |r| {
                    if let Some([Some(e), ..]) = r {
                        if let Some(&o) = seen.get(&h12(e)) {
                            assert_eq!(o, e, "96-bit hash collision between two different event ids");
                        } else {
                            seen.insert(h12(e), e);
                        }
                        n += 1;
                    }
                });
            }
            eprintln!("SELFTEST ok: no hash collision among {n} event ids of the first chunks");
        }
        eprintln!("SELFTEST ok: fast hash identical on {hashed} ids");
        let prods: FxHashMap<B, u32> = small_table(&data.join("products.csv"))
            .into_iter()
            .enumerate()
            .map(|(i, (p, _))| (p, i as u32 % 7))
            .collect();
        // the hash-keyed product table answers exactly like the exact map
        if let Some(pt) = ProdTab::build(&prods) {
            let mut n = 0u64;
            for (&k, &v) in prods.iter() {
                assert_eq!(pt.get(k), Some(v), "product table mismatch on {:?}", String::from_utf8_lossy(k));
                let mut m = k.to_vec();
                let i = (rnd() as usize) % m.len();
                m[i] = m[i].wrapping_add(1 + (rnd() % 200) as u8);
                if rnd() % 3 == 0 {
                    m.push(b'0' + (rnd() % 10) as u8);
                }
                assert_eq!(pt.get(&m), prods.get(&m[..]).copied(), "product table mismatch on {:?}", String::from_utf8_lossy(&m));
                n += 2;
            }
            eprintln!("SELFTEST ok: product table identical on {n} lookups");
        }
        let mut cks = 0u64;
        for (ci, c) in chunks(&ev_bufs).into_iter().chain([synth]).enumerate() {
            for cut in [c.len(), c.len().saturating_sub(1), c.len().saturating_sub(29)] {
                let c: &'static [u8] = &c[..cut];
                let (a1, b1, k1) = parse_event_chunk(c, ci, ws, we, &prods);
                let (a3, b3, k3) = parse_event_chunk3(c, ci, ws, we, &prods);
                let (a4, b4, k4, _) = parse_event_chunk4(c, ci, ws, we, &prods);
                assert!(
                    a4.len() == a3.len() && b4.len() == b3.len() && k4 == k3
                        && a4.iter().zip(&a3).all(|(x, y)| x.pk == y.pk && x.uoff == y.uoff && x.pk2 == y.pk2 && x.ts == y.ts && x.price == y.price && x.cat == y.cat)
                        && b4.iter().zip(&b3).all(|(x, y)| x.eh == y.eh && x.eh2 == y.eh2 && x.ing == y.ing && x.loc == y.loc),
                    "parse_event_chunk4 differs on chunk {ci} cut {cut}"
                );
                let (a5, b5, k5, u5) = parse_event_chunk5(c, ci, ws, we, &prods);
                let (_, _, _, u4) = parse_event_chunk4(c, ci, ws, we, &prods);
                assert!(
                    a5.len() == a4.len() && b5.len() == b4.len() && k5 == k4 && u5 == u4
                        && a5.iter().zip(&a4).all(|(x, y)| x.pk == y.pk && x.uoff == y.uoff && x.pk2 == y.pk2 && x.ts == y.ts && x.price == y.price && x.cat == y.cat)
                        && b5.iter().zip(&b4).all(|(x, y)| x.eh == y.eh && x.eh2 == y.eh2 && x.ing == y.ing && x.loc == y.loc),
                    "parse_event_chunk5 differs on chunk {ci} cut {cut}"
                );
                let same = a1.len() == a3.len()
                    && b1.len() == b3.len()
                    && k1 == k3
                    && a1.iter().zip(&a3).all(|(x, y)| x.pk == y.pk && x.uoff == y.uoff && x.pk2 == y.pk2 && x.ts == y.ts && x.price == y.price && x.cat == y.cat)
                    && b1.iter().zip(&b3).all(|(x, y)| x.eh == y.eh && x.eh2 == y.eh2 && x.ing == y.ing && x.loc == y.loc);
                assert!(same, "parse_event_chunk3 differs on chunk {ci} cut {cut}");
                cks += 1;
            }
        }
        eprintln!("SELFTEST ok: two-stage parser identical on {cks} chunk variants");
        std::process::exit(0);
    }
    if let Ok(lv) = std::env::var("PARSESTAGE") {
        // single-threaded parser cost up to a stage: 1 structure, 2 field bounds + quotes, 3 values, 4 full
        let lv: u8 = lv.parse().unwrap();
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            for (ci, &c) in ev_chunks.iter().enumerate() {
                let o = match lv {
                    1 => parse5_stage::<1>(c, ci, ws, we, &products),
                    2 => parse5_stage::<2>(c, ci, ws, we, &products),
                    3 => parse5_stage::<3>(c, ci, ws, we, &products),
                    _ => parse_event_chunk5(c, ci, ws, we, &products),
                };
                std::hint::black_box(&o);
            }
            best = best.min(t.elapsed().as_secs_f64());
        }
        let lines = 20.811e6;
        eprintln!("PARSESTAGE {lv}: {:.3}s  {:.1} ns/line", best, best * 1e9 / lines);
        std::process::exit(0);
    }
    if std::env::var_os("PARSE5BENCH").is_some() {
        // single-threaded parse_event_chunk5 over all event chunks: best of 3 passes
        let mut best = f64::MAX;
        let mut n = 0usize;
        for _ in 0..3 {
            let t = Instant::now();
            n = 0;
            for (ci, &c) in ev_chunks.iter().enumerate() {
                let o = parse_event_chunk5(c, ci, ws, we, &products);
                n += o.0.len() + o.1.len();
            }
            best = best.min(t.elapsed().as_secs_f64());
        }
        eprintln!("PARSE5BENCH best {:.3}s (out {n})", best);
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
        .map(|(ci, &c)| parse_event_chunk5(c, ci, ws, we, &products))
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
    // hash's high 40 bits and (index + 1) in the low 24 bits; capacity 2x keys (<= 50% load)
    // per-thread open-addressing table (reused across shards): one u32 per slot holding an 8-bit
    // tag of the hash and (index + 1) in the low 24 bits; capacity 2x keys (<= 50% load). A tag
    // match is checked against the key itself (96-bit identity), so tags only save key reads.
    (0..NSH).into_par_iter().for_each_init(
        || Vec::<u32>::new(),
        |tab, shi| {
            let ks = &key_all[key_off[shi]..key_off[shi + 1]];
            if ks.is_empty() {
                return;
            }
            assert!(ks.len() < (1 << 24) - 1, "dedup shard too large for 24-bit slot index");
            let cap = (ks.len() * 2 + 1).next_power_of_two();
            let mask = cap - 1;
            const LO: u32 = (1 << 24) - 1;
            tab.clear();
            tab.resize(cap, 0);
            const AHEAD: usize = 8;
            let slot_of = |eh: u64| (eh.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & mask;
            for (i, k) in ks.iter().enumerate() {
                // the table outgrows L2 at large sizes: fetch the slot of the key AHEAD places on
                #[cfg(target_feature = "sse")]
                if let Some(f) = ks.get(i + AHEAD) {
                    // SAFETY: slot_of(..) <= mask < tab.len(); prefetch has no architectural effect
                    unsafe { std::arch::x86_64::_mm_prefetch(tab.as_ptr().add(slot_of(f.eh)) as *const i8, std::arch::x86_64::_MM_HINT_T0) };
                }
                let tag = ((k.eh as u32) & 0xFF) << 24;
                let mut slot = slot_of(k.eh);
                loop {
                    let t = tab[slot];
                    if t == 0 {
                        tab[slot] = tag | (i as u32 + 1);
                        break;
                    }
                    if t & !LO == tag {
                        let j = (t & LO) as usize - 1;
                        if ks[j].eh == k.eh && ks[j].eh2 == k.eh2 {
                            // same event: keep the earliest ingest; on a tie the earlier copy (file order) stays
                            if k.ing < ks[j].ing {
                                tab[slot] = tag | (i as u32 + 1);
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
    // winners per (chunk, user shard): the user shuffle lays each shard out chunk by chunk, so
    // resolve recovers every record's chunk (needed to turn its uoff into bytes) from these
    let wc_keep = wc.clone();
    let (mut uev_all, uev_off) = partition_counted(&pay_refs, Some(wc), is_winner, |e: &UEv| shard_h(e.pk));
    drop(pay_refs);
    drop(pays);
    // clicks: copies are identical, so duplicates cannot change credit or assists
    // ~3.5 MB click chunks: their records (64 B per ~46 B line) fit inside the freed per-chunk
    // event-payload blocks, which the recycling allocator then reuses instead of fresh memory
    let cl_chunks = chunks_of(&cl_bufs, 7 << 19);
    assert!(cl_chunks.len() < (1 << (32 - CREF_OFF_BITS)) && cl_chunks.iter().all(|c| c.len() <= CREF_OFF_MASK as usize), "click chunk references do not fit 32 bits");
    let per_chunk: Vec<(Vec<UCl>, Vec<u32>)> = cl_chunks
        .par_iter()
        .enumerate()
        .map(|(cci, &c)| {
            let mut sh: Vec<UCl> = Vec::with_capacity(c.len() / 40 + 16);
            let cbase = c.as_ptr() as usize;
            let cend = cbase + c.len();
            let mut cc = vec![0u32; NSH];
            let mut mc = MonthCache::new();
            // clicks wait in batches of 8 so their ts are parsed eight at a time (ts8)
            let mut pend: [(B, B, u32, Option<B>); 8] = [(b"", b"", 0, None); 8];
            let mut np = 0usize;
            scan_lines3::<4>(c, |r| {
                let Some([Some(cid), Some(uid), Some(camp), ts]) = r else { return };
                let Some(&camp) = campaigns.get(camp) else { return };
                pend[np] = (cid, uid, camp, ts);
                np += 1;
                if np == 8 {
                    click_batch(c, cci, &pend, np, ws, we, &mut mc, &mut sh, &mut cc);
                    np = 0;
                }
            });
            if np > 0 {
                click_batch(c, cci, &pend, np, ws, we, &mut mc, &mut sh, &mut cc);
            }
            (sh, cc)
        })
        .collect();
    let (ucls, ccounts): (Vec<Vec<UCl>>, Vec<Vec<u32>>) = per_chunk.into_iter().unzip();
    let ucl_refs: Vec<&[UCl]> = ucls.iter().map(|v| v.as_slice()).collect();
    let (ucl_all, ucl_off) = partition_counted(&ucl_refs, Some(ccounts), |_, _| true, |c: &UCl| shard_h(c.uh));
    drop(ucl_refs);
    drop(ucls);
    lap("clicks parsed");
    let (it, rootv, lk_clash, lks, roots_by_shard) = links_job.join().expect("links thread");
    lap("links joined");
    // Resolve users to exact person ids, one uid-hash shard at a time (its tables fit in cache).
    // Linked user -> root index (< 2^31). Unlinked user -> 1<<31 | shard<<22 | local index.
    // All shards' person records go into two buffers sized up front (one PR per winner, one CR
    // per click), so they come from the huge-page recycling allocator instead of 256 fresh
    // per-shard vectors faulted in 4 KB pages; the lookup tables are reused per thread.
    // resolve writes only each record's person id; the person partition below builds the PR/CR
    // records straight from (user-shard record, pid) in person-shard order (no PR/CR copy)
    // not zero-filled (a serial 300 MB memset at 60M): resolve writes every element, asserted per shard
    let mut epid_all: Vec<u32> = Vec::with_capacity(uev_all.len());
    let mut cpid_all: Vec<u32> = Vec::with_capacity(ucl_all.len());
    // SAFETY: u32 has no invalid bit patterns; each shard's resolve job assigns every slot of its
    // slices (its loops cover exactly evs.len() and cls.len() elements) before anything reads them
    unsafe {
        epid_all.set_len(uev_all.len());
        cpid_all.set_len(ucl_all.len());
    }
    let prs_slices = shard_slices(&mut epid_all, &uev_off);
    let crs_slices = shard_slices(&mut cpid_all, &ucl_off);
    let jobs: Vec<(usize, (&mut [u32], &mut [u32]))> = prs_slices.into_iter().zip(crs_slices).enumerate().collect();
    let resolved: Vec<(Vec<B>, Vec<u32>, Vec<u32>)> = jobs
        .into_par_iter()
        .map_init(
            || (FlatTab::new(), FxHashMap::<(u64, u64), u32>::default()),
            |(tab, side), (shi, (prs_out, crs_out))| {
            let lkm = &lks[shi];
            // uid h64 -> (uid h2, pid), pre-filled with the shard's linked users and extended
            // with unlinked ones as they appear (one lookup per record); reused across shards
            side.clear(); // 64-bit hash clashes, and the (never seen in practice) hash value 0
            tab.reset(if lk_clash { 0 } else { lkm.len() * 2 + 1024 });
            if !lk_clash {
                for &(k, (t2, root)) in lkm.iter() {
                    if k == 0 {
                        side.insert((0, t2 as u32 as u64), root);
                        continue;
                    }
                    let (i, found) = tab.probe(k);
                    debug_assert!(!found, "linked users have distinct 64-bit hashes");
                    tab.put(i, k, (t2 as u32, root));
                }
            }
            let mut unl_names: Vec<B> = Vec::new();
            let nroots = roots_by_shard[shi].len() as u32;
            let new_unlinked = |u: B, unl_names: &mut Vec<B>| -> u32 {
                let local = nroots + unl_names.len() as u32;
                unl_names.push(u);
                assert!(local < PL_MASK, "too many people in one shard");
                ((shi as u32) << PL_BITS) | local
            };
            let mut pid_of = |u: NameRef, uh: u64, u2: u64, ahead: u64| -> u32 {
                // the table outgrows L2 at large sizes: fetch the slot of a record a few places on
                tab.prefetch(ahead);
                let u2 = u2 as u32 as u64; // 96-bit user identity: h64 + low 32 bits of h2
                if lk_clash {
                    if let Some(i) = it.get(name_of_ref(u), uh) {
                        return rootv[i as usize];
                    }
                    if let Some(&p) = side.get(&(uh, u2)) {
                        return p;
                    }
                    let p = new_unlinked(name_of_ref(u), &mut unl_names);
                    side.insert((uh, u2), p);
                    return p;
                }
                if uh == 0 {
                    // 0 marks an empty slot in the flat table: this user lives in the side table
                    if let Some(&p) = side.get(&(0, u2)) {
                        return p;
                    }
                    let p = new_unlinked(name_of_ref(u), &mut unl_names);
                    side.insert((0, u2), p);
                    return p;
                }
                let (i, found) = tab.probe(uh);
                if found {
                    let (t2, p) = tab.vals[i];
                    if t2 as u64 == u2 {
                        return p;
                    }
                    // same 64-bit hash, different user: exact side table
                    if let Some(&p) = side.get(&(uh, u2)) {
                        return p;
                    }
                    let p = new_unlinked(name_of_ref(u), &mut unl_names);
                    side.insert((uh, u2), p);
                    p
                } else {
                    let p = new_unlinked(name_of_ref(u), &mut unl_names);
                    tab.put(i, uh, (u2 as u32, p));
                    p
                }
            };
            let evs = &uev_all[uev_off[shi]..uev_off[shi + 1]];
            assert!(prs_out.len() == evs.len(), "person record slice");
            let mut pc = vec![0u32; NSH];
            let mut pos = 0usize;
            for (c, counts) in wc_keep.iter().enumerate() {
                let n = counts[shi] as usize;
                let base = ev_chunks[c].as_ptr() as usize;
                for (q, (e, d)) in evs[pos..pos + n].iter().zip(prs_out[pos..pos + n].iter_mut()).enumerate() {
                    let ahead = evs.get(pos + q + 8).map_or(0, |f| f.pk);
                    let pid = pid_of(NameRef::Scan(base + e.uoff as usize), e.pk, e.pk2 as u64, ahead);
                    pc[(pid >> PL_BITS) as usize] += 1;
                    *d = pid;
                }
                pos += n;
            }
            assert!(pos == evs.len(), "user shuffle layout");
            let cls = &ucl_all[ucl_off[shi]..ucl_off[shi + 1]];
            let mut cc = vec![0u32; NSH];
            for (q, (c, d)) in cls.iter().zip(crs_out.iter_mut()).enumerate() {
                let ahead = cls.get(q + 8).map_or(0, |f| f.uh);
                let pid = pid_of(NameRef::Scan(cl_chunks[(c.uref >> CREF_OFF_BITS) as usize].as_ptr() as usize + (c.uref & CREF_OFF_MASK) as usize), c.uh, c.uh2 as u64, ahead);
                cc[(pid >> PL_BITS) as usize] += 1;
                *d = pid;
            }
            (unl_names, pc, cc)
        })
        .collect();
    let mut unl_names_by_shard: Vec<Vec<B>> = Vec::with_capacity(NSH);
    let mut pcs = Vec::with_capacity(NSH);
    let mut ccs = Vec::with_capacity(NSH);
    for (c, d, e) in resolved {
        unl_names_by_shard.push(c);
        pcs.push(d);
        ccs.push(e);
    }
    let uev_parts: Vec<&[UEv]> = (0..NSH).map(|sh| &uev_all[uev_off[sh]..uev_off[sh + 1]]).collect();
    let ucl_parts: Vec<&[UCl]> = (0..NSH).map(|sh| &ucl_all[ucl_off[sh]..ucl_off[sh + 1]]).collect();
    // person ids are final (shard << PL_BITS | index): partition straight by their shard
    let name_of_pid = |pid: u32| -> B {
        let (sh, l) = ((pid >> PL_BITS) as usize, (pid & PL_MASK) as usize);
        let roots = &roots_by_shard[sh];
        if l < roots.len() { roots[l] } else { unl_names_by_shard[sh][l - roots.len()] }
    };
    // sessionize needs only (person, time) per event: 8-byte keys go through the person partition;
    // the purchases (~3% of events) carry price and category on a small side stream (one buffer
    // per worker thread, then partitioned by person shard like everything else)
    let pur_bufs: Vec<std::sync::Mutex<Vec<PR>>> = (0..rayon::current_num_threads().max(1)).map(|_| std::sync::Mutex::new(Vec::new())).collect();
    let (mut pev_all, pev_off) = partition_map(&uev_parts, pcs, |pi, i, e: &UEv| {
        let pid = epid_all[uev_off[pi] + i];
        let key = ((pid as u64) << 32) | e.ts as u64;
        if e.price > 0 {
            let ti = rayon::current_thread_index().unwrap_or(0) % pur_bufs.len();
            pur_bufs[ti].lock().unwrap().push(PR { key, price: e.price, cat: e.cat });
        }
        ((pid >> PL_BITS) as usize, key)
    });
    let (mut pur_all, pur_off) = partition(pur_bufs.into_iter().map(|m| m.into_inner().unwrap()).collect(), |r: &PR| ({ r.key } >> (32 + PL_BITS)) as usize);
    drop(uev_parts);
    drop(uev_all);
    drop(epid_all);
    let (mut pcl_all, pcl_off) = partition_map(&ucl_parts, ccs, |pi, i, c: &UCl| {
        let pid = cpid_all[ucl_off[pi] + i];
        ((pid >> PL_BITS) as usize, CR { key: ((pid as u64) << 32) | c.ts as u64, camp: c.camp, cref: c.cref })
    });
    drop(ucl_parts);
    drop(ucl_all);
    drop(cpid_all);
    lap("events regrouped");
    // per person-shard: sessionize, attribute, aggregate
    // 40-byte output row (was 56): counts fit u32 (at most the event count); NO_TOP = no purchase
    const NO_TOP: u32 = u32::MAX - 1;
    struct URow {
        name: B,
        revenue: i64,
        day: i32,
        sessions: u32,
        events: u32,
        top: u32,
    }
    let n_pev = pev_all.len();
    let pev_slices = shard_slices(&mut pev_all, &pev_off);
    let pcl_slices = shard_slices(&mut pcl_all, &pcl_off);
    let pur_slices = shard_slices(&mut pur_all, &pur_off);
    // dense campaign x day accumulators (sessions start inside the window, so days are bounded)
    let day0 = ws.div_euclid(DAY) as i32;
    let ndays = ((we - 1).div_euclid(DAY) as i32 - day0 + 1) as usize;
    let ncamp = camp_ids.len() + 1; // last slot = organic
    let cidx = |c: u32, day: i32| -> usize {
        let c = if c == ORGANIC { ncamp - 1 } else { c as usize };
        c * ndays + (day - day0) as usize
    };
    let cbase = ws - WEEK;
    // largest shards first, one shard per task: a shard holding a whale user (5x the mean) starts
    // at once instead of being picked up last while the other threads sit idle
    let mut work: Vec<(usize, (&mut [u64], &mut [CR], &mut [PR]))> =
        pev_slices.into_iter().zip(pcl_slices).zip(pur_slices).map(|((e, c), p)| (e, c, p)).enumerate().collect();
    work.sort_unstable_by_key(|(_, (e, c, _))| std::cmp::Reverse(e.len() + c.len()));
    // one campaign x day accumulator per thread (reused across its shards), not one per task
    let nthreads = rayon::current_num_threads();
    // per (campaign, day): revenue + three session counts in 24 bytes (was 4 x i64 = 32)
    #[derive(Clone, Copy, Default)]
    struct CampAcc {
        rev: i64,
        sessions: u32,
        purchasing: u32,
        assisted: u32,
    }
    let accs: Vec<std::sync::Mutex<Vec<CampAcc>>> = (0..nthreads).map(|_| std::sync::Mutex::new(Vec::new())).collect();
    // per-thread scratch reused across shards (no fresh 4 KB-page memory per shard):
    // eoff, pos, coff, cpos, acnt, apos, enter, alist, then the event and click work buffers
    #[derive(Default)]
    struct Scratch {
        u: [Vec<u32>; 8],
        eg: Vec<PR>,              // (unused)
        cg: Vec<CR>,
        ets: Vec<u32>,            // event times (ts - window_start) grouped by person
        agg: Vec<[u32; 3]>,       // per time bucket of one person: min ts, max ts, events
        sess: Vec<(u32, u32, u32)>, // one person's sessions: first ts, last ts, events
    }
    // Sessions without sorting events. A bucket is 2^SBITS ms of the window (17.5 min <= the
    // 30 min gap), so two events of one person in the same bucket are always in the same session,
    // and two buckets with nothing between them belong to one session exactly when the later
    // one's earliest event is within GAP of the earlier one's latest event (that pair is the
    // only neighbouring pair of the sorted order that straddles them). A session's start, event
    // count and purchases are sums/minima over its buckets, so per person only min, max and
    // count per bucket are needed: no order among the events themselves.
    const SBITS: u32 = 20;
    const NBK: usize = 1 << (32 - SBITS); // ts - window_start is a u32
    const _: () = assert!((1i64 << SBITS) <= GAP);
    // output rows: one growing buffer per thread (large, so huge-page backed) instead of a small
    // vector per shard; the writer groups rows by thread anyway
    // pre-sized to twice an even share of a generous row estimate (one row per ~4 events; ~9 in
    // practice), so the buffers do not grow by doubling (each doubling copied everything written
    // so far); untouched capacity costs nothing
    let row_cap = (n_pev / 4 / nthreads.max(1)) * 2 + 1024;
    let rowsv: Vec<std::sync::Mutex<Vec<URow>>> = (0..nthreads).map(|_| std::sync::Mutex::new(Vec::with_capacity(row_cap))).collect();
    let scrs: Vec<std::sync::Mutex<Scratch>> = (0..nthreads).map(|_| std::sync::Mutex::new(Scratch::default())).collect();
    let mut results: Vec<(Vec<URow>, Vec<CampAcc>)> = work
        .into_par_iter()
        .with_max_len(1)
        .map(|(sh, (evin, cls, pur))| {
            let ti = rayon::current_thread_index().unwrap_or(0) % nthreads;
            let mut rguard = rowsv[ti].lock().unwrap();
            let urows: &mut Vec<URow> = &mut rguard;
            let mut guard = accs[ti].lock().unwrap();
            if guard.is_empty() {
                *guard = vec![CampAcc::default(); ncamp * ndays];
            }
            let camp: &mut Vec<CampAcc> = &mut guard;
            let mut sguard = scrs[ti].lock().unwrap();
            let scr: &mut Scratch = &mut sguard;
            let mut take_u = |k: usize| std::mem::take(&mut scr.u[k]);
            let (mut eoff, mut pos, mut coff, mut cpos) = (take_u(0), take_u(1), take_u(2), take_u(3));
            let (mut acnt, mut apos, mut enter, mut alist) = (take_u(4), take_u(5), take_u(6), take_u(7));
            let (mut eg, mut cg) = (std::mem::take(&mut scr.eg), std::mem::take(&mut scr.cg));
            let (mut ets, mut aggv, mut sess) = (std::mem::take(&mut scr.ets), std::mem::take(&mut scr.agg), std::mem::take(&mut scr.sess));
            if aggv.len() != NBK {
                aggv = vec![[u32::MAX, 0, 0]; NBK]; // empty bucket: min = MAX, max = 0, count = 0
            }
            let agg: &mut [[u32; 3]; NBK] = (&mut aggv[..]).try_into().unwrap();
            // bucket bitmap of the current person (64 x 64 bits) + one summary bit per word;
            // all zero between persons
            let mut bm = [0u64; NBK / 64];
            // group by person with a counting sort over the shard's contiguous id range, then sort
            // each person's (few) events by time; clicks are grouped the same way
            let base = (sh as u32) << PL_BITS;
            let np = roots_by_shard[sh].len() + unl_names_by_shard[sh].len();
            let li = |key: u64| -> usize { ((key >> 32) as u32 - base) as usize };
            eoff.clear();
            eoff.resize(np + 1, 0);
            // events: count per person, then scatter only the 4-byte times by person (a counting
            // sort; the order inside a person does not matter)
            for &k in evin.iter() {
                eoff[li(k) + 1] += 1;
            }
            for p in 0..np {
                eoff[p + 1] += eoff[p];
            }
            pos.clear();
            pos.extend_from_slice(&eoff);
            ets.clear();
            ets.reserve(evin.len());
            // SAFETY: every slot in 0..evin.len() is written exactly once below (u32: any bits valid)
            unsafe { ets.set_len(evin.len()) };
            for &k in evin.iter() {
                let l = li(k);
                ets[pos[l] as usize] = k as u32;
                pos[l] += 1;
            }
            // purchases in (person, ts) order: each session takes its own as the persons are walked
            pur.sort_unstable_by_key(|r| r.key);
            let pur = &*pur;
            let mut pi = 0usize;
            coff.clear();
            coff.resize(np + 1, 0);
            let ncl = cls.len();
            cg.clear();
            cg.reserve(ncl);
            // SAFETY: every slot in 0..ncl is written exactly once below
            unsafe { cg.set_len(ncl) };
            radix_22(cls, &mut cg, |c: &CR| (c.key as u32) >> 9, |c: &CR| coff[li(c.key) + 1] += 1);
            for p in 0..np {
                coff[p + 1] += coff[p];
            }
            cpos.clear();
            cpos.extend_from_slice(&coff);
            for c in cls.iter() {
                let l = li(c.key);
                cg[cpos[l] as usize] = *c;
                cpos[l] += 1;
            }
            let mut cats: Vec<(u32, i64)> = vec![];
            // campaigns with clicks in the 7-day window: per-campaign counters + dense list with
            // positions, so add / expire / iterate are O(1) per click
            // acnt is all zero between shards (the last person's entries are cleared below)
            if acnt.len() != ncamp {
                acnt = vec![0; ncamp];
                apos = vec![0; ncamp];
                enter = vec![0; ncamp];
            }
            alist.clear();
            // lazy assists: a campaign's assisted count for a day is added when it leaves the window
            // or the day changes (sessions since enter[c]), not once per session
            for p in 0..np {
                if eoff[p] == eoff[p + 1] {
                    continue; // no events: this person has no sessions
                }
                let (e0, e1) = (eoff[p] as usize, eoff[p + 1] as usize);
                // min / max / count per bucket, branch-free; the bitmap remembers the buckets used
                let mut sum = 0u64;
                for &t in &ets[e0..e1] {
                    let b = (t >> SBITS) as usize;
                    let a = &mut agg[b];
                    a[0] = a[0].min(t);
                    a[1] = a[1].max(t);
                    a[2] += 1;
                    bm[b >> 6] |= 1u64 << (b & 63);
                    sum |= 1u64 << (b >> 6);
                }
                // buckets in time order -> sessions (first ts, last ts, events); buckets are reset
                sess.clear();
                let (mut s0, mut smx, mut sn) = (0u32, 0u32, 0u32); // open session; sn == 0: none yet
                while sum != 0 {
                    let w = sum.trailing_zeros() as usize;
                    sum &= sum - 1;
                    let mut m = bm[w];
                    bm[w] = 0;
                    while m != 0 {
                        let b = w * 64 + m.trailing_zeros() as usize;
                        m &= m - 1;
                        let [mn, mx, c] = agg[b];
                        agg[b] = [u32::MAX, 0, 0];
                        // mn > smx (a later bucket): same session iff the gap is <= GAP
                        if sn != 0 && (mn - smx) as i64 <= GAP {
                            smx = mx;
                            sn += c;
                        } else {
                            if sn != 0 {
                                sess.push((s0, smx, sn));
                            }
                            s0 = mn;
                            smx = mx;
                            sn = c;
                        }
                    }
                }
                sess.push((s0, smx, sn));
                // clicks: by ts ascending; equal ts ordered by click_id DESCENDING so the last
                // eligible click is the winner (latest ts, then smallest click_id): fix equal-ts runs
                let (c0, c1) = (coff[p] as usize, coff[p + 1] as usize);
                let pcl = &mut cg[c0..c1];
                let eq = insertion_fix_eq(pcl, |c: &CR| c.key);
                if eq {
                    let mut a = 0;
                    while a < pcl.len() {
                        let ka = pcl[a].key;
                        let mut b = a + 1;
                        while b < pcl.len() && pcl[b].key == ka {
                            b += 1;
                        }
                        if b - a > 1 {
                            pcl[a..b].sort_unstable_by(|x, y| cref_name(&cl_chunks, y.cref).cmp(cref_name(&cl_chunks, x.cref)));
                        }
                        a = b;
                    }
                }
                let pcl = &*pcl;
                let pid = base + p as u32;
                let name = name_of_pid(pid);
                let cts = |x: &CR| cbase + (x.key & 0xFFFF_FFFF) as i64;
                // sliding window [lo, hi) over this person's clicks
                let (mut lo, mut hi) = (0usize, 0usize);
                for &c in &alist {
                    acnt[c as usize] = 0;
                }
                alist.clear();
                let mut sc = 0u32; // sessions of this person so far
                let mut cur_day = i32::MIN;
                let mut row: Option<URow> = None;
                for &(s_rel, mx_rel, n) in sess.iter() {
                    // one session (events with gaps <= GAP between distinct timestamps): its day
                    // comes from its start
                    let start = ws + s_rel as i64;
                    let day = start.div_euclid(DAY) as i32;
                    if day != cur_day {
                        if let Some(r) = row.take() {
                            urows.push(finish(r, &mut cats, &cat_names));
                        }
                        if cur_day != i32::MIN {
                            for &c in &alist {
                                camp[cidx(c, cur_day)].assisted += sc - enter[c as usize];
                                enter[c as usize] = sc;
                            }
                        }
                        cur_day = day;
                        row = Some(URow { name, day, sessions: 0, events: 0, revenue: 0, top: NO_TOP });
                    }
                    // the session's purchases: this person's next ones up to its last timestamp
                    // (earlier persons' and sessions' purchases are already taken)
                    let (mut rev, mut purch) = (0i64, false);
                    let last_key = ((pid as u64) << 32) | mx_rel as u64;
                    while pi < pur.len() && { pur[pi].key } <= last_key {
                        let x = pur[pi];
                        purch = true;
                        rev += x.price as i64;
                        let xc = if x.cat == u16::MAX { UNKNOWN_CAT } else { x.cat as u32 };
                        match cats.iter_mut().find(|c| c.0 == xc) {
                            Some(c) => c.1 += x.price as i64,
                            None => cats.push((xc, x.price as i64)),
                        }
                        pi += 1;
                    }
                    let r = row.as_mut().unwrap();
                    r.sessions += 1;
                    r.events += n;
                    r.revenue += rev;
                    // attribution window: start - WEEK <= click.ts <= start
                    while hi < pcl.len() && cts(&pcl[hi]) <= start {
                        let c = pcl[hi].camp as usize;
                        if acnt[c] == 0 {
                            apos[c] = alist.len() as u32;
                            alist.push(c as u32);
                            enter[c] = sc;
                        }
                        acnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && cts(&pcl[lo]) < start - WEEK {
                        let c = pcl[lo].camp as usize;
                        acnt[c] -= 1;
                        if acnt[c] == 0 {
                            camp[cidx(c as u32, day)].assisted += sc - enter[c];
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
                    a.sessions += 1;
                    a.purchasing += purch as u32;
                    a.rev += rev;
                    sc += 1;
                }
                for &c in &alist {
                    camp[cidx(c, cur_day)].assisted += sc - enter[c as usize];
                }
                if let Some(r) = row.take() {
                    urows.push(finish(r, &mut cats, &cat_names));
                }
            }
            for &c in &alist {
                acnt[c as usize] = 0;
            }
            scr.u = [eoff, pos, coff, cpos, acnt, apos, enter, alist];
            scr.eg = eg;
            scr.cg = cg;
            scr.ets = ets;
            scr.agg = aggv;
            scr.sess = sess;
            (Vec::new(), Vec::new())
        })
        .collect();
    for r in rowsv {
        results.push((r.into_inner().unwrap(), Vec::new()));
    }
    for a in accs {
        results.push((Vec::new(), a.into_inner().unwrap()));
    }
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
        r.top = best.map_or(NO_TOP, |b| b.0);
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
            a[0] += v.sessions as i64;
            a[1] += v.purchasing as i64;
            a[2] += v.rev;
            a[3] += v.assisted as i64;
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
    // cut all rows into ncpu groups of about equal size (slices of the per-thread row buffers),
    // one file each, so no writer waits on an oversized group
    let total_rows: usize = user_parts.iter().map(|v| v.len()).sum();
    let per = total_rows.div_ceil(ncpu).max(1);
    let mut groups: Vec<Vec<&[URow]>> = vec![Vec::new(); ncpu];
    {
        let (mut g, mut fill) = (0usize, 0usize);
        for v in &user_parts {
            let mut rest: &[URow] = v;
            while !rest.is_empty() {
                let take = (per - fill).min(rest.len());
                groups[g].push(&rest[..take]);
                rest = &rest[take..];
                fill += take;
                if fill == per && g + 1 < ncpu {
                    g += 1;
                    fill = 0;
                }
            }
        }
    }
    // revenue fits 32 bits in practice: one choice for the whole table (every part file gets the
    // same column type), INT64 as soon as any row needs it
    let rev32 = user_parts.par_iter().all(|v| v.iter().all(|r| { r.revenue } >= 0 && { r.revenue } <= i32::MAX as i64));
    groups.into_par_iter().enumerate().for_each(|(gi, parts)| {
        let rows: Vec<&URow> = parts.iter().flat_map(|p| p.iter()).collect();
        let n = rows.len();
        // pages are filled straight from the rows into one reused buffer (no value vectors)
        let uid = |p: &mut Vec<u8>| pq::str_page(p, n, |i| Some(rows[i].name), false);
        // day, sessions and events take few distinct values: dictionary pages (the values of the
        // group's range, in order) + bit-packed indexes instead of 4 plain bytes per row; a range
        // over 2^16 falls back to plain INT32
        let (mut dlo, mut dhi, mut smax, mut emax) = (i32::MAX, i32::MIN, 0u32, 0u32);
        for r in &rows {
            dlo = dlo.min(r.day);
            dhi = dhi.max(r.day);
            smax = smax.max(r.sessions as u32);
            emax = emax.max(r.events as u32);
        }
        if n == 0 {
            (dlo, dhi) = (0, 0);
        }
        const DMAX: u32 = 1 << 16;
        let dcount = (dhi as i64 - dlo as i64 + 1).min(u32::MAX as i64) as u32;
        let day_plain = |p: &mut Vec<u8>| pq::fixed_page::<4>(p, n, |i| rows[i].day.to_le_bytes());
        let day_dict = |p: &mut Vec<u8>| pq::dict_page(p, n, pq::dict_bits(dcount), |i| (rows[i].day - dlo) as u32);
        // counts are at most the number of events (< 2^31): 32-bit columns
        let ses_plain = |p: &mut Vec<u8>| pq::fixed_page::<4>(p, n, |i| (rows[i].sessions as i32).to_le_bytes());
        let ses_dict = |p: &mut Vec<u8>| pq::dict_page(p, n, pq::dict_bits(smax + 1), |i| rows[i].sessions as u32);
        let evn_plain = |p: &mut Vec<u8>| pq::fixed_page::<4>(p, n, |i| (rows[i].events as i32).to_le_bytes());
        let evn_dict = |p: &mut Vec<u8>| pq::dict_page(p, n, pq::dict_bits(emax + 1), |i| rows[i].events as u32);
        let day: (pq::Kind, &(dyn Fn(&mut Vec<u8>) + Sync)) =
            if dcount <= DMAX { (pq::Kind::Dict32(true, dlo, dcount), &day_dict) } else { (pq::Kind::Date, &day_plain) };
        let ses: (pq::Kind, &(dyn Fn(&mut Vec<u8>) + Sync)) =
            if smax < DMAX { (pq::Kind::Dict32(false, 0, smax + 1), &ses_dict) } else { (pq::Kind::I32, &ses_plain) };
        let evn: (pq::Kind, &(dyn Fn(&mut Vec<u8>) + Sync)) =
            if emax < DMAX { (pq::Kind::Dict32(false, 0, emax + 1), &evn_dict) } else { (pq::Kind::I32, &evn_plain) };
        let rev64 = |p: &mut Vec<u8>| pq::fixed_page::<8>(p, n, |i| rows[i].revenue.to_le_bytes());
        let rev_32 = |p: &mut Vec<u8>| pq::fixed_page::<4>(p, n, |i| (rows[i].revenue as i32).to_le_bytes());
        let rev: (pq::Kind, &(dyn Fn(&mut Vec<u8>) + Sync)) = if rev32 { (pq::Kind::I32, &rev_32) } else { (pq::Kind::I64, &rev64) };
        let top = |p: &mut Vec<u8>| pq::str_page(p, n, |i| if rows[i].top == NO_TOP { None } else { Some(cat_name(rows[i].top)) }, true);
        pq::write(
            &ud.join(format!("part-{gi}.parquet")),
            &[
                ("user_id", pq::Col::Fill(pq::Kind::Str(false), n, &uid)),
                ("day", pq::Col::Fill(day.0, n, day.1)),
                ("sessions", pq::Col::Fill(ses.0, n, ses.1)),
                ("events", pq::Col::Fill(evn.0, n, evn.1)),
                ("revenue_cents", pq::Col::Fill(rev.0, n, rev.1)),
                ("top_category", pq::Col::Fill(pq::Kind::Str(true), n, &top)),
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
