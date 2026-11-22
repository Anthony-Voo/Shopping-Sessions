//! Global allocator: large blocks (>= BIG) come from our own anonymous mappings with
//! madvise(MADV_HUGEPAGE), so they fault and zero 2 MB at a time instead of 4 KB (glibc's
//! heap left ~90% of our memory on 4 KB pages: one fault + zeroing per 4 KB written).
//! Freed large blocks are kept and reused (best fit), and realloc grows in place when the
//! block already has room. Small allocations go to the system allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, Ordering};

const BIG: usize = 1 << 20;
const HUGE: usize = 2 << 20;
const SLOTS: usize = 4096;

pub struct Recycler;

struct Pool {
    free: [(usize, usize); SLOTS], // (ptr, mapped size); ptr 0 = empty
    live: [(usize, usize); SLOTS],
}

static LOCK: AtomicBool = AtomicBool::new(false);
static mut POOL: Pool = Pool { free: [(0, 0); SLOTS], live: [(0, 0); SLOTS] };

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn mremap(old: *mut u8, old_len: usize, new_len: usize, flags: i32, new_addr: *mut u8) -> *mut u8;
}
const MREMAP_MAYMOVE: i32 = 1;
const MREMAP_FIXED: i32 = 2;
const MAP_FIXED: i32 = 0x10;
static REMAP_OFF: AtomicBool = AtomicBool::new(false);
static REMAP_INIT: AtomicBool = AtomicBool::new(false);

/// Build a contiguous block of `len` bytes (a multiple of 2 MB) out of freed blocks by moving
/// their pages with mremap (page-table moves: no copy and no zeroing); any shortfall is fresh.
/// Called with the pool lock held. Returns null if nothing could be gathered.
unsafe fn gather(len: usize) -> *mut u8 {
    let pl = pool();
    let total: usize = pl.free.iter().filter(|s| s.0 != 0).map(|s| s.1).sum();
    if total < HUGE {
        return std::ptr::null_mut();
    }
    // reserve a 2 MB-aligned address range
    let raw = unsafe { mmap(std::ptr::null_mut(), len + HUGE, 0, MAP_PRIVATE_ANON, -1, 0) };
    if raw as isize == -1 {
        return std::ptr::null_mut();
    }
    let base = (raw as usize).div_ceil(HUGE) * HUGE;
    let head = base - raw as usize;
    if head > 0 {
        unsafe { munmap(raw, head) };
    }
    let tail = HUGE - head;
    if tail > 0 {
        unsafe { munmap((base + len) as *mut u8, tail) };
    }
    let mut off = 0usize;
    while off < len {
        // largest free block first
        let mut bi: Option<usize> = None;
        for (i, s) in pl.free.iter().enumerate() {
            if s.0 != 0 && bi.is_none_or(|b| pl.free[b].1 < s.1) {
                bi = Some(i);
            }
        }
        let Some(i) = bi else { break };
        let (p, sz) = pl.free[i];
        let take = sz.min(len - off);
        let r = unsafe { mremap(p as *mut u8, take, take, MREMAP_MAYMOVE | MREMAP_FIXED, (base + off) as *mut u8) };
        if r as isize == -1 {
            break;
        }
        if take < sz {
            pl.free[i] = (p + take, sz - take);
        } else {
            pl.free[i] = (0, 0);
        }
        off += take;
    }
    if off < len {
        // the rest is fresh memory, advised for huge pages like map_huge
        let r = unsafe { mmap((base + off) as *mut u8, len - off, PROT_RW, MAP_PRIVATE_ANON | MAP_FIXED, -1, 0) };
        if r as isize == -1 {
            unsafe { munmap(base as *mut u8, len) };
            return std::ptr::null_mut();
        }
        unsafe { madvise((base + off) as *mut u8, len - off, MADV_HUGEPAGE) };
    }
    base as *mut u8
}
const PROT_RW: i32 = 0x1 | 0x2;
const MAP_PRIVATE_ANON: i32 = 0x02 | 0x20;
const MADV_HUGEPAGE: i32 = 14;

fn lock() {
    while LOCK.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
        std::hint::spin_loop();
    }
}
fn unlock() {
    LOCK.store(false, Ordering::Release);
}
fn pool() -> &'static mut Pool {
    unsafe { &mut *std::ptr::addr_of_mut!(POOL) }
}

/// Map `size` bytes (rounded to 2 MB) aligned to 2 MB, advised for transparent huge pages.
unsafe fn map_huge(size: usize) -> (*mut u8, usize) {
    let len = size.div_ceil(HUGE) * HUGE;
    let raw = unsafe { mmap(std::ptr::null_mut(), len + HUGE, PROT_RW, MAP_PRIVATE_ANON, -1, 0) };
    if raw as isize == -1 {
        return (std::ptr::null_mut(), 0);
    }
    // trim to a 2 MB-aligned window
    let start = (raw as usize).div_ceil(HUGE) * HUGE;
    let head = start - raw as usize;
    if head > 0 {
        unsafe { munmap(raw, head) };
    }
    let tail = HUGE - head;
    if tail > 0 {
        unsafe { munmap((start + len) as *mut u8, tail) };
    }
    unsafe { madvise(start as *mut u8, len, MADV_HUGEPAGE) };
    (start as *mut u8, len)
}

fn track_live(p: usize, size: usize) -> bool {
    for s in pool().live.iter_mut() {
        if s.0 == 0 {
            *s = (p, size);
            return true;
        }
    }
    false
}

fn live_size(p: usize) -> Option<usize> {
    pool().live.iter().find(|s| s.0 == p).map(|s| s.1)
}

unsafe impl GlobalAlloc for Recycler {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() < BIG || layout.align() > HUGE {
            return unsafe { System.alloc(layout) };
        }
        lock();
        let pl = pool();
        let mut best: Option<usize> = None;
        for (i, s) in pl.free.iter().enumerate() {
            if s.0 != 0 && s.1 >= layout.size() && best.is_none_or(|b| pl.free[b].1 > s.1) {
                best = Some(i);
            }
        }
        if let Some(i) = best {
            let (p, sz) = pl.free[i];
            if track_live(p, sz) {
                pl.free[i] = (0, 0);
                unlock();
                return p as *mut u8;
            }
        }
        if !REMAP_INIT.swap(true, Ordering::Relaxed) {
            REMAP_OFF.store(std::env::var_os("REMAP_OFF").is_some(), Ordering::Relaxed);
        }
        if !REMAP_OFF.load(Ordering::Relaxed) {
            let len = layout.size().div_ceil(HUGE) * HUGE;
            let p = unsafe { gather(len) };
            if !p.is_null() {
                if track_live(p as usize, len) {
                    unlock();
                    return p;
                }
                unsafe { munmap(p, len) };
            }
        }
        let (p, sz) = unsafe { map_huge(layout.size()) };
        if !p.is_null() && !track_live(p as usize, sz) {
            // bookkeeping full: give the mapping back and use the system allocator
            unsafe { munmap(p, sz) };
            unlock();
            return unsafe { System.alloc(layout) };
        }
        unlock();
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { self.alloc(layout) };
        if !p.is_null() {
            unsafe { std::ptr::write_bytes(p, 0, layout.size()) };
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() < BIG || layout.align() > HUGE {
            return unsafe { System.dealloc(ptr, layout) };
        }
        lock();
        let pl = pool();
        let mut real = None;
        for s in pl.live.iter_mut() {
            if s.0 == ptr as usize {
                real = Some(s.1);
                *s = (0, 0);
                break;
            }
        }
        match real {
            Some(sz) => {
                for s in pl.free.iter_mut() {
                    if s.0 == 0 {
                        *s = (ptr as usize, sz);
                        unlock();
                        return;
                    }
                }
                unlock();
                unsafe { munmap(ptr, sz) };
            }
            None => {
                unlock();
                unsafe { System.dealloc(ptr, layout) }; // came from the system fallback
            }
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if layout.size() >= BIG && layout.align() <= HUGE {
            lock();
            let cap = live_size(ptr as usize);
            unlock();
            if let Some(cap) = cap {
                if new_size <= cap {
                    return ptr; // the mapping already has room: grow (or shrink) in place
                }
            }
        }
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        let np = unsafe { self.alloc(new_layout) };
        if !np.is_null() {
            unsafe { std::ptr::copy_nonoverlapping(ptr, np, layout.size().min(new_size)) };
            unsafe { self.dealloc(ptr, layout) };
        }
        np
    }
}
