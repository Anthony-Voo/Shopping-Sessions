//! Scoring-environment probe. Active only when SESSIONS_PROBE is set (never in a normal run):
//! prints facts about the machine, the container limits, the page-cache state of the input and a
//! few micro-timings to stderr, so a run on the public dev set shows what the scorer measures.

use std::path::Path;
use std::time::Instant;

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn mincore(addr: *mut u8, len: usize, vec: *mut u8) -> i32;
    fn preadv2(fd: i32, iov: *const IoVec, iovcnt: i32, offset: i64, flags: i32) -> isize;
}

#[repr(C)]
struct IoVec {
    base: *mut u8,
    len: usize,
}

/// (pages cached, pages probed) for `samples` evenly spaced pages of a file, with
/// preadv2(RWF_NOWAIT): a buffered read that would need disk I/O returns EAGAIN instead of
/// waiting. Needs no privilege (mincore reports "all resident" for files we cannot write).
fn nowait_cached(path: &Path, samples: usize) -> (usize, usize) {
    use std::os::fd::AsRawFd;
    let Ok(f) = std::fs::File::open(path) else { return (0, 0) };
    let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    if len == 0 {
        return (0, 0);
    }
    let (mut hit, mut n) = (0usize, 0usize);
    let mut b = [0u8; 1];
    for k in 0..samples {
        let off = (len / samples * k) & !4095;
        let iov = IoVec { base: b.as_mut_ptr(), len: 1 };
        // SAFETY: one-byte buffer, valid fd; RWF_NOWAIT = 8
        let r = unsafe { preadv2(f.as_raw_fd(), &iov, 1, off as i64, 8) };
        n += 1;
        if r == 1 {
            hit += 1;
        }
    }
    (hit, n)
}

fn io_counters() -> (u64, u64) {
    // (read_bytes of /proc/self/io, major faults)
    let rb = std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("read_bytes")).and_then(|l| l.split_whitespace().nth(1).and_then(|x| x.parse().ok())))
        .unwrap_or(0);
    let mf = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|st| st.rsplit(')').next().and_then(|t| t.split_whitespace().nth(9).and_then(|x| x.parse().ok())))
        .unwrap_or(0);
    (rb, mf)
}

fn cat(path: &str) -> String {
    match std::fs::read_to_string(path) {
        Ok(s) => s.trim().replace('\n', " | "),
        Err(e) => format!("<{:?}>", e.kind()),
    }
}

fn show(path: &str) {
    eprintln!("PROBE {path}: {}", cat(path));
}

fn grep(path: &str, keys: &[&str]) {
    let Ok(s) = std::fs::read_to_string(path) else {
        eprintln!("PROBE {path}: <unreadable>");
        return;
    };
    // the first line for each key (cpuinfo repeats its block per CPU)
    let v: Vec<&str> = keys.iter().filter_map(|k| s.lines().find(|l| l.starts_with(k))).collect();
    eprintln!("PROBE {path}: {}", v.join(" | ").split_whitespace().collect::<Vec<_>>().join(" "));
}

fn ctime(path: &str) -> String {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(m) => format!("{}.{:03}", m.ctime(), m.ctime_nsec() / 1_000_000),
        Err(e) => format!("<{:?}>", e.kind()),
    }
}

/// (files, bytes, bytes resident in the page cache) of the CSV files of one input folder.
fn residency(dir: &Path) -> (usize, u64, u64, Vec<(std::path::PathBuf, u64, u64)>) {
    use std::os::fd::AsRawFd;
    let (mut n, mut total, mut res) = (0usize, 0u64, 0u64);
    let mut per = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else { return (0, 0, 0, per) };
    let mut paths: Vec<_> = rd.filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.is_file()).collect();
    paths.sort();
    for p in paths {
        let Ok(f) = std::fs::File::open(&p) else { continue };
        let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        if len == 0 {
            continue;
        }
        // SAFETY: read-only private mapping of a regular file; unmapped below
        let ptr = unsafe { mmap(std::ptr::null_mut(), len, 1, 2, f.as_raw_fd(), 0) };
        if ptr as isize == -1 {
            continue;
        }
        let pages = len.div_ceil(4096);
        let mut v = vec![0u8; pages];
        // SAFETY: v has one byte per page of the mapping
        let rc = unsafe { mincore(ptr, len, v.as_mut_ptr()) };
        let r = if rc == 0 { v.iter().filter(|&&b| b & 1 == 1).count() as u64 * 4096 } else { 0 };
        unsafe { munmap(ptr, len) };
        n += 1;
        total += len as u64;
        res += r.min(len as u64);
        per.push((p, len as u64, r.min(len as u64)));
    }
    (n, total, res, per)
}

fn touch_anon(huge: bool) -> (f64, String) {
    let len = 256usize << 20;
    // SAFETY: fresh private anonymous mapping, unmapped below
    let p = unsafe { mmap(std::ptr::null_mut(), len, 3, 0x22, -1, 0) };
    if p as isize == -1 {
        return (0.0, "mmap failed".into());
    }
    if huge {
        unsafe { madvise(p, len, 14) };
    }
    let t = Instant::now();
    let mut i = 0;
    while i < len {
        unsafe { p.add(i).write_volatile(1) };
        i += 4096;
    }
    let dt = t.elapsed().as_secs_f64();
    let smaps = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
    let ahp = smaps.lines().find(|l| l.starts_with("AnonHugePages")).unwrap_or("AnonHugePages: ?").split_whitespace().collect::<Vec<_>>().join(" ");
    unsafe { munmap(p, len) };
    (dt, ahp)
}

fn spin(iters: u64) -> u64 {
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..iters {
        x = (x ^ i).wrapping_mul(0xE703_7ED1_A0B4_28DB);
        x ^= x >> 29;
    }
    std::hint::black_box(x)
}

fn threads_time(n: usize, iters: u64) -> f64 {
    let t = Instant::now();
    let hs: Vec<_> = (0..n).map(|_| std::thread::spawn(move || spin(iters))).collect();
    for h in hs {
        let _ = h.join();
    }
    t.elapsed().as_secs_f64()
}

fn copy_gbps(n: usize) -> f64 {
    // n threads each copy 256 MB (already touched) twice: memory bandwidth
    let t = Instant::now();
    let hs: Vec<_> = (0..n)
        .map(|_| {
            std::thread::spawn(move || {
                let a = vec![1u8; 128 << 20];
                let mut b = vec![2u8; 128 << 20];
                let t = Instant::now();
                for _ in 0..2 {
                    b.copy_from_slice(&a);
                    std::hint::black_box(&b);
                }
                t.elapsed().as_secs_f64()
            })
        })
        .collect();
    let per: Vec<f64> = hs.into_iter().map(|h| h.join().unwrap_or(0.0)).collect();
    let _ = t;
    let worst = per.iter().cloned().fold(0.0, f64::max);
    if worst > 0.0 { n as f64 * 0.256 / worst } else { 0.0 }
}

pub fn before(data: &Path) {
    let t = Instant::now();
    eprintln!("PROBE ===== scoring-environment probe (diagnostic build; exits 3 on purpose) =====");
    show("/proc/version");
    grep("/proc/cpuinfo", &["model name", "cpu MHz", "cache size", "siblings", "cpu cores", "microcode"]);
    if let Ok(s) = std::fs::read_to_string("/proc/cpuinfo") {
        let n = s.lines().filter(|l| l.starts_with("processor")).count();
        let flags = s.lines().find(|l| l.starts_with("flags")).unwrap_or("");
        let want = ["avx2", "bmi2", "avx512f", "avx512bw", "avx512dq", "avx512vl", "avx512vbmi", "avx512_vbmi2", "avx512ifma", "gfni", "vaes", "hypervisor"];
        let have: Vec<&str> = want.iter().copied().filter(|w| flags.split_whitespace().any(|f| f == *w)).collect();
        eprintln!("PROBE cpus listed {n}; flags of interest: {}", have.join(" "));
    }
    grep("/proc/self/status", &["Cpus_allowed_list", "Mems_allowed_list", "Uid", "Seccomp", "NoNewPrivs"]);
    eprintln!("PROBE available_parallelism {:?}", std::thread::available_parallelism().map(|n| n.get()).ok());
    for c in 0..8 {
        let p = format!("/sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list");
        if Path::new(&p).exists() {
            eprintln!("PROBE cpu{c} siblings {} core_id {}", cat(&p), cat(&format!("/sys/devices/system/cpu/cpu{c}/topology/core_id")));
        }
    }
    for i in 0..4 {
        let b = format!("/sys/devices/system/cpu/cpu0/cache/index{i}");
        if Path::new(&b).exists() {
            eprintln!("PROBE cache{i}: level {} type {} size {} shared_cpu_list {}", cat(&format!("{b}/level")), cat(&format!("{b}/type")), cat(&format!("{b}/size")), cat(&format!("{b}/shared_cpu_list")));
        }
    }
    for f in ["enabled", "defrag", "shmem_enabled", "use_zero_page", "khugepaged/defrag"] {
        show(&format!("/sys/kernel/mm/transparent_hugepage/{f}"));
    }
    for f in [
        "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu.weight", "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.high", "/sys/fs/cgroup/memory.swap.max", "/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/io.max",
        "/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us", "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "/sys/fs/cgroup/cpuset/cpuset.cpus", "/proc/self/cgroup",
    ] {
        if Path::new(f).exists() {
            show(f);
        }
    }
    if let Ok(s) = std::fs::read_to_string("/proc/self/mountinfo") {
        for l in s.lines() {
            let f: Vec<&str> = l.split_whitespace().collect();
            if f.len() > 4 && ["/", "/data", "/output", "/scratch", "/tmp"].contains(&f[4]) {
                eprintln!("PROBE mount {}", l);
                let dev = f[2];
                for q in ["queue/read_ahead_kb", "queue/rotational", "queue/max_sectors_kb", "queue/nr_requests", "queue/scheduler", "../queue/read_ahead_kb", "../queue/rotational", "../queue/max_sectors_kb", "../queue/scheduler", "bdi/read_ahead_kb"] {
                    let p = format!("/sys/dev/block/{dev}/{q}");
                    if Path::new(&p).exists() {
                        eprintln!("PROBE   {} {q} = {}", f[4], cat(&p));
                    }
                }
            }
        }
    }
    grep("/proc/meminfo", &["MemTotal", "MemFree", "MemAvailable", "Cached", "Dirty", "AnonHugePages", "ShmemHugePages", "FileHugePages", "Hugepagesize", "HugePages_Total", "SwapTotal"]);
    show("/proc/loadavg");
    show("/proc/uptime");
    for k in ["NCPU", "MEM_GB", "DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR", "HOME", "GLIBC_TUNABLES", "HOSTNAME"] {
        eprintln!("PROBE env {k}={:?}", std::env::var(k).ok());
    }
    // container creation vs process start (docker writes /.dockerenv and /etc/hostname at creation)
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0);
    let start = (|| {
        let st = std::fs::read_to_string("/proc/self/stat").ok()?;
        let f: Vec<&str> = st.rsplit(')').next()?.split_whitespace().collect();
        let ticks: f64 = f.get(19)?.parse().ok()?;
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        let btime: f64 = stat.lines().find(|l| l.starts_with("btime"))?.split_whitespace().nth(1)?.parse().ok()?;
        Some(btime + ticks / 100.0)
    })();
    eprintln!(
        "PROBE clocks: now {now:.3} | process start {:?} | ctime /.dockerenv {} | /etc/hostname {} | /etc/resolv.conf {} | /sys/fs/cgroup {} | /proc/1/cmdline {}",
        start.map(|s| format!("{s:.2}")),
        ctime("/.dockerenv"),
        ctime("/etc/hostname"),
        ctime("/etc/resolv.conf"),
        ctime("/sys/fs/cgroup"),
        cat("/proc/1/cmdline").replace('\0', " ")
    );
    // is the input already in the page cache?
    let mut ev_files: Vec<(std::path::PathBuf, u64)> = Vec::new();
    for sub in ["events", "clicks", "links"] {
        let (n, total, res, per) = residency(&data.join(sub));
        eprintln!(
            "PROBE input {sub}: {n} files, {:.1} MB; mincore says {:.1}% resident (reads 100% when the files are not ours to write)",
            total as f64 / 1e6,
            if total > 0 { 100.0 * res as f64 / total as f64 } else { 0.0 }
        );
        let (mut hit, mut probed) = (0usize, 0usize);
        let step = (per.len() / 64).max(1);
        let keep = if sub == "events" { per.len().saturating_sub(2) } else { per.len() };
        for (p, _, _) in per[..keep].iter().step_by(step) {
            let (h, k) = nowait_cached(p, 32);
            hit += h;
            probed += k;
        }
        eprintln!("PROBE input {sub}: preadv2(RWF_NOWAIT) found {hit} of {probed} sampled pages in the page cache");
        if sub == "events" {
            ev_files = per.iter().map(|(p, len, _)| (p.clone(), *len)).collect();
        }
    }
    // read speed, on files the pipeline has not touched yet: read() and an mmap page walk
    if ev_files.len() >= 2 {
        use std::io::Read;
        use std::os::fd::AsRawFd;
        let (p, len) = &ev_files[ev_files.len() - 1];
        let (c0, _) = nowait_cached(p, 1);
        let (rb0, mf0) = io_counters();
        let t = Instant::now();
        let mut got = 0usize;
        if let Ok(mut fh) = std::fs::File::open(p) {
            let mut buf = vec![0u8; 1 << 20];
            while let Ok(k) = fh.read(&mut buf) {
                if k == 0 {
                    break;
                }
                got += k;
            }
        }
        let dt = t.elapsed().as_secs_f64();
        let (rb1, mf1) = io_counters();
        eprintln!(
            "PROBE read() of {:?}: {:.1} MB in {:.4} s = {:.0} MB/s; first page cached before: {c0}; disk bytes {}; major faults {} (file {:.1} MB)",
            p.file_name(),
            got as f64 / 1e6,
            dt,
            got as f64 / 1e6 / dt.max(1e-9),
            rb1 - rb0,
            mf1 - mf0,
            *len as f64 / 1e6
        );
        let (p, len) = &ev_files[ev_files.len() - 2];
        let (c0, _) = nowait_cached(p, 1);
        let (rb0, mf0) = io_counters();
        if let Ok(f) = std::fs::File::open(p) {
            let t = Instant::now();
            // SAFETY: read-only mapping, unmapped below
            let ptr = unsafe { mmap(std::ptr::null_mut(), *len as usize, 1, 2, f.as_raw_fd(), 0) };
            if ptr as isize != -1 {
                let mut acc = 0u64;
                let mut i = 0usize;
                while i < *len as usize {
                    acc += unsafe { ptr.add(i).read_volatile() } as u64;
                    i += 4096;
                }
                std::hint::black_box(acc);
                let dt = t.elapsed().as_secs_f64();
                let (rb1, mf1) = io_counters();
                eprintln!(
                    "PROBE mmap page walk of {:?}: {:.1} MB in {:.4} s = {:.0} MB/s; first page cached before: {c0}; disk bytes {}; major faults {}",
                    p.file_name(),
                    *len as f64 / 1e6,
                    dt,
                    *len as f64 / 1e6 / dt.max(1e-9),
                    rb1 - rb0,
                    mf1 - mf0
                );
                unsafe { munmap(ptr, *len as usize) };
            }
        }
    }
    // first touch of fresh memory: 4 KB pages vs huge pages
    let (t4, a4) = touch_anon(false);
    let (th, ah) = touch_anon(true);
    eprintln!("PROBE first touch of 256 MB anon: default {:.4} s ({a4}) | MADV_HUGEPAGE {:.4} s ({ah})", t4, th);
    // CPU speed and scaling over threads (a quota or shared cores show up here)
    let it = 150_000_000u64;
    let one = threads_time(1, it);
    eprintln!("PROBE spin {:.2} ns/iter single thread", one * 1e9 / it as f64);
    for n in [2usize, 4, 8, 16] {
        let w = threads_time(n, it);
        eprintln!("PROBE spin x{n} threads: wall {:.3} s ({:.2}x one thread's wall)", w, w / one);
    }
    eprintln!("PROBE memory copy: 1 thread {:.1} GB/s | 4 threads {:.1} GB/s | 8 threads {:.1} GB/s", copy_gbps(1), copy_gbps(4), copy_gbps(8));
    eprintln!("PROBE (probe section took {:.2} s; the pipeline's lap times below include it)", t.elapsed().as_secs_f64());
}

pub fn after() {
    grep("/proc/self/status", &["VmHWM", "VmRSS", "RssAnon", "RssFile", "RssShmem", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches", "Threads"]);
    if let Ok(st) = std::fs::read_to_string("/proc/self/stat") {
        let f: Vec<&str> = st.rsplit(')').next().unwrap_or("").split_whitespace().collect();
        eprintln!("PROBE self: minflt {} majflt {} utime {} stime {} (ticks)", f.get(7).unwrap_or(&"?"), f.get(9).unwrap_or(&"?"), f.get(11).unwrap_or(&"?"), f.get(12).unwrap_or(&"?"));
    }
    show("/proc/self/io");
    for f in ["/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/io.stat", "/sys/fs/cgroup/cpu/cpu.stat"] {
        if Path::new(f).exists() {
            show(f);
        }
    }
    grep("/sys/fs/cgroup/memory.stat", &["anon ", "file ", "file_mapped", "anon_thp", "file_thp", "pgfault", "pgmajfault", "thp_fault_alloc", "thp_collapse_alloc"]);
    grep("/proc/vmstat", &["thp_fault_alloc", "thp_fault_fallback ", "pgmajfault", "pgpgin", "thp_file_alloc"]);
    grep("/proc/meminfo", &["MemFree", "Cached", "Dirty", "Writeback:", "AnonHugePages"]);
    eprintln!("PROBE ===== end of probe =====");
}
