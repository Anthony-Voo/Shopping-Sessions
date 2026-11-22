//! Scoring-environment probe, second form. Active only when SESSIONS_PROBE is set (never in a
//! normal run). It builds 20M- and 60M-event datasets from the public dev set inside the
//! container (every replica gets its own event, click and user ids, so nothing dedups away),
//! runs several builds of the pipeline on them as child processes, and prints a summary small
//! enough for the 2,000-character log window; then exits with status 3 on purpose.

use rayon::prelude::*;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

unsafe extern "C" {
    fn memfd_create(name: *const u8, flags: u32) -> i32;
}

static OUT_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// One summary line; the whole log stays under ~1,950 bytes.
fn say(line: String) {
    let used = OUT_BYTES.load(std::sync::atomic::Ordering::Relaxed);
    let room = 1950usize.saturating_sub(used);
    let mut l = line;
    if l.len() + 1 > room {
        l.truncate(room.saturating_sub(1));
    }
    if !l.is_empty() {
        OUT_BYTES.fetch_add(l.len() + 1, std::sync::atomic::Ordering::Relaxed);
        eprintln!("{l}");
    }
}

fn first_line(path: &str) -> String {
    std::fs::read_to_string(path).ok().and_then(|s| s.lines().next().map(|l| l.trim().to_string())).unwrap_or_else(|| "?".into())
}

fn key_of(path: &str, key: &str) -> String {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with(key)).map(|l| l.split(':').nth(1).unwrap_or("").split_whitespace().collect::<Vec<_>>().join(" ")))
        .unwrap_or_else(|| "?".into())
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
    let hs: Vec<_> = (0..n)
        .map(|_| {
            std::thread::spawn(move || {
                let a = vec![1u8; 96 << 20];
                let mut b = vec![2u8; 96 << 20];
                let t = Instant::now();
                for _ in 0..2 {
                    b.copy_from_slice(&a);
                    std::hint::black_box(&b);
                }
                t.elapsed().as_secs_f64()
            })
        })
        .collect();
    let worst = hs.into_iter().map(|h| h.join().unwrap_or(0.0)).fold(0.0, f64::max);
    if worst > 0.0 { n as f64 * 0.192 / worst } else { 0.0 }
}

// ---------------------------------------------------------------- dataset replication
struct Src {
    name: String,
    header: Vec<u8>,
    body: Vec<u8>,
}

fn load_dir(dir: &Path) -> Vec<Src> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| rd.filter_map(|e| e.ok().map(|e| e.path())).filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file()).collect())
        .unwrap_or_default();
    paths.sort();
    let mut v = Vec::new();
    for p in paths {
        let Ok(mut b) = std::fs::read(&p) else { continue };
        if b.last() != Some(&b'\n') {
            b.push(b'\n');
        }
        let h = memchr::memchr(b'\n', &b).map_or(b.len(), |k| k + 1);
        let body = b.split_off(h);
        v.push(Src { name: p.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default(), header: b, body });
    }
    v
}

/// At a field start: copy an opening quote if there is one, then insert the replica tag unless
/// the field is empty. Returns the position reached in `line`.
#[inline]
fn put_tagged(out: &mut Vec<u8>, line: &[u8], mut p: usize, t: &[u8; 2]) -> usize {
    if p < line.len() && line[p] == b'"' {
        out.push(b'"');
        p += 1;
        if p < line.len() && line[p] != b'"' && line[p] != b',' && line[p] != b'\n' {
            out.extend_from_slice(t);
        }
    } else if p < line.len() && line[p] != b',' && line[p] != b'\n' {
        out.extend_from_slice(t);
    }
    p
}

/// Append `body` with the first two fields of every line (the ids) prefixed by replica r's tag;
/// replica 0 is the data as it is.
fn replicate(out: &mut Vec<u8>, body: &[u8], r: usize) {
    if r == 0 {
        out.extend_from_slice(body);
        return;
    }
    let t = [b'A' + (r / 26) as u8, b'A' + (r % 26) as u8];
    let mut i = 0;
    while i < body.len() {
        let end = memchr::memchr(b'\n', &body[i..]).map_or(body.len(), |k| i + k + 1);
        let line = &body[i..end];
        let mut p = put_tagged(out, line, 0, &t);
        match memchr::memchr(b',', &line[p..]) {
            Some(k) => {
                out.extend_from_slice(&line[p..p + k + 1]);
                p += k + 1;
                p = put_tagged(out, line, p, &t);
                out.extend_from_slice(&line[p..]);
            }
            None => out.extend_from_slice(&line[p..]),
        }
        i = end;
    }
}

/// k replicas of the dev set as in-memory files (memfd), reachable through symlinks under
/// `root`. Events are grouped `ev_group` replicas per file (files of ~30-90 MB, like the real
/// data). Returns the open files (they must stay open) and the total bytes.
fn build(root: &Path, data: &Path, k: usize, ev_group: usize) -> (Vec<std::fs::File>, u64) {
    use std::os::fd::{AsRawFd, FromRawFd};
    let _ = std::fs::remove_dir_all(root);
    let mut keep = Vec::new();
    let mut total = 0u64;
    for f in ["manifest.json", "products.csv", "campaigns.csv"] {
        let _ = std::fs::create_dir_all(root);
        let _ = std::fs::copy(data.join(f), root.join(f));
    }
    for (sub, group) in [("events", ev_group), ("clicks", k), ("links", k)] {
        let dir = root.join(sub);
        let _ = std::fs::create_dir_all(&dir);
        let srcs = load_dir(&data.join(sub));
        let reps: Vec<usize> = (0..k).collect();
        let jobs: Vec<(usize, usize, &[usize])> = srcs.iter().enumerate().flat_map(|(si, _)| reps.chunks(group.max(1)).enumerate().map(move |(bi, b)| (si, bi, b))).collect();
        let files: Vec<(String, std::fs::File, u64)> = jobs
            .par_iter()
            .filter_map(|&(si, bi, batch)| {
                let s = &srcs[si];
                let mut buf: Vec<u8> = Vec::with_capacity(s.header.len() + (s.body.len() + s.body.len() / 16) * batch.len() + 64);
                buf.extend_from_slice(&s.header);
                for &r in batch {
                    replicate(&mut buf, &s.body, r);
                }
                // SAFETY: plain libc call; the returned descriptor is owned by the File
                let fd = unsafe { memfd_create(b"pd\0".as_ptr(), 0) };
                if fd < 0 {
                    return None;
                }
                let mut f = unsafe { std::fs::File::from_raw_fd(fd) };
                f.write_all(&buf).ok()?;
                Some((format!("{}-b{bi:02}.csv", s.name), f, buf.len() as u64))
            })
            .collect();
        for (name, f, n) in files {
            let _ = std::os::unix::fs::symlink(format!("/proc/{}/fd/{}", std::process::id(), f.as_raw_fd()), dir.join(name));
            total += n;
            keep.push(f);
        }
    }
    (keep, total)
}

// ---------------------------------------------------------------- runs
struct Run {
    wall: f64,
    laps: Vec<(String, f64)>,
    ok: bool,
}

fn run(bin: &Path, data: &Path, out: &Path, envs: &[(&str, &str)]) -> Run {
    let _ = std::fs::remove_dir_all(out);
    let t = Instant::now();
    let mut c = Command::new(bin);
    c.arg(data).arg(out).env_remove("SESSIONS_PROBE").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    for (k, v) in envs {
        c.env(k, v);
    }
    let o = c.output();
    let wall = t.elapsed().as_secs_f64();
    let mut laps = Vec::new();
    let mut ok = false;
    if let Ok(o) = o {
        ok = o.status.success();
        for l in String::from_utf8_lossy(&o.stderr).lines() {
            let mut it = l.split_whitespace();
            if let Some(tk) = it.next() {
                if let Some(sec) = tk.strip_suffix('s').and_then(|x| x.parse::<f64>().ok()) {
                    laps.push((it.collect::<Vec<_>>().join(" "), sec));
                }
            }
        }
    }
    Run { wall, laps, ok }
}

fn lap(r: &Run, label: &str) -> f64 {
    r.laps.iter().find(|(l, _)| l == label).map_or(f64::NAN, |x| x.1)
}

/// Stage durations of one run: read, parse lines, key partition, dedup, user shuffle + clicks,
/// regroup, sessionize, write, and process exit (wall - written).
fn stages(r: &Run) -> String {
    let (rd, pl, ep, dd, ck, rg, ss, wr) = (lap(r, "read"), lap(r, "parse lines"), lap(r, "events parsed"), lap(r, "events deduped"), lap(r, "clicks parsed"), lap(r, "events regrouped"), lap(r, "sessionized"), lap(r, "written"));
    format!("rd{:.3} pl{:.3} kp{:.3} dd{:.3} sc{:.3} rg{:.3} ss{:.3} wr{:.3} ex{:.3}", rd, pl - rd, ep - pl, dd - ep, ck - dd, rg - ck, ss - rg, wr - ss, r.wall - wr)
}

pub fn run_probe(data: &Path, out: &Path) -> ! {
    let t_all = Instant::now();
    let scratch = PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    // ---- machine
    let allowed = key_of("/proc/self/status", "Cpus_allowed_list");
    let c0: String = allowed.split(|c| c == ',' || c == '-').next().unwrap_or("0").to_string();
    let thp = first_line("/sys/kernel/mm/transparent_hugepage/enabled");
    let thp = thp.split('[').nth(1).and_then(|s| s.split(']').next()).unwrap_or("?").to_string();
    let quota = if Path::new("/sys/fs/cgroup/cpu.max").exists() { first_line("/sys/fs/cgroup/cpu.max") } else { first_line("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") };
    let fs_of = |mp: &str| -> String {
        std::fs::read_to_string("/proc/self/mountinfo")
            .ok()
            .and_then(|s| s.lines().find(|l| l.split_whitespace().nth(4) == Some(mp)).and_then(|l| l.split(" - ").nth(1).map(|x| x.split_whitespace().next().unwrap_or("?").to_string())))
            .unwrap_or_else(|| "?".into())
    };
    say(format!(
        "P2 {} | cpus {} sib{} {} | L2 {} L3 {} | thp {} | cpu.max {} | load {}",
        key_of("/proc/cpuinfo", "model name").replace("Intel(R) Xeon(R) ", "").replace("CPU ", ""),
        allowed,
        c0,
        first_line(&format!("/sys/devices/system/cpu/cpu{c0}/topology/thread_siblings_list")),
        first_line(&format!("/sys/devices/system/cpu/cpu{c0}/cache/index2/size")),
        first_line(&format!("/sys/devices/system/cpu/cpu{c0}/cache/index3/size")),
        thp,
        quota,
        first_line("/proc/loadavg").split_whitespace().next().unwrap_or("?")
    ));
    say(format!("P2 fs data {} out {} scratch {} | MemAvail {} kB", fs_of("/data"), fs_of("/output"), fs_of("/scratch"), key_of("/proc/meminfo", "MemAvailable").replace(" kB", "")));
    let it = 60_000_000u64;
    let one = threads_time(1, it);
    let sc: Vec<String> = [2usize, 4, 8, 16].iter().map(|&n| format!("x{n} {:.2}", threads_time(n, it) / one)).collect();
    say(format!("P2 spin {:.2}ns {} | copy 1t {:.1} 4t {:.1} 8t {:.1} GB/s", one * 1e9 / it as f64, sc.join(" "), copy_gbps(1), copy_gbps(4), copy_gbps(8)));
    // ---- binaries
    let me = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/app/sessions"));
    let bins_dir = me.parent().unwrap_or(Path::new("/app")).join("bins");
    let old = |v: &str| bins_dir.join(format!("sessions_{v}"));
    let pout = out.join("pr");
    let med = |v: &mut Vec<f64>| -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    // the big set is 60 replicas with the scorer's 24 GB (the in-memory files count against the
    // limit), 36 on a smaller machine
    let big = if std::env::var("MEM_GB").ok().and_then(|x| x.parse::<f64>().ok()).is_some_and(|g| g >= 20.0) { 60usize } else { 36 };
    let big_label = format!("{big}M");
    for (label, k, ev_group, rounds) in [("20M", 20usize, 10usize, 3usize), (big_label.as_str(), big, big / 2, 2)] {
        if t_all.elapsed().as_secs_f64() > 95.0 {
            say(format!("P2 {label} skipped (time)"));
            continue;
        }
        let root = scratch.join(format!("pd{k}"));
        let tg = Instant::now();
        let (keep, bytes) = build(&root, data, k, ev_group);
        say(format!("P2 {label}: {} files {:.2} GB built in {:.2}s", keep.len(), bytes as f64 / 1e9, tg.elapsed().as_secs_f64()));
        let mut variants: Vec<(&str, PathBuf, Vec<(&str, &str)>)> = vec![
            ("v59", old("v59"), vec![]),
            ("v63", old("v63"), vec![]),
            ("v65", old("v65"), vec![]),
            ("v68", me.clone(), vec![]),
            ("68nice0", me.clone(), vec![("WORKER_NICE", "0")]),
            ("68t4", me.clone(), vec![("NCPU", "4")]),
        ];
        if k == 20 {
            variants.push(("68t6", me.clone(), vec![("NCPU", "6")]));
            variants.push(("68pop", me.clone(), vec![("MAPMODE", "populate")]));
        }
        variants.retain(|v| v.1.exists());
        let mut walls: Vec<Vec<f64>> = vec![Vec::new(); variants.len()];
        let mut best: Vec<Option<Run>> = variants.iter().map(|_| None).collect();
        let mut bad = 0;
        'rounds: for _ in 0..rounds {
            for (vi, (_, bin, envs)) in variants.iter().enumerate() {
                if t_all.elapsed().as_secs_f64() > 125.0 {
                    break 'rounds;
                }
                // PROBE_KEEP (local testing only): one output folder per variant, left in place
                let po = if std::env::var_os("PROBE_KEEP").is_some() { out.join(format!("pr_{label}_{}", variants[vi].0)) } else { pout.clone() };
                let r = run(bin, &root, &po, envs);
                if !r.ok {
                    bad += 1;
                }
                walls[vi].push(r.wall);
                if best[vi].as_ref().is_none_or(|b| r.wall < b.wall) {
                    best[vi] = Some(r);
                }
            }
        }
        let parts: Vec<String> = variants
            .iter()
            .zip(walls.iter_mut())
            .filter(|(_, w)| !w.is_empty())
            .map(|((name, _, _), w)| {
                let lo = w.iter().cloned().fold(f64::MAX, f64::min);
                format!("{name} {:.3}/{:.3}", med(w), lo)
            })
            .collect();
        say(format!("P2 {label} med/min s: {}{}", parts.join(" | "), if bad > 0 { format!(" | FAILED RUNS {bad}") } else { String::new() }));
        for (vi, (name, _, _)) in variants.iter().enumerate() {
            if *name == "v59" || *name == "v68" {
                if let Some(b) = &best[vi] {
                    say(format!("P2 {label} {name} best: {}", stages(b)));
                }
            }
        }
        drop(keep);
        let _ = std::fs::remove_dir_all(&root);
    }
    let _ = std::fs::remove_dir_all(&pout);
    say(format!("P2 done in {:.0}s; exit 3 on purpose (diagnostic, not an entry)", t_all.elapsed().as_secs_f64()));
    std::process::exit(3);
}
