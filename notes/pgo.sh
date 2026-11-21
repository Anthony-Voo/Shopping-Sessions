#!/usr/bin/env bash
# Regenerate pgo/merged.profdata for the current source with rustc 1.90 (same as the Dockerfile).
# Run after every source change: a stale profile only loses PGO for changed functions.
set -e
cd /home/user/shopping-sessions
S=/tmp/claude-0/-home-user/7093f2af-7756-5848-9566-55372d396f64/scratchpad
RAW=/tmp/claude-0/pgo-raw; rm -rf $RAW; mkdir -p $RAW
RUSTFLAGS="-C target-cpu=cascadelake -C relocation-model=static -Cprofile-generate=$RAW" cargo +1.90 build --release --locked --offline --target-dir $S/tpgo-gen 2>&1 | tail -1
export GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=17179869184
cd $S/ws
for i in 1 2; do rm -rf out/pg; $S/tpgo-gen/release/sessions data/p20m out/pg >/dev/null 2>&1; done
generate --seed 9101 --events 2000000 --clicks 500000 --output gen/pg1 >/dev/null 2>&1
$S/tpgo-gen/release/sessions gen/pg1 out/pg1 >/dev/null 2>&1; rm -rf gen/pg1 out/pg1 out/pg
~/.rustup/toolchains/1.90-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata merge -o /home/user/shopping-sessions/pgo/merged.profdata $RAW
ls -la /home/user/shopping-sessions/pgo/merged.profdata
