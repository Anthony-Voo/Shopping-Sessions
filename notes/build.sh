#!/usr/bin/env bash
# Build exactly like the scorer target (cascadelake + PGO profile); RUSTFLAGS is required, there is no cargo config for it.
cd /home/user/shopping-sessions && RUSTFLAGS="-C target-cpu=cascadelake -Cprofile-use=/home/user/shopping-sessions/pgo/merged.profdata" cargo ${TC:-} build --release 2>&1 | tail -1
