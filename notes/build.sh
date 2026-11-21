#!/usr/bin/env bash
# Build exactly like the scorer target (cascadelake); RUSTFLAGS is required, there is no cargo config for it.
cd /home/user/shopping-sessions && RUSTFLAGS="-C target-cpu=cascadelake" cargo ${TC:-} build --release 2>&1 | tail -1
