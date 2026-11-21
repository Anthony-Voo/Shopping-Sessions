#!/usr/bin/env bash
# Correctness gate (read-only use of the koillea workspace; all outputs in this scratch dir).
# Usage: check.sh [BIN]   default BIN = /home/user/shopping-sessions/target/release/sessions
export PYTHONDONTWRITEBYTECODE=1
W=$(cd "$(dirname "$0")" && pwd); cd $W
BIN=${1:-/home/user/shopping-sessions/target/release/sessions}
K=$W/kit/starter-duckdb/starter-duckdb
(cd /home/user/goonies-buildathon/tracks/shopping-sessions && sha256sum --quiet -c tools/gate.sha256) || { echo "GATE FAIL: oracle/fixtures/dev-answer changed"; exit 1; }
f=$(cd $K && python3 scripts/run_fixtures.py fixtures -- $BIN 2>&1 | tail -1)
rm -rf out/chk-dev; $BIN data/dev out/chk-dev 2>/dev/null
d=$(cd $K/scripts && python3 compare.py $W/out/chk-dev $W/kit/dev-answer/dev-answer | tail -1)
s=$($W/difftest.sh 300000 41 42 43 44 -- $BIN | tr '\n' ' ')
rm -rf out/e-o out/e-f; python3 oracle/reference.py data/edge-nonl out/e-o >/dev/null; $BIN data/edge-nonl out/e-f 2>/dev/null
e=$(cd $K/scripts && python3 compare.py $W/out/e-f $W/out/e-o | tail -1)
echo "fixtures: $f | dev: $d | edge: $e | oracle: $s"
[[ "$f" == "36/36 fixtures passed" && "$d" == MATCH && "$e" == MATCH && "$s" != *DIFFER* ]] && echo GATE PASS || { echo GATE FAIL; exit 1; }
