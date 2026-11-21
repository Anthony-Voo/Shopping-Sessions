#!/usr/bin/env bash
# Correctness gate for target/release/sessions: kit/oracle integrity, 36 fixtures, exact dev answer,
# no-trailing-newline edge case, and the slow Python oracle on 4 generated seeds. Prints GATE PASS/FAIL.
cd "$(dirname "$0")/.."
sha256sum --quiet -c shopping-tools/gate.sha256 || { echo "GATE FAIL: kit/oracle changed"; exit 1; }
BIN=$PWD/target/release/sessions; K=kit/starter-duckdb/starter-duckdb
f=$(cd $K && python3 scripts/run_fixtures.py fixtures -- $BIN 2>&1 | tail -1)
rm -rf out/chk-dev; $BIN data/dev out/chk-dev 2>/dev/null
d=$(python3 $K/scripts/compare.py out/chk-dev kit/dev-answer/dev-answer | tail -1)
rm -rf out/e-o out/e-f; python3 shopping-tools/reference.py data/edge-nonl out/e-o >/dev/null; $BIN data/edge-nonl out/e-f 2>/dev/null
e=$(python3 $K/scripts/compare.py out/e-f out/e-o | tail -1)
s=$(COMPARE=$K/scripts/compare.py PATH=$PWD/bin:$PATH shopping-tools/difftest.sh 300000 41 42 43 44 | tr '\n' ' ')
echo "fixtures: $f | dev: $d | edge: $e | oracle: $s"
[[ "$f" == "36/36 fixtures passed" && "$d" == MATCH && "$e" == MATCH && "$s" != *DIFFER* ]] && echo GATE PASS || { echo GATE FAIL; exit 1; }
