#!/usr/bin/env bash
# Usage: difftest.sh EVENTS SEED... [-- BIN]
export PYTHONDONTWRITEBYTECODE=1
W=$(cd "$(dirname "$0")" && pwd); cd $W
EV=$1; shift; seeds=(); BIN=/home/user/shopping-sessions/target/release/sessions
while [ $# -gt 0 ]; do if [ "$1" = -- ]; then BIN=$2; break; fi; seeds+=($1); shift; done
CMP=$W/kit/starter-duckdb/starter-duckdb/scripts; fail=0
for s in "${seeds[@]}"; do
  d=gen/s$s-$EV; rm -rf $d out/o-$s out/f-$s
  generate --seed $s --events $EV --clicks $((EV/4)) --output $d >/dev/null 2>&1
  python3 oracle/reference.py $d out/o-$s >/dev/null
  $BIN $d out/f-$s 2>/dev/null
  r=$(cd $CMP && python3 compare.py $W/out/f-$s $W/out/o-$s | tail -1)
  echo "seed $s: $r"; [ "$r" = MATCH ] || fail=1
  [ "$r" = MATCH ] && rm -rf $d out/o-$s out/f-$s
done
exit $fail
