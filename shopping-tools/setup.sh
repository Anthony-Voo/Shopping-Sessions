#!/usr/bin/env bash
# One-time dev setup: kit (fixtures, dev answer, generator) from the event site, test data, build.
# Usage: shopping-tools/setup.sh      (from the repo root; ~3-5 minutes)
set -euo pipefail
cd "$(dirname "$0")/.."
B=https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files
mkdir -p kit bin data
for z in starter-duckdb dev-answer generator; do
  [ -d kit/$z ] || { curl -sSf -o kit/$z.zip $B/$z.zip && mkdir -p kit/$z && (cd kit/$z && unzip -qo ../$z.zip) && rm kit/$z.zip; }
done
[ -x bin/generate ] || { (cd kit/generator/generator && cargo build --release -q) && cp kit/generator/generator/target/release/generate bin/; }
pip install -q polars pyarrow 2>/dev/null || pip install -q --break-system-packages polars pyarrow
[ -d data/dev ] || bin/generate --seed 1 --events 1000000 --clicks 250000 --output data/dev >/dev/null
[ -d data/p20m ] || bin/generate --seed 77 --events 20000000 --clicks 5000000 --output data/p20m >/dev/null
# edge case: files without a trailing newline, last fields empty or quoted
E=data/edge-nonl; rm -rf $E; mkdir -p $E/events $E/clicks $E/links
cp kit/starter-duckdb/starter-duckdb/fixtures/A1/input/{products.csv,campaigns.csv,manifest.json} $E/
printf 'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\ne1,u1,2026-10-03T10:00:00Z,2026-10-03T10:00:01Z,view,p1,\ne2,"u1",2026-10-03T10:05:00Z,2026-10-03T10:05:01Z,purchase,p1,"500"\ne3,u2,2026-10-03T11:00:00Z,2026-10-03T11:00:01Z,view,p2,' > $E/events/a.csv
printf 'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\ne4,u3,2026-10-04T10:00:00Z,2026-10-04T10:00:01Z,"view","p2",""' > $E/events/b.csv
printf 'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents' > $E/events/c.csv
printf 'click_id,user_id,campaign_id,ts\nk1,u1,c_1,2026-10-03T09:00:00Z\nk2,u3,"c_2","2026-10-04T09:00:00Z"' > $E/clicks/a.csv
printf 'user_id_a,user_id_b\nu2,u3' > $E/links/a.csv
RUSTFLAGS="-C target-cpu=native" cargo build --release -q --offline
echo "setup done: run shopping-tools/check.sh then shopping-tools/bench.sh"
