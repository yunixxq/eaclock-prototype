#!/bin/bash
# Full eviction latency benchmark for CLOCK/LIRS/S3FIFO/EACLOCK/WATT
# at Buffer=5%(64MB)/10%(128MB)/15%(256MB), 30s per algorithm

PG_BIN=/opt/LRU-C-PG/pg_install/bin
PG_DATA=/tmp/pgtest_data
PG_SRC=/Users/lyx/Projects/bufferpool/EACLOCK-prototype/src/backend
DB=pgbench_test
DURATION=30

ALGOS="c:CLOCK i:LIRS 3:S3FIFO e:EACLOCK w:WATT"
BUFFER_SIZES="64:5% 128:10% 256:15%"

OUT="$PWD/eviction_benchmark_results.txt"
> "$OUT"

# Deploy custom postgres binary
echo "Deploying custom postgres..."
sudo cp "$PG_SRC/postgres" "$PG_BIN/postgres"
echo "Custom postgres deployed"

echo "==========================================="
echo "Eviction Latency Benchmark Results"
echo "==========================================="

printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "Algorithm" "Buffer" "P50(ns)" "P90(ns)" "P99(ns)" "Samples"
printf "%-10s-+-%-8s-+-%10s-+-%10s-+-%10s-+-%12s\n" "----------" "--------" "----------" "----------" "----------" "------------"

printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "Algorithm" "Buffer" "P50(ns)" "P90(ns)" "P99(ns)" "Samples" >> "$OUT"
printf "%-10s-+-%-8s-+-%10s-+-%10s-+-%10s-+-%12s\n" "----------" "--------" "----------" "----------" "----------" "------------" >> "$OUT"

for BS in $BUFFER_SIZES; do
  BUF_SIZE=$(echo $BS | cut -d: -f1)
  BUF_PCT=$(echo $BS | cut -d: -f2)
  echo ""
  echo "=== Buffer=$BUF_PCT ($BUF_SIZE MB) ==="

  pkill -9 -f pgbench 2>/dev/null || true
  pkill -9 -f "postgres.*$PG_DATA" 2>/dev/null || true
  sleep 2

  $PG_BIN/pg_ctl -D $PG_DATA stop -m fast 2>/dev/null || true
  sleep 2
  sed -i '' "s/shared_buffers = .*/shared_buffers = ${BUF_SIZE}MB/" $PG_DATA/postgresql.conf
  $PG_BIN/pg_ctl -D $PG_DATA -l $PG_DATA/logfile start 2>&1 | tail -1
  sleep 3

  if ! $PG_BIN/pg_ctl -D $PG_DATA status >/dev/null 2>&1; then
    echo "  ERROR: Server failed to start at Buffer=$BUF_PCT"
    continue
  fi

  $PG_BIN/dropdb -p 5432 $DB 2>/dev/null || true
  $PG_BIN/createdb -p 5432 $DB 2>/dev/null
  echo "  Initializing pgbench data (scale=200)..."
  $PG_BIN/pgbench -p 5432 -d $DB -s 200 -i 2>/dev/null | tail -1

  for entry in $ALGOS; do
    ALGO_CMD=$(echo $entry | cut -d: -f1)
    ALGO_FULL=$(echo $entry | cut -d: -f2)
    echo "  Testing $ALGO_FULL (${DURATION}s)..."

    $PG_BIN/psql -p 5432 -d postgres -c "resetlatency" >/dev/null 2>&1
    $PG_BIN/psql -p 5432 -d postgres -c "algorithm $ALGO_CMD" >/dev/null 2>&1
    sleep 0.5

    (
      trap 'kill -9 $$ 2>/dev/null' TERM
      $PG_BIN/pgbench -p 5432 -d $DB -c 10 -j 10 -T $DURATION -S 2>/dev/null | tail -1
    ) &
    PGPID=$!
    for i in $(seq 1 90); do
      sleep 1
      if ! kill -0 $PGPID 2>/dev/null; then wait $PGPID; break; fi
      if [ $i -eq 90 ]; then
        echo "    -> TIMEOUT, killing pgbench..."
        kill -9 $PGPID 2>/dev/null; wait $PGPID 2>/dev/null; break; fi
    done

    sleep 1
    $PG_BIN/psql -p 5432 -d postgres -c "latency" >/dev/null 2>&1
    sleep 2

    LASTLINE=$(grep "LOG:  ${ALGO_FULL}: buf_size=" "$PG_DATA/logfile" 2>/dev/null | tail -1)
    if [ -n "$LASTLINE" ]; then
      PARSE=$(echo "$LASTLINE" | awk '{
        for(i=1;i<=NF;i++) {
          if($i ~ /^samples=/) { sub(/[^0-9]*/,"",$i); SAMPLES=$i }
          if($i ~ /^P50=/) { split($i,a,"="); sub(/ns/,"",a[2]); P50=a[2] }
          if($i ~ /^P90=/) { split($i,a,"="); sub(/ns/,"",a[2]); P90=a[2] }
          if($i ~ /^P99=/) { split($i,a,"="); sub(/ns/,"",a[2]); P99=a[2] }
        }
        printf "%s|%s|%s|%s\n", SAMPLES, P50, P90, P99
      }')
      SAMPLES=$(echo "$PARSE" | cut -d'|' -f1)
      P50=$(echo "$PARSE" | cut -d'|' -f2)
      P90=$(echo "$PARSE" | cut -d'|' -f3)
      P99=$(echo "$PARSE" | cut -d'|' -f4)

      if [ -n "$SAMPLES" ] && [ "$SAMPLES" != "0" ]; then
        printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "$ALGO_FULL" "$BUF_PCT" "${P50}ns" "${P90}ns" "${P99}ns" "$SAMPLES"
        printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "$ALGO_FULL" "$BUF_PCT" "${P50}ns" "${P90}ns" "${P99}ns" "$SAMPLES" >> "$OUT"
        printf "    -> P50=%sns P90=%sns P99=%sns (samples=%s)\n" "$P50" "$P90" "$P99" "$SAMPLES"
      else
        printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "$ALGO_FULL" "$BUF_PCT" "N/A" "N/A" "N/A" "0"
        printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "$ALGO_FULL" "$BUF_PCT" "N/A" "N/A" "N/A" "0" >> "$OUT"
        printf "    -> No samples collected\n"
      fi
    else
      printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "$ALGO_FULL" "$BUF_PCT" "N/A" "N/A" "N/A" "0"
      printf "%-10s | %-8s | %10s | %10s | %10s | %12s\n" "$ALGO_FULL" "$BUF_PCT" "N/A" "N/A" "N/A" "0" >> "$OUT"
      printf "    -> No latency data found\n"
    fi
  done
done

echo ""
echo "==========================================="
echo "Benchmark Complete"
echo "Results saved to: $OUT"
echo "==========================================="
cat "$OUT"
