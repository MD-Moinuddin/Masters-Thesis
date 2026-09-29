#!/bin/bash

# Batched-consensus benchmark orchestrator (batch size 5, 10 ms timeout).
# Requires ZooKeeper on localhost:2181 and a Flink 2.1.0 distribution:
#   FLINK_HOME=/path/to/flink-2.1.0 ./run_batched_all.sh
# Output: combo{1..4}-*.csv in the repo root, full log in $LOG.

set -u
cd "$(dirname "$0")"
FL=${FLINK_HOME:?set FLINK_HOME to your Flink 2.1.0 directory}
JAR=target/flink-multinodes-1.0-SNAPSHOT-shaded.jar
PTS="1,16,32,64,96,128,192,256"
LOG=${LOG:-/tmp/batched_all.log}

deploy() {  # $1 = reply bytes
  # restart cluster if the TaskManager has died
  local tms
  tms=$(curl -s localhost:8081/overview 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('taskmanagers',0))" 2>/dev/null)
  if [ "$tms" != "1" ]; then
    echo "[orch] cluster unhealthy (tms=$tms) -> restart $(date)" >> "$LOG"
    for J in $($FL/bin/flink list 2>/dev/null|grep -iE "RUNNING|RESTARTING"|grep -oE '[0-9a-f]{32}'); do $FL/bin/flink cancel "$J" >/dev/null 2>&1; done
    $FL/bin/stop-cluster.sh >/dev/null 2>&1; sleep 4
    export TARA_BATCH_SIZE=5 TARA_BATCH_TIMEOUT=10; unset TARA_LOG
    $FL/bin/start-cluster.sh >> "$LOG" 2>&1; sleep 12
  fi
  for J in $($FL/bin/flink list 2>/dev/null|grep RUNNING|grep -oE '[0-9a-f]{32}'); do $FL/bin/flink cancel "$J" >/dev/null 2>&1; done
  sleep 4
  REPLY_BYTES=$1 BUFFER_TIMEOUT_MS=5 $FL/bin/flink run -d "$JAR" >> "$LOG" 2>&1
  local i
  for i in $(seq 1 50); do
    sleep 3
    if nc -z -w1 localhost 11100 2>/dev/null && curl -s localhost:12000/control/status 2>/dev/null | grep -q NORMAL; then
      echo "[orch] deployed reply=$1, UP $(date)" >> "$LOG"; return 0
    fi
  done
  echo "[orch] WARN deploy reply=$1 not fully up $(date)" >> "$LOG"
}

warmup() {  # drive sustained load to JIT-compile the pipeline (result discarded)
  echo "[orch] warmup START $(date)" >> "$LOG"
  REQ_BYTES=3072 REPS=1 WARMUP_SEC=0 MEASURE_SEC=300 COOLDOWN_SEC=0 \
    java -cp "$JAR" com.example.client.BatchedBenchmark localhost 11100 /tmp/warmup.csv 128 >> "$LOG" 2>&1
  echo "[orch] warmup DONE  $(date)" >> "$LOG"
}

runcombo() {  # $1 req bytes, $2 csv, $3 label
  echo "[orch] ===== $3 START $(date) =====" >> "$LOG"
  REQ_BYTES=$1 java -cp "$JAR" com.example.client.BatchedBenchmark localhost 11100 "$2" "$PTS" >> "$LOG" 2>&1
  echo "[orch] ===== $3 DONE  $(date) =====" >> "$LOG"
}

echo "[orch] START $(date)  points=$PTS" > "$LOG"

# Fresh cluster so warmth starts from a known cold state
for J in $($FL/bin/flink list 2>/dev/null|grep -iE "RUNNING|RESTARTING"|grep -oE '[0-9a-f]{32}'); do $FL/bin/flink cancel "$J" >/dev/null 2>&1; done
$FL/bin/stop-cluster.sh >/dev/null 2>&1; sleep 4
export TARA_BATCH_SIZE=5 TARA_BATCH_TIMEOUT=10; unset TARA_LOG
$FL/bin/start-cluster.sh >> "$LOG" 2>&1; sleep 12
echo "[orch] fresh cluster started $(date)" >> "$LOG"

deploy 0
warmup                                                       # one warmup -> warms the cold TM JVM for ALL combos
runcombo 0    combo1-null-null-warm.csv   "combo1 null/null"
deploy 0;     runcombo 3072 combo3-large-null-warm.csv  "combo3 large/null"
deploy 3072;  runcombo 0    combo2-null-large-warm.csv  "combo2 null/large"
deploy 3072;  runcombo 3072 combo4-large-large-warm.csv "combo4 large/large"

echo "[orch] ALL DONE $(date)" >> "$LOG"
