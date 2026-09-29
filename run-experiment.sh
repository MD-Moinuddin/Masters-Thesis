#!/bin/bash
# Fault-tolerance experiment driver: kills the current proposer (leader) twice while a load
# generator is running in another terminal.
#
#   Terminal 1: flink run -d target/flink-multinodes-1.0-SNAPSHOT-shaded.jar   (cluster + ZooKeeper up)
#   Terminal 2: start the load generator (see README), then immediately run this script.
#
# The kill times match the dips in "csv files/fault-tolerance-csv" (t = 60 s and t = 120 s).
# Override with FIRST_KILL_SEC / GAP_SEC. Control endpoints listen on localhost only.

FIRST_KILL_SEC=${FIRST_KILL_SEC:-60}
GAP_SEC=${GAP_SEC:-60}

find_leader() {
  for PORT in 12000 12001; do
    STATUS=$(curl -s http://localhost:$PORT/control/status 2>/dev/null)
    if echo "$STATUS" | grep -q "mode=NORMAL"; then
      echo $PORT
      return
    fi
  done
  echo "NONE"
}

kill_leader() {  # $1 = label
  LEADER=$(find_leader)
  if [ "$LEADER" = "NONE" ]; then
    echo "ERROR: no leader found for $1 kill. Is the Flink job running?"
    exit 1
  fi
  echo "[$(date +%T)] Killing leader on port $LEADER ($1 kill)"
  curl -s -X POST http://localhost:$LEADER/control/kill
  echo
}

echo "[$(date +%T)] Experiment started. First kill in ${FIRST_KILL_SEC}s."
sleep "$FIRST_KILL_SEC"
kill_leader first

echo "Second kill in ${GAP_SEC}s."
sleep "$GAP_SEC"
kill_leader second

echo "--- proposer status ---"
curl -s http://localhost:12000/control/status; echo
curl -s http://localhost:12001/control/status; echo
echo "Experiment complete. Wait for the load generator to finish."
