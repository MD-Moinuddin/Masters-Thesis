# Tara on Flink

Master's thesis project: design, implementation and evaluation of the **Tara** stream-based
consensus protocol on **Apache Flink 2.1**. Every Tara role (proposer, committer, executor,
controller) is a Flink operator; the feedback edges that a DAG cannot express (records, GC,
views, replies) go through REST-connected sink/source pairs.

## Layout

| Path | Contents |
|---|---|
| `src/main/java/com/example/App.java` | Builds and submits the Flink job |
| `src/main/java/com/example/Flink{Proposer,Committer,Executor,Controller}.java` | Protocol roles |
| `src/main/java/com/example/Flink*Source.java`, `FlinkSink.java` | Client entry point and feedback loops |
| `src/main/java/com/example/tara/` | Configuration, logging, quorum `Observer`, ZooKeeper helpers |
| `src/main/java/com/example/types/` | Windows, requests, snapshots and the tuple types |
| `src/main/java/com/example/client/` | Test client, closed-loop benchmark, fixed-rate load generator |
| `csv files/` | Recorded evaluation results (`bt0` / `bt2` = Flink buffer timeout 0 / 2 ms). Not in git for now (see `.gitignore`); keep a local copy to use the plot scripts |
| `run_batched_all.sh`, `run-experiment.sh` | Batched benchmark orchestration, leader-kill driver |
| `plot_batched.py`, `plot_exp4.py` | Plots for the throughput/latency and fault-tolerance results |

## Prerequisites

- Java 11, Maven 3.x
- Apache Flink 2.1.0 distribution (with at least 8 task slots, e.g. `taskmanager.numberOfTaskSlots: 8`)
- ZooKeeper on `localhost:2181`
- Python 3 with `matplotlib` for the plots

## Build and test

```bash
mvn clean package        # runs the unit tests, builds target/flink-multinodes-1.0-SNAPSHOT-shaded.jar
```

## Run

```bash
zkServer start
export TARA_BATCH_SIZE=1 TARA_BATCH_TIMEOUT=10 BUFFER_TIMEOUT_MS=0   # set BEFORE starting the cluster
$FLINK_HOME/bin/start-cluster.sh
$FLINK_HOME/bin/flink run -d target/flink-multinodes-1.0-SNAPSHOT-shaded.jar

# smoke test: sends 3 requests, prints the replies
java -cp target/flink-multinodes-1.0-SNAPSHOT-shaded.jar com.example.client.TestClient localhost
```

Proposer status: `curl localhost:12000/control/status` (subtask 1 on port 12001).

### Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `TARA_BATCH_SIZE` | 200 | Commands per request batch. **Use 1 for non-batched runs**, 5 for `run_batched_all.sh` |
| `TARA_BATCH_TIMEOUT` | 1000 | Milliseconds before a partial batch is flushed |
| `BUFFER_TIMEOUT_MS` | 100 | Flink network buffer timeout (the `bt0` / `bt2` results use 0 / 2) |
| `REPLY_BYTES` | unset | Fixed reply size for the payload experiments |
| `TARA_HOST` | 127.0.0.1 | Host the sinks use to reach their sources |
| `TARA_CONTROL_HOST` | 127.0.0.1 | Bind address of the leader kill/revive endpoint (unauthenticated) |
| `TARA_LOG` | 1 | 0 = errors only, 1 = +warnings, 2 = +info, 3 = +debug |

The defaults for batch size and timeout are deliberately not the evaluation settings; with them a
single client sees roughly one request per second.

## Experiments

- **Throughput vs latency** (closed loop): `BatchedBenchmark <host> <port> <csv> <conc1,conc2,...>`
  (environment: `REQ_BYTES`, `WARMUP_SEC`, `MEASURE_SEC`, `COOLDOWN_SEC`, `REPS`). Batching is chosen
  server-side, so `NonbatchedBenchmark` runs the same code. `FLINK_HOME=... ./run_batched_all.sh`
  runs the four payload combinations of the batched experiment.
- **Fault tolerance**: run `LoadGenerator <host> <port> <targetRps> <durationSec> <warmupSec> <concurrency> <csv>`
  and, in a second terminal, `./run-experiment.sh` (kills the leader at t = 60 s and t = 120 s).
  Plot with `python3 plot_exp4.py "csv files/fault-tolerance-csv/fault-bt0-4000-run1.csv"`.
- **Plots**: `python3 plot_batched.py <csv> [<csv> ...]` (writes `batched-throughput-latency.png`).

## Design notes and known limitations

- **Protocol state is held in operator fields, not Flink managed state.** Flink checkpointing
  (`enableCheckpointing(5000)`) therefore does not protect it; recovery relies on Tara's own
  snapshots (written to `~/tara-snapshots`) and garbage collection.
- **One key per subtask.** Operators are keyed by `targetKey`; `TaraConfig.keyForSubtask` finds a key
  that Flink routes to a given subtask (assumes 128 max key groups). This is what makes broadcasts and
  point-to-point delivery possible, but it is a workaround, not idiomatic Flink.
- **Effectively single host.** Sink *i* talks to source *i* on `TARA_HOST`, so sources and sinks must
  be co-located.
- **Fixed configuration**: F = 1 and one partition are compile-time constants in `TaraConfig`.
- Uses the legacy `SourceFunction` / `SinkFunction` APIs (moved to `...legacy` packages in Flink 2.x);
  migrating to FLIP-27 sources and Sink V2 is future work.
- Benchmarks report mean throughput and mean latency only (no percentiles or variance).
