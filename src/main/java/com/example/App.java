package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.Application;
import com.example.types.Payload;
import com.example.types.tuples.BaseTuple;
import com.example.types.tuples.ConsensusTuple;
import com.example.types.tuples.GCTuple;
import com.example.types.tuples.ProgressTuple;
import com.example.types.tuples.RecordTuple;
import com.example.types.tuples.ReplyTuple;
import com.example.types.tuples.ViewTuple;
import com.example.types.tuples.RequestTuple;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * Builds and submits the Tara-on-Flink job: request/view/record/GC sources feed Proposer →
 * Committer → Executor → Controller operators, and sinks return records, GC tuples, replies and
 * views to their sources. Configuration comes from {@link TaraConfig} and the environment
 * variables {@code BUFFER_TIMEOUT_MS} (Flink network buffer timeout, default 100) and
 * {@code REPLY_BYTES} (fixed reply size for the payload experiments).
 */
public class App {
    public static void main(String[] args) throws Exception {

        TaraConfig.initializeZookeeper(true);

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
        config.set(PipelineOptions.FORCE_KRYO, true);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(5000);

        env.setBufferTimeout(Long.parseLong(System.getenv().getOrDefault("BUFFER_TIMEOUT_MS", "100")));

        // Sources

        DataStream<BaseTuple> allRequests = env
                .addSource(new FlinkRequestSource(0))
                .name("Flink-RequestSource")
                .setParallelism(TaraConfig.REQUEST_SOURCES)
                .disableChaining()
                .returns(TypeInformation.of(BaseTuple.class));

        DataStream<BaseTuple> allViews = env
                .addSource(new FlinkViewSource(0))
                .name("Flink-ViewSource")
                .setParallelism(TaraConfig.VIEW_SOURCES)
                .disableChaining()
                .returns(TypeInformation.of(BaseTuple.class));

        DataStream<BaseTuple> allRecords = env
                .addSource(new FlinkRecordSource(0))
                .name("Flink-RecordSource")
                .setParallelism(TaraConfig.RECORD_SOURCES)
                .disableChaining()
                .returns(TypeInformation.of(BaseTuple.class));

        DataStream<BaseTuple> allGC = env
                .addSource(new FlinkGCSource(0))
                .name("Flink-GCSource")
                .setParallelism(TaraConfig.GC_SOURCES)
                .disableChaining()
                .returns(TypeInformation.of(BaseTuple.class));

        // Split the request stream: RequestTuples → Proposer, ProgressTuples → Controller
        DataStream<BaseTuple> requestsOnly = allRequests
                .filter(t -> t instanceof RequestTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .name("Requests-Only");

        DataStream<BaseTuple> progressFromRequests = allRequests
                .filter(t -> t instanceof ProgressTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .name("Request-Progress");

        // Proposer

        DataStream<BaseTuple> proposerInput =
                broadcastToSubtasks(requestsOnly, TaraConfig.PROPOSERS, "Requests-to-Proposers")
                .union(
                        broadcastToSubtasks(allViews,   TaraConfig.PROPOSERS, "Views-to-Proposers"),
                        broadcastToSubtasks(allRecords, TaraConfig.PROPOSERS, "Records-to-Proposers"),
                        broadcastToSubtasks(allGC,      TaraConfig.PROPOSERS, "GC-to-Proposers"));

        DataStream<ConsensusTuple> afterProposer = proposerInput
                .keyBy(t -> t.targetKey)
                .process(new FlinkProposer())
                .name("Flink-Proposer")
                .setParallelism(TaraConfig.PROPOSERS)
                .returns(TypeInformation.of(ConsensusTuple.class));

        // Committer

        DataStream<BaseTuple> committerInput = afterProposer
                .map(ct -> (BaseTuple) ct)
                .returns(TypeInformation.of(BaseTuple.class))
                .union(
                        broadcastToSubtasks(allGC,    TaraConfig.COMMITTERS, "GC-to-Committers"),
                        broadcastToSubtasks(allViews, TaraConfig.COMMITTERS, "Views-to-Committers"));

        DataStream<BaseTuple> afterCommitter = committerInput
                .keyBy(t -> t.targetKey)
                .process(new FlinkCommitter())
                .name("Flink-Committer")
                .setParallelism(TaraConfig.COMMITTERS)
                .returns(TypeInformation.of(BaseTuple.class));

        // RecordTuples: Committer → RecordSink → RecordSource → Proposer
        afterCommitter
                .filter(t -> t instanceof RecordTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .addSink(new FlinkSink.RecordSink())
                .name("Flink-RecordSink")
                .setParallelism(TaraConfig.COMMITTERS);

        // Executor

        DataStream<BaseTuple> commitsOnly = afterCommitter
                .filter(t -> !(t instanceof RecordTuple))
                .returns(TypeInformation.of(BaseTuple.class))
                .name("Commits-Only");

        final String replyEnv = System.getenv("REPLY_BYTES");
        final int replyBytes  = (replyEnv != null) ? Integer.parseInt(replyEnv.trim()) : -1;

        Application app = new Application() {
            @Override public void init() { }
            @Override
            public byte[] execute(Payload p) {
                if (replyBytes >= 0) return new byte[replyBytes];
                String res = "result-" + p.cid + "-" + p.xnr;
                TaraLog.debug(this, "Executing: " + res);
                return res.getBytes();
            }
            @Override public byte[] getState()        { return new byte[0]; }
            @Override public void applyState(byte[] s) { }
        };

        DataStream<BaseTuple> executorInput = commitsOnly
                .union(
                        broadcastToSubtasks(allGC,    TaraConfig.EXECUTORS, "GC-to-Executors"),
                        broadcastToSubtasks(allViews, TaraConfig.EXECUTORS, "Views-to-Executors"));

        DataStream<BaseTuple> afterExecutor = executorInput
                .keyBy(t -> t.targetKey)
                .process(new FlinkExecutor(app))
                .name("Flink-Executor")
                .setParallelism(TaraConfig.EXECUTORS)
                .returns(TypeInformation.of(BaseTuple.class));

        // GCTuples: Executor → GCSink → GCSource → all replicas
        afterExecutor
                .filter(t -> t instanceof GCTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .addSink(new FlinkSink.GCSink())
                .name("Flink-GCSink")
                .setParallelism(TaraConfig.GC_SOURCES);

        afterExecutor
                .filter(t -> t instanceof ReplyTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .keyBy(t -> t.targetKey)
                .addSink(new FlinkSink.ReplySink())
                .name("Flink-ReplySink")
                .setParallelism(TaraConfig.REQUEST_SOURCES);

        // Controller

        DataStream<BaseTuple> progressFromExecutors = afterExecutor
                .filter(t -> t instanceof ProgressTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .name("Executor-Progress");

        DataStream<BaseTuple> controllerInput =
                broadcastToSubtasks(progressFromRequests, TaraConfig.CONTROLLERS, "ReqProgress-to-Controllers")
                .union(
                        broadcastToSubtasks(progressFromExecutors, TaraConfig.CONTROLLERS, "ExecProgress-to-Controllers"),
                        broadcastToSubtasks(allViews,              TaraConfig.CONTROLLERS, "Views-to-Controllers"));

        DataStream<BaseTuple> controllerOutput = controllerInput
                .keyBy(t -> t.targetKey)
                .process(new FlinkController())
                .name("Flink-Controller")
                .setParallelism(TaraConfig.CONTROLLERS)
                .returns(TypeInformation.of(BaseTuple.class))
                .disableChaining();

        DataStream<BaseTuple> controllerViews = controllerOutput
                .filter(t -> t instanceof ViewTuple)
                .returns(TypeInformation.of(BaseTuple.class))
                .name("Controller-Views");

        controllerViews
                .rebalance()
                .addSink(new FlinkSink.ViewSink())
                .name("Flink-ViewSink")
                .setParallelism(TaraConfig.VIEW_SOURCES);

        env.execute("Tara-on-Flink");
    }

    private static DataStream<BaseTuple> broadcastToSubtasks(
            DataStream<BaseTuple> source, int parallelism, String name) {
        return source
                .flatMap(new FlatMapFunction<BaseTuple, BaseTuple>() {
                    @Override
                    public void flatMap(BaseTuple value, Collector<BaseTuple> out) {
                        for (int s = 0; s < parallelism; s++) {
                            BaseTuple copy = value.shallowCopy();
                            copy.targetKey = TaraConfig.keyForSubtask(s, parallelism);
                            out.collect(copy);
                        }
                    }
                })
                .returns(TypeInformation.of(BaseTuple.class))
                .name(name);
    }
}
