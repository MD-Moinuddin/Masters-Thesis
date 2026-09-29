package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.tuples.BaseTuple;
import com.example.types.tuples.GCTuple;

import java.net.URI;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Feeds garbage-collection signals back into the dataflow: receives {@code GCTuple}s from
 * {@link FlinkSink.GCSink} over REST (port 11200 + subtask) and emits them to all replicas.
 */
public class FlinkGCSource extends RichParallelSourceFunction<BaseTuple> {

    private static final long serialVersionUID = 1L;

    private final int sourceId;
    private transient BaseTuple.NodeId id;

    private volatile boolean running = true;
    private int partition;
    private Queue<GCTuple> gcQueue;
    private transient HttpServer restServer;

    public FlinkGCSource(int sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

        this.partition = TaraConfig.getPartition(subtask);
        this.id        = new BaseTuple.NodeId(NodeType.GC_SOURCE, subtask);
        this.gcQueue   = new ConcurrentLinkedQueue<>();

        startRestServer(subtask);

        TaraLog.info(this, "FlinkGCSource started, id=" + sourceId +
                " partition=" + partition + " subtask=" + subtask);
    }

    private void startRestServer(int subtask) throws Exception {
        int port = TaraConfig.GC_SOURCE_BASE_PORT + subtask;
        URI uri  = UriBuilder.fromUri("http://0.0.0.0:" + port + "/").build();

        ResourceConfig config = new ResourceConfig();
        config.register(new GCSourceWebService(this));
        config.register(JacksonFeature.class);

        restServer = GrizzlyHttpServerFactory.createHttpServer(uri, config, false);
        restServer.start();
        TaraLog.info(this, "GCSource REST started on port " + port);
    }

    @Override
    public void run(SourceContext<BaseTuple> ctx) throws Exception {
        while (running) {
            GCTuple gc = gcQueue.poll();

            if (gc != null) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(gc);
                }
                TaraLog.info(this, "Emitting GC signal: " + gc);
            } else {
                Thread.sleep(10);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (restServer != null) restServer.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (restServer != null) restServer.shutdownNow();
        super.close();
    }

    public void receiveGCTuple(GCTuple gc) {
        gcQueue.add(gc);
    }

    @Path("/gcSource")
    public static class GCSourceWebService {

        private final FlinkGCSource source;

        public GCSourceWebService(FlinkGCSource source) {
            this.source = source;
        }

        @POST
        @Consumes(MediaType.APPLICATION_JSON)
        public Response receive(String rawBody) {
            try {
                BaseTuple base = new ObjectMapper().readValue(rawBody, BaseTuple.class);
                if (!(base instanceof GCTuple)) {
                    return Response.status(400).entity("Not a GCTuple").build();
                }
                source.receiveGCTuple((GCTuple) base);
                return Response.noContent().build();
            } catch (Exception e) {
                TaraLog.warning(source, "GCTuple deserialization failed: " + e.getMessage());
                return Response.status(400).entity(String.valueOf(e.getMessage())).build();
            }
        }
    }

    @Override
    public String toString() {
        return "FlinkGCSource{" +
                "sourceId=" + sourceId +
                ", partition=" + partition +
                '}';
    }
}
