package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.tuples.BaseTuple;
import com.example.types.tuples.RecordTuple;

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
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Feeds committer records back to the proposers during a view change: receives
 * {@code RecordTuple}s from {@link FlinkSink.RecordSink} over REST (port 11400 + subtask).
 */
public class FlinkRecordSource extends RichParallelSourceFunction<BaseTuple> {

    private static final long serialVersionUID = -8801267881249323686L;

    private final int sourceId;

    private transient int partition;
    private transient Queue<RecordTuple> records;
    private transient volatile boolean running;
    private transient org.glassfish.grizzly.http.server.HttpServer restServer;

    public FlinkRecordSource(int sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        int subtask  = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.partition = TaraConfig.getPartition(subtask);
        this.records   = new ConcurrentLinkedQueue<>();
        this.running   = true;

        startRecordWebService(subtask);

        TaraLog.info(this, "FlinkRecordSource started, id=" + sourceId
                + " partition=" + partition + " subtask=" + subtask);
    }

    private void startRecordWebService(int subtask) throws Exception {
        int port = TaraConfig.RECORD_SOURCE_BASE_PORT + subtask;
        URI uri  = UriBuilder.fromUri("http://0.0.0.0:" + port + "/").build();

        ResourceConfig config = new ResourceConfig();
        FlinkRecordSourceWebService wsInstance = new FlinkRecordSourceWebService(this);
        config.register(wsInstance);
        config.register(JacksonFeature.class);

        restServer = GrizzlyHttpServerFactory.createHttpServer(uri, config, false);
        restServer.start();

        TaraLog.info(this, "RecordSource REST started on port " + port);
    }

    @Override
    public void run(SourceContext<BaseTuple> ctx) throws Exception {
        while (running) {
            RecordTuple tuple = records.poll();

            if (tuple != null) {
                TaraLog.info(this, "Emitting RecordTuple from committer=" + tuple.creator
                        + " window=[" + tuple.records.min() + "," + tuple.records.pos() + ")");

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(tuple);
                }
            } else {
                Thread.sleep(10);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (restServer != null) {
            restServer.shutdownNow();
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (restServer != null) {
            restServer.shutdownNow();
            TaraLog.info(this, "RecordSource REST server stopped");
        }
        super.close();
    }

    /** Called by the REST endpoint on every incoming POST. */
    public void receiveRecordTuple(RecordTuple r) {
        records.add(r);
    }

    @Path("/source")
    public static class FlinkRecordSourceWebService {

        private final FlinkRecordSource source;

        public FlinkRecordSourceWebService(FlinkRecordSource source) {
            this.source = source;
        }

        @POST
        @Consumes(MediaType.APPLICATION_JSON)
        public Response receive(String rawBody) {
            try {
                BaseTuple base = new ObjectMapper().readValue(rawBody, BaseTuple.class);
                if (!(base instanceof RecordTuple)) {
                    return Response.status(400).entity("Not a RecordTuple").build();
                }
                source.receiveRecordTuple((RecordTuple) base);
                return Response.noContent().build();
            } catch (Exception e) {
                TaraLog.warning(source, "RecordTuple deserialization failed: " + e.getMessage());
                return Response.status(400).entity(String.valueOf(e.getMessage())).build();
            }
        }
    }
}