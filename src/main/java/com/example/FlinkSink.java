package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.tuples.BaseTuple;
import com.example.types.tuples.RecordTuple;
import com.example.types.tuples.ViewTuple;
import com.example.types.tuples.GCTuple;
import com.example.types.tuples.ReplyTuple;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Collections;

/**
 * Sinks close the loops that a Flink DAG cannot express: Tara needs tuples to flow from later
 * stages back to earlier ones, so each sink POSTs its tuple as JSON to the matching source's
 * REST endpoint. The sink of subtask <i>i</i> talks to source <i>i</i> on {@code TARA_HOST}
 * (default 127.0.0.1). Delivery is at-most-once: a failed POST is logged, the sink reconnects,
 * and the tuple is dropped.
 */
public abstract class FlinkSink extends RichSinkFunction<BaseTuple> {

    private static final long serialVersionUID = 3635199459506346574L;

    protected transient Client  client;
    protected transient WebTarget source;
    protected transient BaseTuple.NodeId id;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.id     = new BaseTuple.NodeId(type(), subtask);
        this.client = ClientBuilder.newClient().register(JacksonFeature.class);
        this.source = connectSource();
        TaraLog.info(this, "FlinkSink " + type() + " open: subtask=" + subtask
                + " target=" + (source != null ? source.getUri() : "null"));
    }

    private WebTarget connectSource() {
        try {
            String host = System.getenv("TARA_HOST") != null
                    ? System.getenv("TARA_HOST") : "127.0.0.1";
            int port = basePort() + id.id;
            URI uri  = UriBuilder.fromUri("http://" + host)
                    .port(port).path("/" + sourceName()).build();
            TaraLog.info(this, "Connecting to source at " + uri);
            return client.target(uri);
        } catch (Exception e) {
            TaraLog.warning(this, "Failed to connect sink id=" + id.id + ": " + e.getMessage());
            return null;
        }
    }

    protected abstract int basePort();
    protected abstract String sourceName();
    public abstract NodeType type();
    protected abstract void post(WebTarget src, BaseTuple tuple) throws Exception;

    @Override
    public void invoke(BaseTuple value, Context context) throws Exception {
        if (source == null) {
            source = connectSource();
            if (source == null) {
                TaraLog.warning(this, "Source still null, dropping: " + value);
                return;
            }
        }
        try {
            post(source, value);
        } catch (Exception e) {
            TaraLog.error(this, "Error posting, reconnecting: " + e.getMessage());
            source = connectSource();
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) client.close();
        super.close();
    }

    public static class RecordSink extends FlinkSink {
        @Override public NodeType type()        { return NodeType.RECORD_SINK; }
        @Override protected int basePort()      { return TaraConfig.RECORD_SOURCE_BASE_PORT; }

        @Override protected String sourceName() { return "source"; }

        @Override
        protected void post(WebTarget src, BaseTuple tuple) throws Exception {
            if (!(tuple instanceof RecordTuple)) return;
            RecordTuple r = (RecordTuple) tuple;
            Response response = src.request().post(Entity.json(r));
            response.close();
        }
    }

    public static class GCSink extends FlinkSink {
        @Override public NodeType type()       { return NodeType.GC_SINK; }
        @Override protected int basePort()     { return TaraConfig.GC_SOURCE_BASE_PORT; }
        @Override protected String sourceName(){ return "gcSource"; }

        @Override
        protected void post(WebTarget src, BaseTuple tuple) throws Exception {
            if (!(tuple instanceof GCTuple)) return;
            GCTuple gt = (GCTuple) tuple;

            src.request().post(Entity.json(gt)).close();
        }
    }

    public static class ReplySink extends FlinkSink {
        @Override public NodeType type()       { return NodeType.REPLY_SINK; }
        @Override protected int basePort()     { return TaraConfig.RESULT_SINK_BASE_PORT; }
        @Override protected String sourceName(){ return "resultQueue"; }

        @Override
        protected void post(WebTarget src, BaseTuple tuple) throws Exception {
            if (!(tuple instanceof ReplyTuple)) return;
            ReplyTuple rt = (ReplyTuple) tuple;
            src.request().post(Entity.json(
                    Collections.singletonList(rt.result))).close();
        }
    }

    public static class ViewSink extends RichSinkFunction<BaseTuple> {

        private static final long serialVersionUID = 7312089054960463L;

        private transient Client client;
        private transient WebTarget[] targets;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            client  = ClientBuilder.newClient().register(JacksonFeature.class);
            targets = new WebTarget[TaraConfig.VIEW_SOURCES];
            for (int i = 0; i < TaraConfig.VIEW_SOURCES; i++) {
                TaraLog.info(this, "VIEW_SINK open: connecting to ViewSource[" + i
                        + "] port=" + (TaraConfig.VIEW_SOURCE_BASE_PORT + i));
                targets[i] = connect(i);
            }
        }

        @Override
        public void invoke(BaseTuple value, Context context) throws Exception {
            if (!(value instanceof ViewTuple)) {
                TaraLog.warning(this, "ViewSink received non-ViewTuple: "
                        + value.getClass().getSimpleName() + " — dropping");
                return;
            }
            ViewTuple vt = (ViewTuple) value;
            TaraLog.info(this, "ViewSink invoke: broadcasting ViewTuple view="
                    + vt.view + " to " + targets.length + " ViewSources");

            for (int i = 0; i < targets.length; i++) {
                if (targets[i] == null) targets[i] = connect(i);
                try {
                    Response r = targets[i].request().post(Entity.json(vt));
                    TaraLog.info(this, "POST → ViewSource[" + i + "] status=" + r.getStatus());
                    r.close();
                } catch (Exception e) {
                    TaraLog.error(this, "POST → ViewSource[" + i + "] FAILED: "
                            + e.getMessage() + " — reconnecting");
                    targets[i] = connect(i);
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (client != null) client.close();
            super.close();
        }

        private WebTarget connect(int index) {
            try {
                String host = System.getenv("TARA_HOST") != null
                        ? System.getenv("TARA_HOST") : "127.0.0.1";
                int port = TaraConfig.VIEW_SOURCE_BASE_PORT + index;
                URI uri  = UriBuilder.fromUri("http://" + host)
                        .port(port).path("/source").build();
                TaraLog.info(this, "Connecting to ViewSource[" + index + "] at " + uri);
                return client.target(uri);   // one shared Client; a new one per reconnect would leak
            } catch (Exception e) {
                TaraLog.warning(this, "Failed to connect ViewSource[" + index + "]: "
                        + e.getMessage());
                return null;
            }
        }
    }
}