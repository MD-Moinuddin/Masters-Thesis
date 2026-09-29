package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.tara.Observer;
import com.example.types.tuples.BaseTuple;
import com.example.types.tuples.ViewTuple;

import java.net.URI;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

/**
 * Feeds view changes back into the dataflow. Receives {@code ViewTuple}s from
 * {@link FlinkSink.ViewSink} and from its peers (gossip every {@code OBSERVER_POLL} ms over REST,
 * port 11300 + subtask) and emits a new view once it advances.
 */
public class FlinkViewSource extends RichParallelSourceFunction<BaseTuple> {

    private static final long serialVersionUID = -2752586948690123687L;

    private int partition;
    private ViewTuple output;
    private Queue<ViewTuple> queue;
    private Observer view;
    private transient Client client;
    private WebTarget[] sources;

    private volatile boolean running = true;
    private transient HttpServer restServer;

    private final int sourceId;
    private transient BaseTuple.NodeId id;

    public FlinkViewSource(int sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.id        = new BaseTuple.NodeId(NodeType.VIEW_SOURCE, subtask);
        this.partition = TaraConfig.getPartition(subtask);

        this.output = new ViewTuple(id, partition, 0);
        this.queue  = new ConcurrentLinkedQueue<>();
        this.view   = new Observer(TaraConfig.CONTROLLERS);

        startRestServer(subtask);

        this.client  = ClientBuilder.newClient().register(JacksonFeature.class);
        this.sources = new WebTarget[TaraConfig.VIEW_SOURCES];
        for (int i = 0; i < TaraConfig.VIEW_SOURCES; i++) {
            if (i != subtask) {
                sources[i] = connectToViewSource(i);
            }
        }

        startPeriodicThread();

        TaraLog.info(this, "FlinkViewSource started, id=" + sourceId
                + " partition=" + partition + " subtask=" + subtask);
    }

    private void startRestServer(int subtask) throws Exception {
        int port = TaraConfig.VIEW_SOURCE_BASE_PORT + subtask;
        URI uri  = new URI("http://0.0.0.0:" + port + "/");

        final FlinkViewSource self = this;
        ResourceConfig config = new ResourceConfig();
        config.register(FlinkViewSourceWebService.class);
        config.register(JacksonFeature.class);
        config.register(new AbstractBinder() {
            @Override protected void configure() {
                bind(self).to(FlinkViewSource.class);
            }
        });

        restServer = GrizzlyHttpServerFactory.createHttpServer(uri, config, false);
        restServer.start();
        TaraLog.info(this, "ViewSource REST started on port " + port);
    }

    private void startPeriodicThread() {
        Thread t = new Thread(() -> {
            while (running) {
                gossip();
                try { Thread.sleep(TaraConfig.OBSERVER_POLL); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }, "FlinkViewSource-Periodic");
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void run(SourceContext<BaseTuple> ctx) throws Exception {
        while (running) {
            ViewTuple vt = queue.poll();

            if (vt != null) {
                TaraLog.info(this, "Received ViewTuple creator="
                        + vt.creator + " creatorType="
                        + (vt.creator != null ? vt.creator.type : "null")
                        + " view=" + vt.view + " current=" + view.current);

                boolean advanced = false;

                if (vt.creator == null) {
                    TaraLog.warning(this, "ViewTuple with null creator, skipping");

                } else if (vt.creator.type == NodeType.CONTROLLER) {
                    if (vt.view > view.current) {
                        TaraLog.info(this, "Controller ViewTuple: advancing view "
                                + view.current + " → " + vt.view);
                        view.current = vt.view;
                        advanced     = true;
                    } else {
                        TaraLog.info(this, "Controller ViewTuple view=" + vt.view
                                + " ignored — already at " + view.current);
                    }

                } else if (vt.creator.type == NodeType.VIEW_SOURCE) {
                    if (vt.view > view.current) {
                        TaraLog.info(this, "ViewSource gossip: advancing view "
                                + view.current + " → " + vt.view);
                        view.current = vt.view;
                        advanced     = true;
                    }

                } else {
                    if (view.update(vt.creator.id, vt.view)) {
                        TaraLog.info(this, "View quorum reached: view=" + view.current);
                        advanced = true;
                    }
                }

                if (advanced) {
                    this.output           = new ViewTuple(id, partition, view.current);
                    this.output.partition = this.partition;
                    TaraLog.info(this, "Publishing new ViewTuple view=" + view.current);
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(output);
                    }
                }

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
        if (restServer != null) {
            restServer.shutdownNow();
            TaraLog.info(this, "ViewSource REST server stopped");
        }
        if (client != null) client.close();
        super.close();
    }

    public void gossip() {
        for (int i = 0; i < sources.length; i++) {
            if (sources[i] != null) {
                try {
                    sources[i].request().post(Entity.json(output)).close();
                } catch (Exception e) {
                    sources[i] = connectToViewSource(i);
                }
            }
        }
    }

    private WebTarget connectToViewSource(int targetId) {
        try {
            String host = System.getenv("TARA_HOST") != null
                    ? System.getenv("TARA_HOST") : "127.0.0.1";
            int port = TaraConfig.VIEW_SOURCE_BASE_PORT + targetId;
            URI uri  = UriBuilder.fromUri("http://" + host)
                    .port(port).path("/source").build();
            return client.target(uri);
        } catch (Exception e) {
            TaraLog.warning(this, "Failed to connect ViewSource[" + targetId + "]");
            return null;
        }
    }

    public void receiveViewTuple(ViewTuple v) {
        TaraLog.info(this, "REST received ViewTuple creator="
                + v.creator + " view=" + v.view);
        queue.add(v);
    }

    @Singleton
    @Path("/source")
    public static class FlinkViewSourceWebService {

        private final FlinkViewSource source;

        @Inject
        public FlinkViewSourceWebService(FlinkViewSource source) {
            this.source = source;
        }

        @POST
        @Consumes(MediaType.APPLICATION_JSON)
        public Response receive(String rawBody) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                BaseTuple base = mapper.readValue(rawBody, BaseTuple.class);
                if (!(base instanceof ViewTuple)) {
                    TaraLog.plain("[VIEW_SOURCE] Unexpected tuple type: "
                            + base.getClass().getSimpleName() + " | body=" + rawBody);
                    return Response.status(400).entity("Not a ViewTuple").build();
                }
                source.receiveViewTuple((ViewTuple) base);
                return Response.noContent().build();
            } catch (Exception e) {
                TaraLog.plain("[VIEW_SOURCE] Deserialization failed: "
                        + e.getMessage() + " | body=" + rawBody);
                return Response.status(400).entity(e.getMessage()).build();
            }
        }
    }
}