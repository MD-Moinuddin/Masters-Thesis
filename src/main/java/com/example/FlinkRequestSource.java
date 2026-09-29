package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.Payload;
import com.example.types.Request;
import com.example.types.tuples.BaseTuple;
import com.example.types.tuples.ProgressTuple;
import com.example.types.tuples.RequestTuple;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;

/**
 * Entry point for clients. Listens on a TCP socket (port 11100 + subtask), batches incoming
 * commands ({@code TARA_BATCH_SIZE} / {@code TARA_BATCH_TIMEOUT}) into a
 * {@link com.example.types.Request}, and emits it to the proposers together with periodic
 * progress tuples. Replies come back through the {@code /resultQueue} REST endpoint (fed by
 * {@link FlinkSink.ReplySink}) and are written to the client socket, once per (client, xnr).
 */
public class FlinkRequestSource extends RichParallelSourceFunction<BaseTuple> {

    private static final long serialVersionUID = 1L;

    private final int sourceId;
    private transient BaseTuple.NodeId id;

    private volatile boolean running = true;
    private int subtask;
    private int partition;
    private Queue<Payload> commands;
    private int next;
    private int nextActual;
    private long batchTimeout;
    private List<Payload> batch;
    private long progressTimer;

    private Map<Integer, DataOutputStream> clients;
    private Map<Integer, Integer> resultFilter;

    private transient HttpServer restServer;
    private transient ServerSocket clientServerSocket;

    public FlinkRequestSource(int sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.subtask   = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.partition = TaraConfig.getPartition(this.subtask);
        this.id        = new BaseTuple.NodeId(NodeType.REQUEST_SOURCE, this.subtask);
        this.commands  = new ConcurrentLinkedQueue<>();
        this.next      = 0;
        this.nextActual = 0;
        this.batch     = new LinkedList<>();
        this.clients      = new ConcurrentHashMap<>();
        this.resultFilter = new ConcurrentHashMap<>();

        TaraConfig.initializeZookeeper(false);

        // every subtask starts its own REST server on its own port
        boolean disableRest = System.getenv("DISABLE_REST") != null;
        if (!disableRest) {
            startResultWebService();
        }

        TaraConfig.publishRequestSourceLocation(subtask);
        startClientSocketServer(this.subtask);

        TaraLog.info(this, "FlinkRequestSource started subtask=" + subtask
                + " partition=" + partition);
    }

    @Override
    public void run(SourceContext<BaseTuple> ctx) throws Exception {
        batchTimeout  = System.currentTimeMillis() + TaraConfig.BATCH_TIMEOUT;
        progressTimer = System.currentTimeMillis() + TaraConfig.PROGRESS_INTERVAL;

        while (running) {
            long now          = System.currentTimeMillis();
            boolean forceBatch    = now > batchTimeout;
            boolean forceProgress = now > progressTimer;

            if (!forceBatch && commands.isEmpty()) {
                Thread.sleep(2);
                if (System.currentTimeMillis() > progressTimer) {
                    synchronized (ctx.getCheckpointLock()) {
                        emitProgressLocked(ctx);
                    }
                }
                continue;
            }

            while (batch.size() < TaraConfig.BATCH_SIZE) {
                Payload c = commands.poll();
                if (c == null) break;
                batch.add(c);
            }

            if (batch.size() < TaraConfig.BATCH_SIZE && !forceBatch) {
                Thread.sleep(2);
                continue;
            }

            if (batch.isEmpty()) {
                batchTimeout = System.currentTimeMillis() + TaraConfig.BATCH_TIMEOUT;
                if (forceProgress) {
                    synchronized (ctx.getCheckpointLock()) { emitProgressLocked(ctx); }
                }
                continue;
            }

            synchronized (ctx.getCheckpointLock()) {
                Request r = new Request(partition, next, batch);
                batch = new LinkedList<>();
                next++;
                nextActual = next;
                batchTimeout = System.currentTimeMillis() + TaraConfig.BATCH_TIMEOUT;

                RequestTuple rt = new RequestTuple(id, r);
                rt.partition = this.partition;
                // route to the correct Proposer subtask
                rt.targetKey = TaraConfig.keyForSubtask(
                        this.partition % TaraConfig.PROPOSERS, TaraConfig.PROPOSERS);
                ctx.collect(rt);

                emitProgressLocked(ctx);
            }
        }
    }

    private void emitProgressLocked(SourceContext<BaseTuple> ctx) {
        int[] progress = new int[TaraConfig.REQUEST_SOURCES];
        progress[id.id] = nextActual;
        ProgressTuple pt = new ProgressTuple(id, progress);
        pt.partition = this.partition;
        // Route ProgressTuple to the correct Controller subtask
        pt.targetKey = TaraConfig.keyForSubtask(
                this.partition % TaraConfig.CONTROLLERS, TaraConfig.CONTROLLERS);
        ctx.collect(pt);
        progressTimer = System.currentTimeMillis() + TaraConfig.PROGRESS_INTERVAL;
        TaraLog.info(this, "Progress emitted nextActual=" + nextActual);
    }

    @Override
    public void cancel() {
        running = false;
        closeSocket();
        if (restServer != null) restServer.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        running = false;
        closeSocket();
        if (restServer != null) restServer.shutdownNow();
        super.close();
    }

    private void closeSocket() {
        try {
            if (clientServerSocket != null && !clientServerSocket.isClosed())
                clientServerSocket.close();
        } catch (IOException e) {
            TaraLog.warning(this, "Error closing socket: " + e.getMessage());
        }
    }

    private void startClientSocketServer(int subtask) {
        Thread t = new Thread(() -> {
            int port = TaraConfig.REQUEST_SOURCE_CLIENT_PORT + subtask;
            try (ServerSocket ss = new ServerSocket(port)) {
                clientServerSocket = ss;
                while (running) {
                    try {
                        Socket s = ss.accept();
                        s.setTcpNoDelay(true);
                        Thread ct = new Thread(() -> clientLoop(s));
                        ct.setDaemon(true);
                        ct.start();
                    } catch (IOException e) {
                        if (running) TaraLog.error(this, "Accept: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                if (running) TaraLog.error(this, "Socket: " + e.getMessage());
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private void clientLoop(Socket s) {
        try (Socket socket = s;
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream  dis = new DataInputStream(socket.getInputStream())) {

            TaraLog.info(this, "Client connected from " + s.getRemoteSocketAddress());
            while (running) {
                Payload c = Payload.readDataStream(dis);
                if (c == null) {
                    TaraLog.warning(this, "readDataStream returned null — breaking");
                    break;
                }
                commands.add(c);
                clients.put(c.cid, dos);
                TaraLog.info(this, "Queued cid=" + c.cid + " commands.size=" + commands.size());
            }
        } catch (IOException e) {
            TaraLog.warning(this, "clientLoop IOException: " + e.getMessage());
        }
    }

    private void startResultWebService() throws Exception {
        // use this subtask's port, not always subtask-0's port
        int port = TaraConfig.RESULT_SINK_BASE_PORT + this.subtask;
        URI uri  = new URI("http://0.0.0.0:" + port + "/");

        final FlinkRequestSource self = this;
        ResourceConfig config = new ResourceConfig();
        config.register(ResultQueue.class);
        config.register(JacksonFeature.class);
        config.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(self).to(FlinkRequestSource.class);
            }
        });

        restServer = GrizzlyHttpServerFactory.createHttpServer(uri, config, false);
        restServer.start();
        TaraLog.info(this, "Result REST started port=" + port);
    }

    public void sendResult(List<Payload> res) {
        for (Payload p : res) {
            DataOutputStream dos = clients.get(p.cid);
            if (dos == null) continue;

            boolean[] send = { false };
            resultFilter.compute(p.cid, (cid, last) -> {
                if (last == null || p.xnr > last) { send[0] = true; return p.xnr; }
                return last;
            });
            if (!send[0]) continue;

            try {
                synchronized (dos) {
                    Payload.writeDataStream(dos, p);
                    dos.flush();
                }
            } catch (IOException ignored) { }
        }
    }

    @Singleton
    @Path("/resultQueue")
    public static class ResultQueue {

        private final FlinkRequestSource source;

        @Inject
        public ResultQueue(FlinkRequestSource source) {
            this.source = source;
        }

        @POST
        @Consumes("application/json")
        public void receiveReply(List<Payload> res) {
            source.sendResult(res);
        }
    }
}