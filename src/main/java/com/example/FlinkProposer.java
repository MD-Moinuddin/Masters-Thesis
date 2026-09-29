package com.example;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import com.example.tara.Observer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.*;
import com.example.types.tuples.*;
import com.example.types.tuples.BaseTuple.NodeId;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

/**
 * Tara <b>Proposer</b>. Only the current leader (NORMAL mode) orders client requests: it assigns
 * consecutive sequence numbers (snr) and sends a {@link com.example.types.tuples.ConsensusTuple}
 * to every committer. Non-leaders stay IDLE but keep their request windows in sync.
 *
 * <p>On a view change the new leader enters VIEW_CHANGE, collects records from F+1 committers,
 * re-proposes the highest-view request for every open slot (or a NO-OP if none), and only then
 * returns to NORMAL. Leader failure is simulated through the REST control endpoint
 * ({@code /control/kill}, {@code /control/revive}, {@code /control/status}) on port 12000+subtask.
 *
 * <p>Flink-specific: the operator is keyed by {@code targetKey}, which
 * {@link com.example.tara.TaraConfig#keyForSubtask} maps to exactly one subtask, so instance
 * fields (not Flink managed state) hold the protocol state.
 */
public class FlinkProposer extends KeyedProcessFunction<Integer, BaseTuple, ConsensusTuple> {

    private static final long serialVersionUID = 1L;

    private int partition;
    private int next;
    private Windows<Request> requests;
    private WindowOpinions<Record> records;
    private Set<Integer> recorded;
    private Observer gc;
    private Observer view;
    volatile Mode mode;
    private int proposerId;

    volatile boolean killed = false;
    private transient HttpServer controlServer;

    public FlinkProposer() { }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        int subtask    = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.proposerId = subtask;
        this.partition = TaraConfig.getPartition(subtask);
        this.next      = 0;
        this.requests  = new Windows<>(TaraConfig.REQUEST_SOURCES);
        this.records   = new WindowOpinions<>(TaraConfig.COMMITTERS, 0, TaraConfig.WINDOW_SIZE);
        this.recorded  = new HashSet<>();
        this.gc        = new Observer(TaraConfig.GC_SOURCES);
        this.view      = new Observer(TaraConfig.VIEW_SOURCES);
        this.mode      = (subtask == TaraConfig.getCurrentProposer(0, this.partition))
                ? Mode.NORMAL : Mode.IDLE;

        startControlServer(subtask);
        TaraLog.info(this, "FlinkProposer open subtask=" + subtask + " mode=" + mode);
    }

    //Control REST server

    private void startControlServer(int subtask) {
        int port = 12000 + subtask;   // subtask 0 → 12000, subtask 1 → 12001
        try {
            // unauthenticated kill/revive endpoint: loopback only unless TARA_CONTROL_HOST is set
            String bindHost = System.getenv().getOrDefault("TARA_CONTROL_HOST", "127.0.0.1");
            URI uri = new URI("http://" + bindHost + ":" + port + "/");
            final FlinkProposer self = this;
            ResourceConfig config = new ResourceConfig();
            config.register(JacksonFeature.class);
            config.register(ProposerControl.class);
            config.register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(self).to(FlinkProposer.class);
                }
            });
            controlServer = GrizzlyHttpServerFactory.createHttpServer(uri, config, false);
            controlServer.start();
            TaraLog.info(this, "Control server started port=" + port);
        } catch (Exception e) {
            TaraLog.warning(this, "Control server failed to start: " + e.getMessage());
        }
    }

    @Singleton
    @Path("/control")
    public static class ProposerControl {

        private final FlinkProposer proposer;

        @Inject
        public ProposerControl(FlinkProposer proposer) {
            this.proposer = proposer;
        }

        //Kill the leader: immediately stops proposing and enters IDLE.
        @POST
        @Path("/kill")
        public Response kill() {
            proposer.killed = true;
            proposer.mode   = Mode.IDLE;
            TaraLog.info(proposer, "KILLED via REST — mode=IDLE");
            return Response.ok("killed").build();
        }

        @POST
        @Path("/revive")
        public Response revive() {
            proposer.killed = false;
            TaraLog.info(proposer, "REVIVED via REST");
            return Response.ok("revived").build();
        }

        @GET
        @Path("/status")
        @Produces("text/plain")
        public String status() {
            int subtask = proposer.getRuntimeContext()
                    .getTaskInfo().getIndexOfThisSubtask();
            return "subtask=" + subtask
                    + " mode=" + proposer.mode
                    + " killed=" + proposer.killed
                    + " view=" + proposer.view.current
                    + " next=" + proposer.next;
        }
    }


    @Override
    public void processElement(BaseTuple value, Context ctx,
                               Collector<ConsensusTuple> out) throws Exception {
        if (value instanceof ViewTuple)         processView((ViewTuple) value, out);
        else if (value instanceof RequestTuple) processRequest((RequestTuple) value, out);
        else if (value instanceof GCTuple)      processGC((GCTuple) value, out);
        else if (value instanceof RecordTuple)  processRecord((RecordTuple) value, out);
        else TaraLog.warning(this, "Unexpected: " + value.getClass().getSimpleName());
    }

    private void processRequest(RequestTuple rt, Collector<ConsensusTuple> out) {
        if (rt.creator == null) return;
        Window<Request> win = requests.get(rt.creator.id);
        if (!win.appendable(rt.request.rnr)) {
            if (mode == Mode.IDLE && win.pos() < rt.request.rnr) {
                win.move(rt.request.rnr);
                win.append(rt.request);
            }
            return;
        }
        win.append(rt.request);
        if (mode != Mode.NORMAL) {
            win.move(rt.request.rnr - TaraConfig.CHECKPOINT_INTERVAL);
            return;
        }
        propose(out);
    }

    private void propose(Collector<ConsensusTuple> out) {
        if (mode != Mode.NORMAL) return;
        if (killed) return;

        if (next >= gc.current + TaraConfig.WINDOW_SIZE) {
            TaraLog.info(this, "Window full, suspended snr=" + next);
            return;
        }

        NodeId proposerNode = new NodeId(NodeType.PROPOSER, this.proposerId);

        while (next < gc.current + TaraConfig.WINDOW_SIZE) {
            Request r = null;
            for (int i = 0; i < TaraConfig.REQUEST_SOURCES; i++) {
                Window<Request> win = requests.get(i);
                if (win.min() == win.pos()) continue;
                r = win.get(win.min());
                break;
            }
            if (r == null) break;

            ConsensusTuple ct = new ConsensusTuple(proposerNode, partition, next, view.current, r);
            for (int c = 0; c < TaraConfig.COMMITTERS; c++) {
                ConsensusTuple copy = (ConsensusTuple) ct.shallowCopy();
                copy.targetKey = TaraConfig.keyForSubtask(c, TaraConfig.COMMITTERS);
                out.collect(copy);
            }
            TaraLog.info(this, "Proposal snr=" + next + " view=" + view.current
                    + " → " + TaraConfig.COMMITTERS + " committers");
            next++;
            requests.get(r.source).move(r.rnr + 1);
        }
    }

    private void processGC(GCTuple gt, Collector<ConsensusTuple> out) {
        if (gt.creator == null) return;
        if (!gc.update(gt.creator.id, gt.snr())) return;
        records.move(gc.current);
        if (mode == Mode.NORMAL) propose(out);
    }

    private void processView(ViewTuple vt, Collector<ConsensusTuple> out) {
        if (vt.creator == null) return;
        if (!view.update(vt.creator.id, vt.view)) return;
        applyViewChange(view.current, out);
    }

    private void applyViewChange(int newView, Collector<ConsensusTuple> out) {
        next = gc.current;
        records.sync(gc.current);
        recorded.clear();

        int subtask  = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        boolean isLeader = (subtask == TaraConfig.getCurrentProposer(newView, partition));

        if (!isLeader) {
            mode = Mode.IDLE;
            TaraLog.info(this, "Idle view=" + newView);
            return;
        }

        killed = false;
        mode   = Mode.VIEW_CHANGE;
        TaraLog.info(this, "New leader view=" + newView + " entering VIEW_CHANGE");
    }

    private void processRecord(RecordTuple rt, Collector<ConsensusTuple> out) {
        if (mode != Mode.VIEW_CHANGE) return;
        if (rt.creator == null) return;
        if (recorded.contains(rt.creator.id)) return;

        if (rt.records != null) {
            for (int i = rt.records.min(); i < rt.records.pos(); i++) {
                records.put(rt.creator.id, i, rt.records.get(i));
            }
        }
        recorded.add(rt.creator.id);

        TaraLog.info(this, "RecordTuple from committer=" + rt.creator.id
                + " total=" + recorded.size() + "/" + (TaraConfig.F + 1));

        if (recorded.size() < TaraConfig.F + 1) return;

        NodeId proposerNode = new NodeId(NodeType.PROPOSER, this.proposerId);
        records.move(gc.current);


        int recoverUpTo = gc.current;
        for (int j = 0; j < TaraConfig.COMMITTERS; j++) {
            recoverUpTo = Math.max(recoverUpTo, records.get(j).pos());
        }

        for (int i = gc.current; i < recoverUpTo; i++) {
            Record best = new Record(-1, null);
            for (int j = 0; j < TaraConfig.COMMITTERS; j++) {
                if (i >= records.get(j).pos()) continue;
                Record candidate = records.get(j, i);
                if (candidate != null && candidate.view > best.view) best = candidate;
            }

            ConsensusTuple ct;
            if (best.view == -1 || best.request == null) {
                Request noop = new Request(-1, -1, null);
                ct = new ConsensusTuple(proposerNode, partition, next, view.current, noop);
                TaraLog.info(this, "Gap-fill NO-OP snr=" + next);
            } else {
                requests.get(best.request.source).move(best.request.rnr + 1);
                ct = new ConsensusTuple(proposerNode, partition, next, view.current, best.request);
                TaraLog.info(this, "Replay snr=" + next + " view=" + best.view);
            }
            // Broadcast replay tuples to all committers too (keyForSubtask: raw indices collide)
            for (int c = 0; c < TaraConfig.COMMITTERS; c++) {
                ConsensusTuple copy = (ConsensusTuple) ct.shallowCopy();
                copy.targetKey = TaraConfig.keyForSubtask(c, TaraConfig.COMMITTERS);
                out.collect(copy);
            }
            next++;
        }

        mode   = Mode.NORMAL;
        killed = false;
        TaraLog.info(this, "Leader active view=" + view.current);
        propose(out);
    }

    @Override
    public void close() throws Exception {
        if (controlServer != null) controlServer.shutdownNow();
        super.close();
    }
}