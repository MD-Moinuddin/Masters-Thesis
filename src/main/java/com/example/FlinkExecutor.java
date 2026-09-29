package com.example;

import java.io.*;
import java.util.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.tara.Observer;
import com.example.types.*;
import com.example.types.tuples.*;

/**
 * Tara <b>Executor</b>. Executes a request once F+1 committers have committed the same slot,
 * strictly in slot order, and deduplicates per client via {@code filter} (last executed xnr) and
 * {@code results} (cached reply). Every {@code CHECKPOINT_INTERVAL} slots it writes a local
 * snapshot and emits a {@link com.example.types.tuples.GCTuple}; a quorum of GC tuples lets all
 * roles discard older state. It also emits periodic progress tuples for the controllers.
 */
public class FlinkExecutor extends KeyedProcessFunction<Integer, BaseTuple, BaseTuple> {

    private Application app;
    private int next;
    private ExecutorPartition[] partitions;
    private Map<Integer, Integer> filter;
    private Map<Integer, Payload> results;
    private int checkpoint;
    private Observer gc;
    private String snapshotDir;
    private BaseTuple.NodeId self;
    private long progressTimer;

    public FlinkExecutor(Application app) {
        this.app = app;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        int subtask        = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.app.init();
        this.next          = 0;
        this.checkpoint    = 0;
        this.gc            = new Observer(TaraConfig.GC_SOURCES);
        this.filter        = new HashMap<>();
        this.results       = new HashMap<>();
        this.self          = new BaseTuple.NodeId(NodeType.EXECUTOR, subtask);
        this.snapshotDir   = TaraConfig.SNAPSHOT_DIR + "/executor-" + subtask;
        this.progressTimer = System.currentTimeMillis() + TaraConfig.PROGRESS_INTERVAL;
        new File(snapshotDir).mkdirs();
        this.partitions    = new ExecutorPartition[TaraConfig.PARTITIONS];
        for (int i = 0; i < TaraConfig.PARTITIONS; i++)
            this.partitions[i] = new ExecutorPartition();
        TaraLog.info(this, "FlinkExecutor open subtask=" + subtask);
    }

    @Override
    public void processElement(BaseTuple value, Context ctx,
                               Collector<BaseTuple> out) throws Exception {
        if (value instanceof ConsensusTuple)      processConsensus((ConsensusTuple) value, ctx, out);
        else if (value instanceof GCTuple)        processGC((GCTuple) value, out);
        else if (value instanceof ViewTuple)      processView((ViewTuple) value);
        else TaraLog.warning(this, "Unexpected: " + value.getClass().getSimpleName());
    }

    private void processConsensus(ConsensusTuple ct, Context ctx,
                                  Collector<BaseTuple> out) {
        if (ct.partition < 0 || ct.partition >= TaraConfig.PARTITIONS) return;
        if (!partitions[ct.partition].command(ct)) return;

        while (true) {
            ExecutorPartition p = partitions[next % TaraConfig.PARTITIONS];
            ExecutorPartition.PayloadList cmds = p.commands.get(next / TaraConfig.PARTITIONS);
            if (cmds == null) break;

            if (!cmds.noop) {
                for (Payload x : cmds.cmds) {
                    int lastXnr = filter.getOrDefault(x.cid, -1);
                    if (lastXnr < x.xnr) {
                        byte[] a = app.execute(x);
                        Payload r = new Payload(x.cid, x.xnr, a);
                        results.put(x.cid, r);
                        filter.put(x.cid, x.xnr);
                        ReplyTuple reply = new ReplyTuple(self, ct.partition, r);
                        reply.type = TupleType.REPLY;
                        // Route the reply back to the RequestSource that owns the client socket
                        reply.targetKey = TaraConfig.keyForSubtask(
                                cmds.source % TaraConfig.REQUEST_SOURCES, TaraConfig.REQUEST_SOURCES);
                        out.collect(reply);
                    } else {
                        Payload cached = results.get(x.cid);
                        if (cached != null) {
                            ReplyTuple reply = new ReplyTuple(self, ct.partition, cached);
                            reply.type = TupleType.REPLY;
                            reply.targetKey = TaraConfig.keyForSubtask(
                                    cmds.source % TaraConfig.REQUEST_SOURCES, TaraConfig.REQUEST_SOURCES);
                            out.collect(reply);
                        }
                    }
                }
            }
            next++;

            int c = next / TaraConfig.CHECKPOINT_INTERVAL;
            if (checkpoint < c) {
                Snapshot snap = new Snapshot(app.getState(),
                        new HashMap<>(filter), new HashMap<>(results));
                storeSnapshot(snap, c);
                checkpoint = c;

                Set<Integer> executorSet = new HashSet<>();
                executorSet.add(self.id);
                GCTuple gct = new GCTuple(self, c, executorSet);
                gct.type      = TupleType.GC;
                gct.partition = ct.partition;
                out.collect(gct);
                TaraLog.info(this, "Checkpoint c=" + c);
            }
        }

        long now = System.currentTimeMillis();
        if (now >= progressTimer) {
            emitProgress(ct.partition, out);
            progressTimer = now + TaraConfig.PROGRESS_INTERVAL;
        }
    }

    private void emitProgress(int partition, Collector<BaseTuple> out) {
        int[] progress = new int[TaraConfig.REQUEST_SOURCES];
        for (int i = 0; i < TaraConfig.PARTITIONS; i++) {
            int[] pa = partitions[i].progressActual;
            for (int s = 0; s < TaraConfig.REQUEST_SOURCES; s++)
                progress[s] = Math.max(progress[s], pa[s]);
        }
        ProgressTuple pt = new ProgressTuple(self, progress);
        pt.partition = partition;
        out.collect(pt);
    }

    private void processGC(GCTuple gt, Collector<BaseTuple> out) {
        if (gt.creator == null) return;
        if (!gc.update(gt.creator.id, gt.cnr * TaraConfig.CHECKPOINT_INTERVAL)) return;
        int cnr = gc.current / TaraConfig.CHECKPOINT_INTERVAL;
        if (next < gc.current) {
            Snapshot snap = loadSnapshot(cnr);
            if (snap == null) { TaraLog.warning(this, "No snap cnr=" + cnr); return; }
            app.applyState(snap.state);
            next       = cnr * TaraConfig.CHECKPOINT_INTERVAL;
            filter     = new HashMap<>(snap.filter);
            results    = new HashMap<>(snap.results);
            checkpoint = cnr;
            for (int i = 0; i < TaraConfig.PARTITIONS; i++)
                partitions[i].gc(next / TaraConfig.PARTITIONS);
        } else {
            for (int i = 0; i < TaraConfig.PARTITIONS; i++)
                partitions[i].gc(gc.current / TaraConfig.PARTITIONS);
        }
        deleteOldSnapshots(cnr);
    }

    private void processView(ViewTuple vt) {
        if (vt.partition < 0 || vt.partition >= TaraConfig.PARTITIONS) return;
        partitions[vt.partition].view(vt);
    }

    private void storeSnapshot(Snapshot s, int cnr) {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(snapshotDir + "/snap-" + cnr + ".bin"))) {
            oos.writeObject(s);
        } catch (IOException e) {
            TaraLog.warning(this, "Store snap failed: " + e.getMessage());
        }
    }

    private Snapshot loadSnapshot(int cnr) {
        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(snapshotDir + "/snap-" + cnr + ".bin"))) {
            return (Snapshot) ois.readObject();
        } catch (Exception e) { return null; }
    }

    private void deleteOldSnapshots(int cnr) {
        File[] files = new File(snapshotDir).listFiles();
        if (files == null) return;
        for (File f : files) {
            try {
                int c = Integer.parseInt(
                        f.getName().replace("snap-", "").replace(".bin", ""));
                if (c < cnr) f.delete();
            } catch (NumberFormatException ignored) { }
        }
    }

    private class ExecutorPartition {
        WindowOpinions<Request> commits;
        Window<PayloadList> commands;
        int[] progress;
        int[] progressActual;
        Observer view;

        ExecutorPartition() {
            this.commits        = new WindowOpinions<>(TaraConfig.COMMITTERS, 0, TaraConfig.WINDOW_SIZE);
            this.commands       = new Window<>(0, TaraConfig.WINDOW_SIZE);
            this.progress       = new int[TaraConfig.REQUEST_SOURCES];
            this.progressActual = new int[TaraConfig.REQUEST_SOURCES];
            this.view           = new Observer(TaraConfig.VIEW_SOURCES);
        }

        boolean command(ConsensusTuple ct) {
            if (ct.view != view.current) return false;
            if (!commits.get(ct.creator.id).appendable(ct.snr)) return false;
            commits.get(ct.creator.id).append(ct.request);
            if (commits.available(ct.snr) < TaraConfig.F + 1) return false;
            commits.fill(ct.snr);
            commands.put(ct.snr, ct.request.noop
                    ? new PayloadList(ct.request.source)
                    : new PayloadList(ct.request.cmd, ct.request.source));
            progress[ct.request.source] = ct.request.rnr + 1;
            if (!ct.request.noop) progressActual[ct.request.source] = ct.request.rnr + 1;
            return true;
        }

        void gc(int s) { commits.move(s); commands.move(s); }

        void view(ViewTuple vt) {
            if (!view.update(vt.creator.id, vt.view)) return;
            commits.sync(commands.pos());
        }

        class PayloadList {
            boolean noop;
            List<Payload> cmds;
            int source;   // originating RequestSource id — used to route replies back
            PayloadList(List<Payload> c, int source) { this.cmds = c; this.noop = false; this.source = source; }
            PayloadList(int source)                  { this.noop = true; this.cmds = new LinkedList<>(); this.source = source; }
        }
    }
}