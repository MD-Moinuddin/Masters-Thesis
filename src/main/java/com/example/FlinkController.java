package com.example;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.tara.Observer;
import com.example.types.ProgressOpinions;
import com.example.types.tuples.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

/**
 * Tara <b>Controller</b> (failure detector). Tracks how far request sources have asked for
 * (target) versus how far F+1 executors have executed (actual). If progress is pending longer than
 * {@code CONTROLLER_TIMEOUT}, it emits a {@link com.example.types.tuples.ViewTuple} for the next
 * view and escalates again if no recovery is observed within the grace period.
 */
public class FlinkController extends KeyedProcessFunction<Integer, BaseTuple, BaseTuple> {

    private static final long serialVersionUID = -8444367742034280019L;

    private int partition;
    private Map<Integer, Long> reports;
    private ProgressOpinions ordered;
    private int[] target;
    private int[] actual;
    private long timeoutMs;
    private long deadlineMs;
    private Observer view;
    private int controllerId;
    private BaseTuple.NodeId id;
    private boolean viewChangePending;
    private int pendingView;
    private long viewChangeGraceUntil;   // earliest time we may (re-)trigger while pending
    private long nextTimer;
    private boolean firstTimerRegistered;

    public FlinkController() { }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        int subtask               = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.controllerId         = subtask;
        this.partition            = TaraConfig.getPartition(subtask);
        this.id                   = new BaseTuple.NodeId(NodeType.CONTROLLER, subtask);
        this.reports              = new HashMap<>();
        this.ordered              = new ProgressOpinions(TaraConfig.EXECUTORS, TaraConfig.REQUEST_SOURCES);
        this.target               = new int[TaraConfig.REQUEST_SOURCES];
        this.actual               = new int[TaraConfig.REQUEST_SOURCES];
        this.timeoutMs            = TaraConfig.CONTROLLER_TIMEOUT;
        this.deadlineMs           = Long.MAX_VALUE;
        this.view                 = new Observer(TaraConfig.CONTROLLERS);
        this.viewChangePending    = false;
        this.pendingView          = 0;
        this.viewChangeGraceUntil = 0L;
        this.nextTimer            = 0L;
        this.firstTimerRegistered = false;

        long now = System.currentTimeMillis();
        for (int i = partition; i < TaraConfig.REQUEST_SOURCES; i += TaraConfig.PARTITIONS)
            reports.put(i, now);

        recomputeDeadline();
        TaraLog.info(this, "FlinkController open controllerId=" + controllerId);
    }

    @Override
    public void processElement(BaseTuple tuple, Context ctx, Collector<BaseTuple> out)
            throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        if (!firstTimerRegistered) {
            nextTimer = now + TaraConfig.OBSERVER_POLL;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
            firstTimerRegistered = true;
        }
        if (tuple instanceof ProgressTuple) processProgress((ProgressTuple) tuple, now);
        else if (tuple instanceof ViewTuple)  processView((ViewTuple) tuple, now);
        else TaraLog.warning(this, "Unexpected: " + tuple.getClass().getSimpleName());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<BaseTuple> out) throws Exception {
        if (timestamp == nextTimer) {
            periodically(timestamp, out);
            nextTimer = timestamp + TaraConfig.OBSERVER_POLL;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }
    }

    private void processProgress(ProgressTuple pt, long now) {
        if (pt.creator.type == NodeType.REQUEST_SOURCE) {
            int src = pt.creator.id;
            if (src < 0 || src >= TaraConfig.REQUEST_SOURCES) return;
            if (pt.progress[src] <= target[src]) return;
            boolean wasCaughtUp = (actual[src] >= target[src]);
            target[src] = pt.progress[src];
            if (wasCaughtUp) reports.put(src, now);
            TaraLog.info(this, "TARGET[" + src + "]=" + target[src]);

        } else if (pt.creator.type == NodeType.EXECUTOR) {
            int exec = pt.creator.id;
            if (exec < 0 || exec >= TaraConfig.EXECUTORS) return;
            if (leq(pt.progress, ordered.get(exec))) return;
            ordered.put(exec, pt.progress);
            int[] p = ordered.highest(TaraConfig.F + 1);
            if (!leq(p, actual)) {
                actual    = p;
                timeoutMs = TaraConfig.CONTROLLER_TIMEOUT;
                if (viewChangePending) {
                    viewChangePending = false;
                    view.current      = Math.max(view.current, pendingView);
                    TaraLog.info(this, "Recovery observed — clearing pending view change");
                }
            }
            for (int i = partition; i < TaraConfig.REQUEST_SOURCES; i += TaraConfig.PARTITIONS)
                reports.put(i, now);
        }
        recomputeDeadline();
    }

    private void processView(ViewTuple vt, long now) {
        if (vt.creator == null) return;

        // Ignore our own ViewTuple echoed back to us.
        if (vt.creator.type == NodeType.CONTROLLER && vt.creator.id == this.controllerId) return;

        if (vt.view > view.current) {
            TaraLog.info(this, "Adopting newer view " + view.current + " → " + vt.view);
            view.current = vt.view;
            if (viewChangePending) {
                pendingView          = view.current;
                viewChangeGraceUntil = now + timeoutMs;
            }
        }
    }

    private void recomputeDeadline() {
        long time = Long.MAX_VALUE;
        for (int i = partition; i < TaraConfig.REQUEST_SOURCES; i += TaraConfig.PARTITIONS) {
            if (actual[i] < target[i]) {
                long rt = reports.getOrDefault(i, Long.MAX_VALUE);
                if (rt != Long.MAX_VALUE) time = Math.min(time, rt + timeoutMs);
            }
        }
        deadlineMs = time;
    }

    private void periodically(long now, Collector<BaseTuple> out) {
        if (viewChangePending) {
            if (now < viewChangeGraceUntil) return;
            TaraLog.info(this, "Grace expired without recovery — escalating view change");
        } else {
            if (now < deadlineMs) return;   // normal monitoring: leader still healthy
        }

        int newView = view.current + 1;
        TaraLog.info(this, "Progress timeout — view change → " + newView);

        ViewTuple vt = new ViewTuple(id, partition, newView);
        vt.partition  = this.partition;
        out.collect(vt);
        TaraLog.info(this, "ViewTuple emitted view=" + newView);

        pendingView          = newView;
        viewChangePending    = true;
        viewChangeGraceUntil = now + timeoutMs;
    }

    private boolean leq(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) if (a[i] > b[i]) return false;
        return true;
    }
}