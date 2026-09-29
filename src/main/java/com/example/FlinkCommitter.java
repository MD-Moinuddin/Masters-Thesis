package com.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.example.tara.Observer;
import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.*;
import com.example.types.tuples.*;
import com.example.types.tuples.BaseTuple.NodeId;

/**
 * Tara <b>Committer</b>. Accepts a proposal only if it belongs to the committer's current view and
 * is the next expected slot, remembers it as a {@link com.example.types.Record}, and broadcasts a
 * commit to all executors. On a view change it emits a {@link com.example.types.tuples.RecordTuple}
 * with everything it has accepted so the new leader can recover uncommitted slots.
 */
public class FlinkCommitter extends KeyedProcessFunction<Integer, BaseTuple, BaseTuple> {

    private int partition;
    private Window<Request> commits;
    private RecordWindow records;
    private Observer gc;
    private Observer view;
    private int committerId;

    public FlinkCommitter() { }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.committerId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.partition   = TaraConfig.getPartition(this.committerId);
        this.commits     = new Window<>(0, TaraConfig.WINDOW_SIZE);
        this.records     = new RecordWindow(0, TaraConfig.WINDOW_SIZE);
        this.gc          = new Observer(TaraConfig.GC_SOURCES);
        this.view        = new Observer(TaraConfig.VIEW_SOURCES);
        TaraLog.info(this, "FlinkCommitter open committerId=" + committerId);
    }

    @Override
    public void processElement(BaseTuple value, Context ctx,
                               Collector<BaseTuple> out) throws Exception {
        if (value instanceof ConsensusTuple) processProposal((ConsensusTuple) value, out);
        else if (value instanceof GCTuple)   processGC((GCTuple) value);
        else if (value instanceof ViewTuple) processView((ViewTuple) value, out);
        else TaraLog.warning(this, "Unexpected: " + value.getClass().getSimpleName());
    }

    private void processProposal(ConsensusTuple ct, Collector<BaseTuple> out) {
        if (ct.view != view.current) return;
        if (!commits.appendable(ct.snr)) return;
        commits.append(ct.request);

        Record existing = records.get(ct.snr);
        if (existing == null || ct.view >= existing.view) {
            records.put(ct.snr, new Record(ct.view, ct.request));
        }

        NodeId committerNode = new NodeId(NodeType.COMMITTER, this.committerId);
        ConsensusTuple commit = new ConsensusTuple(
                committerNode, partition, ct.snr, ct.view, ct.request);
        commit.type = TupleType.CONSENSUS;
        TaraLog.info(this, "Commit snr=" + ct.snr + " view=" + ct.view
                + " → broadcasting to " + TaraConfig.EXECUTORS + " executors");
        for (int e = 0; e < TaraConfig.EXECUTORS; e++) {
            ConsensusTuple copy = (ConsensusTuple) commit.shallowCopy();
            copy.targetKey = TaraConfig.keyForSubtask(e, TaraConfig.EXECUTORS);
            out.collect(copy);
        }
    }

    private void processGC(GCTuple gt) {
        if (!gc.update(gt.creator.id, gt.snr())) return;
        commits.move(gc.current);
        records.move(gc.current);
    }

    private void processView(ViewTuple vt, Collector<BaseTuple> out) {
        if (vt.creator == null) return;

        if (!view.update(vt.creator.id, vt.view)) return;
        applyViewChange(vt.view, out);

    }

    private void applyViewChange(int newView, Collector<BaseTuple> out) {
        NodeId committerNode = new NodeId(NodeType.COMMITTER, this.committerId);
        RecordTuple record   = new RecordTuple(committerNode, partition, newView, records);
        record.type          = TupleType.RECORD;
        TaraLog.info(this, "RecordTuple for newView=" + newView);
        out.collect(record);
    }
}