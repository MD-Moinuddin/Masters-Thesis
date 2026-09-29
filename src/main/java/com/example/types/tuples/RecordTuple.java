package com.example.types.tuples;

import com.example.types.RecordWindow;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordTuple extends BaseTuple {

    public int view;
    public RecordWindow records;

    public RecordTuple() {
        super();
    }

    public RecordTuple(NodeId creator, int p, int v, RecordWindow rs) {
        super(creator);
        this.partition = p;
        this.view      = v;
        this.records   = rs;
    }

    @Override
    public String toString() {
        return "(RecordTuple " + partition + "|" + view + ": " + records + ")";
    }

    /*******************
     ** SERIALIZATION **
     *******************/
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(partition);
        out.writeInt(view);
        out.writeObject(records);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.partition = in.readInt();
        this.view      = in.readInt();
        this.records   = (RecordWindow) in.readObject();
    }
}