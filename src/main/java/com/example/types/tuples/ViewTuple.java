package com.example.types.tuples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewTuple extends BaseTuple {

    public int view;

    public ViewTuple() {
        super();
    }

    public ViewTuple(NodeId creator, int p, int v) {
        super(creator);
        this.partition = p;
        this.view      = v;
    }

    @Override
    public String toString() {
        return "(ViewTuple: " + partition + "|" + view + ")";
    }

    /*******************
     ** SERIALIZATION **
     *******************/
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(partition);
        out.writeInt(view);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.partition = in.readInt();
        this.view      = in.readInt();
    }
}