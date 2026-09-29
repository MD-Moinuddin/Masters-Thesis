package com.example.types.tuples;

import com.example.types.Payload;

public class ReplyTuple extends BaseTuple {

    public Payload result;

    public ReplyTuple(NodeId creator) {
        super(null);
    }

    public ReplyTuple(NodeId creator, int partition, Payload result) {
        super(creator);
        this.partition = partition;
        this.result    = result;
        this.type      = TupleType.REPLY;
    }
}