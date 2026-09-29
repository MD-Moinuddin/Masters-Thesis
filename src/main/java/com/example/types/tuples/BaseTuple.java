package com.example.types.tuples;

import com.example.NodeType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

// REST endpoint can deserialize BaseTuple subclasses correctly.
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "_type", defaultImpl = BaseTuple.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ViewTuple.class,      name = "ViewTuple"),
        @JsonSubTypes.Type(value = ConsensusTuple.class, name = "ConsensusTuple"),
        @JsonSubTypes.Type(value = RecordTuple.class,    name = "RecordTuple"),
        @JsonSubTypes.Type(value = GCTuple.class,        name = "GCTuple"),
        @JsonSubTypes.Type(value = ReplyTuple.class,     name = "ReplyTuple"),
        @JsonSubTypes.Type(value = ProgressTuple.class,  name = "ProgressTuple"),
        @JsonSubTypes.Type(value = RequestTuple.class,   name = "RequestTuple"),
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseTuple implements Externalizable, Cloneable {

    public NodeId creator;
    public TupleType type;
    public int partition;
    /** keyBy key that routes a tuple to one specific operator subtask.
     *  Set by App.broadcastToSubtasks() for fan-out and by the sources/operators
     *  for point-to-point delivery (see TaraConfig.keyForSubtask). */
    public int targetKey = 0;

    /** Shallow copy — copies all fields; used by App.broadcastToSubtasks() replication. */
    public BaseTuple shallowCopy() {
        try {
            return (BaseTuple) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("BaseTuple must implement Cloneable", e);
        }
    }

    public BaseTuple() {}

    public BaseTuple(NodeId creator) {
        this.creator = creator;
    }

    /*******************
     ** SERIALIZATION **
     *******************/
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(creator);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.creator = (NodeId) in.readObject();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NodeId implements Externalizable {
        public NodeType type;
        public int id;

        // no-arg constructor: required by Jackson AND Externalizable
        public NodeId() {}

        public NodeId(NodeType type, int id) {
            this.type = type;
            this.id   = id;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type.ordinal());
            out.writeInt(id);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.type = NodeType.values()[in.readInt()];
            this.id   = in.readInt();
        }

        @Override
        public String toString() {
            return type + "#" + id;
        }
    }
}