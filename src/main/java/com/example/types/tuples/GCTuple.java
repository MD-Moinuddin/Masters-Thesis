package com.example.types.tuples;

import com.example.tara.TaraConfig;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

public class GCTuple extends BaseTuple {

	public int cnr;
	public Set<Integer> executors;
	
	public GCTuple() {}   // required by Jackson for JSON deserialization at gcSource
	public GCTuple(NodeId creator, int cnr, Set<Integer> es) {
		super(creator);
		this.cnr = cnr;
		this.executors = es;
	}
	
	public int snr() {
		return (cnr * TaraConfig.CHECKPOINT_INTERVAL) / TaraConfig.PARTITIONS;
	}
	
	@Override
	public String toString() {
		return "(GCTuple: " + cnr + ")";
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeInt(cnr);
		if (executors == null) {
			out.writeInt(0);
		} else {
			out.writeInt(executors.size());
			for (Integer i : executors) {
				out.writeInt(i);
			}
		}
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		this.cnr = in.readInt();
		int esize = in.readInt();
		if (esize != 0) this.executors = new HashSet<Integer>();
		for (int i=0; i < esize; i++) {
			this.executors.add(in.readInt());
		}
	}
	
}
