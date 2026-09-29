package com.example.types.tuples;

import com.example.types.Request;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ConsensusTuple extends BaseTuple {

	public int snr;
	public int view;
	public Request request;

	public ConsensusTuple(NodeId source, int p, int s, int v, Request r) {
		super(source);
		this.partition = p;
		this.snr = s;
		this.view = v;
		this.request = r;
	}
	
	@Override
	public String toString() {
		return "(ConsensusTuple " + partition + "|" + snr + "|" + view + ": " + request + ")";
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeInt(partition);
		out.writeInt(snr);
		out.writeInt(view);
		out.writeObject(request);
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		this.partition = in.readInt();
		this.snr = in.readInt();
		this.view = in.readInt();
		this.request = (Request) in.readObject();
	}
	
}
