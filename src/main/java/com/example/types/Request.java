package com.example.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

public class Request implements Externalizable {
	private static final long serialVersionUID = 8224585858299882832L;

	public List<Payload> cmd; // Command
	public boolean noop;
	public int source;
	public int rnr;


	public Request() {}
	public Request(int source, int rnr, List<Payload> command) {
		this.noop = false;
		this.cmd = command;
		this.source = source;
		this.rnr = rnr;
	}
	
	public Request(int source, int rnr) {
		this.noop = true;
		this.source = source;
		this.rnr = rnr;
		
	}

	@Override
	public String toString() {
		return "(Request " + source + "|" + rnr + ": " + (noop ? "NOOP" : cmd) + ")";
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(rnr);
		out.writeBoolean(noop);
		if (noop) return;
		if (cmd == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(cmd.size());
			for (Payload p : cmd) {
				out.writeObject(p);
			}
		}
	}
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.source = in.readInt();
		this.rnr = in.readInt();
		this.noop = in.readBoolean();
		if (this.noop) return;
		
		int size = in.readInt();
		if (size >= 0) this.cmd = new LinkedList<Payload>();
		for (int i=0; i < size; i++) {
			this.cmd.add((Payload) in.readObject());
		}

	}
}
