package com.example.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class Snapshot implements Externalizable {

	public byte[] state;
	public Map<Integer, Integer> filter;
	public Map<Integer, Payload> results;

	public Snapshot() {}
	public Snapshot(byte[] state, Map<Integer, Integer> filter, Map<Integer, Payload> results) {
		this.state = state;
		this.filter = filter;
		this.results = results;
	}

	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// state
		if (state == null) {
			out.writeInt(0);
		} else {
			out.writeInt(state.length);
			out.write(state);
		}
		// filter and results have the same size and keys!
		if (filter == null) {
			out.writeInt(0);
		} else {
			out.writeInt(filter.size());
			for (Integer i : filter.keySet()) {
				out.writeInt(i);
				out.writeInt(filter.get(i));
				out.writeObject(results.get(i));
			}
		}
	}


	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// state
		int size = in.readInt();
		if (size != 0) {
			this.state = new byte[size];
			in.readFully(this.state);
		}
		// filter & results
		size  = in.readInt();
		this.filter = new HashMap<>();
		this.results = new HashMap<>();
		if (size != 0) {
			for (int i=0; i < size; i++) {
				int key = in.readInt();
				this.filter.put(key, in.readInt());
				this.results.put(key, (Payload) in.readObject());
			}
		}
	}

}
