package com.example.types.tuples;

import com.example.tara.TaraConfig;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ProgressTuple extends BaseTuple {

	public int[] progress;
	
	public ProgressTuple(NodeId creator, int[] p) {
		super(creator);
		this.progress = p;
	}
	
	public ProgressTuple(NodeId source, int r) {
		super(source);
		this.progress = new int[TaraConfig.REQUEST_SOURCES];
		this.progress[source.id] = r;
	}
	
	@Override
	public String toString() {
		String s = "(ProgressTuple: ";
		for (int i : progress ) {
			s += i + ",";
		}
		s += ")";
		return s;
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		if (progress == null) {
			out.writeInt(0);
		} else {
			out.writeInt(progress.length);
			for (int i : progress) {
				out.writeInt(i);
			}
		}
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		int size = in.readInt();
		if (size != 0) this.progress = new int[size];
		for (int i=0; i < size; i++) {
			progress[i] = in.readInt();
		}
	}
	
}
