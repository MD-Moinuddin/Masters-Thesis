package com.example.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Record implements Externalizable {

	public int view;
	public Request request;
	
	public Record() {}
	public Record(int view, Request request) {
		this.view = view;
		this.request = request;
	}
	
	@Override
	public String toString() {
		return "(Record: " + view + "|" + request + ")";
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(view);
		out.writeObject(request);
	}
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.view = in.readInt();
		this.request = (Request) in.readObject();
	}
}
