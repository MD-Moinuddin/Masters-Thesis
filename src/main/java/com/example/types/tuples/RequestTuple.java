package com.example.types.tuples;

import com.example.types.Request;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class RequestTuple extends BaseTuple {

	public Request request;
	
	public RequestTuple(NodeId source, Request r) {
		super(source);
		this.request = r;
        this.type = TupleType.REQUEST;
	}
	
	@Override
	public String toString() {
		return "(RequestTuple: "  + request + ")";
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(request);
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		this.request = (Request) in.readObject();
	}
}
