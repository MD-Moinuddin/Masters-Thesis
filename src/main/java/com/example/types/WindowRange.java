package com.example.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class WindowRange<T> implements Externalizable {
	private static final long serialVersionUID = 6655498100371471027L;

	private int capacity;
	public int min;
	public int max;
	public int pos;
	
	public WindowRange() {}
	public WindowRange(int min, int max) {
		this.capacity = max-min;
		this.min = min;
		this.max = max;
		this.pos = min;
	}
	
	// Needed for JAX-RS JSON serialization
	public int getCapacity() { return this.capacity; }
	public void setCapacity(int c) { this.capacity = c; }
	
	public boolean put(int index, T value) {
		if (pos == max) return false;
		if (pos != index) return false;
		
		pos = index+1;
		return true;
	}
	
	public boolean get(int index) {
		if (index <  min) return false;
		return !(index >= pos);
	}
	
	public boolean append(T e) {
		return put(pos, e);
	}
	
	public void move(int min) {
		// Update state
		this.min = min;
		this.max = min + this.capacity;
		this.pos = Math.max(this.pos, min);
	}
	
	public void clear(int from) {
		this.pos = from;
	}
	
	public boolean appendable(int index) {
		if (pos == max) return false;
		return (pos == index);
	}
	
	@Override
	public String toString() {
		return "(Window: " + min + "-" + max + " " + pos + ")";
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(capacity);
		out.writeInt(min);
		out.writeInt(max);
		out.writeInt(pos);
	}
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.capacity = in.readInt();
		this.min = in.readInt();
		this.max = in.readInt();
		this.pos = in.readInt();
	}
}
