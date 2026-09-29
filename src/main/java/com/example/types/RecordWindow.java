package com.example.types;

import com.example.tara.TaraConfig;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class RecordWindow implements Externalizable {

	private WindowRange<Record> range;
	private Record[] values;

	public RecordWindow() {}
	public RecordWindow(int min, int max) {
		this.range = new WindowRange<Record>(min, max);
		this.values = new Record[TaraConfig.WINDOW_SIZE];
	}

	// Needed for JAX-RS JSON serialization
	public WindowRange<Record> getRange() { return this.range; }
	public void setRange(WindowRange<Record> range) { this.range = range; }
	public Record[] getValues() { return this.values; }
	public void setValues (Record[] values) { this.values = values; }
	
	// Auxiliary Attributes
	public int pos() { return range.pos; }
	public int min() { return range.min; }
	public int max() { return range.max; }
	
	public void put(int index, Record value) {
		if(range.put(index, value)) values[index - range.min] = value;
	}
	
	public void append(Record value) {
		put(range.pos, value);
	}

	public Record get(int index) {
		return range.get(index) ? (Record) values[index - range.min] : null;
	}

	public void fill(int to) {
		if (to < range.pos) return;
		if (to > range.max) return;
		this.range.pos = to + 1;
	}

	public void move(int min) {
		// Only move forward
		if (min < range.min) return;
		
		// New State
		int size = range.pos - min;
		int diff = min - range.min;
		for (int i = 0; i < size; i++) {
			values[i] = values[i+diff];
		}
		// Move window
		range.move(min);
	}
	
	public void sync(Window<Record> window) {
		move(window.min());
		clear(window.pos());
	}
	
	public void clear(int from) {
		if (from <= range.min) {
			// Clear whole window
			range.clear(range.min);
		} else {
			// Clear all elements from start
			int start = from - range.min;
			range.clear(start);
		}
	}
	
	public void reset() {
		clear(range.min);
	}

	public boolean appendable(int index) {
		return range.appendable(index);
	}
	
	@Override
	public String toString() {
		return range.toString() + " Values: " + values.length;
	}
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(range);
		int size = range.pos - range.min;
		out.writeInt(size);
		for (int i=0; i<size; i++) {
			out.writeObject(values[i]);
		}
	}
	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.range = (WindowRange<Record>) in.readObject();
		int size = in.readInt();
		this.values = new Record[TaraConfig.WINDOW_SIZE];
		for (int i=0; i < size; i++) {
			this.values[i] = (Record) in.readObject();
		}
	}
}
