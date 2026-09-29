package com.example.types;

import java.io.Serializable;

public class WindowOpinions<T> implements Serializable {
	private static final long serialVersionUID = 8396380762538727112L;

	private Window<T>[] opinions;
	
	@SuppressWarnings("unchecked")
	public WindowOpinions(int maxId, int min, int max) {
		this.opinions = new Window[maxId];
		for (int i=0; i<maxId; i++) {
			this.opinions[i] = new Window<T>(min, max);
		}
	}
	
	public void put(int id, int index, T value) {
		this.opinions[id].put(index, value);
	}
	
	public Window<T> get(int id) {
		return opinions[id];
	}
	
	public T get(int a, int b) {
		return opinions[a].get(b);
	}
	
	public void move(int min) {
		for (Window<T> w : opinions) {
			w.move(min);
		}
	}
	
	public void sync(Window<T> window) {
		for (Window<T> w : opinions) {
			w.sync(window);
		}
	}
	
	public void sync(int min) {
		for (Window<T> w : opinions) {
			w.move(min);
			w.clear(min);
		}
	}
	
	public int available(int index) {
		int count = 0;
		for(Window<T> w : opinions) {
			if (index < w.pos()) count++;
		}
		return count;
	}
	
	public void clear(int from) {
		for (Window<T> w : opinions) {
			w.clear(from);
		}
	}
	
	public void fill(int to) {
		for (Window<T> w : opinions) {
			w.fill(to);
		}
	}

}
