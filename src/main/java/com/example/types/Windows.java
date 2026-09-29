package com.example.types;

import com.example.tara.TaraConfig;

import java.util.HashMap;

public class Windows<T> extends HashMap<Integer, Window<T>> {
	private static final long serialVersionUID = -1164539610253283454L;

	public Windows(int id) {
		for (int i=0; i<id; i++) {
			put(i, new Window<T>(0, TaraConfig.WINDOW_SIZE));
		}
	}
}