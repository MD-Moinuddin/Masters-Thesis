package com.example.types;

import java.io.Serializable;

public class ProgressOpinions implements Serializable {
	private static final long serialVersionUID = -5630593554028564776L;

	private int[][] opinions;
	
	public ProgressOpinions(int outerId, int innerId) {
		this.opinions = new int[outerId][];
		for (int i=0; i<outerId; i++) {
			this.opinions[i] = new int[innerId];
		}
	}
	
	public void put(int id, int[] value) {
		this.opinions[id] = value;
	}
	
	public int[] get(int id) {
		return this.opinions[id];
	}
	
	public int[] highest(int threshold) {
		int[] result = new int[this.opinions[0].length];
		for (int id=0; id<this.opinions[0].length; id++) {
			NumberOpinions no = new NumberOpinions(this.opinions.length);
			for (int key=0; key<this.opinions.length; key++) {
				no.put(key, this.opinions[key][id]);
			}
			result[id] = no.highest(threshold);
		}
		return result;
	}
	
}
