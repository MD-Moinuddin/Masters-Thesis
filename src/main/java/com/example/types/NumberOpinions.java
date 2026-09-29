package com.example.types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NumberOpinions  implements Serializable {
	private static final long serialVersionUID = -9213494546704689401L;

	private Integer[] opinions;
	
	public NumberOpinions(int maxId) {
		this.opinions = new Integer[maxId];
		for (int i=0; i<maxId; i++) {
			this.opinions[i] = 0;
		}
	}
	
	public void put(int id, int n) {
		this.opinions[id] = n;
	}
	
	public Integer get(int id) {
		return this.opinions[id];
	}
	
	public int highest(int threshold) {
		// Sort a copy in descending order: sorting the backing array would
		// scramble which node each stored opinion belongs to.
		List<Integer> ranking = new ArrayList<>(Arrays.asList(opinions));
		Collections.sort(ranking, Collections.reverseOrder());
		
		return ranking.get(threshold-1);
	}
}
