package com.example.tara;

import com.example.types.NumberOpinions;

/**
 * Tracks a monotonically increasing value reported by several nodes and exposes the value that at
 * least F+1 distinct nodes have reached (used for views and GC points).
 */
public class Observer {
    public NumberOpinions thresholds;
    public int current;

    public Observer(int maxId) {
        this.thresholds = new NumberOpinions(maxId);
        this.current = 0;
    }

    public boolean update(int index, int threshold) {
        if (threshold <= this.current) return false;
        if (threshold <= thresholds.get(index)) return false;

        // Store input and update state
        this.thresholds.put(index, threshold);
        int old = current;
        this.current = thresholds.highest(TaraConfig.F + 1);
        return (current != old);
    }
}
