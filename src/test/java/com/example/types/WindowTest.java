package com.example.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class WindowTest {

    @Test
    public void appendsOnlyInOrder() {
        Window<String> w = new Window<>(0, 10);
        assertTrue(w.appendable(0));
        assertFalse(w.appendable(1));
        w.append("a");
        w.append("b");
        assertEquals(2, w.pos());
        assertEquals("b", w.get(1));
        assertNull(w.get(2));
    }

    @Test
    public void refusesAppendsWhenFull() {
        Window<Integer> w = new Window<>(0, 3);
        for (int i = 0; i < 3; i++) w.append(i);
        assertFalse(w.appendable(3));
    }

    @Test
    public void moveDiscardsOldEntriesAndKeepsNewOnes() {
        Window<String> w = new Window<>(0, 10);
        for (String s : new String[] {"a", "b", "c", "d"}) w.append(s);
        w.move(2);
        assertEquals(2, w.min());
        assertNull(w.get(1));
        assertEquals("c", w.get(2));
        assertEquals("d", w.get(3));
        assertEquals(4, w.pos());
    }

    @Test
    public void moveNeverGoesBackwards() {
        Window<String> w = new Window<>(0, 10);
        w.append("a");
        w.move(1);
        w.move(0);
        assertEquals(1, w.min());
    }

    @Test
    public void windowOpinionsCountsNodesThatHaveAnEntry() {
        WindowOpinions<String> wo = new WindowOpinions<>(3, 0, 10);
        wo.get(0).append("x");
        wo.get(2).append("x");
        assertEquals(2, wo.available(0));
        assertEquals(0, wo.available(1));
    }
}
