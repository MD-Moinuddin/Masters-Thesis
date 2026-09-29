package com.example.tara;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.example.types.NumberOpinions;
import org.junit.Test;

public class ObserverTest {

    @Test
    public void highestDoesNotReorderStoredOpinions() {
        NumberOpinions no = new NumberOpinions(3);
        no.put(0, 1);
        no.put(1, 5);
        no.put(2, 3);
        assertEquals(3, no.highest(2));
        assertEquals(Integer.valueOf(1), no.get(0));
        assertEquals(Integer.valueOf(5), no.get(1));
        assertEquals(Integer.valueOf(3), no.get(2));
    }

    @Test
    public void singleNodeCannotFormAQuorumAlone() {
        // F = 1, so the current value needs F + 1 = 2 distinct nodes to advance.
        Observer o = new Observer(3);
        assertFalse(o.update(0, 2));
        assertTrue(o.update(1, 3));
        assertEquals(2, o.current);

        // Node 1 racing ahead must not move the quorum value beyond what node 0 supports.
        o.update(1, 4);
        o.update(1, 9);
        o.update(1, 12);
        assertEquals(2, o.current);
    }

    @Test
    public void quorumAdvancesWhenSecondNodeCatchesUp() {
        Observer o = new Observer(3);
        o.update(0, 5);
        o.update(1, 7);
        assertEquals(5, o.current);
        assertTrue(o.update(0, 8));
        assertEquals(7, o.current);
    }
}
