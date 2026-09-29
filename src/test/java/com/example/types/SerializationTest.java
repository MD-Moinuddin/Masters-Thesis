package com.example.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SerializationTest {

    @Test
    public void payloadSurvivesSocketWireFormat() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Payload.writeDataStream(new DataOutputStream(bytes), new Payload(7, 42, new byte[] {1, 2, 3}));
        Payload p = Payload.readDataStream(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        assertEquals(7, p.cid);
        assertEquals(42, p.xnr);
        assertArrayEquals(new byte[] {1, 2, 3}, p.any);
    }

    @Test
    public void snapshotRoundTripKeepsFilterAndResults() throws Exception {
        Map<Integer, Integer> filter = new HashMap<>();
        Map<Integer, Payload> results = new HashMap<>();
        filter.put(5, 9);
        results.put(5, new Payload(5, 9, new byte[] {4}));

        Snapshot s = roundTrip(new Snapshot(new byte[] {8}, filter, results));
        assertArrayEquals(new byte[] {8}, s.state);
        assertEquals(Integer.valueOf(9), s.filter.get(5));
        assertArrayEquals(new byte[] {4}, s.results.get(5).any);
    }

    @Test
    public void emptySnapshotRestoresToEmptyMapsNotNull() throws Exception {
        Snapshot s = roundTrip(new Snapshot(new byte[0], new HashMap<>(), new HashMap<>()));
        assertNotNull(s.filter);
        assertNotNull(s.results);
        assertEquals(0, s.filter.size());
    }

    private static Snapshot roundTrip(Snapshot in) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(in);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (Snapshot) ois.readObject();
        }
    }
}
