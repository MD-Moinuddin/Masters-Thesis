package com.example.tara;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import javax.ws.rs.core.UriBuilder;

/**
 * Protocol constants (fault tolerance F, partitions, window/checkpoint sizes), replica counts per
 * role, port layout, and ZooKeeper helpers for publishing request-source addresses.
 * Batching is tunable via {@code TARA_BATCH_SIZE} and {@code TARA_BATCH_TIMEOUT}; note the
 * defaults (200 / 1000 ms) are large; {@code run_batched_all.sh} uses 5 / 10 ms, and single-request
 * (non-batched) runs need {@code TARA_BATCH_SIZE=1}.
 */
public class TaraConfig {

    public static final int F = 1;
    public static final int PARTITIONS = 1;
    public static final boolean ALO = true;
    public static final int WINDOW_SIZE = 2000;
    public static final int CHECKPOINT_INTERVAL = 1000;
    public static final long CONTROLLER_TIMEOUT = 6000;


    public static final int BATCH_SIZE    = envInt("TARA_BATCH_SIZE", 200);
    public static final int BATCH_TIMEOUT = envInt("TARA_BATCH_TIMEOUT", 1000);

    private static int envInt(String name, int def) {
        String v = System.getenv(name);
        if (v != null) {
            try { return Integer.parseInt(v.trim()); } catch (NumberFormatException ignored) { }
        }
        return def;
    }

    public static final int REQUEST_SOURCES = PARTITIONS * (F+1);
    public static final int GC_SOURCES = 2*F+1;
    public static final int VIEW_SOURCES = PARTITIONS * (2*F+1);
    public static final int RECORD_SOURCES = PARTITIONS * (2*F+1);
    public static final int PROPOSERS = PARTITIONS * (F+1);
    public static final int COMMITTERS = PARTITIONS * (2*F+1);
    public static final int EXECUTORS = 2*F+1;
    public static final int CONTROLLERS = PARTITIONS * (2*F+1);
    public static final int RESULT_SINKS = REQUEST_SOURCES;

    public static final int F_PLUS_ONE = F + 1;

    // Sinks
    public static final int GC_SINKS     = 2*F+1;
    public static final int VIEW_SINKS   = PARTITIONS * (2*F+1);
    public static final int RECORD_SINKS = PARTITIONS * (2*F+1);

    public static final int REQUEST_SOURCE_CLIENT_PORT = 11100;
    public static final int GC_SOURCE_BASE_PORT = 11200;
    public static final int VIEW_SOURCE_BASE_PORT = 11300;
    public static final int RECORD_SOURCE_BASE_PORT = 11400;
    public static final int RESULT_SINK_BASE_PORT = 11000;
    public static final int EXECUTOR_SNAPSHOT_BASE_PORT = 11500;
    public static final int OBSERVER_POLL = 100;


    // Interval (ms) at which RequestSource and Executor emit progress tuples
    public static final long PROGRESS_INTERVAL = 500L;

    // Base directory for executor checkpoints/snapshots
    public static final String SNAPSHOT_DIR = System.getProperty("user.home") + "/tara-snapshots";



    private static final String ZOOKEEPER_QUORUM = "localhost:2181";
    private static final String ZOOKEEPER_ROOT = "/tara-flink";
    private static ZooKeeper zk;

    public static int getPartition(int taskId) {
        return taskId % PARTITIONS;
    }

    public static int getCurrentProposer(int view, int partition) {
        int proposersPerPartition = PROPOSERS / PARTITIONS;
        int baseSubtask = partition * proposersPerPartition;
        return baseSubtask + (view % proposersPerPartition);
    }

    public static int getSubtaskForPartition(int partition, int parallelism) {
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(partition, 128, parallelism);
    }

    public static void initializeZookeeper(boolean create) {
        CountDownLatch latch = new CountDownLatch(1);
        try {
            zk = new ZooKeeper(ZOOKEEPER_QUORUM, 5000, event -> {
                if (event.getState() == KeeperState.SyncConnected) {
                    latch.countDown();
                }
            });
            latch.await();
            TaraLog.info(TaraConfig.class, "ZooKeeper connected: " + ZOOKEEPER_QUORUM);
        } catch (Exception e) {
            TaraLog.error(TaraConfig.class, "ZooKeeper failed: " + e.getMessage());
            throw new RuntimeException(e);
        }

        if (create) {
            createPath(ZOOKEEPER_ROOT);
            createPath(ZOOKEEPER_ROOT + "/requestSources");
        }
    }

    private static void createPath(String path) {
        try {
            zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception ignored) {
            // Path exists or race condition
        }
    }

    public static void publishRequestSourceLocation(int localId) {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            String location = host + ":" + (REQUEST_SOURCE_CLIENT_PORT + localId);
            String path = ZOOKEEPER_ROOT + "/requestSources/" + localId;

            if (zk.exists(path, false) == null) {
                zk.create(path, location.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(path, location.getBytes(), -1);   // replace stale location from a previous run
            }
            TaraLog.info(TaraConfig.class, "Published RequestSource[" + localId + "]: " + location);
        } catch (Exception e) {
            TaraLog.warning(TaraConfig.class, "Publish failed [" + localId + "]: " + e.getMessage());
        }
    }

    public static List<URI> getRequestSources() {
        List<URI> uris = new ArrayList<>();
        for (int i = 0; i < REQUEST_SOURCES; i++) {
            try {
                byte[] data = zk.getData(ZOOKEEPER_ROOT + "/requestSources/" + i, false, null);
                String location = new String(data);
                String[] parts = location.split(":");
                URI uri = UriBuilder.fromUri("http://" + parts[0]).port(Integer.parseInt(parts[1])).build();
                uris.add(uri);
            } catch (Exception e) {
                TaraLog.warning(TaraConfig.class, "RequestSource[" + i + "] unavailable");
            }
        }
        return uris;
    }

    public static void cleanupZookeeper() {
        if (zk == null) return;
        try {
            deleteRecursive(ZOOKEEPER_ROOT + "/requestSources");
            TaraLog.info(TaraConfig.class, "ZK cleanup done");
        } catch (Exception e) {
            TaraLog.warning(TaraConfig.class, "ZK cleanup failed");
        }
    }

    private static void deleteRecursive(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) return;
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            deleteRecursive(path + "/" + child);
        }
        zk.delete(path, -1);
    }


    public static int keyForSubtask(int subtask, int parallelism) {

        for (int k = subtask; k < parallelism * 1000; k += parallelism) {
            if (KeyGroupRangeAssignment.assignKeyToParallelOperator(k, 128, parallelism) == subtask) {
                return k;
            }
        }
        return subtask; // fallback
    }
}
