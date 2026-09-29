package com.example.client;

import com.example.types.Payload;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


/**
 * Fixed-rate load generator used for the fault-tolerance experiment: sends {@code targetRps}
 * requests per second (open loop by default, {@code CLOSED_LOOP=1} for closed loop) and writes
 * per-second sent/received counts and latency to a CSV.
 */
public class LoadGenerator {

    private final String  host;
    private final int     port;
    private final int     targetRps;
    private final int     durationSec;
    private final int     warmupSec;
    private final int     concurrency;
    private final String  csvPath;
    private final int     measureSec;
    private final boolean closedLoop;

    private final AtomicBoolean running    = new AtomicBoolean(true);
    private final AtomicBoolean warmupDone = new AtomicBoolean(false);

    private final AtomicInteger xnrCounter = new AtomicInteger(1);

    private final ConcurrentHashMap<Integer, Long> inFlight = new ConcurrentHashMap<>();

    private volatile long measureStartNs = 0;

    private final LongAdder[] sent;
    private final LongAdder[] rcvd;
    private final LongAdder[] latSum;
    private final LongAdder[] latCount;

    private volatile long[] minArr;
    private volatile long[] maxArr;

    private final Object tbLock   = new Object();
    private double       tbTokens = 0;
    private long         tbLastNs = 0;

    public LoadGenerator(String host, int port, int targetRps, int durationSec,
                         int warmupSec, int concurrency, String csvPath, boolean closedLoop) {
        this.host        = host;
        this.port        = port;
        this.targetRps   = targetRps;
        this.durationSec = durationSec;
        this.warmupSec   = warmupSec;
        this.concurrency = concurrency;
        this.csvPath     = csvPath;
        this.closedLoop  = closedLoop;
        this.measureSec  = durationSec - warmupSec;

        if (measureSec <= 0)
            throw new IllegalArgumentException("durationSec must be > warmupSec");

        sent     = makeLongAdders(measureSec);
        rcvd     = makeLongAdders(measureSec);
        latSum   = makeLongAdders(measureSec);
        latCount = makeLongAdders(measureSec);
    }

    private LongAdder[] makeLongAdders(int n) {
        LongAdder[] arr = new LongAdder[n];
        for (int i = 0; i < n; i++) arr[i] = new LongAdder();
        return arr;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: LoadGenerator <host> <port> <targetRps> <durationSec> <warmupSec> <concurrency> [csvFile]");
            System.exit(1);
        }
        String host        = args[0];
        int    port        = Integer.parseInt(args[1]);
        int    targetRps   = Integer.parseInt(args[2]);
        int    durationSec = Integer.parseInt(args[3]);
        int    warmupSec   = Integer.parseInt(args[4]);
        int    concurrency = Integer.parseInt(args[5]);
        String csvPath     = args.length > 6 ? args[6] : "results.csv";

        String cl          = System.getenv("CLOSED_LOOP");
        boolean closedLoop = cl != null && (cl.equals("1") || cl.equalsIgnoreCase("true"));

        new LoadGenerator(host, port, targetRps, durationSec, warmupSec, concurrency, csvPath, closedLoop).run();
    }

    public void run() throws Exception {
        System.out.printf("[LoadGenerator] host=%s port=%d targetRps=%d durationSec=%d warmupSec=%d concurrency=%d mode=%s%n",
                host, port, targetRps, durationSec, warmupSec, concurrency,
                closedLoop ? "CLOSED-LOOP" : "open-loop");

        System.out.print("[LoadGenerator] Testing connectivity... ");
        try (Socket test = new Socket()) {
            test.connect(new InetSocketAddress(host, port), 3000);
            System.out.println("OK");
        } catch (Exception e) {
            System.out.println("FAILED");
            System.err.println("[LoadGenerator] Cannot connect to " + host + ":" + port + " — " + e.getMessage());
            System.err.println("[LoadGenerator] Make sure Flink is running and FlinkRequestSource has started.");
            System.exit(1);
        }

        long[] localMin = new long[measureSec];
        long[] localMax = new long[measureSec];
        Arrays.fill(localMin, Long.MAX_VALUE);
        minArr = localMin;
        maxArr = localMax;

        tbLastNs = System.nanoTime();

        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < concurrency; i++) {
            final int id = i;
            pool.submit(() -> { if (closedLoop) closedLoopWorker(id); else workerLoop(id); });
        }

        Thread stats = new Thread(this::statsLoop, "stats");
        stats.setDaemon(true);
        stats.start();

        new Thread(() -> {
            sleepMs(warmupSec * 1000L);
            measureStartNs = System.nanoTime();
            warmupDone.set(true);
            System.out.println("[LoadGenerator] Warmup done — measurement started.");
        }, "warmup").start();

        sleepMs(durationSec * 1000L);
        running.set(false);

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        writeCsv();
        printSummary();
    }


    private void workerLoop(int workerId) {
        final int cid = workerId + 1;
        int consecutiveFails = 0;

        while (running.get()) {
            try (Socket socket = new Socket(host, port)) {
                socket.setTcpNoDelay(true);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream  dis = new DataInputStream(socket.getInputStream());

                consecutiveFails = 0;
                System.out.printf("[worker-%d] Connected to %s:%d (cid=%d)%n", workerId, host, port, cid);

                Thread reader = new Thread(() -> readerLoop(workerId, dis), "reader-" + workerId);
                reader.setDaemon(true);
                reader.start();

                while (running.get() && !socket.isClosed()) {
                    acquireToken();

                    int  xnr    = xnrCounter.getAndIncrement();
                    long sendNs = System.nanoTime();

                    inFlight.put(xnr, sendNs);

                    int bucket = measureBucketAt(sendNs);

                    Payload req = new Payload(cid, xnr, ("req-" + xnr).getBytes());
                    Payload.writeDataStream(dos, req);
                    dos.flush();

                    if (bucket >= 0 && bucket < measureSec) {
                        sent[bucket].increment();
                    }
                }

                reader.interrupt();

            } catch (Exception e) {
                consecutiveFails++;
                System.err.printf("[worker-%d] Error (attempt %d): %s%n",
                        workerId, consecutiveFails, e.getMessage());
                if (consecutiveFails >= 5) return;
                sleepMs(500L * consecutiveFails);
            }
        }
    }

    private void closedLoopWorker(int workerId) {
        final int cid = workerId + 1;
        int consecutiveFails = 0;

        while (running.get()) {
            try (Socket socket = new Socket(host, port)) {
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(15_000);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream  dis = new DataInputStream(socket.getInputStream());

                consecutiveFails = 0;
                System.out.printf("[worker-%d] (closed-loop) Connected to %s:%d (cid=%d)%n",
                        workerId, host, port, cid);

                while (running.get() && !socket.isClosed()) {
                    int  xnr    = xnrCounter.getAndIncrement();
                    long sendNs = System.nanoTime();

                    Payload req = new Payload(cid, xnr, ("req-" + xnr).getBytes());
                    Payload.writeDataStream(dos, req);
                    dos.flush();
                    int sb = measureBucketAt(sendNs);
                    if (sb >= 0 && sb < measureSec) sent[sb].increment();


                    Payload reply;
                    try {
                        reply = Payload.readDataStream(dis);
                    } catch (java.net.SocketTimeoutException ste) {

                        break;
                    }
                    if (reply == null) break;

                    long latUs = (System.nanoTime() - sendNs) / 1_000L;

                    if (sb >= 0 && sb < measureSec) {
                        rcvd[sb].increment();
                        latSum[sb].add(latUs);
                        latCount[sb].increment();
                        long cur = minArr[sb];
                        while (latUs < cur) { minArr[sb] = latUs; cur = minArr[sb]; }
                        cur = maxArr[sb];
                        while (latUs > cur) { maxArr[sb] = latUs; cur = maxArr[sb]; }
                    }
                }
            } catch (Exception e) {
                consecutiveFails++;
                if (running.get())
                    System.err.printf("[worker-%d] closed-loop error (attempt %d): %s%n",
                            workerId, consecutiveFails, e.getMessage());
                if (consecutiveFails >= 8) return;
                sleepMs(300L * consecutiveFails);
            }
        }
    }


    private void readerLoop(int workerId, DataInputStream dis) {
        try {
            while (running.get()) {
                Payload reply = Payload.readDataStream(dis);
                long rcvdNs  = System.nanoTime();

                Long sentNs = inFlight.remove(reply.getCommandId()); // getCommandId() returns xnr
                if (sentNs == null) continue;

                long latUs = (rcvdNs - sentNs) / 1_000L;


                int bucket = measureBucketAt(sentNs);
                if (bucket < 0 || bucket >= measureSec) continue;

                rcvd[bucket].increment();
                latSum[bucket].add(latUs);
                latCount[bucket].increment();

                // Update per-second min
                long cur = minArr[bucket];
                while (latUs < cur) {
                    minArr[bucket] = latUs;
                    cur = minArr[bucket];
                }
                // Update per-second max
                cur = maxArr[bucket];
                while (latUs > cur) {
                    maxArr[bucket] = latUs;
                    cur = maxArr[bucket];
                }
            }
        } catch (Exception e) {
            if (running.get()) {
                System.err.printf("[reader-%d] Disconnected: %s%n", workerId, e.getMessage());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Token-bucket rate limiter (shared across all workers)
    // -----------------------------------------------------------------------
    private void acquireToken() {
        while (true) {
            synchronized (tbLock) {
                long now    = System.nanoTime();
                double secs = (now - tbLastNs) / 1e9;
                tbTokens   += secs * targetRps;
                tbLastNs    = now;
                if (tbTokens > targetRps) tbTokens = targetRps;
                if (tbTokens >= 1.0) {
                    tbTokens -= 1.0;
                    return;
                }
            }
            long parkNs = (long) (1e9 / ((double) targetRps * concurrency));
            LockSupport.parkNanos(Math.max(parkNs, 100_000L));
        }
    }

    private int measureBucket() {
        return measureBucketAt(System.nanoTime());
    }

    /** Measurement-second bucket for an arbitrary timestamp (e.g. a request's send time). */
    private int measureBucketAt(long ns) {
        if (!warmupDone.get() || measureStartNs == 0) return -1;
        long elapsed = ns - measureStartNs;
        if (elapsed < 0) return -1;
        return (int) (elapsed / 1_000_000_000L);
    }

    // -----------------------------------------------------------------------
    // Stats / CSV / Summary
    // -----------------------------------------------------------------------
    private void statsLoop() {
        // Replies are counted in their request's send-second (cohort bucketing), so a
        // second's count keeps growing for ~one latency after the second ends. Wait
        // this many seconds before printing a second live, so the displayed numbers
        // match the final CSV (otherwise the live line undercounts). The CSV is always
        // the authoritative, fully-settled data.
        final int lagSec = 2;
        int printed = -1;
        while (running.get()) {
            sleepMs(250);
            if (!warmupDone.get()) continue;
            int settledUpTo = measureBucket() - 1 - lagSec;
            for (int b = printed + 1; b <= settledUpTo && b < measureSec; b++) {
                long s   = sent[b].sum();
                long r   = rcvd[b].sum();
                long cnt = latCount[b].sum();
                long avg = cnt > 0 ? latSum[b].sum() / cnt : 0;
                long mn  = cnt > 0 ? minArr[b] : 0;
                long mx  = maxArr[b];
                System.out.printf("[sec %3d] sent=%5d  rcvd=%5d  avg=%6dµs  min=%6dµs  max=%6dµs%n",
                        b + 1, s, r, avg, mn, mx);
                printed = b;
            }
        }
    }

    private void writeCsv() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(csvPath))) {
            pw.println("second,sent,received,avg_latency_us,min_latency_us,max_latency_us");
            for (int i = 0; i < measureSec; i++) {
                long cnt = latCount[i].sum();
                long avg = cnt > 0 ? latSum[i].sum() / cnt : 0;
                long mn  = cnt > 0 ? minArr[i] : 0;
                long mx  = maxArr[i];
                pw.printf("%d,%d,%d,%d,%d,%d%n",
                        i + 1, sent[i].sum(), rcvd[i].sum(), avg, mn, mx);
            }
            System.out.println("[LoadGenerator] Results written to " + csvPath);
        } catch (Exception e) {
            System.err.println("[LoadGenerator] CSV write failed: " + e.getMessage());
        }
    }

    private void printSummary() {
        long tSent = 0, tRcvd = 0, tCnt = 0, tLatSum = 0;
        long oMin = Long.MAX_VALUE, oMax = 0;
        for (int i = 0; i < measureSec; i++) {
            tSent   += sent[i].sum();
            tRcvd   += rcvd[i].sum();
            tCnt    += latCount[i].sum();
            tLatSum += latSum[i].sum();
            if (latCount[i].sum() > 0 && minArr[i] < oMin) oMin = minArr[i];
            if (maxArr[i] > oMax) oMax = maxArr[i];
        }
        long avg = tCnt > 0 ? tLatSum / tCnt : 0;
        System.out.println("\n========== LOAD GENERATOR SUMMARY ==========");
        System.out.printf("  Measurement window : %d seconds%n",     measureSec);
        System.out.printf("  Target RPS         : %d%n",             targetRps);
        System.out.printf("  Concurrency        : %d connections%n", concurrency);
        System.out.printf("  Total sent         : %d%n",             tSent);
        System.out.printf("  Total received     : %d%n",             tRcvd);
        System.out.printf("  Effective RPS      : %.1f%n",           (double) tRcvd / measureSec);
        System.out.printf("  Avg latency        : %d µs%n",          avg);
        System.out.printf("  Min latency        : %d µs%n",          oMin == Long.MAX_VALUE ? 0 : oMin);
        System.out.printf("  Max latency        : %d µs%n",          oMax);
        System.out.printf("  CSV output         : %s%n",             csvPath);
        System.out.println("=============================================");
    }

    private static void sleepMs(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}