package com.example.client;

import com.example.types.Payload;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Closed-loop throughput/latency benchmark: for every concurrency level, N client threads each
 * keep exactly one request in flight against a RequestSource, and the run is split into
 * warm-up / measurement / cool-down windows (all configurable via environment variables).
 *
 * <p>Whether requests are batched is decided by the <em>server</em> ({@code TARA_BATCH_SIZE},
 * {@code TARA_BATCH_TIMEOUT}), not by this client, so {@link NonbatchedBenchmark} simply runs the
 * same code.
 *
 * <p>Usage: {@code BatchedBenchmark <host> <port> <csv> <conc1,conc2,...>}
 * <br>Environment: REQ_BYTES, WARMUP_SEC, MEASURE_SEC, COOLDOWN_SEC, REPS, CID_BASE.
 */
public class BatchedBenchmark {

    private static String host;
    private static int    port;
    private static int    reqBytes;
    private static int    totalSec;       // warmup + measure + cooldown

    private static final AtomicInteger cidSeq = new AtomicInteger();
    private static final AtomicInteger xnrSeq = new AtomicInteger(1);

    private static volatile boolean running;
    private static volatile long    repStartNs;
    private static volatile LongAdder[] secCount;
    private static volatile LongAdder[] secLatSumUs;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: BatchedBenchmark <host> <port> <csv> <conc1,conc2,...>");
            System.exit(1);
        }
        host = args[0];
        port = Integer.parseInt(args[1]);
        String csv = args[2];
        String[] conc = args[3].split(",");

        reqBytes        = envInt("REQ_BYTES", 0);
        int warmupSec   = envInt("WARMUP_SEC", 30);
        int measureSec  = envInt("MEASURE_SEC", 80);
        int cooldownSec = envInt("COOLDOWN_SEC", 10);
        int reps        = envInt("REPS", 3);
        totalSec        = warmupSec + measureSec + cooldownSec;
        int cidBase     = envInt("CID_BASE", (int) (System.currentTimeMillis() % 1_000_000) * 1000 + 1);
        cidSeq.set(cidBase);

        System.out.printf("[BatchedBenchmark] host=%s port=%d REQ_BYTES=%d  warmup=%ds measure=%ds cooldown=%ds reps=%d  cidBase=%d%n",
                host, port, reqBytes, warmupSec, measureSec, cooldownSec, reps, cidBase);

        try (PrintWriter pw = new PrintWriter(new FileWriter(csv))) {
            pw.println("concurrency,throughput_rps,avg_latency_ms");
            for (String cs : conc) {
                int c = Integer.parseInt(cs.trim());
                double[] thrRuns = new double[reps];
                double[] latRuns = new double[reps];

                running = true;
                List<Thread> workers = new ArrayList<>();
                for (int i = 0; i < c; i++) {
                    Thread t = new Thread(BatchedBenchmark::worker, "client-" + i);
                    t.setDaemon(true);
                    t.start();
                    workers.add(t);
                }

                for (int rep = 0; rep < reps; rep++) {
                    secCount    = newAdders(totalSec);
                    secLatSumUs = newAdders(totalSec);
                    repStartNs  = System.nanoTime();
                    Thread.sleep(totalSec * 1000L);

                    double thrSum = 0, latSum = 0;
                    int latN = 0;
                    for (int b = warmupSec; b < warmupSec + measureSec; b++) {
                        long cnt = secCount[b].sum();
                        thrSum += cnt;
                        if (cnt > 0) {
                            latSum += (secLatSumUs[b].sum() / (double) cnt) / 1000.0;
                            latN++;
                        }
                    }
                    thrRuns[rep] = thrSum / measureSec;
                    latRuns[rep] = latN > 0 ? latSum / latN : 0;
                    System.out.printf("    conc=%-5d run %d/%d : throughput=%9.1f req/s   avg_latency=%8.3f ms%n",
                            c, rep + 1, reps, thrRuns[rep], latRuns[rep]);
                }

                running = false;
                for (Thread t : workers) t.join(2000);

                double thr = mean(thrRuns);
                double lat = mean(latRuns);
                pw.printf("%d,%.1f,%.3f%n", c, thr, lat);
                pw.flush();
                System.out.printf("[point] conc=%-5d  throughput=%9.1f req/s   avg_latency=%8.3f ms   (avg of %d runs)%n",
                        c, thr, lat, reps);
            }
        }
        System.out.println("[BatchedBenchmark] done -> " + csv);
    }

    private static void worker() {
        final int cid = cidSeq.getAndIncrement();
        final byte[] payload = new byte[reqBytes];
        while (running) {
            try (Socket s = new Socket(host, port)) {
                s.setTcpNoDelay(true);
                s.setSoTimeout(15_000);
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                DataInputStream  dis = new DataInputStream(s.getInputStream());

                while (running && !s.isClosed()) {
                    int  xnr    = xnrSeq.getAndIncrement();
                    long sendNs = System.nanoTime();

                    Payload.writeDataStream(dos, new Payload(cid, xnr, payload));
                    dos.flush();

                    Payload reply = Payload.readDataStream(dis);
                    if (reply == null) break;

                    long recvNs = System.nanoTime();
                    int b = (int) ((recvNs - repStartNs) / 1_000_000_000L);
                    LongAdder[] cnt = secCount, lat = secLatSumUs;
                    if (b >= 0 && b < totalSec && cnt != null) {
                        cnt[b].increment();
                        lat[b].add((recvNs - sendNs) / 1000L);
                    }
                }
            } catch (Exception e) {
                if (running) {
                    try { Thread.sleep(200); } catch (InterruptedException ie) { return; }
                }
            }
        }
    }

    private static LongAdder[] newAdders(int n) {
        LongAdder[] a = new LongAdder[n];
        for (int i = 0; i < n; i++) a[i] = new LongAdder();
        return a;
    }

    private static double mean(double[] v) {
        double s = 0;
        for (double x : v) s += x;
        return v.length > 0 ? s / v.length : 0;
    }

    private static int envInt(String name, int def) {
        String v = System.getenv(name);
        if (v != null) {
            try { return Integer.parseInt(v.trim()); } catch (NumberFormatException ignored) { }
        }
        return def;
    }
}
