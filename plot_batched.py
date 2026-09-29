#!/usr/bin/env python3
"""Plot throughput-vs-latency curves for the batched-consensus benchmark (Tara Fig. 6 style).

Each CSV is one payload combination, with columns:
    concurrency,throughput_rps,avg_latency_ms

Usage:
    python3 plot_batched.py combo1-null-null.csv [combo2-...csv ...]

Writes batched-throughput-latency.png. Requires matplotlib (pip3 install matplotlib).
"""
import csv
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("matplotlib not installed.  Run:  pip3 install matplotlib")


def load(path):
    thr, lat = [], []
    with open(path) as f:
        for r in csv.DictReader(f):
            thr.append(float(r["throughput_rps"]) / 1000.0)   # to 1000 reqs/s
            lat.append(float(r["avg_latency_ms"]))
    return thr, lat


def main():
    paths = sys.argv[1:] or ["combo1-null-null.csv"]
    plt.figure(figsize=(7, 5))
    for p in paths:
        if not os.path.isfile(p):
            print("skip (not found):", p); continue
        thr, lat = load(p)
        label = os.path.splitext(os.path.basename(p))[0]
        plt.plot(thr, lat, marker="+", linestyle=":", label=label)
    plt.xlabel("Throughput [1,000 reqs/s]")
    plt.ylabel("Latency [ms]")
    plt.title("Batched consensus (batch size 5) — throughput vs latency")
    plt.ylim(bottom=0)
    plt.xlim(left=0)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    out = "batched-throughput-latency.png"
    plt.savefig(out, dpi=150)
    print("wrote", out)


if __name__ == "__main__":
    main()
