#!/usr/bin/env python3
"""Plot Experiment 4 (fault tolerance) — throughput over time with leader-kill markers.

Reads per-second CSVs and writes a PNG shaped like the Tara paper's Fig. 8. Accepted formats:
    second,sent,received,...            (LoadGenerator output)
    second,received_rps,throughput_kps  (committed "csv files/fault-tolerance-csv")

Usage:
    python3 plot_exp4.py [csv ...]                   # one or more CSVs overlaid (e.g. 3 runs)
    KILL_TIMES=60,120 python3 plot_exp4.py run1.csv  # leader-kill markers in seconds

Requires matplotlib:  pip3 install matplotlib
"""
import csv
import sys
import os

try:
    import matplotlib
    matplotlib.use("Agg")  # headless-safe
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("matplotlib not installed.  Run:  pip3 install matplotlib")

KILL_TIMES = tuple(int(x) for x in os.environ.get("KILL_TIMES", "60,120").split(","))  # seconds; see run-experiment.sh


def load(path):
    sec, rcvd = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            sec.append(int(row["second"]))
            rcvd.append(int(row["received"] if "received" in row else row["received_rps"]))
    return sec, rcvd


def main():
    paths = sys.argv[1:] or ["exp4-fault-tolerance.csv"]
    missing = [p for p in paths if not os.path.isfile(p)]
    if missing:
        sys.exit("CSV not found: " + ", ".join(missing))

    plt.figure(figsize=(8, 4))
    peak = 1
    for path in paths:
        sec, rcvd = load(path)
        peak = max(peak, max(rcvd) if rcvd else 1)
        label = os.path.splitext(os.path.basename(path))[0]
        plt.plot(sec, rcvd, lw=1.5, label=label)

    for t in KILL_TIMES:
        plt.axvline(t, color="r", ls="--", alpha=0.6)
        plt.text(t + 1, peak * 0.92, f"leader kill @{t}s",
                 color="r", fontsize=8, rotation=0)

    plt.xlabel("Time [s]")
    plt.ylabel("Throughput [reqs/s]")
    plt.title("Experiment 4: Impact of proposer failures (TARA on Flink)")
    plt.ylim(bottom=0)
    plt.grid(True, alpha=0.3)
    if len(paths) > 1:
        plt.legend(fontsize=8)
    plt.tight_layout()

    out = "exp4-fault-tolerance.png"
    plt.savefig(out, dpi=150)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
