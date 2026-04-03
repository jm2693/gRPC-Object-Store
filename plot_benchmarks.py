#!/usr/bin/env python3
import csv
import matplotlib
matplotlib.use("Agg") 
import matplotlib.pyplot as plt


def load_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def plot_bench1():
    rows = load_csv("bench1_results.csv")

    put_rows = [r for r in rows if r["operation"] == "put"]
    get_rows = [r for r in rows if r["operation"] == "get"]

    put_conc = [int(r["concurrency"]) for r in put_rows]
    put_ops  = [float(r["ops_per_sec"]) for r in put_rows]
    put_p99  = [float(r["p99_ms"]) for r in put_rows]

    get_conc = [int(r["concurrency"]) for r in get_rows]
    get_ops  = [float(r["ops_per_sec"]) for r in get_rows]
    get_p99  = [float(r["p99_ms"]) for r in get_rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(put_conc, put_ops, "o-", label="Put (write)", linewidth=2, markersize=6)
    ax.plot(get_conc, get_ops, "s-", label="Get (read)", linewidth=2, markersize=6)
    ax.set_xlabel("Concurrent Clients", fontsize=12)
    ax.set_ylabel("Operations / second", fontsize=12)
    ax.set_title("Benchmark 1: Throughput vs. Concurrency (4 KiB objects)", fontsize=13)
    ax.set_xticks(put_conc)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig("bench1_throughput.png", dpi=150)
    print("Saved bench1_throughput.png")

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(put_conc, put_p99, "o-", label="Put p99", linewidth=2, markersize=6)
    ax.plot(get_conc, get_p99, "s-", label="Get p99", linewidth=2, markersize=6)
    ax.set_xlabel("Concurrent Clients", fontsize=12)
    ax.set_ylabel("p99 Latency (ms)", fontsize=12)
    ax.set_title("Benchmark 1: p99 Latency vs. Concurrency", fontsize=13)
    ax.set_xticks(put_conc)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig("bench1_latency.png", dpi=150)
    print("Saved bench1_latency.png")


def plot_bench2():
    rows = load_csv("bench2_results.csv")

    configs  = [r["config"] for r in rows]
    ops_sec  = [float(r["ops_per_sec"]) for r in rows]
    p99_ms   = [float(r["p99_ms"]) for r in rows]

    x = range(len(configs))

    fig, ax1 = plt.subplots(figsize=(8, 5))

    bars = ax1.bar(x, ops_sec, width=0.5, color="#4C72B0", alpha=0.85, label="Throughput")
    ax1.set_xlabel("Cluster Configuration", fontsize=12)
    ax1.set_ylabel("Operations / second", fontsize=12, color="#4C72B0")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(configs, fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#4C72B0")

    for bar, val in zip(bars, ops_sec):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(ops_sec) * 0.01,
                 f"{val:.0f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax2 = ax1.twinx()
    ax2.plot(list(x), p99_ms, "ro-", linewidth=2, markersize=8, label="p99 Latency")
    ax2.set_ylabel("p99 Latency (ms)", fontsize=12, color="red")
    ax2.tick_params(axis="y", labelcolor="red")

    ax1.set_title("Benchmark 2: Replication Cost (8 clients, 4 KiB puts, 30s)", fontsize=13)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=11)

    fig.tight_layout()
    fig.savefig("bench2_replication.png", dpi=150)
    print("Saved bench2_replication.png")


def main():
    try:
        plot_bench1()
    except FileNotFoundError:
        print("bench1_results.csv not found — skipping Benchmark 1 plots")

    try:
        plot_bench2()
    except FileNotFoundError:
        print("bench2_results.csv not found — skipping Benchmark 2 plots")


if __name__ == "__main__":
    main()