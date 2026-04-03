#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import tempfile
import time

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_SCRIPT = os.path.join(SCRIPT_DIR, "bench_worker.py")

sys.path.insert(0, os.path.join(SCRIPT_DIR, ".."))

CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]
DURATION = 30         
VALUE_SIZE = 4096 


def run_workers(cluster, operation, num_workers, duration, value_size):
    tmpdir = tempfile.mkdtemp()
    procs = []

    for i in range(num_workers):
        outfile = os.path.join(tmpdir, f"worker_{i}.txt")
        cmd = [
            sys.executable, WORKER_SCRIPT,
            "--cluster", cluster,
            "--operation", operation,
            "--duration", str(duration),
            "--value-size", str(value_size),
            "--worker-id", str(i),
            "--output", outfile,
        ]
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append((p, outfile))

    for p, _ in procs:
        p.wait()

    all_latencies = []
    for _, outfile in procs:
        if os.path.exists(outfile):
            with open(outfile) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        all_latencies.append(float(line))
            os.remove(outfile)
    os.rmdir(tmpdir)

    if not all_latencies:
        return 0, 0.0, 0.0, 0.0, 0.0

    all_latencies.sort()
    total_ops = len(all_latencies)
    ops_per_sec = total_ops / duration

    p50 = all_latencies[int(total_ops * 0.50)]
    p95 = all_latencies[int(total_ops * 0.95)]
    p99 = all_latencies[int(min(total_ops * 0.99, total_ops - 1))]

    return total_ops, ops_per_sec, p50, p95, p99


def reset_server(cluster):
    try:
        endpoints = sorted(e.lower().strip() for e in cluster.split(","))
        primary = endpoints[0]
        channel = grpc.insecure_channel(primary)
        stub = pb_grpc.ObjectStoreStub(channel)
        stub.Reset(empty_pb2.Empty())
    except Exception as e:
        print(f"Warning: reset failed: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Benchmark 1: Throughput vs Concurrency")
    parser.add_argument("--cluster", required=True, help="Comma-separated list of host:port endpoints")
    parser.add_argument("--duration", type=int, default=DURATION, help=f"Seconds per run (default: {DURATION})")
    args = parser.parse_args()

    results = [] 

    for operation in ["put", "get"]:
        print(f"\n{'='*60}")
        print(f"  Benchmark 1: {operation.upper()} — Throughput vs Concurrency")
        print(f"  Value size: {VALUE_SIZE} bytes, Duration: {args.duration}s")
        print(f"{'='*60}")
        print(f"{'Clients':>8} {'Total Ops':>10} {'Ops/s':>10} "
              f"{'p50 (ms)':>10} {'p95 (ms)':>10} {'p99 (ms)':>10}")
        print("-" * 60)

        for n in CONCURRENCY_LEVELS:
            reset_server(args.cluster)
            time.sleep(1) 

            total, ops_s, p50, p95, p99 = run_workers(args.cluster, operation, n, args.duration, VALUE_SIZE)

            print(f"{n:>8} {total:>10} {ops_s:>10.1f} "f"{p50*1000:>10.2f} {p95*1000:>10.2f} {p99*1000:>10.2f}")

            results.append((operation, n, total, ops_s, p50, p95, p99))

    csv_file = "bench1_results.csv"
    with open(csv_file, "w") as f:
        f.write("operation,concurrency,total_ops,ops_per_sec,p50_ms,p95_ms,p99_ms\n")
        for op, n, total, ops_s, p50, p95, p99 in results:
            f.write(f"{op},{n},{total},{ops_s:.1f},{p50*1000:.2f},{p95*1000:.2f},{p99*1000:.2f}\n")

    print(f"\nResults saved to {csv_file}")


if __name__ == "__main__":
    main()