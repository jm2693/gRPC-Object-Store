#!/usr/bin/env python3
import argparse
import sys
import time

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

EMPTY = empty_pb2.Empty()


def parse_cluster(cluster_arg):
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints


def make_stub(endpoint):
    channel = grpc.insecure_channel(endpoint)
    return pb_grpc.ObjectStoreStub(channel)


def run_put(primary_stub, value, duration, worker_id):
    latencies = []
    counter = 0
    deadline = time.time() + duration

    while time.time() < deadline:
        key = f"w{worker_id}_k{counter}"
        start = time.perf_counter()
        try:
            primary_stub.Put(pb.PutRequest(key=key, value=value))
            elapsed = time.perf_counter() - start
            latencies.append(elapsed)
        except grpc.RpcError:
            pass
        counter += 1

    return latencies


def run_get(all_stubs, duration, worker_id, read_key):
    latencies = []
    idx = 0
    deadline = time.time() + duration

    while time.time() < deadline:
        stub = all_stubs[idx % len(all_stubs)]
        idx += 1
        start = time.perf_counter()
        try:
            stub.Get(pb.GetRequest(key=read_key))
            elapsed = time.perf_counter() - start
            latencies.append(elapsed)
        except grpc.RpcError:
            pass

    return latencies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--operation", required=True, choices=["put", "get"])
    parser.add_argument("--duration", type=int, required=True, help="Seconds to run")
    parser.add_argument("--value-size", type=int, default=4096, help="Value size in bytes")
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--output", required=True, help="File to write latencies to")
    args = parser.parse_args()

    primary, all_endpoints = parse_cluster(args.cluster)
    primary_stub = make_stub(primary)
    all_stubs = [make_stub(ep) for ep in all_endpoints]
    value = b"x" * args.value_size

    if args.operation == "put":
        latencies = run_put(primary_stub, value, args.duration, args.worker_id)

    elif args.operation == "get":
        read_key = f"bench_read_w{args.worker_id}"
        try:
            primary_stub.Put(pb.PutRequest(key=read_key, value=value))
        except grpc.RpcError:
            pass  
        latencies = run_get(all_stubs, args.duration, args.worker_id, read_key)

    with open(args.output, "w") as f:
        for lat in latencies:
            f.write(f"{lat}\n")

    print(f"Worker {args.worker_id}: {len(latencies)} ops in {args.duration}s", file=sys.stderr)


if __name__ == "__main__":
    main()