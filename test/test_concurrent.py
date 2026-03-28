#!/usr/bin/env python3
import argparse
import os
import random
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

EMPTY = empty_pb2.Empty()

NUM_THREADS = 20
OPS_PER_THREAD = 200

KEY_POOL = [f"key_{i}" for i in range(50)]


def parse_cluster(cluster_arg):
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints


def make_stub(endpoint):
    channel = grpc.insecure_channel(endpoint)
    return pb_grpc.ObjectStoreStub(channel)


class ThreadResult:
    def __init__(self):
        self.puts = 0
        self.gets = 0
        self.deletes = 0
        self.errors = 0


def worker(stub, thread_id, result):
    rng = random.Random(thread_id) 

    for _ in range(OPS_PER_THREAD):
        key = rng.choice(KEY_POOL)
        value = f"t{thread_id}_v{rng.randint(0, 99999)}".encode()
        op = rng.choice(["put", "get", "delete"])

        try:
            if op == "put":
                stub.Put(pb.PutRequest(key=key, value=value))
                result.puts += 1
            elif op == "get":
                stub.Get(pb.GetRequest(key=key))
                result.gets += 1
            elif op == "delete":
                stub.Delete(pb.DeleteRequest(key=key))
                result.deletes += 1
        except grpc.RpcError as e:
            # Expected errors: ALREADY_EXISTS, NOT_FOUND from contention
            code = e.code()
            if code in (grpc.StatusCode.ALREADY_EXISTS,
                        grpc.StatusCode.NOT_FOUND):
                result.errors += 1
            else:
                result.errors += 1
                print(f"  Thread {thread_id}: unexpected {code.name}: {e.details()}",
                      file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Concurrent correctness test")
    parser.add_argument("--cluster", required=True,
                        help="Comma-separated list of host:port endpoints")
    args = parser.parse_args()

    primary, all_endpoints = parse_cluster(args.cluster)
    stub = make_stub(primary)

    stub.Reset(EMPTY)

    print(f"Starting {NUM_THREADS} threads, {OPS_PER_THREAD} ops each, " f"{len(KEY_POOL)} overlapping keys")
    print(f"Cluster: {args.cluster}\n")

    threads = []
    results = []
    for i in range(NUM_THREADS):
        r = ThreadResult()
        results.append(r)

        t_stub = make_stub(primary)
        t = threading.Thread(target=worker, args=(t_stub, i, r))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_puts = sum(r.puts for r in results)
    total_gets = sum(r.gets for r in results)
    total_deletes = sum(r.deletes for r in results)
    total_errors = sum(r.errors for r in results)
    total_ops = total_puts + total_gets + total_deletes + total_errors

    print(f"All threads finished.")
    print(f"  Thread-side totals: {total_puts} puts, {total_gets} gets, {total_deletes} deletes, {total_errors} expected errors")
    print(f"  Total operations attempted: {total_ops}")
    
    failures = 0

    stats = stub.Stats(EMPTY)
    print(f"\n  Server stats:")
    print(f"    live_objects: {stats.live_objects}")
    print(f"    total_bytes:  {stats.total_bytes}")
    print(f"    puts:         {stats.puts}")
    print(f"    gets:         {stats.gets}")
    print(f"    deletes:      {stats.deletes}")

    if stats.puts != total_puts:
        print(f"  FAIL: server puts={stats.puts}, threads counted {total_puts}")
        failures += 1
    else:
        print(f"  PASS: puts counter matches ({total_puts})")

    if stats.gets != total_gets:
        print(f"  FAIL: server gets={stats.gets}, threads counted {total_gets}")
        failures += 1
    else:
        print(f"  PASS: gets counter matches ({total_gets})")

    if stats.deletes != total_deletes:
        print(f"  FAIL: server deletes={stats.deletes}, threads counted {total_deletes}")
        failures += 1
    else:
        print(f"  PASS: deletes counter matches ({total_deletes})")

    list_resp = stub.List(EMPTY)
    live_keys = {e.key for e in list_resp.entries}
    live_sizes = {e.key: e.size_bytes for e in list_resp.entries}

    if stats.live_objects != len(live_keys):
        print(f"  FAIL: live_objects={stats.live_objects}, but List returned {len(live_keys)} entries")
        failures += 1
    else:
        print(f"  PASS: live_objects matches List length ({len(live_keys)})")

    phantom_count = 0
    for key in live_keys:
        try:
            resp = stub.Get(pb.GetRequest(key=key))
            val = resp.value
            if not val:
                print(f"  FAIL: key '{key}' has empty value")
                failures += 1
        except grpc.RpcError as e:
            print(f"  FAIL: phantom key '{key}' listed but Get returned {e.code().name}")
            phantom_count += 1
            failures += 1

    if phantom_count == 0:
        print(f"  PASS: no phantom keys (all {len(live_keys)} listed keys are gettable)")

    for key in live_keys:
        if key not in KEY_POOL:
            print(f"  FAIL: unexpected key '{key}' not in KEY_POOL")
            failures += 1

    actual_total = sum(live_sizes[k] for k in live_keys)
    if stats.total_bytes != actual_total:
        print(f"  FAIL: total_bytes={stats.total_bytes}, actual sum={actual_total}")
        failures += 1
    else:
        print(f"  PASS: total_bytes matches ({actual_total})")

    if stats.live_objects > total_puts:
        print(f"  FAIL: more live objects ({stats.live_objects}) than total puts ({total_puts})")
        failures += 1
    else:
        print(f"  PASS: live_objects ({stats.live_objects}) <= total puts ({total_puts})")

    print(f"\n{'-'*50}")
    if failures == 0:
        print("ALL CONSISTENCY CHECKS PASSED")
    else:
        print(f"{failures} CONSISTENCY CHECK(S) FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()