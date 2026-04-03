# Distributed Object Store

A replicated in-memory key-value store using gRPC, with a REST proxy and CLI interface.

## Project Structure

```
├── server.py                 # Replicated storage server (primary/replica)
├── cli.py                    # Command-line client
├── restproxy.py              # REST-to-gRPC proxy (HTTP interface)
├── objectstore.proto         # gRPC service definition
├── objectstore_pb2.py        # Generated protobuf messages
├── objectstore_pb2_grpc.py   # Generated gRPC stubs
├── bench/
│   ├── bench_worker.py           # Single benchmark worker process
│   ├── bench1_throughput.py      # Benchmark 1: Throughput vs. Concurrency
│   └── bench2_replication.py     # Benchmark 2: Replication Cost
├── test/
│   ├── test_concurrent.py        # Concurrent correctness test (20 threads)
│   └── testcmds.txt              # CLI test commands
├── testclient.py             # Interactive test suite (single-node & cluster)
├── plot_benchmarks.py        # Generates plots from benchmark CSVs
└── writeup.md                # Design discussion and benchmark analysis
```

## Setup

```bash
pip install -r requirements.txt
```

## Running Tests

### Interactive tests:

```bash
python3 testclient.py
```

### Concurrent correctness test:

```bash
python3 test/test_concurrent.py --cluster localhost:50051
```

## Running Benchmarks

Start a 3-node cluster on separate machines, then run from a client machine:

### Benchmark 1 — Throughput vs. Concurrency:

```bash
python3 bench/bench1_throughput.py --cluster host1:50051,host2:50051,host3:50051
```

### Benchmark 2 — Replication Cost (interactive, prompts for cluster config between runs):

```bash
python3 bench/bench2_replication.py
```

## Plotting Results

After benchmarks produce `bench1_results.csv` and `bench2_results.csv`:

```bash
python3 plot_benchmarks.py
```

This generates `bench1_throughput.png`, `bench1_latency.png`, and `bench2_replication.png`.