import argparse
import sys
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

class ObjectStoreCLI:
    def __init__(self, primary_endpoint, all_endpoints):
        self.primary_stub = make_stub(primary_endpoint)
        self.all_stubs = [make_stub(ep) for ep in all_endpoints]
        self.read_index = 0

    def _read_stub(self):
        stub = self.all_stubs[self.read_index % len(self.all_stubs)]
        self.read_index += 1
        return stub

    def do_put(self, args):
        parts = args.split(None, 1)
        if len(parts) < 2:
            print("Usage: put <key> <value>")
            return
        key, value = parts[0], parts[1]
        try:
            self.primary_stub.Put(pb.PutRequest(key=key, value=value.encode()))
            print("OK")
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def do_delete(self, args):
        key = args.strip()
        if not key:
            print("Usage: delete <key>")
            return
        try:
            self.primary_stub.Delete(pb.DeleteRequest(key=key))
            print("OK")
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def do_update(self, args):
        parts = args.split(None, 1)
        if len(parts) < 2:
            print("Usage: update <key> <value>")
            return
        key, value = parts[0], parts[1]
        try:
            self.primary_stub.Update(pb.UpdateRequest(key=key, value=value.encode()))
            print("OK")
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def do_reset(self):
        try:
            self.primary_stub.Reset(EMPTY)
            print("OK")
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def do_get(self, args):
        key = args.strip()
        if not key:
            print("Usage: get <key>")
            return
        try:
            resp = self._read_stub().Get(pb.GetRequest(key=key))
            try:
                print(resp.value.decode())
            except UnicodeDecodeError:
                print(repr(resp.value))
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def do_list(self):
        try:
            resp = self._read_stub().List(EMPTY)
            if not resp.entries:
                print("(empty)")
            else:
                for entry in resp.entries:
                    print(f"  {entry.key}  ({entry.size_bytes} bytes)")
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def do_stats(self):
        try:
            resp = self._read_stub().Stats(EMPTY)
            print(f"  live_objects: {resp.live_objects}")
            print(f"  total_bytes:  {resp.total_bytes}")
            print(f"  puts:         {resp.puts}")
            print(f"  gets:         {resp.gets}")
            print(f"  deletes:      {resp.deletes}")
            print(f"  updates:      {resp.updates}")
        except grpc.RpcError as e:
            print(f"ERROR {e.code().name}: {e.details()}")

    def run(self):
        """Read commands from stdin until EOF or quit."""
        interactive = sys.stdin.isatty()

        while True:
            if interactive:
                try:
                    line = input("objstore> ")
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
            else:
                line = sys.stdin.readline()
                if not line:
                    break
                line = line.rstrip("\n")
                print(f">> {line}")

            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split(None, 1)
            cmd = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""

            if cmd == "put":
                self.do_put(args)
            elif cmd == "get":
                self.do_get(args)
            elif cmd == "delete":
                self.do_delete(args)
            elif cmd == "update":
                self.do_update(args)
            elif cmd == "list":
                self.do_list()
            elif cmd == "stats":
                self.do_stats()
            elif cmd == "reset":
                self.do_reset()
            elif cmd in ("quit", "exit"):
                break
            else:
                print(f"Unknown command: {cmd}")
                print("Commands: put, get, delete, update, list, stats, reset, quit")

def main():
    parser = argparse.ArgumentParser(description="CLI for the distributed object store")
    parser.add_argument("--cluster", required=True, help="Comma-separated list of host:port endpoints")
    args = parser.parse_args()

    primary, all_endpoints = parse_cluster(args.cluster)

    cli = ObjectStoreCLI(primary, all_endpoints)
    cli.run()


if __name__ == "__main__":
    main()