from concurrent import futures
import sys
import threading
import argparse
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

MAX_KEY_LEN = 128
MAX_VAL_SZ = 1048576

EMPTY = empty_pb2.Empty()

def parse_cluster(cluster_arg):
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints

def valid_endpoints(endpoints):
    for endpoint in endpoints:
        parts = endpoint.split(':')
        if len(parts) != 2:
            print(f"ERROR: endpoint must have exactly 1 \":\" that splits host and port; {endpoint}")
            return False
        
        host, port = parts
        if not host:
            print(f"ERROR: host must have 1 or more characters; {endpoint}")
            return False
        if not port.isdigit() or not (1 <= int(port) <= 65535):
            print(f"ERROR: port must be an integer between 1 and 65535 (inclusive); {endpoint}")
            return False
        
    return True

def make_stub(endpoint):
    channel = grpc.insecure_channel(endpoint)
    return pb_grpc.ObjectStoreStub(channel)

class ObjectStoreServicer(pb_grpc.ObjectStoreServicer):
    # all_endpoints for later replicas
    def __init__(self, listen_addr, primary_addr, all_endpoints):
        self.store = {}  
        self.lock = threading.Lock()
 
        self.listen_addr = listen_addr
        self.primary_addr = primary_addr
        self.is_primary = (listen_addr == primary_addr)
        
        self.cluster_size = len(all_endpoints)
        self.majority = self.cluster_size // 2 + 1
 
        self.replica_stubs = []
        if self.is_primary:
            for ep in all_endpoints:
                if ep != listen_addr:
                    self.replica_stubs.append(make_stub(ep))
 
        self.counter_puts = 0
        self.counter_gets = 0
        self.counter_deletes = 0
        self.counter_updates = 0
        
    
    def _validate_key(self, key, context):
        """INVALID_ARGUMENT if the key goes against any constraint."""
        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Key must not be empty")
        if len(key) > MAX_KEY_LEN:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"Key exceeds maximum length of {MAX_KEY_LEN} characters")
        for ch in key:
            if not (0x21 <= ord(ch) <= 0x7E):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              "Key contains invalid character. Only ASCII (0x21-0x7E) allowed")
                
    def _validate_value(self, value, context):
        """INVALID_ARGUMENT if value exceeds size limit."""
        if len(value) > MAX_VAL_SZ:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Value exceeds maximum size of {MAX_VAL_SZ} bytes")
            
    def _replicate(self, op):
        """Send a WriteOp to every replica. Primary committed already"""
        acknowledgments = 0
        for stub in self.replica_stubs:
            try:
                stub.ApplyWrite(op)
                acknowledgments += 1
            except grpc.RpcError as e:
                print(f"ERROR: Failed to replicate to a replica: {e.details()}", file=sys.stderr)
                
        return acknowledgments
            
    def _require_primary(self, context):
        """Replicas must reject any client write with FAILED_PRECONDITION."""
        if not self.is_primary:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "This node is a replica. Send writes to the primary")

    def Put(self, request, context):
        self._require_primary(context)
        self._validate_key(request.key, context)
        self._validate_value(request.value, context)
 
        with self.lock:
            if request.key in self.store:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, f"Key '{request.key}' already exists")
                
            self.store[request.key] = request.value
            self.counter_puts += 1
 
        replica_acks = self._replicate(pb.WriteOp(type=pb.PUT, key=request.key, value=request.value))
        total_acks = 1 + replica_acks
 
        if total_acks < self.majority:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Failed to reach majority; write not committed")
            
        return EMPTY

    def Get(self, request, context):
        self._validate_key(request.key, context)
 
        with self.lock:
            if request.key not in self.store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Key '{request.key}' not found")
                
            value = self.store[request.key]
            self.counter_gets += 1
 
        return pb.GetResponse(value=value)

    def Delete(self, request, context):
        self._require_primary(context)
        self._validate_key(request.key, context)
 
        with self.lock:
            if request.key not in self.store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Key '{request.key}' not found")
            del self.store[request.key]
            self.counter_deletes += 1
 
        replica_acks = self._replicate(pb.WriteOp(type=pb.DELETE, key=request.key))
        total_acks = 1 + replica_acks
 
        if total_acks < self.majority:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Failed to reach majority; write may not be committed")
            
        return EMPTY

    def Update(self, request, context):
        self._require_primary(context)
        self._validate_key(request.key, context)
        self._validate_value(request.value, context)
 
        with self.lock:
            if request.key not in self.store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Key '{request.key}' not found")
                
            self.store[request.key] = request.value
            self.counter_updates += 1
 
        replica_acks = self._replicate(pb.WriteOp(type=pb.UPDATE, key=request.key, value=request.value))
        total_acks = 1 + replica_acks
 
        if total_acks < self.majority:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Failed to reach majority; write may not be committed")
 
        return EMPTY

    def List(self, request, context):
        with self.lock:
            entries = [
                pb.ListEntry(key=k, size_bytes=len(v))
                for k, v in self.store.items()
            ]
        return pb.ListResponse(entries=entries)

    def Reset(self, request, context):
        self._require_primary(context)
        
        with self.lock:
            self.store.clear()
            self.counter_puts = 0
            self.counter_gets = 0
            self.counter_deletes = 0
            self.counter_updates = 0
 
        replica_acks = self._replicate(pb.WriteOp(type=pb.RESET))
        total_acks = 1 + replica_acks
 
        if total_acks < self.majority:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Failed to reach majority; write may not be committed")
            
        return EMPTY

    def Stats(self, request, context):
        with self.lock:
            return pb.StatsResponse(
                live_objects=len(self.store),
                total_bytes=sum(len(v) for v in self.store.values()),
                puts=self.counter_puts,
                gets=self.counter_gets,
                deletes=self.counter_deletes,
                updates=self.counter_updates,
            )

    def ApplyWrite(self, request, context):
        """Intra-cluster RPC: primary -> replicas only.
        Clients must never call this directly.
        """
        with self.lock:
            if request.type == pb.PUT:
                self.store[request.key] = request.value
            elif request.type == pb.DELETE:
                self.store.pop(request.key, None)
            elif request.type == pb.UPDATE:
                self.store[request.key] = request.value
            elif request.type == pb.RESET:
                self.store.clear()
                self.counter_puts = 0
                self.counter_gets = 0
                self.counter_deletes = 0
                self.counter_updates = 0
            else:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Unknown WriteOpType: {request.type}")
                
        return EMPTY
    
def serve(listen_addr, primary_addr, all_endpoints):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    servicer = ObjectStoreServicer(listen_addr, primary_addr, all_endpoints)
    pb_grpc.add_ObjectStoreServicer_to_server(servicer, server)
    server.add_insecure_port(listen_addr)
    server.start()
    print(f"Server listening on {listen_addr}", file=sys.stderr)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nServer shutting down.", file=sys.stderr)
        server.stop(0)
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True, help="host:port this instance binds to")
    parser.add_argument("--cluster", required=True, help="Comma-separated list of all host:port endpoints in the cluster")
    args = parser.parse_args()
    
    primary, all_endpoints = parse_cluster(args.cluster)
    listen_addr = args.listen.lower().strip()
 
    if listen_addr not in all_endpoints:
        print(f"ERROR: --listen address '{listen_addr}' is not in --cluster list", file=sys.stderr)
        sys.exit(1)
        
    all_endpoints_valid, listen_addr_valid = valid_endpoints(all_endpoints), valid_endpoints([listen_addr])
    if not all_endpoints_valid or not listen_addr_valid:
        sys.exit(1)
 
    serve(listen_addr, primary, all_endpoints)
    
if __name__ == "__main__":
    main()