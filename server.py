import sys
import threading
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

MAX_KEY_LEN = 128
MAX_VAL_SZ = 1048576

EMPTY = empty_pb2.Empty()

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
 
        # for later replica stuff
        # self.replica_stubs = []
        # if self.is_primary:
        #     for ep in all_endpoints:
        #         if ep != listen_addr:
        #             self.replica_stubs.append(make_stub(ep))
 
        self.counter_puts = 0
        self.counter_gets = 0
        self.counter_deletes = 0
        self.counter_updates = 0
        
    def _replicate(self, op):
        """Send a WriteOp to every replica. Primary committed already"""
        pass
    
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

    def Put(self, request, context):
        self._validate_key(request.key, context)
        self._validate_value(request.value, context)
 
        with self.lock:
            if request.key in self.store:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, f"Key '{request.key}' already exists")
                
            self.store[request.key] = request.value
            self.counter_puts += 1
 
        # self._replicate(pb.WriteOp(type=pb.PUT, key=request.key, value=request.value))
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
 
        # self._replicate(pb.WriteOp(type=pb.DELETE, key=request.key))
        return EMPTY

    def Update(self, request, context):
        self._validate_key(request.key, context)
        self._validate_value(request.value, context)
 
        with self.lock:
            if request.key not in self.store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Key '{request.key}' not found")
                
            self.store[request.key] = request.value
            self.counter_updates += 1
 
        # self._replicate(pb.WriteOp(type=pb.UPDATE, key=request.key, value=request.value))
        return EMPTY

    def List(self, request, context):
        with self.lock:
            entries = [
                pb.ListEntry(key=k, size_bytes=len(v))
                for k, v in self.store.items()
            ]
        return pb.ListResponse(entries=entries)

    def Reset(self, request, context):
        with self.lock:
            self.store.clear()
            self.counter_puts = 0
            self.counter_gets = 0
            self.counter_deletes = 0
            self.counter_updates = 0
 
        # self._replicate(pb.WriteOp(type=pb.RESET))
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