#!/usr/bin/env python3
from concurrent import futures
import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

EMPTY = empty_pb2.Empty()


class TestObjectStore(pb_grpc.ObjectStoreServicer):
    def Put(self, request, context):
        print(f"Put  -> key={request.key!r}, value={request.value!r}")
        return EMPTY

    def Get(self, request, context):
        print(f"Get  -> key={request.key!r}")
        return pb.GetResponse(value=b"hardcoded-value")

    def Delete(self, request, context):
        print(f"Delete -> key={request.key!r}")
        return EMPTY

    def Update(self, request, context):
        print(f"Update -> key={request.key!r}, value={request.value!r}")
        return EMPTY

    def List(self, request, context):
        print("List ->")
        return pb.ListResponse(entries=[
            pb.ListEntry(key="fake-key-1", size_bytes=10),
            pb.ListEntry(key="fake-key-2", size_bytes=20),
        ])

    def Reset(self, request, context):
        print("Reset ->")
        return EMPTY

    def Stats(self, request, context):
        print("Stats ->")
        return pb.StatsResponse(
            live_objects=2, total_bytes=30,
            puts=1, gets=1, deletes=0, updates=0,
        )

    def ApplyWrite(self, request, context):
        print(f"ApplyWrite -> type={request.type}, key={request.key!r}, value={request.value!r}")
        return EMPTY


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_ObjectStoreServicer_to_server(TestObjectStore(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Test server listening on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()