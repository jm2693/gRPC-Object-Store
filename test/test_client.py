import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

EMPTY = empty_pb2.Empty()


def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = pb_grpc.ObjectStoreStub(channel)

    # Put 
    print("Calling Put('mykey', b'hello')...")
    stub.Put(pb.PutRequest(key="mykey", value=b"hello"))
    print("  OK\n")

    # Get
    print("Calling Get('mykey')...")
    resp = stub.Get(pb.GetRequest(key="mykey"))
    print(f"  Got value: {resp.value!r}\n")

    # Update
    print("Calling Update('mykey', b'world')...")
    stub.Update(pb.UpdateRequest(key="mykey", value=b"world"))
    print("  OK\n")

    # List 
    print("Calling List()...")
    resp = stub.List(EMPTY)
    for entry in resp.entries:
        print(f"  {entry.key}  ({entry.size_bytes} bytes)")
    print()

    # Stats
    print("Calling Stats()...")
    resp = stub.Stats(EMPTY)
    print(f"  live_objects={resp.live_objects}, total_bytes={resp.total_bytes}")
    print(f"  puts={resp.puts}, gets={resp.gets}, deletes={resp.deletes}, updates={resp.updates}\n")

    # Delete
    print("Calling Delete('mykey')...")
    stub.Delete(pb.DeleteRequest(key="mykey"))
    print("  OK\n")

    # Reset 
    print("Calling Reset()...")
    stub.Reset(EMPTY)
    print("  OK\n")

    # ApplyWrite 
    print("Calling ApplyWrite (PUT op)...")
    stub.ApplyWrite(pb.WriteOp(type=pb.PUT, key="replkey", value=b"replvalue"))
    print("  OK\n")

    print("All RPCs succeeded!")


if __name__ == "__main__":
    main()