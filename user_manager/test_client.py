import grpc
import user_pb2, user_pb2_grpc

def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = user_pb2_grpc.UserManagerStub(channel)

    print("== Register User ==")
    req = user_pb2.UserData(
        email="test2@example.com",
        name="Mario",
        surname="Rossi"
    )
    resp = stub.RegisterUser(req)
    print(resp.ok, resp.message)

    print("== Check User ==")
    req_check = user_pb2.UserIdentifier(email="test2@example.com")
    resp_check = stub.CheckUserExists(req_check)
    print("exists:", resp_check.exists)

    print("== Delete User ==")
    resp_del = stub.DeleteUser(req_check)
    print(resp_del.ok, resp_del.message)

if __name__ == "__main__":
    main()