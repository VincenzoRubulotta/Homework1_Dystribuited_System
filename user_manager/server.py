
import grpc
from concurrent import futures
import os
import logging
import user_pb2, user_pb2_grpc
from db_user import SessionLocal, engine
from models import Base, User, ProcessedRequest
from sqlalchemy.exc import IntegrityError
from grpc_reflection.v1alpha import reflection

Base.metadata.create_all(bind=engine)

class UserManagerServicer(user_pb2_grpc.UserManagerServicer):
    def __init__(self):
        self.Session = SessionLocal

    def RegisterUser(self, request, context):
        session = self.Session()

        if not request.email or not request.request_id:
            return user_pb2.Status(ok=False, message="Email e ID della richesta sono obbligatori")
        try:

            if session.query(ProcessedRequest).filter_by(request_id=request.request_id).first():
                return user_pb2.Status(ok=False, message="richiesta già processata")
            
            existing = session.get(User, request.email)
            if existing:
                return user_pb2.Status(ok=False, message="User already exists")

            user = User(email=request.email, name=request.name, surname=request.surname)

            request_track = ProcessedRequest(request_id=request.request_id, response_src="UserManager")

            session.add(user)
            session.add(request_track)
            session.commit()
            return user_pb2.Status(ok=True, message="User created")
        except IntegrityError:
            session.rollback()
            return user_pb2.Status(ok=False, message="User already exists (race)")
        except Exception as e:
            session.rollback()
            logging.exception("DB error in RegisterUser")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return user_pb2.Status(ok=False, message="Internal error")
        finally:
            session.close()

    def DeleteUser(self, request, context):
        session = self.Session()
        try:
            existing = session.get(User, request.email)
            if not existing:
                return user_pb2.Status(ok=False, message="User not found")

            session.delete(existing)
            session.commit()
            return user_pb2.Status(ok=True, message="User deleted")
        except Exception as e:
            session.rollback()
            logging.exception("DB error in DeleteUser")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return user_pb2.Status(ok=False, message="Internal error")
        finally:
            session.close()

    def CheckUserExists(self, request, context):
        session = self.Session()
        try:
            exists = session.get(User, request.email) is not None
            return user_pb2.UserExistsResponse(exists=exists)
        except Exception as e:
            logging.exception("DB error in CheckUserExists")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return user_pb2.UserExistsResponse(exists=False)
        finally:
            session.close()

def serve():
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_pb2_grpc.add_UserManagerServicer_to_server(UserManagerServicer(), server)

    SERVICE_NAMES = (
        user_pb2.DESCRIPTOR.services_by_name["UserManager"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    
    port = int(os.getenv("UM_PORT", 50051))
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"UserManager gRPC server listening on {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
