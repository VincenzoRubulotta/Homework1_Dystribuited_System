
import grpc
from concurrent import futures
import os
import logging
import threading
from flask import Flask, request, jsonify


import user_pb2, user_pb2_grpc
from db_user import SessionLocal, engine
from models import Base, User, ProcessedRequest
from sqlalchemy.exc import IntegrityError
from grpc_reflection.v1alpha import reflection

Base.metadata.create_all(bind=engine)


GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))
HTTP_PORT = int(os.getenv("HTTP_PORT", 5050))

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def http_health():
    return jsonify({"status": "ok"}), 200

@app.route('/register', methods=['POST'])
def http_register():
    data = request.json
    email = data.get('email')
    req_id = data.get('request_id')

    if not email or not req_id:
        return jsonify({"ok": False, "message": "Email e ID della richesta sono obbligatori"}), 400
    
    session = SessionLocal()
    try:
        if session.query(ProcessedRequest).filter_by(request_id=req_id).first():
            return jsonify({"ok": False, "message": "richiesta già processata"}), 400
        
        if session.get(User, email):
            return jsonify({"ok": False, "message": "User already exists"}), 400
        
        user = User(email=email, name=data.get('name'), surname=data.get('surname'))
        request_track = ProcessedRequest(request_id=req_id, response_src="UserManager_HTTP")

        session.add(user)
        session.add(request_track)
        session.commit()
        return jsonify({"ok": True, "message": "User created"}), 201
    except IntegrityError:
        session.rollback()
        return jsonify({"ok": False, "message": "User conflict"}), 409
    except Exception as e:
        session.rollback()
        logging.exception("DB error in HTTP RegisterUser")
        return jsonify({"ok": False, "message": "Internal error"}), 500
    finally:
        session.close()

@app.route('/delete', methods=['DELETE'])
def http_delete():
    email = request.args.get('email')
    if not email:
        return jsonify({"ok": False, "message": "Email is required"}), 400
    session = SessionLocal()
    try:
        existing = session.get(User, email)
        if not existing:
            return jsonify({"ok": False, "message": "User not found"}), 404

        session.delete(existing)
        session.commit()
        return jsonify({"ok": True, "message": "User deleted"}), 200
    except Exception as e:
        session.rollback()
        return jsonify({"ok": False, "message": "Internal error"}), 500
    finally:
        session.close()

@app.route('/exists', methods=['GET'])
def http_check_exists():
    email = request.args.get('email')
    session = SessionLocal()
    try:
        exists = session.get(User, email) is not None
        return jsonify({"exists": exists}), 200
    except Exception as e:
        logging.exception("DB error in HTTP CheckUserExists")
        return jsonify({"exists": False}), 500
    finally:
        session.close()
    
def run_http():
    app.run(host='0.0.0.0', port=HTTP_PORT, debug=False, use_reloader=False)


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
    
def run_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserManagerServicer_to_server(UserManagerServicer(), server)
        
    SERVICE_NAMES = (
        user_pb2.DESCRIPTOR.services_by_name["UserManager"].full_name,            
        reflection.SERVICE_NAME,
    )

    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    print(f"UserManager gRPC server in ascolto su {GRPC_PORT}")
    server.start()
    server.wait_for_termination()



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    t_http = threading.Thread(target=run_http, daemon=True)
    t_http.start()

    run_grpc()
