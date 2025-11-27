
from flask import Flask, request, jsonify
import grpc
import os
import sys
import uuid
import traceback

import user_pb2
import user_pb2_grpc
import data_collector_pb2
import data_collector_pb2_grpc

app = Flask(__name__)

UM_HOST = os.getenv("USER_MANAGER_ADDRESS", "user_manager:50051")
DC_HOST = os.getenv("DATA_COLLECTOR_ADDRESS", "data_collector:50052")

def get_um_stub():
    channel = grpc.insecure_channel(UM_HOST)
    return user_pb2_grpc.UserManagerStub(channel)

def get_dc_stub():
    channel = grpc.insecure_channel(DC_HOST)
    return data_collector_pb2_grpc.DataCollectorStub(channel)

@app.route('/register', methods=['POST'])
def register_user():
    data = request.json
    req_id = data.get('request_id')
    if not req_id:
        req_id = str(uuid.uuid4())

    try:
        stub = get_um_stub()
        user_req = user_pb2.UserData(
            request_id=req_id,
            email=data.get('email'),
            name=data.get('name'),
            surname=data.get('surname', '')
        )
        response = stub.RegisterUser(user_req)
        
        if response.ok:
            return jsonify({'ok': True, 'message': response.message}), 201
        else:
            return jsonify({'ok': False, 'message': response.message}), 409
            
    except grpc.RpcError as e:
        return jsonify({'error': 'User Manager non disponibile'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/users/<email>', methods=['DELETE'])
def delete_user(email):
    try:
        stub = get_um_stub()
        req = user_pb2.UserIdentifier(email=email)
        response = stub.DeleteUser(req)
        
        if response.ok:
            return jsonify({'ok': True, 'message': response.message}), 200
        else:
            return jsonify({'ok': False, 'message': response.message}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/users/<email>', methods=['GET'])
def check_user(email):
    try:
        stub = get_um_stub()
        req = user_pb2.UserIdentifier(email=email)
        response = stub.CheckUserExists(req)
        return jsonify({'exists': response.exists}), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/interests', methods=['POST'])
def register_interests():
    data = request.json
    try:
        stub = get_dc_stub()
        req = data_collector_pb2.InterestRequest(
            user_email=data.get('email'),
            airport_icao=data.get('airports') 
        )
        response = stub.RegisterInterest(req)
        
        if response.ok:
            return jsonify({'ok': True, 'message': response.message}), 200
        else:
            return jsonify({'ok': False, 'message': response.message}), 404
            
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
             return jsonify({'error': 'Utente non trovato'}), 404
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/flights', methods=['GET'])
def get_flights():
    email = request.args.get('email')
    airport = request.args.get('airport')
    try:
        stub = get_dc_stub()
        req = data_collector_pb2.InterestRequest(
            user_email=email,
            airport_icao=[airport], 
            request_type=0 
        )
        
        response = stub.GetHistoricalData(req)
        
        flights = []
        if hasattr(response, 'flights'):
            for f in response.flights:
                est_arr = getattr(f, 'est_arrival_airport', '')
                flights.append({
                    'icao24': getattr(f, 'icao24', 'N/A'),
                    'callsign': getattr(f, 'callsign', 'N/A'),
                    'last_seen': getattr(f, 'last_seen', 0),
                    'type': 'arrival' if est_arr == airport else 'departure'
                })
        
        if response.success:
            return jsonify({
                'success': True,
                'message': response.message,
                'flights': flights
            }), 200
        else:
            return jsonify({
                'success': False,
                'message': response.message,
                'flights': []
            }), 404

    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.NOT_FOUND:
            return jsonify({'error': 'Utente non trovato'}), 404
        elif code == grpc.StatusCode.INVALID_ARGUMENT:
            return jsonify({'error': 'Dati mancanti o non validi'}), 400
        elif code == grpc.StatusCode.UNAVAILABLE:
            return jsonify({'error': 'Data Collector non disponibile'}), 503
        else:
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/statistics', methods=['GET'])
def get_statistics():
    email = request.args.get('email')
    airport = request.args.get('airport')
    days = int(request.args.get('days', 7)) 
    
    try:
        stub = get_dc_stub()
        req = data_collector_pb2.StatisticsRequest(
            user_email=email,
            airport_icao=airport,
            days=days
        )
        response = stub.GetStatistics(req)
        
        if response.success:
            return jsonify({
                'success': True,
                'message': response.message,
                'avg_arrivals': getattr(response, 'avg_arrivals', 0.0),
                'avg_departures': getattr(response, 'avg_departures', 0.0)
            }), 200
        else:
            return jsonify({
                'success': False,
                'message': response.message
            }), 404

    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            return jsonify({'error': 'Utente non trovato'}), 404
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)