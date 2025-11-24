
from flask import Flask, request, jsonify
import grpc
import os
import sys

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

@app.route('/users/<email>', methods=['DELETE'])
def delete_user(email):
    try:
        stub = get_um_stub()
        req = user_pb2.UserIdentifier(email=email)
        response = stub.DeleteUser(req)
        return jsonify({'ok': response.ok, 'message': response.message})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/users/<email>', methods=['GET'])
def check_user(email):
    try:
        stub = get_um_stub()
        req = user_pb2.UserIdentifier(email=email)
        response = stub.CheckUserExists(req)
        return jsonify({'exists': response.exists})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/register', methods=['POST'])
def register_user():
    data = request.json
    try:
        stub = get_um_stub()
        user_req = user_pb2.UserData(
            email=data.get('email'),
            name=data.get('name'),
            surname=data.get('surname', '')
        )
        response = stub.RegisterUser(user_req)
        return jsonify({'ok': response.ok, 'message': response.message})
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
            request_type= 0
        )
        response = stub.GetHistoricalData(req)
        flights = []
        for f in response.flights:
            flights.append({
                'icao24': f.icao24,
                'callsign': f.callsign,
                'last_seen': f.last_seen
            })
            
        return jsonify({
            'success': response.success,
            'message': response.message,
            'flights': flights
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
  
@app.route('/interests', methods=['POST'])
def register_interests():
    data = request.json
    try:
        stub = get_dc_stub()
        req = data_collector_pb2.InterestRequest(
            user_email=data.get('email'),
            airport_icao=data.get('airports') # Assicurati di passare una lista
        )
        response = stub.RegisterInterest(req)
        return jsonify({'ok': response.ok, 'message': response.message})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/statistics', methods=['GET'])
def get_statistics():
    email = request.args.get('email')
    airport = request.args.get('airport')
    days = int(request.args.get('days', 7)) # Default 7 giorni
    
    try:
        stub = get_dc_stub()
        req = data_collector_pb2.StatisticsRequest(
            user_email=email,
            airport_icao=airport,
            days=days
        )
        response = stub.GetStatistics(req)
        return jsonify({
            'success': response.success,
            'message': response.message,
            'avg_arrivals': response.avg_arrivals,
            'avg_departures': response.avg_departures
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)