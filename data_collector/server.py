import sys 
import traceback
import grpc
import time
import os
import json
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout
import threading
from concurrent import futures
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from collector_db import init_db_connection, get_db_session
from models import UserInterest, FlightData 
import data_collector_pb2 as dc_pb2
import data_collector_pb2_grpc as dc_pb2_grpc
import user_pb2 as user_pb2 
import user_pb2_grpc as user_pb2_grpc 

UM_ADDRESS = os.getenv("USER_MANAGER_ADDRESS", "user_manager:50051") 
OPENSKY_CREDENTIALS_PATH = os.getenv("OPENSKY_CREDS_PATH", "credentials.json")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", 50052))
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL", 12 * 3600)) # Default 12 ore


class DataCollectorServicer(dc_pb2_grpc.DataCollectorServicer):
    
    def __init__(self):
        print("Inizializzazione Data Collector...")

        self.engine = init_db_connection()
        if not self.engine:
            raise Exception("Impossibile avviare Data Collector senza connessione al DB.")
        self.SessionLocal = get_db_session()

        self.opensky_token = self._get_opensky_token()
        if not self.opensky_token:
            print("AVVISO: Impossibile ottenere il token OpenSky. Il monitoraggio ciclico fallirà.")

        self._start_cyclic_monitoring() 

    def GetStatistics(self, request, context):
       user_email = request.user_email
       airport_icao = request.airport_icao
       days = request.days

       if days <= 0:
           context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
           return dc_pb2.StatisticsResponse(success =False, message="Il numero di giorni deve essere positivo.")
       if not airport_icao:
           context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
           return dc_pb2.StatisticsResponse(success =False, message="Codice aeroporto mancante nella richiesta.")
       if not self._check_user_exists(user_email):
           context.set_code(grpc.StatusCode.NOT_FOUND)
           return dc_pb2.StatisticsResponse(success=False, message="Utente non trovato nell'User Manager.")
       
       db = self.SessionLocal()
       try:
           seconds_back = days * 86400
           start_timestamp = int(time.time()) - seconds_back

           count_arrivals = db.query(func.count(FlightData.id))\
                                .filter(FlightData.airport_icao_ref == airport_icao)\
                                .filter(FlightData.flight_type == "arrival")\
                                .filter(FlightData.last_seen_time >= start_timestamp)\
                                .scalar()
            
           count_departures = db.query(func.count(FlightData.id))\
                                .filter(FlightData.airport_icao_ref == airport_icao)\
                                .filter(FlightData.flight_type == "departure")\
                                .filter(FlightData.last_seen_time >= start_timestamp)\
                                .scalar()
           
           avg_arr = count_arrivals / days
           avg_dep = count_departures / days

           return dc_pb2.StatisticsResponse(
               success=True,
               message=f"Statistiche per {airport_icao} negli ultimi {days} giorni.",
               avg_arrivals = avg_arr,
               avg_departures = avg_dep
           )
           
       except Exception as e:
            print(f"ERRORE durante il calcolo delle statistiche: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return dc_pb2.StatisticsResponse(success=False, message="Errore interno durante il calcolo delle statistiche.") 
       finally:
            db.close()
              
    def _get_opensky_token(self):
        try:
        
            with open(OPENSKY_CREDENTIALS_PATH, "r") as f:
                credenziali = json.load(f)
            
            CLIENT_ID = credenziali["clientId"]
            CLIENT_SECRET = credenziali["clientSecret"]

            token_data = {
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
            
            
            response = requests.post(
                "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
                data=token_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10
            )
            
            response.raise_for_status() 
            
            return response.json().get("access_token")

        except requests.exceptions.HTTPError as e:
            print(f"ERRORE HTTP ({e.response.status_code}): Credenziali OpenSky non valide o non autorizzate.")
            return None
        except requests.exceptions.ConnectionError:
            print("ERRORE DI CONNESSIONE: Impossibile raggiungere il server OpenSky Auth.")
            return None
        
        except FileNotFoundError:
            print(f"ERRORE: File credenziali non trovato in {OPENSKY_CREDENTIALS_PATH}")
            return None
        except json.JSONDecodeError:
            print("ERRORE: Formato JSON non valido nel file credenziali.")
            return None
        
        except Exception as e:
            print(f"ERRORE GENERALE non gestito durante INIT token OpenSky: {e}")
            return None

    def _check_user_exists(self, email):
        try:
            with grpc.insecure_channel(UM_ADDRESS) as channel:
                stub = user_pb2_grpc.UserManagerStub(channel)
                request = user_pb2.UserIdentifier(email=email)
                response = stub.CheckUserExists(request, timeout=5)
                return response.exists
        except grpc.RpcError as e:
            print(f"ERRORE gRPC (User Manager): {e.details()}")
            return False 

    def RegisterInterest(self, request, context):
        user_email = request.user_email
        
        if not self._check_user_exists(user_email):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Utente {user_email} non registrato.")
            return dc_pb2.Status(ok=False, message="Utente non trovato nell'User Manager.")

        db = self.SessionLocal()
        try:
            db.query(UserInterest).filter(UserInterest.user_email == user_email).delete()
            
            new_interests = []
            for icao in request.airport_icao:
                if icao: 
                    new_interests.append(UserInterest(
                        user_email=user_email, 
                        airport_icao=icao
                    ))
            
            db.add_all(new_interests)
            db.commit()

            print(f"Interessi registrati per {user_email}: {request.airport_icao}")
            return dc_pb2.Status(ok=True, message=f"Interessi per {user_email} registrati con successo.")
            
        except Exception as e:
            db.rollback()
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Errore DB: {e}")
            return dc_pb2.Status(ok=False, message=f"Errore salvataggio interessi: {e}")
        finally:
            print("DEBUG: 9. RegisterInterest completato e DB chiuso.")
            db.close()
            


    def _map_to_flight_response(self, flight_data_record):
        try:
            raw_data = flight_data_record.raw_data if flight_data_record.raw_data else {}

            flight_info = dc_pb2.FlightInfo(
                icao24=flight_data_record.icao24 or '', 
                callsign=flight_data_record.callsign or '',
                est_departure_airport=raw_data.get('estDepartureAirport', ''),
                est_arrival_airport=raw_data.get('estArrivalAirport', ''),
                first_seen=int(raw_data.get('firstSeen', 0) or 0), 
                last_seen=int(flight_data_record.last_seen_time or 0), 
            )
            
            return dc_pb2.HistoricalDataResponse(
                success=True, 
                message=f"Dato trovato per ICAO {flight_data_record.icao24} ({flight_data_record.flight_type}).", 
                flights=[flight_info]
            )

        except Exception as map_e:
            print(f"ERRORE DI MAPPATURA/SERIALIZZAZIONE: {map_e}")
            raise map_e
   
    def GetHistoricalData(self, request, context):
        user_email = request.user_email
        db = None 

        try:
            if not request.airport_icao:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                return dc_pb2.HistoricalDataResponse(success=False, message="Codice aeroporto mancante nella richiesta.")
                
            airport_icao = request.airport_icao[0]

            request_type = request.request_type

            if not self._check_user_exists(user_email):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return dc_pb2.HistoricalDataResponse(success=False, message="Utente non trovato nell'User Manager.")
            
            db = self.SessionLocal()

            def build_query():
                query = db.query(FlightData).filter(FlightData.airport_icao_ref == airport_icao)
                if request_type == dc_pb2.ARRIVAL:
                    query = query.filter(FlightData.flight_type == "arrival")
                elif request_type == dc_pb2.DEPARTURE:
                    query = query.filter(FlightData.flight_type == "departure")
                
                return query.order_by(FlightData.last_seen_time.desc())
            

            latest_flight_data = build_query().first()
            
            if latest_flight_data:
                return self._map_to_flight_response(latest_flight_data) 
                
            end_time = int(time.time())
            begin_time = end_time - MONITOR_INTERVAL_SECONDS
            
            fetched_data = self._fetch_flight_data(airport_icao, begin_time, end_time)

            has_data = False
            if fetched_data.get("arrivals"):
                self._save_fetched_data(airport_icao, fetched_data["arrivals"], "arrival", db)
                has_data = True
            if(fetched_data.get("departures")):
                self._save_fetched_data(airport_icao, fetched_data["departures"], "departure", db)
                has_data = True
                
                latest_flight_data = db.query(FlightData).filter(FlightData.airport_icao_ref == airport_icao).first()
                
            if has_data:
                latest_flight_data = build_query().first()

                if latest_flight_data:
                    return self._map_to_flight_response(latest_flight_data)
            
            type_str = "voli"
            if request_type == dc_pb2.ARRIVAL: type_str = "arrivi"
            if request_type == dc_pb2.DEPARTURE: type_str = "partenze"

            return dc_pb2.HistoricalDataResponse(success=False, 
                                                message=f"Nessun volo attivo trovato dall'API OpenSky per {airport_icao}.")
                
        except (HTTPError, ConnectionError) as e:
            if db: db.rollback()
            status_code = getattr(e, 'response', None).status_code if hasattr(e, 'response') else 'N/A'
            print(f"ERRORE: API/Connessione - {type(e).__name__} {status_code}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return dc_pb2.HistoricalDataResponse(success=False, message=f"Errore esterno: {type(e).__name__} {status_code}.")

        except grpc.RpcError as e:
            if db: db.rollback()
            print(f"ERRORE: RPC fallita con UM - {e.details()}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return dc_pb2.HistoricalDataResponse(success=False, message=f"Errore di comunicazione con User Manager: {e.details()}")
            
        except Exception as e:
            if db: db.rollback()
            print(f"ERRORE CRITICO NON GESTITO: {type(e).__name__}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL) 
            return dc_pb2.HistoricalDataResponse(success=False, message=f"Errore interno del server DC.")

        finally:
            if db:
                db.close()

    def _start_cyclic_monitoring(self):
        monitor_thread = threading.Thread(target=self._run_monitoring_loop, daemon=True)
        monitor_thread.start()

    def _get_unique_airports_to_monitor(self, db):
        try:
            results = db.query(UserInterest.airport_icao).distinct().all()
            return [r[0] for r in results if r[0] is not None]
        except Exception as e:
            print(f"Errore lettura interessi dal DB: {e}")
            return []

    def _save_fetched_data(self, icao_ref, flight_data_list, flight_type, db):
        saved_count = 0
        for data_item in flight_data_list:
            db.begin_nested() 
            try:
                new_flight = FlightData(
                    icao24=data_item.get('icao24', 'N/A'),
                    flight_type=flight_type,
                    airport_icao_ref=icao_ref,
                    callsign=data_item.get('callsign'),
                    last_seen_time=data_item.get('lastSeen', int(time.time())),
                    raw_data=data_item 
                )
                db.add(new_flight)
                db.commit() 
                saved_count += 1
            except IntegrityError:
                db.rollback() 
            except Exception as e:
                db.rollback()
                print(f"Errore durante la mappatura/salvataggio: {e}")
        
        db.commit()
        return saved_count


    def _fetch_flight_data(self, icao, begin, end):
        if not self.opensky_token:
            raise Exception("Token OpenSky non disponibile.")

        auth_header = {"Authorization": f"Bearer {self.opensky_token}"}

        def get_data(url, params):
            try:
                resp = requests.get(url, headers=auth_header, params=params, timeout=15)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"AVVISO API: Nessun dato (404) per {icao} su endpoint {url.split('/')[-1]}")
                    return [] 
                raise e 

        arrivals_url = "https://opensky-network.org/api/flights/arrival"
        arrivals_params = {"airport": icao, "begin": begin, "end": end}
        arrivals = get_data(arrivals_url, arrivals_params)

        departures_url = "https://opensky-network.org/api/flights/departure"
        departures_params = {"airport": icao, "begin": begin, "end": end}
        departures = get_data(departures_url, departures_params)
        
        return {"arrivals": arrivals, "departures": departures}


    def _run_monitoring_loop(self):
        """Il loop principale del monitoraggio ciclico."""
        time.sleep(5) 
        print(f"Monitoraggio avviato. Intervallo: {MONITOR_INTERVAL_SECONDS} secondi.")

        while True:
            db = self.SessionLocal()
            start_time = time.time()
            try:
                print(f"\n--- Ciclo Monitoraggio Avviato ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}) ---")
                
                airports_to_monitor = self._get_unique_airports_to_monitor(db)
                
                if not airports_to_monitor:
                    print("Nessun interesse registrato. Salto il recupero.")
                else:
                    end_time = int(time.time())
                    begin_time = end_time - MONITOR_INTERVAL_SECONDS 
                    
                    for icao in airports_to_monitor:
                        try:
                            flight_data = self._fetch_flight_data(icao, begin_time, end_time)

                            arrivals_count = self._save_fetched_data(icao, flight_data["arrivals"], "arrival", db)
                            departures_count = self._save_fetched_data(icao, flight_data["departures"], "departure", db)
                            
                            print(f"-> {icao}: Salvati {arrivals_count} arrivi e {departures_count} partenze.")
                            
                        except requests.HTTPError as he:
                             print(f"ERRORE HTTP (OpenSky) per {icao}: {he.response.status_code} - {he.response.text}")
                        except Exception as e:
                            print(f"ERRORE fetch/salvataggio per {icao}: {e}")
                
            except Exception as e:
                print(f"ERRORE CRITICO nel loop di monitoraggio: {e}")
                db.rollback()
            finally:
                db.close()
                
            elapsed = time.time() - start_time
            sleep_duration = MONITOR_INTERVAL_SECONDS - elapsed
            if sleep_duration > 0:
                print(f"--- Ciclo completato in {elapsed:.2f}s. Attendo {sleep_duration:.0f}s ---")
                time.sleep(sleep_duration)
            else:
                 print(f"--- Ciclo completato. Tempo elapsed: {elapsed:.2f}s (nessuna attesa) ---")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dc_pb2_grpc.add_DataCollectorServicer_to_server(
        DataCollectorServicer(), server)
    server.add_insecure_port(f'[::]:{LISTEN_PORT}')
    server.start()
    print(f"Data Collector avviato sulla porta {LISTEN_PORT}...")
    try:
        while True:
            server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    try:
        time.sleep(5) 
        serve()
    except Exception as e:
        print("\n" + "="*80)
        print("!! ERRORE CRITICO: Il server gRPC non è riuscito ad avviarsi !!")
        print(f"Causa: {type(e).__name__}: {str(e)}")
        print("="*80)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1) 