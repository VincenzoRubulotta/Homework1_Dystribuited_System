import json
import time
from confluent_kafka import Producer, Consumer

KAFKA_BROKER = 'kafka:9092'
TOPIC_INPUT = 'to-alert-system'
TOPIC_OUTPUT = 'to_notifier'

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'alert-system-group',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': KAFKA_BROKER
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe([TOPIC_INPUT])

def process_flight_data(data):
    airport = data.get('airport_icao')
    current_flights = data.get('current_flights')
    email = data.get('email')
    min_limit = data.get('min_threshold')
    max_limit = data.get('max_threshold')

    if airport is None or current_flights is None or email is None:
        print("Dati mancanti nel messaggio ricevuto.")
        return
    
    alert_triggered = False
    limit_breached = 0
    violation_type = ""
    
    if min_limit is not None and current_flights < min_limit:
        alert_triggered = True
        violation_type = "sotto il limite minimo"
        limit_breached = min_limit
    elif max_limit is not None and current_flights > max_limit:
        alert_triggered = True
        violation_type = "sopra il limite massimo"
        limit_breached = max_limit
    
    if alert_triggered:
        send_notification_request(email, airport, current_flights, limit_breached, violation_type)
    else:
        pass

def send_notification_request(emaail, airport,current_val, limit_val,violation_type):
    
    if violation_type == "sotto il limite minimo":
        msg_text = f"L'aeroporto {airport} ha un numero di voli attuali ({current_val}) sotto il limite minimo impostato ({limit_val})."
    else:
        msg_text = f"L'aeroporto {airport} ha un numero di voli attuali ({current_val}) sopra il limite massimo impostato ({limit_val})."

        notification_payload = {
            'email': emaail,
            'subject': f"ALERT VOLI: {airport} - {violation_type}",
            "message_body": msg_text,

            "details": {
                "airport_icao": airport,
                "current_flights": current_val,
                "limit_value": limit_val,
                "violation_type": violation_type
            }
        }
    try:
        producer.produce(TOPIC_OUTPUT, json.dumps(notification_payload).encode('utf-8'))
        producer.flush()
        print(f"Richiesta di notifica inviata per {airport} a {emaail}.")
    except Exception as e:
        print(f"Errore nell'invio della notifica: {e}")

if __name__ == "__main__":
    print("Alert System in esecuzione...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Errore nel consumo del messaggio: {msg.error()}")
                continue

            try:
                raw_data = msg.value().decode('utf-8')
                flight_data = json.loads(raw_data)
                process_flight_data(flight_data)
            except json.JSONDecodeError as e:
                print(f"Errore nel parsing del messaggio JSON: {e}")
            except Exception as e:
                print(f"Errore generico nel loop principale: {e}")
    except KeyboardInterrupt:
        print("Chiusura dell'Alert System...")
    finally:
        consumer.close()
    