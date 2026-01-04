import json
import os
import smtplib
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from confluent_kafka import Consumer

KAFKA_BROKER = 'kafka-service:9092'
TOPIC_INPUT = 'to_notifier'

SMTP_SERTVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')

if not SENDER_EMAIL or not SENDER_PASSWORD:
    print("Error: SENDER_EMAIL and SENDER_PASSWORD environment variables must be set.")
    sys.exit(1)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'notifier-real-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_INPUT])

def send_email(recipient, subject, body):
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        print(f"connessuine al server SMTP {SMTP_SERTVER}:{SMTP_PORT}")
        server = smtplib.SMTP(SMTP_SERTVER, SMTP_PORT)
        server.ehlo()

        server.starttls()
        server.ehlo()

        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        text_message = msg.as_string()
        server.sendmail(SENDER_EMAIL, recipient, text_message)

        server.quit()

        print(f"Email mandata a  {recipient}")
        return True 
    except smtplib.SMTPException as e:
        print(f"email e password non corrette {recipient}: {e}")
    except Exception as e:
        print(f"Errore nell'invio dell'email a {recipient}: {e}")

    return False


if __name__ == "__main__":
    print(f"Noifier System avviato per {SENDER_EMAIL}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Errore nel messaggio: {msg.error()}")
                continue
            try:
                raw_data = msg.value().decode('utf-8')
                data = json.loads(raw_data)
                email_to = data.get('email')
                subject = data.get('subject', 'Avviso dal allert system')
                body = data.get('body', 'Nessun contenuto specificato.')

                if email_to:
                    print(f"Invio email a {email_to}")
                    send_email(email_to, subject, body)
                else:
                    print("Indirizzo email mancante nel messaggio.")
            except json.JSONEncoderError as e:
                print(f"Errore nella codifica del messaggio JSON: {e}")
            except json.JSONDecodeError as e:
                print(f"Errore nel parsing del messaggio JSON: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Notifier System terminato.")
    