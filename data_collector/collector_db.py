from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import os
import time
from models import Base, UserInterest, FlightData 

DB_HOST = os.getenv("DB_HOST", "db_collector")
DB_NAME = os.getenv("POSTGRES_DB", "collector_db")
DB_USER = os.getenv("POSTGRES_USER", "collector_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "collector_password")
DB_PORT = os.getenv("DB_PORT", "5432")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Engine = None

def init_db_connection(max_retries=10, retry_delay=5):
    global Engine
    print(f"Tentativo di connessione al DB su {DB_HOST}...")

    for i in range(max_retries):
        try:
            Engine = create_engine(DATABASE_URL)
            
            with Engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            Base.metadata.create_all(Engine)
            print("Connessione al DB stabilita e tabelle create/verificate con successo.")
            
            return Engine
        
        except OperationalError as e:
            print(f"Tentativo {i+1}/{max_retries}: DB non ancora disponibile. Attendo {retry_delay}s. Dettagli: {e}")
            time.sleep(retry_delay)
        
        except Exception as e:
            print(f"Errore non gestito durante l'inizializzazione del DB: {e}")
            return None

    print("ERRORE CRITICO: Fallita la connessione al database dopo diversi tentativi.")
    return None

def get_db_session():
    if Engine is None:
        raise Exception("L'Engine del database non è stato inizializzato correttamente.")
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=Engine)
    return SessionLocal