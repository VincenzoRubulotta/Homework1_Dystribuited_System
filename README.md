# Homework 1 - Sistema di Monitoraggio Distribuito Voli Aerei

**Autore:** Vincenzo Rubulotta
**Data:** Novembre 2025
**Obiettivo:** Progettazione e implementazione di un'architettura a microservizi basata su gRPC e Docker per la gestione degli utenti e il monitoraggio dei dati di volo in tempo reale da OpenSky Network.

---

## 1. Architettura del Sistema e Scelte Progettuali

Il sistema è composto da **4 servizi containerizzati** gestiti da Docker Compose. È stata adottata un'architettura ibrida che espone un'interfaccia REST pubblica (tramite un API Gateway) mantenendo comunicazioni gRPC ad alte prestazioni all'interno del cluster.

### Componenti e Protocolli

| Servizio | Ruolo | Protocollo Esterno | Protocollo Interno | Rete Docker |
| :--- | :--- | :--- | :--- | :--- |
| **API Gateway (Flask)** | Entry point unico e traduttore di protocollo | **REST/HTTP** (Porta 8000) | gRPC (Client) | `service_net` |
| **User Manager (UM)** | Gestione CRUD Utenti e persistenza | - | gRPC (Server) | `service_net`, `user_db_net` |
| **Data Collector (DC)** | Logica di Business, API OpenSky, Statistiche | - | gRPC (Server/Client) | `service_net`, `data_db_net` |
| **PostgreSQL DBs** | Due istanze database separate per isolamento dati | - | TCP (5432) | `user_db_net` / `data_db_net` |

### Scelte di Design Critiche

1.  **API Gateway Pattern:** È stato introdotto un servizio Gateway per disaccoppiare i client esterni (Postman) dalla logica interna. Il Gateway traduce le richieste JSON HTTP in chiamate gRPC Protobuf verso i microservizi.
2.  **Network Isolation (Sicurezza):** Sono state implementate **3 reti Docker distinte** (`service_net`, `user_db_net`, `data_db_net`). Questo garantisce il **Principio del Minimo Privilegio**: il Data Collector non ha alcuna connettività di rete verso il Database Utenti e viceversa.
3.  **Comunicazione gRPC:** Tutti i servizi interni comunicano esclusivamente tramite gRPC per garantire tipizzazione forte, contratti definiti (`.proto`) e bassa latenza.
4.  **Logica Cache Miss:** Il sistema ottimizza le chiamate all'API esterna. Se un dato è presente nel DB locale (cache hit), viene restituito subito; altrimenti, viene interrogata l'API OpenSky in tempo reale e il dato viene salvato (cache miss).

---

## 2. Prerequisiti e Configurazione

### Nota sulla Compilazione Protobuf
**I file Protobuf (`.proto`) sono già stati compilati.**
Tutti i file Python generati (`*_pb2.py` e `*_pb2_grpc.py`) necessari per il funzionamento dei microservizi e del Gateway sono già inclusi nelle rispettive directory del repository. **Non è necessario eseguire comandi `protoc` prima dell'avvio.**

### Requisiti
Per avviare il progetto è necessario avere:
* **Docker** e **Docker Compose** installati e attivi.
* Il file **`credentials.json`** (con le credenziali OpenSky Network) posizionato nella cartella:
  `./data_collector/credentials.json`

### 2.1. Avvio del Sistema
L'intera infrastruttura si avvia con un unico comando dalla root del progetto:

```bash
docker compose up -d --build

Questo comando costruisce le immagini, crea le reti isolate e avvia i container in background.

3. Testing e Automazione (Postman)
L'interfaccia pubblica del sistema è accessibile su localhost:8000.

3.1. Test Automatizzato (Collection Allegata)

Nella repository è incluso il file homework1_rest_test_collection.json. Questa Collection Postman contiene una suite di test completa ed automatizzata che verifica l'intero ciclo di vita del dato.

Come eseguire il test:

Importare il file JSON in Postman.

Aprire il Collection Runner.

Avviare l'esecuzione.

Nota: La collection include uno script (Pre-request Script) che genera automaticamente un'email casuale ad ogni esecuzione, permettendo di ripetere il test infinite volte senza conflitti di "Utente già esistente".

3.2. Endpoints API Implementati

Ecco la lista delle funzionalità esposte dal Gateway e testate dalla Collection:

Metodo	Endpoint	Descrizione Funzionalità
POST	/register	Registrazione Utente: Crea un utente nel DB garantendo la politica "at-most-once".
GET	/users/<email>	Verifica: Controlla se un utente esiste nel sistema.
POST	/interests	Interessi: Associa una lista di aeroporti (ICAO) all'utente.
GET	/flights	Dati Volo: Recupera l'ultimo volo. Se non presente nel DB, interroga l'API OpenSky in tempo reale e salva il risultato.
GET	/statistics	Statistiche: Calcola la media dei voli (Arrivi/Partenze) per un aeroporto negli ultimi X giorni.
DELETE	/users/<email>	Cancellazione: Rimuove l'utente e i suoi dati associati.
Struttura delle Directory

Plaintext
Homework1/
├── api_gateway/       # Codice Flask, Dockerfile e adattatori gRPC
├── user_manager/      # Microservizio gestione utenti e definizioni Proto
├── data_collector/    # Microservizio logica voli, API OpenSky e definizioni Proto
├── docker-compose.yml # Orchestrazione container e definizione reti
└── README.md          # Questa documentazione
