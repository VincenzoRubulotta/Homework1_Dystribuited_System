# Homework 2

**Autore:** Vincenzo Rubulotta
**Data:** Dicembre 2025
**Obiettivo:** Evoluzione dell'architettura a microservizi con introduzione di Nginx come Reverse Proxy, Pattern Circuit Breaker e un'architettura **Event-Driven basata su Kafka** per la gestione asincrona degli alert e delle notifiche.

---

## 1. Architettura del Sistema e Scelte Progettuali

Il sistema è stato aggiornato sostituendo il Gateway applicativo con **Nginx** e introducendo un bus di messaggistica (Kafka) per disaccoppiare la logica di business dalla gestione degli allarmi.

### Componenti e Protocolli

| Servizio | Ruolo | Protocollo Esterno | Protocollo Interno | Rete Docker |
| :--- | :--- | :--- | :--- | :--- |
| **Nginx (Reverse Proxy)** | Entry point, Load Balancing e routing | **REST/HTTPS** (Porta 443) | gRPC / HTTPS | `service_net` |
| **User Manager (UM)** | Gestione CRUD Utenti e persistenza | - | gRPC (Server) | `service_net`, `user_db_net` |
| **Data Collector (DC)** | Logica Voli, API OpenSky, Circuit Breaker | - | gRPC (Server) / **Kafka** (Producer) | `service_net`, `data_db_net` |
| **Alert System** | Elaborazione regole e trigger allarmi | - | **Kafka** (Producer & Consumer) | `service_net` |
| **Notifier System** | Spedizione notifiche finali | - | **Kafka** (Consumer) | `service_net` |
| **Kafka & Zookeeper** | Message Broker e coordinamento | - | TCP (9092) | `service_net` |
| **PostgreSQL DBs** | Persistenza dati isolata | - | TCP (5432) | `user_db_net` / `data_db_net` |

### Scelte di Design Critiche

1.  **Event-Driven Architecture (Kafka):** La comunicazione per gli allarmi asincrona.
    * **Data Collector:** Agisce come Producer (per dati di volo).
    * **Alert System:** Consuma i dati di volo, verifica le soglie e Produce eventi di allarme.
    * **Notifier System:** Consuma gli eventi di allarme e gestisce l'invio della notifica tramite posta eletronica.
2.  **Nginx come Reverse Proxy:** Sostituzione del precedente API Gateway Flask con Nginx per gestire l'ingresso del traffico HTTPS e il routing verso i microservizi.
3.  **Fault Tolerance (Circuit Breaker):** Implementazione custom del pattern Circuit Breaker nel Data Collector per gestire i fallimenti delle chiamate verso API esterne (OpenSky), prevenendo il sovraccarico.
4.  **Network Isolation:** Introduzione di una rete dedicata `kafka_net` per il traffico di messaggistica, oltre alle reti di servizio e database.

---

## 2. Prerequisiti e Configurazione

### Nota sulla Compilazione Protobuf
**I file Protobuf (`.proto`) sono già stati compilati.**
Tutti i file Python generati necessari per il funzionamento dei microservizi gRPC sono inclusi.

### Requisiti
Per avviare il progetto è necessario avere:
* **Docker** e **Docker Compose** installati e attivi.
* Il file **`credentials.json`** (con le credenziali OpenSky Network) posizionato in:
  `./data_collector/credentials.json`

### 2.1. Avvio del Sistema
L'intera infrastruttura (inclusi i container Kafka e Zookeeper) si avvia con un unico comando:

```bash
docker compose up -d --build


3. Testing e Automazione (Postman)
L'interfaccia pubblica del sistema è accessibile tramite Nginx (port 443 SSL).

3.1. Test Automatizzato

Nella repository è inclusa la Collection Postman aggiornata che riflette i nuovi endpoint e flussi di interazione. I test verificano sia le risposte sincrone (REST/gRPC) sia l'integrità del flusso asincrono gestito da Kafka (Se si vuoile testare completamente il monitoraggio cicilico con invio notifiche via email si deve testare tutta la collection eccetto l'API REST 6.Delete User).
Si consiglia di creare un file .env dove configurare tutte le variabili richieste all'interno del docker-compose.yaml



Struttura delle Directory

Homework2/
├── nginx.conf        
├── user_manager/     
├── data_collector/    
├── alert_system/      
├── notifier_system/   
├── docker-compose.yml 
└── README.md        