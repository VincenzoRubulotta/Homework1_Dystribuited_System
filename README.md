# Homework 3: Kubernetes Migration & White-box Monitoring

**Autore:** Vincenzo Rubulotta
**Data:** Gennaio 2026
**Obiettivo:** Migrazione dell'architettura a microservizi su **Kubernetes (Kind)**, implementazione del pattern **White-box Monitoring** tramite **Prometheus** e automazione del processo di build & deploy.

---

## 1. Architettura del Sistema e Scelte Progettuali

L'infrastruttura è stata evoluta abbandonando l'orchestrazione tramite Docker Compose in favore di un cluster **Kubernetes** locale. Il sistema mantiene la natura Event-Driven (basata su Kafka) ma introduce nuove componenti per l'osservabilità e la gestione del traffico.

### Componenti Principali

| Componente | Ruolo | Tipo Service K8s | Porte Esposte (Host -> Node -> Pod) |
| :--- | :--- | :--- | :--- |
| **Nginx Gateway** | Ingress Point e terminazione SSL | **NodePort** | 443 -> 30443 -> 443 |
| **Prometheus** | Server di monitoraggio metriche | **NodePort** | 9090 -> 30090 -> 9090 |
| **User Manager** | Gestione Utenti (gRPC/REST) | ClusterIP | - |
| **Data Collector** | Logica Voli e Circuit Breaker | ClusterIP | - |
| **Kafka & Zookeeper** | Message Broker Asincrono | ClusterIP | - |
| **Alert & Notifier** | Consumer per notifiche e email | - (Deployment) | - |
| **Database** | PostgreSQL e MongoDB | ClusterIP | - |

### Scelte di Design Critiche

1.  **Kubernetes & Kind:** L'intero stack viene eseguito su un cluster Kind configurato con `extraPortMappings` per esporre direttamente le porte **443** (Gateway) e **9090** (Prometheus) su `localhost`, eliminando la necessità di port-forwarding manuali.
2.  **White-box Monitoring (Prometheus):** I microservizi *User Manager* e *Data Collector* sono stati strumentati per esporre metriche custom (Counter e Gauge) sulla porta `8000`. Prometheus effettua lo *scraping* di queste metriche sfruttando il Service Discovery di Kubernetes.
3.  **Gestione Segreti (Security):** Le credenziali sensibili non sono incluse nei manifest ma iniettate tramite oggetti `Secret` di Kubernetes.
4.  **Automazione:** Un singolo script bash gestisce l'intero ciclo di vita: creazione cluster, build delle immagini, caricamento in Kind e applicazione dei manifest.

---

## 2. Prerequisiti e Configurazione Segreti

### Requisiti Software
* **Docker Desktop** (attivo)
* **Kind** (Kubernetes in Docker)
* **Kubectl** (CLI Kubernetes)

### Configurazione di Sicurezza (Obbligatoria)
Prima di avviare il deployment, è **necessario** creare manualmente il file dei segreti, poiché è escluso dal repository per motivi di sicurezza.

1. Creare il file: `k8s/00-secret.yaml`
2. Inserire il seguente contenuto compilando i campi mancanti:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: app-secrets
type: Opaque
stringData:
  # Database Credentials
  POSTGRES_USER: "admin"
  POSTGRES_PASSWORD: "CHANGE_ME"
  
  # Security
  JWT_SECRET: "CHANGE_ME_SECURE_KEY"
  
  # External APIs
  OPENSKY_USER: "tuo_username_opensky"
  OPENSKY_PASS: "tua_password_opensky"
  
  # Email Service
  SMTP_EMAIL: "tua_email@gmail.com"
  SMTP_PASSWORD: "tua_app_password"


  Nota: I certificati SSL (nginx-selfsigned.crt e .key) per il Gateway sono gestiti automaticamente tramite un Secret dedicato o montati come volumi. Assicurarsi che siano presenti nella cartella cert/ nella root del progetto.

  3. Build & Deployment Automatizzato
Non è più necessario utilizzare Docker Compose. È stato predisposto uno script di automazione deploy.sh che esegue in sequenza:

Creazione del Cluster Kind con mappatura porte (80, 443, 9090).

Build delle immagini Docker dai sorgenti locali.

Caricamento delle immagini nei nodi del cluster (evitando il push su registry remoti).

Applicazione di tutti i manifest Kubernetes (k8s/*.yaml).

Avvio del Sistema

Eseguire dalla root del progetto:

chmod +x deploy.sh
./deploy.sh


Attendere che lo script termini e che i Pod siano in stato Running (verificare con kubectl get pods).

Accesso ai Servizi

Una volta completato il deploy, il sistema è accessibile ai seguenti indirizzi:

Applicazione Web (API Gateway): https://localhost (Accettare il certificato self-signed).

Dashboard Prometheus: http://localhost:9090.


4. Testing e Monitoraggio
4.1 Test Funzionali (Postman)

Nella repository è inclusa la Collection Postman aggiornata. I test verificano l'intero flusso:

Registrazione/Login utente.

Avvio raccolta dati voli.

Verifica ricezione notifiche (Email) tramite architettura asincrona Kafka.

4.2 Verifica Metriche

Per verificare il funzionamento del monitoraggio:

Generare traffico usando la collection Postman.

Accedere a http://localhost:9090.

Eseguire query sulle metriche custom, ad esempio:

user_manager_requests_total

data_collector_processing_time_seconds

5. Struttura delle Directory

Homework3/
├── deploy.sh              # Script di automazione Build & Deploy
├── kind-config.yaml       # Configurazione porte Cluster Kind
├── k8s/                   # Manifest Kubernetes (Deployment, Service, ConfigMap)
│   ├── 00-secret.yaml     # (DA CREARE MANUALMENTE - vedi punto 2)
│   ├── prometheus.yaml
│   ├── api-gateway.yaml
│   └── ...
├── nginx.conf             # Configurazione Ingress Gateway
├── user_manager/          # Codice sorgente e Dockerfile
├── data_collector/        # Codice sorgente e Dockerfile
├── alert_system/          # Codice sorgente e Dockerfile
├── notifier_system/       # Codice sorgente e Dockerfile
├── cert/                  # Certificati SSL self-signed
└── README.md