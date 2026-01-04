#!/bin/bash

CLUSTER_NAME="homework3"
CONFIG_FILE="kind-config.yaml"

GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN} AVVIO SETUP AUTOMATIZZATO HOMEWORK 3...${NC}"

if kind get clusters | grep -q "^$CLUSTER_NAME$"; then
  echo -e "${GREEN} Il cluster '$CLUSTER_NAME' è già attivo.${NC}"
else
  echo -e "${GREEN} Creazione del cluster '$CLUSTER_NAME' usando il file $CONFIG_FILE...${NC}"
  kind create cluster --config $CONFIG_FILE --name $CLUSTER_NAME
fi

echo -e "${GREEN}  Costruzione delle immagini Docker...${NC}"

docker build -t user-manager:latest ./user_manager
docker build -t data-collector:latest ./data_collector
docker build -t api-gateway:v1 .
docker build -t alert-system:latest ./alert_system
docker build -t notifier-system:v5 ./notifier_system


echo -e "${GREEN} Caricamento delle immagini nel cluster Kind...${NC}"

kind load docker-image user-manager:latest --name $CLUSTER_NAME
kind load docker-image data-collector:latest --name $CLUSTER_NAME
kind load docker-image api-gateway:v1 --name $CLUSTER_NAME
kind load docker-image alert-system:latest --name $CLUSTER_NAME
kind load docker-image notifier-system:v5 --name $CLUSTER_NAME


echo -e "${GREEN} Applicazione dei file di configurazione (k8s/)...${NC}"

kubectl apply -f k8s/

echo -e "${GREEN} Deploy completato con successo!${NC}"
echo "Attendi qualche secondo che i pod passino allo stato 'Running'."
echo "Stato attuale dei pod:"
kubectl get pods