
echo "Apertura tunnel per API Gateway (8443)..."
kubectl port-forward deployment/api-gateway 8443:443 &

echo "Apertura tunnel per Prometheus (9090)..."
kubectl port-forward deployment/prometheus 9090:9090 &

echo "Tunnel attivi. Premi CTRL+C per chiuderli tutti."
wait