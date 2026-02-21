# Deployment Guide

## 1. Single Node (Docker Compose)

### Prerequisites
- Docker 24+
- Docker Compose v2

### Start
```bash
cd deploy
docker compose -f docker-compose.single-node.yml up -d --build
```

### Verify
```bash
curl http://127.0.0.1:8000/health
```

### Stop
```bash
cd deploy
docker compose -f docker-compose.single-node.yml down
```

Notes:
- SQLite files are persisted at host path `./data`.
- Configure `AUTH_API_KEYS` and `TUSHARE_TOKEN` via shell env before startup.

## 2. Private Cloud (Kubernetes)

### Build and push image
```bash
docker build -f deploy/docker/Dockerfile -t <registry>/trading-assistant:0.7.0 .
docker push <registry>/trading-assistant:0.7.0
```

Update image in `deploy/k8s/private-cloud/trading-assistant.yaml`:
- `image: <registry>/trading-assistant:0.7.0`

### Deploy
```bash
kubectl apply -f deploy/k8s/private-cloud/trading-assistant.yaml
```

### Verify
```bash
kubectl -n trading-assistant get pods
kubectl -n trading-assistant get svc
kubectl -n trading-assistant logs deploy/trading-assistant
```

## 3. Production Hardening Checklist

- Enable `AUTH_ENABLED=true` and provide `AUTH_API_KEYS`.
- Set `OPS_SCHEDULER_ENABLED=true` for periodic job scheduling.
- Restrict outbound network and rotate secret values regularly.
- Configure backup for PVC and test restore procedure.
- Use dedicated storage class with snapshot support.
