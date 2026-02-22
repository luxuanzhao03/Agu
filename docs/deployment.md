# 部署指南

## 1. 单机部署（Docker Compose）

### 前置条件
- Docker 24+
- Docker Compose v2

### 启动
```bash
cd deploy
docker compose -f docker-compose.single-node.yml up -d --build
```

### 验证
```bash
curl http://127.0.0.1:8000/health
```

### 停止
```bash
cd deploy
docker compose -f docker-compose.single-node.yml down
```

说明：
- SQLite 数据默认持久化在宿主机 `./data`。
- 启动前请通过环境变量配置 `AUTH_API_KEYS` 和 `TUSHARE_TOKEN`。

## 2. 私有云部署（Kubernetes）

### 构建并推送镜像
```bash
docker build -f deploy/docker/Dockerfile -t <registry>/trading-assistant:0.7.0 .
docker push <registry>/trading-assistant:0.7.0
```

在 `deploy/k8s/private-cloud/trading-assistant.yaml` 中更新镜像：
- `image: <registry>/trading-assistant:0.7.0`

### 部署
```bash
kubectl apply -f deploy/k8s/private-cloud/trading-assistant.yaml
```

### 验证
```bash
kubectl -n trading-assistant get pods
kubectl -n trading-assistant get svc
kubectl -n trading-assistant logs deploy/trading-assistant
```

## 3. 生产加固检查清单

- 启用 `AUTH_ENABLED=true`，并配置 `AUTH_API_KEYS`。
- 启用 `OPS_SCHEDULER_ENABLED=true`，让调度作业按周期运行。
- 收紧外网访问策略，并定期轮换密钥/令牌。
- 为 PVC 配置备份策略并定期演练恢复。
- 使用支持快照能力的专用存储类。
