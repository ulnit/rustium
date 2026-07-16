# Rustium Helm Chart

## English

This chart deploys one Rustium connector with a durable SQLite checkpoint. Rustium currently has one source-position owner per connector, so the chart enforces `replicaCount: 1` and a `Recreate` deployment strategy. The checkpoint PVC is retained on Helm uninstall by default.

The default values contain a non-secret PostgreSQL example and must be replaced before production use. Prefer an externally managed Kubernetes Secret containing the complete `rustium.yaml` file:

```bash
kubectl -n rustium create secret generic rustium-config \
  --from-file=rustium.yaml=./rustium.yaml

helm upgrade --install rustium ./deploy/helm/rustium \
  --namespace rustium \
  --create-namespace \
  --set config.existingSecret=rustium-config
```

The mounted configuration should bind the management server to `0.0.0.0:8080`, store SQLite at `/var/lib/rustium/rustium.db`, and interpolate credentials from `extraEnv` or `envFrom`. Do not put database, Kafka, or Schema Registry passwords in a public values file. The pod runs as UID/GID `65532`, drops all Linux capabilities, disables service-account token mounting, and uses a read-only root filesystem. Only the state PVC and `/tmp` are writable.

Expose the health and metrics service inside the cluster:

```bash
kubectl -n rustium port-forward service/rustium 8080:8080
curl http://127.0.0.1:8080/health/live
curl http://127.0.0.1:8080/v1/connector/status
```

Set `serviceMonitor.enabled=true` only when the Prometheus Operator CRD is installed. Set `persistence.existingClaim` when storage is provisioned outside the chart. Disabling persistence is suitable only for disposable development; replacing the pod then loses the checkpoint and may require a new snapshot.

The chart is linted and rendered by the repository packaging gate:

```bash
helm lint --strict deploy/helm/rustium
bash scripts/test-packaging.sh
```

## 简体中文

该 Chart 部署一个带持久 SQLite checkpoint 的 Rustium connector。Rustium 当前每个 connector 只有一个源位点所有者，因此 Chart 强制 `replicaCount: 1` 和 `Recreate` 部署策略。默认情况下，Helm 卸载时会保留 checkpoint PVC。

默认 values 只包含不带真实凭据的 PostgreSQL 示例，生产环境部署前必须替换。推荐使用外部维护的 Kubernetes Secret，Secret 中包含完整的 `rustium.yaml`：

```bash
kubectl -n rustium create secret generic rustium-config \
  --from-file=rustium.yaml=./rustium.yaml

helm upgrade --install rustium ./deploy/helm/rustium \
  --namespace rustium \
  --create-namespace \
  --set config.existingSecret=rustium-config
```

挂载的配置应将管理 Server 绑定到 `0.0.0.0:8080`，将 SQLite 路径设为 `/var/lib/rustium/rustium.db`，并通过 `extraEnv` 或 `envFrom` 插入凭据。不要把数据库、Kafka 或 Schema Registry 密码写入公开 values 文件。Pod 以 UID/GID `65532` 运行，删除全部 Linux capabilities，关闭 ServiceAccount token 挂载，并使用只读根文件系统；只有 state PVC 和 `/tmp` 可写。

可在集群内访问健康检查和指标：

```bash
kubectl -n rustium port-forward service/rustium 8080:8080
curl http://127.0.0.1:8080/health/live
curl http://127.0.0.1:8080/v1/connector/status
```

只有在集群已安装 Prometheus Operator CRD 时才设置 `serviceMonitor.enabled=true`。如果存储由 Chart 外部管理，设置 `persistence.existingClaim`。关闭持久化只适合一次性开发环境；Pod 替换会丢失 checkpoint，可能需要重新执行 snapshot。

仓库 packaging gate 会 lint 和渲染该 Chart：

```bash
helm lint --strict deploy/helm/rustium
bash scripts/test-packaging.sh
```
