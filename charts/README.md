# Verana Indexer Helm Chart

This Helm chart deploys **Verana Indexer** application with a StatefulSet, supporting private and public ingress, persistent storage, and configurable environment variables. It is designed to be flexible, supporting PostgreSQL and Redis integrations.

## Features

* Deploys Verana Indexer with configurable replicas
* Supports private and public ingress with TLS certificates via cert-manager
* Persistent storage using PersistentVolumeClaim with customizable storage class and size
* Configurable environment variables for indexer ports, endpoints, and external services
* Optional PostgreSQL and Redis support
* Customizable deployment color label for easy versioning or environment differentiation

## Kubernetes Resources

* **Service:** Exposes a public API TCP port as well as the ones for redis and db (if enabled)
* **Ingress:**
  * Public ingress for external access with TLS
* **PersistentVolumeClaim:** Provides persistent storage for indexer data (Note: not currently used).
* **StatefulSet:** Runs Verana Indexer container(s) with configurable replicas.

## Configuration

### General

| Parameter                      | Description                                 | Default       |
| ------------------------------ | ------------------------------------------- | ------------- |
| `name`                         | Application name                            | `idx`    |
| `namespace`                    | Kubernetes namespace                        | `default`     |
| `replicas`                     | Number of indexer pods                      | `1`           |
| `domain`                       | Domain for ingress hosts                    | `example.com` |

### Ports

| Parameter     | Description                              | Default |
| ------------- | ---------------------------------------- | ------- |
| `apiPort`   | Port for public API | `3001`  |

### Indexer Configuration

| Parameter                  | Description                                      | Default                          |
| -------------------------- | ------------------------------------------------ | -------------------------------- |
| `extraEnv`                 | Additional environment variables for indexer     | `[]`                            |

### Database Configuration (Optional)

| Parameter                    | Description                                                         | Default             |
| ---------------------------- | ------------------------------------------------------------------- | ------------------- |
| `database.enabled`           | Enable PostgreSQL database                                          | `false`             |
| `database.user`              | PostgreSQL username                                                 | `verana_testnet1`   |
| `database.pwd`               | PostgreSQL password (plain text). If set, takes precedence over `pwdSecret`. Leave empty to use `pwdSecret`. | `pass` |
| `database.pwdSecret.name`    | Name of the Kubernetes Secret containing the DB password            | `""`                |
| `database.pwdSecret.key`     | Key inside the Secret for the DB password                           | `""`                |
| `database.db`                | PostgreSQL database name                                            | `verana_testnet1`   |

**Password via Kubernetes Secret:**

```yaml
database:
  pwd: ""              # leave empty to use the secret below
  pwdSecret:
    name: my-db-secret
    key: password
```

### Redis Configuration (Optional)

| Parameter                  | Description                                      | Default                          |
| -------------------------- | ------------------------------------------------ | -------------------------------- |
| `redis.enabled`            | Enable Redis                                     | `false`                         |
| `redis.host`               | Redis host                                       | `your-redis-host`               |
| `redis.password`           | Redis password                                   | `myRedisPass123`                |

### Persistent Storage

| Parameter                  | Description                                      | Default                          |
| -------------------------- | ------------------------------------------------ | -------------------------------- |
| `storage.size`             | Size of the persistent volume for Indexer      | `1Gi`                           |
| `storage.storageClassName` | Storage class for the persistent volume          | `csi-cinder-high-speed`         |

### Ingress

| Parameter                      | Description                                 | Default       |
| ------------------------------ | ------------------------------------------- | ------------- |
| `ingress.public.enableCors`    | Enable CORS for public ingress              | `true`        |

### Extra Environment Variables

Add any valid Kubernetes env entry to the Verana Indexer container with `extraEnv`. Supports plain values, `secretKeyRef`, `configMapKeyRef`, or any other `valueFrom` source:

```yaml
extraEnv:
  - name: MY_VAR
    value: "my-value"
  - name: MY_SECRET
    valueFrom:
      secretKeyRef:
        name: my-secret
        key: my-key
  - name: MY_CONFIGMAP_VAR
    valueFrom:
      configMapKeyRef:
        name: my-configmap
        key: my-key
```

---

### Resources

Configurable CPU/Memory requests and limits for Verana Indexer container and, if enabled, for PostgreSQL and Redis. Defaults are conservative and can be adjusted after observing real usage.

#### Indexer container

| Parameter                   | Description                    | Default |
| --------------------------- | ------------------------------ | ------- |
| `resources.requests.cpu`    | Minimum reserved CPU           | `100m`  |
| `resources.requests.memory` | Minimum reserved memory        | `256Mi` |
| `resources.limits.cpu`      | Maximum allowed CPU            | `500m`  |
| `resources.limits.memory`   | Maximum allowed memory         | `512Mi` |

#### PostgreSQL (optional)

> Applies only when `database.enabled: true`.

| Parameter                                  | Description              | Default |
| ------------------------------------------ | ------------------------ | ------- |
| `database.resources.requests.cpu`          | Minimum reserved CPU     | `150m`  |
| `database.resources.requests.memory`       | Minimum reserved memory  | `256Mi` |
| `database.resources.limits.cpu`            | Maximum allowed CPU      | `400m`  |
| `database.resources.limits.memory`         | Maximum allowed memory   | `512Mi` |

#### Redis (optional)

> Applies only when `redis.enabled: true`.

| Parameter                             | Description               | Default |
| ------------------------------------- | ------------------------- | ------- |
| `redis.resources.requests.cpu`        | Minimum reserved CPU      | `25m`   |
| `redis.resources.requests.memory`     | Minimum reserved memory   | `64Mi`  |
| `redis.resources.limits.cpu`          | Maximum allowed CPU       | `100m`  |
| `redis.resources.limits.memory`       | Maximum allowed memory    | `128Mi` |

#### Quick Helm overrides

```bash
helm upgrade --install idx ./verana-indexer-chart \
  -n your-namespace \
  --set resources.requests.cpu=100m \
  --set resources.requests.memory=256Mi \
  --set resources.limits.cpu=500m \
  --set resources.limits.memory=512Mi
```

## Reindexing

When you need to re-process all blocks from the beginning (e.g., after schema changes or data fixes), use the built-in reindex support instead of manually changing commands.

### How it works

1. A **pre-upgrade Helm hook Job** runs `pnpm reindex:prepare`, which:
   - Drops module tables (transaction, accounts, etc.)
   - Resets all service checkpoints to 0
   - Runs migrations to recreate tables
   - Resets ID sequences
   - Preserves the `block` table (blocks are NOT re-fetched from RPC)
2. The Job connects to the DB via the K8s Service name (not localhost)
3. After the Job succeeds, the StatefulSet rolls out normally
4. `pnpm start` picks up from the reset checkpoints and re-processes all blocks

### Configuration

| Parameter                          | Description                              | Default |
| ---------------------------------- | ---------------------------------------- | ------- |
| `reindex.enabled`                  | Enable the pre-upgrade reindex Job       | `false` |
| `reindex.resources.requests.cpu`   | CPU request for the Job                  | `100m`  |
| `reindex.resources.requests.memory`| Memory request for the Job               | `256Mi` |
| `reindex.resources.limits.cpu`     | CPU limit for the Job                    | `500m`  |
| `reindex.resources.limits.memory`  | Memory limit for the Job                 | `512Mi` |
| `reindex.backoffLimit`             | Number of retries before marking failed  | `3`     |
| `reindex.activeDeadlineSeconds`    | Timeout for the Job                      | `600`   |

### Usage

**Trigger a reindex:**

```bash
helm upgrade --set reindex.enabled=true <release> <chart> -n <namespace>
```

**After the upgrade completes, disable reindex for future upgrades:**

```bash
helm upgrade --set reindex.enabled=false <release> <chart> -n <namespace>
```

> **Important:** If you leave `reindex.enabled=true`, every subsequent `helm upgrade` will re-run the reindex Job. Always set it back to `false` after the reindex completes.

### Local development

For local development (outside K8s), you can still use the original commands:

```bash
# Full reindex with auto-restart (local dev)
pnpm reindex:dev

# Or just the DB reset step (no service startup)
pnpm reindex:prepare:dev
```

---

## Usage

1. Update values in your `values.yaml` file as needed.
2. Install or upgrade the chart using Helm:

```bash
helm upgrade --install idx ./verana-indexer-chart -n your-namespace -f values.yaml
```

3. Monitor pods and ingress resources to ensure deployment success.

4. To uninstall and remove the deployment:

```bash
helm uninstall idx -n your-namespace
```

This will delete all resources created by the chart in the specified namespace.