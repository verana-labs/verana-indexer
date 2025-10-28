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

| Parameter                  | Description                                      | Default                          |
| -------------------------- | ------------------------------------------------ | -------------------------------- |
| `database.enabled`         | Enable PostgreSQL database                       | `false`                         |
| `database.user`            | PostgreSQL username                              | `verana_testnet1`               |
| `database.pwd`             | PostgreSQL password                              | `pass`                          |
| `database.db`             | PostgreSQL database name                          | `verana_testnet1`               |

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

Add additional environment variables to Verana Indexer container with `extraEnv`:

```yaml
extraEnv:
  - name: CUSTOM_ENV_VAR
    value: custom-value
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