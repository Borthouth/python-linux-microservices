# DockerSentinel 🛡️

Lightweight, high-performance container and host infrastructure monitoring microservice built with **FastAPI**, **Docker Python SDK**, and **psutil**. 

`dockersentinel` acts as a modern replacement for legacy monitoring stacks, providing real-time telemetry on container states, restart anomalies, host resource consumption, and webhook alerting.

---

## Features

- **Docker Socket Integration:** Reads live state from `/var/run/docker.sock` with zero overhead.
- **Anomaly Detection:** Automatically flags containers exhibiting excessive restarts, unhealthy healthchecks, or abnormal exit codes.
- **Host Resource Telemetry:** Monitors real-time RAM and disk utilization against configurable thresholds.
- **Granular Filtering:** Query specific containers on the fly using the `?name=` API parameter.
- **Self-Monitoring:** Built-in Docker health check to monitor its own uptime and responsiveness.
- **Secure by Default:** Supports HTTPS out-of-the-box via Uvicorn SSL configurations.
- **Internal Webhook Dispatching:** Automatically triggers notifications on critical system status changes.

---

## Tech Stack

- **Framework:** FastAPI / Uvicorn
- **Container Control:** Docker Python SDK
- **System Metrics:** `psutil`
- **Configuration:** Pydantic Settings

---

## Project Structure

```text
dockersentinel/
├── Dockerfile
├── docker-compose.yml
├── LICENSE
├── pyproject.toml
├── README.md
├── certs/
│   ├── cert.pem
│   └── key.pem
└── sentinel/
    ├── __init__.py
    ├── main.py
    ├── config.py
    ├── docker_monitor.py
    ├── host_monitor.py
    └── notifier.py
```

---

## Configuration & Environment Variables

`dockersentinel` is fully configurable via environment variables (either set in your shell, a `.env` file, or directly inside the `docker-compose.yml` environment block).

| Variable Name | Default Value | Description |
| :--- | :--- | :--- |
| `APP_NAME` | `DockerSentinel Resource & Container Watch` | The title identifier for the FastAPI application instance. |
| `DEBUG` | `false` | Enables or disables FastAPI debug mode. |
| `DISK_THRESHOLD_PERCENT` | `85.0` | Host disk utilization percentage that triggers a `CRITICAL` system status alert. |
| `RAM_THRESHOLD_PERCENT` | `90.0` | Host RAM utilization percentage that triggers a `CRITICAL` system status alert. |
| `CONTAINER_RESTART_THRESHOLD` | `3` | Number of container restarts required to flag a container as anomalous. |
| `DOCKER_SOCKET_PATH` | `unix://var/run/docker.sock` | Connection URI for the Docker daemon socket. |
| `WEBHOOK_URL` | *None* (Disabled) | Internal webhook target URL (e.g., Slack, Discord, Teams) to dispatch alerts when a `CRITICAL` state is detected. |

---

## Quick Start Guide

### Step 1: Generate Local SSL Certificates
Run this in the project root to create local self-signed certificates for HTTPS:
```bash
mkdir -p certs
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout certs/key.pem \
  -out certs/cert.pem \
  -subj "/CN=localhost"
```

### Step 2: Configure Environment Variables
Create a `.env` file in the project root if you prefer file-based configuration:
```env
DISK_THRESHOLD_PERCENT=85.0
RAM_THRESHOLD_PERCENT=90.0
CONTAINER_RESTART_THRESHOLD=3
WEBHOOK_URL=https://your-webhook-target-here
```

### Step 3: Build and Run via Docker Compose
```bash
docker compose up -d --build
```

---

## API Endpoints

### Health Check
- **URL:** `GET /health`
- **Description:** Verifies that the sentinel service is active and responsive.

### System & Container Summary
- **URL:** `GET /api/v1/sentinel/summary`
- **Query Parameters:** 
  - `name` (optional): Filter container results by name (partial match). *Example: `/api/v1/sentinel/summary?name=bacs`*
- **Example cURL Request (HTTPS with self-signed certificate bypass):**
  ```bash
  curl -sk https://localhost:8000/api/v1/sentinel/summary | jq
  ```

