# Azure IoT Hub Data Observability

## Prerequisites

- Docker Desktop
- dotnet-script global tool

Install dotnet-script once:

```powershell
dotnet tool install -g dotnet-script
```

## Start local database

From repo root:

```powershell
docker compose up -d
```

Useful database access commands:

```powershell
psql -p 5432 -h localhost -U postgres
docker exec -it timescaledb psql -U postgres
```

GUI option: [DBeaver](https://dbeaver.io/)

## Grafana (visualization)

`docker compose up -d` also starts **Grafana OSS** alongside TimescaleDB.

- URL: http://localhost:3000
- Login: `admin` / `admin` (change on first login if desired)

TimescaleDB and Postgres are connected automatically via Grafana's
[provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)
feature. On container start, Grafana reads `grafana/provisioning/datasources/postgres.yaml`, which points at the `timescaledb` service using the same `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` values set in `docker-compose.yml`.

To verify it worked: **Connections → Data sources** in Grafana should already
show a `TimescaleDB` entry marked as default; click **Save & test** to
confirm connectivity.

Dashboards added as JSON files into
`grafana/provisioning/dashboards/json/` are linked automatically the same
way (see `grafana/provisioning/dashboards/dashboards.yaml`).

If you change the Postgres credentials, update them in both the
`timescaledb` and `grafana` service blocks in `docker-compose.yml`.

## Configure environment variables

Create a file named .env in repo root with:

```env
AZURE_STORAGE_CONNECTION_STRING=<your-storage-connection-string>
AZURE_STORAGE_CONTAINER_NAME=messages
AZURE_IOT_HUB_NAME=<your-iot-hub-name>
```

Notes:

- AZURE_STORAGE_CONTAINER_NAME must match the routed storage container.
- AZURE_IOT_HUB_NAME must match the `{iothub}` segment used by the IoT Hub routing file format.
- STATE_FILE_PATH is optional. If omitted, the script uses last_run_state.txt in repo root.


## Run the script

From repo root:

```powershell
dotnet script ingest.csx
```