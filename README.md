# Data Reliability Guardrails (`drg`)

> "A local-first guardrail system that prevents silent data failures in batch pipelines."

## Problem Statement
Batch pipelines often fail silently—bad data enters, is processed, and leads to incorrect business reports hours later. This system introduces a **strict validation gate** that checks data against a contract *before* downstream jobs run.

## Quickstart

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Make

### Run the Demo
1. **Start Infrastructure**:
   ```bash
   make up
   ```
   (Wait for Postgres & Grafana to be ready)

2. **Run a "Good" Pipeline**:
   ```bash
   make demo-good
   ```
   - Ingests valid data.
   - Validates (Pass).
   - Runs Downstream Job.

3. **Run a "Bad" Pipeline**:
   ```bash
   make demo-bad
   ```
   - Injects a schema failure or freshness breach.
   - Validates (Fail).
   - Creates Incident.
   - Blocks Downstream Job.

4. **Replay & Fix**:
   ```bash
   make replay
   ```
   - Simulates fixing the data.
   - Reruns validation.
   - Unblocks downstream.

### Observability
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

## Architecture
See [DESIGN.md](DESIGN.md) for details.

## Commands
```bash
# Ingest data
python -m drg.cli ingest --run-id <uuid> --output data/raw/ <scenarios>

# Validate
python -m drg.cli validate --run-id <uuid> --dataset data/raw/file.parquet

# Check Gate & Run Downstream
python -m drg.cli downstream run --run-id <uuid>

# Replay
python -m drg.cli replay --run-id <uuid>
```

