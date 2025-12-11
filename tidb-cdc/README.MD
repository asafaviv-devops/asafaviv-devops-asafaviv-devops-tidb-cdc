README – TiDB CDC Full Pipeline (Kafka → Node → Elasticsearch → Grafana)
🚀 Overview

This project provides a complete real-time Change Data Capture (CDC) pipeline built on TiDB, streaming database changes into Kafka, processing them via a Node.js CDC Consumer, and indexing results into Elasticsearch, with full monitoring through Prometheus and Grafana.

Supported CDC operations:

INSERT

UPDATE

DELETE

DDL events (ignored by consumer, counted as invalid_format)

This setup is ideal for:

Real-time analytics

Audit logs

Search indexing

Monitoring DB activity

📦 Architecture
          ┌──────────┐
          │  TiDB    │
          │ (MySQL   │
          │ compatible) 
          └─────┬────┘
                │  (TiCDC)
                ▼
         ┌─────────────┐
         │   TiDB CDC  │
         └──────┬──────┘
                │ sink: canal-json
                ▼
       ┌──────────────────┐
       │     Kafka        │
       │ topic: tidb-cdc  │
       └────────┬─────────┘
                │
                ▼
       ┌──────────────────┐
       │ Node.js Consumer │
       │  - Validates CDC │
       │  - Parses events │
       │  - Indexes ES    │
       └────────┬─────────┘
                │
                ▼
      ┌────────────────────┐
      │  Elasticsearch      │
      │ index: tidb-cdc…   │
      └────────────────────┘

Monitoring:
  - Node.js exposes /metrics → Prometheus
  - Grafana visualizes Prometheus + ES dashboards

🏗 Project Structure
.
├── docker-compose.yml
├── node-consumer/         # Node.js CDC processor
├── prometheus/            # Prometheus config
├── grafana/               # Dashboards + datasources
├── sql/                   # schema.sql + seed.sql
├── run_test.sh            # CDC automatic test suite
└── QUICK-START.sh         # One-command startup

▶️ How to Start
1️⃣ Start all services
docker compose up -d

2️⃣ Verify TiDB Connection
docker run -it --rm --network=tidb-cdc-v2-final-working_tidb-net mysql:8 \
  mysql -h tidb-server -P 4000 -u root

3️⃣ Run the CDC Test Suite
./run_test.sh


This script performs:

Product inserts

Price updates

User creation

Order creation

Order updates

Deletes

Batch operations

🔍 Checking Results
Elasticsearch
curl -X GET "localhost:9200/tidb-cdc-events/_search?pretty"

Prometheus

👉 http://localhost:9091

Grafana

👉 http://localhost:3001

User: admin
Pass: admin

Dashboards are auto-provisioned.

📊 Metrics Exposed
Node.js Consumer Metrics
Metric	Description
tidb_cdc_operations_total	Counters per table & op
tidb_cdc_processing_duration_seconds	Processing latency
kafka_messages_processed_total	Success / invalid
elasticsearch_operations_total	Indexed docs
Node.js process metrics	CPU, memory, heap, event loop

These appear automatically in Prometheus.

📈 Grafana Dashboards

Preloaded dashboard includes:

CDC Operations over time

Operation rate (per table)

Total inserts / updates

Raw CDC event table (from Elasticsearch)

Node.js performance stats

🛠 Development
Rebuild Node.js Consumer
docker compose build node-consumer
docker compose up -d node-consumer

Reset Everything
docker compose down -v
docker compose up -d

✔️ Requirements

Docker + Docker Compose

At least 4 GB RAM (Elasticsearch requires it)

Linux recommended (Windows WSL2 works)
