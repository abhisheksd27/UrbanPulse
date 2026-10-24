# 🏙️ UrbanPulse — San Francisco Urban Mobility Data Platform

> **Industry-grade, end-to-end Data Engineering Platform on AWS**
>
> Ingests · Processes · Transforms · Serves · Monitors San Francisco taxi and ride-hail data at scale.

[![CI](https://github.com/your-org/urbanpulse/actions/workflows/pr_checks.yml/badge.svg)](https://github.com/your-org/urbanpulse/actions)
[![dbt](https://img.shields.io/badge/dbt-Core-orange)](https://www.getdbt.com/)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)](https://www.terraform.io/)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Orchestration-Airflow-red)](https://airflow.apache.org/)

---

## 📌 Project Overview

UrbanPulse is a production-ready data platform that mirrors what real data engineering teams build at companies like Lyft, Uber, and city transportation agencies. It covers every layer of the modern data stack:

| Layer | Technology |
|---|---|
| **Infrastructure** | Terraform → AWS (S3, Redshift, MSK, EMR, VPC, IAM) |
| **Ingestion** | Python batch loaders + Kafka MSK event streaming |
| **Processing** | PySpark on Amazon EMR / AWS Glue |
| **Orchestration** | Apache Airflow (5 production DAGs) |
| **Transformation** | dbt Core (staging → core → marts) |
| **Data Quality** | Great Expectations + dbt Tests |
| **CI/CD** | GitHub Actions (lint → test → deploy) |
| **Analytics** | Metabase (local) / Amazon QuickSight (AWS) |

---

## 🗺️ Architecture

```
DataSF Taxi API ──▶ Python Loader ──▶ S3 Raw (Bronze)
Open-Meteo API  ──▶ Python Loader ──▶ S3 Raw (Bronze)  ──▶ PySpark EMR ──▶ S3 Staging (Silver)
Kafka Producer  ──▶ Kafka MSK    ──▶ Spark Streaming  ──▶ S3 Curated (Gold)
                                                                │
                                                    Redshift COPY ──▶ dbt ──▶ Marts
                                                                              │
                                                              Metabase / QuickSight
```

Full architecture diagram and deep-dive: [`docs/architecture.md`](docs/architecture.md)

---

## 🚀 Quick Start (Local Development)

### Prerequisites
- Python 3.11+
- Docker Desktop
- AWS CLI configured (`aws configure`)
- Terraform 1.6+

### 1. Clone & Configure
```bash
git clone https://github.com/your-org/urbanpulse.git
cd urbanpulse
cp .env.example .env
# Fill in your values in .env
```

### 2. Install Python Dependencies
```bash
pip install -e ".[dev]"
```

### 3. Start Local Stack (Airflow + Kafka + Postgres)
```bash
make local-up
```

### 4. Provision AWS Infrastructure
```bash
make tf-init
make tf-plan
make tf-apply
```

### 5. Run First Ingestion
```bash
make ingest-taxi YEAR=2024 MONTH=01
make ingest-weather YEAR=2024 MONTH=01
```

### 6. Run dbt
```bash
make dbt-run
make dbt-test
make dbt-docs
```

---

## 📁 Project Structure

```
UrbanPulse/
├── ingestion/       # Python batch loaders + Kafka producer
├── spark/           # PySpark batch jobs (bronze→silver→gold)
├── airflow/         # DAGs + plugins + local Docker setup
├── dbt/             # SQL transformation models + tests + docs
├── quality/         # Great Expectations suites + checkpoints
├── terraform/       # AWS infrastructure as code
├── .github/         # GitHub Actions CI/CD workflows
├── docker/          # Local dev stack (Kafka, Metabase)
├── docs/            # Architecture, data dictionary, runbook
└── scripts/         # Utility scripts (bootstrap, teardown)
```

---

## 📊 Data Sources

| Source | Description | Update Frequency |
|---|---|---|
| [DataSF Taxi Trips](https://data.sfgov.org/Transportation/Taxi-Trips/yitu-d5am) | SFMTA permitted taxi trip records | Monthly |
| [Open-Meteo](https://open-meteo.com/) | SF hourly weather (temp, rain, wind) | Daily |
| SF Neighborhoods GeoJSON | Official SF neighborhood boundaries | Static |
| Kafka Event Producer | Simulated real-time taxi events | Continuous |

---

## 📚 Documentation

| Doc | Description |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Full system architecture deep-dive |
| [`docs/data_dictionary.md`](docs/data_dictionary.md) | Field definitions for every table |
| [`docs/runbook.md`](docs/runbook.md) | How to operate and monitor the pipeline |
| [`docs/decisions/`](docs/decisions/) | Architecture Decision Records (ADRs) |

---

## 🏗️ Build Status

| Step | Component | Status |
|---|---|---|
| 0 | Foundation (repo, Makefile, env) | ✅ Done |
| 1 | Terraform Infrastructure | ✅ Done |
| 2 | Data Ingestion | ✅ Done |
| 3 | PySpark Processing | 🔲 Pending |
| 4 | Airflow Orchestration | 🔲 Pending |
| 5 | dbt Transformation | 🔲 Pending |
| 6 | Great Expectations Quality | 🔲 Pending |
| 7 | GitHub Actions CI/CD | 🔲 Pending |
| 8 | Kafka Streaming | 🔲 Pending |
| 9 | Analytics Dashboards | 🔲 Pending |
| 10 | Governance & Security | 🔲 Pending |

---

## 🤝 Contributing

See [`docs/runbook.md`](docs/runbook.md) for development workflow and conventions.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
