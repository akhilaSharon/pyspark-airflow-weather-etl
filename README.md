PySpark + Airflow + Weather ETL (MinIO/S3)  
Build a real, production-style ETL: **Bronze → Silver → Gold** with PySpark, orchestrated by Airflow, stored in S3-compatible object storage (MinIO). Optional BI with Metabase.

<p align="left">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.10+-blue">
  <img alt="PySpark" src="https://img.shields.io/badge/PySpark-3.x-orange">
  <img alt="Airflow" src="https://img.shields.io/badge/Airflow-2.x-%23017CEE">
  <img alt="Storage" src="https://img.shields.io/badge/Storage-MinIO%20(S3)-green">
  <img alt="License" src="https://img.shields.io/badge/License-MIT-lightgrey">
</p>

## 🔎 What this project shows
- **Cloud-style storage locally:** S3 semantics via MinIO (swap to AWS/GCS/Azure later).
- **Data Lakehouse layers:** immutable **Bronze**, curated **Silver**, analytics-ready **Gold**.
- **Orchestration:** Airflow DAG with retries, idempotent writes, and parameterized dates.
- **PySpark data engineering:** partitioning, schema typing, small-file mitigation.
- **(Optional) BI:** push Gold to Postgres and visualize in Metabase.

## 📐 Architecture
Open-Meteo API → [Airflow: Extract] → s3a://bronze/openmeteo/
│
└─ spark-submit → s3a://silver/openmeteo/ (typed Parquet, partitioned by date)
│
└─ spark-submit → s3a://gold/openmeteo/ (aggregates)
└─(optional)→ Postgres → Metabase


## 🗂️ Repo structure
├─ dags/ # Airflow DAGs
├─ spark_jobs/ # PySpark jobs (silver, gold, optional loaders)
├─ apps/ # Small extract scripts (requests → JSON)
├─ config/
│ └─ settings.yaml # Config: buckets, API params, etc.
├─ docker/
│ ├─ airflow/requirements.txt
│ └─ spark/spark-defaults.conf
├─ tests/ # (coming) unit tests for transforms
├─ README.md
└─ .gitignore