=# 🚀 PySpark + Airflow Weather ETL

An end-to-end **data engineering project** that demonstrates how to build a modern ETL pipeline using:

- **Apache Airflow** – workflow orchestration  
- **PySpark** – distributed data transformations  
- **MinIO** – S3-compatible object storage  
- **PostgreSQL** – final analytics store  
- **Docker Compose** – reproducible local environment  

---

## 📖 Overview

This pipeline ingests **hourly weather data** from [Open-Meteo](https://open-meteo.com/), lands it in an S3-style **Bronze layer**, transforms it with PySpark into **Silver** (cleaned) and **Gold** (aggregated) datasets, and finally loads the results into Postgres using **idempotent UPSERTs**.

It follows a modern **Medallion Architecture** (Bronze → Silver → Gold) and is perfect for practicing real-world data engineering skills.

---

## 📂 Project Structure

```
pyspark-airflow-weather-etl/
├── dags/                     # Airflow DAGs (ETL workflows)
│   └── weather_daily_etl.py  # Main DAG (Bronze → Silver → Gold → Postgres)
├── spark_jobs/               # PySpark transformation & load scripts
│   ├── silver_openmeteo.py
│   ├── gold_openmeteo.py
│   └── load_gold_to_pg.py
├── config/
│   └── settings.yaml         # Config: API URL, location, parameters
├── docker/
│   ├── docker-compose.yml    # Services: Airflow, Spark, MinIO, Postgres
│   └── spark/                # Spark config files
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Prerequisites
- [Docker Desktop](https://docs.docker.com/get-docker/)  
- [VS Code](https://code.visualstudio.com/) (optional but recommended)  
- Python 3.11+ (if you want to run PySpark jobs locally outside Docker)

### 2️⃣ Clone & Configure
```bash
git clone https://github.com/akhilaSharon/pyspark-airflow-weather-etl.git
cd pyspark-airflow-weather-etl
cp config/settings.yaml.example config/settings.yaml
# Edit settings.yaml to set your latitude, longitude, and parameters
```

### 3️⃣ Start the Local Stack
```bash
cd docker
docker compose up -d
```

This launches:
- **Airflow** (web UI at http://localhost:8080)
- **Spark Master & Worker** (Spark UI at http://localhost:8081)
- **MinIO** (S3 UI at http://localhost:9000)
- **Postgres** (localhost:5432 configured)
- **Metabase** (Web UI at http://localhost:3000) (Not in the Pipeline but on the Pipeline, where you can build dashboards)

### 4️⃣ Open Airflow 
- **Username:** `airflow`  
- **Password:** `airflow`  
- Trigger the `weather_daily_etl` DAG manually to test.

---

## 🔄 Pipeline Flow

1. **Bronze:** Extract raw JSON from Open-Meteo and store in MinIO (partitioned by date).  
2. **Silver:** Transform hourly JSON into clean Parquet format.  
3. **Gold:** Aggregate to daily min/max/avg/precipitation metrics.  
4. **Stage:** Write daily results into Postgres staging table.  
5. **UPSERT:** Merge into final `weather_daily` table (safe to re-run without duplicates).

---

## 🧪 Backfilling Historical Data

To load the **previous 10 days sequentially**:

```bash
docker compose exec airflow bash -lc 'python - <<PY
from datetime import date, timedelta
import subprocess
dates = [(date.today() - timedelta(days=i)).isoformat() for i in range(10,0,-1)]
for ds in dates:
    print("Triggering", ds)
    subprocess.run(["airflow","dags","trigger","-e",ds,"weather_daily_etl"])
print("Queued", len(dates), "runs. max_active_runs=1 ensures sequential execution.")
PY'
```

Because `max_active_runs=1` is set in the DAG, Airflow will execute these runs **one after the other**.

---

## 📊 Next Steps

- Connect **Metabase**, **Superset**, or **Power BI** to Postgres and build dashboards.  
- Automate scheduling by enabling `schedule_interval` in the DAG.  
- Deploy to **Astronomer**, **MWAA**, or **GCP Composer** for production usage.

---

## ✨ Why This Project?

This repo demonstrates **real-world data engineering skills**:

- Workflow orchestration (Airflow)  
- Distributed compute (PySpark)  
- Data modeling (Bronze/Silver/Gold)  
- Idempotent loading (UPSERT)  
- Reproducible Dev environment (Docker Compose)  

Perfect for interviews, portfolio projects, or learning data engineering end-to-end.

---

## 🙌 Acknowledgments

- [Open-Meteo](https://open-meteo.com/) for the free weather API  
- [Bitnami Spark](https://hub.docker.com/r/bitnami/spark) & [Postgres images](https://hub.docker.com/_/postgres)  
- [Apache Airflow](https://airflow.apache.org/)
