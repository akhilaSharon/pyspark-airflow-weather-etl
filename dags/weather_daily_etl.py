# dags/weather_daily_etl.py
from datetime import datetime, timedelta
import os, json, requests, yaml, boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- Config ---
CONFIG_PATH = "/opt/airflow/config/settings.yaml"
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS   = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET   = os.getenv("S3_SECRET_KEY", "minio123")
BRONZE_BUCKET = "bronze"

# ABSOLUTE host path to your repo:
host_root = "/Users/akhilavallabhaneni/dev_code/pyspark-airflow-weather-etl"  # <- change if needed

# --- Helpers ---
def load_cfg():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def extract_to_bronze(execution_date: str, **_):
    cfg = load_cfg()
    src = cfg["source"]
    url = (
        f"{src['base_url']}?latitude={src['lat']}&longitude={src['lon']}"
        f"&timezone={src['timezone']}&hourly={src['params']['hourly']}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()  # IMPORTANT: call .json()

    y, m, d = execution_date[:4], execution_date[5:7], execution_date[8:10]
    key = f"openmeteo/y={y}/m={m}/d={d}/openmeteo_{execution_date}.json"

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
    )
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"✅ Wrote: s3a://{BRONZE_BUCKET}/{key}")

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_daily_etl",
    start_date=datetime(2025, 9, 1),
    schedule=None,                  # set to "0 6 * * *" when ready to automate
    catchup=False,
    default_args=default_args,
    tags=["bronze", "silver", "gold", "spark", "minio", "postgres", "upsert"],
    max_active_runs=1, 
) as dag:

    # --- Bronze: extract raw JSON to MinIO ---
    bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_to_bronze,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # --- Silver: transform to clean Parquet ---
    silver = DockerOperator(
        task_id="transform_silver",
        image="bitnami/spark:3.5.1",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        command=(
            "/opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark_jobs/silver_openmeteo.py --date {{ ds }}"
        ),
        mounts=[
            Mount(target="/opt/spark_jobs", source=f"{host_root}/spark_jobs", type="bind", read_only=True),
            Mount(target="/opt/bitnami/spark/conf/spark-defaults.conf",
                  source=f"{host_root}/docker/spark/spark-defaults.conf", type="bind", read_only=True),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "minio123",
            "S3_ENDPOINT": "http://minio:9000",
        },
        auto_remove=True,
        mount_tmp_dir=False,   # macOS tmp bind fix
    )

    # --- Gold: daily aggregates (Parquet) ---
    gold = DockerOperator(
        task_id="aggregate_gold",
        image="bitnami/spark:3.5.1",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        command=(
            "/opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark_jobs/gold_openmeteo.py --date {{ ds }}"
        ),
        mounts=[
            Mount(target="/opt/spark_jobs", source=f"{host_root}/spark_jobs", type="bind", read_only=True),
            Mount(target="/opt/bitnami/spark/conf/spark-defaults.conf",
                  source=f"{host_root}/docker/spark/spark-defaults.conf", type="bind", read_only=True),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "minio123",
            "S3_ENDPOINT": "http://minio:9000",
        },
        auto_remove=True,
        mount_tmp_dir=False,
    )

    # --- Stage: Spark writes Gold for {{ ds }} into Postgres stage table ---
    # (Assumes spark_jobs/load_gold_to_pg.py writes to weather_daily_stage)
    load_pg_stage = DockerOperator(
        task_id="stage_gold_to_postgres",
        image="bitnami/spark:3.5.1",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        command=(
            "/opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3 "
            "/opt/spark_jobs/load_gold_to_pg.py --date {{ ds }}"
        ),
        mounts=[
            Mount(target="/opt/spark_jobs", source=f"{host_root}/spark_jobs", type="bind", read_only=True),
            Mount(target="/opt/bitnami/spark/conf/spark-defaults.conf",
                  source=f"{host_root}/docker/spark/spark-defaults.conf", type="bind", read_only=True),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "minio123",
        },
        auto_remove=True,
        mount_tmp_dir=False,
    )

    # --- UPSERT: merge stage -> target (idempotent), then clean stage ---
    pg_upsert = DockerOperator(
    task_id="pg_upsert_from_stage",
    image="postgres:16",
    api_version="auto",
    docker_url="unix://var/run/docker.sock",
    network_mode="docker_default",
    environment={"PGPASSWORD": "airflow"},
    command=(
        'bash -lc \'set -euo pipefail; '
        'Y={{ ds[:4] }}; M={{ ds[5:7] }}; D={{ ds[8:10] }}; '
        'psql -h postgres -U airflow -d airflow -v ON_ERROR_STOP=1 '
        '-v y=$Y -v m=$M -v d=$D <<SQL\n'
        'CREATE TABLE IF NOT EXISTS weather_daily (\n'
        '  y INT, m INT, d INT,\n'
        '  min_temp_c FLOAT, max_temp_c FLOAT, avg_temp_c FLOAT,\n'
        '  precip_mm_sum FLOAT, avg_humidity_pct FLOAT,\n'
        '  PRIMARY KEY (y,m,d)\n'
        ');\n'
        'CREATE TABLE IF NOT EXISTS weather_daily_stage (\n'
        '  y INT, m INT, d INT,\n'
        '  min_temp_c FLOAT, max_temp_c FLOAT, avg_temp_c FLOAT,\n'
        '  precip_mm_sum FLOAT, avg_humidity_pct FLOAT\n'
        ');\n'
        '\\echo Staged rows for :y-:m-:d:\n'
        'SELECT COUNT(*) FROM weather_daily_stage WHERE y=:y::int AND m=:m::int AND d=:d::int;\n'
        '\n'
        '-- Deduplicate: aggregate any duplicates for this day into a single row\n'
        'WITH agg AS (\n'
        '  SELECT :y::int AS y, :m::int AS m, :d::int AS d,\n'
        '         AVG(min_temp_c)       AS min_temp_c,\n'
        '         AVG(max_temp_c)       AS max_temp_c,\n'
        '         AVG(avg_temp_c)       AS avg_temp_c,\n'
        '         AVG(precip_mm_sum)    AS precip_mm_sum,\n'
        '         AVG(avg_humidity_pct) AS avg_humidity_pct\n'
        '  FROM weather_daily_stage\n'
        '  WHERE y=:y::int AND m=:m::int AND d=:d::int\n'
        ')\n'
        'INSERT INTO weather_daily (\n'
        '  y,m,d,min_temp_c,max_temp_c,avg_temp_c,precip_mm_sum,avg_humidity_pct\n'
        ')\n'
        'SELECT y,m,d,min_temp_c,max_temp_c,avg_temp_c,precip_mm_sum,avg_humidity_pct FROM agg\n'
        'ON CONFLICT (y,m,d) DO UPDATE SET\n'
        '  min_temp_c=EXCLUDED.min_temp_c,\n'
        '  max_temp_c=EXCLUDED.max_temp_c,\n'
        '  avg_temp_c=EXCLUDED.avg_temp_c,\n'
        '  precip_mm_sum=EXCLUDED.precip_mm_sum,\n'
        '  avg_humidity_pct=EXCLUDED.avg_humidity_pct;\n'
        '\n'
        '-- Clean stage for that date so re-runs are idempotent\n'
        'DELETE FROM weather_daily_stage WHERE y=:y::int AND m=:m::int AND d=:d::int;\n'
        'SQL\''
    ),
    auto_remove=True,
    mount_tmp_dir=False,
)


bronze >> silver >> gold >> load_pg_stage >> pg_upsert
