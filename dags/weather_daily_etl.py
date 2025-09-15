# dags/weather_daily_etl.py
from datetime import datetime, timedelta
import os, json, requests, yaml, boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

CONFIG_PATH = "/opt/airflow/config/settings.yaml"
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS   = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET   = os.getenv("S3_SECRET_KEY", "minio123")
BRONZE_BUCKET = "bronze"

def load_cfg():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def extract_to_bronze(execution_date: str, **_):
    cfg = load_cfg(); src = cfg["source"]
    url = (f"{src['base_url']}?latitude={src['lat']}&longitude={src['lon']}"
           f"&timezone={src['timezone']}&hourly={src['params']['hourly']}")
    r = requests.get(url, timeout=30); r.raise_for_status()
    payload = r.json()
    y, m, d = execution_date[:4], execution_date[5:7], execution_date[8:10]
    key = f"openmeteo/y={y}/m={m}/d={d}/openmeteo_{execution_date}.json"
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                      aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET,
                      region_name="us-east-1")
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key,
                  Body=json.dumps(payload).encode("utf-8"),
                  ContentType="application/json")
    print(f"✅ Wrote: s3a://{BRONZE_BUCKET}/{key}")

default_args = {"owner":"you","retries":1,"retry_delay":timedelta(minutes=2)}

host_root = "/Users/akhilavallabhaneni/dev_code/pyspark-airflow-weather-etl"  # <— update if your path differs

with DAG(
    dag_id="weather_daily_etl",
    start_date=datetime(2025, 9, 1),
    schedule="0 6 * * *",      # run daily at 06:00 (server time)
    catchup=False,
    default_args=default_args,
    tags=["bronze","silver","gold","spark","minio"],
) as dag:

    bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_to_bronze,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

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
        environment={"AWS_ACCESS_KEY_ID":"minio","AWS_SECRET_ACCESS_KEY":"minio123","S3_ENDPOINT":"http://minio:9000"},
        auto_remove=True,
    )

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
        environment={"AWS_ACCESS_KEY_ID":"minio","AWS_SECRET_ACCESS_KEY":"minio123","S3_ENDPOINT":"http://minio:9000"},
        auto_remove=True,
    )

    bronze >> silver >> gold
