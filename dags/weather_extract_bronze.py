from datetime import datetime, timedelta
import os, json, requests, yaml
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

CONFIG_PATH = "/opt/airflow/config/settings.yaml"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS   = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET   = os.getenv("S3_SECRET_KEY", "minio123")
BRONZE_BUCKET = "bronze"

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
    payload = r.json()

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
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_extract_bronze",
    start_date=datetime(2025, 9, 1),
    schedule=None,   # run on-demand for now
    catchup=False,
    default_args=default_args,
    tags=["extract", "bronze", "minio", "open-meteo"],
) as dag:
    PythonOperator(
        task_id="extract_to_bronze",
        python_callable=extract_to_bronze,
        op_kwargs={"execution_date": "{{ ds }}"},
    )