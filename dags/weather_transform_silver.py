from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="weather_transform_silver",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["silver", "spark", "docker"],
) as dag:

    # absolute path to your repo on the host (adjust if your path differs)
    host_root = "/Users/akhilavallabhaneni/dev_code/pyspark-airflow-weather-etl"

    spark_silver = DockerOperator(
        task_id="spark_silver_transform",
        image="bitnami/spark:3.5.1",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",  # << join the same network as your spark & worker
        command=(
            "/opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark_jobs/silver_openmeteo.py --date {{ ds }}"
        ),
        mounts=[
            Mount(target="/opt/spark_jobs", source=f"{host_root}/spark_jobs", type="bind", read_only=True),
            Mount(
                target="/opt/bitnami/spark/conf/spark-defaults.conf",
                source=f"{host_root}/docker/spark/spark-defaults.conf",
                type="bind", read_only=True
            ),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "minio123",
            "S3_ENDPOINT": "http://minio:9000",
        },
        auto_remove=True,
    )
