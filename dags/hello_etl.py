from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("Airflow is alive ✅")

with DAG(
    dag_id="hello_etl",
    start_date=datetime(2025, 9, 1),
    schedule=None,   # run on-demand
    catchup=False,
    tags=["smoke"],
) as dag:
    PythonOperator(task_id="hello", python_callable=say_hello)
