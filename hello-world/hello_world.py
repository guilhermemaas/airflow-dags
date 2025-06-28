from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello, Airflow!")

with DAG(
    dag_id="gmaas_hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["exemplo"]
) as dag:
    t1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello
    )
