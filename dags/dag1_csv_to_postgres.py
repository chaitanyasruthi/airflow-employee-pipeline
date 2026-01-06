from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

default_args = {"owner": "airflow"}

with DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    def create_table():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run("""
            CREATE TABLE IF NOT EXISTS raw_employee_data (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                age INT,
                city VARCHAR(50),
                salary FLOAT,
                join_date DATE
            );
        """)

    def truncate_table():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run("TRUNCATE TABLE raw_employee_data;")

    def load_csv():
        df = pd.read_csv("/opt/airflow/data/input.csv")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()
        df.to_sql("raw_employee_data", engine, if_exists="append", index=False)
        return len(df)

    create = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    truncate = PythonOperator(
        task_id="truncate_table",
        python_callable=truncate_table
    )

    load = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv
    )

    create >> truncate >> load
