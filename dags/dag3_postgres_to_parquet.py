from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

OUTPUT_DIR = "/opt/airflow/output"

with DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    def check_table():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        records = hook.get_first(
            "SELECT COUNT(*) FROM transformed_employee_data"
        )
        if records[0] == 0:
            raise ValueError("No data found in transformed_employee_data")
        return True

    def export_to_parquet(ds):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_sql(
            "SELECT * FROM transformed_employee_data",
            engine
        )

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/employee_data_{ds}.parquet"

        df.to_parquet(
            file_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        return {
            "file_path": file_path,
            "row_count": len(df),
            "file_size_bytes": os.path.getsize(file_path)
        }

    def validate_parquet(ds):
        file_path = f"{OUTPUT_DIR}/employee_data_{ds}.parquet"
        df = pd.read_parquet(file_path)
        if df.empty:
            raise ValueError("Parquet file is empty")
        return True

    check_task = PythonOperator(
        task_id="check_source_table",
        python_callable=check_table
    )

    export_task = PythonOperator(
        task_id="export_to_parquet",
        python_callable=export_to_parquet
    )

    validate_task = PythonOperator(
        task_id="validate_parquet",
        python_callable=validate_parquet
    )

    check_task >> export_task >> validate_task
