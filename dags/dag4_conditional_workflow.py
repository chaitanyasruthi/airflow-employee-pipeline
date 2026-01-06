from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def choose_branch(execution_date, **context):
        day = execution_date.weekday()
        if day <= 2:
            return "weekday_task"
        elif day <= 4:
            return "end_of_week_task"
        else:
            return "weekend_task"

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="branch_by_day",
        python_callable=choose_branch
    )

    weekday_task = PythonOperator(
        task_id="weekday_task",
        python_callable=lambda: {"type": "weekday"}
    )

    weekday_summary = EmptyOperator(task_id="weekday_summary")

    end_of_week_task = PythonOperator(
        task_id="end_of_week_task",
        python_callable=lambda: {"type": "end_of_week"}
    )

    end_of_week_report = EmptyOperator(task_id="end_of_week_report")

    weekend_task = PythonOperator(
        task_id="weekend_task",
        python_callable=lambda: {"type": "weekend"}
    )

    weekend_cleanup = EmptyOperator(task_id="weekend_cleanup")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    start >> branch

    branch >> weekday_task >> weekday_summary >> end
    branch >> end_of_week_task >> end_of_week_report >> end
    branch >> weekend_task >> weekend_cleanup >> end
