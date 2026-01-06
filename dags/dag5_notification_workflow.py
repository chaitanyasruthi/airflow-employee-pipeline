from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def success_callback(context):
        return {
            "status": "success",
            "task": context["task_instance"].task_id
        }

    def failure_callback(context):
        return {
            "status": "failure",
            "task": context["task_instance"].task_id,
            "error": str(context.get("exception"))
        }

    def risky_task(execution_date, **context):
        if execution_date.day % 5 == 0:
            raise ValueError("Simulated failure")
        return {"status": "ok"}

    def cleanup():
        return {"cleanup": "done"}

    start = EmptyOperator(task_id="start")

    risky = PythonOperator(
        task_id="risky_operation",
        python_callable=risky_task,
        on_success_callback=success_callback,
        on_failure_callback=failure_callback
    )

    success = EmptyOperator(
        task_id="success_notification",
        trigger_rule="all_success"
    )

    failure = EmptyOperator(
        task_id="failure_notification",
        trigger_rule="all_failed"
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        trigger_rule="all_done"
    )

    start >> risky >> [success, failure]
    [success, failure] >> cleanup_task
