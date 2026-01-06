from airflow.models import DagBag

def test_dag1_loaded():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert "csv_to_postgres_ingestion" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag1_task_count():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.dags["csv_to_postgres_ingestion"]
    assert len(dag.tasks) == 3

def test_dag1_dependencies():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.dags["csv_to_postgres_ingestion"]

    create = dag.get_task("create_table")
    truncate = dag.get_task("truncate_table")
    load = dag.get_task("load_csv")

    assert truncate in create.downstream_list
    assert load in truncate.downstream_list

def test_dag1_no_cycles():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.dags["csv_to_postgres_ingestion"]

    # If there is a cycle, Airflow raises an exception during DAG parsing
    assert dag is not None
    assert len(dag.task_dict) == 3

def test_dag1_schedule():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.dags["csv_to_postgres_ingestion"]
    assert dag.schedule_interval == "@daily"
