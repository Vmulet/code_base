from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.appendToDatabase import main


# Define Paths
NAS_PATH = "/mnt/NAS-THELOCKER/tftp_root_15min/"


# Task 1 extract daily spotter data
def append_live_data_database(**context):
    execution_date = context["data_interval_start"]
    print(execution_date)
    data_date = (execution_date).strftime("%Y-%m-%d")
    print(data_date)
    main(data_date, NAS_PATH)
    return


# Default arguments for the DAG
default_args = {
    "owner": "Valentin Mulet",
    "start_date": days_ago(2),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "Append_live_data_database",
    default_args=default_args,
    schedule_interval="5,20,35,50 * * * *",
    tags=["monitoring", "15min"],
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task to list new files
    push_live_data_database = PythonOperator(
        task_id="append_live_data_database",
        python_callable=append_live_data_database,
        provide_context=True,
    )

    push_live_data_database
