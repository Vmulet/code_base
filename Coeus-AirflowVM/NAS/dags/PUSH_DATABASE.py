from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.pushDatabaseForMonitoring import main


# Define Paths
NAS_PATH = "/mnt/NAS-THELOCKER/tftp_root_15min/"


# Task 1 extract daily spotter data
def push_daily_data_database(**context):

    execution_date = context["data_interval_start"]
    data_date = execution_date.strftime("%Y-%m-%d")

    main(data_date, NAS_PATH)
    return


# Default arguments for the DAG
default_args = {
    "owner": "Valentin Mulet",
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "Push_data_database",
    default_args=default_args,
    schedule_interval="00 1 * * *",
    tags=["monitoring"],
    catchup=True,
    max_active_runs=1,
) as dag:

    # Task to list new files
    push_data_database = PythonOperator(
        task_id="push_daily_data_database",
        python_callable=push_daily_data_database,
        provide_context=True,
    )

    push_data_database
