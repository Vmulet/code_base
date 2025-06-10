from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.fetch_daily_spotter import main
import os


# Define Paths
OUTPUT_PATH = "/mnt/NAS-THELOCKER/tftp_root_15min/SPOTTER/"


# Task 1 extract daily spotter data
def extract_daily_spotter_data(**context):

    execution_date = context["data_interval_start"]
    data_date = execution_date.strftime("%Y-%m-%d")

    # Construct the output folder path with yesterday's date
    output_file = os.path.join(OUTPUT_PATH, f"SPOTTER-{data_date}.parquet.gzip")

    main(data_date, output_file)
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
    "Daily_Spotter_Data",
    default_args=default_args,
    schedule_interval="5,20,35,50 * * * *",
    tags=["daily", "3rd-party"],
    catchup=True,
    max_active_runs=1,
) as dag:

    # Task to list new files
    extract_daily_spotter = PythonOperator(
        task_id="extract_daily_spotter_data",
        python_callable=extract_daily_spotter_data,
        provide_context=True,
    )

    extract_daily_spotter
