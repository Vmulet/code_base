import pandas as pd

import sys
sys.path.append("C:/Users/ValentinMulet/Work/save/Coeus-AirflowVM/NAS/dags/scripts")

from fetch_daily_spotter import main

# Generate the array of dates
report_dates = pd.date_range(start="2025-05-20", end="2025-05-26")

input_path = 'C:/Users/ValentinMulet/Work/Code/ExtractSpotter/extracted_data/'

def convert_parquet_to_csv(parquet_file, csv_file):
    """
    Converts a Parquet file to a CSV file.

    Args:
        parquet_file (str): The path to the input Parquet file.
        csv_file (str): The path to the output CSV file.

    Returns:
        None
    """
    df = pd.read_parquet(parquet_file)
    if 'timestamp' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')
    df.to_csv(csv_file)

def convert_csv_to_parquet(csv_file, parquet_file):
    """
    Converts a CSV file to a Parquet file.

    Args:
        csv_file (str): The path to the input CSV file.
        parquet_file (str): The path to the output Parquet file.

    Returns:
        None
    """
    df = pd.read_csv(csv_file)
    # Uncomment the following line if the first column needs to be dropped
    # df = df.drop(df.columns[0], axis=1)
    df.to_parquet(parquet_file, engine='pyarrow')

# Process each date in the report_dates range
for report_date in report_dates:
    """
    For each date in the report_dates range:
    - Generates the Parquet file name based on the date.
    - Calls the `main` function from `fetch_daily_spotter` to fetch and process data.
    - Converts the generated Parquet file to a CSV file.

    Args:
        None (uses global variables `report_dates` and `input_path`).

    Returns:
        None
    """
    parquet_name = input_path + f'SPOTTER-{report_date.strftime("%Y-%m-%d")}.parquet.gzip'
    print(report_date)
    main(report_date.strftime('%Y-%m-%d'), parquet_name)
    convert_parquet_to_csv(parquet_name, parquet_name.replace(".parquet.gzip", ".csv"))