import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def find_files_substring_in_filename(directory, substring):
    """Find all file in a directory that contains the substring."""
    matching_files = []
    ignore_files = [
        "HCHDM",
        "WIXDR",
        "GNZDA",
        "BMS",
        "TEMP_PDU",
        "TEMP_CELL",
    ]
    skip_dirs = ["SPOTTER"]
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in skip_dirs]

        for file_name in files:
            if substring in file_name and not any(
                ignore in file_name for ignore in ignore_files
            ):
                matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files


def filter_dataframe(dataframe, table_name, time_column, engine):
    dataframe[time_column] = pd.to_datetime(dataframe[time_column])
    query = f'SELECT {time_column} FROM "{table_name}";'
    existing_times = pd.read_sql(query, engine)
    existing_times[time_column] = pd.to_datetime(existing_times[time_column])
    new_data = dataframe[~dataframe[time_column].isin(existing_times[time_column])]
    return new_data

def quaternion_to_yaw(q0, q1, q2, q3):
    """Convert quaternion to yaw (heading) in degrees."""
    yaw = np.arctan2(2 * (q0 * q3 + q1 * q2), 1 - 2 * (q2**2 + q3**2))
    return np.degrees(yaw)

def append_last_data_to_table(path, tableName):
    if os.path.exists(path):
        df = pd.read_parquet(path)
        if "PSONCMS" in path:
            df_transformed = df[['record_time', 'acquisition_time']].copy()
            df_transformed['heading'] = df.apply(lambda row: quaternion_to_yaw(row['q0'], row['q1'], row['q2'], row['q3']), axis=1)
            df = df_transformed
        engine = create_engine(
            "postgresql://userdata:vi7FA:&5z7@192.168.1.125:5432/ocg_data"
        )
        df_filtered = filter_dataframe(df, tableName, "acquisition_time", engine)
        df_filtered.to_sql(tableName, engine, if_exists="append", index=False)


def main(dateData, input_path):
    for path in find_files_substring_in_filename(input_path, dateData):
        append_last_data_to_table(path, path.split(input_path)[-1].split("-")[0])
