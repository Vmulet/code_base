import pandas as pd
from sqlalchemy import create_engine, text
import os
import numpy as np
from datetime import datetime, timedelta
YEARLY_DATABASE_NAME = "ocg_data_year"
DAILY_DATABASE_NAME = "ocg_data"

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
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            if substring in file_name and not any(
                ignore in file_name for ignore in ignore_files
            ):
                matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files

def quaternion_to_yaw(q0, q1, q2, q3):
    """Convert quaternion to yaw (heading) in degrees."""
    yaw = np.arctan2(2 * (q0 * q3 + q1 * q2), 1 - 2 * (q2**2 + q3**2))
    return np.degrees(yaw)

def filter_average_per_period(data, period):
    """
    Computes average values over a specified time period.

    Args:
        data (pd.DataFrame): Input DataFrame with 'acquisition_time' as datetime column.
        period (int): Time period in seconds for averaging.

    Returns:
        pd.DataFrame: Averaged numeric data with formatted timestamps.
    """
    data = data.copy()
    data['acquisition_time'] = pd.to_datetime(data['acquisition_time'])
    data = data.sort_values(by='acquisition_time')

    # Select only numeric columns
    numeric_columns = data.select_dtypes(include='number').columns.tolist()

    # Format timestamp
    timestamp_format = '%Y-%m-%d %H:%M:%S' if period < 60 else '%Y-%m-%d %H:%M'

    timestamps = []
    averages = [[] for _ in numeric_columns]
    accumulators = [0.0] * len(numeric_columns)
    count = 0
    current_time = data['acquisition_time'].iloc[0]

    for i, timestamp in enumerate(data['acquisition_time']):
        count += 1
        row = data.iloc[i]
        for j, col_name in enumerate(numeric_columns):
            accumulators[j] += float(row[col_name])

        if (timestamp - current_time).total_seconds() >= period:
            timestamps.append(current_time.strftime(timestamp_format))
            for j, total in enumerate(accumulators):
                averages[j].append(total / count)
            current_time = timestamp
            accumulators = [0.0] * len(numeric_columns)
            count = 0

    # Add remaining data
    timestamps.append(current_time.strftime(timestamp_format))
    for j, total in enumerate(accumulators):
        averages[j].append(total / count if count else None)

    # Construct output DataFrame
    result = {'acquisition_time': timestamps}
    for j, col_name in enumerate(numeric_columns):
        result[col_name] = averages[j]

    return pd.DataFrame(result)


def createTableDaily(path, tableName):
    if os.path.exists(path):
        df = pd.read_parquet(path)
        if "PSONCMS" in path:
            df_transformed = df[['record_time', 'acquisition_time']].copy()
            df_transformed['heading'] = df.apply(lambda row: quaternion_to_yaw(row['q0'], row['q1'], row['q2'], row['q3']), axis=1)
            df = df_transformed
        engine = create_engine(
            f"postgresql://userdata:vi7FA:&5z7@192.168.1.125:5432/{DAILY_DATABASE_NAME}"
        )
        df.to_sql(tableName, engine, if_exists="replace", index=False)

def delete_old_data(engine, table_name, time_column="acquisition_time"):

    query = f'SELECT * FROM "{table_name}"'
    df = pd.read_sql(query, engine)

    df[time_column] = pd.to_datetime(df[time_column])
    old_rows = df[df[time_column] <= datetime.now() - timedelta(days=365)]

    with engine.begin() as conn:
        for timestamp in old_rows[time_column]:
            delete_query = text(f'DELETE FROM "{table_name}" WHERE "{time_column}" = :timestamp')
            conn.execute(delete_query, {"timestamp": timestamp})

def update_yearly_data(path, table_name, time_column="acquisition_time", delete_old_data = True):
    engine = create_engine(
        f"postgresql://userdata:vi7FA:&5z7@192.168.1.125:5432/{YEARLY_DATABASE_NAME}"
    )
    delete_old_data(engine, table_name)
    if os.path.exists(path):
        df = pd.read_parquet(path)
        df = filter_average_per_period(df, 60)
        df.to_sql(table_name, engine, if_exists="append", index=False)

def backfill_yearly_data(input_path):
    start_date = datetime.today().date() - timedelta(days=365)
    end_date = datetime.today().date()
    date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]
    for date in date_list:
        paths = find_files_substring_in_filename(input_path, date)
        for path in paths:
            table_name = path.split(input_path)[-1].split("-")[0]
            update_yearly_data(path, table_name, delete_old_data = False)

def main(dateData, input_path, backfill = False):
    if backfill:
        backfill_yearly_data(input_path)
        return
    paths = find_files_substring_in_filename(input_path, dateData)
    for path in paths:
        table_name = path.split(input_path)[-1].split("-")[0]
        createTableDaily(path, table_name)
        update_yearly_data(path, table_name)
