import pandas as pd
import os

def convert_parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file)
    if 'timestamp' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')
    df.to_csv(csv_file)


def find_files_substring_in_filename(directory, substring, whitelistFile):
    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            if substring in file_name and any(
                file in file_name for file in whitelistFile
            ):
                matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files

def main(path):
    paths = find_files_substring_in_filename(path, "SPOTTER", ["parquet"])
    for path in paths:
        data = pd.read_parquet(path)
        if "timestamp" in data:
            data["acquisition_time"] = data["timestamp"]
            data = data.drop(columns="timestamp")
        data.to_parquet(path)
convert_parquet_to_csv("C:/Users/ValentinMulet/Work/Code/SPOTTER/SPOTTER-2025-05-17.parquet.gzip", "C:/Users/ValentinMulet/Work/Code/SPOTTER/SPOTTER1-2025-05-17.csv")
main("C:/Users/ValentinMulet/Work/Code/SPOTTER/")
convert_parquet_to_csv("C:/Users/ValentinMulet/Work/Code/SPOTTER/SPOTTER-2025-05-17.parquet.gzip", "C:/Users/ValentinMulet/Work/Code/SPOTTER/SPOTTER2-2025-05-17.csv")
