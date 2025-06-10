import pandas as pd
import os

def convert_parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file)
    if 'timestamp' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')
    df.to_csv(csv_file)

def convert_csv_to_parquet(csv_file, parquet_file):
    df = pd.read_csv(csv_file)
    df = df.drop(df.columns[0], axis=1)
    df.to_parquet(parquet_file, engine='pyarrow')

#convert_csv_to_parquet('test.csv', 'test2.parquet.gzip' )
#convert_parquet_to_csv('test.parquet.gzip', 'test.csv' )