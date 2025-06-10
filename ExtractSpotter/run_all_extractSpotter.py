from fetch_daily_spotter import main
import pandas as pd

# Generate the array of dates
report_dates = pd.date_range(start="2025-05-20", end="2025-05-26")

input_path = 'C:/Users/ValentinMulet/Work/Code/ExtractSpotter/extracted_data/'
def convert_parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file)
    if 'timestamp' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')
    df.to_csv(csv_file)

def convert_csv_to_parquet(csv_file, parquet_file):
    df = pd.read_csv(csv_file)
    #df = df.drop(df.columns[0], axis=1)
    df.to_parquet(parquet_file, engine='pyarrow')

for report_date in report_dates:
    parquet_name = input_path + f'SPOTTER-{report_date.strftime("%Y-%m-%d")}.parquet.gzip'
    print(report_date)
    main(report_date.strftime('%Y-%m-%d'), parquet_name)
    convert_parquet_to_csv(parquet_name, parquet_name.replace(".parquet.gzip", ".csv"))