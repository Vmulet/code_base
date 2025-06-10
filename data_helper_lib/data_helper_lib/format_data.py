import pandas as pd
from file_manipulation import find_files_substring_in_filename, get_week_dates
#TO FORMAT DATA: -----------------------------------------------------------------------------------------------
def filterAveragePerPeriod(data, period, dataColums=[-1]):
    """Sort, format and merge values by calculating the average value for a given period."""
    if data.empty:
        return data
    filtered_timestamp = []
    averaged_data = [[] for col in dataColums]
    formatTimestamp = "%Y-%m-%d %H:%M:%S"
    if period < 1:
        formatTimestamp = "%X.%f"
    data["acquisition_time"] = pd.to_datetime(data["acquisition_time"])
    data = data.sort_values(by="acquisition_time")
    if period == 0:
        return data
    currentAverage = [0 for column in dataColums]
    for time in data["acquisition_time"]:
        currentTimestamp = time
        break
    diviser = 0
    for index, time in enumerate(data["acquisition_time"]):
        diviser += 1
        for tmp, indexColumn in enumerate(dataColums):
            currentAverage[tmp] += float(data[data.columns[indexColumn]].values[index])
        if (time - currentTimestamp).total_seconds() >= period:
            filtered_timestamp.append(currentTimestamp.strftime(formatTimestamp))
            currentTimestamp = time
            for index, average in enumerate(currentAverage):
                averaged_data[index].append(average / diviser)
            currentAverage = [0 for column in dataColums]
            diviser = 0
    filtered_timestamp.append(currentTimestamp.strftime(formatTimestamp))
    for index, average in enumerate(currentAverage):
        averaged_data[index].append(average / diviser)
    newData = {"acquisition_time": filtered_timestamp}
    for index, average in enumerate(averaged_data):
        newData[data.columns[dataColums[index]]] = average
    return pd.DataFrame(newData)

def filter_data(data):
    """Filter data to include only rows where seconds are divisible by 30."""
    data["acquisition_time"] = pd.to_datetime(data["acquisition_time"])
    data = data.sort_values(by="acquisition_time")
    filter = data["acquisition_time"].dt.second % 30 == 0
    filtered_data = data[filter].copy()
    filtered_data["acquisition_time"] = filtered_data["acquisition_time"].dt.strftime(
        "%X.%f"
    )
    return filtered_data

def drop_non_numeric_columns(df):
    cols_to_drop = []
    for col in df.columns:
        if df[col].dtype == bool:
            cols_to_drop.append(col)
            continue
        try:
            pd.to_numeric(df[col], errors='raise')
        except Exception:
            cols_to_drop.append(col)
    return df.drop(columns=cols_to_drop)

def syncData(dataArray):
    syncedArray = []
    common_time_index = (
        pd.concat([dataframe["acquisition_time"] for dataframe in dataArray])
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    for data in dataArray:
        data = data.set_index("acquisition_time").reindex(common_time_index)
        data = data.apply(pd.to_numeric, errors="coerce").interpolate().reset_index()
        syncedArray.append(data)
    return syncedArray

def packData(files, data = pd.DataFrame()):
    for file in files:
        if 'parquet' in file:
            data_read = pd.read_parquet(file, engine='pyarrow')
        if 'csv' in file:
            data_read = pd.read_csv(file)
            if 'EPEVER' in file:
                data_read = decodeEPEVER(data_read)
        data = pd.concat([data, data_read], ignore_index=True)
    return filter_low_count_days(data)

def filter_low_count_days(df, column='acquisition_time', threshold=100):
    if not df.empty and column in df.columns:
        df[column] = pd.to_datetime(df[column])
        df['date'] = df[column].dt.date
        counts = df['date'].value_counts()
        valid_dates = counts[counts >= threshold].index
        return df[df['date'].isin(valid_dates)].drop(columns=['date'])
    else:
        return df
        

def extract_period_data(input_path, period, year, month = True):
    if month:
        if period < 10:
            substring = '{}-0{}'.format(year, period)
        else:
            substring = '{}-{}'.format(year, period)
    else:
        start, end =   get_week_dates(year, period)
        start_month = start 
    paths = find_files_substring_in_filename(input_path, substring, ["EPEVER"])
    dataEPEVER = packData(paths)
    paths = find_files_substring_in_filename(input_path, substring, ["WIND_TURBINE"])
    pass    

def combine_hl_columns(df):
    bases = set()
    for col in df.columns:
        if col.endswith(("_l", "_h")):
            base = col[:-2]
            bases.add(base)

    for base in bases:
        l_col = f"{base}_l"
        h_col = f"{base}_h"

        if l_col in df.columns and h_col in df.columns:
            # Combine and scale
            df[base] = df.apply(lambda row: (row[h_col] << 16 | row[l_col]), axis=1)
            df = df.drop([l_col, h_col], axis=1)
        else:
            print(f"Warning: Missing pair for {base}")

    return df

def combine_hl_float_values(df, shift_factor=16):
    bases = set()
    for col in df.columns:
        if col.endswith(("_l", "_h")):
            base = col[:-2]
            bases.add(base)

    for base in bases:
        l_col = f"{base}_l"
        h_col = f"{base}_h"

        if l_col in df.columns and h_col in df.columns:
            df[base] = df.apply(lambda row: row[h_col] * (2 ** shift_factor) + row[l_col], axis=1)
            df = df.drop([l_col, h_col], axis=1)
        else:
            print(f"Warning: Missing pair for {base}")
    return df

def decodeEPEVER(EPEVER_df):
    # Data Ratio
    for col in EPEVER_df.columns:
        if col != 'record_time' and EPEVER_df[col].dtype == 'object':  # Ensures column contains strings
            EPEVER_df[col] = EPEVER_df[col].apply(lambda x: int(x, 16) if isinstance(x, str) and all(c in "0123456789abcdefABCDEF" for c in x)else x)
    EPEVER_df = EPEVER_df.drop(EPEVER_df[EPEVER_df['device_down'] == 1].index)
    EPEVER_df = EPEVER_df[~EPEVER_df.apply(lambda row: row.astype(str).str.contains('TO').any(), axis=1)]
    EPEVER_df = combine_hl_columns(EPEVER_df)
    scale_100 = [
        "pv_array_input_voltage",
        "pv_array_input_current",
        "pv_array_input_power",
        "load_voltage",
        "load_current",
        "load_power",
        "battery_temperature",
        "device_temperature",
        "battery_soc",
        "consumed_energy_this_month",
        "consumed_energy_this_year",
        "total_consumed_energy",
        "generated_energy_today",
        "generated_energy_this_month",
        "generated_energy_this_year",
        "total_generated_energy",
        "consumed_energy_today",
    ]

    scale_10 = ["maximum_battery_voltage_today", "minimum_battery_voltage_today"]
    EPEVER_df[scale_100] = EPEVER_df[scale_100] / 100
    EPEVER_df[scale_10] = EPEVER_df[scale_10] / 10