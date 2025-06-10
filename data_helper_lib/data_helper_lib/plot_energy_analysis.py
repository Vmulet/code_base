import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import os
from get_data import get_energy_data_from_csv, get_turbines_data_weekly, get_epever_data, get_epever_data_weekly, get_turbines_data
from format_data import combine_hl_float_values, drop_non_numeric_columns
from pdf_creation_handling import new_pdf_page, add_text_to_fig
from plot import save_figure, plot_bar_chart

#MAIN CODE: ----------------------------------------------------------------------------------------------------

def create_empty_df(columns):
    return pd.DataFrame(columns=columns).astype(float)

def calculate_energy_generated(epever1, epever2, epever3, wind1, wind2, wind3):
    # Handling empty wind DataFrames
    wind_column = ["acquisition_time", "record_time","errcode","sysstatus","daynight","sbatvol","sbatcurr","ssolarvol","ssolarcurr","swindvol","swindcurr","soutcurr","temp1","temp2","qpercent","sumkwl","sumkwh","sumwl","sumwh","windrpm","batnvol","floatvol","finalvol","finalrvol","fullvol","fullrvol","norcap","handbrake","windvmax","windamax","windrmax","windpole","windcutinv","windabrkdly","windratedp","windratedv","mpptmode","wind_power_generated","wind_power_battery"]
    if wind1.empty:
        wind1 = create_empty_df(wind_column)
    else:
        wind1["wind_power_generated"] = wind1["swindvol"] * wind1["swindcurr"]
        wind1["wind_power_battery"] = wind1["sbatvol"] * wind1["sbatcurr"]

    if wind2.empty:
        wind2 = create_empty_df(wind_column)
    else:
        wind2["wind_power_generated"] = wind2["windvol"] * wind2["windcurr"]
        wind2["wind_power_battery"] = wind2["batvol"] * wind2["batcurr"]

    if wind3.empty:
        wind3 = create_empty_df(wind_column)
    else:
        wind3["wind_power_generated"] = wind3["windvol"] * wind3["windcurr"]
        wind3["wind_power_battery"] = wind3["batvol"] * wind3["batcurr"]

    # Handling empty solar DataFrames
    for i, epever in enumerate([epever1, epever2, epever3]):
        if epever.empty:
            epever_columns = ["acquisition_time","record_time","RTU_register","RTU_address","RTU_data","pv_array_input_voltage","pv_array_input_current","load_voltage","load_current","battery_temperature","device_temperature","battery_SOC","maximum_battery_voltage_today","minimum_battery_voltage_today","battery_power","generated_energy_this_month","consumed_energy_this_month","load_power","consumed_energy_this_year","pv_array_input_power","generated_energy_today","generated_energy_this_year","total_consumed_energy","total_generated_energy","consumed_energy_today"]
            if i == 0:
                epever1 = create_empty_df(epever_columns)
            elif i == 1:
                epever2 = create_empty_df(epever_columns)
            elif i == 2:
                epever3 = create_empty_df(epever_columns)

    # Convert acquisition_time to datetime
    for df in [epever1, epever2, epever3, wind1, wind2, wind3]:
        df["acquisition_time"] = pd.to_datetime(df["acquisition_time"], errors="coerce")



    # Resample data to weekly and reset index
    for i, df in enumerate([epever1, epever2, epever3, wind1, wind2, wind3]):
        if not df.empty and "acquisition_time" in df.columns:
            if i == 0:
                epever1 = df.resample('ME', on="acquisition_time").mean().reset_index()
                epever1 = combine_hl_float_values(epever1)
            if i == 1:
                epever2 = df.resample('ME', on="acquisition_time").mean().reset_index()
                epever2 = combine_hl_float_values(epever2)
            if i == 2:
                epever3 = df.resample('ME', on="acquisition_time").mean().reset_index()
                epever3 = combine_hl_float_values(epever3)
            if i == 3:
                wind1 = df.resample('ME', on="acquisition_time").mean().reset_index()
            if i == 4:
                wind2 = df.resample('ME', on="acquisition_time").mean().reset_index()
            if i == 5:
                wind3 = df.resample('ME', on="acquisition_time").mean().reset_index()
    
    # Rename columns
    wind1 = wind1.rename(columns=lambda x: x + "_wind1" if x != "acquisition_time" else x)
    wind2 = wind2.rename(columns=lambda x: x + "_wind2" if x != "acquisition_time" else x)
    wind3 = wind3.rename(columns=lambda x: x + "_wind3" if x != "acquisition_time" else x)
    epever1 = epever1.rename(columns=lambda x: x + "_solar1" if x != "acquisition_time" else x)
    epever2 = epever2.rename(columns=lambda x: x + "_solar2" if x != "acquisition_time" else x)
    epever3 = epever3.rename(columns=lambda x: x + "_solar3" if x != "acquisition_time" else x)

    # Merge all DataFrames, ensuring missing values are filled with 0
    merged_df = wind1.merge(wind2, on="acquisition_time", how="outer") \
                     .merge(wind3, on="acquisition_time", how="outer") \
                     .merge(epever1, on="acquisition_time", how="outer") \
                     .merge(epever2, on="acquisition_time", how="outer") \
                     .merge(epever3, on="acquisition_time", how="outer") \
                     .fillna(0)  # Fill missing values with 0
    
    # Compute total power values
    merged_df["total_wind_power"] = merged_df["wind_power_generated_wind1"] + merged_df["wind_power_generated_wind2"] + merged_df["wind_power_generated_wind3"]
    merged_df["total_solar_power"] = merged_df["pv_array_input_power_solar1"] + merged_df["pv_array_input_power_solar2"] + merged_df["pv_array_input_power_solar3"]

    return merged_df
    
def generate_csv_energy_data_weekly(year, input_path, file_name, week = []):
    wind1, wind2, wind3 = get_turbines_data_weekly(year, input_path, week)
    epever1, epever2, epever3 = get_epever_data_weekly(year, input_path, week)
    data = calculate_energy_generated(epever1, epever2, epever3, wind1, wind2, wind3)
    data.to_csv(file_name)

def merge_csv_list(file_list, on_column, how="inner"):
    if not file_list:
        raise ValueError("The list of CSV files cannot be empty.")
    merged_df = pd.read_csv(file_list[0])
    for file in file_list[1:]:
        df = pd.read_csv(file)
        merged_df = pd.merge(merged_df, df, on=on_column, how=how)
    return merged_df

import glob
def merge_csv(csv_name):
    # Get list of all CSV files in a directory
    #csv_files = glob.glob("path/to/your/csv/folder/*.csv")  # Replace with your actual directory
    csv_files = [f"{i}.csv" for i in range(1, 15)]
    # Read and merge all CSV files
    df_list = [pd.read_csv(file) for file in csv_files]
    merged_df = pd.concat(df_list, axis=0, join="outer")
    # Fill missing values with 0
    #merged_df.fillna(0, inplace=True)
    # Save the merged dataset
    merged_df.to_csv(csv_name, index=False)


def generate_csv_energy_data(input_path, file_name, start_date, end_date):
    wind1, wind2, wind3 = get_turbines_data(input_path, start_date, end_date)
    epever1, epever2, epever3 = get_epever_data(input_path, start_date, end_date)
    
    wind1 = drop_non_numeric_columns(wind1)
    epever1 = drop_non_numeric_columns(epever1)
    wind2 = drop_non_numeric_columns(wind2)
    epever2 = drop_non_numeric_columns(epever2)
    wind3 = drop_non_numeric_columns(wind3)
    epever3 = drop_non_numeric_columns(epever3)

    data = calculate_energy_generated(epever1, epever2, epever3, wind1, wind2, wind3)
    data.to_csv(file_name)

def plot_in_pdf(pdf, data, data_column):
    if "total_solar_power" == data_column:
        title = "Monthly solar power generated"
    if "total_wind_power" == data_column:
        title = "Monthly wind power generated"
    if "total_power_generated" == data_column:
        title = "Monthly total power generated"
    fig = new_pdf_page()
    add_text_to_fig(data_column, fig, 20)
    plot_bar_chart(data, fig, 111, data_column, title, data_column=data_column, color="tab:blue")
    save_figure(fig, pdf)

def plot_energy_data(data, pdf_name):
    #olumn_to_plot = ["total_solar_power", "total_wind_power", "wind_power_battery_wind1", "wind_power_generated_wind1", "wind_power_battery_wind2", "wind_power_generated_wind2", "wind_power_battery_wind3", "wind_power_generated_wind3"]
    column_to_plot = ["total_power_generated", "total_solar_power", "total_wind_power"]
    #column_to_plot1 = ["solarvol_wind3", "wind_chg_pwr_wind3", "bat_chg_pwr_wind3" , "wind_power_generated_wind3", "wind_power_battery_wind3"]
    #column_to_plot2 = ["load_voltage_solar1", "battery_power_solar1", "generated_energy_today_solar1", "total_generated_energy_solar1"]
    #column_to_plot = ["pv_array_input_power_solar1", "pv_array_input_power_solar2", "pv_array_input_power_solar3", "wind_power_generated_wind1", "wind_power_generated_wind2", "wind_power_generated_wind3"]
    with PdfPages(pdf_name) as pdf:
        for col in column_to_plot:
            plot_in_pdf(pdf, data, col)

def count_down_devices(data):
    data["wind_zero_count"] = (data[["wind_power_generated_wind1", "wind_power_generated_wind2", "wind_power_generated_wind3"]] == 0).sum(axis=1)
    # Count zeros in solar columns per row
    data["solar_zero_count"] = (data[["pv_array_input_power_solar1", "pv_array_input_power_solar2", "pv_array_input_power_solar3"]] == 0).sum(axis=1)
    # Apply transformation only when (3 - wind_zero_count) is not zero
    data["adjusted_wind"] = data[["wind_power_generated_wind1", "wind_power_generated_wind2", "wind_power_generated_wind3"]].sum(axis=1) * 3 / (3 - data["wind_zero_count"])

    # Replace any division by zero cases with NaN (or another placeholder if needed)
    data["adjusted_wind"] = data["adjusted_wind"].replace([float("inf"), -float("inf")], None)
    
    data["adjusted_solar"] = data[["pv_array_input_power_solar1", "pv_array_input_power_solar2", "pv_array_input_power_solar3"]].sum(axis=1) * 3 / (3 - data["solar_zero_count"])

    # Replace any division by zero cases with NaN (or another placeholder if needed)
    data["adjusted_solar"] = data["adjusted_solar"].replace([float("inf"), -float("inf")], None)
    print(data[["wind_zero_count", "solar_zero_count", "adjusted_wind", "adjusted_solar"]])

    return data

#RUNNING CODE: ----------------------------------------------------------------------------------------------------
def main():
    start_date = "2024-04-09"
    end_date = "2025-05-20"
    #csv_name = "energy_data_{}_to_{}_weekly.csv".format(start_date, end_date)
    input_path = 'C:/Users/ValentinMulet/Work/Data_tftp_root_15min/'
    #input_path = '/Users/swirast/Documents/MyFiles/Code/Work/Data/'
    date_ranges = [
    #    ("2024-04-01", "2024-04-30"),
    #    ("2024-05-01", "2024-05-31"),
    #    ("2024-06-01", "2024-06-30"),
    #    ("2024-07-01", "2024-07-31"),
    #    ("2024-08-01", "2024-08-31"),
    #    ("2024-09-01", "2024-09-30")
    #    ("2024-10-01", "2024-10-31"),
    #    ("2024-11-01", "2024-11-30"),
    #    ("2024-12-01", "2024-12-31"),
    #    ("2025-01-01", "2025-01-31"),
    #    ("2025-02-01", "2025-02-28"),
    #    ("2025-03-01", "2025-03-31"),
    #    ("2025-04-01", "2025-04-30"),
    #    ("2025-05-01", "2025-05-21"),
    ]

    # for i, (start, end) in enumerate(date_ranges, start=1):
    #     print("---------------------------------------------------------------")
    #     print(f"Start {start} to {end}")
    #     generate_csv_energy_data(input_path, f"{i}.csv", start, end)

    #generate_csv_energy_data(input_path, csv_name, start_date, end_date)
    
    csv_name = "full_csv_energy.csv"
    #merge_csv(csv_name)
    for i, (start, end) in enumerate(date_ranges, start=1):
        os.remove(f"{i}.csv")
    data = get_energy_data_from_csv(csv_name)
    data["total_wind_power"] = data["wind_power_generated_wind1"] + data["wind_power_generated_wind2"] + data["wind_power_generated_wind3"]
    data["total_solar_power"] = data["pv_array_input_power_solar1"] + data["pv_array_input_power_solar2"] + data["pv_array_input_power_solar3"]
    data = count_down_devices(data)
    
    # Define the time period
    start_date = '2024-05'
    end_date = '2024-11'

    # Multiply values within the specified range
    data.loc[(data['acquisition_time'] >= start_date) & (data['acquisition_time'] <= end_date), 'total_wind_power'] *= 1.3
    data.loc[(data['acquisition_time'] >= start_date) & (data['acquisition_time'] <= end_date), 'total_solar_power'] *= 1.3

    data['total_power_generated'] = data['total_solar_power'] + data['total_wind_power']
    data["acquisition_time"] = pd.to_datetime(data["acquisition_time"], format="%d/%m/%Y")
    data["acquisition_time"] = data["acquisition_time"].dt.strftime("%Y-%m")
    plot_energy_data(data, "energy_data_year.pdf".format(start_date, end_date))

main()


