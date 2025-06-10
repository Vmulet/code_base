import pandas as pd
import format_data
import file_manipulation

def find_and_filter_data_weekly(input_path, year, week, type_of_files, column_to_keep):
    period = 0
    data = format_data.filterAveragePerPeriod(format_data.packData(file_manipulation.find_weekly_files(input_path, year, week, [type_of_files])), period, column_to_keep)
    return data

def get_energy_data_from_csv(file_name):
    data = pd.read_csv(file_name)
    data["acquisition_time"] = pd.to_datetime(data["acquisition_time"])
    data = data.sort_values(by="acquisition_time")
    return data

def find_and_filter_data(input_path, start_date, end_date , type_of_files, column_to_keep):
    period = 0
    data = format_data.filterAveragePerPeriod(format_data.packData(file_manipulation.find_files_between_dates(input_path, start_date, end_date, [type_of_files])), period, column_to_keep)
    return data

def get_turbines_data(input_path, start_date, end_date):
    print("Start wind1 ")
    wind1 =  find_and_filter_data(input_path, start_date, end_date, "WIND_CTRL1", [5, 6, 9, 10])
    print("Start wind2")
    wind2 =  find_and_filter_data(input_path, start_date, end_date, "WIND_CTRL2", [6, 7, 10, 11])
    print("Start wind3")
    wind3 =  find_and_filter_data(input_path, start_date, end_date, "WIND_CTRL3", [6, 7, 10, 11])
    return wind1, wind2, wind3

def get_epever_data(input_path, start_date, end_date):
    print("Start epever1")
    epever1 = find_and_filter_data(input_path, start_date, end_date, "EPEVER1", [5, 6, 7, 15, 17])
    print("Start epever2")
    epever2 = find_and_filter_data(input_path, start_date, end_date, "EPEVER2", [5, 6, 7, 15, 17])
    print("Start epever3")
    epever3 = find_and_filter_data(input_path, start_date, end_date, "EPEVER3", [5, 6, 7, 15, 17])
    return epever1, epever2, epever3

def get_turbines_data_weekly(year, input_path, weeks = []):
    for week in weeks:
        print("Start wind1 week {}".format(week))
        wind1 =  find_and_filter_data_weekly(input_path, year, week, "WIND_CTRL1", [5, 6, 9, 10])
        wind2 =  find_and_filter_data_weekly(input_path, year, week, "WIND_CTRL2", [6, 7, 10, 11])
        wind3 =  find_and_filter_data_weekly(input_path, year, week, "WIND_CTRL3", [6, 7, 10, 11])
    return wind1, wind2, wind3

def get_epever_data_weekly(year, input_path, week = 1):
    print("Start epever1")
    epever1 = find_and_filter_data_weekly(input_path, year, week, "EPEVER1", [5, 6, 7, 15, 17])
    print("Start epever2")
    epever2 = find_and_filter_data_weekly(input_path, year, week, "EPEVER2", [5, 6, 7, 15, 17])
    print("Start epever3")
    epever3 = find_and_filter_data_weekly(input_path, year, week, "EPEVER3", [5, 6, 7, 15, 17])
    return epever1, epever2, epever3