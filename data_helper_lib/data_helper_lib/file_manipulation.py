from datetime import datetime, timedelta
import os

def find_files_substring_in_filename(directory, substring, whitelistFile):
    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            if substring in file_name and any(
                file in file_name for file in whitelistFile
            ):
                matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files

def find_all_files_substring_in_filename(directory, substring):
    matching_files = []
    ignore_files = ['HCHDM', '~ PSONCMS', 'WIXDR', 'GNZDA', 'BMS', 'TEMP_PDU', 'TEMP_CELL']
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            if substring in file_name and not any(ignore in file_name for ignore in ignore_files):
                    matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files

def get_week_dates(year, week):
    first_day = datetime(year, 1, 1).date()
    first_monday = first_day + timedelta(days=(7 - first_day.isocalendar()[2]))
    start_date = first_monday + timedelta(weeks=week - 1)
    end_date = start_date + timedelta(days=6)
    return start_date, end_date

def find_weekly_files(input_path, year, week, folders):
    start_date, end_date = get_week_dates(year, week)
    valid_substrings = [start_date.strftime("%Y-%m"), end_date.strftime("%Y-%m")] 
    valid_dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]  
    matching_files = find_files_substring_in_filename(input_path, valid_substrings[0], folders)
    if len(valid_substrings)>1:
        matching_files += find_files_substring_in_filename(input_path, valid_substrings[1], folders)
    filtered_files = [file for file in matching_files if any(date in file for date in valid_dates)]
    return filtered_files

def find_files_between_dates(input_path, start_date, end_date, folders):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    month_list = sorted(set(start_date.strftime("%Y-%m") for start_date in 
                            [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]))
    matching_files = []
    for month_substring in month_list:
        files = find_files_substring_in_filename(input_path, month_substring, folders)
        matching_files.extend([f for f in files if any(date in f for date in 
                                                       [start_date.strftime("%Y-%m-%d") for start_date in 
                                                        [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]] )])

    return matching_files

def date_to_week(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    week_array = []
    current_date = start_date
    while current_date <= end_date:
        week_number = current_date.isocalendar()[1]
        if week_number not in week_array:
            week_array.append(week_number)
        current_date += timedelta(days=1)
    return week_array