from datetime import datetime, timedelta
import os

def find_files_substring_in_filename(directory, substring, whitelistFile):
    """
    Finds files in a directory (and its subdirectories) whose names contain a specific substring
    and are also present in a whitelist of file names.

    Args:
        directory (str): The directory to search in.
        substring (str): The substring to look for in file names.
        whitelistFile (list): A list of file names to filter the results.

    Returns:
        list: A list of matching file paths.
    """
    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            if substring in file_name and any(
                file in file_name for file in whitelistFile
            ):
                matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files

def find_all_files_substring_in_filename(directory, substring):
    """
    Finds all files in a directory (and its subdirectories) whose names contain a specific substring,
    excluding files that match certain ignored patterns.

    Args:
        directory (str): The directory to search in.
        substring (str): The substring to look for in file names.

    Returns:
        list: A list of matching file paths.
    """
    matching_files = []
    ignore_files = ['HCHDM', '~ PSONCMS', 'WIXDR', 'GNZDA', 'BMS', 'TEMP_PDU', 'TEMP_CELL']
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            if substring in file_name and not any(ignore in file_name for ignore in ignore_files):
                    matching_files.append(os.path.join(root, file_name).replace("\\", "/"))
    return matching_files

def get_week_dates(year, week):
    """
    Calculates the start and end dates of a specific ISO week in a given year.

    Args:
        year (int): The year for which the week dates are calculated.
        week (int): The ISO week number.

    Returns:
        tuple: A tuple containing the start and end dates of the week.
    """
    first_day = datetime(year, 1, 1).date()
    first_monday = first_day + timedelta(days=(7 - first_day.isocalendar()[2]))
    start_date = first_monday + timedelta(weeks=week - 1)
    end_date = start_date + timedelta(days=6)
    return start_date, end_date

def find_weekly_files(input_path, year, week, folders):
    """
    Finds files in a directory (and its subdirectories) that match the dates of a specific ISO week.
    Filters files based on valid substrings and exact dates within the week.

    Args:
        input_path (str): The directory to search in.
        year (int): The year of the ISO week.
        week (int): The ISO week number.
        folders (list): A list of folder names to filter the results.

    Returns:
        list: A list of matching file paths.
    """
    start_date, end_date = get_week_dates(year, week)
    valid_substrings = [start_date.strftime("%Y-%m"), end_date.strftime("%Y-%m")] 
    valid_dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]  
    matching_files = find_files_substring_in_filename(input_path, valid_substrings[0], folders)
    if len(valid_substrings) > 1:
        matching_files += find_files_substring_in_filename(input_path, valid_substrings[1], folders)
    filtered_files = [file for file in matching_files if any(date in file for date in valid_dates)]
    return filtered_files

def find_files_between_dates(input_path, start_date, end_date, folders):
    """
    Finds files in a directory (and its subdirectories) that fall within a specific date range.
    Filters files based on valid month substrings and exact dates within the range.

    Args:
        input_path (str): The directory to search in.
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.
        folders (list): A list of folder names to filter the results.

    Returns:
        list: A list of matching file paths.
    """
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
    """
    Converts a date range into a list of ISO week numbers.

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.

    Returns:
        list: A list of ISO week numbers within the date range.
    """
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