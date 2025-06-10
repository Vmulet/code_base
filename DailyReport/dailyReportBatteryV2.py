import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta
import numpy as np
import os
import requests


def find_data(input_path, date=None):
    """Find parquet files for the given date."""
    date = date or datetime.datetime.now().strftime('%Y-%m-%d')
    paths = [
        "TYVA2/BATT_STATE/BATT_STATE-{0}.parquet.gzip".format(date),
        "TYVA2/CURRENT_BAT/CURRENT_BAT-{0}.parquet.gzip".format(date),
        "TYVA2/VOLT_BAT/VOLT_BAT-{0}.parquet.gzip".format(date),
        "TYVA3/BATT_STATE/BATT_STATE-{0}.parquet.gzip".format(date),
        "TYVA3/CURRENT_BAT/CURRENT_BAT-{0}.parquet.gzip".format(date),
        "TYVA3/VOLT_BAT/VOLT_BAT-{0}.parquet.gzip".format(date),
        "ANEMOMETER/IIMWV/IIMWV-{0}.parquet.gzip".format(date),
        #"EPEVER1/EPEVER1-{0}.parquet.gzip".format(date),
        #"EPEVER2/EPEVER2-{0}.parquet.gzip".format(date),
        #"EPEVER3/EPEVER3-{0}.parquet.gzip".format(date),
        #"TEMP_HUM_B/TEMP_HUM_B-{0}.parquet.gzip".format(date),
        #"TEMP_HUM_T/TEMP_HUM_T-{0}.parquet.gzip".format(date),
        #"GPS2/GNGGA/GNGGA-{0}.parquet.gzip".format(date),
        #"GPS3/GNGGA/GNGGA-{0}.parquet.gzip".format(date),
        "6DOF/PHTRO/PHTRO-{0}.parquet.gzip".format(date),
        "ACCELERO/ACCELERO-{0}.parquet.gzip".format(date)
        # "/WIND_CTRL1/WIND_CTRL1-{0}.parquet.gzip".format(date),
    ]
    return [os.path.join(input_path, path) for path in paths]


def save_figure(fig, pdf):
    """Save the figure to the PDF."""
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def plot_data(data, fig, index, ylabel, data_column=-1, color='tab:blue', option='', numericYticks=False):
    """Plot data on the figure."""
    ycolumn = data[data.columns[data_column]]
    if numericYticks:
        ycolumn = data[data.columns[data_column]].astype(float)
    ax = fig.add_subplot(index)
    ax.plot(data["acquisition_time"].values, ycolumn, color, marker=option)
    ax.set_title(f'Plot of {data.columns[data_column]}')
    ax.set_xlabel("acquisition_time")
    ax.set_ylabel(ylabel)
    ax.grid(True)
    ax.set_xticks(ticks=range(0, len(data), max(1, len(data)//10)),
                  labels=data["acquisition_time"][::max(1, len(data)//10)], rotation=45)


def filterAveragePerPeriod(data, period, dataColums=[-1]):
    """Sort, format and merge values by calculating the average value for a given period."""
    filtered_timestamp = []
    averaged_data = [[] for col in dataColums]
    formatTimestamp = '%X'
    if period < 1:
        formatTimestamp = '%X.%f'
    data['acquisition_time'] = pd.to_datetime(data['acquisition_time'])
    data = data.sort_values(by='acquisition_time')
    currentAverage = [0 for column in dataColums]
    for time in data['acquisition_time']:
        currentTimestamp = time
        break
    diviser = 0
    for index, time in enumerate(data['acquisition_time']):
        diviser += 1
        for tmp, indexColumn in enumerate(dataColums):
            currentAverage[tmp] += float(
                data[data.columns[indexColumn]].values[index])
        if (time - currentTimestamp).total_seconds() >= period:
            filtered_timestamp.append(
                currentTimestamp.strftime(formatTimestamp))
            currentTimestamp = time
            for index, average in enumerate(currentAverage):
                averaged_data[index].append(average/diviser)
            currentAverage = [0 for column in dataColums]
            diviser = 0
    filtered_timestamp.append(currentTimestamp.strftime(formatTimestamp))
    for index, average in enumerate(currentAverage):
        averaged_data[index].append(average/diviser)
    newData = {'acquisition_time': filtered_timestamp}
    for index, average in enumerate(averaged_data):
        newData[data.columns[dataColums[index]]] = average
    return pd.DataFrame(newData)


def new_pdf_page():
    """Create a new PDF page with a fixed size."""
    return plt.figure(figsize=(12, 15))


def add_text_to_fig(text, fig, index, position=111):
    """Add text to a figure."""
    ax = fig.add_subplot(position)
    ax.text(0.5, 0.9, text, size=index, ha="center", wrap=True)
    ax.axis('off')


def filter_data(data):
    """Filter data to include only rows where seconds are divisible by 30."""
    data['acquisition_time'] = pd.to_datetime(data['acquisition_time'])
    data = data.sort_values(by='acquisition_time')
    filter = (data['acquisition_time'].dt.second % 30 == 0)
    filtered_data = data[filter].copy()
    filtered_data['acquisition_time'] = filtered_data['acquisition_time'].dt.strftime(
        '%X.%f')
    return filtered_data


def multi_plot(dataArray, fig, index, ylabel, labels, data_column=-1, option=''):
    ax = fig.add_subplot(index)
    dataArray = syncData(dataArray)
    color = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple',
             'tab:brown', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan']
    for plotIndex, data in enumerate(dataArray):
        ax.plot(data['acquisition_time'].values, data[data.columns[data_column]],
                color[plotIndex], marker=option, label=labels[plotIndex % len(labels)])
    ax.set_xticks(ticks=range(0, len(dataArray[0]), max(1, len(
        dataArray[0])//10)), labels=dataArray[0]['acquisition_time'][::max(1, len(dataArray[0])//10)], rotation=45)
    ax.set_title(f'Plot of {dataArray[0].columns[data_column]}')
    ax.set_xlabel("acquisition_time")
    ax.set_ylabel(ylabel)
    ax.grid(True)
    ax.legend()


def next_file(parquet_files):
    if not hasattr(next_file, "counter"):
        next_file.counter = 0
    next_file.counter += 1
    return pd.read_parquet(parquet_files[next_file.counter], engine='pyarrow')


def calculatePowerBattery(dataCurrent, dataVoltage):
    dataSynced = syncData([dataCurrent, dataVoltage])
    dataCurrent = dataSynced[0]
    dataVoltage = dataSynced[1]
    dataCurrent.iloc[:, -
                     1] = pd.to_numeric(dataCurrent.iloc[:, -1], errors='coerce')
    dataVoltage.iloc[:, -
                     1] = pd.to_numeric(dataVoltage.iloc[:, -1], errors='coerce')
    resultAveragePower = (
        dataCurrent.iloc[:, -1] * dataVoltage.iloc[:, -1]).mean()
    resultEnergyGain = resultAveragePower * 24
    return resultEnergyGain


def syncData(dataArray):
    syncedArray = []
    common_time_index = pd.concat([dataframe['acquisition_time'] for dataframe in dataArray]).drop_duplicates(
    ).sort_values().reset_index(drop=True)
    for data in dataArray:
        data = data.set_index('acquisition_time').reindex(common_time_index)
        data = data.apply(
            pd.to_numeric, errors='coerce').interpolate().reset_index()
        syncedArray.append(data)
    return syncedArray


def getSpotterData(report_date):
    url = "https://api.sofarocean.com/api/devices"
    api_key = "b36c0f94b440b6903e98e028e70987"
    headers = {
        "token": f"{api_key}",
        "Content-Type": "application/json"
    }
    start_date = (datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=1)
                  ).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    end_date = (datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=1)
                ).replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()
    period = "startDate={}&endDate={}".format(start_date, end_date)
    url = "https://api.sofarocean.com/api/wave-data?spotterId=SPOT-30333R&{}".format(
        period)
    response = requests.get(url, headers=headers)
    responseData = response.json()
    if response.status_code != 200:
        print(f"Request failed with status code: {response.status_code}")
    timestamps = []
    significantWaveHeight = []
    for data in responseData["data"]['waves']:
        timestamps.append(datetime.strptime(
            data["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%X'))
        significantWaveHeight.append(data["significantWaveHeight"])
    spotterData = {'acquisition_time': timestamps,
                   'significantWaveHeight': significantWaveHeight}
    return pd.DataFrame(spotterData)


def parquets_to_dailyReport(file_date, parquet_files, pdf_file):
    """Generate a daily report from parquet files with specific plots for each dataset."""
    period = 6  # seconds
    REPORT_TITLE = f"{file_date} Energy Report OCG-DATA Blue Oracle Platform"
    with PdfPages(pdf_file) as pdf:
        fig = new_pdf_page()
        add_text_to_fig(REPORT_TITLE, fig, 15, position=412)

        # Pack2&3
        dataSoC2 = pd.read_parquet(parquet_files[0], engine='pyarrow')
        dataCurrent2 = next_file(parquet_files)
        dataVoltage2 = next_file(parquet_files)
        dataSoC3 = next_file(parquet_files)
        dataCurrent3 = next_file(parquet_files)
        dataVoltage3 = next_file(parquet_files)
        power2 = calculatePowerBattery(dataCurrent2, dataVoltage2)
        power3 = calculatePowerBattery(dataCurrent3, dataVoltage3)
        dataSoC2 = filterAveragePerPeriod(dataSoC2, period, [-2])
        dataCurrent2 = filterAveragePerPeriod(dataCurrent2, period, [-1])
        dataVoltage2 = filterAveragePerPeriod(dataVoltage2, period, [-1])
        dataSoC3 = filterAveragePerPeriod(dataSoC3, period, [-2])
        dataCurrent3 = filterAveragePerPeriod(dataCurrent3, period, [-1])
        dataVoltage3 = filterAveragePerPeriod(dataVoltage3, period, [-1])

        add_text_to_fig("Energy change battery Total : {}Wh ( battery 2 : {}Wh and battery 3 : {}Wh)".format(
            int(power2 + power3), int(power2), int(power3)), fig, 14, position=413)
        save_figure(fig, pdf)  # close page 1

        fig = new_pdf_page()
        position = 311
        multi_plot([dataSoC2, dataSoC3], fig, position, "Pack SOC(%)", [
                   "Pack 2 10-min avg", "Pack 3 10-min avg"], -1)
        position = 312
        multi_plot([dataCurrent2, dataCurrent3], fig, position, "Pack Current (A)", [
                   "Pack 2 10-min avg", "Pack 3 10-min avg"], -1)
        position = 313
        multi_plot([dataVoltage2, dataVoltage3], fig, position, "Pack Voltage (V)", [
                   "Pack 2 10-min avg", "Pack 3 10-min avg"], -1)
        save_figure(fig, pdf)

        # Anemometer
        data = next_file(parquet_files)

        fig = new_pdf_page()
        position = 111
        multi_plot([filterAveragePerPeriod(data, 6, [-4]), filterAveragePerPeriod(data, 600, [-4])],
                   fig, position, "Anemometer wind speed (m/s)", ["6 seconds average", "10 minutes average"], -1)
        save_figure(fig, pdf)

        # EPEVER1&2&3 Data
        # dataPack1 = next_file(parquet_files)
        # dataPack2 = next_file(parquet_files)
        # dataPack3 = next_file(parquet_files)

        # dataPack1averaged = filterAveragePerPeriod(
        #     dataPack1, period, [5, 6, 7, 15, 17])
        # dataPack2averaged = filterAveragePerPeriod(
        #     dataPack2, period, [5, 6, 7, 15, 17])
        # dataPack3averaged = filterAveragePerPeriod(
        #     dataPack3, period, [5, 6, 7, 15, 17])

        # fig = new_pdf_page()
        # position = 321
        # multi_plot([dataPack1averaged, dataPack2averaged, dataPack3averaged], fig, position,
        #            "Solar panel input voltage(v)", ["Pack 1 10-min avg", "Pack 2 10-min avg", "Pack 3 10-min avg"], -5)
        # position = 322
        # multi_plot([dataPack1averaged, dataPack2averaged, dataPack3averaged], fig, position,
        #            "Solar panel input current(A)", ["Pack 1 10-min avg", "Pack 2 10-min avg", "Pack 3 10-min avg"], -4)
        # position = 323
        # multi_plot([dataPack1averaged, dataPack2averaged, dataPack3averaged], fig, position,
        #            "Solar panel input power_l (W)", ["Pack 1 10-min avg", "Pack 2 10-min avg", "Pack 3 10-min avg"], -3)
        # position = 324
        # multi_plot([dataPack1averaged, dataPack2averaged, dataPack3averaged], fig, position,
        #            "Solar panel battery temperature l(°C)", ["Pack 1 10-min avg", "Pack 2 10-min avg", "Pack 3 10-min avg"], -2)
        # save_figure(fig, pdf)

        # Temperature of the central column
        # dataBottomColumn = next_file(parquet_files)
        # dataTopColumn = next_file(parquet_files)

        # dataTopColumn = filterAveragePerPeriod(dataTopColumn, period, [2, 3])
        # dataBottomColumn = filterAveragePerPeriod(
        #     dataBottomColumn, period, [2, 3])

        # fig = new_pdf_page()
        # position = 211
        # multi_plot([dataTopColumn, dataBottomColumn], fig, position, "Temperature(°C)", [
        #            "Top Column 10-min avg", "Bottom Column 10-min avg"], -2)
        # position = 212
        # multi_plot([dataTopColumn, dataBottomColumn], fig, position, "Humidity(%)", [
        #            "Top Column 10-min avg", "Bottom Column 10-min avg"], -1)
        # save_figure(fig, pdf)

        # # GPS2 and 3
        # dataGps2 = next_file(parquet_files)
        # dataGps3 = next_file(parquet_files)

        # fig = new_pdf_page()
        # plot_gps(dataGps2, fig, 211, 'GPS2', file_date)
        # plot_gps(dataGps3, fig, 212, 'GPS3', file_date)
        # save_figure(fig, pdf)

        # 6DOF
        data = next_file(parquet_files)

        fig = new_pdf_page()
        position = 211
        multi_plot([filter_data(data)[['acquisition_time', data.columns[2]]], filterAveragePerPeriod(
            data, 600, [2])], fig, position, "Pitch(°)", ["Data every 30 seconds", "10 minutes average"], -1)
        position = 212
        multi_plot([filter_data(data)[['acquisition_time', data.columns[4]]], filterAveragePerPeriod(
            data, 600, [4])], fig, position, "Roll(°)", ["Data every 30 seconds", "10 minutes average"], -1)
        save_figure(fig, pdf)

        # Spotter
        fig = new_pdf_page()
        multi_plot([getSpotterData(file_date)], fig, 111, "significantWaveHeight (m)", [
                   "raw significantWaveHeight data from spotter"], -1)
        save_figure(fig, pdf)
    next_file.counter = 0
    print(f'{pdf_file} created successfully.')


def plot_gps(data, fig, position, ylabel, file_date):
    Earth_radius = 6371000
    latitude_ref = 42.82931667
    longitude_ref = 3.422366667

    # Generate points for the 250m circle
    theta = np.linspace(0, 2*np.pi, 100)
    X250 = 250 * np.cos(theta)
    Y250 = 250 * np.sin(theta)

    # Convert to latitude and longitude
    lat250 = latitude_ref + X250 / Earth_radius * 180 / np.pi
    long250 = longitude_ref + Y250 / Earth_radius / \
        np.cos(latitude_ref * np.pi / 180) * 180 / np.pi

    ax = fig.add_subplot(position)
    # Plot the 250m limit circle
    ax.plot(long250, lat250, 'r', label='250m limit')

    # Plot the GPS data from the DataFrame
    ax.plot(data['Longitude'], data['Latitude'], 'b-.', label='BLUE ORACLE')
    ax.plot(longitude_ref, latitude_ref, marker='^', color='red', markersize=5)

    # Set title (assuming 'filename' is defined somewhere)
    titre = f'{ylabel} tracks : {file_date}'
    ax.set_title(titre)

    # Set labels and legend
    ax.set_xlabel('Longitude, Decimal Degrees, E')
    ax.set_ylabel('Latitude, Decimal Degrees, N')
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, linestyle='--', alpha=0.5)


def main(report_date, input_path, pdf_name):
    paths = find_data(input_path, report_date)
    parquets_to_dailyReport(report_date, paths, pdf_name)

main('2025-03-09', 'C:/Users/ValentinMulet/Code/DailyReport/tftp_root_decoded/', 'C:/Users/ValentinMulet/Code/DailyReport/output_plot.pdf')