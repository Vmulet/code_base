import pandas as pd
from datetime import datetime
import requests
SPOTTER_ID = ["SPOT-30330R","SPOT-30333R"]

def init():
    url = "https://api.sofarocean.com/api/devices"
    api_key = "b36c0f94b440b6903e98e028e70987"
    headers = {"token": f"{api_key}", "Content-Type": "application/json"}
    return url, api_key, headers

def get_Spotter_ID_list():
    url, api_key, headers = init()
    response = requests.get(url, headers=headers)
    spotter_ids = []
    responseData = response.json()
    if response.status_code != 200:
        print(f"Request failed with status code: {response.status_code}")
    for data in responseData["data"]["devices"]:
        spotter_ids.append(data["spotterId"])
    return spotter_ids

SPOTTER_ID = get_Spotter_ID_list()

def getSpotterData(report_date):
    spotterData = {}
    url, api_key, headers = init()
    start_date = (
        (datetime.strptime(report_date, "%Y-%m-%d"))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat()
    )
    end_date = (
        (datetime.strptime(report_date, "%Y-%m-%d"))
        .replace(hour=23, minute=59, second=59, microsecond=999999)
        .isoformat()
    )
    period = "startDate={}&endDate={}".format(start_date, end_date)
    for spotter in SPOTTER_ID:
        url = "https://api.sofarocean.com/api/wave-data?spotterId={}&{}".format(
            spotter, 
            period
        )
        response = requests.get(url, headers=headers)
        responseData = response.json()
        if response.status_code != 200:
            print(f"Request failed with status code: {response.status_code}")
        if len(responseData["data"]["waves"]):
            break
    if len(responseData["data"]["waves"]):
        for key in responseData["data"]["waves"][0]:
            if key == "timestamp":
                key = "acquisition_time"
            spotterData[key] = []
        for data in responseData["data"]["waves"]:
            for key in data:
                if key == "timestamp":
                    spotterData["acquisition_time"].append(
                        datetime.strptime(data[key], "%Y-%m-%dT%H:%M:%S.%fZ")
                    )
                else:
                    spotterData[key].append(data[key])
    return pd.DataFrame(spotterData)

def main(report_date, output_parquet_file):
    getSpotterData(report_date).to_parquet(output_parquet_file, engine="pyarrow")