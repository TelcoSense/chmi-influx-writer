import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone

import requests
from dateutil.relativedelta import relativedelta
from influxdb_client import InfluxDBClient, WriteOptions
from tqdm import tqdm

from config import config
from influx_writer_realtime import download_file

for month in range(1, 2):
    year = 2025
    month_folder = f"{month:02d}"
    metadata_dir = f"./{year}/processed_metadata"
    meta_file = f"{metadata_dir}/{month_folder}/meta-{year}{month_folder}.json"

    with open(meta_file, "r", encoding="utf-8") as file:
        meta = json.load(file)

    client = InfluxDBClient(
        url=config.get("influxdb", "url"),
        token=config.get("influxdb", "token"),
        org=config.get("influxdb", "org"),
    )
    write_api = client.write_api(write_options=WriteOptions(batch_size=5000))

    input_base_dir = f"./{year}/data/10min"
    input_folder = os.path.join(input_base_dir, month_folder)
    print(f"Writing month {month} out of 12.")
    for data_file in tqdm(os.listdir(input_folder), ascii=True):
        wsi = data_file.removeprefix("10m-").removesuffix(f"-{year}{month_folder}.json")
        gh_id = meta[wsi]["GH_ID"]
        with open(
            f"./{year}/data/10min/{month_folder}/{data_file}", "r", encoding="utf-8"
        ) as file:
            data = json.load(file)
        values = data["data"]["data"]["values"]
        data_to_write = []
        for value in values:
            if value[-1] == 0.0 and type(value[-3]) == float:
                dt = datetime.strptime(value[-4], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
                data_to_write.append(
                    {
                        "measurement": value[1],
                        "fields": {gh_id: value[-3]},
                        # time in nanoseconds for efficiency
                        "time": int(dt.timestamp() * 1e9),
                    },
                )
        # must write in ns
        write_api.write(bucket="chmi_data", record=data_to_write, write_precision="ns")

    print("Closing connection.")
    write_api.close()
    client.close()


# def write_single_month_data():
#     pass
