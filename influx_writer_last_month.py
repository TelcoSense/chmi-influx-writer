import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone

import requests
from dateutil.relativedelta import relativedelta
from influxdb_client import InfluxDBClient, WriteOptions
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from config import DB_CONNECTION_STRING, config
from influx_writer_realtime import download_file
from ws_db_models import WeatherStation

logging.basicConfig(
    # filename="influx_writer_last_month.log",
    level=logging.INFO,
    format="[%(asctime)s] -- %(levelname)s -- %(message)s",
    stream=sys.stdout,
)


def get_data_urls(folder_url: str, measurement_type: str = "10m") -> list[str]:
    response = requests.get(folder_url)
    if response.status_code == 200:
        html_text = response.text
        file_urls = [
            folder_url + line.split('"')[1]
            for line in html_text.splitlines()
            if ".json" in line and 'href="' in line and measurement_type in line
        ]
        return file_urls
    else:
        logging.warning(f"Failed to access folder {folder_url}: {response.status_code}")
        return []


def write_single_month_data(data_folder, year, month) -> None:
    client = InfluxDBClient(
        url=config.get("influxdb", "url"),
        token=config.get("influxdb", "token"),
        org=config.get("influxdb", "org"),
    )
    write_api = client.write_api(write_options=WriteOptions(batch_size=5000))
    # mariadb connection
    engine = create_engine(DB_CONNECTION_STRING)
    session = Session(engine)

    "TODO: wipeout the last month data from the bucket!"

    for data_file in os.listdir(data_folder):
        wsi = data_file.removeprefix("10m-").removesuffix(f"-{year}{month:02d}.json")
        ws_db = session.scalar(select(WeatherStation).where(WeatherStation.wsi == wsi))
        # if the current weather station is not in the db, don't write any data
        if not ws_db:
            continue
        gh_id = ws_db.gh_id
        with open(f"{data_folder}/{data_file}", "r", encoding="utf-8") as file:
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
        # write_api.write(bucket="chmi_data", record=data_to_write, write_precision="ns")
    print("Closing connection.")
    write_api.close()
    client.close()


def write_last_month_data(measurement_folder: str = "10min"):
    logging.info("Checking the CHMI data...")
    last_month_folder = config.get("folders", "last_month_folder")
    if os.path.exists(last_month_folder):
        shutil.rmtree(last_month_folder)
    os.makedirs(last_month_folder, exist_ok=True)
    last_month_dt = datetime.now(tz=timezone.utc) - relativedelta(months=1)
    # define remote folder
    year = last_month_dt.year
    month = last_month_dt.month
    remote_folder = config.get("folders", "chmi_data_folder")
    remote_folder = f"{remote_folder}{measurement_folder}/{month:02d}/"
    file_urls = get_data_urls(remote_folder)
    for file_url in file_urls:
        if not f"{year}{month:02d}" in file_url:
            logging.info("Data is not ready")
            return
    logging.info("Downloading data from CHMI...")
    for file_url in file_urls:
        download_file(file_url, last_month_folder)
    # write the last month data
    write_single_month_data(last_month_folder, year, month)
    # cleanup
    logging.info("Cleaning up the folder...")
    shutil.rmtree(last_month_folder)
    logging.info("Writing finished successfully.")


def main():
    try:
        write_last_month_data()
    except Exception as e:
        logging.error(f"Error during data writing: {e}", exc_info=True)


if __name__ == "__main__":
    main()
