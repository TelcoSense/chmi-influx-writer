import json
import os
import shutil
from datetime import datetime, timedelta, timezone

import requests
from dateutil.relativedelta import relativedelta
from influxdb_client import InfluxDBClient, WriteOptions
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from config import DB_CONNECTION_STRING, config
from config import last_month_logger as logger
from ws_db_models import WeatherStation


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
        logger.warning(f"Failed to access folder {folder_url}: {response.status_code}")
        return []


def download_file(file_url: str, data_folder: str) -> None:
    local_file_path = os.path.join(data_folder, os.path.basename(file_url))
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        with open(local_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    else:
        logger.warning(f"Failed to download {file_url}: {response.status_code}")


def delete_single_month_data(client: InfluxDBClient, year: int, month: int) -> None:
    start_time = datetime(year=year, month=month, day=1, tzinfo=timezone.utc)
    end_time = datetime(
        year=year, month=month + 1, day=1, tzinfo=timezone.utc
    ) - timedelta(seconds=1)
    start_time_iso = start_time.isoformat().replace("+00:00", "Z")
    end_time_iso = end_time.isoformat().replace("+00:00", "Z")
    delete_api = client.delete_api()
    logger.info("Deleting last month data...")
    delete_api.delete(
        start=start_time_iso,
        stop=end_time_iso,
        bucket="chmi_data",
        org=config.get("influxdb", "org"),
        # empty predicate must be defined in order to delete all data
        predicate="",
    )
    logger.info("Data successfully deleted.")


def write_single_month_data(
    data_folder,
    year,
    month,
    delete_bucket_data: bool = True,
    measurement: str = None,
    measurement_type: str = "10m",
) -> None:
    client = InfluxDBClient(
        url=config.get("influxdb", "url"),
        token=config.get("influxdb", "token"),
        org=config.get("influxdb", "org"),
    )
    write_api = client.write_api(write_options=WriteOptions(batch_size=5000))
    # mariadb connection
    engine = create_engine(DB_CONNECTION_STRING)
    session = Session(engine)
    # delete data that was written using real time writer
    if delete_bucket_data:
        delete_single_month_data(client, year, month)

    logger.info("Writing started.")
    for data_file in os.listdir(data_folder):
        wsi = data_file.removeprefix(f"{measurement_type}-").removesuffix(
            f"-{year}{month:02d}.json"
        )
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
            if measurement:
                if (
                    value[-1] == 0.0
                    and type(value[-3]) == float
                    and measurement == value[1]
                ):
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
            else:
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
    logger.info("Disconnecting from the DBs...")
    write_api.close()
    client.close()
    session.close()
    engine.dispose()
    logger.info("Connection closed.")


def write_last_month_data(
    measurement_folder: str = "10min",
    measurement_type: str = "10m",
    delete_bucket_data: bool = True,
    measurement: str = None,
):
    logger.info("Checking the CHMI data...")
    last_month_folder = config.get("folders", "last_month_folder")
    if os.path.exists(last_month_folder):
        shutil.rmtree(last_month_folder)
    os.makedirs(last_month_folder, exist_ok=True)
    last_month_dt = datetime.now(tz=timezone.utc) - relativedelta(months=2)
    # define remote folder
    year = last_month_dt.year
    month = last_month_dt.month
    remote_folder = config.get("folders", "chmi_data_folder")
    remote_folder = f"{remote_folder}{measurement_folder}/{month:02d}/"
    file_urls = get_data_urls(remote_folder, measurement_type)
    for file_url in file_urls:
        if not f"{year}{month:02d}" in file_url:
            logger.info("Data is not ready")
            return
    logger.info("Downloading data from CHMI...")
    for file_url in file_urls:
        download_file(file_url, last_month_folder)
    # write the last month data
    write_single_month_data(
        last_month_folder,
        year,
        month,
        delete_bucket_data,
        measurement,
        measurement_type,
    )
    # cleanup
    logger.info("Cleaning up the folder...")
    shutil.rmtree(last_month_folder)
    logger.info("Writing finished successfully.")


def main():
    try:
        write_last_month_data("10min", "10m", True)
        # write also daily rainfall
        write_last_month_data("daily", "dly", False, "SRA")
    except Exception as e:
        logger.error(f"Error during data writing: {e}", exc_info=True)


if __name__ == "__main__":
    main()
