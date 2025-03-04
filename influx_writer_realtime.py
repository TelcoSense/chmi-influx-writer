import json
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dateutil.relativedelta import relativedelta
from influxdb_client import InfluxDBClient, WriteOptions
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from config import DB_CONNECTION_STRING, config
from parsing_tools import process_metadata
from ws_db_models import Measurement1H, Measurement10M, MeasurementDLY, WeatherStation

# logging setup
logger = logging.getLogger("realtime_logger")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("realtime.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


def get_utc_date() -> str:
    """Get today's date (UTC time) or yesterday's date if the UTC hour is 0.

    Returns:
        str: Date string in the YYYYMMDD format.
    """
    now = datetime.now(timezone.utc)
    if now.hour == 0:
        return (now.date() - timedelta(days=1)).strftime("%Y%m%d")
    return now.date().strftime("%Y%m%d")


def get_data_urls(folder_url: str, measurement_type: str = "10m") -> list[str]:
    current_date = get_utc_date()
    response = requests.get(folder_url)
    if response.status_code == 200:
        html_text = response.text
        file_urls = [
            folder_url + line.split('"')[1]
            for line in html_text.splitlines()
            if ".json" in line
            and 'href="' in line
            and measurement_type in line
            and current_date in line
        ]
        return file_urls
    else:
        logger.warning(f"Failed to access folder {folder_url}: {response.status_code}")
        return []


def get_metadata_urls(folder_url: str) -> list[str]:
    response = requests.get(folder_url)
    if response.status_code == 200:
        html_text = response.text
        return [
            folder_url + line.split('"')[1]
            for line in html_text.splitlines()
            if ".json" in line and 'href="' in line
        ]
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


def update_measurements_db(
    session: Session,
    measurements: list[list],
    measurement_type: Measurement10M | Measurement1H | MeasurementDLY,
) -> None:
    logger.info(f"Updating the {measurement_type.__tablename__} table...")
    for measurement in measurements:
        measurement_db = session.scalar(
            select(measurement_type).where(
                measurement_type.abbreviation == measurement[0]
            )
        )
        if not measurement_db:
            measurement_db = measurement_type(
                abbreviation=measurement[0], name=measurement[1], unit=measurement[2]
            )
            logger.info(f"Created new measurement named: {measurement[1]}")


def update_ws_measurements(
    session: Session,
    ws: WeatherStation,
    measurement_list: list[list],
    measurement_type: Measurement10M | Measurement1H | MeasurementDLY,
) -> None:
    for measurement in measurement_list:
        measurement_db = session.scalar(
            select(measurement_type).where(
                measurement_type.abbreviation == measurement[0]
            )
        )
        if measurement_db and measurement_type == Measurement10M:
            if measurement_db in ws.measurements_10m:
                continue
            else:
                ws.measurements_10m.append(measurement_db)
                logger.info(
                    f"Updated 10m measurements of the weather station: {ws.full_name}"
                )
        elif measurement_db and measurement_type == Measurement1H:
            if measurement_db in ws.measurements_1h:
                continue
            else:
                ws.measurements_1h.append(measurement_db)
                logger.info(
                    f"Updated 1h measurements of the weather station: {ws.full_name}"
                )
        elif measurement_db and measurement_type == MeasurementDLY:
            if measurement_db in ws.measurements_dly:
                continue
            else:
                ws.measurements_dly.append(measurement_db)
                logger.info(
                    f"Updated dly measurements of the weather station: {ws.full_name}"
                )


def update_weather_stations_db(session: Session, weather_stations: dict):
    logger.info(f"Updating the weather_stations table...")
    for wsi in weather_stations:
        weather_station = weather_stations[wsi]
        has_10m = "10M" in weather_station
        has_1h = "1H" in weather_station
        has_dly = "DLY" in weather_station
        has_measurements = has_10m or has_1h or has_dly
        if has_measurements:
            # try to find existing weather station
            weather_station_db = session.scalar(
                select(WeatherStation).where(WeatherStation.wsi == wsi)
            )
            # if it does not exist, create it in the db
            if not weather_station_db:
                weather_station_db = WeatherStation(
                    wsi=wsi,
                    gh_id=weather_station["GH_ID"],
                    full_name=weather_station["FULL_NAME"],
                    X=weather_station["GEOGR1"],
                    Y=weather_station["GEOGR2"],
                    elevation=weather_station["ELEVATION"],
                )
                session.add(weather_station_db)
                logger.info(
                    f"Created new weather station named: {weather_station_db.full_name}"
                )
            # if the ws has 10m measurements, update them
            if has_10m:
                update_ws_measurements(
                    session, weather_station_db, weather_station["10M"], Measurement10M
                )
            # if the ws has 1h measurements, update them
            if has_1h:
                update_ws_measurements(
                    session, weather_station_db, weather_station["1H"], Measurement1H
                )
            # if the ws has dly measurements, update them
            if has_dly:
                update_ws_measurements(
                    session, weather_station_db, weather_station["DLY"], MeasurementDLY
                )


def update_metadata(session: Session) -> None:
    logger.info(f"Updating DB metadata.")
    last_month_dt = datetime.now(tz=timezone.utc) - relativedelta(months=1)
    year = last_month_dt.year
    month = last_month_dt.month
    chmi_folder = f"{config.get("folders", "chmi_metadata_folder")}{month:02d}/"
    local_folder = "metadata"
    if os.path.exists(local_folder):
        shutil.rmtree(local_folder)
    os.makedirs(local_folder, exist_ok=True)
    file_urls = get_metadata_urls(chmi_folder)
    for file_url in file_urls:
        # if the data is not current for some reason, abort the metadata update
        if not f"{year}{month:02d}" in file_url:
            logger.info(f"CHMI metadata was not ready.")
            return
    # download the metadata files
    for file_url in file_urls:
        download_file(file_url, local_folder)
    ws_dict, m10, m1h, mdly = process_metadata(local_folder, year, month)
    # this will add potential new measurements to the db
    update_measurements_db(session, m10, Measurement10M)
    update_measurements_db(session, m1h, Measurement1H)
    update_measurements_db(session, mdly, MeasurementDLY)
    # this will update the weather stations and their measurements
    update_weather_stations_db(session, ws_dict)
    # commit the potential changes
    session.commit()
    logger.info(f"DB update complete.")


def write_latest_data() -> None:
    try:
        logger.info("Connecting to the DBs...")
        # influxdb connection
        client = InfluxDBClient(
            url=config.get("influxdb", "url"),
            token=config.get("influxdb", "token"),
            org="vut",
        )
        write_api = client.write_api(write_options=WriteOptions(batch_size=5000))
        # mariadb connection
        engine = create_engine(DB_CONNECTION_STRING)
        session = Session(engine)
        # utc now date
        utc_now = datetime.now(timezone.utc)
        # subtract one hour from current utc time
        start_time = utc_now - timedelta(hours=1)
        # start at the hour mark
        start_time = start_time.replace(minute=0, second=0, microsecond=0)
        # set the end time (HH:50)
        end_time = start_time + timedelta(minutes=50)
        # update the mariadb once a month (15th day between 02:00 and 03:00)
        if utc_now.day == 15 and utc_now.hour == 2:
            update_metadata(session)

        # delete the realtime folder and its contents
        realtime_folder = config.get("folders", "realtime_folder")
        if os.path.exists(realtime_folder):
            shutil.rmtree(realtime_folder)
        # create it again
        os.makedirs(realtime_folder, exist_ok=True)
        # get the file urls to download
        file_urls = get_data_urls(config.get("folders", "chmi_now_folder"))
        logger.info("Downloading latest data from CHMI...")
        for file_url in file_urls:
            download_file(file_url, realtime_folder)
        data_files = os.listdir(realtime_folder)
        logger.info(f"Parsing data from {len(data_files)} weather stations.")
        for data_file in data_files:
            # sometimes the json cannot be opened
            try:
                with open(
                    os.path.join(realtime_folder, data_file), "r", encoding="utf-8"
                ) as file:
                    data = json.load(file)
            except json.JSONDecodeError:
                logger.error(f"Could not decode file: {data_file}, skipping...")
                continue
            date_string = start_time.strftime("%Y%m%d")
            # get current weather station id (WSI)
            wsi = data_file.removeprefix("10m-").removesuffix(f"-{date_string}.json")
            ws_db = session.scalar(
                select(WeatherStation).where(WeatherStation.wsi == wsi)
            )
            # if the current weather station is not in the db, don't write any data
            if not ws_db:
                continue
            gh_id = ws_db.gh_id
            values = data["data"]["data"]["values"]
            data_to_write = []
            for value in values:
                dt = datetime.strptime(value[-4], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
                # get the last hour data only (typically 6 values for each measurement)
                if type(value[-3]) == float and dt >= start_time and dt <= end_time:
                    data_to_write.append(
                        {
                            "measurement": value[1],
                            "fields": {gh_id: value[-3]},
                            "time": int(dt.timestamp() * 1e9),
                        },
                    )
            write_api.write(
                bucket="chmi_data", record=data_to_write, write_precision="ns"
            )
        logger.info("Disconnecting from the DBs...")
        write_api.close()
        client.close()
        session.close()
        engine.dispose()
        logger.info("Connection closed.")
    except Exception as e:
        logger.error(f"Error in job execution: {e}", exc_info=True)


def main():
    logger.info("CHMI InfluxDB writer started.")
    logger.info("Starting scheduler...")
    scheduler = BlockingScheduler()
    scheduler.add_job(
        write_latest_data,
        trigger=CronTrigger(minute=30, timezone=timezone.utc),
        id="realtime_writer",
        replace_existing=True,
        misfire_grace_time=300,
    )
    try:
        logger.info("Scheduler started. Press Ctrl+C to exit.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
