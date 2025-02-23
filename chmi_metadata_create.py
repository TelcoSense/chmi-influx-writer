import configparser
import json

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from chmi_metadata_db import (
    Base,
    Measurement1H,
    Measurement10M,
    MeasurementDLY,
    WeatherStation,
)

config = configparser.ConfigParser()
config.read("config.ini")

db_user = config["mariadb"]["user"]
db_password = config["mariadb"]["password"]
db_url = config["mariadb"]["url"]
db_name = config["mariadb"]["db_name"]

DATABASE_SERVER_URL = f"mariadb+mariadbconnector://{db_user}:{db_password}@{db_url}"
engine = create_engine(DATABASE_SERVER_URL)

with engine.connect() as conn:
    # drop the db if it already exists
    conn.execute(text(f"DROP DATABASE IF EXISTS chmi_metadata"))
    # proper character set and collation must be defined
    conn.execute(
        text(
            f"CREATE DATABASE IF NOT EXISTS chmi_metadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
    )
    conn.commit()

DATABASE_URL = f"mariadb+mariadbconnector://{db_user}:{db_password}@{db_url}/{db_name}"
engine = create_engine(DATABASE_URL)

# create all tables
Base.metadata.create_all(engine)

# create session for adding rows to tables
session = Session(engine)


def add_measurements_to_db(
    measurements_json_path: str,
    measurement_type: Measurement10M | Measurement1H | MeasurementDLY,
) -> None:
    # add measurements
    with open(measurements_json_path, "r", encoding="utf-8") as file:
        measurements = json.load(file)
    for measurement in measurements:
        measurement_db = measurement_type(
            abbreviation=measurement[0], name=measurement[1], unit=measurement[2]
        )
        session.add(measurement_db)


add_measurements_to_db("./data_db/measurements_10m.json", Measurement10M)
add_measurements_to_db("./data_db/measurements_1h.json", Measurement1H)
add_measurements_to_db("./data_db/measurements_dly.json", MeasurementDLY)

# add weather stations and relations to measurements
with open("./data_db/weather_stations.json", "r", encoding="utf-8") as file:
    weather_stations = json.load(file)


def assign_measurements_ws_db(
    measurement_list: list[list],
    measurement_type: Measurement10M | Measurement1H | MeasurementDLY,
) -> list[Measurement10M | Measurement1H | MeasurementDLY]:
    weather_station_measurements = []
    # create list of measurements for current weather station
    for measurement in measurement_list:
        weather_station_measurements.append(
            session.scalars(
                select(measurement_type).where(
                    measurement_type.abbreviation == measurement[0]
                )
            ).first()
        )
    return weather_station_measurements


for wsi in weather_stations:
    weather_station = weather_stations[wsi]
    has_10m = "10M" in weather_station
    has_1h = "1H" in weather_station
    has_dly = "DLY" in weather_station
    has_measurements = has_10m or has_1h or has_dly
    if has_measurements:
        weather_station_db = WeatherStation(
            wsi=wsi,
            gh_id=weather_station["GH_ID"],
            full_name=weather_station["FULL_NAME"],
            X=weather_station["GEOGR1"],
            Y=weather_station["GEOGR2"],
            elevation=weather_station["ELEVATION"],
        )
        session.add(weather_station_db)
        if has_10m:
            measurements_10m = assign_measurements_ws_db(
                weather_station["10M"], Measurement10M
            )
            weather_station_db.measurements_10m = measurements_10m
        if has_1h:
            measurements_1h = assign_measurements_ws_db(
                weather_station["1H"], Measurement1H
            )
            weather_station_db.measurements_1h = measurements_1h
        if has_dly:
            measurements_dly = assign_measurements_ws_db(
                weather_station["DLY"], MeasurementDLY
            )
            weather_station_db.measurements_dly = measurements_dly

# define the SQL view
SHOW_WEATHER_STATIONS_VIEW = text(
    """
    CREATE OR REPLACE VIEW show_weather_stations AS
    SELECT
        ws.id,
        ws.wsi,
        ws.gh_id,
        ws.full_name,
        ws.X,
        ws.Y,
        ws.elevation,
        GROUP_CONCAT(DISTINCT CONCAT(m10.name, ' [', m10.unit, ']') ORDER BY m10.name SEPARATOR ', ') AS measurements_10m,
        GROUP_CONCAT(DISTINCT CONCAT(m1h.name, ' [', m1h.unit, ']') ORDER BY m1h.name SEPARATOR ', ') AS measurements_1h,
        GROUP_CONCAT(DISTINCT CONCAT(mdly.name, ' [', mdly.unit, ']') ORDER BY mdly.name SEPARATOR ', ') AS measurements_dly
    FROM weather_stations ws
    LEFT JOIN weather_station_measurements_10m wsm10 ON ws.id = wsm10.weather_station_id
    LEFT JOIN measurements_10m m10 ON wsm10.measurement_10m_id = m10.id
    LEFT JOIN weather_station_measurements_1h wsm1h ON ws.id = wsm1h.weather_station_id
    LEFT JOIN measurements_1h m1h ON wsm1h.measurement_1h_id = m1h.id
    LEFT JOIN weather_station_measurements_dly wsmdly ON ws.id = wsmdly.weather_station_id
    LEFT JOIN measurements_dly mdly ON wsmdly.measurement_dly_id = mdly.id
    GROUP BY ws.id, ws.wsi, ws.gh_id, ws.full_name, ws.X, ws.Y, ws.elevation;
"""
)
if not session.scalar(
    text(
        "SELECT COUNT(*) FROM information_schema.views WHERE table_name = 'show_weather_stations'"
    )
):
    session.execute(SHOW_WEATHER_STATIONS_VIEW)

# commit changes and close the connection
session.commit()
session.close()
