import configparser
import json

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from config import DB_CONNECTION_STRING, DB_SERVER_CONNECTION_STRING
from ws_db_models import (
    Base,
    Measurement1H,
    Measurement10M,
    MeasurementDLY,
    WeatherStation,
)

# create the chmi_metadata db from scratch

engine = create_engine(DB_SERVER_CONNECTION_STRING)
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
engine.dispose()

# create all tables
engine = create_engine(DB_CONNECTION_STRING)
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

# define the SQL views
for m in ["10m", "1h", "dly"]:
    SHOW_WEATHER_STATIONS = text(
        f"""
        CREATE OR REPLACE VIEW show_weather_stations_{m} AS
        SELECT
            ws.id,
            ws.wsi,
            ws.gh_id,
            ws.full_name,
            ws.X,
            ws.Y,
            ws.elevation,
            GROUP_CONCAT(DISTINCT CONCAT(m.name, ' [', m.unit, ']') ORDER BY m.name SEPARATOR ', ') AS measurements_{m}
        FROM weather_stations ws
        LEFT JOIN weather_station_measurements_{m} wsm ON ws.id = wsm.weather_station_id
        LEFT JOIN measurements_{m} m ON wsm.measurement_{m}_id = m.id
        GROUP BY ws.id, ws.wsi, ws.gh_id, ws.full_name, ws.X, ws.Y, ws.elevation
        HAVING measurements_{m} IS NOT NULL;
        """
    )
    if not session.scalar(
        text(
            f"SELECT COUNT(*) FROM information_schema.views WHERE table_name = 'show_weather_stations_{m}'"
        )
    ):
        session.execute(SHOW_WEATHER_STATIONS)


# commit changes and close the connection
session.commit()
session.close()
engine.dispose()
