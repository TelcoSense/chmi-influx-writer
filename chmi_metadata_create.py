import json

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from chmi_metadata_db import Base, Measurement10M, WeatherStation

DATABASE_SERVER_URL = f"mariadb+mariadbconnector://{""}:{""}@{""}"
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


DATABASE_URL = f"mariadb+mariadbconnector://{""}:{""}@{""}/{""}"
engine = create_engine(DATABASE_URL)

# create all tables
Base.metadata.create_all(engine)

# create session for adding rows to tables
session = Session(engine)

# add measurements
with open("measurements_10m.json", "r", encoding="utf-8") as file:
    measurements_10m = json.load(file)

for measurement in measurements_10m:
    measurement_db = Measurement10M(
        abbreviation=measurement[0], name=measurement[1], unit=measurement[2]
    )
    session.add(measurement_db)

# add weather stations and relations to measurements
with open("weather_stations.json", "r", encoding="utf-8") as file:
    weather_stations = json.load(file)

for wsi in weather_stations:
    weather_station = weather_stations[wsi]
    try:
        measurement_list = weather_station["10M"]
        weather_station_measurements = []
        # create list of measurements for current weather station
        for measurement in measurement_list:
            weather_station_measurements.append(
                session.scalars(
                    select(Measurement10M).where(
                        Measurement10M.abbreviation == measurement[0]
                    )
                ).first()
            )
        weather_station_db = WeatherStation(
            wsi=wsi,
            gh_id=weather_station["GH_ID"],
            full_name=weather_station["FULL_NAME"],
            X=weather_station["GEOGR1"],
            Y=weather_station["GEOGR2"],
            elevation=weather_station["ELEVATION"],
            measurements_10m=weather_station_measurements,
        )
        session.add(weather_station_db)
    except KeyError:
        # skip the stations that don't have 10m measurements
        pass


# define the SQL view
CREATE_VIEW_SQL = text(
    """
    CREATE VIEW show_weather_stations AS
    SELECT 
        ws.id, 
        ws.wsi, 
        ws.gh_id, 
        ws.full_name, 
        ws.X, 
        ws.Y, 
        ws.elevation, 
        GROUP_CONCAT(CONCAT(m10.name, ' [', m10.unit, ']') SEPARATOR ', ') AS measurements_10m
    FROM weather_stations ws
    JOIN weather_station_measurements_10m wsm ON ws.id = wsm.weather_station_id
    JOIN measurements_10m m10 ON wsm.measurement_10m_id = m10.id
    GROUP BY ws.id, ws.wsi, ws.gh_id, ws.full_name, ws.X, ws.Y, ws.elevation;
"""
)
if not session.scalar(
    text(
        "SELECT COUNT(*) FROM information_schema.views WHERE table_name = 'show_weather_stations'"
    )
):
    session.execute(CREATE_VIEW_SQL)

# commit changes and close the connection
session.commit()
session.close()
