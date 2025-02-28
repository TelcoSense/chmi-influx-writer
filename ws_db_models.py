from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# this file contains all table definitions for the chmi_metadata db


# must define a base class for sqlalchemy
# it is also used for creating the tables in the db
class Base(DeclarativeBase):
    pass


# junction table for the weather stations and the 10m measurements
weather_station_measurements_10m = Table(
    "weather_station_measurements_10m",
    Base.metadata,
    Column(
        "weather_station_id",
        Integer,
        ForeignKey("weather_stations.id"),
        primary_key=True,
    ),
    Column(
        "measurement_10m_id",
        Integer,
        ForeignKey("measurements_10m.id"),
        primary_key=True,
    ),
)

# junction table for the weather stations and the 1h measurements
weather_station_measurements_1h = Table(
    "weather_station_measurements_1h",
    Base.metadata,
    Column(
        "weather_station_id",
        Integer,
        ForeignKey("weather_stations.id"),
        primary_key=True,
    ),
    Column(
        "measurement_1h_id",
        Integer,
        ForeignKey("measurements_1h.id"),
        primary_key=True,
    ),
)

# junction table for the weather stations and the daily measurements
weather_station_measurements_dly = Table(
    "weather_station_measurements_dly",
    Base.metadata,
    Column(
        "weather_station_id",
        Integer,
        ForeignKey("weather_stations.id"),
        primary_key=True,
    ),
    Column(
        "measurement_dly_id",
        Integer,
        ForeignKey("measurements_dly.id"),
        primary_key=True,
    ),
)


class WeatherStation(Base):
    __tablename__ = "weather_stations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wsi: Mapped[str] = mapped_column(String(255), nullable=False)
    gh_id: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    X: Mapped[float] = mapped_column(Float, nullable=False)
    Y: Mapped[float] = mapped_column(Float, nullable=False)
    elevation: Mapped[float] = mapped_column(Float, nullable=False)
    # measurements
    measurements_10m: Mapped[list["Measurement10M"]] = relationship(
        "Measurement10M",
        secondary=weather_station_measurements_10m,
        back_populates="weather_stations",
    )
    measurements_1h: Mapped[list["Measurement1H"]] = relationship(
        "Measurement1H",
        secondary=weather_station_measurements_1h,
        back_populates="weather_stations",
    )
    measurements_dly: Mapped[list["MeasurementDLY"]] = relationship(
        "MeasurementDLY",
        secondary=weather_station_measurements_dly,
        back_populates="weather_stations",
    )


class Measurement10M(Base):
    __tablename__ = "measurements_10m"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    abbreviation: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit: Mapped[str] = mapped_column(String(255), nullable=False)

    weather_stations: Mapped[list["WeatherStation"]] = relationship(
        "WeatherStation",
        secondary=weather_station_measurements_10m,
        back_populates="measurements_10m",
    )


class Measurement1H(Base):
    __tablename__ = "measurements_1h"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    abbreviation: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit: Mapped[str] = mapped_column(String(255), nullable=False)

    weather_stations: Mapped[list["WeatherStation"]] = relationship(
        "WeatherStation",
        secondary=weather_station_measurements_1h,
        back_populates="measurements_1h",
    )


class MeasurementDLY(Base):
    __tablename__ = "measurements_dly"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    abbreviation: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit: Mapped[str] = mapped_column(String(255), nullable=False)

    weather_stations: Mapped[list["WeatherStation"]] = relationship(
        "WeatherStation",
        secondary=weather_station_measurements_dly,
        back_populates="measurements_dly",
    )
