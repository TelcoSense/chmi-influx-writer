from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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


class WeatherStation(Base):
    __tablename__ = "weather_stations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wsi: Mapped[str] = mapped_column(String(255), nullable=False)
    gh_id: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    X: Mapped[float] = mapped_column(Float, nullable=False)
    Y: Mapped[float] = mapped_column(Float, nullable=False)
    elevation: Mapped[float] = mapped_column(Float, nullable=False)

    measurements_10m: Mapped[list["Measurement10M"]] = relationship(
        "Measurement10M",
        secondary=weather_station_measurements_10m,
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
