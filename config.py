import configparser
import logging

config = configparser.ConfigParser()
config.read("config.ini")

db_user = config["mariadb"]["user"]
db_password = config["mariadb"]["password"]
db_url = config["mariadb"]["url"]
db_name = config["mariadb"]["db_name"]


# connection to the db server
DB_SERVER_CONNECTION_STRING = (
    f"mariadb+mariadbconnector://{db_user}:{db_password}@{db_url}"
)

# connection to a single db on the server
DB_CONNECTION_STRING = (
    f"mariadb+mariadbconnector://{db_user}:{db_password}@{db_url}/{db_name}"
)

# loggers
realtime_logger = logging.getLogger("realtime_logger")
realtime_logger.setLevel(logging.INFO)

file_handler1 = logging.FileHandler("realtime.log")
file_handler1.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
realtime_logger.addHandler(file_handler1)


last_month_logger = logging.getLogger("last_month_logger")
last_month_logger.setLevel(logging.INFO)

file_handler2 = logging.FileHandler("last_month.log")
file_handler2.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
last_month_logger.addHandler(file_handler2)
