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
