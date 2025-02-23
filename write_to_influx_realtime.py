import configparser
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
import schedule
from tqdm import tqdm

logging.basicConfig(
    # filename="task_scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


config = configparser.ConfigParser()
config.read("config.ini")


def get_utc_date() -> str:
    """Get today's date (UTC time) or yesterday's date if the UTC hour is 0.

    Returns:
        str: Date string in the YYYYMMDD format.
    """
    now = datetime.now(timezone.utc)
    if now.hour == 0:
        return (now.date() - timedelta(days=1)).strftime("%Y%m%d")
    return now.date().strftime("%Y%m%d")


def get_file_urls(
    folder_url,
) -> list[str]:
    current_date = get_utc_date()
    response = requests.get(folder_url)
    if response.status_code == 200:
        html_text = response.text
        file_urls = [
            folder_url + line.split('"')[1]
            for line in html_text.splitlines()
            if ".json" in line
            and 'href="' in line
            and "10m" in line
            and current_date in line
        ]
        return file_urls
    else:
        logging.info(f"Failed to access folder {folder_url}: {response.status_code}")
        return []


def download_file(file_url, realtime_folder):
    local_file_path = os.path.join(realtime_folder, os.path.basename(file_url))
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        with open(local_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    else:
        logging.info(f"Failed to download {file_url}: {response.status_code}")


def write_latest_data():
    utc_now = datetime.now(timezone.utc)
    # subtract one hour from current utc time
    start_time = utc_now - timedelta(hours=1)
    # start at the hour mark
    start_time = start_time.replace(minute=0, second=0, microsecond=0)
    # delete the realtime folder and its contents
    realtime_folder = config.get("folders", "realtime_folder")

    # if os.path.exists(realtime_folder):
    #     shutil.rmtree(realtime_folder)

    # create it again
    os.makedirs(realtime_folder, exist_ok=True)
    # get the file urls to download
    file_urls = get_file_urls(config.get("folders", "chmi_now_folder"))
    logging.info("Downloading latest data from CHMI...")

    # for file_url in tqdm(file_urls, ascii=True):
    #     download_file(file_url, realtime_folder)

    data_files = os.listdir(realtime_folder)
    logging.info(f"Parsing data from {len(data_files)} weather stations.")
    for data_file in data_files[:1]:
        with open(
            os.path.join(realtime_folder, data_file), "r", encoding="utf-8"
        ) as file:
            data = json.load(file)
        print(start_time.date())
        date_string = start_time.strftime("%Y%m%d")
        # get current weather station id (WSI)
        wsi = data_file.removeprefix("10m-").removesuffix(f"-{date_string}.json")


def schedule_task():
    schedule.every().hour.at(":30").do(write_latest_data)


def run_scheduler():
    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except Exception as e:
            logging.error(f"Error in scheduler: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    logging.info("Starting the program...")
    # schedule_task()
    # run_scheduler()
    write_latest_data()


# def parse_utc_date(date_str):
#     return datetime.strptime(date_str, "%Y%m%d")

# local_folder = "realtime"
# os.makedirs(local_folder, exist_ok=True)

# file_links = get_file_links("https://opendata.chmi.cz/meteorology/climate/now/data/")
# print(len(file_links))

# for link in file_links:
#     download_file_with_progress(link, local_folder)
