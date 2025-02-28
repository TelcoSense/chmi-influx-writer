import os
from datetime import datetime, timezone

import requests


def get_file_links(folder_url):
    response = requests.get(folder_url)
    if response.status_code == 200:
        html_text = response.text
        return [
            folder_url + line.split('"')[1]
            for line in html_text.splitlines()
            if ".json" in line and 'href="' in line
        ]
    else:
        print(f"Failed to access folder {folder_url}: {response.status_code}")
        return []


def download_file_with_progress(file_url, local_folder):
    local_file_path = os.path.join(local_folder, os.path.basename(file_url))
    if not os.path.exists(local_file_path):
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with open(local_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {local_file_path}")
        else:
            print(f"Failed to download {file_url}: {response.status_code}")
    else:
        print(f"File already exists: {local_file_path}")


def main():
    folder_mappings = {}
    measurement_type = "daily"
    for month in range(1, 2):
        folder_mappings[
            f"https://opendata.chmi.cz/meteorology/climate/recent/data/{measurement_type}/{month:02d}/"
        ] = f"./2025/data/{measurement_type}/{month:02d}"
        folder_mappings[
            f"https://opendata.chmi.cz/meteorology/climate/recent/metadata/{month:02d}/"
        ] = f"./2025/metadata/{month:02d}"

    # ensure all specified local folders exist
    for local_folder in folder_mappings.values():
        os.makedirs(local_folder, exist_ok=True)
    for folder_url, local_folder in folder_mappings.items():
        print(f"Checking folder: {folder_url}")
        file_links = get_file_links(folder_url)
        for file_url in file_links:
            download_file_with_progress(file_url, local_folder)


if __name__ == "__main__":
    main()
