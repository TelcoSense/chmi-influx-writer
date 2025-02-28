import json
import os
from collections.abc import Mapping

import pandas as pd

# script for merging weather station metadata from multiple years and respective months


# merging funcs
def deep_merge(*dicts):
    merged = {}
    for d in dicts:
        for key, value in d.items():
            if (
                isinstance(value, Mapping)
                and key in merged
                and isinstance(merged[key], Mapping)
            ):
                merged[key] = deep_merge(merged[key], value)
            elif (
                isinstance(value, list)
                and key in merged
                and isinstance(merged[key], list)
            ):
                merged[key] = merge_lists(merged[key], value)
            else:
                merged[key] = value
    return merged


def merge_lists(list1, list2):
    result = []
    result.extend(list1)
    result.extend(list2)
    return sorted(pd.DataFrame(result).drop_duplicates().values.tolist())


def extract_chmi_metadata(path: str) -> tuple[list, list]:
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
    headers = data["data"]["data"]["header"].split(",")
    values = data["data"]["data"]["values"]
    return headers, values


def add_measurements(
    ws_dict: dict, values: list, meas: str = "10M"
) -> tuple[dict, list]:
    prev_wsi = None
    measurements = []
    for value in values:
        if meas in value:
            current_wsi = value[1]
            try:
                if current_wsi != prev_wsi:
                    ws_dict[current_wsi][meas] = []
                ws_dict[current_wsi][meas].append(value[2:-1])
                measurements.append(value[2:-1])
            except KeyError:
                print(f"WSI {current_wsi} not found.")
                break
            prev_wsi = current_wsi
    measurements_df = pd.DataFrame(measurements).drop_duplicates()
    return ws_dict, sorted(measurements_df.values.tolist())


def process_metadata(year: int, month: int) -> list:
    input_dir = f"{year}/metadata/{month:02d}"
    output_dir = f"{year}/processed_metadata/{month:02d}"
    os.makedirs(output_dir, exist_ok=True)
    meta1 = f"{input_dir}/meta1-{year}{month:02d}.json"
    headers, values = extract_chmi_metadata(meta1)
    ws_dict = {}
    for value in values:
        # sometimes there are spaces in the weather station ids
        wsi = value[0].replace(" ", "")
        if wsi not in ws_dict:
            ws_dict[wsi] = dict(zip(headers[1:], value[1:]))
    meta2 = f"{input_dir}/meta2-{year}{month:02d}.json"
    headers, values = extract_chmi_metadata(meta2)
    # sometimes there are duplicate weather stations
    values_df = pd.DataFrame(values).drop_duplicates()
    # convert back to a list of lists
    values = values_df.values.tolist()
    ws_dict, measurements_10m = add_measurements(ws_dict, values, meas="10M")
    ws_dict, measurements_1h = add_measurements(ws_dict, values, meas="1H")
    ws_dict, measurements_dly = add_measurements(ws_dict, values, meas="DLY")
    measurements = {
        "10M": measurements_10m,
        "1H": measurements_1h,
        "DLY": measurements_dly,
    }
    with open(
        f"{output_dir}/meta-{year}{month:02d}.json", "w", encoding="utf-8"
    ) as file:
        json.dump(ws_dict, file, indent=4, ensure_ascii=False)
    with open(
        f"{output_dir}/measurements-{year}{month:02d}.json", "w", encoding="utf-8"
    ) as file:
        json.dump(measurements, file, indent=4, ensure_ascii=False)
    return ws_dict, measurements_10m, measurements_1h, measurements_dly


all_measurements_10m = []
all_measurements_1h = []
all_measurements_dly = []
ws_dicts = []
for year in [2024, 2025]:
    for month in range(1, 13):
        try:
            ws_dict, measurements_10m, measurements_1h, measurements_dly = (
                process_metadata(year, month)
            )
            ws_dicts.append(ws_dict)
            all_measurements_10m.extend(measurements_10m)
            all_measurements_1h.extend(measurements_1h)
            all_measurements_dly.extend(measurements_dly)
        except FileNotFoundError:
            pass

all_measurements_df_10m = pd.DataFrame(all_measurements_10m).drop_duplicates()
all_measurements_10m = sorted(all_measurements_df_10m.values.tolist())
with open(f"data_db/measurements_10m.json", "w", encoding="utf-8") as file:
    json.dump(all_measurements_10m, file, indent=4, ensure_ascii=False)

all_measurements_df_1h = pd.DataFrame(all_measurements_1h).drop_duplicates()
all_measurements_1h = sorted(all_measurements_df_1h.values.tolist())
with open(f"data_db/measurements_1h.json", "w", encoding="utf-8") as file:
    json.dump(all_measurements_1h, file, indent=4, ensure_ascii=False)

all_measurements_df_dly = pd.DataFrame(all_measurements_dly).drop_duplicates()
all_measurements_dly = sorted(all_measurements_df_dly.values.tolist())
with open(f"data_db/measurements_dly.json", "w", encoding="utf-8") as file:
    json.dump(all_measurements_dly, file, indent=4, ensure_ascii=False)


merged_ws_dict = {}
for json_data in ws_dicts:
    merged_ws_dict = deep_merge(merged_ws_dict, json_data)


with open(f"data_db/weather_stations.json", "w", encoding="utf-8") as file:
    json.dump(merged_ws_dict, file, indent=4, ensure_ascii=False)
