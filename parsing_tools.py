import json

import pandas as pd


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


def process_metadata(input_dir, year, month) -> tuple[dict, list, list, list]:
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
    ws_dict, m10 = add_measurements(ws_dict, values, meas="10M")
    ws_dict, m1h = add_measurements(ws_dict, values, meas="1H")
    ws_dict, mdly = add_measurements(ws_dict, values, meas="DLY")
    return ws_dict, m10, m1h, mdly
