import csv
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests

from .values import headers, tasks, website


def marketstatus():
    return (
        "Open"
        if datetime.strptime("09:15:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:34:00", "%H:%M:%S").time()
        else "Closed"
    )


def get_session(url, base_url, headers_ref):
    session = requests.Session()
    while True:
        try:
            cookies = dict(session.get(base_url, headers=headers_ref).cookies)
            return session.get(url, headers=headers_ref, cookies=cookies)
        except Exception:
            print("Exception while creating session for", url)
            time.sleep(3)


def sort_csv(df):
    df = df.dropna(how="all")
    df["time"] = df["time"].str.strip()
    df = df.sort_values(
        by="time",
        key=lambda x: pd.to_datetime(x, format="%H:%M", errors="coerce"),
    )
    return df


def process_opt_data(record, strikes):
    try:
        for data in record["data"]:
            if data.get("expiryDate") == strikes.get("expiryDate"):
                if data.get("optionType") == "Call":
                    if data.get("strikePrice") == strikes.get("ce_price"):
                        ce_volume = data.get("volume")
                        ce_oi = data.get("openInterest")
                        break
        for data in record["data"]:
            if data.get("expiryDate") == strikes.get("expiryDate"):
                if data.get("optionType") == "Put":
                    if data.get("strikePrice") == strikes.get("pe_price"):
                        pe_volume = data.get("volume")
                        pe_oi = data.get("openInterest")
                        break
        return (ce_volume, pe_volume, ce_oi, pe_oi)
    except Exception as e:
        return None


def process_chain_data(record, strikes):
    try:
        for data in record.get("data", []):
            if data.get("expiryDate") == strikes.get("expiryDate"):
                if data.get("strikePrice") == strikes.get("ce_price"):
                    ce_volume = data.get("CE", {"totalTradedVolume": 0})[
                        "totalTradedVolume"
                    ]
                    ce_oi = data.get("CE", {"openInterest": 0})["openInterest"]
                    break
        for data in record.get("data", []):
            if data.get("expiryDate") == strikes.get("expiryDate"):
                if data.get("strikePrice") == strikes.get("pe_price"):
                    pe_volume = data.get("PE", {"totalTradedVolume": 0})[
                        "totalTradedVolume"
                    ]
                    pe_oi = data.get("PE", {"openInterest": 0})["openInterest"]
                    break
        return (ce_volume, pe_volume, ce_oi, pe_oi)

    except Exception as e:
        return None


def get_data(base_url, rest_url, call_headers, csv_columns, symbol, strikes):
    time.sleep(10)
    today = datetime.today().date().strftime("%d-%b-%Y")
    folder_path = os.path.join(os.getcwd(), "history", symbol)
    os.makedirs(folder_path, exist_ok=True)
    csvfile = os.path.join(folder_path, str(today) + ".csv")

    if not os.path.exists(csvfile):
        with open(csvfile, "w", encoding="utf-8") as f_name:
            f_name.write(",".join(csv_columns))

    while marketstatus() == "Open":
        try:
            response = get_session(base_url + rest_url, base_url, call_headers)
            if response.status_code != 200:
                print("Error while status code for", symbol)
                continue
            if symbol == "niftychain":
                record = json.loads(response.content).get("records")
            else:
                record = json.loads(response.content)
            if record is None:
                print("None record for", symbol)
                continue

            timestamp = record["timestamp"][12:17]
            datestamp = record["timestamp"].split(" ")[0]
            if datestamp != str(today):
                print("Data is not yet updated for", symbol)
                time.sleep(3)
                os.system("clear")
                continue
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            if symbol == "niftychain":
                if (result := process_chain_data(record, strikes)) == None:
                    print("Exception while processing data for", symbol)
                    continue
                ce_volume, pe_volume, ce_oi, pe_oi = result
            else:
                if (result := process_opt_data(record, strikes)) == None:
                    print("Exception while processing data for", symbol)
                    continue
                ce_volume, pe_volume, ce_oi, pe_oi = result
            if ce_volume < 0 or ce_volume in df[f"{symbol}cevolume"].values:
                continue
            if pe_volume < 0 or pe_volume in df[f"{symbol}pevolume"].values:
                continue
            if timestamp in df["time"].values:
                df = df[df["time"] != timestamp]
            sort_csv(df).to_csv(csvfile, index=False)
            with open(csvfile, "a", encoding="utf-8", newline="") as f_name:
                csv.writer(f_name).writerow(
                    [timestamp, ce_volume, pe_volume, ce_oi, pe_oi]
                )
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            sort_csv(df).to_csv(csvfile, index=False)
            time.sleep(3)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            code_line_number = tb[-1].lineno
            print(f"General Exeption: Line number:{code_line_number}")
            time.sleep(5)
            continue
    print("Market closed for", symbol)


def main():

    while marketstatus() == "Closed":
        print("Market is not opened yet!")
        time.sleep(3)
        os.system("clear")

    with ThreadPoolExecutor() as executor:
        for value in tasks.values():
            executor.submit(
                get_data,
                website,
                value["rest_url"],
                headers,
                value["csv_columns"],
                value["symbol"],
                value["strikes"],
            )


if __name__ == "__main__":
    main()
