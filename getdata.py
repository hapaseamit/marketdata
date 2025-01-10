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


def tasks():
    strikes = {
        "ce_price": 22500,
        "pe_price": 23500,
        "expiryDate": "16-Jan-2025",
    }
    return {
        "nifty_opt": {
            "rest_url": "api/liveEquity-derivatives?index=nse50_opt",
            "csv_columns": ["time", "niftyoptvolume"],
            "symbol": "niftyopt",
            "strikes": strikes,
        },
        "nifty_fut": {
            "rest_url": "api/liveEquity-derivatives?index=nse50_fut",
            "csv_columns": ["time", "niftyfutvolume"],
            "symbol": "niftyfut",
            "strikes": strikes,
        },
        "nifty_chain": {
            "rest_url": "api/option-chain-indices?symbol=NIFTY",
            "csv_columns": [
                "time",
                "ce_volume",
                "pe_volume",
            ],
            "symbol": "niftychain",
            "strikes": strikes,
        },
    }


def get_session(url, base_url, headers_ref):
    session = requests.Session()
    while True:
        try:
            cookies = dict(session.get(base_url, headers=headers_ref).cookies)
            return session.get(url, headers=headers_ref, cookies=cookies)
        except Exception:
            print("Exception while creating session for", url)
            time.sleep(3)


def marketstatus():
    return (
        "Open"
        if datetime.strptime("09:15:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:34:00", "%H:%M:%S").time()
        else "Closed"
    )


def sort_csv(df):
    df = df.dropna(how="all")
    df["time"] = df["time"].str.strip()
    df = df.sort_values(
        by="time",
        key=lambda x: pd.to_datetime(x, format="%H:%M", errors="coerce"),
    )
    return df


def process_data(record, strikes):
    try:
        ce_volume, pe_volume, count = 0, 0, 0

        for data in record.get("data", []):
            if data.get("expiryDate") == strikes.get("expiryDate"):
                if data.get("strikePrice") == strikes.get("ce_price"):
                    ce_volume = data.get("CE", {"totalTradedVolume": 0})[
                        "totalTradedVolume"
                    ]
                    count += 1
                if data.get("strikePrice") == strikes.get("pe_price"):
                    pe_volume = data.get("PE", {"totalTradedVolume": 0})[
                        "totalTradedVolume"
                    ]
                    count += 1
                if count == 2:
                    print(ce_volume, pe_volume)
                    break
        return (ce_volume, pe_volume)
    except Exception:
        return None


def process_data_old(record):
    try:
        volume, buyorders, sellorders = 0, 0, 0
        for data in record.get("data", []):
            if data.get("expiryDate") in record.get("expiryDates", [])[:10]:
                volume += (
                    data.get("CE", {"totalTradedVolume": 0})["totalTradedVolume"]
                    + data.get("PE", {"totalTradedVolume": 0})["totalTradedVolume"]
                )
                buyorders += (
                    data.get("CE", {"totalBuyQuantity": 0})["totalBuyQuantity"]
                ) + (data.get("PE", {"totalSellQuantity": 0})["totalSellQuantity"])
                sellorders += (
                    data.get("CE", {"totalSellQuantity": 0})["totalSellQuantity"]
                ) + (data.get("PE", {"totalBuyQuantity": 0})["totalBuyQuantity"])
        return (volume, buyorders, sellorders)
    except Exception:
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
            record = (
                json.loads(response.content).get("records")
                if symbol == "niftychain"
                else json.loads(response.content)
            )
            if record is None:
                print("None record for", symbol)
                continue

            timestamp = record["timestamp"][12:17]
            datestamp = record["timestamp"].split(" ")[0]
            if datestamp != str(today):
                print("Data is not yet updated for", symbol)
                continue
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            if symbol == "niftychain":
                if (result := process_data(record, strikes)) is None:
                    print("Exception while processing data for", symbol)
                    continue
                ce_volume, pe_volume = result
                if ce_volume < 0 or ce_volume in df["cevolume"].values:
                    continue
                if pe_volume < 0 or pe_volume in df["pevolume"].values:
                    continue
            else:
                volume = 0
                for data in record["data"]:
                    volume += int(data["volume"])
                if volume < 0 or volume in df[f"{symbol}volume"].values:
                    continue
            if timestamp in df["time"].values:
                df = df[df["time"] != timestamp]
            sort_csv(df).to_csv(csvfile, index=False)
            if symbol == "niftychain":
                row = [timestamp, ce_volume, pe_volume]
            else:
                row = [timestamp, volume]
            with open(csvfile, "a", encoding="utf-8", newline="") as f_name:
                csv.writer(f_name).writerow(row)
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
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 "
            + "Safari/537.36 Edg/108.0.1462.76"
        ),
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,mr;q=0.7",
        "accept-encoding": "gzip, deflate, br",
    }
    while marketstatus() == "Closed":
        print("Market is not opened yet!")
        time.sleep(3)
        os.system("clear")

    with ThreadPoolExecutor() as executor:
        for value in tasks().values():
            executor.submit(
                get_data,
                "https://www.nseindia.com/",
                value["rest_url"],
                headers,
                value["csv_columns"],
                value["symbol"],
                value["strikes"],
            )


if __name__ == "__main__":
    main()
