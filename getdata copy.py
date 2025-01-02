""" This script gets data from a website and writes it to a csv file."""

import json
import os
import threading
import time
from datetime import datetime

import pandas as pd
import requests


def process_data(record, datatype):
    """Processing data from the record"""
    if datatype == "volume":
        volume = 0
        buyorders = 0
        sellorders = 0
        for data in record["data"]:
            volume = (
                volume
                + data.get("CE", {"totalTradedVolume": 0})["totalTradedVolume"]
                + data.get("PE", {"totalTradedVolume": 0})["totalTradedVolume"]
            )

            buyorders = (
                buyorders
                + (data.get("CE", {"totalBuyQuantity": 0})["totalBuyQuantity"])
                + (data.get("PE", {"totalSellQuantity": 0})["totalSellQuantity"])
            )

            sellorders = (
                sellorders
                + (data.get("CE", {"totalSellQuantity": 0})["totalSellQuantity"])
                + (data.get("PE", {"totalBuyQuantity": 0})["totalBuyQuantity"])
            )

        return volume, buyorders, sellorders
    else:
        turnover = 0
        for data in record["data"]:
            turnover = turnover + int(data["totalTurnover"])
        return turnover


def get_data(base_url, rest_url, call_headers, csv_columns, symbol, datatype):
    """
    This module contains code for processing volume data.
    """
    # csv = create_csv(csv_columns, symbol)
    folder_path = os.path.join(os.getcwd(), "history", symbol)
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):
        try:
            session = requests.Session()
            request = session.get(base_url, headers=call_headers)
            cookies = dict(request.cookies)
            response = session.get(
                base_url + rest_url, headers=call_headers, cookies=cookies
            )
        except Exception:
            print("Exception while fetching response")
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        if datatype == "turnover":
            try:
                status = json.loads(response.content)["marketStatus"][
                    "marketOpenOrClose"
                ]
            except Exception:
                print("Exception while fetching market status")
                continue

            if status != "Open":
                print(status)
                continue

        try:
            if datatype == "volume":
                record = json.loads(response.content)["records"]
            else:
                record = json.loads(response.content)
            timestamp = record["timestamp"][12:17]
        except Exception:
            print("Exception while fetching record")
            continue

        if record is None:
            print("None record")
            continue

        try:
            if datatype == "volume":
                volume, buyorders, sellorders = process_data(record, datatype)
                if volume < 0:
                    continue
                df = pd.read_csv(csv, skip_blank_lines=True)
                if volume in df[symbol].values:
                    continue
                newline = str(
                    "\n"
                    + str(volume)
                    + ","
                    + str(buyorders)
                    + ","
                    + str(sellorders)
                    + ","
                    + str(timestamp),
                )
            else:
                turnover = process_data(record, datatype)
                df = pd.read_csv(csv, skip_blank_lines=True)
                if turnover in df[symbol].values:
                    continue
                newline = str("\n" + str(turnover) + "," + str(timestamp))
        except Exception:
            print("Exception while processing data")
            continue

        if timestamp in df["time"].values:
            df = df[df["time"] != timestamp]
            df_cleaned = df.dropna(how="all")
            df_cleaned.to_csv(csv, index=False)

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


def main():
    """
    Main function to start threads for fetching and processing data.
    """
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 "
            + "Safari/537.36 Edg/108.0.1462.76"
        ),
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,mr;q=0.7",
        "accept-encoding": "gzip, deflate, br",
    }
    website_name = "https://www.nseindia.com/"

    folders = [
        "niftyturnover",
        "bniftyturnover",
        "niftyfutturnover",
        "bniftyfutturnover",
    ]

    sub_urls = ["nse50_opt", "nifty_bank_opt", "nse50_fut", "nifty_bank_fut"]
    threads = []

    for folder, sub_url in zip(folders, sub_urls):
        thread = threading.Thread(
            target=get_data,
            args=(
                website_name,
                f"api/liveEquity-derivatives?index={sub_url}",
                headers,
                f"{folder},time",
                folder,
                "turnover",
            ),
        )
        threads.append(thread)

    for sym in ["nifty", "banknifty"]:
        thread = threading.Thread(
            target=get_data,
            args=(
                website_name,
                f"api/liveEquity-derivatives?index={sym.upper()}",
                headers,
                f"{sym}volume,{sym}buyorders,{sym}sellorders,time",
                f"{sym}volume",
                "volume",
            ),
        )
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
