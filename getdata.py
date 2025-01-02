""" This script gets data from a website and writes it to a csv file."""

import json
import os
import threading
import time
from datetime import datetime

import pandas as pd
import requests


def get_session(url, base_url, headers_ref):
    """
    This funcions returns a session object with cookies set.
    """
    session = requests.Session()
    request = session.get(base_url, headers=headers_ref)
    cookies = dict(request.cookies)
    return session.get(url, headers=headers_ref, cookies=cookies)


def create_csv(csv_columns, symbol):
    """
    This function creates a folder and a csv file if not already present.
    """
    folder_path = os.path.join(os.getcwd(), "history", symbol)
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)
    return csv


def write_csv(newline, csv):
    """
    This function writes a new line to a csv file.
    """
    with open(csv, "a", encoding="utf-8") as f_name:
        f_name.write(newline)
    df = pd.read_csv(csv, skip_blank_lines=True)
    df_cleaned = df.dropna(how="all")
    df_cleaned.to_csv(csv, index=False)
    time.sleep(3)


def check_timestamp(df, timestamp, csv):
    """
    This function checks if the timestamp already exists in the csv file.
    """
    if timestamp in df["time"].values:
        df = df[df["time"] != timestamp]
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)

    df_cleaned = df.dropna(how="all")
    df_cleaned.to_csv(csv, index=False)
    time.sleep(1)


def get_volume(base_url, rest_url, call_headers, csv_columns, symbol):
    """
    This module contains code for processing volume data.
    """
    csv = create_csv(csv_columns, symbol)

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):
        try:
            response = get_session(base_url + rest_url, base_url, call_headers)
        except Exception:
            print("Exception while fetching response")
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        try:
            record = json.loads(response.content)["records"]
        except Exception:
            print("Exception while fetching record")
            continue

        if record is None:
            print("None record")
            continue

        volume = 0
        buyorders = 0
        sellorders = 0

        try:
            for data in json.loads(response.content)["records"]["data"]:

                volume = (
                    volume
                    + (data.get("CE", {"totalTradedVolume": 0})["totalTradedVolume"])
                    + (data.get("PE", {"totalTradedVolume": 0})["totalTradedVolume"])
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
        except Exception:
            print("Exception while additioing values")
            continue

        if volume < 0:
            continue

        df = pd.read_csv(csv, skip_blank_lines=True)
        if volume in df[symbol].values:
            continue

        timestamp = json.loads(response.content)["records"]["timestamp"][12:17]
        check_timestamp(df, timestamp, csv)
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
        write_csv(newline, csv)
    print("Market Closed!")


def get_turnover(base_url, rest_url, call_headers, csv_columns, symbol):
    """
    This module contains code for processing turnover data.
    """
    csv = create_csv(csv_columns, symbol)

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):
        try:
            response = get_session(base_url + rest_url, base_url, call_headers)
        except Exception:
            print("Exception while fetching response")
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        try:
            status = json.loads(response.content)["marketStatus"]["marketOpenOrClose"]
        except Exception:
            print("Exception while fetching market status")
            continue

        if status != "Open":
            print(status)
            continue

        try:
            record = json.loads(response.content)
        except Exception:
            print("Exception while fetching record")
            continue

        if record is None:
            print("No record!")
            continue

        turnover = 0
        try:
            for data in record["data"]:
                turnover = turnover + int(data["totalTurnover"])
        except Exception:
            print("Exception while adding turnover values")
            continue

        df = pd.read_csv(csv, skip_blank_lines=True)
        if turnover in df[symbol].values:
            continue

        timestamp = json.loads(response.content)["timestamp"][12:17]
        check_timestamp(df, timestamp, csv)
        newline = str(
            "\n" + str(turnover) + "," + str(timestamp),
        )

        write_csv(newline, csv)
    print("Market Closed!")


def main():
    """
    Main function to start threads for fetching and processing
    option chain data for Nifty and Bank Nifty and their futures
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
            target=get_turnover,
            args=(
                website_name,
                f"api/liveEquity-derivatives?index={sub_url}",
                headers,
                f"{folder},time",
                folder,
            ),
        )
        threads.append(thread)

    for sym in ["nifty", "banknifty"]:
        thread = threading.Thread(
            target=get_volume,
            args=(
                website_name,
                f"api/liveEquity-derivatives?index={sym.upper()}",
                headers,
                f"{sym}volume,{sym}buyorders,{sym}sellorders,time",
                f"{sym}volume",
            ),
        )
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
