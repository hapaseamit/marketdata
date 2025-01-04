""" This script gets data from a website and writes it to a csv file."""

# pylint: disable=broad-except
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests


def get_session(url, base_url, headers_ref):
    """This funcions returns a session object with cookies set."""
    session = requests.Session()
    request = session.get(base_url, headers=headers_ref)
    cookies = dict(request.cookies)
    return session.get(url, headers=headers_ref, cookies=cookies)


def marketstatus(base_url, headers_ref, symbol):
    """This function checks market status and returns the status"""
    while True:
        try:
            for item in json.loads(
                get_session(
                    base_url + "api/marketStatus",
                    base_url,
                    headers_ref,
                ).content
            )["marketState"]:
                if item["index"] == "NIFTY 50":
                    return item["marketStatus"]
        except Exception:
            print("Exception while fetching market status for", symbol)
            time.sleep(3)


def sort_csv(df):
    """This function sorts the csv file"""
    df = df.dropna(how="all")
    df["time"] = df["time"].str.strip()
    df = df.sort_values(
        by="time",
        key=lambda x: pd.to_datetime(
            x,
            format="%H:%M",
            errors="coerce",
        ),
    )
    return df


def process_data(record):
    """Processing data from the record"""
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


def get_data(base_url, rest_url, call_headers, csv_columns, symbol, datatype):
    """This module contains code for processing market data."""
    time.sleep(10)
    folder_path = os.path.join(os.getcwd(), "history", symbol)
    csvfile = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one
    if not os.path.exists(csvfile):
        with open(csvfile, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    while marketstatus(base_url, call_headers, symbol) == "Open":
        # set session
        try:
            response = get_session(base_url + rest_url, base_url, call_headers)
            if response.status_code != 200:
                raise Exception(response.status_code, symbol)
            if datatype == "volume":
                record = json.loads(response.content).get("records")
            else:
                record = json.loads(response.content)
            if record is None:
                raise Exception("None record for", symbol)

            timestamp = record["timestamp"][12:17]
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            if datatype == "volume":
                volume, buyorders, sellorders = process_data(record)
                if volume < 0 or volume in df[symbol].values:
                    continue
                line = [volume, buyorders, sellorders, timestamp]
            else:
                turnover = 0
                for data in record["data"]:
                    turnover += int(data["totalTurnover"])
                if turnover in df[symbol].values:
                    continue
                line = [turnover, timestamp]

            # check if timestamp is already present
            if timestamp in df["time"].values:
                df = df[df["time"] != timestamp]
                sort_csv(df).to_csv(csvfile, index=False)

            time.sleep(1)
            # Write data to csv
            with open(csvfile, "a", encoding="utf-8", newline="") as f_name:
                csv.writer(f_name).writerow(line)
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            sort_csv(df).to_csv(csvfile, index=False)
            time.sleep(3)
        except Exception:
            time.sleep(5)
            continue
    print("Market closed for", symbol)


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

    while marketstatus(website_name, headers, "initial check") == "Close":
        print("Market is not opened yet!")
        time.sleep(3)
        os.system("clear")

    # Combined task list
    tasks = [
        # Turnover tasks
        (
            "niftyturnover",
            "api/liveEquity-derivatives?index=nse50_opt",
            "turnover",
            "niftyturnover,time",
        ),
        (
            "niftyfutturnover",
            "api/liveEquity-derivatives?index=nse50_fut",
            "turnover",
            "niftyfutturnover,time",
        ),
        # Volume tasks
        (
            "niftyvolume",
            "api/option-chain-indices?symbol=NIFTY",
            "volume",
            "niftyvolume,niftybuyorders,niftysellorders,time",
        ),
    ]

    with ThreadPoolExecutor() as executor:
        for folder, rest_url, datatype, csv_columns in tasks:
            executor.submit(
                get_data,
                website_name,
                rest_url,
                headers,
                csv_columns,
                folder,
                datatype,
            )


if __name__ == "__main__":
    main()
