""" This script gets data from a website and writes it to a csv file."""

import csv
import json
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests


def get_session(url, base_url, headers_ref):
    """This funcions returns a session object with cookies set."""
    session = requests.Session()
    while True:
        try:
            request = session.get(base_url, headers=headers_ref)
            cookies = dict(request.cookies)
            return session.get(url, headers=headers_ref, cookies=cookies)
        except Exception:
            print("Exception while creating session for", url)
            time.sleep(3)


def marketstatus():
    """This function checks market status and returns the status"""
    start_time = datetime.strptime("09:15:00", "%H:%M:%S").time()
    end_time = datetime.strptime("15:34:00", "%H:%M:%S").time()
    now = datetime.now().time()
    if start_time <= now <= end_time:
        return "Open"
    return "Closed"


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


def calculate_diff(csvfile, line, symbol):
    """This function calculates the difference between the last two rows"""
    df = pd.read_csv(csvfile, skip_blank_lines=True)
    if symbol == "niftyvolume":
        if 0 in df["niftyvolumediff"].values:
            line["niftyvolumediff"] = line["volume"] - df["niftyvolume"].iloc[-1]
        else:
            line["niftyvolumediff"] = 0
        if 0 in df["niftybuyordersdiff"].values:
            line["niftybuyordersdiff"] = (
                line["buyorders"] - df["niftybuyorders"].iloc[-1]
            )
        else:
            line["niftybuyordersdiff"] = 0
        if 0 in df["niftysellordersdiff"].values:
            line["niftysellordersdiff"] = (
                line["sellorders"] - df["niftysellorders"].iloc[-1]
            )
        else:
            line["niftysellordersdiff"] = 0
    elif symbol == "niftyfutturnover":
        if 0 in df["niftyfutturnoverdiff"].values:
            line["niftyfutturnoverdiff"] = (
                line["turnover"] - df["niftyfutturnover"].iloc[-1]
            )
        else:
            line["niftyfutturnoverdiff"] = 0
        if 0 in df["niftyfutturnovervolumediff"].values:
            line["niftyfutturnovervolumediff"] = (
                line["turnovervolume"] - df["niftyfutturnovervolume"].iloc[-1]
            )
        else:
            line["niftyfutturnovervolumediff"] = 0
    else:
        if 0 in df["niftyturnoverdiff"].values:
            line["niftyturnoverdiff"] = line["turnover"] - df["niftyturnover"].iloc[-1]
        else:
            line["niftyturnoverdiff"] = 0
        if 0 in df["niftyturnovervolumediff"].values:
            line["niftyturnovervolumediff"] = (
                line["turnovervolume"] - df["niftyturnovervolume"].iloc[-1]
            )
        else:
            line["niftyturnovervolumediff"] = 0
    line_list = [
        round(value) if isinstance(value, (float, int)) else value
        for value in line.values()
    ]
    return line_list


def process_data(record):
    """Processing data from the record"""
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


def get_data(base_url, rest_url, call_headers, csv_columns, symbol, datatype):
    """This module contains code for processing market data."""
    time.sleep(10)
    folder_path = os.path.join(os.getcwd(), "history", symbol)
    csvfile = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one
    if not os.path.exists(csvfile):
        with open(csvfile, "w", encoding="utf-8") as f_name:
            f_name.write(",".join(csv_columns))

    while marketstatus() == "Open":
        # set session
        try:
            response = get_session(base_url + rest_url, base_url, call_headers)
        except Exception:
            print("Exception while fetching response for", symbol)
            continue
        try:
            if response.status_code != 200:
                print("Error while status code for", symbol)
                continue
                # raise Exception
            try:
                if datatype == "volume":
                    record = json.loads(response.content).get("records")
                else:
                    record = json.loads(response.content)
            except Exception:
                print("Exception while fetching record for", symbol)
                continue
            if record is None:
                print("None record for", symbol)
                continue
                # raise Exception("None record for", symbol)

            timestamp = record["timestamp"][12:17]
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            if datatype == "volume":
                if process_data(record) is None:
                    print("Exception while processing data for", symbol)
                    continue
                volume, buyorders, sellorders = process_data(record)
                if volume < 0 or volume in df[symbol].values:
                    continue
                line = {
                    "volume": volume,
                    "buyorders": buyorders,
                    "sellorders": sellorders,
                    "netorders": buyorders - sellorders,  # Derived field
                    "timestamp": timestamp,
                }
            else:
                turnover = 0
                turnovervolume = 0
                for data in record["data"]:
                    turnover += int(data["totalTurnover"])
                    turnovervolume += int(data["volume"])
                if turnovervolume in df[symbol].values:
                    continue
                line = {
                    "turnover": turnover,
                    "turnovervolume": turnovervolume,
                    "timestamp": timestamp,
                }

            # check if timestamp is already present
            if timestamp in df["time"].values:
                df = df[df["time"] != timestamp]

            sort_csv(df).to_csv(csvfile, index=False)
            line_list = calculate_diff(csvfile, line, symbol)
            time.sleep(1)
            # Write data to csv
            with open(csvfile, "a", encoding="utf-8", newline="") as f_name:
                csv.writer(f_name).writerow(line_list)
            df = pd.read_csv(csvfile, skip_blank_lines=True)
            sort_csv(df).to_csv(csvfile, index=False)
            time.sleep(3)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            code_line_number = tb[-1].lineno
            print("General Exeption: Line number:", code_line_number)
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

    while marketstatus() == "Closed":
        print("Market is not opened yet!")
        time.sleep(3)
        os.system("clear")

    # Combined task list
    tasks = {
        "nifty_turnover": {
            "rest_url": "api/liveEquity-derivatives?index=nse50_opt",
            "csv_columns": [
                "niftyturnover",
                "niftyturnovervolume",
                "time",
                "niftyturnoverdiff",
                "niftyturnovervolumediff",
            ],
            "symbol": "niftyturnover",
            "datatype": "turnover",
        },
        "nifty_fut_turnover": {
            "rest_url": "api/liveEquity-derivatives?index=nse50_fut",
            "csv_columns": [
                "niftyfutturnover",
                "niftyfutturnovervolume",
                "time",
                "niftyfutturnoverdiff",
                "niftyfutturnovervolumediff",
            ],
            "symbol": "niftyfutturnover",
            "datatype": "turnover",
        },
        "nifty_volume": {
            "rest_url": "api/option-chain-indices?symbol=NIFTY",
            "csv_columns": [
                "niftyvolume",
                "niftybuyorders",
                "niftysellorders",
                "netorders",
                "time",
                "niftyvolumediff",
                "niftybuyordersdiff",
                "niftysellordersdiff",
            ],
            "symbol": "niftyvolume",
            "datatype": "volume",
        },
    }

    with ThreadPoolExecutor() as executor:
        for task in tasks.values():
            executor.submit(
                get_data,
                website_name,
                task["rest_url"],
                headers,
                task["csv_columns"],
                task["symbol"],
                task["datatype"],
            )


if __name__ == "__main__":
    main()
