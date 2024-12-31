import json
import os
import sys
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


def bankniftyfutcall(base_url, rest_url, call_headers, csv_columns):
    """
    This module contains code for fetching and processing
    option chain data for Bank Nifty.
    """
    folder_path = os.path.join(os.getcwd(), "history", "bniftyfut")
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one.
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    url = base_url + rest_url

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):

        try:
            response = get_session(url, base_url, call_headers)
        except Exception as e:
            print(e)
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        status = json.loads(response.content)["marketStatus"]["marketOpenOrClose"]
        if status != "Open":
            print(status)
            continue

        try:
            record = json.loads(response.content)
        except Exception as e:
            print(e)
            continue

        if record is None:
            print("No record!")
            continue

        turnover = 0
        for data in record["data"]:
            turnover = turnover + int(data["totalTurnover"])

        df = pd.read_csv(csv, skip_blank_lines=True)
        if turnover in df["bankniftyfuttotalTurnover"].values:
            continue

        timestamp = json.loads(response.content)["timestamp"][12:17]
        if timestamp in df["time"].values:
            df = df[df["time"] != timestamp]
            df_cleaned = df.dropna(how="all")
            df_cleaned.to_csv(csv, index=False)

        df = pd.read_csv(csv, skip_blank_lines=True)
        if 0 in df["bankniftyfutdiff"].values:
            diff = turnover - df["bankniftyfuttotalTurnover"].iloc[-1]
            if diff < 0:
                continue
        else:
            diff = 0

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)
        newline = str(
            "\n" + str(turnover) + "," + str(diff) + "," + str(timestamp),
        )

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


def niftyfutcall(base_url, rest_url, call_headers, csv_columns):
    """
    This module contains code for fetching and processing
    option chain data for Bank Nifty.
    """
    folder_path = os.path.join(os.getcwd(), "history", "niftyfut")
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one.
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    url = base_url + rest_url

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):

        try:
            response = get_session(url, base_url, call_headers)
        except Exception as e:
            print(e)
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        status = json.loads(response.content)["marketStatus"]["marketOpenOrClose"]
        if status != "Open":
            print(status)
            continue

        try:
            record = json.loads(response.content)
        except Exception as e:
            print(e)
            continue

        if record is None:
            print("No record!")
            continue

        turnover = 0
        for data in record["data"]:
            turnover = turnover + int(data["totalTurnover"])

        df = pd.read_csv(csv, skip_blank_lines=True)
        if turnover in df["niftyfuttotalTurnover"].values:
            continue

        timestamp = json.loads(response.content)["timestamp"][12:17]
        if timestamp in df["time"].values:
            df = df[df["time"] != timestamp]
            df_cleaned = df.dropna(how="all")
            df_cleaned.to_csv(csv, index=False)

        df = pd.read_csv(csv, skip_blank_lines=True)
        if 0 in df["niftyfutdiff"].values:
            diff = turnover - df["niftyfuttotalTurnover"].iloc[-1]
            if diff < 0:
                continue
        else:
            diff = 0

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)
        newline = str(
            "\n" + str(turnover) + "," + str(diff) + "," + str(timestamp),
        )

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


def niftyopt(base_url, rest_url, call_headers, csv_columns):
    """
    This module contains code for fetching and processing
    option chain data for Bank Nifty.
    """
    folder_path = os.path.join(os.getcwd(), "history", "nifty")
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one.
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    url = base_url + rest_url

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):

        try:
            response = get_session(url, base_url, call_headers)
        except Exception as e:
            print(e)
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        status = json.loads(response.content)["marketStatus"]["marketOpenOrClose"]
        if status != "Open":
            print(status)
            continue

        try:
            record = json.loads(response.content)
        except Exception as e:
            print(e)
            continue

        if record is None:
            print("No record!")
            continue

        turnover = 0
        for data in record["data"]:
            turnover = turnover + int(data["totalTurnover"])

        df = pd.read_csv(csv, skip_blank_lines=True)
        if turnover in df["niftytotalTurnover"].values:
            continue

        timestamp = json.loads(response.content)["timestamp"][12:17]
        if timestamp in df["time"].values:

            df = df[df["time"] != timestamp]
            df_cleaned = df.dropna(how="all")
            df_cleaned.to_csv(csv, index=False)

        df = pd.read_csv(csv, skip_blank_lines=True)
        if 0 in df["niftydiff"].values:
            diff = turnover - df["niftytotalTurnover"].iloc[-1]
            if diff < 0:
                continue
        else:
            diff = 0

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)
        newline = str(
            "\n" + str(turnover) + "," + str(diff) + "," + str(timestamp),
        )

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


def niftycall(base_url, rest_url, call_headers, csv_columns):
    """
    banknifty option chain data

    """
    folder_path = os.path.join(os.getcwd(), "history", "niftychain")
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    url = base_url + rest_url

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):

        try:
            response = get_session(url, base_url, call_headers)
        except Exception as e:
            print(e)
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        try:
            data = json.loads(response.content)["records"]["data"]
        except Exception as e:
            print(e)
            continue

        if data is None:
            print("None data")
            continue

        volume = 0
        buyorders = 0
        sellorders = 0

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

        if volume < 0:
            continue

        df = pd.read_csv(csv, skip_blank_lines=True)
        if volume in df["niftyvolume"].values:
            continue

        timestamp = json.loads(response.content)["records"]["timestamp"][12:17]
        if timestamp in df["time"].values:
            df = df[df["time"] != timestamp]

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)
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

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


def bankniftyopt(base_url, rest_url, call_headers, csv_columns):
    """
    This module contains code for fetching and processing
    option chain data for Bank Nifty.
    """
    folder_path = os.path.join(os.getcwd(), "history", "bnifty")
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one.
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    url = base_url + rest_url

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):

        try:
            response = get_session(url, base_url, call_headers)
        except Exception as e:
            print(e)
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        status = json.loads(response.content)["marketStatus"]["marketOpenOrClose"]
        if status != "Open":
            print(status)
            continue

        try:
            record = json.loads(response.content)
        except Exception as e:
            print(e)
            continue

        if record is None:
            print("No record!")
            continue

        turnover = 0
        for data in record["data"]:
            turnover = turnover + int(data["totalTurnover"])

        df = pd.read_csv(csv, skip_blank_lines=True)
        if turnover in df["bankniftytotalTurnover"].values:
            continue

        timestamp = json.loads(response.content)["timestamp"][12:17]
        if timestamp in df["time"].values:
            df = df[df["time"] != timestamp]
            df_cleaned = df.dropna(how="all")
            df_cleaned.to_csv(csv, index=False)

        df = pd.read_csv(csv, skip_blank_lines=True)
        if 0 in df["bankniftydiff"].values:
            diff = turnover - df["bankniftytotalTurnover"].iloc[-1]
            if diff < 0:
                continue
        else:
            diff = 0

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)
        newline = str(
            "\n" + str(turnover) + "," + str(diff) + "," + str(timestamp),
        )

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


def bankniftycall(base_url, rest_url, call_headers, csv_columns):
    """
    banknifty option chain data

    """
    folder_path = os.path.join(os.getcwd(), "history", "bniftychain")
    csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

    # Check if csv file exists. If don't then create one
    if not os.path.exists(csv):
        with open(csv, "w", encoding="utf-8") as f_name:
            f_name.write(csv_columns)

    url = base_url + rest_url

    while (
        not datetime.strptime("15:35:00", "%H:%M:%S").time()
        <= datetime.now().time()
        <= datetime.strptime("15:40:00", "%H:%M:%S").time()
    ):

        try:
            response = get_session(url, base_url, call_headers)
        except Exception as e:
            print(e)
            continue

        if response.status_code != 200:
            print(response.status_code)
            continue

        try:
            data = json.loads(response.content)["records"]["data"]
        except Exception as e:
            print(e)
            continue

        if data is None:
            print("None data")
            continue

        volume = 0
        buyorders = 0
        sellorders = 0

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

        if volume < 0:
            continue

        df = pd.read_csv(csv, skip_blank_lines=True)
        if volume in df["bankniftyvolume"].values:
            continue

        timestamp = json.loads(response.content)["records"]["timestamp"][12:17]
        if timestamp in df["time"].values:
            df = df[df["time"] != timestamp]

        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(1)
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

        with open(csv, "a", encoding="utf-8") as f_name:
            f_name.write(newline)
        df = pd.read_csv(csv, skip_blank_lines=True)
        df_cleaned = df.dropna(how="all")
        df_cleaned.to_csv(csv, index=False)
        time.sleep(3)
    print("Market Closed!")


if __name__ == "__main__":
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 "
            + "Safari/537.36 Edg/108.0.1462.76"
        ),
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,mr;q=0.7",
        "accept-encoding": "gzip, deflate, br",
    }
    WEBSITE = "https://www.nseindia.com/"

    banknifty_opt_thread = threading.Thread(
        target=bankniftyopt,
        args=(
            WEBSITE,
            "api/liveEquity-derivatives?index=nifty_bank_opt",
            headers,
            "bankniftytotalTurnover,bankniftydiff,time",
        ),
    )

    bankniftycall_thread = threading.Thread(
        target=bankniftycall,
        args=(
            WEBSITE,
            "api/option-chain-indices?symbol=BANKNIFTY",
            headers,
            "bankniftyvolume,bankniftybuyorders,bankniftysellorders,time",
        ),
    )

    nifty_opt_thread = threading.Thread(
        target=niftyopt,
        args=(
            WEBSITE,
            "api/liveEquity-derivatives?index=nse50_opt",
            headers,
            "niftytotalTurnover,niftydiff,time",
        ),
    )

    niftycall_thread = threading.Thread(
        target=niftycall,
        args=(
            WEBSITE,
            "api/option-chain-indices?symbol=NIFTY",
            headers,
            "niftyvolume,niftybuyorders,niftysellorders,time",
        ),
    )

    nifty_fut_thread = threading.Thread(
        target=niftyfutcall,
        args=(
            WEBSITE,
            "api/liveEquity-derivatives?index=nse50_fut",
            headers,
            "niftyfuttotalTurnover,niftyfutdiff,time",
        ),
    )

    banknifty_fut_thread = threading.Thread(
        target=bankniftyfutcall,
        args=(
            WEBSITE,
            "api/liveEquity-derivatives?index=nifty_bank_fut",
            headers,
            "bankniftyfuttotalTurnover,bankniftyfutdiff,time",
        ),
    )

    banknifty_opt_thread.start()
    bankniftycall_thread.start()
    nifty_opt_thread.start()
    niftycall_thread.start()
    # nifty_fut_thread.start()
    # banknifty_fut_thread.start()

    banknifty_opt_thread.join()
    bankniftycall_thread.join()
    nifty_opt_thread.join()
    niftycall_thread.join()
    # nifty_fut_thread.join()
    # banknifty_fut_thread.join()
