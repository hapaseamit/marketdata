""" This script gets data from a website and writes it to a csv file."""

# pylint: disable=broad-except
# pylint: disable=W0719

import os
from datetime import datetime

import matplotlib.pyplot as plt
import mplcursors
import pandas as pd
from matplotlib.animation import FuncAnimation


def main():
    """
    This function runs the script.
    """
    plt.rcParams["toolbar"] = "None"
    # Set the background color of the entire window
    plt.rcParams["figure.facecolor"] = "#131722"

    plt.style.use("fivethirtyeight")
    fig, axs = plt.subplots(
        6,
        1,
        figsize=(16, 8),
        sharex=True,
    )

    # Set background color
    fig.patch.set_facecolor("#131722")
    for ax in axs:
        ax.set_facecolor("#131722")

    csv_file = str(datetime.today().date()) + ".csv"
    cwd = os.getcwd()
    # csv_file = "03-Jan-2025.csv"

    def animatechart(i):
        folder_path = os.path.join(cwd, "history", "niftyvolume")
        niftyvolume_csv = os.path.join(folder_path, csv_file)

        folder_path = os.path.join(cwd, "history", "niftyfutturnover")
        niftyfutturnover_csv = os.path.join(folder_path, csv_file)

        folder_path = os.path.join(cwd, "history", "niftyturnover")
        niftyturnover_csv = os.path.join(folder_path, csv_file)

        chart_path = os.path.join(cwd, "images")
        chart = os.path.join(chart_path, "chart.png")
        while True:
            try:
                niftyvoldata = pd.read_csv(
                    niftyvolume_csv,
                    skip_blank_lines=True,
                )
                niftyfutturnoverdata = pd.read_csv(
                    niftyfutturnover_csv,
                    skip_blank_lines=True,
                )
                niftyturnoverdata = pd.read_csv(
                    niftyturnover_csv,
                    skip_blank_lines=True,
                )
            except Exception:
                print("Exception Occured!")
                continue
            break

        # Scale niftybuyorders & niftysellorders columns
        min_value_buy = niftyvoldata["niftybuyorders"].min()
        max_value_buy = niftyvoldata["niftybuyorders"].max()

        niftyvoldata["scaled_niftybuyorders"] = (
            niftyvoldata["niftybuyorders"] - min_value_buy
        ) / (max_value_buy - min_value_buy) * 999 + 1

        min_value_sell = niftyvoldata["niftysellorders"].min()
        max_value_sell = niftyvoldata["niftysellorders"].max()

        niftyvoldata["scaled_niftysellorders"] = (
            niftyvoldata["niftysellorders"] - min_value_sell
        ) / (max_value_sell - min_value_sell) * 999 + 1

        niftyvoldata["color"] = (
            "red"
            if niftyvoldata["netorders"].diff().fillna(0).lt(0).sum()
            > niftyvoldata["netorders"].diff().gt(0).sum()
            else "green"
        )
        niftyvoldata["net_orders_color"] = (
            "green"
            if niftyvoldata["netorders"].diff().fillna(0).lt(0).sum()
            > niftyvoldata["netorders"].diff().gt(0).sum()
            else "red"
        )

        # Remove rows where the difference is less than or equal to zero
        # niftyvoldata = niftyvoldata[
        #     (niftyvoldata["niftyvolumediff"] > 0)
        #     # & (niftyvoldata["niftybuyordersdiff"] > 0)
        #     # & (niftyvoldata["niftysellordersdiff"] > 0)
        # ]
        # niftyturnoverdata = niftyturnoverdata[
        #     (niftyturnoverdata["niftyturnoverdiff"] > 0)
        #     & (niftyturnoverdata["niftyturnovervolumediff"] > 0)
        # ]
        # niftyfutturnoverdata = niftyfutturnoverdata[
        #     (niftyfutturnoverdata["niftyfutturnoverdiff"] > 0)
        #     & (niftyfutturnoverdata["niftyfutturnovervolumediff"] > 0)
        # ]

        # Merging data
        turnoverdata = pd.merge(
            niftyturnoverdata,
            niftyfutturnoverdata,
            on="time",
            how="inner",
        )

        # Merging data
        data = pd.merge(
            niftyvoldata,
            turnoverdata,
            on="time",
            how="inner",
        )
        # Writing data to a CSV file
        # data.to_csv("merged_data.csv", index=False)

        for ax in axs:
            ax.clear()

        axs[0].bar(
            data["time"],
            data["niftyfutturnovervolumediff"],
            color="#9598a1",
        )

        axs[1].bar(
            data["time"],
            data["niftyturnovervolumediff"],
            color="#9598a1",
        )
        axs[2].bar(
            data["time"],
            data["niftyvolumediff"],
            color=data["color"],
        )
        axs[3].bar(
            data["time"],
            data["niftybuyordersdiff"],
            color="#089981",
        )
        axs[4].bar(
            data["time"],
            data["niftysellordersdiff"],
            color="#f23645",
        )
        axs[5].bar(
            data["time"],
            data["scaled_niftybuyorders"],
            color=data["net_orders_color"],
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        axs[0].set_title(
            "Nifty Future Turnover Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "Nifty Options Turnover Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[2].set_title(
            "Nifty Option Chain Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[3].set_title(
            "Nifty Optin Chain Buy Orders Change",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[4].set_title(
            "Nifty Optin Chain Sell Orders Change",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[5].set_title(
            "Nifty Optin Chain Net Orders",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )

        plt.tight_layout()
        plt.savefig(
            chart,
            facecolor="#131722",
            bbox_inches="tight",
        )

        # # Create custom tooltip using mplcursors
        cursor = mplcursors.cursor(hover=mplcursors.HoverMode.Transient)

        @cursor.connect("add")
        def on_add(sel):
            index = sel.index
            x_value = data["time"].iloc[index]
            y_value = sel.artist[0].get_height()
            sel.annotation.set(
                # text=f"{x_value} \n {y_value}",
                text=f"{x_value}",
                position=(0, 2),
                anncoords="offset points",
                bbox=dict(
                    boxstyle="round,pad=0.1",
                    fc="#9598a1",
                    ec="black",
                    alpha=0.9,
                ),
                fontsize=10,
            )
            sel.annotation.xy = (x_value, y_value)

    ani = FuncAnimation(
        plt.gcf(),
        animatechart,
        interval=1000,
        cache_frame_data=False,
    )

    plt.show()


if __name__ == "__main__":
    main()
