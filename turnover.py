""" This script gets data from a website and writes it to a csv file."""

import os
from datetime import datetime

import matplotlib.pyplot as plt

# import mplcursors
import pandas as pd
from matplotlib.animation import FuncAnimation


def main():
    """
    This function runs the script.
    """
    # plt.rcParams["toolbar"] = "None"
    # Set the background color of the entire window
    plt.rcParams["figure.facecolor"] = "#131722"

    plt.style.use("fivethirtyeight")
    fig, axs = plt.subplots(
        4,
        1,
        figsize=(10, 8),
        # sharex=True,
    )

    # Set background color
    fig.patch.set_facecolor("#131722")
    for ax in axs:
        ax.set_facecolor("#131722")

    today = datetime.today().date().strftime("%d-%b-%Y")
    csv_file = str(today) + ".csv"
    cwd = os.getcwd()
    # csv_file = "2025-01-01.csv"

    def animatechart(i):
        folder_path = os.path.join(cwd, "history", "niftyfutturnover")
        niftyfutturnover_csv = os.path.join(folder_path, csv_file)
        folder_path = os.path.join(cwd, "history", "bniftyfutturnover")
        bniftyfutturnover_csv = os.path.join(folder_path, csv_file)

        folder_path = os.path.join(cwd, "history", "niftyturnover")
        niftyturnover_csv = os.path.join(folder_path, csv_file)
        folder_path = os.path.join(cwd, "history", "bniftyturnover")
        bniftyturnover_csv = os.path.join(folder_path, csv_file)

        chart_path = os.path.join(cwd, "images")
        chart = os.path.join(chart_path, "chart.png")
        while True:
            try:
                niftyfutturnoverdata = pd.read_csv(
                    niftyfutturnover_csv,
                    skip_blank_lines=True,
                )
                bniftyfutturnoverdata = pd.read_csv(
                    bniftyfutturnover_csv,
                    skip_blank_lines=True,
                )
                niftyturnoverdata = pd.read_csv(
                    niftyturnover_csv,
                    skip_blank_lines=True,
                )
                bniftyturnoverdata = pd.read_csv(
                    bniftyturnover_csv,
                    skip_blank_lines=True,
                )
            except Exception as e:
                e = "Exception Occured!"
                print(e)
                continue
            break

        # Calculate volume differences
        niftyfutturnoverdata["niftyfutturnover_diff"] = (
            niftyfutturnoverdata["niftyfutturnover"].diff().fillna(0)
        )
        bniftyfutturnoverdata["bniftyfutturnover_diff"] = (
            bniftyfutturnoverdata["bniftyfutturnover"].diff().fillna(0)
        )
        niftyturnoverdata["niftyturnover_diff"] = (
            niftyturnoverdata["niftyturnover"].diff().fillna(0)
        )
        bniftyturnoverdata["bniftyturnover_diff"] = (
            bniftyturnoverdata["bniftyturnover"].diff().fillna(0)
        )

        # Merging data
        futturnoverdata = pd.merge(
            niftyfutturnoverdata,
            bniftyfutturnoverdata,
            on="time",
            how="inner",
        )
        turnoverdata = pd.merge(
            niftyturnoverdata, bniftyturnoverdata, on="time", how="inner"
        )

        # Remove rows where the difference is less than or equal to zero
        futturnoverdata = futturnoverdata[
            (futturnoverdata["niftyfutturnover_diff"] > 0)
            & (futturnoverdata["bniftyfutturnover_diff"] > 0)
        ]
        turnoverdata = turnoverdata[
            (turnoverdata["niftyturnover_diff"] > 0)
            & (turnoverdata["bniftyturnover_diff"] > 0)
        ]
        data = pd.merge(futturnoverdata, turnoverdata, on="time", how="inner")

        for ax in axs:
            ax.clear()

        axs[0].bar(
            data["time"],
            data["niftyfutturnover_diff"],
            color="#9598a1",
        )
        axs[1].bar(
            data["time"],
            data["niftyturnover_diff"],
            color="#9598a1",
        )
        axs[2].bar(
            data["time"],
            data["bniftyfutturnover_diff"],
            color="#9598a1",
        )
        axs[3].bar(
            data["time"],
            data["bniftyturnover_diff"],
            color="#9598a1",
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        axs[0].set_title(
            "Nifty Future Turnover",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "Nifty Turnover",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[2].set_title(
            "BankNifty Future Turnover",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[3].set_title(
            "BankNifty Turnover",
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
        # cursor = mplcursors.cursor(hover=mplcursors.HoverMode.Transient)

        # @cursor.connect("add")
        # def on_add(sel):
        #     index = sel.index
        #     x_value = data["time"].iloc[index]
        #     y_value = sel.artist[0].get_height()
        #     sel.annotation.set(
        #         # text=f"{x_value} \n {y_value}",
        #         text=f"{x_value}",
        #         position=(0, 2),
        #         anncoords="offset points",
        #         bbox=dict(
        #             boxstyle="round,pad=0.1",
        #             fc="#9598a1",
        #             ec="black",
        #             alpha=0.9,
        #         ),
        #         fontsize=10,
        #     )
        #     sel.annotation.xy = (x_value, y_value)

    ani = FuncAnimation(
        plt.gcf(),
        animatechart,
        interval=1000,
        cache_frame_data=False,
    )
    plt.show()


if __name__ == "__main__":
    main()
