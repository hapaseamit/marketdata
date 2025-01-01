import os
from datetime import datetime

import matplotlib.pyplot as plt

# import mplcursors
import pandas as pd
from matplotlib.animation import FuncAnimation


def run_script():
    """
    This function runs the script.
    """
    # plt.rcParams["toolbar"] = "None"
    # Set the background color of the entire window
    plt.rcParams["figure.facecolor"] = "#131722"

    plt.style.use("fivethirtyeight")
    fig, axs = plt.subplots(
        2,
        1,
        figsize=(10, 8),
        # sharex=True,
    )

    # Set background color
    fig.patch.set_facecolor("#131722")
    for ax in axs:
        ax.set_facecolor("#131722")

    def animatechart(i):
        folder_path = os.path.join(os.getcwd(), "history", "niftyturnover")
        niftyturnover_csv = os.path.join(
            folder_path, str(datetime.today().date()) + ".csv"
        )
        folder_path = os.path.join(os.getcwd(), "history", "bniftyturnover")
        bniftyturnover_csv = os.path.join(
            folder_path, str(datetime.today().date()) + ".csv"
        )

        chart_path = os.path.join(os.getcwd(), "images")
        chart = os.path.join(chart_path, "chart.png")
        while True:
            try:
                niftyturnoverdata = pd.read_csv(
                    niftyturnover_csv, skip_blank_lines=True
                )
                bniftyturnoverdata = pd.read_csv(
                    bniftyturnover_csv, skip_blank_lines=True
                )
            except Exception as e:
                e = "Exception Occured!"
                print(e)
                continue
            break

        # Calculate volume differences
        niftyturnoverdata["niftyturnover_diff"] = (
            niftyturnoverdata["niftytotalTurnover"].diff().fillna(0)
        )
        bniftyturnoverdata["bniftyturnover_diff"] = (
            bniftyturnoverdata["bankniftytotalTurnover"].diff().fillna(0)
        )

        # Merging data
        turnoverdata = pd.merge(
            niftyturnoverdata, bniftyturnoverdata, on="time", how="inner"
        )

        # Remove rows where the difference is less than or equal to zero
        turnoverdata = turnoverdata[
            (turnoverdata["niftyturnover_diff"] > 0)
            & (turnoverdata["bniftyturnover_diff"] > 0)
        ]

        for ax in axs:
            ax.clear()

        axs[0].bar(
            turnoverdata["time"],
            turnoverdata["niftyturnover_diff"],
            color="#9598a1",
        )
        axs[1].bar(
            turnoverdata["time"],
            turnoverdata["bniftyturnover_diff"],
            color="#9598a1",
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        axs[0].set_title(
            "Nifty Turnover",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "BankNifty Turnover",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )

        plt.tight_layout()
        plt.savefig(chart, facecolor="#131722", bbox_inches="tight")

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


run_script()
