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

    csv_file = str(datetime.today().date()) + ".csv"
    csv_file = "2025-01-01.csv"

    def animatechart(i):
        folder_path = os.path.join(os.getcwd(), "history", "niftyvolume")
        niftyvolume_csv = os.path.join(folder_path, csv_file)
        folder_path = os.path.join(os.getcwd(), "history", "bankniftyvolume")
        bniftyvolume_csv = os.path.join(folder_path, csv_file)

        chart_path = os.path.join(os.getcwd(), "images")
        chart = os.path.join(chart_path, "chart.png")
        while True:
            try:
                niftyvoldata = pd.read_csv(
                    niftyvolume_csv,
                    skip_blank_lines=True,
                )
                bniftyvoldata = pd.read_csv(
                    bniftyvolume_csv,
                    skip_blank_lines=True,
                )
            except Exception as e:
                e = "Exception Occured!"
                print(e)
                continue
            break

        # Calculate volume differences
        niftyvoldata["niftyvolume_diff"] = niftyvoldata["niftyvolume"].diff().fillna(0)
        bniftyvoldata["bniftyvolume_diff"] = (
            bniftyvoldata["bankniftyvolume"].diff().fillna(0)
        )

        # Merging data
        volumedata = pd.merge(
            niftyvoldata,
            bniftyvoldata,
            on="time",
            how="inner",
        )

        # Remove rows where the difference is less than or equal to zero
        volumedata = volumedata[
            (volumedata["niftyvolume_diff"] > 0) & (volumedata["bniftyvolume_diff"] > 0)
        ]

        for ax in axs:
            ax.clear()

        axs[0].bar(
            volumedata["time"],
            volumedata["niftyvolume_diff"],
            color="#9598a1",
        )
        axs[1].bar(
            volumedata["time"],
            volumedata["bniftyvolume_diff"],
            color="#9598a1",
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        axs[0].set_title(
            "Nifty Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "Banknifty Volume",
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
