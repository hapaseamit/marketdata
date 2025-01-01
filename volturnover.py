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
        6,
        1,
        figsize=(10, 8),
        # sharex=True,
    )

    # Set background color
    fig.patch.set_facecolor("#131722")
    for ax in axs:
        ax.set_facecolor("#131722")

    def animatechart(i):
        folder_path = os.path.join(os.getcwd(), "history", "niftyvolume")
        niftyvolume_csv = os.path.join(
            folder_path, str(datetime.today().date()) + ".csv"
        )
        folder_path = os.path.join(os.getcwd(), "history", "bniftyvolume")
        bniftyvolume_csv = os.path.join(
            folder_path, str(datetime.today().date()) + ".csv"
        )

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
                niftyvoldata = pd.read_csv(niftyvolume_csv, skip_blank_lines=True)
                bniftyvoldata = pd.read_csv(bniftyvolume_csv, skip_blank_lines=True)
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

        # Calculate buy & sell orders differences
        niftyvoldata["niftybuyorders_diff"] = (
            niftyvoldata["niftybuyorders"].diff().fillna(0).abs()
        )
        niftyvoldata["niftysellorders_diff"] = (
            niftyvoldata["niftysellorders"].diff().fillna(0).abs()
        )
        bniftyvoldata["bniftybuyorders_diff"] = (
            bniftyvoldata["bankniftybuyorders"].diff().fillna(0).abs()
        )
        bniftyvoldata["bniftysellorders_diff"] = (
            bniftyvoldata["bankniftysellorders"].diff().fillna(0).abs()
        )

        # Calculate turnover differences
        niftyturnoverdata["niftyturnover_diff"] = (
            niftyturnoverdata["niftytotalTurnover"].diff().fillna(0)
        )
        bniftyturnoverdata["bniftyturnover_diff"] = (
            bniftyturnoverdata["bankniftytotalTurnover"].diff().fillna(0)
        )

        # Merging data
        volumedata = pd.merge(niftyvoldata, bniftyvoldata, on="time", how="inner")
        turnoverdata = pd.merge(
            niftyturnoverdata, bniftyturnoverdata, on="time", how="inner"
        )

        # Remove rows where the difference is less than or equal to zero
        # volumedata = volumedata[
        #     (volumedata["niftyvolume_diff"] > 0) & (volumedata["bniftyvolume_diff"] > 0)
        # ]

        # Remove rows where the difference is less than or equal to zero
        volumedata = volumedata[
            (volumedata["niftybuyorders_diff"] > 0)
            & (volumedata["niftysellorders_diff"] > 0)
            & (volumedata["bniftybuyorders_diff"] > 0)
            & (volumedata["bniftysellorders_diff"] > 0)
        ]
        turnoverdata = turnoverdata[
            (turnoverdata["niftyturnover_diff"] > 0)
            & (turnoverdata["bniftyturnover_diff"] > 0)
        ]

        # Merging data
        data = pd.merge(volumedata, turnoverdata, on="time", how="inner")

        for ax in axs:
            ax.clear()

        # axs[0].bar(
        #     data["time"],
        #     data["niftyvolume_diff"],
        #     color="#9598a1",
        # )
        # axs[1].bar(
        #     data["time"],
        #     data["bniftyvolume_diff"],
        #     color="#9598a1",
        # )
        axs[0].bar(
            data["time"],
            data["niftyturnover_diff"],
            color="#9598a1",
        )
        axs[1].bar(
            data["time"],
            data["niftybuyorders_diff"],
            color="#089981",
        )
        axs[2].bar(
            data["time"],
            data["niftysellorders_diff"],
            color="#f23645",
        )
        axs[3].bar(
            data["time"],
            data["bniftyturnover_diff"],
            color="#9598a1",
        )
        axs[4].bar(
            data["time"],
            data["bniftybuyorders_diff"],
            color="#089981",
        )
        axs[5].bar(
            data["time"],
            data["bniftysellorders_diff"],
            color="#f23645",
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        # axs[0].set_title(
        #     "Nifty Volume",
        #     loc="left",
        #     color="#9598a1",
        #     fontsize=12,
        # )
        # axs[1].set_title(
        #     "Banknifty Volume",
        #     loc="left",
        #     color="#9598a1",
        #     fontsize=12,
        # )
        axs[0].set_title(
            "Nifty Turnover",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "Nifty Buy Orders",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[2].set_title(
            "Nifty Sell Order",
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
        axs[4].set_title(
            "BankNifty Buy Orders",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[5].set_title(
            "BankNifty Sell Order",
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
