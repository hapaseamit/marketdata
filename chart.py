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
    plt.rcParams["toolbar"] = "None"
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

    def animatechart(i):
        folder_path = os.path.join(os.getcwd(), "history", "nifty")
        nifty_csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")
        folder_path = os.path.join(os.getcwd(), "history", "niftychain")
        nifty_chain_csv = os.path.join(
            folder_path, str(datetime.today().date()) + ".csv"
        )
        folder_path = os.path.join(os.getcwd(), "history", "bnifty")
        bnifty_csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")
        folder_path = os.path.join(os.getcwd(), "history", "bniftychain")
        bchain_csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")
        folder_path = os.path.join(os.getcwd(), "history", "niftyfut")
        niftyfut_csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")
        folder_path = os.path.join(os.getcwd(), "history", "bniftyfut")
        bniftyfut_csv = os.path.join(folder_path, str(datetime.today().date()) + ".csv")

        chart_path = os.path.join(os.getcwd(), "images")
        chart = os.path.join(chart_path, "chart.png")
        while True:
            try:
                data1 = pd.read_csv(nifty_csv, skip_blank_lines=True)
                data2 = pd.read_csv(nifty_chain_csv, skip_blank_lines=True)
                data3 = pd.read_csv(bnifty_csv, skip_blank_lines=True)
                data4 = pd.read_csv(bchain_csv, skip_blank_lines=True)
                data5 = pd.read_csv(niftyfut_csv, skip_blank_lines=True)
                data6 = pd.read_csv(bniftyfut_csv, skip_blank_lines=True)
            except Exception as e:
                e = "Exception Occured!"
                print(e)
                continue
            break
        # Merging data
        data = pd.merge(data1, data2, on="time", how="inner")
        data = pd.merge(data, data3, on="time", how="inner")
        data = pd.merge(data, data4, on="time", how="inner")
        data = pd.merge(data, data5, on="time", how="inner")
        data = pd.merge(data, data6, on="time", how="inner")

        data = data[
            (data["niftydiff"] != 0)
            & (data["bankniftydiff"] != 0)
            & (data["bankniftyfutdiff"] != 0)
            & (data["niftyfutdiff"] != 0)
        ]

        data["nifty_bar_color"] = data.apply(
            lambda row: (
                "#089981"
                if row["niftybuyorders"] > row["niftysellorders"]
                else (
                    "#f23645"
                    if row["niftysellorders"] > row["niftybuyorders"]
                    else "white"
                )
            ),
            axis=1,
        )

        data["banknifty_bar_color"] = data.apply(
            lambda row: (
                "#089981"
                if row["bankniftybuyorders"] > row["bankniftysellorders"]
                else (
                    "#f23645"
                    if row["bankniftysellorders"] > row["bankniftybuyorders"]
                    else "white"
                )
            ),
            axis=1,
        )

        for ax in axs:
            ax.clear()

        axs[0].bar(
            data["time"],
            data["niftydiff"],
            color=data["nifty_bar_color"],
        )
        axs[1].bar(
            data["time"],
            data["niftyfutdiff"],
            color="#9598a1",
        )
        axs[2].bar(
            data["time"],
            data["bankniftydiff"],
            color=data["banknifty_bar_color"],
        )
        axs[3].bar(
            data["time"],
            data["bankniftyfutdiff"],
            color="#9598a1",
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)
        axs[0].set_title(
            "Nifty",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "Nifty Future",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[2].set_title(
            "BankNifty",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[3].set_title(
            "BankNifty Future",
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
