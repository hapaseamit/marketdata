import os
from datetime import datetime

import matplotlib.pyplot as plt
import mplcursors
import pandas as pd
from matplotlib.animation import FuncAnimation

from getdata import tasks


def main():
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

    today = datetime.today().date().strftime("%d-%b-%Y")
    csv_file = str(today) + ".csv"
    tsk = tasks()
    niftyopt = tsk["nifty_opt"]["symbol"]
    niftyfut = tsk["nifty_fut"]["symbol"]
    niftychain = tsk["nifty_chain"]["symbol"]
    cwd = os.getcwd()
    # csv_file = "03-Jan-2025.csv"
    chart_name = os.path.splitext(os.path.basename(__file__))[0]

    def animatechart(i):

        folder_path = os.path.join(cwd, "history", niftyopt)
        niftyopt_csv = os.path.join(folder_path, csv_file)

        folder_path = os.path.join(cwd, "history", niftyfut)
        niftyfut_csv = os.path.join(folder_path, csv_file)

        folder_path = os.path.join(cwd, "history", niftychain)
        niftychain_csv = os.path.join(folder_path, csv_file)

        chart_path = os.path.join(cwd, "images")
        chart = os.path.join(chart_path, chart_name + ".png")
        while True:
            try:
                niftyoptdata = pd.read_csv(
                    niftyopt_csv,
                    skip_blank_lines=True,
                )
                niftyfutdata = pd.read_csv(
                    niftyfut_csv,
                    skip_blank_lines=True,
                )
                niftychaindata = pd.read_csv(
                    niftychain_csv,
                    skip_blank_lines=True,
                )
            except Exception:
                print("Exception Occured!")
                continue
            break

        # get the difference of the columns
        niftyoptdata[f"{niftyopt}volumediff"] = (
            niftyoptdata[f"{niftyopt}volume"].diff().fillna(0)
        )
        niftyfutdata[f"{niftyfut}volumediff"] = (
            niftyfutdata[f"{niftyfut}volume"].diff().fillna(0)
        )
        niftychaindata[f"{niftychain}volumediff"] = (
            niftychaindata[f"{niftychain}volume"].diff().fillna(0)
        )
        niftychaindata[f"{niftychain}buyordersdiff"] = (
            niftychaindata[f"{niftychain}buyorders"].diff().fillna(0)
        )
        niftychaindata[f"{niftychain}sellordersdiff"] = (
            niftychaindata[f"{niftychain}sellorders"].diff().fillna(0)
        )
        # Scale niftybuyorders & niftysellorders columns
        min_value_buy = niftychaindata[f"{niftychain}buyorders"].min()
        max_value_buy = niftychaindata[f"{niftychain}buyorders"].max()

        niftychaindata[f"scaled_{niftychain}buyorders"] = (
            niftychaindata[f"{niftychain}buyorders"] - min_value_buy
        ) / (max_value_buy - min_value_buy) * 999 + 1

        min_value_sell = niftychaindata[f"{niftychain}sellorders"].min()
        max_value_sell = niftychaindata[f"{niftychain}sellorders"].max()

        niftychaindata[f"scaled_{niftychain}sellorders"] = (
            niftychaindata[f"{niftychain}sellorders"] - min_value_sell
        ) / (max_value_sell - min_value_sell) * 999 + 1

        niftychaindata["color"] = (
            "red"
            if niftychaindata["netorders"].diff().fillna(0).lt(0).sum()
            > niftychaindata["netorders"].diff().gt(0).sum()
            else "green"
        )
        niftychaindata["net_orders_color"] = (
            "green"
            if niftychaindata["netorders"].diff().fillna(0).lt(0).sum()
            > niftychaindata["netorders"].diff().gt(0).sum()
            else "red"
        )

        # Merging data
        opt_fut_data = pd.merge(
            niftyoptdata,
            niftyfutdata,
            on="time",
            how="inner",
        )

        # Merging data
        data = pd.merge(
            opt_fut_data,
            niftychaindata,
            on="time",
            how="inner",
        )

        for ax in axs:
            ax.clear()

        axs[0].bar(
            data["time"],
            data[f"{niftyfut}volumediff"],
            color="#9598a1",
        )

        axs[1].bar(
            data["time"],
            data[f"{niftyopt}volumediff"],
            color="#9598a1",
        )
        axs[2].bar(
            data["time"],
            data[f"{niftychain}volumediff"],
            color=data["color"],
        )
        axs[3].bar(
            data["time"],
            data[f"{niftychain}buyordersdiff"],
            color="#089981",
        )
        axs[4].bar(
            data["time"],
            data[f"{niftychain}sellordersdiff"],
            color="#f23645",
        )
        axs[5].bar(
            data["time"],
            data[f"scaled_{niftychain}buyorders"],
            color=data["net_orders_color"],
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        axs[0].set_title(
            "Nifty Future Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            "Nifty Options Volume",
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
            "Nifty Option Chain Net Orders",
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
