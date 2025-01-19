import os
from datetime import datetime

import matplotlib.pyplot as plt
import mplcursors
import pandas as pd
from matplotlib.animation import FuncAnimation

from .values import strikes, tasks


def main():
    plt.rcParams["toolbar"] = "None"
    # Set the background color of the entire window
    plt.rcParams["figure.facecolor"] = "#131722"

    plt.style.use("fivethirtyeight")
    fig, axs = plt.subplots(
        4,
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
    niftyopt = tasks["nifty_opt"]["symbol"]
    niftychain = tasks["nifty_chain"]["symbol"]
    ce_strike = strikes["ce_price"]
    pe_strike = strikes["pe_price"]
    cwd = os.getcwd()
    # csv_file = "03-Jan-2025.csv"
    chart_name = os.path.splitext(os.path.basename(__file__))[0]

    def animatechart(i):

        folder_path = os.path.join(cwd, "history", niftyopt)
        niftyopt_csv = os.path.join(folder_path, csv_file)

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
                niftychaindata = pd.read_csv(
                    niftychain_csv,
                    skip_blank_lines=True,
                )
            except Exception:
                print("Exception Occured!")
                continue
            break

        # get the difference of the columns
        niftyoptdata[f"{niftyopt}cevolumediff"] = (
            niftyoptdata[f"{niftyopt}cevolume"].diff().fillna(0)
        )
        niftyoptdata[f"{niftyopt}pevolumediff"] = (
            niftyoptdata[f"{niftyopt}pevolume"].diff().fillna(0)
        )
        niftychaindata[f"{niftychain}cevolumediff"] = (
            niftychaindata[f"{niftychain}cevolume"].diff().fillna(0)
        )
        niftychaindata[f"{niftychain}pevolumediff"] = (
            niftychaindata[f"{niftychain}pevolume"].diff().fillna(0)
        )

        niftyoptdata = niftyoptdata[
            (niftyoptdata[f"{niftyopt}cevolumediff"] > 0)
            & (niftyoptdata[f"{niftyopt}pevolumediff"] > 0)
        ]
        niftychaindata = niftychaindata[
            (niftychaindata[f"{niftychain}cevolumediff"] > 0)
            & (niftychaindata[f"{niftychain}pevolumediff"] > 0)
        ]

        # Merging data
        data = pd.merge(
            niftyoptdata,
            niftychaindata,
            on="time",
            how="inner",
        )

        for ax in axs:
            ax.clear()

        axs[0].bar(
            data["time"],
            data[f"{niftyopt}cevolumediff"],
            color="#9598a1",
        )
        axs[1].bar(
            data["time"],
            data[f"{niftyopt}pevolumediff"],
            color="#9598a1",
        )
        axs[2].bar(
            data["time"],
            data[f"{niftychain}cevolumediff"],
            color="#9598a1",
        )
        axs[3].bar(
            data["time"],
            data[f"{niftychain}pevolumediff"],
            color="#9598a1",
        )

        for ax in axs:
            ax.axis("off")

        for ax in axs:
            ax.autoscale(tight=True)

        axs[0].set_title(
            f"Nifty Opt CE {ce_strike} Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[1].set_title(
            f"Nifty Opt PE {pe_strike} Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[2].set_title(
            f"Nifty Chain CE {ce_strike} Volume",
            loc="left",
            color="#9598a1",
            fontsize=12,
        )
        axs[3].set_title(
            f"Nifty Chain PE {pe_strike} Volume",
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
