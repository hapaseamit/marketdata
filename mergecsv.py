import os

import pandas as pd


def merge_csv_files(cwd, csv_file):
    # Paths to the CSV files
    folder_path = os.path.join(cwd, "history", "niftyvolume")
    niftyvolume_csv = os.path.join(folder_path, csv_file)

    folder_path = os.path.join(cwd, "history", "niftyfutturnover")
    niftyfutturnover_csv = os.path.join(folder_path, csv_file)

    folder_path = os.path.join(cwd, "history", "niftyturnover")
    niftyturnover_csv = os.path.join(folder_path, csv_file)

    # Load CSV files into dataframes
    try:
        df_niftyvolume = pd.read_csv(niftyvolume_csv)
        df_niftyfutturnover = pd.read_csv(niftyfutturnover_csv)
        df_niftyturnover = pd.read_csv(niftyturnover_csv)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    turnoverdata = pd.merge(
        df_niftyfutturnover,
        df_niftyturnover,
        on="time",
        how="inner",
    )

    merged_df = pd.merge(
        turnoverdata,
        df_niftyvolume,
        on="time",
        how="inner",
    )
    # Output merged data to a new CSV
    output_file = os.path.join(cwd, "history", "merged_data.csv")
    merged_df.to_csv(output_file, index=False)
    print(f"Merged data saved to {output_file}")


# Example usage
cwd = os.getcwd()  # Current working directory
csv_file = "03-Jan-2025.csv"
merge_csv_files(cwd, csv_file)
