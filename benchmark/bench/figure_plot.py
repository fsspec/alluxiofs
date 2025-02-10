import argparse

import matplotlib

matplotlib.use("TkAgg")
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def convert_to_MB(value):
    result = float(value) / (1024 * 1024)
    result = round(result, 1)
    return str(result) + "MiB/s"


# Function to convert 'MiB/s' string to numeric value in MB (float)
def convert_to_numeric(bw_str):
    # Extract the number part and convert to float
    if "MiB/s" in bw_str:
        value = float(bw_str.split("MiB/s")[0])
    elif "KiB/s" in bw_str:
        value = float(bw_str.split("KiB/s")[0]) / 1024
    return value


def plot_show(fuse_df, fsspec_df):
    # Define block sizes and number of jobs for plotting
    block_sizes = ["4KB", "256KB", "1024KB"]
    num_jobs = [1, 4, 16]
    file_sizes = ["1MB", "10MB", "100MB", "1GB"]

    # Create a subplot with 3 rows and 3 columns (for each block_size and numjobs)
    fig, axs = plt.subplots(3, 3, figsize=(18, 18))

    # Iterate over each block_size and numjobs combination to create individual plots
    for i, block_size in enumerate(block_sizes):
        for j, numjob in enumerate(num_jobs):
            ax = axs[i, j]

            # Filter data for the current block size and number of jobs
            fuse_filtered = fuse_df[
                (fuse_df["bs"] == block_size) & (fuse_df["numjobs"] == numjob)
            ].copy()
            fsspec_filtered = fsspec_df[
                (fsspec_df["bs"] == block_size)
                & (fsspec_df["numjobs"] == numjob)
            ].copy()

            fuse_filtered["bw_numeric"] = fuse_filtered["bw"].apply(
                convert_to_numeric
            )
            fsspec_filtered["bw_numeric"] = fsspec_filtered["bw"].apply(
                convert_to_numeric
            )

            # Aggregate data to ensure only one row per file size (average bandwidth)
            fuse_filtered = (
                fuse_filtered.groupby("file_size")
                .agg({"bw_numeric": "mean"})
                .reset_index()
            )
            fsspec_filtered = (
                fsspec_filtered.groupby("file_size")
                .agg({"bw_numeric": "mean"})
                .reset_index()
            )

            # Ensure the data aligns with the file_sizes order
            fuse_filtered = (
                fuse_filtered.set_index("file_size")
                .reindex(file_sizes)
                .reset_index()
            )
            fsspec_filtered = (
                fsspec_filtered.set_index("file_size")
                .reindex(file_sizes)
                .reset_index()
            )

            # Plot the Fuse bandwidth bars for each file size
            ax.bar(
                np.arange(len(file_sizes)) - 0.2,
                fuse_filtered["bw_numeric"],
                0.4,
                label=f"Fuse {block_size}",
                color="blue",
            )

            # Plot the FSSpec bandwidth bars for each file size
            ax.bar(
                np.arange(len(file_sizes)) + 0.2,
                fsspec_filtered["bw_numeric"],
                0.4,
                label=f"FSSpec {block_size}",
                color="orange",
            )

            # Customize plot labels and legend
            ax.set_xlabel("File Size")
            ax.set_ylabel("Bandwidth (MiB/s)")
            ax.set_title(f"Block Size: {block_size}, NumJobs: {numjob}")
            ax.set_xticks(np.arange(len(file_sizes)))
            ax.set_xticklabels(file_sizes)
            ax.legend()

    # Adjust layout to prevent overlap
    plt.tight_layout()
    plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fuse_summarize_dir",
        required=True,
        help="Directory containing summarize file of fuse",
    )
    parser.add_argument(
        "--fsspec_summarize_dir",
        required=True,
        help="Directory containing summarize file of fsspec",
    )
    args = parser.parse_args()
    # File paths to the CSV files
    fuse_csv_path = args.fuse_summarize_dir
    fsspec_csv_path = args.fsspec_summarize_dir

    # Read the CSV files into DataFrames
    fuse_df = pd.read_csv(fuse_csv_path)
    fsspec_df = pd.read_csv(fsspec_csv_path)

    # Call the plot_show function to visualize the results
    plot_show(fuse_df, fsspec_df)


if __name__ == "__main__":
    main()
