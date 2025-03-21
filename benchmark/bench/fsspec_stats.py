import argparse
import json
import os
import re
from pathlib import Path

import pandas as pd


def convert_to_MB(value):
    result = float(value) / (1024 * 1024)
    result = round(result, 1)
    return str(result) + "MiB/s"


# Function to convert 'MiB/s' string to numeric value in MB (float)
def convert_to_numeric(bw_str):
    # Extract the number part and convert to float
    value = float(bw_str.split("MiB/s")[0])
    return value


def convert_to_bytes(size_str):
    match = re.match(r"(\d+)([KMGT]?[B]?)", size_str.strip())

    if not match:
        raise ValueError("Invalid size string format")

    number = int(match.group(1))
    unit = match.group(2).upper()

    unit_mapping = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }
    multiplier = unit_mapping.get(unit, 1)
    return number * multiplier


def size_divide(value1, value2):
    return convert_to_bytes(value1) / convert_to_bytes(value2)


def process_iops(file_templete_name, iops):
    file_size = file_templete_name.split("_")[3]
    bs = file_templete_name.split("_")[4]
    iops = float(iops) * size_divide(file_size, bs)
    return int(iops)


def parse_fsspec_log(file_path):
    """Parse a single FSSPEC log (JSON format) and extract relevant metrics"""
    with open(file_path, "r") as f:
        log_data = json.load(f)
    file_templete_name = str(file_path.parent).split("/")[-1]
    # Extract the relevant metrics
    data = {
        "rw": "read"
        if file_templete_name.split("_")[1] == "download"
        else "write",
        "bw": convert_to_MB(log_data["metrics"].get("bytes_per_second", 0)),
        "iops": process_iops(
            file_templete_name, log_data["metrics"].get("ops_per_second", 0)
        ),
        "file_size": file_templete_name.split("_")[3],
        "bs": file_templete_name.split("_")[4],
        "numjobs": file_templete_name.split("_")[5],
        "worker": log_data.get("worker", 0),
        "lat_avg(usec)": None,  # lat_avg is not available in fsspec logs directly
        "duration": log_data["metrics"].get("duration", 0),
        "total_ops": log_data["metrics"].get("total_ops", 0),
        "total_bytes": log_data["metrics"].get("total_bytes", 0),
        "file_name": file_templete_name,
    }

    return data


def parse_all_fsspec_logs(directory):
    """Parse all FSSPEC logs in a directory"""
    results = []
    for log_file in Path(directory).rglob("*.json"):
        log_data = parse_fsspec_log(log_file)
        if log_data:
            results.append(log_data)
    return results


def summarize_results(results):
    """Summarize results using Pandas DataFrame"""
    df = pd.DataFrame(results)
    df["bw_numeric"] = df["bw"].apply(convert_to_numeric)

    # Group the data by 'file_name' and calculate the mean of 'bs' and 'bw_numeric'
    df_grouped = df.groupby("file_name", as_index=False).agg(
        {
            "rw": "first",  # Keep the first 'rw' of each 'file_name'
            "file_size": "first",  # Keep the first 'file_size' of each 'file_name'
            "bs": "first",  # Calculate the mean of 'bs'
            "numjobs": "first",  # Keep the first 'numjobs' of each 'file_name'
            "bw_numeric": "sum",  # Calculate the sum of 'bw_numeric'
            "iops": "first",  # Keep the first 'iops' of each 'file_name'
            "worker": "first",  # Keep the first 'worker' of each 'file_name'
        }
    )
    # Convert the average 'bw_numeric' back to the original string format
    df_grouped["bw"] = df_grouped["bw_numeric"].apply(
        lambda x: str(round(x, 1)) + "MiB/s"
    )

    # Drop the 'bw_numeric' column as it's no longer needed
    df_grouped = df_grouped.drop(columns=["bw_numeric"])

    # Sort and select the desired columns
    df = df_grouped.sort_values(by="file_name", ascending=True)[
        ["rw", "file_size", "bs", "numjobs", "bw", "iops"]
    ]
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inputs_dir",
        required=True,
        help="Directory containing FSSPEC log files",
    )
    parser.add_argument(
        "--outputs_dir",
        required=True,
        help="Directory to save output CSV files",
    )
    parser.add_argument("--batch", required=False, help="If read_batch")
    args = parser.parse_args()

    inputs_dir = args.inputs_dir
    outputs_dir = args.outputs_dir
    # inputs_dir = "./bench_result/fsspec-benchmark"
    # outputs_dir = "./results"
    os.makedirs(outputs_dir, exist_ok=True)

    logs = parse_all_fsspec_logs(inputs_dir)
    if not logs:
        print(f"No valid FSSPEC log files found in {inputs_dir}")
        return

    df = summarize_results(logs)

    output_csv = os.path.join(outputs_dir, "fsspec_summarize_results.csv")
    df.to_csv(output_csv, index=False)

    print("\n\n\nThe benchmark of fsspec is completed!")
    print(f"Fsspec's results saved to {output_csv}")
    print("\nFSSpec:")
    print(df)


if __name__ == "__main__":
    main()
