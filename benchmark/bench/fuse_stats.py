import argparse
import os
import re
from pathlib import Path

import pandas as pd

templates = [
    r"rw=(?P<rw>\w+)",
    r".*bw=(?P<bw>[\d\.]+[KMGiB/s]+)",
    r".*IOPS=(?P<iops>[\d\.k]+)",
    r".*avg=(?P<lat_avg>[\d\.]+)",
    r"\blat \((?P<unit>\w+)\)",
]

unit_conversion = {"nsec": 1e-3, "usec": 1, "msec": 1e3, "sec": 1e6}

fio_patterns = [re.compile(template) for template in templates]


def unit_convert(value, unit):
    return round(float(value) * unit_conversion[unit], 2)


def parse_fio_log(file_path):
    with open(file_path, "r") as f:
        content = f.read()
    data = {}
    for fio_pattern in fio_patterns:
        match = fio_pattern.search(content)
        if match:
            data.update(match.groupdict())
    if "rw" in data:  # If the read-write operation is found
        data["bw"] = data.get("bw", None)
        data["iops"] = data.get("iops", None)
        data["lat_avg(usec)"] = unit_convert(
            data.get("lat_avg", "-1"), data.get("unit", "usec")
        )
        data["file_size"] = file_path.name.split("_")[2]
        data["bs"] = file_path.name.split("_")[3]
        data["numjobs"] = file_path.name.split("_")[4].split(".")[0]
        data["file_name"] = file_path.name
    return data


def parse_all_logs(directory):
    results = []
    for log_file in Path(directory).rglob("*.log"):
        log_data = parse_fio_log(log_file)
        if log_data:
            results.append(log_data)
    return results


def summarize_results(results):
    df = pd.DataFrame(results)
    df = df.sort_values(by="file_name", ascending=True)[
        ["rw", "file_size", "bs", "numjobs", "bw", "iops", "lat_avg(usec)"]
    ]
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inputs_dir",
        required=True,
        help="Directory containing FIO log files",
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

    # inputs_dir = "./bench_result/fuse-benchmark"
    # outputs_dir = "./results"

    os.makedirs(outputs_dir, exist_ok=True)

    logs = parse_all_logs(inputs_dir)
    if not logs:
        print(f"No valid FIO log files found in {inputs_dir}")
        return

    df = summarize_results(logs)

    output_csv = os.path.join(outputs_dir, "fuse_summarize_results.csv")
    df.to_csv(output_csv, index=False)

    print("\n\n\nThe benchmark of fuse is completed!")
    print(f"Fuse's results saved to {output_csv}")
    print("\nFuse:")
    print(df)


if __name__ == "__main__":
    main()
