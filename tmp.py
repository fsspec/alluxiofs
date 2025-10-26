import json
import multiprocessing
import shutil
import threading
import time
import argparse

import bosfs
import numpy as np
from mcap.reader import make_reader
from alluxiofs import AlluxioFileSystem
import fsspec
import sys

from mcap.writer import Writer


def run_sequential_read_test(process_id, alluxio_path, alluxio_fs, topics_to_read, block_size):
    """
    Performs a sequential read of SPECIFIED TOPICS from the MCAP file and measures throughput.
    """
    import traceback
    print(f"before read: in_alluxio_percentage: {alluxio_fs.info(alluxio_path)["in_alluxio_percentage"]}")
    try:
        # The block_size parameter is for fsspec's internal buffering and can be adjusted as needed.
        with alluxio_fs.open(alluxio_path, "rb", block_size=block_size) as f:
        # with open(local_path, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()

            total_bytes_read = 0
            message_count = 0

            print(f"[{process_id}] Starting sequential read of topics {topics_to_read} from {alluxio_path}...")

            # Start the timer
            start_time = time.perf_counter()
            # f.cache.start_topic_prefetch(topics=topics_to_read)
            # *** MODIFIED: Use the topics parameter to iterate over specified topics only ***
            for message_tuple in reader.iter_messages(topics=topics_to_read):
                if message_tuple:
                    message_count += 1
                    total_bytes_read += len(message_tuple[2].data)

            # Stop the timer
            end_time = time.perf_counter()
            total_time = end_time - start_time

        # Calculate performance metrics
        throughput_mb_s = (total_bytes_read / (1024 * 1024)) / total_time if total_time > 0 else 0
        messages_per_sec = message_count / total_time if total_time > 0 else 0

        # --- Output the summary ---
        print("\n--- Sequential Read Test Summary ---")
        print(f"Process ID:           {process_id}")
        print(f"Topics Read:          {', '.join(topics_to_read)}")
        print(f"Total Messages Read:  {message_count:,}")
        print(f"Total Data Read:      {total_bytes_read / (1024 * 1024):.2f} MB")
        print(f"Total Time Elapsed:   {total_time:.2f} seconds")
        print("--------------------------------------")
        print(f"🚀 Average Throughput: {throughput_mb_s:.2f} MB/s")
        print(f"🚀 Messages per Second: {messages_per_sec:,.2f} msgs/s")
        print("--------------------------------------\n")
    except Exception as e:
        print(f"Error in process {process_id}: {traceback.print_exc()}", file=sys.stderr)


def generate_data(local_path, output_file, num_messages, num_topics, alluxio_fs):
    """Generates a large MCAP file with each topic's messages stored contiguously."""
    print(f"Generating MCAP file with {num_messages} messages across {num_topics} topics at '{output_file}'...")
    try:
        if alluxio_fs.exists(output_file):
            print("  - Existing file found. Deleting it first...")
            alluxio_fs.rm(output_file)
        print(f"  - File existence check after potential deletion: {alluxio_fs.exists(output_file)}")
    except Exception as e:
        print(f"Warning: Could not check or delete existing file: {e}. Attempting to proceed.", file=sys.stderr)

    local_temp_path = local_path + ".tmp"

    try:
        with open(local_temp_path, "wb") as f:
            writer = Writer(f, compression=None)
            try:
                writer.start(profile="x-custom", library="perf-test-gen")

                schema_id = writer.register_schema(
                    name="my_schema",
                    encoding="jsonschema",
                    data=b'{"type":"object","properties":{"id":{"type":"integer"}, "payload":{"type":"string"}}}'
                )

                channel_ids = []
                for i in range(num_topics):
                    topic_name = f"/perf_topic_{i}"
                    channel_id = writer.register_channel(topic=topic_name, message_encoding="json", schema_id=schema_id)
                    channel_ids.append(channel_id)
                print(f"  - Registered {num_topics} topics.")

                # --- 数据聚合逻辑开始 ---
                start_time = time.time_ns()
                messages_per_topic = num_messages // num_topics
                payload = "A" * 1024 * 50  # 50KB payload
                msg_id = 0

                for topic_idx, channel_id in enumerate(channel_ids):
                    topic_name = f"/perf_topic_{topic_idx}"
                    print(f"  - Writing {messages_per_topic} messages for {topic_name}...")
                    for j in range(messages_per_topic):
                        log_time = start_time + (msg_id * 10_000_000)
                        writer.add_message(
                            channel_id=channel_id,
                            log_time=log_time,
                            data=json.dumps({"id": msg_id, "payload": payload}).encode("utf-8"),
                            publish_time=log_time
                        )
                        msg_id += 1
                        if msg_id % 10000 == 0:
                            print(f"  ... {msg_id}/{num_messages} messages written")

                # 补写剩余部分
                remaining = num_messages - msg_id
                if remaining > 0:
                    print(f"  - Writing remaining {remaining} messages to last topic /perf_topic_{num_topics - 1}...")
                    for _ in range(remaining):
                        log_time = start_time + (msg_id * 10_000_000)
                        writer.add_message(
                            channel_id=channel_ids[-1],
                            log_time=log_time,
                            data=json.dumps({"id": msg_id, "payload": payload}).encode("utf-8"),
                            publish_time=log_time
                        )
                        msg_id += 1
                # --- 数据聚合逻辑结束 ---

            finally:
                print("  - Finalizing MCAP file...")
                writer.finish()

        print("✅ Generation complete.")

        target_path = output_file.replace("file://", "")
        shutil.move(local_temp_path, target_path)
        print(f"✅ Move/Upload complete to {target_path}.")

        file_info = alluxio_fs.info(output_file)
        print(f"after write: in_alluxio_percentage: {file_info.get('in_alluxio_percentage')}")

    except Exception as e:
        print(f"❌ Error during file generation or upload: {e}", file=sys.stderr)
        try:
            if shutil.os.path.exists(local_temp_path):
                shutil.os.remove(local_temp_path)
        except:
            pass
        sys.exit(1)


def run_latency_test(process_id, alluxio_path, alluxio_fs, topics_to_read, num_samples):
    """
    Measures the latency of reading individual messages for SPECIFIED TOPICS from the MCAP file.
    """
    try:
        with alluxio_fs.open(alluxio_path, "rb") as f:
            reader = make_reader(f)
            latencies_ms = []
            message_count = 0

            print(
                f"[{process_id}] Starting latency test for {num_samples} messages from topics {topics_to_read} in {alluxio_path}...")

            message_iterator = reader.iter_messages(topics=topics_to_read)

            # Warm-up read to exclude initial file open/seek overhead from the first measurement
            try:
                next(message_iterator)
                print(f"[{process_id}] Completed one warm-up read.")
            except StopIteration:
                print(f"[{process_id}] Not enough messages for a warm-up read.", file=sys.stderr)
                return  # Exit if the file is empty or has only one message

            # Main measurement loop
            while message_count < num_samples:
                start_time = time.perf_counter()
                try:
                    # The core operation being measured is the time it takes for the iterator to yield the next message
                    next(message_iterator)
                    end_time = time.perf_counter()

                    latency_seconds = end_time - start_time
                    latencies_ms.append(latency_seconds * 1000)  # Store in milliseconds
                    message_count += 1
                except StopIteration:
                    print(f"[{process_id}] Reached end of file after reading {message_count} messages.")
                    break  # Exit the loop if there are no more messages

        if not latencies_ms:
            print("No messages were read, cannot calculate latency statistics.", file=sys.stderr)
            return

        # --- Calculate and Output the summary ---
        p50 = np.percentile(latencies_ms, 50)
        p95 = np.percentile(latencies_ms, 95)
        p99 = np.percentile(latencies_ms, 99)
        avg = np.mean(latencies_ms)
        min_lat = np.min(latencies_ms)
        max_lat = np.max(latencies_ms)

        print("\n--- Message Read Latency Test Summary ---")
        print(f"Process ID:           {process_id}")
        print(f"Topics Read:          {', '.join(topics_to_read)}")
        print(f"Messages Sampled:     {message_count:,}")
        print("-----------------------------------------")
        print(f"📊 Average:      {avg:.4f} ms")
        print(f"📊 Median (p50): {p50:.4f} ms")
        print(f"📊 p95:          {p95:.4f} ms")
        print(f"📊 p99:          {p99:.4f} ms")
        print(f"📊 Min/Max:      {min_lat:.4f} ms / {max_lat:.4f} ms")
        print("-----------------------------------------\n")

    except Exception as e:
        print(f"Error in latency test for process {process_id}: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alluxio MCAP sequential read performance tester.")
    parser.add_argument("--process-id", default=1, help="Unique ID for this process.")
    parser.add_argument("--num-messages", default=150000,
                        help="Total number of messages to generate across all topics.")
    parser.add_argument("--num-topics", default=10,
                        help="Total number of topics to generate")
    parser.add_argument("--local-path", default="/home/yxd/alluxio/ufs/mcap/large_test.mcap",
                        help="Path to the MCAP file on local filesystem for data generation.")
    parser.add_argument("--ufs-path", default="file:///home/yxd/alluxio/ufs/large_test.mcap",
                        help="Path to the MCAP file. For cold reads, this is the destination. For hot reads, this is the source.")
    parser.add_argument("--load-balance-domain", default="localhost",
                        help="Alluxio load balance domain, e.g., localhost or a specific domain.")
    parser.add_argument("--hot-read", action="store_true",
                        help="If set, assumes the file already exists and is cached for a hot read test.")

    # *** NEW: Allows the user to specify which topics to read ***
    parser.add_argument("--topics-to-read", default="/perf_topic_0",
                        help="Comma-separated list of topics to read during the test.")
    # *** NEW: Arguments to control the test type and latency sampling ***
    parser.add_argument("--test-type", default="throughput", choices=["throughput", "latency"],
                        help="Type of test to run: 'throughput' for overall speed, 'latency' for per-message read time.")
    parser.add_argument("--latency-samples", type=int, default=10000,
                        help="Number of messages to sample for the latency test.")
    parser.add_argument("--num-threads", type=int, default=1,
                        help="Number of concurrent threads to run the read test.")

    args = parser.parse_args()

    # --- Setup Filesystem ---
    print("🔧 Setting up Alluxio filesystem...")
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    fsspec.register_implementation("bos", bosfs.BOSFileSystem)
    alluxio_fs = fsspec.filesystem(
        "alluxiofs",
        load_balance_domain=args.load_balance_domain,
        mcap_enabled=True,
        local_cache_dir="/tmp/local_cache/",
        # target_protocol="bos"
    )

    def read_task(process_id, alluxio_path, alluxio_fs, topics_to_read, num_samples):
        # while True:
        for i in range(5):
            run_sequential_read_test(process_id, alluxio_path, alluxio_fs, topics_to_read, num_samples)
            #time.sleep(0.1)

    # Convert the comma-separated string to a list
    topics_to_read_list = args.topics_to_read.split(',')

    # Decide whether to regenerate data based on the hot-read flag
    if not args.hot_read:
        # For a cold read, first generate a fresh data file
        generate_data(args.local_path, args.ufs_path, args.num_messages, args.num_topics, alluxio_fs)
    else:
        print("🔥 Hot read mode enabled. Skipping data generation.")

        # 创建一个列表来存放线程
        threads = []

        # if args.test_type == 'throughput':
        #     print(f"\n🚀 Running Throughput Test with {args.num_threads} threads...")
        #     # for i in range(1, args.num_threads + 1):
        #     #     thread_id = f"T{i}"  # 唯一的线程ID
        #     #     # 创建线程，目标函数为 run_sequential_read_test
        #     #     t = threading.Thread(
        #     #         target=read_task,
        #     #         args=(thread_id, args.ufs_path, alluxio_fs, [f"/perf_topic_{i-1}"], 100 * 2 ** 20)
        #     #     )
        #     #     threads.append(t)
        #     #     t.start()  # 启动线程
        #     run_sequential_read_test(77, args.ufs_path, alluxio_fs, [f"/perf_topic_0"], 8 * 1024 * 1024)
        processes = []
        if args.test_type == 'throughput':
            print(f"\n🚀 Running Throughput Test with {args.num_threads} processes...")
            for i in range(1, args.num_threads + 1):
                thread_id = f"T{i}"
                # 创建进程
                p = multiprocessing.Process(
                    target=read_task,
                    args=(thread_id, args.ufs_path, alluxio_fs, [f"/perf_topic_{(i - 1)%10}"], 16 * 2 ** 20)
                )
                processes.append(p)
                p.start()

        elif args.test_type == 'latency':
            print(f"\n⏱️ Running Latency Test with {args.num_threads} threads...")
            run_latency_test(1, args.ufs_path, alluxio_fs, topics_to_read_list, args.latency_samples)


        print("\n✅ All concurrent read tests finished.")
