# An example that uses Alluxiofs to speed up pytorch distributed NLP training with large CSV files.
# The example demonstrates BERT fine-tuning.
# CSV files stored in an S3 directory are loaded into Alluxio first, reducing the time needed to pull data from
# the remote directory during training, decreasing the training time.
# When a specific line in a CSV file is requested by the training model, only that line is loaded into memory,
# preventing loading the entire CSV file into the memory, effectively optimizing memory usage.
# This helps solve the problems:
# 1) The total size of all CSV files needed for training is too large to fit into the memory
# 2) Any single CSV file is too large to fit into the memory
#
# How to use:
# 1) start an alluxio cluster https://github.com/Alluxio/alluxio
# 2) install dependencies specified below
# 3) run the following command in terminal:
#    python3 pytorch_distributed_BERT_training.py --total-epochs <epoch_number> --batch-size <batch_size> --directory-path <'your_s3_directory_path'>
#
# Dependencies: fsspec, alluxiofs, s3fs, torch, transformers, numpy, pandas, tqdm, bisect, os, time
import bisect
import os
import time

import fsspec
import numpy as np
import pandas as pd
import torch
import torch.distributed as dist
import torch.multiprocessing as mp
import torch.nn as nn
import torch.optim as optim
import transformers
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torch.utils.data import Dataset
from torch.utils.data import DistributedSampler
from tqdm import tqdm

from alluxiofs import AlluxioClient
from alluxiofs import AlluxioFileSystem


class BertDataset(Dataset):
    def __init__(
        self,
        alluxio_fs,
        tokenizer,
        max_length,
        directory_path,
        preprocessed_file_info,
        total_length,
        read_chunk_size,
        output_filename,
    ):
        super(BertDataset, self).__init__()
        self.alluxio_fs = alluxio_fs
        self.directory_path = directory_path

        self.tokenizer = tokenizer
        self.max_length = max_length

        self.preprocessed_file_info = preprocessed_file_info
        self.start_line_num_list = sorted(
            list(self.preprocessed_file_info.keys())
        )
        self.total_length = total_length
        self.read_chunk_size = read_chunk_size

        self.output_filename = output_filename  # for output recording purpose
        self.total_access = 0  # for output recording purpose

    def __len__(self):
        return self.total_length

    def __getitem__(self, index):
        """
        Map index into target line number in a specific file.
        To avoid a single big file overloading the memory, a file is read by chunk when accessing the target line.
        :param index: global index of the data point that the model trainer want to access
        :return: BERT specific tensors for the target data point
        """

        # find the target file and the target line where the index located
        target_file_index = (
            bisect.bisect_right(self.start_line_num_list, index) - 1
        )
        target_file_start_line_num = self.start_line_num_list[
            target_file_index
        ]
        target_file_name = self.preprocessed_file_info[
            target_file_start_line_num
        ]
        target_line_index = index - target_file_start_line_num

        # load target file in memory by chunk to avoid memory overloading and then read the target line
        chunk_number = target_line_index // self.read_chunk_size
        line_within_chunk = target_line_index % self.read_chunk_size
        chunk_iterator = pd.read_csv(
            self.alluxio_fs.open(target_file_name, mode="r"),
            chunksize=self.read_chunk_size,
        )

        for i, chunk in enumerate(chunk_iterator):
            # only the chunk will be loaded into memory each time
            # memory occupied by chunk will be freed once outside the loop
            if i == chunk_number:
                target_line = chunk.iloc[line_within_chunk]
                break

        # process target line text for BERT use
        inputs = self.tokenizer.encode_plus(
            target_line.iloc[0],
            None,
            padding="max_length",
            add_special_tokens=True,
            return_attention_mask=True,
            max_length=self.max_length,
            truncation=True,
        )
        ids = inputs["input_ids"]
        token_type_ids = inputs["token_type_ids"]
        mask = inputs["attention_mask"]

        return {
            "ids": torch.tensor(ids, dtype=torch.long),
            "mask": torch.tensor(mask, dtype=torch.long),
            "token_type_ids": torch.tensor(token_type_ids, dtype=torch.long),
            "target": torch.tensor(target_line.iloc[1], dtype=torch.long),
        }


class BERT(nn.Module):
    def __init__(self):
        super(BERT, self).__init__()
        self.bert_model = transformers.BertModel.from_pretrained(
            "bert-base-uncased"
        )
        self.out = nn.Linear(768, 1)

    def forward(self, ids, mask, token_type_ids):
        _, o2 = self.bert_model(
            ids,
            attention_mask=mask,
            token_type_ids=token_type_ids,
            return_dict=False,
        )

        out = self.out(o2)

        return out


def finetune(epochs, dataloader, model, loss_fn, optimizer, rank, cpu_id):

    model.train()

    for epoch in range(epochs):
        if isinstance(dataloader.sampler, DistributedSampler):
            dataloader.sampler.set_epoch(epoch)

        loop = tqdm(enumerate(dataloader), leave=False, total=len(dataloader))
        for batch, dl in loop:

            print(f"rank {cpu_id} epoch {epoch} batch {batch}")

            # Bert training
            ids = dl["ids"].to(rank)
            token_type_ids = dl["token_type_ids"].to(rank)
            mask = dl["mask"].to(rank)
            label = dl["target"].to(rank)
            label = label.unsqueeze(1)

            optimizer.zero_grad()

            output = model(ids=ids, mask=mask, token_type_ids=token_type_ids)
            label = label.type_as(output)

            loss = loss_fn(output, label)
            loss.backward()

            optimizer.step()

            pred = np.where(output.cpu().detach().numpy() >= 0, 1, 0)
            label = label.cpu().detach().numpy()

            num_correct = sum(1 for a, b in zip(pred, label) if a[0] == b[0])
            num_samples = pred.shape[0]
            accuracy = num_correct / num_samples

            print(
                f"Got {num_correct} / {num_samples} with accuracy {float(num_correct) / float(num_samples) * 100:.2f}"
            )

            loop.set_description(f"Epoch={epoch + 1}/{epochs}")
            loop.set_postfix(loss=loss.item(), acc=accuracy)

    return model


def preprocess(directory_path, chunk_size, alluxio_fs):
    """
    Preprocess each file in the directory for Dataset class.
    :param directory_path: directory path contain the files to be processed
    :param chunk_size: read file that has chunk_size to the memory each time
    :return: processed_file_info is a dictionary that contain start_line number for each file;
            total length of all files in the directory
    """

    processed_file_info = {}  # a dictionary of {start_line_number: file_name}
    next_start_line_num = 0
    total_length = 0

    all_files_info = alluxio_fs.ls(directory_path)

    # iterate each file in alluxio cache to get start line number for each file
    for file_info in all_files_info:
        file_name = file_info["name"]
        processed_file_info[next_start_line_num] = file_name

        # calculate length of each file, read the csv by chunk in case the file is too big to fit into the memory
        file_length = 0
        chunk_iterator = pd.read_csv(
            alluxio_fs.open(file_name, mode="r"), chunksize=chunk_size
        )
        for chunk in chunk_iterator:
            file_length += len(chunk)

        next_start_line_num += file_length
        total_length += file_length

    return processed_file_info, total_length


def append_to_output(filename, content):
    with open(filename, "a") as file:
        file.write(content + "\n")


def main(
    rank,
    world_size,
    total_epochs,
    batch_size,
    directory_path,
    preprocessed_file_info,
    total_length,
    alluxio_fs,
):

    # distributed training settings
    print(f"Initializing process group for rank {rank}")
    os.environ["MASTER_ADDR"] = "localhost"
    os.environ["MASTER_PORT"] = "12355"

    dist.init_process_group(backend="gloo", rank=rank, world_size=world_size)

    # train on gpu if gpu is available; otherwise, train on cpu
    device = torch.device(
        f"cuda:{rank}" if torch.cuda.is_available() else "cpu"
    )
    torch.cuda.set_device(device) if torch.cuda.is_available() else None

    # train BERT
    tokenizer = transformers.BertTokenizer.from_pretrained("bert-base-uncased")
    print(f"Loading dataset on rank {rank}")

    dataset = BertDataset(
        alluxio_fs,
        tokenizer,
        max_length=100,
        directory_path=directory_path,
        preprocessed_file_info=preprocessed_file_info,
        total_length=total_length,
        read_chunk_size=1000,
        output_filename=f"output_rank_{rank}.txt",
    )

    sampler = DistributedSampler(dataset)
    dataloader = DataLoader(
        dataset=dataset, batch_size=batch_size, sampler=sampler
    )

    model = BERT().to(device)
    model = DDP(
        model,
        device_ids=[rank] if torch.cuda.is_available() else None,
        find_unused_parameters=True,
    )

    loss_fn = nn.BCEWithLogitsLoss().to(device)
    optimizer = optim.Adam(model.parameters(), lr=0.0001)

    for param in model.module.bert_model.parameters():
        param.requires_grad = False

    model = finetune(
        total_epochs, dataloader, model, loss_fn, optimizer, device, rank
    )
    dist.destroy_process_group()
    print(f"Process group destroyed for rank {rank}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Simple distributed training job"
    )
    parser.add_argument(
        "--total-epochs", type=int, help="Total epochs to train the model"
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Input batch size on each device (default: 32)",
    )
    parser.add_argument(
        "--directory-path", type=str, help="Path to the input data file"
    )
    args = parser.parse_args()

    # initialize AlluxioClient and pull all file from S3 to alluxio
    alluxio_client = AlluxioClient(etcd_hosts="localhost")
    load_success = alluxio_client.submit_load(args.directory_path)
    print("Alluxio Load job submitted successful:", load_success)

    load_progress = "Loading datasets into Alluxio"

    while load_progress != "SUCCEEDED":
        time.sleep(5)
        progress = alluxio_client.load_progress(args.directory_path)
        load_progress = progress[1]["jobState"]
        print("Load progress:", load_progress)

    # set up alluxio filesystem
    # it will be used in preprocess() function and BertDataset class to access files in alluxio
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    alluxio_fs = fsspec.filesystem(
        "alluxiofs",
        etcd_hosts="localhost",
        etcd_port=2379,
        target_protocol="s3",
    )

    # preprocess files in the directory
    preprocessed_file_info, total_length = preprocess(
        args.directory_path, 5000, alluxio_fs
    )

    world_size = (
        torch.cuda.device_count()
        if torch.cuda.is_available()
        else os.cpu_count()
    )
    mp.spawn(
        main,
        args=(
            world_size,
            args.total_epochs,
            args.batch_size,
            args.directory_path,
            preprocessed_file_info,
            total_length,
            alluxio_fs,
        ),
        nprocs=world_size,
        join=True,
    )
