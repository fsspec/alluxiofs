#!/bin/bash

# clear the terminal
clear

# the benchmark for different-size read and write
echo "*****************************************************************"
echo ""
echo "The benchmark for different-size read and write starts"
echo ""
echo "*****************************************************************"
echo ""

# the config of alluxiofs
ETCD_HOSTS="localhost"
ETCD_PORT=2379
CLUSTER_NAME="DefaultAlluxioCluster"
TARGET_PROTOCOL="local"

# the result dir
FUSE_RESULT_DIR="./bench_result/fuse-benchmark"
FSSPEC_RESULT_DIR="./bench_result/fsspec-benchmark"
RUNTIME=10
SUMMARIZE_RESULT="./bench_result"

#
rm -r $FUSE_RESULT_DIR
rm -r $FSSPEC_RESULT_DIR
mkdir -p $FUSE_RESULT_DIR
mkdir -p $FSSPEC_RESULT_DIR

FILE_SIZE=("1MB" "10MB" "100MB" "1GB")
BS=("4KB" "256KB" "1024KB")
NUMJOBS=("1" "4" "16")

# alluxio-fuse
FUSE_PATH="/mnt/alluxio/local/benchmark/fuse"
FUSE_RW=("read" "write")


# alluxio-fsspec
FSSPEC_REMOTE_PATH_READ="file:///home/yxd/alluxio/ufs/benchmark/fsspec/read"
FSSPEC_REMOTE_PATH_WRITE="file:///home/yxd/alluxio/ufs/benchmark/fsspec/write"
FSSPEC_LOCAL_PATH="./test_preprocess"
FSSPEC_OP=("download_data" "upload_data")

# 1. Alluxio-Fuse benchmark
rm -r ${FUSE_PATH}/write/
for rw in "${FUSE_RW[@]}"; do
  for size in "${FILE_SIZE[@]}"; do
    for bs in "${BS[@]}"; do
      if [ "$rw" = "write" ]; then
        _NUMJOBS=("1")
      else
        _NUMJOBS=("${NUMJOBS[@]}")
      fi
      for numjobs in "${_NUMJOBS[@]}"; do
        echo "Running FUSE benchmark for rw=$rw, size=$size, bs=$bs, numjobs=$numjobs"
        fio -filename=$FUSE_PATH/${rw}/${size}_${bs}_${numjobs} \
            -iodepth 384 \
            -thread \
            -ioengine=psync \
            -size=$size \
            -numjobs=$numjobs \
            -runtime=$RUNTIME \
            -group_reporting \
            -name=fuse_${rw}_${size}_${bs}_${numjobs} \
            -rw=$rw \
            -bs=$bs \
            -direct=1 \
            >> $FUSE_RESULT_DIR/fuse_${rw}_${size}_${bs}_${numjobs}.log 2>&1
      done
    done
  done
done

# 2. Alluxio-FSSpec benchmark
# preprocess
# shellcheck disable=SC2145
python ./benchmark/bench/preprocess.py --etcd_hosts=$ETCD_HOSTS \
                                       --etcd_port=$ETCD_PORT \
                                       --target_protocol=$TARGET_PROTOCOL \
                                       --cluster_name=$CLUSTER_NAME \
                                       --op=read \
                                       --size "${FILE_SIZE[@]}" \
                                       --path=$FSSPEC_REMOTE_PATH_READ
# shellcheck disable=SC2145
python ./benchmark/bench/preprocess.py --etcd_hosts=$ETCD_HOSTS \
                                       --etcd_port=$ETCD_PORT \
                                       --target_protocol=$TARGET_PROTOCOL \
                                       --cluster_name=$CLUSTER_NAME \
                                       --op=write \
                                       --size "${FILE_SIZE[@]}" \
                                       --path=$FSSPEC_REMOTE_PATH_WRITE \
                                       --local_path=$FSSPEC_LOCAL_PATH

# run benchmark
for op in "${FSSPEC_OP[@]}"; do
  for size in "${FILE_SIZE[@]}"; do
    for bs in "${BS[@]}"; do
      if [ "$op" = "upload_data" ]; then
          _NUMJOBS=("1")
      else
          _NUMJOBS=("${NUMJOBS[@]}")
      fi
      for numjobs in "${_NUMJOBS[@]}"; do
        echo "Running FSSPEC benchmark for op=$op, size=$size, bs=$bs, numjobs=$numjobs"

        if [ "$op" == "seq_read" ]; then
          path="${FSSPEC_REMOTE_PATH_READ}/${size}"
          local_path=""
        elif [ "$op" == "download_data" ]; then
          path="${FSSPEC_REMOTE_PATH_READ}/${size}"
          local_path=""
        else
          path="${FSSPEC_REMOTE_PATH_WRITE}/${size}"
          local_path=${FSSPEC_LOCAL_PATH}/${size}
        fi
        # the `cluster_name` and `target_protocol` is optional to set
        python bench.py --etcd_hosts=$ETCD_HOSTS \
                        --etcd_port=$ETCD_PORT \
                        --numjobs=$numjobs \
                        --runtime=$RUNTIME \
                        --testsuite=FSSPEC \
                        --path=$path \
                        --target_protocol=$TARGET_PROTOCOL\
                        --cluster_name=$CLUSTER_NAME\
                        --op=$op \
                        --bs=$bs \
                        --local_path=$local_path \
                        --result_dir=$FSSPEC_RESULT_DIR/fsspec_${op}_${size}_${bs}_${numjobs}
      done
    done
  done
done

#postprocess
echo "Post-process the files"
#postprocess
echo "Post-process the files"
python ./benchmark/bench/postprocess.py --local_path=${FUSE_PATH}/read/ \
                                        --etcd_hosts=$ETCD_HOSTS \
                                        --etcd_port=$ETCD_PORT \
                                        --target_protocol=$TARGET_PROTOCOL \
                                        --cluster_name=$CLUSTER_NAME

python ./benchmark/bench/postprocess.py --local_path=${FUSE_PATH}/write/ \
                                        --etcd_hosts=$ETCD_HOSTS \
                                        --etcd_port=$ETCD_PORT \
                                        --target_protocol=$TARGET_PROTOCOL \
                                        --cluster_name=$CLUSTER_NAME

python ./benchmark/bench/postprocess.py --path=$FSSPEC_REMOTE_PATH_READ \
                                        --etcd_hosts=$ETCD_HOSTS \
                                        --etcd_port=$ETCD_PORT \
                                        --target_protocol=$TARGET_PROTOCOL \
                                        --cluster_name=$CLUSTER_NAME

python ./benchmark/bench/postprocess.py --path=$FSSPEC_REMOTE_PATH_WRITE \
                                        --local_path=$FSSPEC_LOCAL_PATH \
                                        --etcd_hosts=$ETCD_HOSTS \
                                        --etcd_port=$ETCD_PORT \
                                        --target_protocol=$TARGET_PROTOCOL \
                                        --cluster_name=$CLUSTER_NAME

# the benchmark for batch-read

echo ""
echo "*****************************************************************"
echo ""
echo "The benchmark for batch-read starts"
echo ""
echo "*****************************************************************"
echo ""



# the result dir
FUSE_RESULT_DIR_BATCH="./bench_result/fuse-batch-benchmark"
FSSPEC_RESULT_DIR_BATCH="./bench_result/fsspec-batch-benchmark"

SUMMARIZE_RESULT_BATCH="./bench_result/batch_summary"
RUNTIME_BATCH=10
NUMJOBS_BATCH=1
BS_BATCH="4KB"

rm -r $FUSE_RESULT_DIR_BATCH
rm -r $FSSPEC_RESULT_DIR_BATCH
mkdir -p $FUSE_RESULT_DIR_BATCH
mkdir -p $FSSPEC_RESULT_DIR_BATCH

FIlE_NUMBER_BATCH=("1" "10" "100")
FILE_SIZE_BATCH=("10KB" "100KB" "1MB")

# alluxio-fuse
FUSE_PATH_READ_BATCH="/mnt/alluxio/local/benchmark/fuse/read_batch"
FUSE_RW_BATCH=("read")


# alluxio-fsspec
FSSPEC_REMOTE_PATH_READ_BATCH="file:///home/yxd/alluxio/ufs/benchmark/fsspec/read_batch"
FSSPEC_OP_BATCH="download_data"


for rw in "${FUSE_RW_BATCH[@]}"; do
  for size in "${FILE_SIZE_BATCH[@]}"; do
    for num in "${FIlE_NUMBER_BATCH[@]}"; do
        # preprocess
        echo "Prepare the files"
        python ./benchmark/bench/preprocess.py --etcd_hosts=$ETCD_HOSTS \
                                               --etcd_port=$ETCD_PORT \
                                               --target_protocol=$TARGET_PROTOCOL \
                                               --cluster_name=$CLUSTER_NAME \
                                               --op=read_batch \
                                               --size=$size \
                                               --number=$num \
                                               --path=${FSSPEC_REMOTE_PATH_READ_BATCH}/${size} \
                                               --local_path=${FUSE_PATH_READ_BATCH}/${size}

        echo "fuse start"
        # fuse
        fio -directory=$FUSE_PATH_READ_BATCH/${size}/ \
            -iodepth 384 \
            -thread \
            -ioengine=psync \
            -size=$size \
            -numjobs=$NUMJOBS_BATCH \
            -runtime=$RUNTIME \
            -group_reporting \
            -name=fuse_${rw}_${size} \
            -rw=$rw \
            -bs=$BS_BATCH \
            >> $FUSE_RESULT_DIR_BATCH/fuse_${rw}_${size}_${BS_BATCH}_${NUMJOBS_BATCH}.log 2>&1


        # fsspec
        path="${FSSPEC_REMOTE_PATH_READ_BATCH}/${size}"
        local_path=""
        echo "fsspec start"
        # the `cluster_name` and `target_protocol` is optional to set
        python bench.py --etcd_hosts=$ETCD_HOSTS \
                        --etcd_port=$ETCD_PORT \
                        --numjobs=$NUMJOBS_BATCH \
                        --runtime=$RUNTIME_BATCH \
                        --testsuite=FSSPEC \
                        --path=$path \
                        --target_protocol=$TARGET_PROTOCOL\
                        --cluster_name=$CLUSTER_NAME\
                        --op=$FSSPEC_OP_BATCH \
                        --bs=$BS_BATCH \
                        --local_path=$local_path \
                        --result_dir=$FSSPEC_RESULT_DIR_BATCH/fsspec_${FSSPEC_OP_BATCH}_${size}_${BS_BATCH}_${NUMJOBS_BATCH}

        #postprocess
        echo "Post-process the files"
        python ./benchmark/bench/postprocess.py --path=$path \
                                                --local_path=$FUSE_PATH_READ_BATCH/${size}/ \
                                                --etcd_hosts=$ETCD_HOSTS \
                                                --etcd_port=$ETCD_PORT \
                                                --target_protocol=$TARGET_PROTOCOL \
                                                --cluster_name=$CLUSTER_NAME
    done
  done
done

echo ""
echo "**********************************************************"
echo ""
echo "The result of different-size read and write:"
echo ""
echo "**********************************************************"
echo ""
python ./benchmark/bench/fuse_stats.py --inputs_dir=$FUSE_RESULT_DIR --outputs_dir=$SUMMARIZE_RESULT
python ./benchmark/bench/fsspec_stats.py --inputs_dir=$FSSPEC_RESULT_DIR --outputs_dir=$SUMMARIZE_RESULT

echo ""
echo "**********************************************************"
echo ""
echo "The result of batch-read:"
echo ""
echo "**********************************************************"
echo ""
python ./benchmark/bench/fuse_stats.py --batch=1 --inputs_dir=$FUSE_RESULT_DIR_BATCH --outputs_dir=$SUMMARIZE_RESULT_BATCH
python ./benchmark/bench/fsspec_stats.py --batch=1 --inputs_dir=$FSSPEC_RESULT_DIR_BATCH --outputs_dir=$SUMMARIZE_RESULT_BATCH


python ./benchmark/bench/figure_plot.py --fuse_summarize_dir="./bench_result/fuse_summarize_results.csv" --fsspec_summarize_dir="./bench_result/fsspec_summarize_results.csv"
echo "All benchmarks completed!"
