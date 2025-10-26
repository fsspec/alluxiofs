# 并行执行版本（同时运行多个，然后等待所有完成）
clear
for i in {0..31}; do
    python tmp.py --file-path="file:///home/yxd/alluxio/ufs/python_sdk_test/${i}" &
    pids+=($!)
done

# 等待所有后台进程完成
for pid in "${pids[@]}"; do
    wait $pid
done