Before optimization

| Test                   | Benchmark | Local FileSystem(nvme) | Local region S3 | AlluxioFsSpec   | AlluxioFsSpec PyO3 optimized |
|------------------------|-----------|-------------------------|----------------|-----------------|------------------------------|
| Ray + XgBoost(10G)     |           |                         |                | PyTorch         |
| Ray + PyTorch(parquet) |           |         |                | CosmoFlow       | Tensorflow                   |
| Ray + PyTorch(image)   |           |           | Forces mean absolute error 0.036       | DimeNet++       | PyTorch                      |
|          |           |  | 0.8 Local Distance Difference Test (lDDT) | AlphaFold2 (PyTorch) | PyTorch                      |


After optimization

