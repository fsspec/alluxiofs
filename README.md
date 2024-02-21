# Alluxio FileSystem

Alluxio filesystem spec implementation

## Dependencies

* Launch Alluxio servers from https://github.com/Alluxio/alluxio.
* Install Alluxio Python Library https://github.com/Alluxio/alluxio-py.

## Install alluxiofs

```
cd alluxiofs && python3 setup.py bdist_wheel && pip3 install dist/alluxiofs-0.1-py3-none-any.whl
```

## Launch Alluxio FileSystem

Minimum requirements to launch Alluxio on top of s3
```
pip install alluxiofs

fsspec.register_implementation("alluxio", AlluxioFileSystem, clobber=True)
alluxio = fsspec.filesystem("alluxio", etcd_host=args.etcd_host, target_protocol="s3")
```
See concrete options descriptions at [Alluxio filesystem initialization description](alluxiofs/core.py)
See a more concrete example at [tests/test_alluxio_fsspec.py](tests/test_alluxio_fsspec.py)

## Development

See [Contributions](CONTRIBUTING.md) for guidelines around making new contributions and reviewing them.
