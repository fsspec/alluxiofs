# GPU Prototype

## Build Process

Done it once:
```
pip install pybind11
```

```
export CMAKE_PREFIX_PATH=$CMAKE_PREFIX_PATH:/opt/anaconda3/lib/python3.11/site-packages/pybind11/share/cmake/pybind11
```

```
python setup.py bdist_wheel
export MACOSX_DEPLOYMENT_TARGET=10.9 # may not be necessary for you
pip install dist/dist/alluxio_gpu-0.1.0-cp311-cp311-macosx_10_9_x86_64.whl
pytest tests/test_example.py
```
