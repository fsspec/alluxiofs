from setuptools import setup
from setuptools_rust import Binding, RustExtension


setup(
    name="alluxiocommon",
    version="0.1",
    rust_extensions=[RustExtension("alluxiocommon", binding=Binding.PyO3)],
    packages=["alluxiocommon"],
    # include any other necessary package metadata
    zip_safe=False,
)
