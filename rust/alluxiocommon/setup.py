from setuptools import setup
from setuptools_rust import Binding
from setuptools_rust import RustExtension


setup(
    name="alluxiocommon",
    version="1.0.3",
    description="Alluxio Fsspec native enhancement module with PyO3 in rust",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    rust_extensions=[RustExtension("alluxiocommon", binding=Binding.PyO3)],
    packages=["alluxiocommon"],
    # include any other necessary package metadata
    zip_safe=False,
)
