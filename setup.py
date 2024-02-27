from setuptools import find_packages
from setuptools import setup

setup(
    name="alluxiofs",
    version="1.0.0",
    description="Alluxio Fsspec 1.0.0 provides Alluxio filesystem spec implementation.",
    url="https://github.com/fsspec/alluxiofs",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "fsspec",
        "alluxio==1.0.0",
    ],
    extras_require={"tests": ["pytest"]},
    python_requires=">=3.8",
    maintainer="Lu Qiu",
    maintainer_email="luqiujob@gmail.com",
    entry_points={
        "fsspec.specs": [
            "alluxio=alluxiofs.AlluxioFileSystem",
        ],
    },
)
