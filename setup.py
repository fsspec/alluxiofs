from setuptools import find_packages
from setuptools import setup

setup(
    name="alluxiofs",
    version="1.1.4",
    description="Alluxio Fsspec provides Alluxio filesystem spec implementation.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/fsspec/alluxiofs",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # Alluxio fs dependencies
        "fsspec",
        # Alluxio client dependencies
        "aiohttp",
        "decorator",
        "humanfriendly",
        "requests",
        "etcd3",
        "mmh3",
        "sortedcontainers",
        "pycurl",
        "cachetools",
    ],
    extras_require={
        "tests": [
            "pytest",
            "pytest-aiohttp",
            "ray",
            "pyarrow",
        ]
    },
    python_requires=">=3.10",
    maintainer="Jiaming Mai, Xiaodong Yang, Lu Qiu",
    maintainer_email="jiamingmai@163.com, xiaodong163831@gmail.com, luqiujob@gmail.com",
    entry_points={
        "fsspec.specs": [
            "alluxio=alluxiofs.AlluxioFileSystem",
        ],
    },
)
