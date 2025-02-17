import logging
import os
import shlex
import subprocess
import time
from urllib.parse import urlparse

import fsspec
import pytest
import requests

from alluxiofs import AlluxioClient
from alluxiofs import AlluxioFileSystem

LOGGER = logging.getLogger("alluxio_test")
TEST_ROOT = os.getenv("TEST_ROOT", "file:///opt/alluxio/ufs/")
# This is the path to the file you want to access
TEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
LOCAL_FILE_PATH = os.path.join(TEST_DIR, "test.csv")
ALLUXIO_FILE_PATH = "file://{}".format("/opt/alluxio/ufs/test.csv")
MASTER_CONTAINER = "alluxio-master"
WORKER_CONTAINER = "alluxio-worker"
ETCD_CONTAINER = "etcd"
LICENSE_ID = ""
LICENSE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "license-id"
)
if os.path.exists(LICENSE_PATH):
    with open(LICENSE_PATH, "r") as f:
        LICENSE_ID = f.read()


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        LOGGER.debug("Stopping existing container %s" % cid)
        subprocess.call(["docker", "rm", "-f", "-v", cid])


def yield_url():
    url = "http://127.0.0.1:28080"
    timeout = 10
    while True:
        try:
            LOGGER.debug("trying to connected to alluxio")
            r = requests.get(url + "/v1/files?path=/")
            LOGGER.debug("successfullly connected to alluxio")
            if r.ok:
                return url
                break
        except Exception as e:  # noqa: E722
            timeout -= 1
            if timeout < 0:
                raise SystemError from e
            time.sleep(10)


def launch_alluxio_dockers(with_etcd=True):
    network_cmd = "docker network create alluxio_network"

    run_cmd_master = (
        "docker run --platform linux/amd64 -d --rm --net=alluxio_network -p 19999:19999 -p 19998:19998 "
        f"--name=alluxio-master -v {TEST_DIR}:/opt/alluxio/ufs "
        '-e ALLUXIO_JAVA_OPTS=" -Dalluxio.master.hostname=alluxio-master '
        "-Dalluxio.security.authentication.type=NOSASL "
        "-Dalluxio.security.authorization.permission.enabled=false "
        "-Dalluxio.security.authorization.plugins.enabled=false "
        "-Dlicense.check.enabled=false "
        "-Dalluxio.master.journal.type=NOOP "
        f"-Dalluxio.license={LICENSE_ID} "
        "-Dalluxio.master.scheduler.initial.wait.time=1s "
        "-Dalluxio.dora.client.ufs.root=file:/// "
        + (
            "-Dalluxio.worker.membership.manager.type=ETCD "
            "-Dalluxio.etcd.endpoints=http://etcd:2379 "
            if with_etcd
            else ""
        )
        + '-Dalluxio.underfs.xattr.change.enabled=false " alluxio-local:MAIN-SNAPSHOT master'
    )

    run_cmd_worker = (
        "docker run --platform linux/amd64 -d --rm --net=alluxio_network -p 28080:28080 -p 29999:29999 -p 29997:29997 "
        f"--name=alluxio-worker --shm-size=1G -v {TEST_DIR}:/opt/alluxio/ufs "
        '-e ALLUXIO_JAVA_OPTS=" -Dalluxio.master.hostname=alluxio-master '
        "-Dalluxio.security.authentication.type=NOSASL "
        "-Dlicense.check.enabled=false "
        "-Dalluxio.security.authorization.permission.enabled=false "
        "-Dalluxio.security.authorization.plugins.enabled=false "
        f"-Dalluxio.license={LICENSE_ID} "
        "-Dalluxio.dora.client.ufs.root=file:/// "
        + (
            "-Dalluxio.worker.hostname=localhost "
            "-Dalluxio.worker.container.hostname=alluxio-worker "
            "-Dalluxio.worker.membership.manager.type=ETCD "
            "-Dalluxio.etcd.endpoints=http://etcd:2379 "
            if with_etcd
            else ""
        )
        + '-Dalluxio.underfs.xattr.change.enabled=false " alluxio-local:MAIN-SNAPSHOT worker'
    )

    run_cmd_etcd = (
        "docker run --platform linux/amd64 -d --rm --net=alluxio_network -p 4001:4001 -p 2380:2380 -p 2379:2379 "
        f"-v {TEST_DIR}:/etc/ssl/certs "
        "--name etcd quay.io/coreos/etcd:v3.5.13 "
        "/usr/local/bin/etcd "
        "--data-dir=/etcd-data "
        "--name etcd1 "
        "--listen-client-urls http://0.0.0.0:2379 "
        "--advertise-client-urls http://0.0.0.0:2379"
    )

    stop_docker(WORKER_CONTAINER)
    stop_docker(MASTER_CONTAINER)
    if with_etcd:
        stop_docker(ETCD_CONTAINER)
    subprocess.run(
        shlex.split(network_cmd)
    )  # could return error code if network already exists
    if with_etcd:
        subprocess.check_output(shlex.split(run_cmd_etcd))
    subprocess.check_output(shlex.split(run_cmd_master))
    subprocess.check_output(shlex.split(run_cmd_worker))


def stop_alluxio_dockers(with_etcd=False):
    stop_docker(WORKER_CONTAINER)
    stop_docker(MASTER_CONTAINER)
    if with_etcd:
        stop_docker(ETCD_CONTAINER)


@pytest.fixture(scope="session")
def docker_alluxio():
    if "ALLUXIO_URL" in os.environ:
        # assume we already have a server already set up
        yield os.getenv("ALLUXIO_URL")
        return
    launch_alluxio_dockers()
    yield yield_url()
    stop_alluxio_dockers()


@pytest.fixture(scope="session")
def docker_alluxio_with_etcd():
    if "ALLUXIO_URL" in os.environ:
        # assume we already have a server already set up
        yield os.getenv("ALLUXIO_URL")
        return
    launch_alluxio_dockers(True)
    yield yield_url()
    stop_alluxio_dockers(True)


@pytest.fixture
def alluxio_client(docker_alluxio):
    LOGGER.debug(f"get AlluxioClient connect to {docker_alluxio}")
    parsed_url = urlparse(docker_alluxio)
    host = parsed_url.hostname
    port = parsed_url.port
    alluxio_client = AlluxioClient(worker_hosts=host, worker_http_port=port)
    yield alluxio_client


@pytest.fixture
def alluxio_client_alluxiocommon(docker_alluxio):
    LOGGER.debug(
        f"get AlluxioClient with alluxiocommon connect to {docker_alluxio}"
    )
    parsed_url = urlparse(docker_alluxio)
    host = parsed_url.hostname
    port = parsed_url.port
    alluxio_options = {"alluxio.common.extension.enable": "True"}
    alluxio_client = AlluxioClient(
        worker_hosts=host, worker_http_port=port, options=alluxio_options
    )
    yield alluxio_client


@pytest.fixture
def etcd_alluxio_client(docker_alluxio_with_etcd):
    LOGGER.debug(
        f"get etcd AlluxioClient connect to {docker_alluxio_with_etcd}"
    )
    parsed_url = urlparse(docker_alluxio_with_etcd)
    host = parsed_url.hostname
    etcd_alluxio_client = AlluxioClient(etcd_hosts=host)
    yield etcd_alluxio_client


@pytest.fixture
def alluxio_file_system(docker_alluxio):
    LOGGER.debug(f"get AlluxioFileSystem connect to {docker_alluxio}")
    parsed_url = urlparse(docker_alluxio)
    host = parsed_url.hostname
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    alluxio_file_system = fsspec.filesystem(
        "alluxiofs",
        worker_hosts=host,
        target_protocol="file",
        preload_path=ALLUXIO_FILE_PATH,
    )
    yield alluxio_file_system


@pytest.fixture
def alluxio_file_system_alluxiocommon(docker_alluxio):
    LOGGER.debug(
        f"get AlluxioFileSystem connect to {docker_alluxio}"
        f" with alluxiocommon enabled."
    )
    parsed_url = urlparse(docker_alluxio)
    host = parsed_url.hostname
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    alluxio_options = {"alluxio.common.extension.enable": "True"}
    alluxio_file_system = fsspec.filesystem(
        "alluxiofs",
        worker_hosts=host,
        target_protocol="file",
        options=alluxio_options,
        preload_path=ALLUXIO_FILE_PATH,
    )
    yield alluxio_file_system


@pytest.fixture
def etcd_alluxio_file_system(docker_alluxio_with_etcd):
    LOGGER.debug(
        f"get etcd AlluxioFileSystem connect to {docker_alluxio_with_etcd}"
    )
    parsed_url = urlparse(docker_alluxio_with_etcd)
    host = parsed_url.hostname
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    etcd_alluxio_file_system = fsspec.filesystem(
        "alluxiofs",
        etcd_hosts=host,
        target_protocol="file",
        preload_path=ALLUXIO_FILE_PATH,
    )
    yield etcd_alluxio_file_system
