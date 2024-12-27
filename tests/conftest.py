import asyncio
import os
import platform
import socket
import time

import docker
import pytest
import pytest_asyncio

from sqlalchemy.ext.asyncio import create_async_engine


POSTGRES_USER = os.environ.get("POSTGRES_USER", "test")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "test")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "test")


def get_ip(container):
    if platform.system() == "Darwin":
        return "127.0.0.1"
    return container.attrs["NetworkSettings"]["IPAddress"]


class ContainerExecutor:
    def __init__(self):
        self.containers = []
        self.client = docker.from_env()
        self.kwds = {"detach": True}

    def run(self, image, **kwds):
        container_kwds = self.kwds.copy()
        container_kwds.update(kwds)
        container = self.client.containers.run(image, **container_kwds)
        self.containers.append(container)
        return container

    def run_wait_up(self, image, **kwds):
        container = self.run(image, **kwds)
        container.reload()
        tend = time.monotonic() + 10
        while container.status != "running" and time.monotonic() < tend:
            time.sleep(0.1)
        time.sleep(0.1)
        container.reload()
        if container.status != "running":
            print(container.logs().decode())
            raise Exception("Container error")
        return container

    def run_wait_exit(self, image, **kwds):
        container = self.run(image, **kwds)
        container.reload()
        container.wait()
        return container


def wait_socket_available(address, timeout):
    timeout_time = time.monotonic() + timeout
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(1)
            sock.connect(address)
            break
        except (socket.timeout, ConnectionError) as e:
            if time.monotonic() >= timeout_time:
                raise TimeoutError from e
            time.sleep(1)
        finally:
            sock.close()


@pytest.fixture(scope="class")
def container_executor():
    container_executor = ContainerExecutor()
    try:
        yield container_executor
    finally:
        for container in container_executor.containers:
            container.stop()
            container.remove(v=True)


@pytest.fixture(scope="class")
def postgres(container_executor):
    container = container_executor.run_wait_up(
        "postgres:15-alpine",
        environment={
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "POSTGRES_DB": POSTGRES_DB,
        },
        ports={"5432": "5432"},
        healthcheck={
            "test": ["CMD-SHELL", "pg_isready -U test"],
            "interval": 10**9,
            "timeout": 10**9,
            "retries": 20,
        },
    )

    ip = get_ip(container)
    port = 5432

    wait_socket_available((ip, port), 30)

    yield container


@pytest_asyncio.fixture
async def engine(postgres):
    await asyncio.sleep(1)
    ip = get_ip(postgres)
    engine = create_async_engine(
        f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{ip}/{POSTGRES_DB}",
        echo=True,
    )
    for _ in range(10):
        try:
            async with engine.begin():
                break
        except ConnectionResetError:
            await asyncio.sleep(1)
    yield engine
