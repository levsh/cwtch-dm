import os

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tests.utils import ContainerExecutor, get_ip, wait_socket_available

POSTGRES_USER = os.environ.get("POSTGRES_USER", "test")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "test")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "test")


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
        shm_size="1G",
    )
    ip = get_ip(container)
    port = 5432
    wait_socket_available((ip, port), 30)
    yield container


@pytest.fixture
def engine(postgres):
    ip = get_ip(postgres)
    yield create_async_engine(
        f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{ip}/{POSTGRES_DB}",
        echo=True,
    )
