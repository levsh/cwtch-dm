import socket
import time

import docker


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


def get_ip(container):
    return (
        container.attrs["NetworkSettings"]["IPAddress"]
        or container.attrs["NetworkSettings"]["Networks"]["shared"]["IPAddress"]
    )
