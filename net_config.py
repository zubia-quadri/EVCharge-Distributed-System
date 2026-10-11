# net_config.py
import os

RUN_ENV = os.environ.get("RUN_ENV", "local")

LOCAL_HOSTS = {
    "service-a": "localhost:60401",
    "service-b": "localhost:60402",
    "service-c": "localhost:60403",
    "worker": "localhost:60201",
}

DOCKER_HOSTS = {
    "service-a": "service-a:60401",
    "service-b": "service-b:60402",
    "service-c": "service-c:60403",
    "worker": "worker:60201",
}

def resolve(name):
    hosts = DOCKER_HOSTS if RUN_ENV == "docker" else LOCAL_HOSTS
    return hosts.get(name, f"{name}:60401")
