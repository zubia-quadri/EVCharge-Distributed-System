# -*- coding: utf-8 -*-
"""
backend/net_config.py - Resolves service addresses for local runs (localhost) and
for containers (service names). Set RUN_ENV=docker to switch modes.
"""
import os

RUN_ENV = os.environ.get("RUN_ENV", "local")

LOCAL_HOSTS = {
    "dfs-meta": "localhost:60501",
    "dfs-store-1": "localhost:60511",
    "dfs-store-2": "localhost:60512",
    "dfs-store-3": "localhost:60513",
}

DOCKER_HOSTS = {
    "dfs-meta": "dfs-meta:60501",
    "dfs-store-1": "dfs-store-1:60511",
    "dfs-store-2": "dfs-store-2:60512",
    "dfs-store-3": "dfs-store-3:60513",
}


def resolve(name):
    hosts = DOCKER_HOSTS if RUN_ENV == "docker" else LOCAL_HOSTS
    return hosts[name]
