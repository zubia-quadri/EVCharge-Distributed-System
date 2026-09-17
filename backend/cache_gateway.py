import os
import sys
import json

import grpc
import redis

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


# Redis running inside WSL
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Existing EVCharge backend
BACKEND_SERVICE_ADDR = "localhost:50051"

# Short TTL so cache behaviour is easy to demonstrate
CACHE_TTL_SECONDS = 10

# Two separate keys:
# Fresh cache expires after 10 seconds
# Stale cache never expires
FRESH_KEY_PREFIX = "fresh:charger:"
STALE_KEY_PREFIX = "stale:charger:"


def get_redis():
    """
    Create a fresh Redis connection for each request.
    Short timeouts allow graceful failure if Redis is unavailable.
    """

    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        socket_connect_timeout=1,
        socket_timeout=1
    )


def fetch_from_backend(charger_id):
    """
    Call the existing EVCharge gRPC backend.
    """

    with grpc.insecure_channel(
        BACKEND_SERVICE_ADDR
    ) as channel:

        stub = evcharge_pb2_grpc.ChargingServiceStub(
            channel
        )

        response = stub.CheckChargerAvailability(
            evcharge_pb2.ChargerRequest(
                charger_id=charger_id
            ),
            timeout=2
        )

        return {
            "charger_id": response.charger_id,
            "available": response.available
        }


def get_charger(charger_id):

    fresh_key = (
        f"{FRESH_KEY_PREFIX}{charger_id}"
    )

    stale_key = (
        f"{STALE_KEY_PREFIX}{charger_id}"
    )

    # -------------------------------------------------
    # 1. TRY REDIS CACHE FIRST
    # -------------------------------------------------

    try:

        r = get_redis()

        cached = r.get(fresh_key)

        if cached:

            print(
                f"[Gateway] CACHE HIT "
                f"for charger {charger_id}"
            )

            return (
                json.loads(cached),
                "cache-hit"
            )

        print(
            f"[Gateway] CACHE MISS "
            f"for charger {charger_id} "
            f"-- querying backend"
        )

    except redis.exceptions.RedisError as e:

        print(
            f"[Gateway] REDIS UNAVAILABLE "
            f"({e}) -- falling back to backend"
        )

        r = None


    # -------------------------------------------------
    # 2. CACHE MISS OR REDIS DOWN
    #    → QUERY BACKEND
    # -------------------------------------------------

    try:

        data = fetch_from_backend(
            charger_id
        )

        print(
            f"[Gateway] Fetched charger "
            f"{charger_id} from BACKEND"
        )

        # Save result in Redis if Redis is available
        if r is not None:

            try:

                # Fresh cache - expires
                r.set(
                    fresh_key,
                    json.dumps(data),
                    ex=CACHE_TTL_SECONDS
                )

                # Stale backup - never expires
                r.set(
                    stale_key,
                    json.dumps(data)
                )

                print(
                    f"[Gateway] Cached charger "
                    f"{charger_id}"
                )

            except redis.exceptions.RedisError:

                print(
                    "[Gateway] Redis write failed, "
                    "continuing without caching"
                )

        return data, "backend"


    # -------------------------------------------------
    # 3. BACKEND DOWN
    #    → TRY STALE CACHE
    # -------------------------------------------------

    except grpc.RpcError as e:

        print(
            f"[Gateway] BACKEND UNAVAILABLE "
            f"({e.code()}) "
            f"-- checking stale cache"
        )

        if r is not None:

            try:

                stale = r.get(stale_key)

                if stale:

                    print(
                        f"[Gateway] Serving STALE "
                        f"cached copy for charger "
                        f"{charger_id}"
                    )

                    return (
                        json.loads(stale),
                        "stale-fallback"
                    )

            except redis.exceptions.RedisError:

                pass

        raise RuntimeError(
            f"Charger {charger_id} unavailable: "
            f"both backend and cache failed"
        )


if __name__ == "__main__":

    charger_id = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "C1"
    )

    data, source = get_charger(
        charger_id
    )

    print(
        f"\nResult (source={source}): "
        f"{data}"
    )