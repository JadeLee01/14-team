import httpx


def create_async_http_client(timeout_sec: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_sec),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
