"""Tests for APIKeyAuthMiddleware accepted credential forms."""
import asyncio

from meta_ads_mcp.core.server import APIKeyAuthMiddleware

KEY = "test-key"


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


def _status(headers=None, query=b""):
    mw = APIKeyAuthMiddleware(_ok_app, api_key=KEY)
    sent = []

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(msg):
        sent.append(msg)

    scope = {"type": "http", "headers": headers or [], "query_string": query,
             "method": "POST", "path": "/mcp"}
    asyncio.run(mw(scope, receive, send))
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


def test_no_credentials_rejected():
    assert _status() == 401


def test_bearer_header_accepted():
    assert _status([(b"authorization", f"Bearer {KEY}".encode())]) == 200


def test_x_api_key_header_accepted():
    assert _status([(b"x-api-key", KEY.encode())]) == 200


def test_query_key_accepted():
    assert _status(query=f"key={KEY}".encode()) == 200


def test_wrong_x_api_key_rejected():
    assert _status([(b"x-api-key", b"nope")]) == 401
