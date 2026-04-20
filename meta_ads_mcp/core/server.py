"""MCP server configuration for Meta Ads API — Fly.io deployment."""

import os
import sys
import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.responses import Response
from .utils import logger

# Initialize FastMCP server
mcp_server = FastMCP("meta-ads")


# ── Bearer Token Auth Middleware (matches existing Fly.io pattern) ──
class APIKeyAuthMiddleware:
    def __init__(self, app, api_key: str):
        self.app = app
        self.api_key = api_key

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and self.api_key:
            # Check Bearer token header
            headers = dict(scope.get("headers", []))
            auth = headers.get(b"authorization", b"").decode()
            # Check ?key= query param
            qs = scope.get("query_string", b"").decode()
            key_param = ""
            for part in qs.split("&"):
                if part.startswith("key="):
                    key_param = part[4:]
                    break
            if auth != f"Bearer {self.api_key}" and key_param != self.api_key:
                resp = Response("Unauthorized", status_code=401)
                await resp(scope, receive, send)
                return
        await self.app(scope, receive, send)


def main():
    """Main entry point — starts the server with Streamable HTTP transport."""
    logger.info("Meta Ads MCP server starting")
    logger.debug(f"Python version: {sys.version}")

    # Import all tool modules to ensure they are registered
    from . import accounts, campaigns, adsets, ads, insights
    from . import ads_library, budget_schedules, reports, targeting
    from . import duplication, depth_insights

    # Build the ASGI app with streamable HTTP transport
    app = mcp_server.streamable_http_app()

    # Wrap with bearer token auth middleware if AUTH_TOKEN is set
    api_key = os.getenv("AUTH_TOKEN", "")
    if api_key:
        app = APIKeyAuthMiddleware(app, api_key=api_key)

    port = int(os.getenv("PORT", 8000))
    logger.info(f"Starting server on 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
