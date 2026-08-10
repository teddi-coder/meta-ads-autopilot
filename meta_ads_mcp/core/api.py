"""Core API functionality for Meta Ads API."""

from typing import Any, Dict, Optional, Callable
import json
import hmac
import hashlib
import httpx
import asyncio
import functools
import os
import random
import re
import time
from . import auth
from .auth import needs_authentication, auth_manager, start_callback_server, shutdown_callback_server
from .utils import logger

class McpToolError(Exception):
    """Base class for MCP tool errors that must set isError: true.

    Subclasses should be raised (not returned) from tool handlers.
    meta_api_tool re-raises these so FastMCP sees them and sets
    isError: true in the JSON-RPC response, which triggers the usage
    credit refund in the Next.js proxy.
    """
    pass


def ensure_act_prefix(account_id: str) -> str:
    """Ensure account_id has the 'act_' prefix required by Meta's Graph API."""
    if account_id and not account_id.startswith("act_"):
        return f"act_{account_id}"
    return account_id


# Constants
META_GRAPH_API_VERSION = "v24.0"
META_GRAPH_API_BASE = f"https://graph.facebook.com/{META_GRAPH_API_VERSION}"
USER_AGENT = "meta-ads-mcp/1.0"

# Log key environment and configuration at startup
logger.info("Core API module initialized")
logger.info(f"Graph API Version: {META_GRAPH_API_VERSION}")
logger.info(f"META_APP_ID env var present: {'Yes' if os.environ.get('META_APP_ID') else 'No'}")
logger.info(f"META_APP_SECRET env var present (appsecret_proof will be {'enabled' if os.environ.get('META_APP_SECRET') else 'disabled'})")

# Rate-limit error codes per Meta's error reference — retry with backoff on these
_RATE_LIMIT_CODES = frozenset({4, 17, 32, 613})
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2.0  # seconds; doubles per attempt

# Proactive throttle: account_id → monotonic timestamp to delay until
_account_throttle_until: Dict[str, float] = {}
_THROTTLE_DELAY = 5.0  # seconds to wait when usage header reports >= 80%

# In-memory outcome counter; logged after every call for observability
_outcome_counts: Dict[str, int] = {
    "success": 0,
    "rate_limited_retried_success": 0,
    "rate_limited_exhausted": 0,
    "auth_error": 0,
    "malformed_request": 0,
    "other_error": 0,
}


def _record_outcome(outcome: str) -> None:
    """Increment the outcome counter and log current totals."""
    _outcome_counts[outcome] = _outcome_counts.get(outcome, 0) + 1
    logger.info(f"api_outcome outcome={outcome} totals={_outcome_counts}")

class GraphAPIError(Exception):
    """Exception raised for errors from the Graph API."""
    def __init__(self, error_data: Dict[str, Any]):
        self.error_data = error_data
        self.message = error_data.get('message', 'Unknown Graph API error')
        super().__init__(self.message)
        
        # Log error details
        logger.error(f"Graph API Error: {self.message}")
        logger.debug(f"Error details: {error_data}")
        
        # Check if this is an auth error (code 4 is rate limiting, NOT auth)
        if "code" in error_data and error_data["code"] in [190, 102]:
            logger.warning(f"Auth error detected (code: {error_data['code']}). Invalidating token.")
            auth_manager.invalidate_token()
        elif "code" in error_data and error_data["code"] == 4:
            logger.warning(f"Rate limit error detected (code: 4, subcode: {error_data.get('error_subcode', 'N/A')}). Token is still valid — NOT invalidating.")


def _log_meta_rate_limit_headers(headers: dict, endpoint: str) -> None:
    """Log Meta's rate limit headers and set proactive throttle for high-usage accounts."""
    app_usage = headers.get("x-app-usage")
    biz_usage = headers.get("x-business-use-case-usage")
    ad_account_usage = headers.get("x-ad-account-usage")

    if not (app_usage or biz_usage or ad_account_usage):
        return

    usage_data = {}
    if app_usage:
        try:
            usage_data["app_usage"] = json.loads(app_usage)
        except (json.JSONDecodeError, TypeError):
            usage_data["app_usage_raw"] = str(app_usage)
    if biz_usage:
        try:
            usage_data["business_use_case_usage"] = json.loads(biz_usage)
        except (json.JSONDecodeError, TypeError):
            usage_data["business_use_case_usage_raw"] = str(biz_usage)
    if ad_account_usage:
        try:
            usage_data["ad_account_usage"] = json.loads(ad_account_usage)
        except (json.JSONDecodeError, TypeError):
            usage_data["ad_account_usage_raw"] = str(ad_account_usage)

    # Set proactive throttle for any account approaching its rate limit
    biz_data = usage_data.get("business_use_case_usage", {})
    if isinstance(biz_data, dict):
        for acct_id, entries in biz_data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                pct = max(
                    entry.get("call_count", 0),
                    entry.get("total_cputime", 0),
                    entry.get("total_time", 0),
                )
                if isinstance(pct, (int, float)) and pct >= 80:
                    _account_throttle_until[acct_id] = time.monotonic() + _THROTTLE_DELAY
                    logger.warning(
                        f"proactive_throttle account={acct_id} delay={_THROTTLE_DELAY}s "
                        f"usage={json.dumps(entry)}"
                    )

    # Warn at high usage levels (any field >= 80%)
    is_high = any(
        isinstance(pct, (int, float)) and pct >= 80
        for val in usage_data.values()
        if isinstance(val, dict)
        for pct in val.values()
    )

    log_fn = logger.warning if is_high else logger.info
    log_fn(f"meta_rate_limit_usage endpoint={endpoint} {json.dumps(usage_data)}")


async def make_api_request(
    endpoint: str,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET"
) -> Dict[str, Any]:
    """
    Make a request to the Meta Graph API with retry/backoff for rate-limit errors.

    Rate-limit responses (codes 4, 17, 32, 613 or HTTP 429) are retried up to
    _MAX_RETRIES times with exponential backoff. Auth errors and malformed-request
    errors are never retried. Outcome counts are logged after every call.
    """
    if not access_token:
        logger.error("API request attempted with blank access token")
        return {
            "error": {
                "message": "Authentication Required",
                "details": "A valid access token is required to access the Meta API",
                "action_required": "Please authenticate first"
            }
        }

    url = f"{META_GRAPH_API_BASE}/{endpoint}"
    headers = {"User-Agent": USER_AGENT}

    request_params = params or {}
    request_params["access_token"] = access_token

    # appsecret_proof — required for system user tokens
    app_secret = os.environ.get("META_APP_SECRET", "")
    if app_secret and access_token:
        request_params["appsecret_proof"] = hmac.new(
            app_secret.encode("utf-8"),
            access_token.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    masked_params = {k: "***MASKED***" if k in ("access_token", "appsecret_proof") else v for k, v in request_params.items()}
    logger.debug(f"API Request: {method} {url}")
    logger.debug(f"Request params: {masked_params}")

    app_id = auth_manager.app_id
    logger.debug(f"Current app_id from auth_manager: {app_id}")

    # Proactive throttle: if this account was flagged as near rate-limit, delay before sending.
    acct_match = re.search(r'(act_\d+)', endpoint)
    if acct_match:
        acct_id = acct_match.group(1)
        wait_secs = _account_throttle_until.get(acct_id, 0) - time.monotonic()
        if wait_secs > 0:
            logger.info(f"proactive_throttle_delay account={acct_id} wait={wait_secs:.1f}s")
            await asyncio.sleep(wait_secs)
            _account_throttle_until.pop(acct_id, None)

    # Prepare method-specific params once (mutations are idempotent, safe across retries)
    if method == "GET":
        encoded_params: Dict[str, Any] = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else v
            for k, v in request_params.items()
        }
    elif method == "POST":
        if 'targeting' in request_params and isinstance(request_params['targeting'], dict):
            request_params['targeting'] = json.dumps(request_params['targeting'])
        for key, value in list(request_params.items()):
            if isinstance(value, (list, dict)):
                request_params[key] = json.dumps(value)
        logger.debug(f"POST params (prepared): {masked_params}")
    elif method == "PUT":
        put_query_params = {k: request_params[k] for k in ("access_token", "appsecret_proof") if k in request_params}
        put_body_params = {
            k: json.dumps(v) if isinstance(v, (list, dict)) else v
            for k, v in request_params.items()
            if k not in ("access_token", "appsecret_proof")
        }

    rate_limited_count = 0

    async with httpx.AsyncClient() as client:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                if method == "GET":
                    response = await client.get(url, params=encoded_params, headers=headers, timeout=30.0)
                elif method == "POST":
                    response = await client.post(url, data=request_params, headers=headers, timeout=30.0)
                elif method == "PUT":
                    response = await client.put(url, params=put_query_params, data=put_body_params, headers=headers, timeout=30.0)
                elif method == "DELETE":
                    response = await client.delete(url, params=request_params, headers=headers, timeout=30.0)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()
                logger.debug(f"API Response status: {response.status_code}")
                _log_meta_rate_limit_headers(response.headers, endpoint)

                try:
                    result = response.json()
                except json.JSONDecodeError:
                    result = {"text_response": response.text, "status_code": response.status_code}

                outcome = "rate_limited_retried_success" if rate_limited_count > 0 else "success"
                _record_outcome(outcome)
                return result

            except httpx.HTTPStatusError as e:
                error_info: Dict[str, Any] = {}
                try:
                    error_info = e.response.json()
                except Exception:
                    error_info = {"status_code": e.response.status_code, "text": e.response.text}

                logger.error(f"HTTP Error: {e.response.status_code} - {error_info}")
                _log_meta_rate_limit_headers(e.response.headers, endpoint)

                error_obj = error_info.get("error", {}) if isinstance(error_info.get("error"), dict) else {}
                error_code = error_obj.get("code") if error_obj else None

                is_rate_limit = (error_code in _RATE_LIMIT_CODES) or (e.response.status_code == 429)

                if is_rate_limit and attempt < _MAX_RETRIES:
                    rate_limited_count += 1
                    delay = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"rate_limit_backoff attempt={attempt + 1}/{_MAX_RETRIES} "
                        f"delay={delay:.1f}s code={error_code} endpoint={endpoint}"
                    )
                    await asyncio.sleep(delay)
                    continue

                # Not retrying — classify outcome and apply existing error handling
                if is_rate_limit:
                    _record_outcome("rate_limited_exhausted")
                    logger.error(f"rate_limit_exhausted retries={_MAX_RETRIES} endpoint={endpoint}")
                elif error_code == 100:
                    _record_outcome("malformed_request")
                    logger.warning(f"Malformed request (code=100), not retrying: {error_obj.get('message', '')}")
                elif error_code in [190, 102, 200, 10] or e.response.status_code in [401, 403]:
                    _record_outcome("auth_error")
                    logger.warning(f"Detected Facebook API auth error: {error_code}")
                    if error_code == 200 and "Provide valid app ID" in error_obj.get("message", ""):
                        logger.error("Meta API authentication configuration issue")
                        logger.error(f"Current app_id: {app_id}")
                        return {
                            "error": {
                                "message": "Meta API authentication configuration issue. Please check your app credentials.",
                                "original_error": error_obj.get("message"),
                                "code": error_code
                            }
                        }
                    auth_manager.invalidate_token()
                else:
                    _record_outcome("other_error")

                full_response = {
                    "headers": dict(e.response.headers),
                    "status_code": e.response.status_code,
                    "url": str(e.response.url),
                    "reason": getattr(e.response, "reason_phrase", "Unknown reason"),
                    "request_method": e.request.method,
                    "request_url": str(e.request.url)
                }
                return {
                    "error": {
                        "message": f"HTTP Error: {e.response.status_code}",
                        "details": error_info,
                        "full_response": full_response
                    }
                }

            except Exception as e:
                logger.error(f"Request Error: {str(e)}")
                _record_outcome("other_error")
                return {"error": {"message": str(e)}}

    # Unreachable — loop always returns — but satisfies type checker
    return {"error": {"message": "Unexpected: retry loop exhausted"}}


# Generic wrapper for all Meta API tools
def meta_api_tool(func):
    """Decorator for Meta API tools that handles authentication and error handling."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            # Log function call
            logger.debug(f"Function call: {func.__name__}")
            logger.debug(f"Args: {args}")
            # Log kwargs without sensitive info
            safe_kwargs = {k: ('***TOKEN***' if k == 'access_token' else v) for k, v in kwargs.items()}
            logger.debug(f"Kwargs: {safe_kwargs}")
            
            # Log app ID information
            app_id = auth_manager.app_id
            logger.debug(f"Current app_id: {app_id}")
            logger.debug(f"META_APP_ID env var: {os.environ.get('META_APP_ID')}")
            
            # If access_token is not in kwargs or not kwargs['access_token'], try to get it from auth_manager
            if 'access_token' not in kwargs or not kwargs['access_token']:
                try:
                    access_token = await auth.get_current_access_token()
                    if access_token:
                        kwargs['access_token'] = access_token
                        logger.debug("Using access token from auth_manager")
                    else:
                        logger.warning("No access token available from auth_manager")
                        # Add more details about why token might be missing
                        if (auth_manager.app_id == "YOUR_META_APP_ID" or not auth_manager.app_id) and not auth_manager.use_pipeboard:
                            logger.error("TOKEN VALIDATION FAILED: No valid app_id configured")
                            logger.error("Please set META_APP_ID environment variable or configure in your code")
                        elif auth_manager.use_pipeboard:
                            logger.error("TOKEN VALIDATION FAILED: Pipeboard authentication enabled but no valid token available")
                            logger.error("Complete authentication via Pipeboard service or check PIPEBOARD_API_TOKEN")
                        else:
                            logger.error("Check logs above for detailed token validation failures")
                except Exception as e:
                    logger.error(f"Error getting access token: {str(e)}")
                    # Add stack trace for better debugging
                    import traceback
                    logger.error(f"Stack trace: {traceback.format_exc()}")
            
            # Final validation - if we still don't have a valid token, return authentication required
            if 'access_token' not in kwargs or not kwargs['access_token']:
                logger.warning("No access token available, authentication needed")
                
                # Add more specific troubleshooting information
                auth_url = auth_manager.get_auth_url()
                app_id = auth_manager.app_id
                using_pipeboard = auth_manager.use_pipeboard
                
                logger.error("TOKEN VALIDATION SUMMARY:")
                logger.error(f"- Current app_id: '{app_id}'")
                logger.error(f"- Environment META_APP_ID: '{os.environ.get('META_APP_ID', 'Not set')}'")
                logger.error(f"- Pipeboard API token configured: {'Yes' if os.environ.get('PIPEBOARD_API_TOKEN') else 'No'}")
                logger.error(f"- Using Pipeboard authentication: {'Yes' if using_pipeboard else 'No'}")
                
                # Check for common configuration issues - but only if not using Pipeboard
                if not using_pipeboard and (app_id == "YOUR_META_APP_ID" or not app_id):
                    logger.error("ISSUE DETECTED: No valid Meta App ID configured")
                    logger.error("ACTION REQUIRED: Set META_APP_ID environment variable with a valid App ID")
                elif using_pipeboard:
                    logger.error("ISSUE DETECTED: Pipeboard authentication configured but no valid token available")
                    logger.error("ACTION REQUIRED: Complete authentication via Pipeboard service")
                
                # Provide different guidance based on authentication method
                if using_pipeboard:
                    return json.dumps({
                        "error": {
                            "message": "Pipeboard Authentication Required",
                            "details": {
                                "description": "Your Pipeboard API token is invalid or has expired",
                                "action_required": "Update your Pipeboard token",
                                "setup_url": "https://pipeboard.co/setup",
                                "token_url": "https://pipeboard.co/api-tokens",
                                "configuration_status": {
                                    "app_id_configured": bool(app_id) and app_id != "YOUR_META_APP_ID",
                                    "pipeboard_enabled": True,
                                },
                                "troubleshooting": "Go to https://pipeboard.co/setup to verify your account setup, then visit https://pipeboard.co/api-tokens to obtain a new API token",
                                "setup_link": "[Verify your Pipeboard account setup](https://pipeboard.co/setup)",
                                "token_link": "[Get a new Pipeboard API token](https://pipeboard.co/api-tokens)"
                            }
                        }
                    }, indent=2)
                else:
                    return json.dumps({
                        "error": {
                            "message": "Authentication Required",
                            "details": {
                                "description": "You need to authenticate with the Meta API before using this tool",
                                "action_required": "Please authenticate first",
                                "auth_url": auth_url,
                                "configuration_status": {
                                    "app_id_configured": bool(app_id) and app_id != "YOUR_META_APP_ID",
                                    "pipeboard_enabled": False,
                                },
                                "troubleshooting": "Check logs for TOKEN VALIDATION FAILED messages",
                                "markdown_link": f"[Click here to authenticate with Meta Ads API]({auth_url})"
                            }
                        }
                    }, indent=2)
                
            # Call the original function
            result = await func(*args, **kwargs)
            
            # If the result is a string (JSON), try to parse it to check for errors
            if isinstance(result, str):
                try:
                    result_dict = json.loads(result)
                    if "error" in result_dict:
                        logger.error(f"Error in API response: {result_dict['error']}")
                        # If this is an app ID error, log more details
                        if isinstance(result_dict.get("details", {}).get("error", {}), dict):
                            error_obj = result_dict["details"]["error"]
                            if error_obj.get("code") == 200 and "Provide valid app ID" in error_obj.get("message", ""):
                                logger.error("Meta API authentication configuration issue")
                                logger.error(f"Current app_id: {app_id}")
                                # Replace the confusing error with a more user-friendly one
                                return json.dumps({
                                    "error": {
                                        "message": "Meta API Configuration Issue",
                                        "details": {
                                            "description": "Your Meta API app is not properly configured",
                                            "action_required": "Check your META_APP_ID environment variable",
                                            "current_app_id": app_id,
                                            "original_error": error_obj.get("message")
                                        }
                                    }
                                }, indent=2)
                except Exception:
                    # Not JSON or other parsing error, wrap it in a dictionary
                    return json.dumps({"data": result}, indent=2)
            
            # If result is already a dictionary, ensure it's properly serialized
            if isinstance(result, dict):
                return json.dumps(result, indent=2)
            
            return result
        except McpToolError:
            raise  # Let FastMCP set isError: true and refund the usage credit
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return json.dumps({"error": str(e)}, indent=2)

    return wrapper 