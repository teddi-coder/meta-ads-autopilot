"""hedgehog_meta.py — Meta Ads read-only MCP server.

All write/mutation tools live in hedgehog_meta_writer.py.
All Meta API calls go through _meta_get() / _meta_post() — no direct requests calls.

Required env vars:
  META_ACCESS_TOKEN   — long-lived system-user or page token
  META_APP_SECRET     — enables appsecret_proof (strongly recommended)
  GRAPH_VERSION       — Graph API version (default v24.0)
  MCP_API_KEY         — Bearer token for clients connecting to this server
"""

import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import httpx
from fastmcp import FastMCP

# ── Server setup ─────────────────────────────────────────────────────────────

mcp = FastMCP("hedgehog-meta")

GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v24.0")
META_GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _token() -> str:
    return os.getenv("META_ACCESS_TOKEN", "")


def _appsecret_proof(token: str) -> str:
    secret = os.getenv("META_APP_SECRET", "")
    if not secret or not token:
        return ""
    return hmac.new(secret.encode(), token.encode(), hashlib.sha256).hexdigest()


def _encode_params(params: dict) -> dict:
    """JSON-encode any dict/list values so httpx sends them as strings."""
    encoded: dict = {}
    for k, v in params.items():
        if isinstance(v, (dict, list)):
            encoded[k] = json.dumps(v)
        else:
            encoded[k] = v
    return encoded


async def _meta_get(endpoint: str, params: Optional[dict] = None) -> dict:
    """GET {GRAPH_BASE}/{endpoint} with auth + appsecret_proof injected."""
    token = _token()
    p = dict(params or {})
    p["access_token"] = token
    proof = _appsecret_proof(token)
    if proof:
        p["appsecret_proof"] = proof
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{META_GRAPH_BASE}/{endpoint}",
            params=_encode_params(p),
            timeout=30.0,
        )
    try:
        return r.json()
    except Exception:
        return {"error": {"message": r.text, "status_code": r.status_code}}


async def _meta_post(endpoint: str, data: Optional[dict] = None) -> dict:
    """POST {GRAPH_BASE}/{endpoint} with auth + appsecret_proof injected."""
    token = _token()
    d = dict(data or {})
    d["access_token"] = token
    proof = _appsecret_proof(token)
    if proof:
        d["appsecret_proof"] = proof
    # Meta POST expects form data; encode dicts/lists to JSON strings
    for k, v in list(d.items()):
        if isinstance(v, (dict, list)):
            d[k] = json.dumps(v)
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{META_GRAPH_BASE}/{endpoint}",
            data=d,
            timeout=30.0,
        )
    try:
        return r.json()
    except Exception:
        return {"error": {"message": r.text, "status_code": r.status_code}}


def _ensure_act(account_id: str) -> str:
    if account_id and not account_id.startswith("act_"):
        return f"act_{account_id}"
    return account_id


# ── Currency helpers ─────────────────────────────────────────────────────────

_ZERO_DECIMAL_CURRENCIES = {
    "BIF", "CLP", "DJF", "GNF", "JPY", "KMF", "KRW", "MGA",
    "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF",
}


def _cents_to_currency(amount, currency: str) -> str:
    try:
        amount_int = int(amount)
    except (TypeError, ValueError):
        return str(amount)
    if currency.upper() in _ZERO_DECIMAL_CURRENCIES:
        return str(amount_int)
    return f"{amount_int / 100:.2f}"


def _normalize_account_monetary_fields(account: dict) -> dict:
    currency = account.get("currency", "USD")
    for field in ("amount_spent", "balance"):
        if field in account:
            account[field] = _cents_to_currency(account[field], currency)
    return account


# ── Depth insight helpers ─────────────────────────────────────────────────────

def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _extract_action_value(actions: list, action_type: str) -> float:
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return _safe_float(a.get("value"))
    return 0.0


def _time_params(time_range: Union[str, Dict[str, str]]) -> dict:
    if isinstance(time_range, dict):
        return {"time_range": json.dumps(time_range)}
    return {"date_preset": time_range}


# ── Lead form time helpers ────────────────────────────────────────────────────

def _preset_to_unix(preset: str):
    now = datetime.now(timezone.utc)
    presets = {"last_7d": timedelta(days=7), "last_30d": timedelta(days=30)}
    delta = presets.get(preset.lower())
    if delta is None:
        return None, None
    start = now - delta
    return int(start.timestamp()), int(now.timestamp())


def _date_str_to_unix(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _resolve_time_range(time_range):
    if time_range is None:
        return None, None
    if isinstance(time_range, str):
        return _preset_to_unix(time_range)
    if isinstance(time_range, dict):
        since = time_range.get("since")
        until = time_range.get("until")
        if since and until:
            return _date_str_to_unix(since), _date_str_to_unix(until)
    return None, None


# ── compute_image_crops helpers ───────────────────────────────────────────────

_VALID_CROP_KEYS = [
    ("100x100", 100, 100),
    ("100x72", 100, 72),
    ("400x500", 400, 500),
    ("400x150", 400, 150),
    ("600x360", 600, 360),
    ("90x160", 90, 160),
]
_VALID_CROP_KEY_NAMES = [k for k, _, _ in _VALID_CROP_KEYS]


def _compute_crop_box(src_w: int, src_h: int, crop_w: int, crop_h: int) -> list:
    src_ratio = src_w / src_h
    crop_ratio = crop_w / crop_h
    if src_ratio > crop_ratio:
        fit_h = src_h
        fit_w = int(round(fit_h * crop_ratio))
    else:
        fit_w = src_w
        fit_h = int(round(fit_w / crop_ratio))
    left = (src_w - fit_w) // 2
    top = (src_h - fit_h) // 2
    right = left + fit_w
    bottom = top + fit_h
    return [[left, top], [right, bottom]]


# ── get_ad_creatives helper ───────────────────────────────────────────────────

def _extract_creative_image_urls(creative: dict) -> list:
    urls = []
    if creative.get("image_url"):
        urls.append(creative["image_url"])
    oss = creative.get("object_story_spec", {})
    ld = oss.get("link_data", {})
    if ld.get("picture"):
        urls.append(ld["picture"])
    if creative.get("thumbnail_url"):
        urls.append(creative["thumbnail_url"])
    return list(dict.fromkeys(urls))


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Account & Campaign Structure ─────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_ad_accounts(user_id: str = "me", limit: int = 200) -> str:
    """Get ad accounts accessible to the authenticated user.

    Args:
        user_id: Meta user ID or "me" for the current user (default: "me")
        limit: Maximum number of accounts to return (default: 200)
    """
    data = await _meta_get(
        f"{user_id}/adaccounts",
        {
            "fields": "id,name,account_id,account_status,amount_spent,balance,currency,age,business_city,business_country_code",
            "limit": limit,
        },
    )
    if "data" in data:
        data["data"] = [_normalize_account_monetary_fields(acc) for acc in data["data"]]
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_account_info(account_id: str) -> str:
    """Get detailed information about a specific ad account.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX or XXXXXXXXX)
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"}, indent=2)
    account_id = _ensure_act(account_id)
    data = await _meta_get(
        account_id,
        {"fields": "id,name,account_id,account_status,amount_spent,balance,currency,age,business_city,business_country_code,timezone_name"},
    )
    if "error" not in data:
        _normalize_account_monetary_fields(data)
        if "business_country_code" in data:
            eu = {"DE","FR","IT","ES","NL","BE","AT","IE","DK","SE","FI","NO"}
            data["dsa_required"] = data["business_country_code"] in eu
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_campaigns(
    account_id: str,
    limit: int = 10,
    status_filter: str = "",
    objective_filter: Union[str, List[str]] = "",
    after: str = "",
) -> str:
    """Get campaigns for a Meta Ads account.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        limit: Maximum number of campaigns to return (default: 10)
        status_filter: Filter by status — ACTIVE, PAUSED, ARCHIVED (leave empty for all)
        objective_filter: Filter by objective(s) — single string or list
        after: Pagination cursor from a previous response
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"}, indent=2)
    account_id = _ensure_act(account_id)
    params: dict = {
        "fields": "id,name,objective,status,daily_budget,lifetime_budget,buying_type,start_time,stop_time,created_time,updated_time,bid_strategy,special_ad_categories",
        "limit": limit,
    }
    if status_filter:
        params["effective_status"] = json.dumps([status_filter])
    filters = []
    if objective_filter:
        objs = [objective_filter] if isinstance(objective_filter, str) else list(objective_filter)
        objs = [o for o in objs if o]
        if objs:
            filters.append({"field": "objective", "operator": "IN", "value": objs})
    if filters:
        params["filtering"] = json.dumps(filters)
    if after:
        params["after"] = after
    data = await _meta_get(f"{account_id}/campaigns", params)
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_campaign_details(campaign_id: str) -> str:
    """Get detailed information about a specific campaign.

    Args:
        campaign_id: Meta Ads campaign ID
    """
    if not campaign_id:
        return json.dumps({"error": "campaign_id is required"}, indent=2)
    data = await _meta_get(
        campaign_id,
        {"fields": "id,name,objective,status,daily_budget,lifetime_budget,buying_type,start_time,stop_time,created_time,updated_time,bid_strategy,special_ad_categories,special_ad_category_country,budget_remaining,configured_status"},
    )
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_adsets(
    account_id: str,
    limit: int = 10,
    campaign_id: str = "",
) -> str:
    """Get ad sets for a Meta Ads account, optionally filtered by campaign.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        limit: Maximum number of ad sets to return (default: 10)
        campaign_id: Optional campaign ID to filter by
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"}, indent=2)
    account_id = _ensure_act(account_id)
    fields = "id,name,campaign_id,status,daily_budget,lifetime_budget,targeting,bid_amount,bid_strategy,bid_constraints,optimization_goal,billing_event,start_time,end_time,created_time,updated_time,is_dynamic_creative,frequency_control_specs{event,interval_days,max_frequency}"
    if campaign_id:
        endpoint = f"{campaign_id}/adsets"
    else:
        endpoint = f"{account_id}/adsets"
    data = await _meta_get(endpoint, {"fields": fields, "limit": limit})
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_adset_details(adset_id: str) -> str:
    """Get detailed information about a specific ad set.

    Args:
        adset_id: Meta Ads ad set ID
    """
    if not adset_id:
        return json.dumps({"error": "adset_id is required"}, indent=2)
    data = await _meta_get(
        adset_id,
        {"fields": "id,name,campaign_id,status,frequency_control_specs{event,interval_days,max_frequency},daily_budget,lifetime_budget,targeting,bid_amount,bid_strategy,bid_constraints,optimization_goal,billing_event,start_time,end_time,created_time,updated_time,attribution_spec,destination_type,promoted_object,pacing_type,budget_remaining,dsa_beneficiary,dsa_payor,is_dynamic_creative"},
    )
    if "frequency_control_specs" not in data:
        data["_meta"] = {"note": "No frequency_control_specs returned — no frequency cap is set or the API did not include this field."}
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_ads(
    account_id: str,
    limit: int = 10,
    campaign_id: str = "",
    adset_id: str = "",
) -> str:
    """Get ads for a Meta Ads account with optional filtering.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        limit: Maximum number of ads to return (default: 10)
        campaign_id: Optional campaign ID to filter by
        adset_id: Optional ad set ID to filter by (takes priority over campaign_id)
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"}, indent=2)
    account_id = _ensure_act(account_id)
    fields = "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs"
    if adset_id:
        endpoint = f"{adset_id}/ads"
    elif campaign_id:
        endpoint = f"{campaign_id}/ads"
    else:
        endpoint = f"{account_id}/ads"
    data = await _meta_get(endpoint, {"fields": fields, "limit": limit})
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_ad_details(ad_id: str) -> str:
    """Get detailed information about a specific ad.

    Args:
        ad_id: Meta Ads ad ID
    """
    if not ad_id:
        return json.dumps({"error": "ad_id is required"}, indent=2)
    data = await _meta_get(
        ad_id,
        {"fields": "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs,preview_shareable_link"},
    )
    return json.dumps(data, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Performance Insights ─────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

# Redundant action-type prefixes to strip in compact mode
_REDUNDANT_ACTION_PREFIXES = (
    "omni_",
    "onsite_web_app_",
    "onsite_web_",
    "onsite_app_",
    "web_app_in_store_",
    "offsite_conversion.fb_pixel_",
)


def _strip_redundant_actions(row: dict) -> dict:
    for key in ("actions", "action_values", "cost_per_action_type"):
        items = row.get(key)
        if not isinstance(items, list):
            continue
        row[key] = [
            item for item in items
            if not any(
                item.get("action_type", "").startswith(pfx)
                for pfx in _REDUNDANT_ACTION_PREFIXES
            )
        ]
    return row


@mcp.tool()
async def get_campaign_insights(
    object_id: str = "",
    time_range: Union[str, Dict[str, str]] = "maximum",
    breakdown: str = "",
    level: str = "ad",
    limit: int = 25,
    after: str = "",
    action_attribution_windows: Optional[List[str]] = None,
    compact: bool = False,
    account_id: str = "",
    campaign_id: str = "",
    adset_id: str = "",
    ad_id: str = "",
) -> str:
    """Get performance insights for a campaign, ad set, ad, or account.

    Args:
        object_id: ID of the object to query (account, campaign, ad set, or ad)
        time_range: Preset string (today, yesterday, last_7d, last_30d, last_month,
                    this_month, maximum, etc.) or {"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}
        breakdown: Optional breakdown dimension (age, gender, country, device_platform,
                   publisher_platform, platform_position, impression_device, etc.)
        level: Aggregation level — ad, adset, campaign, or account (default: ad)
        limit: Maximum results per page (default: 25)
        after: Pagination cursor from a previous response
        action_attribution_windows: List of attribution windows e.g. ["1d_click","7d_click"]
        compact: Strip redundant action-type duplicates to shrink response (~60% smaller)
        account_id: Alias for object_id when querying account-level insights
        campaign_id: Alias for object_id when querying campaign-level insights
        adset_id: Alias for object_id when querying ad-set-level insights
        ad_id: Alias for object_id when querying ad-level insights
    """
    if not object_id:
        object_id = account_id or campaign_id or adset_id or ad_id
    if not object_id:
        return json.dumps({"error": "Provide object_id, account_id, campaign_id, adset_id, or ad_id"}, indent=2)

    params: dict = {
        "fields": "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,cpc,cpm,ctr,reach,frequency,actions,action_values,conversions,unique_clicks,cost_per_action_type",
        "level": level,
        "limit": limit,
    }
    if isinstance(time_range, dict):
        if "since" in time_range and "until" in time_range:
            params["time_range"] = json.dumps(time_range)
        else:
            return json.dumps({"error": "time_range dict must contain 'since' and 'until' keys"}, indent=2)
    else:
        params["date_preset"] = time_range
    if breakdown:
        params["breakdowns"] = breakdown
    if after:
        params["after"] = after
    if action_attribution_windows:
        params["action_attribution_windows"] = "[" + ",".join(f"'{w}'" for w in action_attribution_windows) + "]"

    data = await _meta_get(f"{object_id}/insights", params)
    if compact and isinstance(data, dict):
        for row in data.get("data", []):
            if isinstance(row, dict):
                _strip_redundant_actions(row)
    return json.dumps(data, indent=2)


@mcp.tool()
async def ad_creative_performance(
    account_id: str,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    sort_by: str = "spend",
    limit: int = 25,
) -> str:
    """Get ad-level creative performance ranked by spend, ROAS, CTR, or conversions.

    Returns one row per ad with core metrics plus thumbnail URL and primary copy.
    Use when asked "which creatives are working" or "what's our best performing ad".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        time_range: Preset string or {"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}
        campaign_ids: Optional list of campaign IDs to filter to
        sort_by: One of spend, roas, ctr, conversions (default: spend)
        limit: Max ads to return (default: 25)
    """
    account_id = _ensure_act(account_id)
    params: dict = {
        "fields": "ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions,action_values,purchase_roas",
        "level": "ad",
        "limit": limit,
        **_time_params(time_range),
    }
    if campaign_ids:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "IN", "value": campaign_ids}])

    data = await _meta_get(f"{account_id}/insights", params)
    rows = data.get("data", [])

    results = []
    for row in rows:
        spend = _safe_float(row.get("spend"))
        impressions = _safe_float(row.get("impressions"))
        clicks = _safe_float(row.get("clicks"))
        ctr = _safe_float(row.get("ctr"))
        cpc = _safe_float(row.get("cpc"))
        conversions = _extract_action_value(row.get("actions", []), "purchase")
        if conversions == 0:
            conversions = _extract_action_value(row.get("actions", []), "lead")
        if conversions == 0:
            conversions = _extract_action_value(row.get("actions", []), "complete_registration")
        conv_value = _extract_action_value(row.get("action_values", []), "purchase")
        roas_raw = row.get("purchase_roas", [])
        roas = _safe_float(roas_raw[0].get("value")) if roas_raw else (conv_value / spend if spend > 0 else 0)
        results.append({
            "ad_id": row.get("ad_id"),
            "ad_name": row.get("ad_name"),
            "campaign_name": row.get("campaign_name"),
            "adset_name": row.get("adset_name"),
            "spend": spend,
            "impressions": int(impressions),
            "clicks": int(clicks),
            "ctr": round(ctr, 4),
            "cpc": round(cpc, 2),
            "conversions": conversions,
            "conversion_value": conv_value,
            "roas": round(roas, 2),
        })

    sort_key = {
        "spend": lambda r: r["spend"],
        "roas": lambda r: r["roas"],
        "ctr": lambda r: r["ctr"],
        "conversions": lambda r: r["conversions"],
    }.get(sort_by, lambda r: r["spend"])
    results.sort(key=sort_key, reverse=True)

    # Enrich with thumbnails for the top ads
    ad_ids = [r["ad_id"] for r in results[:limit] if r["ad_id"]]
    if ad_ids:
        creative_data = await _meta_get(
            f"{account_id}/ads",
            {
                "fields": "id,creative{id,thumbnail_url,body,title}",
                "filtering": json.dumps([{"field": "id", "operator": "IN", "value": ad_ids}]),
                "limit": limit,
            },
        )
        creative_map: dict = {}
        for ad in creative_data.get("data", []):
            creative = ad.get("creative", {})
            creative_map[ad.get("id")] = {
                "thumbnail_url": creative.get("thumbnail_url", ""),
                "primary_text": creative.get("body", ""),
                "headline": creative.get("title", ""),
            }
        for r in results:
            cr = creative_map.get(r["ad_id"], {})
            r["thumbnail_url"] = cr.get("thumbnail_url", "")
            r["primary_text"] = cr.get("primary_text", "")
            r["headline"] = cr.get("headline", "")

    return json.dumps({"data": results[:limit], "total_ads": len(results)}, indent=2)


@mcp.tool()
async def ad_creative_fatigue(
    account_id: str,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    ad_ids: Optional[List[str]] = None,
    limit: int = 10,
) -> str:
    """Detect creative fatigue by analysing daily frequency and CTR trends per ad.

    Returns a fatigue_signal per ad: "fatiguing" (frequency rising + CTR falling),
    "healthy", or "insufficient_data". Use when asked "is the audience fatiguing"
    or "do we need fresh creative".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        time_range: Preset string or date dict (default: last_30d)
        ad_ids: Optional list of specific ad IDs to check (default: top N by spend)
        limit: Max ads to analyse when ad_ids is not provided (default: 10)
    """
    account_id = _ensure_act(account_id)
    params: dict = {
        "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,frequency,actions",
        "level": "ad",
        "time_increment": 1,
        "limit": 500,
        **_time_params(time_range),
    }
    if ad_ids:
        params["filtering"] = json.dumps([{"field": "ad.id", "operator": "IN", "value": ad_ids}])

    data = await _meta_get(f"{account_id}/insights", params)
    rows = data.get("data", [])

    by_ad: dict = {}
    for row in rows:
        aid = row.get("ad_id")
        if aid not in by_ad:
            by_ad[aid] = {"ad_id": aid, "ad_name": row.get("ad_name", ""), "total_spend": 0.0, "daily_series": []}
        spend = _safe_float(row.get("spend"))
        by_ad[aid]["total_spend"] += spend
        conversions = _extract_action_value(row.get("actions", []), "purchase")
        if conversions == 0:
            conversions = _extract_action_value(row.get("actions", []), "lead")
        by_ad[aid]["daily_series"].append({
            "date": row.get("date_start", ""),
            "frequency": _safe_float(row.get("frequency")),
            "ctr": _safe_float(row.get("ctr")),
            "conversions": conversions,
        })

    ads_list = sorted(by_ad.values(), key=lambda a: a["total_spend"], reverse=True)
    if not ad_ids:
        ads_list = ads_list[:limit]

    for ad in ads_list:
        series = sorted(ad["daily_series"], key=lambda d: d["date"])
        if len(series) < 7:
            ad["fatigue_signal"] = "insufficient_data"
            continue
        last_7 = series[-7:]
        first_half = last_7[:3]
        second_half = last_7[-3:]
        avg_freq_early = sum(d["frequency"] for d in first_half) / len(first_half)
        avg_freq_late = sum(d["frequency"] for d in second_half) / len(second_half)
        avg_ctr_early = sum(d["ctr"] for d in first_half) / len(first_half)
        avg_ctr_late = sum(d["ctr"] for d in second_half) / len(second_half)
        freq_rising = avg_freq_late > avg_freq_early * 1.1
        ctr_falling = avg_ctr_late < avg_ctr_early * 0.9
        ad["fatigue_signal"] = "fatiguing" if (freq_rising and ctr_falling) else "healthy"

    return json.dumps({"data": ads_list}, indent=2)


@mcp.tool()
async def ad_performance_by_placement(
    account_id: str,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    level: str = "campaign",
) -> str:
    """Get performance broken down by placement (Feed vs Stories vs Reels etc).

    Returns one row per (entity, publisher_platform, platform_position,
    impression_device). Use when asked "where is spend going" or
    "which placements are working".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        time_range: Preset string or date dict (default: last_30d)
        campaign_ids: Optional campaign ID filter
        level: Aggregation level — campaign, adset, or ad (default: campaign)
    """
    account_id = _ensure_act(account_id)
    params: dict = {
        "fields": "campaign_name,adset_name,ad_name,spend,impressions,clicks,ctr,cpc,actions,action_values,purchase_roas",
        "level": level,
        "breakdowns": "publisher_platform,platform_position,impression_device",
        "limit": 200,
        **_time_params(time_range),
    }
    if campaign_ids:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "IN", "value": campaign_ids}])

    data = await _meta_get(f"{account_id}/insights", params)
    rows = data.get("data", [])

    results = []
    for row in rows:
        spend = _safe_float(row.get("spend"))
        conversions = _extract_action_value(row.get("actions", []), "purchase")
        if conversions == 0:
            conversions = _extract_action_value(row.get("actions", []), "lead")
        conv_value = _extract_action_value(row.get("action_values", []), "purchase")
        roas_raw = row.get("purchase_roas", [])
        roas = _safe_float(roas_raw[0].get("value")) if roas_raw else (conv_value / spend if spend > 0 else 0)
        results.append({
            "campaign_name": row.get("campaign_name", ""),
            "adset_name": row.get("adset_name", ""),
            "ad_name": row.get("ad_name", ""),
            "publisher_platform": row.get("publisher_platform", ""),
            "platform_position": row.get("platform_position", ""),
            "impression_device": row.get("impression_device", ""),
            "spend": spend,
            "impressions": int(_safe_float(row.get("impressions"))),
            "clicks": int(_safe_float(row.get("clicks"))),
            "ctr": round(_safe_float(row.get("ctr")), 4),
            "cpc": round(_safe_float(row.get("cpc")), 2),
            "conversions": conversions,
            "conversion_value": conv_value,
            "roas": round(roas, 2),
        })

    results.sort(key=lambda r: r["spend"], reverse=True)
    return json.dumps({"data": results, "total_rows": len(results)}, indent=2)


@mcp.tool()
async def ad_performance_by_demographic(
    account_id: str,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    dimension: str = "age_gender",
) -> str:
    """Get performance broken down by demographic dimension.

    Returns one row per (campaign, dimension_value). Use when asked
    "which demographics convert best" or "where are our customers".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        time_range: Preset string or date dict (default: last_30d)
        campaign_ids: Optional campaign ID filter
        dimension: One of age_gender, country, region, dma (default: age_gender)
    """
    account_id = _ensure_act(account_id)
    breakdown_map = {
        "age_gender": "age,gender",
        "country": "country",
        "region": "region",
        "dma": "dma",
    }
    breakdowns = breakdown_map.get(dimension, "age,gender")
    params: dict = {
        "fields": "campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions,action_values,purchase_roas",
        "level": "campaign",
        "breakdowns": breakdowns,
        "limit": 200,
        **_time_params(time_range),
    }
    if campaign_ids:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "IN", "value": campaign_ids}])

    data = await _meta_get(f"{account_id}/insights", params)
    rows = data.get("data", [])

    results = []
    for row in rows:
        spend = _safe_float(row.get("spend"))
        impressions = _safe_float(row.get("impressions"))
        clicks = _safe_float(row.get("clicks"))
        conversions = _extract_action_value(row.get("actions", []), "purchase")
        if conversions == 0:
            conversions = _extract_action_value(row.get("actions", []), "lead")
        conv_value = _extract_action_value(row.get("action_values", []), "purchase")
        roas_raw = row.get("purchase_roas", [])
        roas = _safe_float(roas_raw[0].get("value")) if roas_raw else (conv_value / spend if spend > 0 else 0)
        cpa = spend / conversions if conversions > 0 else 0
        entry: dict = {
            "campaign_name": row.get("campaign_name", ""),
            "spend": spend,
            "impressions": int(impressions),
            "clicks": int(clicks),
            "ctr": round(_safe_float(row.get("ctr")), 4),
            "conversions": conversions,
            "cpa": round(cpa, 2),
            "roas": round(roas, 2),
        }
        if dimension == "age_gender":
            entry["age"] = row.get("age", "")
            entry["gender"] = row.get("gender", "")
        elif dimension == "country":
            entry["country"] = row.get("country", "")
        elif dimension == "region":
            entry["region"] = row.get("region", "")
        elif dimension == "dma":
            entry["dma"] = row.get("dma", "")
        results.append(entry)

    results.sort(key=lambda r: r["spend"], reverse=True)
    return json.dumps({"data": results, "total_rows": len(results)}, indent=2)


@mcp.tool()
async def video_ad_performance(
    account_id: str,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    sort_by: str = "spend",
    limit: int = 25,
) -> str:
    """Get video-specific engagement metrics: hook rate, hold rate, thruplay rate.

    Computed metrics:
    - hook_rate: 3-second plays / impressions (0.0–1.0)
    - hold_rate: 75% completions / 3-second plays (0.0–1.0)
    - thruplay_rate: thruplays / impressions (0.0–1.0)
    - completion_rate: 100% completions / video plays (0.0–1.0)

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        time_range: Preset string or date dict (default: last_30d)
        campaign_ids: Optional campaign ID filter
        sort_by: One of spend, thruplay_rate, hook_rate, hold_rate (default: spend)
        limit: Max ads to return (default: 25)
    """
    account_id = _ensure_act(account_id)
    params: dict = {
        "fields": (
            "ad_id,ad_name,campaign_name,adset_name,spend,impressions,"
            "video_play_actions,video_p25_watched_actions,video_p50_watched_actions,"
            "video_p75_watched_actions,video_p100_watched_actions,"
            "video_thruplay_watched_actions,video_avg_time_watched_actions"
        ),
        "level": "ad",
        "limit": limit,
        **_time_params(time_range),
    }
    if campaign_ids:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "IN", "value": campaign_ids}])

    data = await _meta_get(f"{account_id}/insights", params)
    rows = data.get("data", [])

    def _vaction(actions_list, action_type):
        if not actions_list:
            return 0.0
        for a in actions_list:
            if a.get("action_type") == action_type:
                return _safe_float(a.get("value"))
        return 0.0

    results = []
    for row in rows:
        spend = _safe_float(row.get("spend"))
        impressions = _safe_float(row.get("impressions"))
        plays = _vaction(row.get("video_play_actions", []), "video_view")
        plays_3s = _vaction(row.get("video_play_actions", []), "video_view")  # 3s plays
        p75 = _vaction(row.get("video_p75_watched_actions", []), "video_view")
        p100 = _vaction(row.get("video_p100_watched_actions", []), "video_view")
        thruplays = _vaction(row.get("video_thruplay_watched_actions", []), "video_view")
        hook_rate = plays_3s / impressions if impressions > 0 else 0
        hold_rate = p75 / plays_3s if plays_3s > 0 else 0
        thruplay_rate = thruplays / impressions if impressions > 0 else 0
        completion_rate = p100 / plays if plays > 0 else 0
        results.append({
            "ad_id": row.get("ad_id"),
            "ad_name": row.get("ad_name"),
            "campaign_name": row.get("campaign_name"),
            "adset_name": row.get("adset_name"),
            "spend": spend,
            "impressions": int(impressions),
            "video_plays": int(plays),
            "hook_rate": round(hook_rate, 4),
            "hold_rate": round(hold_rate, 4),
            "thruplay_rate": round(thruplay_rate, 4),
            "completion_rate": round(completion_rate, 4),
            "thruplays": int(thruplays),
        })

    sort_key = {
        "spend": lambda r: r["spend"],
        "thruplay_rate": lambda r: r["thruplay_rate"],
        "hook_rate": lambda r: r["hook_rate"],
        "hold_rate": lambda r: r["hold_rate"],
    }.get(sort_by, lambda r: r["spend"])
    results.sort(key=sort_key, reverse=True)
    return json.dumps({"data": results[:limit], "total_ads": len(results)}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Creatives & Assets ───────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_ad_creatives(ad_id: str) -> str:
    """Get creative details for a specific ad.

    Requires an ad_id (not account_id). Use get_ads first to find ad IDs.
    Image hashes in asset_feed_spec are automatically resolved to URLs.

    Args:
        ad_id: Meta Ads ad ID (required)
    """
    if not ad_id:
        return json.dumps({"error": "ad_id is required"}, indent=2)

    data = await _meta_get(
        f"{ad_id}/adcreatives",
        {"fields": "id,name,status,thumbnail_url,image_url,image_hash,object_story_spec,object_type,body,title,effective_object_story_id,asset_feed_spec,url_tags,image_urls_for_viewing,product_set_id,degrees_of_freedom_spec"},
    )

    if "data" in data:
        # Resolve asset_feed_spec image hashes to URLs
        image_hashes: set = set()
        for creative in data["data"]:
            afs = creative.get("asset_feed_spec", {})
            for image in afs.get("images", []):
                if "hash" in image and "url" not in image:
                    image_hashes.add(image["hash"])

        if image_hashes:
            ad_info = await _meta_get(ad_id, {"fields": "account_id"})
            acct = ad_info.get("account_id")
            if acct:
                img_data = await _meta_get(
                    f"act_{acct}/adimages",
                    {"fields": "hash,url,width,height", "hashes": json.dumps(list(image_hashes))},
                )
                hash_to_url: dict = {}
                for img in img_data.get("data", []):
                    if "hash" in img and "url" in img:
                        hash_to_url[img["hash"]] = img["url"]
                for creative in data["data"]:
                    for image in creative.get("asset_feed_spec", {}).get("images", []):
                        if "hash" in image and image["hash"] in hash_to_url:
                            image["url"] = hash_to_url[image["hash"]]

        for creative in data["data"]:
            creative["image_urls_for_viewing"] = _extract_creative_image_urls(creative)

    return json.dumps(data, indent=2)


@mcp.tool()
async def get_creative_details(creative_id: str) -> str:
    """Get detailed information about a specific ad creative by its ID.

    Args:
        creative_id: Meta Ads creative ID
    """
    if not creative_id:
        return json.dumps({"error": "creative_id is required"}, indent=2)
    data = await _meta_get(
        creative_id,
        {"fields": "id,name,status,thumbnail_url,image_url,image_hash,object_story_spec,object_type,body,title,effective_object_story_id,asset_feed_spec{images,videos,bodies,titles,descriptions,link_urls,ad_formats,call_to_action_types,optimization_type,asset_customization_rules},url_tags,link_url"},
    )
    if isinstance(data, dict) and "id" in data:
        for opt_field in ["dynamic_creative_spec", "degrees_of_freedom_spec", "product_set_id"]:
            opt_data = await _meta_get(creative_id, {"fields": opt_field})
            if isinstance(opt_data, dict) and opt_field in opt_data:
                data[opt_field] = opt_data[opt_field]
        if "product_set_id" in data:
            catalog_data = await _meta_get(data["product_set_id"], {"fields": "product_catalog{id,name}"})
            catalog = catalog_data.get("product_catalog", {})
            if catalog.get("id"):
                data["catalog_id"] = catalog["id"]
                if catalog.get("name"):
                    data["catalog_name"] = catalog["name"]
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_ad_video(
    ad_id: str = "",
    video_id: str = "",
    account_id: str = "",
) -> str:
    """Get video details and source URL for a Meta ad video creative.

    Provide either ad_id (auto-extracts the video from the creative) or video_id
    directly. Providing account_id is strongly recommended — it uses the advideos
    edge which works with Business Manager tokens.

    Args:
        ad_id: Meta Ads ad ID (will extract video_id from the creative)
        video_id: Meta video ID (use if you already have it from get_ad_creatives)
        account_id: Ad account ID — enables advideos edge lookup
    """
    if not ad_id and not video_id:
        return json.dumps({"error": "Provide either ad_id or video_id"}, indent=2)

    if not video_id:
        creatives_json = await get_ad_creatives(ad_id=ad_id)
        creative_data = json.loads(creatives_json)
        if "error" in creative_data:
            return json.dumps({"error": f"Could not get creatives for ad {ad_id}", "details": creative_data}, indent=2)
        if "data" in creative_data and creative_data["data"]:
            creative = creative_data["data"][0]
            oss = creative.get("object_story_spec", {})
            if "video_data" in oss:
                video_id = str(oss["video_data"].get("video_id", ""))
            if not video_id:
                afs = creative.get("asset_feed_spec", {})
                vids = afs.get("videos", [])
                if vids:
                    video_id = str(vids[0].get("video_id", ""))
        if not video_id:
            return json.dumps({"error": "No video found in this ad creative. This may be an image ad — use get_ad_creatives."}, indent=2)

    video_fields = "source,title,description,length,picture,thumbnails,created_time"
    if account_id and account_id.startswith("act_"):
        account_id = account_id[4:]
    if not account_id and ad_id:
        ad_info = await _meta_get(ad_id, {"fields": "account_id"})
        account_id = ad_info.get("account_id", "")

    video_data = None
    if account_id:
        advideos = await _meta_get(
            f"act_{account_id}/advideos",
            {"fields": video_fields, "filtering": json.dumps([{"field": "id", "operator": "IN", "value": [video_id]}])},
        )
        if "data" in advideos and advideos["data"]:
            video_data = advideos["data"][0]

    if not video_data:
        video_data = await _meta_get(video_id, {"fields": video_fields})

    if "error" in video_data:
        return json.dumps({"error": f"Could not get video {video_id}", "details": video_data}, indent=2)

    result = {
        "video_id": video_id,
        "source_url": video_data.get("source"),
        "thumbnail_url": video_data.get("picture"),
        "title": video_data.get("title"),
        "description": video_data.get("description"),
        "duration_seconds": video_data.get("length"),
        "created_time": video_data.get("created_time"),
    }
    if ad_id:
        result["ad_id"] = ad_id
    if not result["source_url"]:
        result["warning"] = "No source URL returned — the video may be deleted or you may lack permissions."
    return json.dumps(result, indent=2)


@mcp.tool()
async def compute_image_crops(
    image_width: int,
    image_height: int,
    crop_keys: Optional[List[str]] = None,
) -> str:
    """Compute image_crops coordinates for a source image — pure calculation, no API call.

    Returns the image_crops dict ready to pass to create_ad_creative.

    Args:
        image_width: Width of the source image in pixels (e.g. 1080)
        image_height: Height of the source image in pixels (e.g. 1080)
        crop_keys: Optional list of specific crop keys. Defaults to all 6:
                   "100x100" (1:1 square), "100x72" (~1.39:1), "400x500" (4:5 portrait),
                   "400x150" (~2.67:1 banner), "600x360" (~1.67:1), "90x160" (9:16 tall)
    """
    if image_width <= 0 or image_height <= 0:
        return json.dumps({"error": "image_width and image_height must be positive integers"}, indent=2)
    requested = crop_keys if crop_keys else _VALID_CROP_KEY_NAMES
    key_map = {k: (kw, kh) for k, kw, kh in _VALID_CROP_KEYS}
    crops: dict = {}
    warnings: list = []
    for key in requested:
        if key not in key_map:
            warnings.append(f"'{key}' is not a valid Meta API crop key. Valid: {', '.join(_VALID_CROP_KEY_NAMES)}")
            continue
        kw, kh = key_map[key]
        crops[key] = _compute_crop_box(image_width, image_height, kw, kh)
    result: dict = {
        "image_crops": crops,
        "usage": "Pass image_crops directly to create_ad_creative.",
        "source_dimensions": {"width": image_width, "height": image_height},
    }
    if warnings:
        result["warnings"] = warnings
    return json.dumps(result, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Audience Research & Targeting ────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def search_interests(query: str, limit: int = 25) -> str:
    """Search for interest targeting options by keyword.

    Args:
        query: Search term (e.g. "baseball", "cooking", "travel")
        limit: Maximum results to return (default: 25)
    """
    if not query:
        return json.dumps({"error": "query is required"}, indent=2)
    data = await _meta_get("search", {"type": "adinterest", "q": query, "limit": limit})
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_interest_suggestions(interest_list: List[str], limit: int = 25) -> str:
    """Get interest suggestions based on a list of existing interests.

    Args:
        interest_list: List of interest names to get suggestions for
        limit: Maximum suggestions to return (default: 25)
    """
    if not interest_list:
        return json.dumps({"error": "interest_list is required"}, indent=2)
    data = await _meta_get(
        "search",
        {"type": "adinterestsuggestion", "interest_list": json.dumps(interest_list), "limit": limit},
    )
    return json.dumps(data, indent=2)


@mcp.tool()
async def estimate_audience_size(
    account_id: str,
    targeting: Optional[Dict[str, Any]] = None,
    optimization_goal: str = "REACH",
    interest_list: Optional[List[str]] = None,
    interest_fbid_list: Optional[List[str]] = None,
) -> str:
    """Estimate audience size for targeting specifications.

    For simple interest validation pass interest_list or interest_fbid_list.
    For full audience estimation pass account_id + targeting dict.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX) — required for full estimation
        targeting: Complete targeting spec (age, geo, interests, behaviors, etc.)
        optimization_goal: Optimization goal for estimation (default: REACH)
        interest_list: List of interest names for simple validation (backwards compat)
        interest_fbid_list: List of interest IDs for simple validation (backwards compat)
    """
    is_simple = bool(interest_list or interest_fbid_list) and not targeting
    if is_simple:
        params: dict = {"type": "adinterestvalid"}
        if interest_list:
            params["interest_list"] = json.dumps(interest_list)
        if interest_fbid_list:
            params["interest_fbid_list"] = json.dumps(interest_fbid_list)
        data = await _meta_get("search", params)
        return json.dumps(data, indent=2)

    if not account_id:
        return json.dumps({"error": "account_id is required for full audience estimation"}, indent=2)
    if not targeting:
        return json.dumps({"error": "targeting is required for full audience estimation"}, indent=2)
    account_id = _ensure_act(account_id)

    data = await _meta_get(f"{account_id}/reachestimate", {"targeting_spec": targeting})
    if "error" in data:
        # Attempt delivery_estimate fallback
        fb_data = await _meta_get(
            f"{account_id}/delivery_estimate",
            {"targeting_spec": json.dumps(targeting), "optimization_goal": optimization_goal},
        )
        if "data" in fb_data and fb_data["data"]:
            est = fb_data["data"][0]
            return json.dumps({
                "success": True,
                "account_id": account_id,
                "estimated_audience_size": est.get("estimate_mau", 0),
                "estimate_details": {
                    "monthly_active_users": est.get("estimate_mau", 0),
                    "daily_outcomes_curve": est.get("estimate_dau", []),
                },
                "endpoint_used": "delivery_estimate",
                "raw_response": fb_data,
            }, indent=2)
        return json.dumps({
            "error": "Both reachestimate and delivery_estimate failed",
            "reachestimate_error": data.get("error"),
            "delivery_estimate_response": fb_data,
        }, indent=2)

    # Normalise response
    response_data = data.get("data", data)
    if isinstance(response_data, dict):
        lower = response_data.get("users_lower_bound", response_data.get("estimate_mau_lower_bound"))
        upper = response_data.get("users_upper_bound", response_data.get("estimate_mau_upper_bound"))
        midpoint = int((lower + upper) / 2) if isinstance(lower, (int, float)) and isinstance(upper, (int, float)) else None
        return json.dumps({
            "success": True,
            "account_id": account_id,
            "estimated_audience_size": midpoint or 0,
            "estimate_details": {"users_lower_bound": lower, "users_upper_bound": upper},
            "raw_response": data,
        }, indent=2)

    return json.dumps(data, indent=2)


@mcp.tool()
async def search_behaviors(limit: int = 50) -> str:
    """Get all available behavior targeting options.

    Args:
        limit: Maximum results to return (default: 50)
    """
    data = await _meta_get("search", {"type": "adTargetingCategory", "class": "behaviors", "limit": limit})
    return json.dumps(data, indent=2)


@mcp.tool()
async def search_demographics(
    demographic_class: str = "demographics",
    limit: int = 50,
) -> str:
    """Get demographic targeting options.

    Args:
        demographic_class: Type of demographics — demographics, life_events,
                           industries, income, family_statuses, user_device, user_os
        limit: Maximum results to return (default: 50)
    """
    data = await _meta_get(
        "search",
        {"type": "adTargetingCategory", "class": demographic_class, "limit": limit},
    )
    return json.dumps(data, indent=2)


@mcp.tool()
async def search_geo_locations(
    query: str,
    location_types: Optional[List[str]] = None,
    limit: int = 25,
) -> str:
    """Search for geographic targeting locations.

    Args:
        query: Search term (e.g. "New York", "California", "Japan")
        location_types: Types to search: country, region, city, zip, geo_market,
                        electoral_district. Leave empty to search all types.
        limit: Maximum results to return (default: 25)
    """
    if not query:
        return json.dumps({"error": "query is required"}, indent=2)
    params: dict = {"type": "adgeolocation", "q": query, "limit": limit}
    if location_types:
        params["location_types"] = json.dumps(location_types)
    data = await _meta_get("search", params)
    return json.dumps(data, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Ad Library (Competitor Research) ─────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def search_ads_archive(
    search_terms: str,
    ad_reached_countries: List[str],
    ad_type: str = "ALL",
    limit: int = 25,
    fields: str = "ad_creation_time,ad_creative_body,ad_creative_link_caption,ad_creative_link_description,ad_creative_link_title,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,currency,demographic_distribution,funding_entity,impressions,page_id,page_name,publisher_platform,region_distribution,spend",
) -> str:
    """Search the Facebook Ads Library archive.

    Args:
        search_terms: The search query for ads
        ad_reached_countries: List of country codes (e.g. ["US", "GB"])
        ad_type: Type of ads — POLITICAL_AND_ISSUE_ADS, HOUSING_ADS, ALL (default: ALL)
        limit: Maximum number of ads to return (default: 25)
        fields: Comma-separated fields to retrieve for each ad
    """
    if not search_terms:
        return json.dumps({"error": "search_terms is required"}, indent=2)
    if not ad_reached_countries:
        return json.dumps({"error": "ad_reached_countries is required"}, indent=2)
    data = await _meta_get(
        "ads_archive",
        {
            "search_terms": search_terms,
            "ad_type": ad_type,
            "ad_reached_countries": json.dumps(ad_reached_countries),
            "limit": limit,
            "fields": fields,
        },
    )
    return json.dumps(data, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Demographics & Placements ────────────────────────────────────────────────
# (Pages & Lead Forms)
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def search_pages_by_name(
    account_id: str,
    search_term: Optional[str] = None,
) -> str:
    """Search for Facebook Pages by name within an account.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        search_term: Optional search term to filter pages by name
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"}, indent=2)
    account_id = _ensure_act(account_id)
    params: dict = {
        "fields": "id,name,username,category,fan_count,link,verification_status,picture",
        "limit": 50,
    }
    if search_term:
        params["q"] = search_term
    results = []

    # Try me/accounts
    user_pages = await _meta_get("me/accounts", params)
    pages = user_pages.get("data", [])
    if search_term:
        pages = [p for p in pages if search_term.lower() in p.get("name", "").lower()]
    results.extend(pages)

    # Try ad account client_pages
    client_data = await _meta_get(f"{account_id}/client_pages", params)
    for p in client_data.get("data", []):
        if not any(r["id"] == p["id"] for r in results):
            if not search_term or search_term.lower() in p.get("name", "").lower():
                results.append(p)

    return json.dumps({"data": results, "total": len(results)}, indent=2)


@mcp.tool()
async def get_account_pages(account_id: str) -> str:
    """Get Facebook Pages associated with a Meta Ads account.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX) or "me"
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"}, indent=2)

    fields = "id,name,username,category,fan_count,link,verification_status,picture"

    if account_id == "me":
        data = await _meta_get("me/accounts", {"fields": fields})
        return json.dumps(data, indent=2)

    account_id = _ensure_act(account_id)
    all_pages: dict = {}

    # me/accounts
    user_pages = await _meta_get("me/accounts", {"fields": fields, "limit": 200})
    for p in user_pages.get("data", []):
        all_pages[p["id"]] = p

    # owned_pages
    raw = account_id.replace("act_", "")
    owned = await _meta_get(f"{raw}/owned_pages", {"fields": fields, "limit": 200})
    for p in owned.get("data", []):
        all_pages[p["id"]] = p

    # client_pages
    client = await _meta_get(f"{account_id}/client_pages", {"fields": fields, "limit": 200})
    for p in client.get("data", []):
        all_pages[p["id"]] = p

    return json.dumps({"data": list(all_pages.values()), "total": len(all_pages)}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Audiences & Pixels ───────────────────────────────────────────────────────
# (Lead Forms)
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_lead_forms(
    page_id: str,
    limit: int = 25,
    status: str = "",
) -> str:
    """List all lead generation forms for a Facebook Page.

    Use this to find form IDs before pulling submissions via get_form_leads.
    Requires: leads_retrieval + pages_read_engagement permissions.

    Args:
        page_id: Facebook Page ID (use get_account_pages to find this)
        limit: Max forms to return (default: 25)
        status: Filter by status — ACTIVE, ARCHIVED, or '' for all (default: '')
    """
    params: dict = {
        "fields": "id,name,status,created_time,questions,privacy_policy_url,leads_count",
        "limit": limit,
    }
    data = await _meta_get(f"{page_id}/leadgen_forms", params)

    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", "")
        if "leads_retrieval" in msg.lower() or code in (200, 10, 190):
            return json.dumps({
                "error": msg,
                "permissions_note": "This endpoint requires 'leads_retrieval' and 'pages_read_engagement' permissions.",
            }, indent=2)
        return json.dumps(data, indent=2)

    forms = data.get("data", [])
    if status:
        forms = [f for f in forms if f.get("status", "").upper() == status.upper()]
    if not forms:
        return json.dumps({"message": f"No lead forms found for page {page_id}" + (f" with status '{status}'" if status else ""), "total": 0}, indent=2)

    lines = [f"Lead Forms for Page {page_id}", f"Found {len(forms)} form(s)" + (f" matching status '{status.upper()}'" if status else ""), "=" * 50]
    for i, form in enumerate(forms, 1):
        q_summary = ", ".join((q.get("key") or q.get("type", "?")) for q in form.get("questions", [])[:5])
        if len(form.get("questions", [])) > 5:
            q_summary += f", +{len(form['questions']) - 5} more"
        lines.append(
            f"{i}. {form.get('name', '(unnamed)')}\n"
            f"   Status: {form.get('status', '—')}  |  Leads: {form.get('leads_count', 0):,}  |  Created: {(form.get('created_time') or '')[:10]}\n"
            f"   Fields: {q_summary or '(none)'}\n"
            f"   Form ID: {form.get('id', '—')}"
        )
    return "\n".join(lines)


@mcp.tool()
async def get_form_leads(
    form_id: str,
    limit: int = 25,
    time_range: Optional[Union[str, Dict[str, str]]] = None,
) -> str:
    """Retrieve lead form submissions (contact details) for a Meta lead gen form.

    IMPORTANT: Lead data contains PII. Do not store or log individual contact
    details — use this tool only for in-session reporting.

    Args:
        form_id: Lead form ID (from get_lead_forms)
        limit: Max leads to return (default: 25, max: 500)
        time_range: Optional filter — preset string (last_7d, last_30d) or
                    {"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}
    """
    limit = min(int(limit), 500)
    params: dict = {
        "fields": "id,created_time,field_data,ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,is_organic",
        "limit": limit,
    }
    unix_start, unix_end = _resolve_time_range(time_range)
    if unix_start is not None and unix_end is not None:
        params["filtering"] = json.dumps([
            {"field": "time_created", "operator": "GREATER_THAN", "value": unix_start},
            {"field": "time_created", "operator": "LESS_THAN", "value": unix_end},
        ])

    data = await _meta_get(f"{form_id}/leads", params)

    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", "")
        if code == 100 and "does not exist" in msg.lower():
            return json.dumps({"error": f"Form not found: {form_id}. Verify with get_lead_forms(page_id=...)."}, indent=2)
        if "leads_retrieval" in msg.lower() or code in (200, 10, 190):
            return json.dumps({"error": f"Permission error: get_form_leads requires 'leads_retrieval' permission. Raw: {msg}"}, indent=2)
        return json.dumps(data, indent=2)

    leads = data.get("data", [])
    paging = data.get("paging", {})
    total_count = paging.get("total")

    if not leads:
        return json.dumps({"message": "No leads found" + (" for the specified time range" if time_range else "") + f" on form {form_id}.", "total": 0}, indent=2)

    total_str = str(total_count) if total_count is not None else "?"
    lines = [f"Showing {len(leads)} of {total_str} total leads for form {form_id}", "=" * 50]
    for i, lead in enumerate(leads, 1):
        created = (lead.get("created_time") or "")[:10]
        is_organic = lead.get("is_organic", False)
        fields_str = ", ".join(
            f"{fd.get('name')}: {', '.join(str(v) for v in fd.get('values', []))}"
            for fd in lead.get("field_data", [])
        )
        parts = [p for p in [lead.get("campaign_name", ""), lead.get("adset_name", ""), lead.get("ad_name", "")] if p]
        attribution = " → ".join(parts) if parts else ("(organic)" if is_organic else "(unknown)")
        lines.append(
            f"{i}. [{created}] {fields_str}\n"
            f"   Campaign: {attribution}{'  [organic]' if is_organic else ''}\n"
            f"   Lead ID: {lead.get('id', '—')}"
        )
    if paging.get("next"):
        lines.append("\n(More leads available — increase limit or paginate)")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Diagnostics & Troubleshooting ────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

# (No additional diagnostic tools at this time — the read tools above cover
#  the full Meta Ads data surface for query purposes.)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    from starlette.responses import Response

    app = mcp.http_app()

    api_key = os.getenv("MCP_API_KEY", "")

    if api_key:
        class _BearerAuth:
            def __init__(self, inner, key):
                self.inner = inner
                self.key = key

            async def __call__(self, scope, receive, send):
                if scope["type"] == "http":
                    headers = dict(scope.get("headers", []))
                    auth = headers.get(b"authorization", b"").decode()
                    qs = scope.get("query_string", b"").decode()
                    key_param = next((p[4:] for p in qs.split("&") if p.startswith("key=")), "")
                    if auth != f"Bearer {self.key}" and key_param != self.key:
                        await Response("Unauthorized", status_code=401)(scope, receive, send)
                        return
                await self.inner(scope, receive, send)

        app = _BearerAuth(app, api_key)

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
