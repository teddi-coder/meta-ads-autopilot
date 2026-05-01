"""Depth insight tools for Meta Ads — creative performance, fatigue, placement,
demographic, and video engagement breakdowns.

These complement the generic ``get_insights`` tool by returning pre-shaped,
actionable data at the dimensions where Meta performance actually varies.
"""

import json
from typing import Optional, List, Union, Dict
from .api import meta_api_tool, make_api_request, ensure_act_prefix
from .server import mcp_server


# ---------------------------------------------------------------------------
# Helper: resolve date preset / dict into params
# ---------------------------------------------------------------------------

def _time_params(time_range: Union[str, Dict[str, str]]) -> dict:
    """Return the correct params dict for a Meta API time_range."""
    if isinstance(time_range, dict):
        return {"time_range": json.dumps(time_range)}
    return {"date_preset": time_range}


def _safe_float(val, default=0.0) -> float:
    """Coerce a Meta API value to float (they return strings for money)."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _extract_action_value(actions: list, action_type: str) -> float:
    """Pull a specific action_type value from Meta's actions array."""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return _safe_float(a.get("value"))
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Tool 1: Ad Creative Performance
# ═══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
@meta_api_tool
async def ad_creative_performance(
    account_id: str,
    access_token: Optional[str] = None,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    sort_by: str = "spend",
    limit: int = 25,
) -> str:
    """Get ad-level creative performance ranked by spend, ROAS, CTR or conversions.

    Returns one row per ad with core metrics, thumbnail URL, and primary ad
    copy where available.  Use when asked "which creatives are working" or
    "what's our best performing ad".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        access_token: Meta API access token (uses cached token if omitted)
        time_range: Preset string (last_7d, last_30d …) or {"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}
        campaign_ids: Optional list of campaign IDs to filter to
        sort_by: One of spend, roas, ctr, conversions (default spend)
        limit: Max ads to return (default 25)
    """
    account_id = ensure_act_prefix(account_id)
    endpoint = f"{account_id}/insights"

    fields = (
        "ad_id,ad_name,campaign_name,adset_name,"
        "spend,impressions,clicks,ctr,cpc,"
        "actions,action_values,purchase_roas"
    )

    params = {
        "fields": fields,
        "level": "ad",
        "limit": limit,
        **_time_params(time_range),
    }

    if campaign_ids:
        params["filtering"] = json.dumps([
            {"field": "campaign.id", "operator": "IN", "value": campaign_ids}
        ])

    data = await make_api_request(endpoint, access_token, params)
    rows = data.get("data", [])

    # Enrich each row
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

    # Sort
    sort_key = {
        "spend": lambda r: r["spend"],
        "roas": lambda r: r["roas"],
        "ctr": lambda r: r["ctr"],
        "conversions": lambda r: r["conversions"],
    }.get(sort_by, lambda r: r["spend"])
    results.sort(key=sort_key, reverse=True)

    # Now fetch creative thumbnails for the top ads
    ad_ids = [r["ad_id"] for r in results[:limit]]
    if ad_ids:
        creative_endpoint = f"{account_id}/ads"
        creative_params = {
            "fields": "id,creative{id,thumbnail_url,body,title}",
            "filtering": json.dumps([
                {"field": "id", "operator": "IN", "value": ad_ids}
            ]),
            "limit": limit,
        }
        creative_data = await make_api_request(creative_endpoint, access_token, creative_params)
        creative_map = {}
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


# ═══════════════════════════════════════════════════════════════════════════
# Tool 2: Ad Creative Fatigue
# ═══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
@meta_api_tool
async def ad_creative_fatigue(
    account_id: str,
    access_token: Optional[str] = None,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    ad_ids: Optional[List[str]] = None,
    limit: int = 10,
) -> str:
    """Detect creative fatigue by analysing daily frequency and CTR trends per ad.

    Returns a fatigue_signal per ad: "fatiguing" (frequency up + CTR down over
    last 7 days), "healthy", or "insufficient_data".  Use when asked "is the
    audience fatiguing" or "do we need fresh creative".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        access_token: Meta API access token (uses cached token if omitted)
        time_range: Preset string or date dict (default last_30d)
        ad_ids: Optional list of specific ad IDs to check (default: top 10 by spend)
        limit: Max ads to return if ad_ids not specified (default 10)
    """
    account_id = ensure_act_prefix(account_id)
    endpoint = f"{account_id}/insights"

    fields = "ad_id,ad_name,spend,impressions,clicks,ctr,frequency,actions"

    params = {
        "fields": fields,
        "level": "ad",
        "time_increment": 1,
        "limit": 500,
        **_time_params(time_range),
    }

    if ad_ids:
        params["filtering"] = json.dumps([
            {"field": "ad.id", "operator": "IN", "value": ad_ids}
        ])

    data = await make_api_request(endpoint, access_token, params)
    rows = data.get("data", [])

    # Group by ad_id
    by_ad: dict = {}
    for row in rows:
        aid = row.get("ad_id")
        if aid not in by_ad:
            by_ad[aid] = {
                "ad_id": aid,
                "ad_name": row.get("ad_name", ""),
                "total_spend": 0.0,
                "daily_series": [],
            }
        spend = _safe_float(row.get("spend"))
        by_ad[aid]["total_spend"] += spend
        conversions = _extract_action_value(row.get("actions", []), "purchase")
        if conversions == 0:
            conversions = _extract_action_value(row.get("actions", []), "lead")
        by_ad[aid]["daily_series"].append({
            "date": row.get("date_start", ""),
            "frequency": _safe_float(row.get("frequency")),
            "ctr": _safe_float(row.get("ctr")),
            "cpc": _safe_float(row.get("spend")) / max(_safe_float(row.get("clicks")), 1),
            "conversions": conversions,
        })

    # If no ad_ids filter, take top N by spend
    ads_list = sorted(by_ad.values(), key=lambda a: a["total_spend"], reverse=True)
    if not ad_ids:
        ads_list = ads_list[:limit]

    # Compute fatigue signal
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

        freq_rising = avg_freq_late > avg_freq_early * 1.1  # 10%+ increase
        ctr_falling = avg_ctr_late < avg_ctr_early * 0.9   # 10%+ decrease

        if freq_rising and ctr_falling:
            ad["fatigue_signal"] = "fatiguing"
        else:
            ad["fatigue_signal"] = "healthy"

    return json.dumps({"data": ads_list}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# Tool 3: Performance by Placement
# ═══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
@meta_api_tool
async def ad_performance_by_placement(
    account_id: str,
    access_token: Optional[str] = None,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    level: str = "campaign",
) -> str:
    """Get performance broken down by placement (Feed vs Stories vs Reels etc).

    Returns one row per (entity, publisher_platform, platform_position,
    impression_device) combination.  Use when asked "where is spend going" or
    "which placements are working".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        access_token: Meta API access token (uses cached token if omitted)
        time_range: Preset string or date dict (default last_30d)
        campaign_ids: Optional campaign ID filter
        level: Aggregation level — campaign, adset, or ad (default campaign)
    """
    account_id = ensure_act_prefix(account_id)
    endpoint = f"{account_id}/insights"

    fields = (
        "campaign_name,adset_name,ad_name,"
        "spend,impressions,clicks,ctr,cpc,"
        "actions,action_values,purchase_roas"
    )

    params = {
        "fields": fields,
        "level": level,
        "breakdowns": "publisher_platform,platform_position,impression_device",
        "limit": 200,
        **_time_params(time_range),
    }

    if campaign_ids:
        params["filtering"] = json.dumps([
            {"field": "campaign.id", "operator": "IN", "value": campaign_ids}
        ])

    data = await make_api_request(endpoint, access_token, params)
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


# ═══════════════════════════════════════════════════════════════════════════
# Tool 4: Performance by Demographic
# ═══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
@meta_api_tool
async def ad_performance_by_demographic(
    account_id: str,
    access_token: Optional[str] = None,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    dimension: str = "age_gender",
) -> str:
    """Get performance broken down by demographic dimension (age+gender, country, region, or DMA).

    Returns one row per (campaign, dimension_value) with core metrics.  Use when
    asked "which demographics convert best" or "where are our customers".

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        access_token: Meta API access token (uses cached token if omitted)
        time_range: Preset string or date dict (default last_30d)
        campaign_ids: Optional campaign ID filter
        dimension: One of age_gender, country, region, dma (default age_gender)
    """
    account_id = ensure_act_prefix(account_id)
    endpoint = f"{account_id}/insights"

    breakdown_map = {
        "age_gender": "age,gender",
        "country": "country",
        "region": "region",
        "dma": "dma",
    }
    breakdowns = breakdown_map.get(dimension, "age,gender")

    fields = (
        "campaign_name,adset_name,"
        "spend,impressions,clicks,ctr,cpc,"
        "actions,action_values,purchase_roas"
    )

    params = {
        "fields": fields,
        "level": "campaign",
        "breakdowns": breakdowns,
        "limit": 200,
        **_time_params(time_range),
    }

    if campaign_ids:
        params["filtering"] = json.dumps([
            {"field": "campaign.id", "operator": "IN", "value": campaign_ids}
        ])

    data = await make_api_request(endpoint, access_token, params)
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

        entry = {
            "campaign_name": row.get("campaign_name", ""),
            "spend": spend,
            "impressions": int(impressions),
            "clicks": int(clicks),
            "ctr": round(_safe_float(row.get("ctr")), 4),
            "conversions": conversions,
            "cpa": round(cpa, 2),
            "roas": round(roas, 2),
        }

        # Add dimension-specific fields
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


# ═══════════════════════════════════════════════════════════════════════════
# Tool 5: Video Ad Performance
# ═══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
@meta_api_tool
async def video_ad_performance(
    account_id: str,
    access_token: Optional[str] = None,
    time_range: Union[str, Dict[str, str]] = "last_30d",
    campaign_ids: Optional[List[str]] = None,
    sort_by: str = "spend",
    limit: int = 25,
) -> str:
    """Get video-specific engagement metrics: hook rate, hold rate, thruplay rate.

    Returns one row per video ad with computed engagement rates.  Use when asked
    "how are our videos performing" or "what's the hook rate".

    Computed metrics:
    - hook_rate: 3-second plays / impressions (0.0-1.0)
    - hold_rate: 75% completions / 3-second plays (0.0-1.0)
    - thruplay_rate: thruplays / impressions (0.0-1.0)
    - completion_rate: 100% completions / video plays (0.0-1.0)

    Args:
        account_id: Meta ad account ID (with or without act_ prefix)
        access_token: Meta API access token (uses cached token if omitted)
        time_range: Preset string or date dict (default last_30d)
        campaign_ids: Optional campaign ID filter
        sort_by: One of spend, thruplay_rate, hook_rate, hold_rate (default spend)
        limit: Max ads to return (default 25)
    """
    account_id = ensure_act_prefix(account_id)
    endpoint = f"{account_id}/insights"

    fields = (
        "ad_id,ad_name,campaign_name,adset_name,"
        "spend,impressions,"
        "video_play_actions,"
        "video_p25_watched_actions,"
        "video_p50_watched_actions,"
        "video_p75_watched_actions,"
        "video_p100_watched_actions,"
        "video_thruplay_watched_actions,"
        "video_avg_time_watched_actions"
    )

    params = {
        "fields": fields,
        "level": "ad",
        "limit": 200,
        **_time_params(time_range),
    }

    if campaign_ids:
        params["filtering"] = json.dumps([
            {"field": "campaign.id", "operator": "IN", "value": campaign_ids}
        ])

    data = await make_api_request(endpoint, access_token, params)
    rows = data.get("data", [])

    results = []
    for row in rows:
        # Extract video action values
        def _video_val(actions_list):
            if not actions_list:
                return 0.0
            for a in actions_list:
                if a.get("action_type") == "video_view":
                    return _safe_float(a.get("value"))
            # Fallback: first entry
            return _safe_float(actions_list[0].get("value")) if actions_list else 0.0

        video_plays = _video_val(row.get("video_play_actions"))
        if video_plays == 0:
            continue  # Not a video ad

        impressions = _safe_float(row.get("impressions"))
        p25 = _video_val(row.get("video_p25_watched_actions"))
        p50 = _video_val(row.get("video_p50_watched_actions"))
        p75 = _video_val(row.get("video_p75_watched_actions"))
        p100 = _video_val(row.get("video_p100_watched_actions"))
        thruplays = _video_val(row.get("video_thruplay_watched_actions"))

        avg_watch_raw = row.get("video_avg_time_watched_actions", [])
        avg_watch_secs = _safe_float(avg_watch_raw[0].get("value")) if avg_watch_raw else 0.0

        # Compute rates (as 0.0-1.0 decimals)
        hook_rate = video_plays / impressions if impressions > 0 else 0
        hold_rate = p75 / video_plays if video_plays > 0 else 0
        thruplay_rate = thruplays / impressions if impressions > 0 else 0
        completion_rate = p100 / video_plays if video_plays > 0 else 0

        results.append({
            "ad_id": row.get("ad_id"),
            "ad_name": row.get("ad_name"),
            "campaign_name": row.get("campaign_name"),
            "adset_name": row.get("adset_name"),
            "spend": _safe_float(row.get("spend")),
            "impressions": int(impressions),
            "video_plays": int(video_plays),
            "hook_rate": round(hook_rate, 4),
            "hold_rate": round(hold_rate, 4),
            "thruplay_rate": round(thruplay_rate, 4),
            "completion_rate": round(completion_rate, 4),
            "avg_watch_time_secs": round(avg_watch_secs, 1),
            "p25_views": int(p25),
            "p50_views": int(p50),
            "p75_views": int(p75),
            "p100_views": int(p100),
            "thruplays": int(thruplays),
        })

    # Sort
    sort_key = {
        "spend": lambda r: r["spend"],
        "thruplay_rate": lambda r: r["thruplay_rate"],
        "hook_rate": lambda r: r["hook_rate"],
        "hold_rate": lambda r: r["hold_rate"],
    }.get(sort_by, lambda r: r["spend"])
    results.sort(key=sort_key, reverse=True)

    return json.dumps({"data": results[:limit], "total_video_ads": len(results)}, indent=2)
