"""hedgehog_meta_writer.py — Meta Ads write/mutation MCP server.

All read-only tools live in hedgehog_meta.py.
All Meta API calls go through _meta_get() / _meta_post() — no direct requests calls.

Required env vars:
  META_ACCESS_TOKEN   — long-lived system-user or page token
  META_APP_SECRET     — enables appsecret_proof (strongly recommended)
  GRAPH_VERSION       — Graph API version (default v24.0)
  MCP_API_KEY         — Bearer token for clients connecting to this server
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Union

import httpx
from fastmcp import FastMCP

# ── Server setup ─────────────────────────────────────────────────────────────

mcp = FastMCP("hedgehog-meta-writer")

GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v24.0")
META_GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"

logger = logging.getLogger(__name__)


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


# ── Placement helpers for create_ad_creative ─────────────────────────────────

# Maps user-friendly placement group names to Meta API positions.
_PLACEMENT_GROUP_TO_POSITIONS: Dict[str, Dict[str, List[str]]] = {
    "FEED": {
        "publisher_platforms": ["facebook", "instagram"],
        "facebook_positions": ["feed"],
        "instagram_positions": ["stream", "profile_feed"],
    },
    "STORY": {
        "publisher_platforms": ["facebook", "instagram"],
        "facebook_positions": ["story"],
        "instagram_positions": ["story"],
    },
    "MESSENGER": {
        "publisher_platforms": ["messenger"],
    },
    "INSTREAM_VIDEO": {
        "publisher_platforms": ["facebook"],
        "facebook_positions": ["instream_video"],
    },
    "SEARCH": {
        "publisher_platforms": ["facebook"],
        "facebook_positions": ["search"],
    },
    "SHOP": {
        "publisher_platforms": ["instagram"],
        "instagram_positions": ["shop"],
    },
    "AUDIENCE_NETWORK": {
        "publisher_platforms": ["audience_network"],
        "audience_network_positions": ["classic", "instream_video"],
    },
}

# All writable creative_features_spec keys for Meta Ads API v24+.
_ALL_ENHANCEMENT_KEYS: tuple = (
    "add_text_overlay",
    "creative_stickers",
    "description_automation",
    "image_animation",
    "image_background_gen",
    "image_templates",
    "image_touchups",
    "image_uncrop",
    "inline_comment",
    "media_type_automation",
    "music_generation",
    "pac_relaxation",
    "product_extensions",
    "profile_card",
    "reveal_details_over_time",
    "show_destination_blurbs",
    "show_summary",
    "site_extensions",
    "text_optimizations",
    "text_translation",
    "translate_voiceover",
    "video_auto_crop",
    "video_highlights",
)


def _translate_asset_customization_rules(
    rules: List[Dict[str, Any]],
    images_array: List[Dict[str, Any]],
) -> tuple:
    """Translate user-friendly placement_groups format to Meta API format for images."""
    if not rules or not any("placement_groups" in r for r in rules):
        return rules, images_array

    hash_to_label: Dict[str, str] = {}
    label_counter = 0
    translated_rules = []

    for rule in rules:
        if "placement_groups" not in rule:
            translated_rules.append(rule)
            continue

        placement_groups = rule.get("placement_groups", [])
        cspec_input = rule.get("customization_spec", {})

        publisher_platforms: set = set()
        facebook_positions: set = set()
        instagram_positions: set = set()
        audience_network_positions: set = set()

        for pg in placement_groups:
            mapping = _PLACEMENT_GROUP_TO_POSITIONS.get(pg, {})
            publisher_platforms.update(mapping.get("publisher_platforms", []))
            facebook_positions.update(mapping.get("facebook_positions", []))
            instagram_positions.update(mapping.get("instagram_positions", []))
            audience_network_positions.update(mapping.get("audience_network_positions", []))

        meta_cspec: Dict[str, Any] = {}
        if publisher_platforms:
            meta_cspec["publisher_platforms"] = sorted(publisher_platforms)
        if facebook_positions:
            meta_cspec["facebook_positions"] = sorted(facebook_positions)
        if instagram_positions:
            meta_cspec["instagram_positions"] = sorted(instagram_positions)
        if audience_network_positions:
            meta_cspec["audience_network_positions"] = sorted(audience_network_positions)

        for text_field in ("bodies", "titles", "descriptions", "link_urls", "call_to_action_types"):
            if text_field in cspec_input:
                meta_cspec[text_field] = cspec_input[text_field]

        translated_rule: Dict[str, Any] = {"customization_spec": meta_cspec}

        img_hashes = cspec_input.get("image_hashes", [])
        vid_ids = cspec_input.get("video_ids", [])
        if img_hashes:
            h = img_hashes[0]
            if h not in hash_to_label:
                hash_to_label[h] = f"PBOARD_IMG_{label_counter}"
                label_counter += 1
            translated_rule["image_label"] = {"name": hash_to_label[h]}
        elif vid_ids:
            v = vid_ids[0]
            if v not in hash_to_label:
                hash_to_label[v] = f"PBOARD_VID_{label_counter}"
                label_counter += 1
            translated_rule["video_label"] = {"name": hash_to_label[v]}

        translated_rules.append(translated_rule)

    updated_images = []
    for img in images_array:
        img_hash = img.get("hash", "")
        if img_hash in hash_to_label:
            updated = dict(img)
            updated["adlabels"] = [{"name": hash_to_label[img_hash]}]
            updated_images.append(updated)
        else:
            updated_images.append(img)

    return translated_rules, updated_images


def _translate_video_customization_rules(
    rules: List[Dict[str, Any]],
    videos_array: List[Dict[str, Any]],
) -> tuple:
    """Translate user-friendly placement_groups format to Meta API format for videos[]."""
    if not rules or not any("placement_groups" in r for r in rules):
        return rules, videos_array

    existing_vid_to_label: Dict[str, str] = {}
    for v in videos_array:
        vid_id = str(v.get("video_id", ""))
        adlabels = v.get("adlabels")
        if vid_id and vid_id not in existing_vid_to_label and isinstance(adlabels, list) and adlabels:
            first = adlabels[0]
            if isinstance(first, dict) and isinstance(first.get("name"), str):
                existing_vid_to_label[vid_id] = first["name"]

    vid_to_label: Dict[str, str] = {}
    label_counter = 0
    translated_rules: List[Dict[str, Any]] = []

    for rule in rules:
        if "placement_groups" not in rule:
            translated_rules.append(rule)
            continue

        placement_groups = rule.get("placement_groups", [])
        cspec_input = rule.get("customization_spec", {})

        publisher_platforms: set = set()
        facebook_positions: set = set()
        instagram_positions: set = set()
        audience_network_positions: set = set()

        for pg in placement_groups:
            mapping = _PLACEMENT_GROUP_TO_POSITIONS.get(pg, {})
            publisher_platforms.update(mapping.get("publisher_platforms", []))
            facebook_positions.update(mapping.get("facebook_positions", []))
            instagram_positions.update(mapping.get("instagram_positions", []))
            audience_network_positions.update(mapping.get("audience_network_positions", []))

        meta_cspec: Dict[str, Any] = {}
        if publisher_platforms:
            meta_cspec["publisher_platforms"] = sorted(publisher_platforms)
        if facebook_positions:
            meta_cspec["facebook_positions"] = sorted(facebook_positions)
        if instagram_positions:
            meta_cspec["instagram_positions"] = sorted(instagram_positions)
        if audience_network_positions:
            meta_cspec["audience_network_positions"] = sorted(audience_network_positions)

        for text_field in ("bodies", "titles", "descriptions", "link_urls", "call_to_action_types"):
            if text_field in cspec_input:
                meta_cspec[text_field] = cspec_input[text_field]

        translated_rule: Dict[str, Any] = {"customization_spec": meta_cspec}

        vid_ids = cspec_input.get("video_ids", [])
        raw_video_label = cspec_input.get("video_label")
        if vid_ids:
            v = str(vid_ids[0])
            if v not in vid_to_label:
                if v in existing_vid_to_label:
                    vid_to_label[v] = existing_vid_to_label[v]
                else:
                    vid_to_label[v] = f"PBOARD_VID_{label_counter}"
                    label_counter += 1
            translated_rule["video_label"] = {"name": vid_to_label[v]}
        elif isinstance(raw_video_label, str):
            translated_rule["video_label"] = {"name": raw_video_label}
        elif isinstance(raw_video_label, dict):
            translated_rule["video_label"] = raw_video_label

        translated_rules.append(translated_rule)

    updated_videos: List[Dict[str, Any]] = []
    for v in videos_array:
        vid_id = str(v.get("video_id", ""))
        if vid_id in vid_to_label and "adlabels" not in v:
            updated = dict(v)
            updated["adlabels"] = [{"name": vid_to_label[vid_id]}]
            updated_videos.append(updated)
        else:
            updated_videos.append(v)

    return translated_rules, updated_videos


def _translate_video_customization_rules_for_existing_post(
    rules: List[Dict[str, Any]],
) -> tuple:
    """Translate placement_groups-format rules to Meta API format for object_story_id creatives."""
    if not rules or not any("placement_groups" in r for r in rules):
        return rules, []

    vid_to_label: Dict[str, str] = {}
    label_counter = 0
    translated_rules = []

    for rule in rules:
        if "placement_groups" not in rule:
            translated_rules.append(rule)
            continue

        placement_groups = rule.get("placement_groups", [])
        cspec_input = rule.get("customization_spec", {})

        publisher_platforms: set = set()
        facebook_positions: set = set()
        instagram_positions: set = set()
        audience_network_positions: set = set()

        for pg in placement_groups:
            mapping = _PLACEMENT_GROUP_TO_POSITIONS.get(pg, {})
            publisher_platforms.update(mapping.get("publisher_platforms", []))
            facebook_positions.update(mapping.get("facebook_positions", []))
            instagram_positions.update(mapping.get("instagram_positions", []))
            audience_network_positions.update(mapping.get("audience_network_positions", []))

        meta_cspec: Dict[str, Any] = {}
        if publisher_platforms:
            meta_cspec["publisher_platforms"] = sorted(publisher_platforms)
        if facebook_positions:
            meta_cspec["facebook_positions"] = sorted(facebook_positions)
        if instagram_positions:
            meta_cspec["instagram_positions"] = sorted(instagram_positions)
        if audience_network_positions:
            meta_cspec["audience_network_positions"] = sorted(audience_network_positions)

        for text_field in ("bodies", "titles", "descriptions", "link_urls", "call_to_action_types"):
            if text_field in cspec_input:
                meta_cspec[text_field] = cspec_input[text_field]

        translated_rule: Dict[str, Any] = {"customization_spec": meta_cspec}

        vid_ids = cspec_input.get("video_ids", [])
        if vid_ids:
            v = vid_ids[0]
            if v not in vid_to_label:
                vid_to_label[v] = f"PBOARD_VID_{label_counter}"
                label_counter += 1
            translated_rule["video_label"] = {"name": vid_to_label[v]}

        translated_rules.append(translated_rule)

    videos_array = [
        {"video_id": vid_id, "adlabels": [{"name": label}]}
        for vid_id, label in vid_to_label.items()
    ]

    return translated_rules, videos_array


async def _fetch_video_thumbnail(vid_id: str) -> Optional[str]:
    """Fetch a thumbnail URL for a Meta video. Returns None on any failure."""
    try:
        info = await _meta_get(vid_id, {"fields": "picture,thumbnails"})
        if isinstance(info, dict):
            thumbs = info.get("thumbnails", {}).get("data", [])
            if thumbs and thumbs[0].get("uri"):
                return thumbs[0]["uri"]
            return info.get("picture") or None
    except Exception as e:
        logger.warning(f"Failed to auto-fetch thumbnail for video {vid_id}: {e}")
    return None


async def _discover_pages_for_account(account_id: str) -> dict:
    """Discover pages for an account using multiple fallback approaches."""
    try:
        # Approach 1: Extract page IDs from tracking_specs in ads
        tracking_ads_data = await _meta_get(
            f"{account_id}/ads",
            {"fields": "tracking_specs", "limit": 100},
        )
        tracking_page_ids: set = set()
        if "data" in tracking_ads_data:
            for ad in tracking_ads_data.get("data", []):
                for spec in ad.get("tracking_specs", []) or []:
                    if isinstance(spec, dict) and "page" in spec:
                        for page_id in spec["page"]:
                            if isinstance(page_id, (str, int)) and str(page_id).isdigit():
                                tracking_page_ids.add(str(page_id))
        if tracking_page_ids:
            page_id = list(tracking_page_ids)[0]
            page_data = await _meta_get(page_id, {"fields": "id,name"})
            if "id" in page_data:
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_name": page_data.get("name", "Unknown"),
                    "source": "tracking_specs",
                }

        # Approach 2: client_pages edge
        client_pages_data = await _meta_get(
            f"{account_id}/client_pages",
            {"fields": "id,name"},
        )
        if "data" in client_pages_data and client_pages_data["data"]:
            page = client_pages_data["data"][0]
            return {"success": True, "page_id": str(page["id"]), "page_name": page.get("name", ""), "source": "client_pages"}

        # Approach 3: assigned_pages edge
        pages_data = await _meta_get(
            f"{account_id}/assigned_pages",
            {"fields": "id,name", "limit": 1},
        )
        if "data" in pages_data and pages_data["data"]:
            page = pages_data["data"][0]
            return {"success": True, "page_id": str(page["id"]), "page_name": page.get("name", ""), "source": "assigned_pages"}

        return {"success": False, "message": "No suitable pages found. Provide page_id manually."}

    except Exception as e:
        return {"success": False, "message": f"Error during page discovery: {e}"}


async def _download_image(url: str) -> bytes:
    """Download image bytes from a public URL."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.content


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Campaign Tools ───────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def create_campaign(
    account_id: str,
    name: str,
    objective: str,
    status: str = "PAUSED",
    special_ad_categories: Optional[List[str]] = None,
    daily_budget: Optional[int] = None,
    lifetime_budget: Optional[int] = None,
    buying_type: Optional[str] = None,
    bid_strategy: str = "LOWEST_COST_WITHOUT_CAP",
    bid_cap: Optional[int] = None,
    spend_cap: Optional[int] = None,
    campaign_budget_optimization: Optional[bool] = None,
    ab_test_control_setups: Optional[List[Dict[str, Any]]] = None,
    use_adset_level_budgets: bool = False,
) -> str:
    """Create a new campaign in a Meta Ads account.

    Note: Campaigns do not support start_time — set start_time on the ad set instead.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        name: Campaign name
        objective: Campaign objective (ODAX). Must be one of:
                   OUTCOME_AWARENESS, OUTCOME_TRAFFIC, OUTCOME_ENGAGEMENT,
                   OUTCOME_LEADS, OUTCOME_SALES, OUTCOME_APP_PROMOTION.
        status: Initial campaign status (default: PAUSED)
        special_ad_categories: List of special ad categories if applicable
        daily_budget: Daily budget in account currency (in cents, only if use_adset_level_budgets=False)
        lifetime_budget: Lifetime budget in account currency (in cents, only if use_adset_level_budgets=False)
        buying_type: Buying type (e.g., 'AUCTION')
        bid_strategy: Bid strategy (default: LOWEST_COST_WITHOUT_CAP)
        bid_cap: Bid cap in account currency (in cents)
        spend_cap: Spending limit in account currency (in cents)
        campaign_budget_optimization: Enable campaign budget optimization
        ab_test_control_setups: A/B test settings
        use_adset_level_budgets: If True, budgets are set at the ad set level (default: False)
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"}, indent=2)
    if not name:
        return json.dumps({"error": "No campaign name provided"}, indent=2)
    if not objective:
        return json.dumps({"error": "No campaign objective provided"}, indent=2)

    account_id = _ensure_act(account_id)

    _user_provided_categories = special_ad_categories is not None
    if special_ad_categories is None:
        special_ad_categories = []

    compliance_warning = None
    if objective == "OUTCOME_LEADS" and not special_ad_categories and not _user_provided_categories:
        compliance_warning = (
            "Warning: Campaign objective is OUTCOME_LEADS but no special_ad_categories were specified. "
            "If this campaign is for a regulated industry (insurance, housing, employment, credit), "
            "you must set special_ad_categories to comply with Meta advertising policies."
        )

    if not daily_budget and not lifetime_budget and not use_adset_level_budgets:
        daily_budget = "1000"

    endpoint = f"{account_id}/campaigns"
    params: Dict[str, Any] = {
        "name": name,
        "objective": objective,
        "status": status,
        "special_ad_categories": special_ad_categories,
    }

    if not use_adset_level_budgets:
        if daily_budget is not None:
            params["daily_budget"] = str(daily_budget)
        if lifetime_budget is not None:
            params["lifetime_budget"] = str(lifetime_budget)
        if campaign_budget_optimization is not None:
            params["campaign_budget_optimization"] = "true" if campaign_budget_optimization else "false"
    else:
        params["is_adset_budget_sharing_enabled"] = "false"

    if buying_type:
        params["buying_type"] = buying_type
    if bid_strategy:
        params["bid_strategy"] = bid_strategy
    if bid_cap is not None:
        params["bid_cap"] = str(bid_cap)
    if spend_cap is not None:
        params["spend_cap"] = str(spend_cap)
    if ab_test_control_setups:
        params["ab_test_control_setups"] = ab_test_control_setups

    try:
        data = await _meta_post(endpoint, params)
        if use_adset_level_budgets:
            data["budget_strategy"] = "ad_set_level"
            data["note"] = "Campaign created with ad set level budgets. Set budgets when creating ad sets."
        if compliance_warning:
            data["compliance_warning"] = compliance_warning
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({"error": "Failed to create campaign", "details": str(e)}, indent=2)


@mcp.tool()
async def update_campaign(
    campaign_id: str,
    name: Optional[str] = None,
    status: Optional[str] = None,
    special_ad_categories: Optional[List[str]] = None,
    daily_budget: Optional[int] = None,
    lifetime_budget: Optional[int] = None,
    bid_strategy: Optional[str] = None,
    bid_cap: Optional[int] = None,
    spend_cap: Optional[int] = None,
    campaign_budget_optimization: Optional[bool] = None,
    objective: Optional[str] = None,
    use_adset_level_budgets: Optional[bool] = None,
) -> str:
    """Update an existing campaign in a Meta Ads account.

    Args:
        campaign_id: Meta Ads campaign ID
        name: New campaign name
        status: New campaign status (e.g., 'ACTIVE', 'PAUSED')
        special_ad_categories: List of special ad categories
        daily_budget: New daily budget in cents. Set to empty string to remove.
        lifetime_budget: New lifetime budget in cents. Set to empty string to remove.
        bid_strategy: New bid strategy
        bid_cap: New bid cap in cents
        spend_cap: New spending limit in cents
        campaign_budget_optimization: Enable/disable campaign budget optimization
        objective: New campaign objective (may not always be updatable)
        use_adset_level_budgets: If True, removes campaign-level budgets
    """
    if not campaign_id:
        return json.dumps({"error": "No campaign ID provided"}, indent=2)

    params: Dict[str, Any] = {}

    if name is not None:
        params["name"] = name
    if status is not None:
        params["status"] = status
    if special_ad_categories is not None:
        params["special_ad_categories"] = special_ad_categories

    if use_adset_level_budgets is not None:
        if use_adset_level_budgets:
            params["daily_budget"] = ""
            params["lifetime_budget"] = ""
            if campaign_budget_optimization is not None:
                params["campaign_budget_optimization"] = "false"
        else:
            if daily_budget is not None:
                params["daily_budget"] = str(daily_budget) if daily_budget != "" else ""
            if lifetime_budget is not None:
                params["lifetime_budget"] = str(lifetime_budget) if lifetime_budget != "" else ""
            if campaign_budget_optimization is not None:
                params["campaign_budget_optimization"] = "true" if campaign_budget_optimization else "false"
    else:
        if daily_budget is not None:
            params["daily_budget"] = str(daily_budget) if daily_budget != "" else ""
        if lifetime_budget is not None:
            params["lifetime_budget"] = str(lifetime_budget) if lifetime_budget != "" else ""
        if campaign_budget_optimization is not None:
            params["campaign_budget_optimization"] = "true" if campaign_budget_optimization else "false"

    if bid_strategy is not None:
        params["bid_strategy"] = bid_strategy
    if bid_cap is not None:
        params["bid_cap"] = str(bid_cap)
    if spend_cap is not None:
        params["spend_cap"] = str(spend_cap)
    if objective is not None:
        params["objective"] = objective

    if not params:
        return json.dumps({"error": "No update parameters provided"}, indent=2)

    try:
        data = await _meta_post(campaign_id, params)
        if use_adset_level_budgets is not None and use_adset_level_budgets:
            data["budget_strategy"] = "ad_set_level"
            data["note"] = "Campaign updated to use ad set level budgets."
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to update campaign {campaign_id}", "details": str(e)}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Ad Set Tools ─────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def create_adset(
    account_id: str,
    campaign_id: str,
    name: str,
    optimization_goal: str,
    billing_event: str,
    status: str = "PAUSED",
    daily_budget: Optional[int] = None,
    lifetime_budget: Optional[int] = None,
    targeting: Optional[Dict[str, Any]] = None,
    bid_amount: Optional[int] = None,
    bid_strategy: Optional[str] = None,
    bid_constraints: Optional[Dict[str, Any]] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    dsa_beneficiary: Optional[str] = None,
    dsa_payor: Optional[str] = None,
    promoted_object: Optional[Dict[str, Any]] = None,
    destination_type: Optional[str] = None,
    is_dynamic_creative: Optional[bool] = None,
    frequency_control_specs: Optional[List[Dict[str, Any]]] = None,
    multi_advertiser_ads: Optional[int] = None,
) -> str:
    """Create a new ad set in a Meta Ads account.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        campaign_id: Meta Ads campaign ID this ad set belongs to
        name: Ad set name
        optimization_goal: Conversion optimization goal (e.g., 'LINK_CLICKS', 'REACH', 'CONVERSIONS', 'VALUE')
        billing_event: How you're charged (e.g., 'IMPRESSIONS', 'LINK_CLICKS')
        status: Initial ad set status (default: PAUSED)
        daily_budget: Daily budget in cents
        lifetime_budget: Lifetime budget in cents
        targeting: Targeting specs (age, location, interests, etc.)
        bid_amount: Bid amount in cents. Required for LOWEST_COST_WITH_BID_CAP, COST_CAP.
        bid_strategy: Bid strategy. Valid values:
                     LOWEST_COST_WITHOUT_CAP, LOWEST_COST_WITH_BID_CAP (requires bid_amount),
                     COST_CAP (requires bid_amount), LOWEST_COST_WITH_MIN_ROAS (requires bid_constraints).
        bid_constraints: Required for LOWEST_COST_WITH_MIN_ROAS. Use {"roas_average_floor": <value>}.
        start_time: Start time in ISO 8601 format
        end_time: End time in ISO 8601 format. Required with lifetime_budget.
        dsa_beneficiary: DSA beneficiary for European compliance
        dsa_payor: DSA payor for European compliance
        promoted_object: App config for APP_INSTALLS (requires application_id, object_store_url)
        destination_type: Where users go after click (e.g., 'WEBSITE', 'WHATSAPP', 'MESSENGER')
        is_dynamic_creative: Enable Dynamic Creative for this ad set
        frequency_control_specs: Frequency cap specs. MUST be set at creation (immutable after).
                                 Example: [{"event": "IMPRESSIONS", "interval_days": 7, "max_frequency": 1}]
        multi_advertiser_ads: Set to 0 to opt out of Multi-Advertiser Ads, 1 to opt in
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"}, indent=2)
    account_id = _ensure_act(account_id)
    if not campaign_id:
        return json.dumps({"error": "No campaign ID provided"}, indent=2)
    if not name:
        return json.dumps({"error": "No ad set name provided"}, indent=2)
    if not optimization_goal:
        return json.dumps({"error": "No optimization goal provided"}, indent=2)
    if not billing_event:
        return json.dumps({"error": "No billing event provided"}, indent=2)

    if optimization_goal == "APP_INSTALLS":
        if not promoted_object:
            return json.dumps({"error": "promoted_object is required for APP_INSTALLS optimization goal", "required_fields": ["application_id", "object_store_url"]}, indent=2)
        if not isinstance(promoted_object, dict):
            return json.dumps({"error": "promoted_object must be a dictionary"}, indent=2)
        if "application_id" not in promoted_object:
            return json.dumps({"error": "promoted_object missing required field: application_id"}, indent=2)
        if "object_store_url" not in promoted_object:
            return json.dumps({"error": "promoted_object missing required field: object_store_url"}, indent=2)
        store_url = promoted_object["object_store_url"]
        if not any(p in store_url for p in ["apps.apple.com", "play.google.com", "itunes.apple.com"]):
            return json.dumps({"error": "Invalid object_store_url — must be App Store or Google Play URL", "provided_url": store_url}, indent=2)

    if not targeting:
        targeting = {
            "age_min": 18,
            "age_max": 65,
            "geo_locations": {"countries": ["US"]},
            "targeting_automation": {"advantage_audience": 1},
        }
    if "targeting_automation" not in targeting:
        targeting["targeting_automation"] = {"advantage_audience": 0}

    strategies_requiring_bid_amount = ["LOWEST_COST_WITH_BID_CAP", "COST_CAP", "TARGET_COST"]

    if bid_strategy:
        if bid_strategy == "LOWEST_COST":
            return json.dumps({"error": "'LOWEST_COST' is not valid. Use 'LOWEST_COST_WITHOUT_CAP'."}, indent=2)
        if bid_strategy in strategies_requiring_bid_amount and bid_amount is None:
            return json.dumps({"error": f"bid_amount is required for bid_strategy '{bid_strategy}'"}, indent=2)
        if bid_strategy == "LOWEST_COST_WITH_MIN_ROAS" and not bid_constraints:
            return json.dumps({"error": "bid_constraints is required for LOWEST_COST_WITH_MIN_ROAS. Use {'roas_average_floor': <value>}."}, indent=2)

    if bid_amount is None:
        try:
            campaign_data = await _meta_get(campaign_id, {"fields": "bid_strategy,name"})
            campaign_bid_strategy = campaign_data.get("bid_strategy")
            if campaign_bid_strategy and campaign_bid_strategy in strategies_requiring_bid_amount:
                return json.dumps({
                    "error": f"bid_amount is required because the parent campaign uses bid_strategy '{campaign_bid_strategy}'",
                    "details": f"Campaign '{campaign_data.get('name', campaign_id)}' requires bid_amount on all child ad sets.",
                }, indent=2)
        except Exception:
            pass

    params: Dict[str, Any] = {
        "name": name,
        "campaign_id": campaign_id,
        "status": status,
        "optimization_goal": optimization_goal,
        "billing_event": billing_event,
        "targeting": targeting,
    }

    if daily_budget is not None:
        params["daily_budget"] = str(daily_budget)
    if lifetime_budget is not None:
        params["lifetime_budget"] = str(lifetime_budget)
    if bid_amount is not None:
        params["bid_amount"] = str(bid_amount)
    if bid_strategy:
        params["bid_strategy"] = bid_strategy
    if bid_constraints:
        params["bid_constraints"] = bid_constraints
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time
    if dsa_beneficiary:
        params["dsa_beneficiary"] = dsa_beneficiary
    if dsa_payor:
        params["dsa_payor"] = dsa_payor
    if promoted_object:
        params["promoted_object"] = promoted_object
    if destination_type:
        params["destination_type"] = destination_type
    if is_dynamic_creative is not None:
        params["is_dynamic_creative"] = "true" if bool(is_dynamic_creative) else "false"
    if frequency_control_specs is not None:
        params["frequency_control_specs"] = frequency_control_specs
    if multi_advertiser_ads is not None:
        params["multi_advertiser_ads"] = str(multi_advertiser_ads)

    try:
        data = await _meta_post(f"{account_id}/adsets", params)
        return json.dumps(data, indent=2)
    except Exception as e:
        error_msg = str(e)
        if "dsa_beneficiary" in error_msg.lower() or "benefits from ads" in error_msg:
            return json.dumps({"error": "DSA beneficiary required for EU compliance.", "details": error_msg}, indent=2)
        return json.dumps({"error": "Failed to create ad set", "details": error_msg}, indent=2)


@mcp.tool()
async def update_adset(
    adset_id: str,
    name: Optional[str] = None,
    status: Optional[str] = None,
    targeting: Optional[Dict[str, Any]] = None,
    optimization_goal: Optional[str] = None,
    daily_budget: Optional[int] = None,
    lifetime_budget: Optional[int] = None,
    bid_strategy: Optional[str] = None,
    bid_amount: Optional[int] = None,
    bid_constraints: Optional[Dict[str, Any]] = None,
    frequency_control_specs: Optional[List[Dict[str, Any]]] = None,
    is_dynamic_creative: Optional[bool] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    dsa_beneficiary: Optional[str] = None,
    dsa_payor: Optional[str] = None,
    multi_advertiser_ads: Optional[int] = None,
) -> str:
    """Update an ad set with new settings including frequency caps and budgets.

    Args:
        adset_id: Meta Ads ad set ID
        name: New ad set name
        status: Update ad set status (ACTIVE, PAUSED, etc.)
        targeting: Complete targeting specifications (replaces existing targeting)
        optimization_goal: Conversion optimization goal
        daily_budget: Daily budget in cents
        lifetime_budget: Lifetime budget in cents
        bid_strategy: Bid strategy. Valid values: LOWEST_COST_WITHOUT_CAP,
                     LOWEST_COST_WITH_BID_CAP (requires bid_amount),
                     COST_CAP (requires bid_amount), LOWEST_COST_WITH_MIN_ROAS (requires bid_constraints)
        bid_amount: Bid amount in cents. Required for LOWEST_COST_WITH_BID_CAP, COST_CAP.
        bid_constraints: Required for LOWEST_COST_WITH_MIN_ROAS. Use {"roas_average_floor": <value>}.
        frequency_control_specs: Frequency cap specs
                                 (e.g. [{"event": "IMPRESSIONS", "interval_days": 7, "max_frequency": 3}])
        is_dynamic_creative: Enable/disable Dynamic Creative (WARNING: immutable after creation)
        start_time: Start time in ISO 8601 format
        end_time: End time in ISO 8601 format
        dsa_beneficiary: DSA beneficiary for European compliance
        dsa_payor: DSA payor for European compliance
        multi_advertiser_ads: 0 to opt out, 1 to opt in
    """
    if not adset_id:
        return json.dumps({"error": "No ad set ID provided"}, indent=2)

    if bid_strategy is not None:
        if bid_strategy == "LOWEST_COST":
            return json.dumps({"error": "'LOWEST_COST' is not valid. Use 'LOWEST_COST_WITHOUT_CAP'."}, indent=2)
        strategies_requiring_bid_amount = ["LOWEST_COST_WITH_BID_CAP", "COST_CAP", "TARGET_COST"]
        if bid_strategy in strategies_requiring_bid_amount and bid_amount is None:
            return json.dumps({"error": f"bid_amount is required for bid_strategy '{bid_strategy}'"}, indent=2)
        if bid_strategy == "LOWEST_COST_WITH_MIN_ROAS" and not bid_constraints:
            return json.dumps({"error": "bid_constraints is required for LOWEST_COST_WITH_MIN_ROAS."}, indent=2)

    params: Dict[str, Any] = {}
    if name is not None:
        params["name"] = name
    if status is not None:
        params["status"] = status
    if optimization_goal is not None:
        params["optimization_goal"] = optimization_goal
    if targeting is not None:
        params["targeting"] = targeting
    if daily_budget is not None:
        params["daily_budget"] = str(daily_budget)
    if lifetime_budget is not None:
        params["lifetime_budget"] = str(lifetime_budget)
    if bid_strategy is not None:
        params["bid_strategy"] = bid_strategy
    if bid_amount is not None:
        params["bid_amount"] = str(bid_amount)
    if bid_constraints is not None:
        params["bid_constraints"] = bid_constraints
    if frequency_control_specs is not None:
        params["frequency_control_specs"] = frequency_control_specs
    if is_dynamic_creative is not None:
        params["is_dynamic_creative"] = "true" if bool(is_dynamic_creative) else "false"
    if start_time is not None:
        params["start_time"] = start_time
    if end_time is not None:
        params["end_time"] = end_time
    if dsa_beneficiary is not None:
        params["dsa_beneficiary"] = dsa_beneficiary
    if dsa_payor is not None:
        params["dsa_payor"] = dsa_payor
    if multi_advertiser_ads is not None:
        params["multi_advertiser_ads"] = str(multi_advertiser_ads)

    if not params:
        return json.dumps({"error": "No update parameters provided"}, indent=2)

    try:
        data = await _meta_post(adset_id, params)
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to update ad set {adset_id}", "details": str(e)}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Ad Tools ─────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def create_ad(
    account_id: str,
    name: str,
    adset_id: str,
    creative_id: str,
    status: str = "PAUSED",
    bid_amount: Optional[int] = None,
    tracking_specs: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Create a new ad with an existing creative.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        name: Ad name
        adset_id: Ad set ID where this ad will be placed
        creative_id: ID of an existing creative to use
        status: Initial ad status (default: PAUSED)
        bid_amount: Optional bid amount in cents
        tracking_specs: Optional tracking specifications
                       (e.g., [{"action.type":"offsite_conversion","fb_pixel":["YOUR_PIXEL_ID"]}])

    Note: Dynamic Creative creatives require the parent ad set to have is_dynamic_creative=true.
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"}, indent=2)
    account_id = _ensure_act(account_id)
    if not name:
        return json.dumps({"error": "No ad name provided"}, indent=2)
    if not adset_id:
        return json.dumps({"error": "No ad set ID provided"}, indent=2)
    if not creative_id:
        return json.dumps({"error": "No creative ID provided"}, indent=2)

    params: Dict[str, Any] = {
        "name": name,
        "adset_id": adset_id,
        "creative": {"creative_id": creative_id},
        "status": status,
    }
    if bid_amount is not None:
        params["bid_amount"] = str(bid_amount)
    if tracking_specs is not None:
        params["tracking_specs"] = tracking_specs

    try:
        data = await _meta_post(f"{account_id}/ads", params)
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({"error": "Failed to create ad", "details": str(e)}, indent=2)


@mcp.tool()
async def update_ad(
    ad_id: str,
    status: Optional[str] = None,
    bid_amount: Optional[int] = None,
    tracking_specs: Optional[List[Dict[str, Any]]] = None,
    creative_id: Optional[Union[str, int]] = None,
) -> str:
    """Update an ad with new settings.

    Args:
        ad_id: Meta Ads ad ID
        status: Update ad status (ACTIVE, PAUSED, etc.)
        bid_amount: Bid amount in cents
        tracking_specs: Optional tracking specifications
        creative_id: ID of the creative to associate with this ad
    """
    if not ad_id:
        return json.dumps({"error": "Ad ID is required"}, indent=2)

    if creative_id is not None:
        creative_id = str(creative_id)

    params: Dict[str, Any] = {}
    if status:
        params["status"] = status
    if bid_amount is not None:
        params["bid_amount"] = str(bid_amount)
    if tracking_specs is not None:
        params["tracking_specs"] = tracking_specs
    if creative_id is not None:
        params["creative"] = {"creative_id": creative_id}

    if not params:
        return json.dumps({"error": "No update parameters provided (status, bid_amount, tracking_specs, or creative_id)"}, indent=2)

    try:
        data = await _meta_post(ad_id, params)
        if creative_id is not None and "error" in data:
            error_obj = data.get("error", {})
            error_subcode = None
            if isinstance(error_obj, dict):
                error_details = error_obj.get("details", {})
                if isinstance(error_details, dict):
                    inner_error = error_details.get("error", {})
                    error_subcode = inner_error.get("error_subcode") if isinstance(inner_error, dict) else None
                else:
                    error_subcode = error_obj.get("error_subcode")
            if error_subcode == 3858355:
                return json.dumps({
                    "error": "Cannot swap creative on this ad due to FLEX image mismatch",
                    "error_subcode": 3858355,
                    "workaround": "Create a new ad with the new creative and pause the old ad.",
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                }, indent=2)
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to update ad: {e}"}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Creative & Asset Tools ───────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def upload_ad_image(
    account_id: str,
    file: Optional[str] = None,
    image_url: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Upload an image to use in Meta Ads creatives.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        file: Data URL or raw base64 string of the image (e.g., "data:image/png;base64,iVBORw0KG...")
        image_url: Direct URL to an image to fetch and upload
        name: Optional name for the image (default: filename)

    Returns:
        JSON response with image details including hash for creative creation
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"}, indent=2)
    if not file and not image_url:
        return json.dumps({"error": "Provide either 'file' (data URL or base64) or 'image_url'"}, indent=2)

    account_id = _ensure_act(account_id)

    try:
        encoded_image: str = ""
        inferred_name: str = name or ""

        if file:
            data_url_prefix = "data:"
            base64_marker = "base64,"
            if file.startswith(data_url_prefix) and base64_marker in file:
                header, base64_payload = file.split(base64_marker, 1)
                encoded_image = base64_payload.strip()
                if not inferred_name:
                    mime_type = header[len(data_url_prefix):].split(";")[0].strip()
                    extension_map = {
                        "image/png": ".png", "image/jpeg": ".jpg", "image/jpg": ".jpg",
                        "image/webp": ".webp", "image/gif": ".gif", "image/bmp": ".bmp",
                    }
                    ext = extension_map.get(mime_type, ".png")
                    inferred_name = f"upload{ext}"
            else:
                encoded_image = file.strip()
                if not inferred_name:
                    inferred_name = "upload.png"
        else:
            try:
                image_bytes = await _download_image(image_url)
            except Exception as download_error:
                return json.dumps({
                    "error": "Could not download the image from the provided URL.",
                    "image_url": image_url,
                    "details": str(download_error),
                    "suggestion": "Ensure the URL is publicly accessible (no login or IP restrictions).",
                }, indent=2)

            if not image_bytes:
                return json.dumps({
                    "error": "No image data returned from the URL.",
                    "image_url": image_url,
                }, indent=2)

            encoded_image = base64.b64encode(image_bytes).decode("utf-8")
            if not inferred_name:
                try:
                    path_no_query = image_url.split("?")[0]
                    filename_from_url = os.path.basename(path_no_query)
                    inferred_name = filename_from_url if filename_from_url else "upload.jpg"
                except Exception:
                    inferred_name = "upload.jpg"

        final_name = name or inferred_name or "upload.png"

        params = {"bytes": encoded_image, "name": final_name}
        data = await _meta_post(f"{account_id}/adimages", params)

        if isinstance(data, dict) and "images" in data and isinstance(data["images"], dict) and data["images"]:
            images_dict = data["images"]
            images_list = []
            for hash_key, info in images_dict.items():
                normalized = {
                    "hash": (info.get("hash") or hash_key),
                    "url": info.get("url"),
                    "width": info.get("width"),
                    "height": info.get("height"),
                    "name": info.get("name"),
                }
                normalized = {k: v for k, v in normalized.items() if v is not None}
                images_list.append(normalized)
            images_list.sort(key=lambda i: i.get("hash", ""))
            primary_hash = images_list[0].get("hash") if images_list else None
            return json.dumps({
                "success": True,
                "account_id": account_id,
                "name": final_name,
                "image_hash": primary_hash,
                "images_count": len(images_list),
                "images": images_list,
            }, indent=2)

        if isinstance(data, dict) and "error" in data:
            return json.dumps({"error": "Failed to upload image", "details": data.get("error"), "account_id": account_id}, indent=2)

        return json.dumps({"success": True, "account_id": account_id, "name": final_name, "raw_response": data}, indent=2)

    except Exception as e:
        return json.dumps({"error": "Failed to upload image", "details": str(e)}, indent=2)


@mcp.tool()
async def create_ad_creative(
    account_id: str,
    image_hash: Optional[str] = None,
    name: Optional[str] = None,
    page_id: Optional[Union[str, int]] = None,
    link_url: Optional[str] = None,
    message: Optional[str] = None,
    messages: Optional[List[str]] = None,
    headline: Optional[str] = None,
    headlines: Optional[List[str]] = None,
    description: Optional[str] = None,
    descriptions: Optional[List[str]] = None,
    image_hashes: Optional[List[str]] = None,
    video_id: Optional[Union[str, int]] = None,
    thumbnail_url: Optional[str] = None,
    optimization_type: Optional[str] = None,
    dynamic_creative_spec: Optional[Dict[str, Any]] = None,
    call_to_action_type: Optional[str] = None,
    lead_gen_form_id: Optional[Union[str, int]] = None,
    instagram_actor_id: Optional[str] = None,
    ad_formats: Optional[List[str]] = None,
    asset_customization_rules: Optional[List[Dict[str, Any]]] = None,
    creative_features_spec: Optional[Dict[str, Any]] = None,
    phone_number: Optional[str] = None,
    url_tags: Optional[str] = None,
    caption: Optional[str] = None,
    image_crops: Optional[Dict[str, Any]] = None,
    object_story_id: Optional[str] = None,
    disable_all_enhancements: Optional[bool] = None,
    event_id: Optional[Union[str, int]] = None,
    reminder_data: Optional[Dict[str, Any]] = None,
    videos: Optional[List[Dict[str, Any]]] = None,
    images: Optional[List[Dict[str, Any]]] = None,
    facebook_branded_content: Optional[Dict[str, Any]] = None,
    instagram_branded_content: Optional[Dict[str, Any]] = None,
) -> str:
    """Create a new ad creative using an uploaded image hash, video ID, or an existing post.

    Supports five creative modes:
    - Existing post: Provide object_story_id (format: {page_id}_{post_id}) to promote an existing post.
    - Simple image/video: Single image_hash or video_id with object_story_spec.
    - Multi-variant copy: Use plural text params (messages[], headlines[], descriptions[]) to test variants.
    - Dynamic Creative: Multiple variants with dynamic_creative_spec (requires is_dynamic_creative on ad set).
    - FLEX/DOF (Advantage+): Set optimization_type="DEGREES_OF_FREEDOM" for Meta to auto-optimize.

    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        image_hash: Hash of a single uploaded image (cannot be used with image_hashes or video_id)
        name: Creative name
        page_id: Facebook Page ID (auto-discovered if not provided)
        link_url: Destination URL (required unless using lead_gen_form_id, object_story_id, or reminder_data)
        message: Single ad copy/text (cannot be used with messages)
        messages: List of primary text variants for multi-variant copy testing
        headline: Single headline (cannot be used with headlines)
        headlines: List of headline variants (max 5, max 40 chars each)
        description: Single description (cannot be used with descriptions)
        descriptions: List of description variants (max 5, max 125 chars each)
        image_hashes: List of image hashes for FLEX/multi-image creatives (up to 10)
        video_id: Meta video ID for video creatives (cannot be used with image_hash or image_hashes)
        thumbnail_url: Thumbnail image URL for video creatives
        optimization_type: "DEGREES_OF_FREEDOM" (FLEX/Advantage+) or "PLACEMENT"
        dynamic_creative_spec: Dynamic creative optimization settings
        call_to_action_type: Call to action button type (e.g., LEARN_MORE, SHOP_NOW, SIGN_UP,
                            BOOK_NOW, CALL_NOW, GET_QUOTE, CONTACT_US, SUBSCRIBE, APPLY_NOW)
        lead_gen_form_id: Lead generation form ID for lead gen campaigns
        instagram_actor_id: Instagram account ID for Instagram placements
        ad_formats: List of ad format strings (e.g., ["SINGLE_IMAGE"], ["SINGLE_VIDEO"])
        asset_customization_rules: Placement-specific asset overrides
        creative_features_spec: Advantage+ Creative feature opt-ins/opt-outs
        phone_number: Phone number for CALL_NOW ads (E.164 format, e.g., "+18005551234")
        url_tags: URL tracking parameters (e.g., "utm_source=facebook&utm_medium=cpc")
        caption: Display URL shown in the ad (e.g., "example.com")
        image_crops: Crop coordinates dict for different aspect ratios
        object_story_id: Existing post ID to promote (format: "{page_id}_{post_id}")
        disable_all_enhancements: Opt out of all Advantage+ Creative enhancements
        event_id: Facebook Event ID for EVENT_RESPONSES campaigns
        reminder_data: Inline reminder event data for Instagram Reminder Ads
        videos: List of video objects for placement asset customization
        images: List of image objects for placement asset customization
        facebook_branded_content: Facebook partnership ad settings {"sponsor_page_id": "<page_id>"}
        instagram_branded_content: Instagram partnership ad settings {"sponsor_id": "<ig_user_id>"}
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"}, indent=2)

    # Coerce numeric IDs to strings
    if video_id is not None:
        video_id = str(video_id)
    if instagram_actor_id is not None:
        instagram_actor_id = str(instagram_actor_id).strip('"').strip("'")
    if lead_gen_form_id is not None:
        lead_gen_form_id = str(lead_gen_form_id)
    if event_id is not None:
        event_id = str(event_id)

    # Defensive coercion for JSON string parameters
    for _attr, _default_type in [
        ("asset_customization_rules", list),
        ("creative_features_spec", dict),
        ("image_crops", dict),
        ("reminder_data", dict),
        ("videos", list),
        ("images", list),
        ("facebook_branded_content", dict),
        ("instagram_branded_content", dict),
    ]:
        val = locals().get(_attr) if _attr in ("asset_customization_rules", "creative_features_spec") else None
        pass

    # Manual coercion for each string param
    if isinstance(asset_customization_rules, str):
        try:
            _p = json.loads(asset_customization_rules)
            if isinstance(_p, list):
                asset_customization_rules = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(creative_features_spec, str):
        try:
            _p = json.loads(creative_features_spec)
            if isinstance(_p, dict):
                creative_features_spec = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(image_crops, str):
        try:
            _p = json.loads(image_crops)
            if isinstance(_p, dict):
                image_crops = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(reminder_data, str):
        try:
            _p = json.loads(reminder_data)
            if isinstance(_p, dict):
                reminder_data = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(videos, str):
        try:
            _p = json.loads(videos)
            if isinstance(_p, list):
                videos = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(images, str):
        try:
            _p = json.loads(images)
            if isinstance(_p, list):
                images = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(facebook_branded_content, str):
        try:
            _p = json.loads(facebook_branded_content)
            if isinstance(_p, dict):
                facebook_branded_content = _p
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(instagram_branded_content, str):
        try:
            _p = json.loads(instagram_branded_content)
            if isinstance(_p, dict):
                instagram_branded_content = _p
        except (json.JSONDecodeError, TypeError):
            pass
    for _pname, _pref in [
        ("image_hashes", "image_hashes"), ("messages", "messages"),
        ("headlines", "headlines"), ("descriptions", "descriptions"), ("ad_formats", "ad_formats"),
    ]:
        val = {"image_hashes": image_hashes, "messages": messages, "headlines": headlines,
               "descriptions": descriptions, "ad_formats": ad_formats}[_pname]
        if isinstance(val, str):
            try:
                _p = json.loads(val)
                if isinstance(_p, list):
                    if _pname == "image_hashes":
                        image_hashes = _p
                    elif _pname == "messages":
                        messages = _p
                    elif _pname == "headlines":
                        headlines = _p
                    elif _pname == "descriptions":
                        descriptions = _p
                    elif _pname == "ad_formats":
                        ad_formats = _p
            except (json.JSONDecodeError, TypeError):
                pass

    # Validate media mutual exclusivity
    media_params = sum(1 for x in [image_hash, image_hashes, video_id, videos, images] if x)
    if media_params > 1:
        return json.dumps({"error": "Only one media source allowed: image_hash, image_hashes, video_id, videos, or images."}, indent=2)
    if media_params == 0 and not object_story_id:
        return json.dumps({"error": "No media provided. Specify image_hash, image_hashes, video_id, videos, images, or object_story_id."}, indent=2)

    if image_hashes and len(image_hashes) > 10:
        return json.dumps({"error": "Maximum 10 image hashes allowed for FLEX creatives"}, indent=2)
    if thumbnail_url and not video_id:
        return json.dumps({"error": "thumbnail_url can only be used with video_id."}, indent=2)

    dof_multi_image_warning = (
        f"DEGREES_OF_FREEDOM mode with {len(image_hashes)} image_hashes: Meta will only serve "
        "ONE image at delivery time."
    ) if (optimization_type == "DEGREES_OF_FREEDOM" and image_hashes and len(image_hashes) > 1) else None

    if message and messages:
        return json.dumps({"error": "Cannot specify both 'message' and 'messages'."}, indent=2)
    if not link_url and not lead_gen_form_id and not object_story_id and not reminder_data:
        return json.dumps({"error": "No link_url provided. A destination URL is required (unless using lead_gen_form_id, object_story_id, or reminder_data)."}, indent=2)
    if headline and headlines:
        return json.dumps({"error": "Cannot specify both 'headline' and 'headlines'."}, indent=2)
    if description and descriptions:
        return json.dumps({"error": "Cannot specify both 'description' and 'descriptions'."}, indent=2)

    if headlines:
        if len(headlines) > 5:
            return json.dumps({"error": "Maximum 5 headlines allowed for dynamic creatives"}, indent=2)
        for i, h in enumerate(headlines):
            if len(h) > 40:
                return json.dumps({"error": f"Headline {i+1} exceeds 40 character limit"}, indent=2)
    if descriptions:
        if len(descriptions) > 5:
            return json.dumps({"error": "Maximum 5 descriptions allowed for dynamic creatives"}, indent=2)
        for i, d in enumerate(descriptions):
            if len(d) > 125:
                return json.dumps({"error": f"Description {i+1} exceeds 125 character limit"}, indent=2)

    if not name:
        name = f"Creative {int(time.time())}"

    account_id = _ensure_act(account_id)

    # Auto-discover page_id if not provided
    if not page_id and not object_story_id:
        try:
            page_discovery_result = await _discover_pages_for_account(account_id)
            if page_discovery_result.get("success"):
                page_id = page_discovery_result["page_id"]
            else:
                return json.dumps({
                    "error": "No page ID provided and no suitable pages found for this account",
                    "details": page_discovery_result.get("message", "Page discovery failed"),
                    "suggestions": ["Use get_account_pages to see available pages", "Provide a page_id parameter manually"],
                }, indent=2)
        except Exception as e:
            return json.dumps({"error": "Error during page discovery", "details": str(e)}, indent=2)

    if page_id is not None:
        page_id = str(page_id)

    endpoint = f"{account_id}/adcreatives"

    try:
        creative_data: Dict[str, Any] = {"name": name}

        # Auto-downgrade DOF when asset_customization_rules is provided
        dof_downgraded = False
        if optimization_type == "DEGREES_OF_FREEDOM" and asset_customization_rules:
            logger.info("Dropping optimization_type=DEGREES_OF_FREEDOM because asset_customization_rules is set")
            optimization_type = None
            dof_downgraded = True

        # Determine whether to use asset_feed_spec path
        use_asset_feed = bool(
            headlines or descriptions or messages or image_hashes or videos or images
            or optimization_type or asset_customization_rules or (video_id and description)
            or (video_id and instagram_actor_id)
        )
        is_video = bool(video_id or videos)

        # Auto-fetch thumbnail for single video if not provided
        if video_id and not thumbnail_url:
            fetched = await _fetch_video_thumbnail(video_id)
            if fetched:
                thumbnail_url = fetched

        if object_story_id:
            # Existing-post path
            creative_data["object_story_id"] = object_story_id

            if asset_customization_rules:
                translated_rules_osi, videos_array_osi = _translate_video_customization_rules_for_existing_post(
                    asset_customization_rules
                )
                asset_feed_spec_osi: Dict[str, Any] = {}
                if videos_array_osi:
                    asset_feed_spec_osi["videos"] = videos_array_osi
                if translated_rules_osi:
                    asset_feed_spec_osi["asset_customization_rules"] = translated_rules_osi
                if link_url:
                    asset_feed_spec_osi["link_urls"] = [{"website_url": link_url}]
                if call_to_action_type:
                    if lead_gen_form_id or phone_number:
                        cta_osi_value: Dict[str, Any] = {}
                        if link_url:
                            cta_osi_value["link"] = link_url
                        if lead_gen_form_id:
                            cta_osi_value["lead_gen_form_id"] = lead_gen_form_id
                        if phone_number:
                            cta_osi_value["phone_number"] = phone_number
                        asset_feed_spec_osi["call_to_actions"] = [{"type": call_to_action_type, "value": cta_osi_value}]
                    else:
                        asset_feed_spec_osi["call_to_action_types"] = [call_to_action_type]
                if asset_feed_spec_osi:
                    creative_data["asset_feed_spec"] = asset_feed_spec_osi
            elif call_to_action_type:
                cta_osi: Dict[str, Any] = {"type": call_to_action_type}
                cta_osi_value2: Dict[str, Any] = {}
                if link_url:
                    cta_osi_value2["link"] = link_url
                if lead_gen_form_id:
                    cta_osi_value2["lead_gen_form_id"] = lead_gen_form_id
                if phone_number:
                    cta_osi_value2["phone_number"] = phone_number
                if cta_osi_value2:
                    cta_osi["value"] = cta_osi_value2
                creative_data["call_to_action"] = cta_osi

            if instagram_actor_id:
                creative_data["instagram_actor_id"] = instagram_actor_id

        elif use_asset_feed:
            videos_array = None
            images_array = None

            if videos:
                thumb_coros = [
                    _fetch_video_thumbnail(str(v["video_id"]))
                    for v in videos if not v.get("thumbnail_url")
                ]
                fetched_iter = iter(await asyncio.gather(*thumb_coros) if thumb_coros else [])
                videos_array = []
                for v in videos:
                    vid_id = str(v["video_id"])
                    entry: Dict[str, Any] = {"video_id": vid_id}
                    if v.get("thumbnail_url"):
                        entry["thumbnail_url"] = v["thumbnail_url"]
                    else:
                        fetched_thumb = next(fetched_iter, None)
                        if fetched_thumb:
                            entry["thumbnail_url"] = fetched_thumb
                    if v.get("label"):
                        entry["adlabels"] = [{"name": v["label"]}]
                    elif v.get("adlabels"):
                        entry["adlabels"] = v["adlabels"]
                    videos_array.append(entry)
            elif video_id:
                videos_array = [{"video_id": video_id}]
                if thumbnail_url:
                    videos_array[0]["thumbnail_url"] = thumbnail_url
            elif images:
                images_array = []
                for img in images:
                    entry2: Dict[str, Any] = {"hash": img.get("image_hash") or img.get("hash")}
                    if img.get("label"):
                        entry2["adlabels"] = [{"name": img["label"]}]
                    elif img.get("adlabels"):
                        entry2["adlabels"] = img["adlabels"]
                    images_array.append(entry2)
            elif image_hashes:
                images_array = [{"hash": h} for h in image_hashes]
            elif image_hash:
                images_array = [{"hash": image_hash}]

            if asset_customization_rules:
                if images_array:
                    asset_customization_rules, images_array = _translate_asset_customization_rules(
                        asset_customization_rules, images_array
                    )
                elif videos_array:
                    asset_customization_rules, videos_array = _translate_video_customization_rules(
                        asset_customization_rules, videos_array
                    )

            is_dof = optimization_type == "DEGREES_OF_FREEDOM"
            if is_dof:
                asset_feed_spec: Dict[str, Any] = {"optimization_type": optimization_type}
                if ad_formats:
                    asset_feed_spec["ad_formats"] = ad_formats
            else:
                resolved_ad_formats = ad_formats or (["SINGLE_VIDEO"] if is_video else ["SINGLE_IMAGE"])
                asset_feed_spec = {
                    "link_urls": [{"website_url": link_url}],
                    "ad_formats": resolved_ad_formats,
                }
                if optimization_type:
                    asset_feed_spec["optimization_type"] = optimization_type

            if videos_array:
                asset_feed_spec["videos"] = videos_array
            if images_array:
                asset_feed_spec["images"] = images_array

            if headlines:
                asset_feed_spec["titles"] = [{"text": h} for h in headlines]
            elif headline:
                asset_feed_spec["titles"] = [{"text": headline}]

            if descriptions:
                asset_feed_spec["descriptions"] = [{"text": d} for d in descriptions]
            elif description:
                asset_feed_spec["descriptions"] = [{"text": description}]

            if messages:
                asset_feed_spec["bodies"] = [{"text": m} for m in messages]
            elif message:
                asset_feed_spec["bodies"] = [{"text": message}]

            if call_to_action_type and not is_dof:
                if lead_gen_form_id or phone_number:
                    cta_value: Dict[str, Any] = {}
                    if link_url:
                        cta_value["link"] = link_url
                    if lead_gen_form_id:
                        cta_value["lead_gen_form_id"] = lead_gen_form_id
                    if phone_number:
                        cta_value["phone_number"] = phone_number
                    asset_feed_spec["call_to_actions"] = [{"type": call_to_action_type, "value": cta_value}]
                else:
                    asset_feed_spec["call_to_action_types"] = [call_to_action_type]

            if asset_customization_rules:
                asset_feed_spec["asset_customization_rules"] = asset_customization_rules

            creative_data["asset_feed_spec"] = asset_feed_spec

            if video_id or not is_dof:
                creative_data["object_story_spec"] = {"page_id": page_id}
            else:
                link_data: Dict[str, Any] = {}
                if link_url:
                    link_data["link"] = link_url
                if image_hashes:
                    link_data["image_hash"] = image_hashes[0]
                elif image_hash:
                    link_data["image_hash"] = image_hash
                if caption:
                    link_data["caption"] = caption
                if image_crops:
                    link_data["image_crops"] = image_crops
                if event_id:
                    link_data["event_id"] = event_id
                if reminder_data:
                    link_data["reminder_data"] = reminder_data
                if call_to_action_type:
                    cta2: Dict[str, Any] = {"type": call_to_action_type}
                    cta_value2: Dict[str, Any] = {}
                    if link_url:
                        cta_value2["link"] = link_url
                    if lead_gen_form_id:
                        cta_value2["lead_gen_form_id"] = lead_gen_form_id
                    if phone_number:
                        cta_value2["phone_number"] = phone_number
                    if event_id and call_to_action_type in ("EVENT_RSVP", "BUY_TICKETS"):
                        cta_value2["event_id"] = event_id
                    if cta_value2:
                        cta2["value"] = cta_value2
                    link_data["call_to_action"] = cta2
                creative_data["object_story_spec"] = {"page_id": page_id, "link_data": link_data}

        else:
            if is_video:
                video_data: Dict[str, Any] = {"video_id": video_id}
                if thumbnail_url:
                    video_data["image_url"] = thumbnail_url
                if message:
                    video_data["message"] = message
                if headline:
                    video_data["title"] = headline
                cta_value3: Dict[str, Any] = {}
                if link_url:
                    cta_value3["link"] = link_url
                if lead_gen_form_id:
                    cta_value3["lead_gen_form_id"] = lead_gen_form_id
                if phone_number:
                    cta_value3["phone_number"] = phone_number
                cta_type = call_to_action_type or ("LEARN_MORE" if link_url else None)
                if cta_type:
                    cta3: Dict[str, Any] = {"type": cta_type}
                    if cta_value3:
                        cta3["value"] = cta_value3
                    video_data["call_to_action"] = cta3
                creative_data["object_story_spec"] = {"page_id": page_id, "video_data": video_data}
            else:
                link_data2: Dict[str, Any] = {"image_hash": image_hash}
                if link_url:
                    link_data2["link"] = link_url
                creative_data["object_story_spec"] = {"page_id": page_id, "link_data": link_data2}
                if message:
                    creative_data["object_story_spec"]["link_data"]["message"] = message
                if headline:
                    creative_data["object_story_spec"]["link_data"]["name"] = headline
                if description:
                    creative_data["object_story_spec"]["link_data"]["description"] = description
                if caption:
                    creative_data["object_story_spec"]["link_data"]["caption"] = caption
                if image_crops:
                    creative_data["object_story_spec"]["link_data"]["image_crops"] = image_crops
                if event_id:
                    creative_data["object_story_spec"]["link_data"]["event_id"] = event_id
                if reminder_data:
                    creative_data["object_story_spec"]["link_data"]["reminder_data"] = reminder_data
                if call_to_action_type:
                    cta4: Dict[str, Any] = {"type": call_to_action_type}
                    cta_value4: Dict[str, Any] = {}
                    if lead_gen_form_id:
                        cta_value4["lead_gen_form_id"] = lead_gen_form_id
                    if phone_number:
                        cta_value4["phone_number"] = phone_number
                    if event_id and call_to_action_type in ("EVENT_RSVP", "BUY_TICKETS"):
                        cta_value4["event_id"] = event_id
                    if cta_value4:
                        cta4["value"] = cta_value4
                    creative_data["object_story_spec"]["link_data"]["call_to_action"] = cta4

        if dynamic_creative_spec:
            creative_data["dynamic_creative_spec"] = dynamic_creative_spec

        if creative_features_spec:
            creative_data["degrees_of_freedom_spec"] = {"creative_features_spec": creative_features_spec}

        if disable_all_enhancements:
            dof = creative_data.setdefault("degrees_of_freedom_spec", {})
            cfs = dof.setdefault("creative_features_spec", {})
            for key in _ALL_ENHANCEMENT_KEYS:
                if key not in cfs:
                    cfs[key] = {"enroll_status": "OPT_OUT"}
            if "contextual_multi_ads" not in creative_data:
                creative_data["contextual_multi_ads"] = {"enroll_status": "OPT_OUT"}

        if url_tags:
            creative_data["url_tags"] = url_tags

        if instagram_actor_id and "object_story_spec" in creative_data:
            creative_data["object_story_spec"]["instagram_user_id"] = instagram_actor_id

        if facebook_branded_content:
            creative_data["facebook_branded_content"] = facebook_branded_content
        if instagram_branded_content:
            creative_data["instagram_branded_content"] = instagram_branded_content

        data = await _meta_post(endpoint, creative_data)

        if instagram_actor_id and "error" in data:
            err_details = data.get("error", {}).get("details", {})
            inner_msg = ""
            if isinstance(err_details, dict):
                inner_err = err_details.get("error", {})
                if isinstance(inner_err, dict):
                    inner_msg = inner_err.get("message", "")
            if "valid Instagram account id" in inner_msg or "instagram_actor_id" in inner_msg.lower():
                return json.dumps({
                    "error": "Instagram account not authorized for advertising",
                    "explanation": "Your access token may be missing the 'instagram_basic' permission.",
                    "instagram_actor_id": instagram_actor_id,
                    "meta_error": inner_msg,
                }, indent=2)

        if "id" in data:
            creative_id = data["id"]
            creative_details = await _meta_get(
                creative_id,
                {"fields": "id,name,status,thumbnail_url,image_url,image_hash,object_story_spec,object_type,body,title,effective_object_story_id,asset_feed_spec{images,videos,bodies,titles,descriptions,link_urls,ad_formats,call_to_action_types,optimization_type,asset_customization_rules},url_tags,link_url"},
            )
            result: Dict[str, Any] = {"success": True, "creative_id": creative_id, "details": creative_details}

            posted_afs = creative_data.get("asset_feed_spec") if isinstance(creative_data.get("asset_feed_spec"), dict) else None
            posted_images = posted_afs.get("images") if posted_afs else None
            posted_rules = posted_afs.get("asset_customization_rules") if posted_afs else None
            stored_afs = creative_details.get("asset_feed_spec") if isinstance(creative_details, dict) else None
            collapsed = bool(
                posted_images and len(posted_images) > 1
                and posted_rules
                and (not stored_afs or not stored_afs.get("images"))
            )

            warnings_: List[str] = []
            if dof_downgraded:
                warnings_.append(
                    "optimization_type=DEGREES_OF_FREEDOM was dropped because asset_customization_rules was provided. "
                    "Whether placement rules take effect depends on the ad set's is_dynamic_creative capability."
                )
            elif dof_multi_image_warning:
                warnings_.append(dof_multi_image_warning)
            if collapsed:
                warnings_.append(
                    "Meta silently rewrote this creative from multi-image asset_feed_spec to single-image object_story_spec. "
                    "Only the first image will serve. Attach to an ad set with is_dynamic_creative=true, or use image_crops."
                )
            if warnings_:
                result["warning"] = warnings_[0] if len(warnings_) == 1 else warnings_
            return json.dumps(result, indent=2)

        return json.dumps(data, indent=2)

    except Exception as e:
        logger.exception("create_ad_creative failed")
        return json.dumps({"error": "Failed to create ad creative", "details": str(e)}, indent=2)


@mcp.tool()
async def update_ad_creative(
    creative_id: str,
    name: Optional[str] = None,
    message: Optional[str] = None,
    messages: Optional[List[str]] = None,
    headline: Optional[str] = None,
    headlines: Optional[List[str]] = None,
    description: Optional[str] = None,
    descriptions: Optional[List[str]] = None,
    optimization_type: Optional[str] = None,
    dynamic_creative_spec: Optional[Dict[str, Any]] = None,
    call_to_action_type: Optional[str] = None,
    lead_gen_form_id: Optional[Union[str, int]] = None,
    ad_formats: Optional[List[str]] = None,
    creative_features_spec: Optional[Dict[str, Any]] = None,
) -> str:
    """Update an existing ad creative's name or optimization settings.

    IMPORTANT — Meta API limitation: The Meta API does NOT allow updating content
    fields (message, headline, description, CTA, image, video, URL) on existing
    creatives. Only the creative name and optimization settings (asset_feed_spec)
    can be changed. To change ad content, create a new creative and update the ad
    to reference it via update_ad.

    Args:
        creative_id: Meta Ads creative ID to update
        name: New creative name (most reliable update)
        message: New ad copy/text — NOTE: Meta API may reject this on existing creatives
        messages: List of primary text variants — NOTE: Meta API may reject this on existing creatives
        headline: Single headline — NOTE: Meta API may reject this on existing creatives
        headlines: New list of headlines — NOTE: Meta API may reject this on existing creatives
        description: Single description — NOTE: Meta API may reject this on existing creatives
        descriptions: New list of descriptions — NOTE: Meta API may reject this on existing creatives
        optimization_type: Set to "DEGREES_OF_FREEDOM" for FLEX (Advantage+) creatives
        dynamic_creative_spec: New dynamic creative optimization settings
        call_to_action_type: New call to action button type — NOTE: Meta API may reject this
        lead_gen_form_id: Lead generation form ID for lead gen campaigns
        ad_formats: List of ad format strings (e.g., ["SINGLE_IMAGE"])
        creative_features_spec: Dict of Advantage+ Creative feature opt-ins/opt-outs.
                   Each key is a feature name, value is {"enroll_status": "OPT_IN"|"OPT_OUT"}.
                   Sent as a top-level field (not inside degrees_of_freedom_spec).
    """
    if lead_gen_form_id is not None:
        lead_gen_form_id = str(lead_gen_form_id)
    if not creative_id:
        return json.dumps({"error": "No creative ID provided"}, indent=2)

    if headline and headlines:
        return json.dumps({"error": "Cannot specify both 'headline' and 'headlines'."}, indent=2)
    if description and descriptions:
        return json.dumps({"error": "Cannot specify both 'description' and 'descriptions'."}, indent=2)
    if message and messages:
        return json.dumps({"error": "Cannot specify both 'message' and 'messages'."}, indent=2)
    if optimization_type and optimization_type != "DEGREES_OF_FREEDOM":
        return json.dumps({"error": f"Invalid optimization_type '{optimization_type}'. Only 'DEGREES_OF_FREEDOM' is supported."}, indent=2)

    if headlines:
        if len(headlines) > 5:
            return json.dumps({"error": "Maximum 5 headlines allowed"}, indent=2)
        for i, h in enumerate(headlines):
            if len(h) > 40:
                return json.dumps({"error": f"Headline {i+1} exceeds 40 character limit"}, indent=2)
    if descriptions:
        if len(descriptions) > 5:
            return json.dumps({"error": "Maximum 5 descriptions allowed"}, indent=2)
        for i, d in enumerate(descriptions):
            if len(d) > 125:
                return json.dumps({"error": f"Description {i+1} exceeds 125 character limit"}, indent=2)

    update_data: Dict[str, Any] = {}
    if name:
        update_data["name"] = name

    use_asset_feed = bool(headlines or descriptions or messages or optimization_type or dynamic_creative_spec)

    if use_asset_feed:
        asset_feed_spec: Dict[str, Any] = {}
        if ad_formats:
            asset_feed_spec["ad_formats"] = ad_formats
        else:
            asset_feed_spec["ad_formats"] = ["SINGLE_IMAGE"]
        if optimization_type:
            asset_feed_spec["optimization_type"] = optimization_type
        if headlines:
            asset_feed_spec["titles"] = [{"text": h} for h in headlines]
        elif headline:
            asset_feed_spec["titles"] = [{"text": headline}]
        if descriptions:
            asset_feed_spec["descriptions"] = [{"text": d} for d in descriptions]
        elif description:
            asset_feed_spec["descriptions"] = [{"text": description}]
        if messages:
            asset_feed_spec["bodies"] = [{"text": m} for m in messages]
        elif message:
            asset_feed_spec["bodies"] = [{"text": message}]
        if call_to_action_type:
            asset_feed_spec["call_to_action_types"] = [call_to_action_type]
        update_data["asset_feed_spec"] = asset_feed_spec
    else:
        if message or headline or description or call_to_action_type or lead_gen_form_id:
            update_data["object_story_spec"] = {"link_data": {}}
            if message:
                update_data["object_story_spec"]["link_data"]["message"] = message
            if headline:
                update_data["object_story_spec"]["link_data"]["name"] = headline
            if description:
                update_data["object_story_spec"]["link_data"]["description"] = description
            if call_to_action_type or lead_gen_form_id:
                cta_data: Dict[str, Any] = {}
                if call_to_action_type:
                    cta_data["type"] = call_to_action_type
                if lead_gen_form_id:
                    cta_data["value"] = {"lead_gen_form_id": lead_gen_form_id}
                if cta_data:
                    update_data["object_story_spec"]["link_data"]["call_to_action"] = cta_data

    if dynamic_creative_spec:
        update_data["dynamic_creative_spec"] = dynamic_creative_spec
    if creative_features_spec:
        update_data["creative_features_spec"] = creative_features_spec

    try:
        data = await _meta_post(creative_id, update_data)

        if "id" in data:
            creative_details = await _meta_get(
                creative_id,
                {"fields": "id,name,status,thumbnail_url,image_url,image_hash,object_story_spec,url_tags,link_url,dynamic_creative_spec,degrees_of_freedom_spec"},
            )
            return json.dumps({"success": True, "creative_id": creative_id, "details": creative_details}, indent=2)

        error_obj = data.get("error", {})
        error_subcode = None
        if isinstance(error_obj, dict):
            error_details = error_obj.get("details", {})
            if isinstance(error_details, dict):
                inner_error = error_details.get("error", {})
                error_subcode = inner_error.get("error_subcode") if isinstance(inner_error, dict) else None
            else:
                error_subcode = error_obj.get("error_subcode")
        if error_subcode == 1815573:
            return json.dumps({
                "error": "Content updates are not allowed on existing creatives",
                "explanation": "The Meta API does not allow updating content fields on existing creatives. Only the name can be changed.",
                "workaround": "Create a new creative with create_ad_creative, then call update_ad with the new creative_id.",
                "creative_id": creative_id,
            }, indent=2)

        return json.dumps(data, indent=2)

    except Exception as e:
        return json.dumps({"error": "Failed to update ad creative", "details": str(e)}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Budget Tools ─────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def create_budget_schedule(
    campaign_id: str,
    budget_value: int,
    budget_value_type: str,
    time_start: int,
    time_end: int,
) -> str:
    """Create a budget schedule for a Meta Ads campaign.

    Allows scheduling budget increases based on anticipated high-demand periods.

    Args:
        campaign_id: Meta Ads campaign ID
        budget_value: Amount of budget increase. Interpreted based on budget_value_type.
        budget_value_type: Type of budget value — "ABSOLUTE" or "MULTIPLIER"
        time_start: Unix timestamp for when the high demand period starts
        time_end: Unix timestamp for when the high demand period ends
    """
    if not campaign_id:
        return json.dumps({"error": "Campaign ID is required"}, indent=2)
    if budget_value is None:
        return json.dumps({"error": "Budget value is required"}, indent=2)
    if not budget_value_type:
        return json.dumps({"error": "Budget value type is required"}, indent=2)
    if budget_value_type not in ["ABSOLUTE", "MULTIPLIER"]:
        return json.dumps({"error": "Invalid budget_value_type. Must be ABSOLUTE or MULTIPLIER"}, indent=2)
    if time_start is None:
        return json.dumps({"error": "Time start is required"}, indent=2)
    if time_end is None:
        return json.dumps({"error": "Time end is required"}, indent=2)

    params: Dict[str, Any] = {
        "budget_value": budget_value,
        "budget_value_type": budget_value_type,
        "time_start": time_start,
        "time_end": time_end,
    }

    try:
        data = await _meta_post(f"{campaign_id}/budget_schedules", params)
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({
            "error": "Failed to create budget schedule",
            "details": str(e),
            "campaign_id": campaign_id,
        }, indent=2)


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

    port = int(os.getenv("PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
