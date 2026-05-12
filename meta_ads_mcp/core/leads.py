"""Lead form retrieval functionality for Meta Ads API.

Provides tools for listing Facebook Page lead gen forms and retrieving
the actual form submissions (lead data).

Requires: leads_retrieval + pages_read_engagement permissions on the access token.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, Dict

from .api import meta_api_tool, make_api_request
from .server import mcp_server


# ── Time range helpers ──────────────────────────────────────────────────────

def _preset_to_unix(preset: str):
    """Convert a time-range preset string to (unix_start, unix_end) integers.

    Supported presets: last_7d, last_30d  (case-insensitive).
    Returns None, None if the preset is unrecognised.
    """
    now = datetime.now(timezone.utc)
    presets = {
        "last_7d": timedelta(days=7),
        "last_30d": timedelta(days=30),
    }
    delta = presets.get(preset.lower())
    if delta is None:
        return None, None
    start = now - delta
    return int(start.timestamp()), int(now.timestamp())


def _date_str_to_unix(date_str: str) -> int:
    """Convert YYYY-MM-DD string to Unix timestamp (midnight UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _resolve_time_range(time_range):
    """Return (unix_start, unix_end) or (None, None) from a time_range value.

    Accepts:
      - preset string: "last_7d" or "last_30d"
      - dict:  {"since": "YYYY-MM-DD", "until": "YYYY-MM-DD"}
    """
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


# ── Tools ────────────────────────────────────────────────────────────────────

@mcp_server.tool()
@meta_api_tool
async def get_lead_forms(
    page_id: str,
    access_token: Optional[str] = None,
    limit: int = 25,
    status: str = "",
) -> str:
    """List all lead generation forms for a Facebook Page.

    Use this to find form IDs before pulling lead submissions via get_form_leads.

    Args:
        page_id: Facebook Page ID (use get_account_pages to find this)
        access_token: Meta API access token (optional - uses cached token if not provided)
        limit: Max forms to return (default 25)
        status: Filter by form status — 'ACTIVE', 'ARCHIVED', or '' for all (default: '')
    """
    params = {
        "fields": "id,name,status,created_time,questions,privacy_policy_url,leads_count",
        "limit": limit,
    }

    data = await make_api_request(f"{page_id}/leadgen_forms", access_token, params)

    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", "")
        if "leads_retrieval" in msg.lower() or code in (200, 10, 190):
            return json.dumps({
                "error": msg,
                "permissions_note": (
                    "This endpoint requires the 'leads_retrieval' and "
                    "'pages_read_engagement' permissions. "
                    "If these are missing, the access token must be regenerated "
                    "with those scopes — see the PR notes."
                ),
            }, indent=2)
        return json.dumps(data, indent=2)

    forms = data.get("data", [])

    # Client-side status filter (API doesn't support server-side filtering on this edge)
    if status:
        status_upper = status.upper()
        forms = [f for f in forms if f.get("status", "").upper() == status_upper]

    if not forms:
        return json.dumps({
            "message": f"No lead forms found for page {page_id}"
            + (f" with status '{status}'" if status else ""),
            "total": 0,
        }, indent=2)

    lines = [
        f"Lead Forms for Page {page_id}",
        f"Found {len(forms)} form(s)" + (f" matching status '{status.upper()}'" if status else ""),
        "=" * 50,
    ]

    for i, form in enumerate(forms, 1):
        form_id = form.get("id", "—")
        name = form.get("name", "(unnamed)")
        form_status = form.get("status", "—")
        created = (form.get("created_time") or "")[:10]
        leads_count = form.get("leads_count", 0)
        questions = form.get("questions", [])
        q_summary = ", ".join(
            q.get("key") or q.get("type", "?") for q in questions[:5]
        )
        if len(questions) > 5:
            q_summary += f", +{len(questions) - 5} more"

        lines.append(
            f"{i}. {name}\n"
            f"   Status: {form_status}  |  Leads (lifetime): {leads_count:,}  |  Created: {created}\n"
            f"   Fields: {q_summary or '(none)'}\n"
            f"   Form ID: {form_id}"
        )

    return "\n".join(lines)


@mcp_server.tool()
@meta_api_tool
async def get_form_leads(
    form_id: str,
    access_token: Optional[str] = None,
    limit: int = 25,
    time_range: Optional[Union[str, Dict[str, str]]] = None,
) -> str:
    """Retrieve lead form submissions (contact details) for a specific Meta lead gen form.

    Returns the actual lead data — names, emails, phone numbers — submitted by users.
    Also includes campaign attribution for each lead.

    IMPORTANT: Lead data contains PII. Do not store or log individual contact
    details — use this tool only for in-session reporting.

    Args:
        form_id: Lead form ID (from get_lead_forms)
        access_token: Meta API access token (optional - uses cached token if not provided)
        limit: Max leads to return (default 25, max 500)
        time_range: Optional date filter. Preset string (last_7d, last_30d) or
                    dict {"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}
    """
    limit = min(int(limit), 500)

    params = {
        "fields": (
            "id,created_time,field_data,"
            "ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,is_organic"
        ),
        "limit": limit,
    }

    # Apply time_range filtering using Unix timestamps
    unix_start, unix_end = _resolve_time_range(time_range)
    if unix_start is not None and unix_end is not None:
        params["filtering"] = json.dumps([
            {"field": "time_created", "operator": "GREATER_THAN", "value": unix_start},
            {"field": "time_created", "operator": "LESS_THAN", "value": unix_end},
        ])

    data = await make_api_request(f"{form_id}/leads", access_token, params)

    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", "")

        if code == 100 and "does not exist" in msg.lower():
            return json.dumps({
                "error": f"Form not found: {form_id}. "
                         "Verify the form_id using get_lead_forms(page_id=...)."
            }, indent=2)

        if "leads_retrieval" in msg.lower() or code in (200, 10, 190):
            return json.dumps({
                "error": msg,
                "permissions_note": (
                    "This endpoint requires 'leads_retrieval' permission. "
                    "The current access token may not have this scope. "
                    "Regenerate the token with leads_retrieval + pages_read_engagement."
                ),
            }, indent=2)

        return json.dumps(data, indent=2)

    leads = data.get("data", [])
    paging = data.get("paging", {})
    total_count = data.get("paging", {}).get("total", None)

    if not leads:
        return json.dumps({
            "message": "No leads found"
            + (f" for the specified time range" if time_range else "")
            + f" on form {form_id}.",
            "total": 0,
        }, indent=2)

    # Try to get form name from first lead's context (not always available)
    form_name = f"form {form_id}"

    total_str = str(total_count) if total_count is not None else "?"
    lines = [
        f"Showing {len(leads)} of {total_str} total leads for {form_name}",
        "=" * 50,
    ]

    for i, lead in enumerate(leads, 1):
        lead_id = lead.get("id", "—")
        created = (lead.get("created_time") or "")[:10]
        is_organic = lead.get("is_organic", False)

        # Flatten field_data — do NOT store/log this PII outside of session
        field_data = lead.get("field_data", [])
        fields_str = ", ".join(
            f"{fd.get('name')}: {', '.join(str(v) for v in fd.get('values', []))}"
            for fd in field_data
        )

        # Attribution chain
        campaign_name = lead.get("campaign_name") or ""
        adset_name = lead.get("adset_name") or ""
        ad_name = lead.get("ad_name") or ""
        attribution_parts = [p for p in [campaign_name, adset_name, ad_name] if p]
        attribution = " → ".join(attribution_parts) if attribution_parts else "(organic)" if is_organic else "(unknown)"

        organic_tag = "  [organic]" if is_organic else ""
        lines.append(
            f"{i}. [{created}] {fields_str}\n"
            f"   Campaign: {attribution}{organic_tag}\n"
            f"   Lead ID: {lead_id}"
        )

    if paging.get("next"):
        lines.append(f"\n(More leads available — increase limit or paginate)")

    return "\n".join(lines)
