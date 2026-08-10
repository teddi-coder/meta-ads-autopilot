# Add rate-limit backoff and proactive throttling to Meta Marketing API wrappers

**Repo:** `/Volumes/KINGSTON/meta-ads-autopilot` (confirm the exact GitHub repo name/org with `git remote -v` if you need it for anything beyond local work)
**Branch:** create new branch named `fix/ads-api-backoff-throttle`
**Deploy:** Fly.io — **do not assume the app name.** Run `cat fly.toml | grep "^app ="` (or check `[grep -n "app_name\|app ="] fly.toml`) to get the exact app name before doing anything that references it (e.g. the log pull in "How to verify" below).

> **Before you start:** read `README.md` (and `CLAUDE.md` if present) in this repo
> in full before writing or changing any code. It documents the live architecture
> and the constraints this brief assumes. If anything below contradicts the README,
> stop and flag it rather than proceeding.

---

## What's needed

This follows on from a permission/error-rate audit already run in this repo (results saved under `.claude/briefs/`). That audit found two separate Marketing API call wrappers, neither of which retries or backs off on failure — and this is very likely the reason Meta's App Review rejected this app's Marketing API Access Tier request for having too high an error rate on the last 500 API calls:

- **`meta_ads_mcp/core/api.py` → `make_api_request`** already correctly distinguishes rate-limit errors (code 4 — does not invalidate the token) from auth errors (codes 190/102 — invalidates the token), and already logs the `X-App-Usage`, `X-Business-Use-Case-Usage`, and `X-Ad-Account-Usage` response headers at warning level when usage crosses 80%. But it takes no action on any of this — no retry, no backoff, no throttling. The infrastructure to do the right thing is already there; it's just not connected to anything.
- **`hedgehog_meta.py` → `_meta_get` / `_meta_post`** (lines 55–96) are simpler wrappers with no error classification at all — a single attempt, and on failure they just return `{"error": {...}}`.

With 30 client ad accounts being managed through these wrappers and zero retry/backoff anywhere in the call path, every transient rate-limit hit becomes a permanent failed call that counts against Meta's error-rate measurement.

---

## File(s) to change

- `meta_ads_mcp/core/api.py` → `make_api_request` (run `grep -n "def make_api_request" meta_ads_mcp/core/api.py` to get the current line number — it may have moved since the audit)
- `hedgehog_meta.py` → `_meta_get` / `_meta_post`, lines 55–96

---

## The fix

1. **In `make_api_request`:** add retry with exponential backoff, but ONLY for the rate-limit error family (codes 4, 17, 32, 613 — same family the function's existing auth-invalidation logic already excludes). Suggested: max 3 retries, base delay 2s doubling each retry, plus small random jitter to avoid synchronised retries across accounts. Do not retry auth errors (190/102) — retrying an invalid token wastes another call and can't succeed.

2. **Turn the existing usage-header logging into an actual throttle.** The function already reads and logs `X-Business-Use-Case-Usage` / `X-Ad-Account-Usage` at ≥80% usage — when that threshold is hit for a given ad account, proactively delay the *next* call against that same account (rather than waiting for Meta to reject it) before the rate limit is actually breached.

3. **In `hedgehog_meta.py`'s `_meta_get` / `_meta_post`:** check whether these can be refactored to call through `make_api_request` instead of duplicating logic — if the auth/base-URL setup is compatible, that's strongly preferred so there's one source of truth for retry/backoff behaviour. If they genuinely can't be merged (different auth path, different base URL), add the same retry/backoff logic directly to these two functions instead.

4. **Add a lightweight outcome counter** (in-memory or logged) tallying calls by result — success, rate-limited-then-succeeded, rate-limited-exhausted-retries, auth-error, malformed-request, other — so a future error-rate check doesn't require a raw Fly.io log grep to classify.

---

## Do NOT

- Do NOT retry auth/token errors (codes 190/102) — these need a human or token fix, not a retry.
- Do NOT retry malformed-request errors (code 100) — retrying an invalid request wastes a call and won't fix itself.
- Do NOT change the Slack write-approval workflow or the PAUSED-status safety behaviour for created campaigns/ad sets/ads — this fix is scoped to the transport/error-handling layer only.
- Do NOT touch anything related to `pages_read_user_content`, `pages_manage_posts`, `pages_manage_engagement`, `catalog_management`, or `threads_business_basic` — confirmed unused in the earlier audit and out of scope here.

---

## How to verify

1. Get the correct app name from `fly.toml`, then pull recent logs (requires Teddi's authenticated `flyctl` session — this can't run in a sandboxed environment):
   ```
   flyctl logs --app <app-name-from-fly.toml> --num 1000 2>&1 | grep -E '"code":[0-9]+|HTTP Error [4-9]|rate.limit|auth.*error|invalid.*token' | head -100
   ```
   Classify by error code (4/17/32/613 = rate limit; 190/102 = auth; 100 = malformed; 803/2635 = stale ID) — do this once **before** deploying the fix to confirm rate-limit errors are in fact the dominant category, and once **after** to confirm the fix worked.
2. After deploying, compare the before/after error rate over a comparable ~500-call sample. Rate-limit-caused failures specifically should drop close to zero (since they're now retried instead of failing outright).
3. Confirm no regression — check for an existing test suite (`grep -rln "def test_" .`) and run it if present; manually confirm a normal `ads_read` call still succeeds end-to-end.
4. Report the outcome-counter tallies from point 4 above alongside the log classification, so both sources agree.

---

## Context

192 Marketing API call sites were found across 17 files in this repo, concentrated in `hedgehog_meta.py` (45 hits), `meta_ads_mcp/core/ads.py` (41), and `hedgehog_meta_writer.py` (22). This app manages ~30 client Meta ad accounts via a single Business Manager System User access token (no OAuth/frontend login flow — server-to-server only). All write actions already go through a Slack human-approval step before touching the Meta API, and created campaigns/ad sets/ads are created PAUSED as a second safety layer — neither of those should be affected by this fix.
