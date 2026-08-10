# Reduce Ads API error rate blocking Marketing API Access Tier

**Repo:** `hedgehog-meta-ads` (confirm exact repo name/org — inferred from the Meta App Review submission text, not verified against an actual repo listing. If this doesn't match, stop and ask rather than guessing.)
**Branch:** create new branch named `fix/ads-api-error-rate`
**Deploy:** Fly.io (app inferred as `hedgehog-meta-ads` from the submission — the System User token is stored as a Fly.io secret per the review notes; confirm the actual app name with `flyctl apps list` if unsure)

> **Before you start:** read `README.md` (and `CLAUDE.md` if present) in this repo
> in full before writing or changing any code. It documents the live architecture
> and the constraints this brief assumes. If anything below contradicts the README,
> stop and flag it rather than proceeding.

---

## Symptom

Meta's App Review team rejected this app's request for **Marketing API Access Tier** (standard access) with this exact reason:

> "Our records show that the error rate for Ads API in the last 500 calls by this application is too high. It is required that the application have a sufficiently low error rate before it is approved for Marketing API standard access tier."

This is a hard blocker on scaled access — Hedgehog Marketing manages ~30 client ad accounts through this app via a Meta Business Manager System User token (no OAuth/frontend login flow; authentication is server-to-server). Without standard tier, `ads_read` and `ads_management` calls are rate-limited in a way that doesn't work for managing that many accounts. This is separate from, and independent of, a different batch of permission rejections tied to screencast documentation — this brief only covers the error-rate problem.

Meta's review page links to their own criteria doc for the exact error-rate threshold required (the link text was just "here" in the review UI) — locate and note the actual threshold if visible from the App Dashboard's Marketing API Access Tier request page, since the brief below doesn't have that number.

---

## Diagnostic steps

Root cause isn't known yet — work through these in order and report what you find before making any fix.

1. **Locate every Marketing API call site.** The language/framework isn't confirmed, so widen the grep rather than trust an assumption:
   ```
   grep -rn "graph.facebook.com\|FacebookAdsApi\|marketing_api\|/act_\|ads_insights\|AdAccount(\|Campaign(\|AdSet(\|/campaigns\b\|/adsets\b" --include="*.js" --include="*.ts" --include="*.py" .
   ```
   Report the actual count and file list — if this returns zero hits, the pattern is wrong for this codebase and needs widening further before continuing.

2. **Check for centralised error handling around those call sites.** Is there a single wrapper/client function all Marketing API calls go through, or are calls scattered with ad-hoc error handling? Report which.

3. **Pull recent logs from Fly.io** (adjust app name if step 1 above turned up a different one):
   ```
   flyctl logs --app hedgehog-meta-ads --num 1000
   ```
   Filter for HTTP status codes ≥400 or Meta Graph API error responses (they carry a `code` and `error_subcode` field in the JSON body, e.g. `{"error":{"code":190,...}}`).

4. **Classify the errors found** into these known Marketing API failure categories, and report a rough tally for each:
   - **Rate limiting** — HTTP 429, or error codes 4, 17, 32, 613 (Meta's rate-limit family). Often caused by retrying without backoff, or too many concurrent calls per ad account.
   - **Auth/token errors** — error code 190 (invalid/expired OAuth token) or 200-series permission errors. Could mean the System User token itself is stale, or a specific client's ad account access was revoked after the app was granted access.
   - **Malformed request errors** — error code 100 (invalid parameter), often from deprecated fields, bad date range formats, or fields not valid for a given object type.
   - **Object-not-found errors** — error codes 803 or 2635, usually from calling against a stale campaign/ad set/ad account ID that was deleted or is no longer accessible (e.g. cached IDs from an account Hedgehog no longer manages).

5. Report which category dominates before proposing a fix — do not guess and fix the wrong thing.

---

## The fix (choose based on diagnosis)

- **If rate-limit errors dominate:** add exponential backoff on 429/rate-limit responses, and respect Meta's `X-Business-Use-Case-Usage` / `X-Ad-Account-Usage` response headers to throttle proactively rather than reactively. Cap concurrent in-flight calls per ad account.
- **If auth/token errors dominate:** add a pre-flight check that validates the System User token still has access to a given ad account before calling it, and skip with a clear log line (`"skipping <account_id>: access revoked or token invalid"`) instead of calling and failing. Do not retry auth failures — they won't succeed on retry.
- **If malformed-request errors dominate:** identify the specific parameters/fields causing 400s from the log samples, correct them, and add basic request validation before the call is made so future regressions fail fast in testing rather than showing up as API errors in production.
- **If stale-ID errors dominate:** add a periodic reconciliation step that removes or flags ad account/campaign/ad set IDs Meta reports as gone, instead of retrying them every cycle.

It's plausible more than one category is contributing — fix the dominant one first, re-measure, then address the next if the error rate is still too high.

---

## Do NOT

- Do NOT change which Meta permissions/scopes this app requests — that's a separate, already-in-progress workstream (screencast documentation for a different set of rejected permissions). This brief is scoped to the error rate only.
- Do NOT remove error handling or retries entirely — add backoff/validation, don't strip out failure handling.
- Do NOT touch MCP tools unrelated to the Marketing API call path (e.g. anything touching Pages, Instagram, or catalog endpoints).

---

## How to verify

1. After the fix, run the server against a representative sample of real client ad accounts (or replay the same call pattern from the diagnostic logs) and count errors vs. total calls over a comparable sample size (~500 calls, matching what Meta measured).
2. Report the before/after error count by category.
3. Confirm no previously-passing functionality (successful `ads_read` / `ads_management` calls) regressed.

---

## Context

This app is a backend MCP (Model Context Protocol) server with no UI — it's called by a Slack bot, scheduled reporting tasks, and Claude Desktop, all using the same Meta Business Manager System User access token stored as an encrypted Fly.io secret. It manages ~30 client ad accounts on behalf of Hedgehog Marketing, an Australian digital marketing agency. All write actions (budget/campaign changes) already go through a human Slack-approval step before touching the Meta API — that flow is unrelated to this error-rate issue and shouldn't be touched.
