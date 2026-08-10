# Audit which of 5 Meta permissions the MCP server actually uses

**Repo:** `hedgehog-meta-ads` (confirm exact repo name/org — inferred from the Meta App Review submission text, not verified. If this doesn't match, stop and ask rather than guessing.)
**Branch:** none needed — this is a read-only investigation. Do not commit or push any code changes.
**Deploy:** N/A (diagnostic only)

> **Before you start:** read `README.md` (and `CLAUDE.md` if present) in this repo
> in full before investigating. It documents the live architecture and may already
> list which Meta permissions/tools are implemented.

---

## What's needed

Meta's App Review rejected 9 requested permissions for this app, on the grounds that the screencast didn't demonstrate their use. Before re-recording a screencast and resubmitting, Hedgehog needs to know which of these are **actually implemented and callable** in the MCP server today versus requested but unused. Unused permissions should likely be dropped from the resubmission rather than defended with new demo footage.

This brief covers 5 of the 9 rejected permissions where usage is genuinely uncertain (the other 4 — `ads_read`, `read_insights`, `instagram_manage_insights`, `instagram_basic` — are assumed to be in active use for reporting and are out of scope here):

- `pages_read_user_content`
- `pages_manage_posts`
- `pages_manage_engagement`
- `catalog_management`
- `threads_business_basic`

---

## Diagnostic steps

For each permission below, grep for the Graph API endpoints/patterns it corresponds to, then report findings. Widen the grep if the first pattern returns nothing — a zero-result grep on the first try likely means the pattern doesn't match this codebase's conventions, not that the feature is absent.

1. **`pages_read_user_content`** — reading posts/comments/ratings created by users (not the Page itself) on a Page, or posts the Page is tagged in.
   ```
   grep -rn "\/comments\b\|\/ratings\b\|tagged\|read_user_content" --include="*.js" --include="*.ts" --include="*.py" .
   ```

2. **`pages_manage_posts`** — creating, editing, publishing, scheduling, or deleting Page posts.
   ```
   grep -rn "\/feed\b.*POST\|createPost\|publishPost\|schedulePost\|\/photos\b\|\/videos\b" --include="*.js" --include="*.ts" --include="*.py" .
   ```

3. **`pages_manage_engagement`** — creating/editing/deleting comments, or managing Page likes on content.
   ```
   grep -rn "createComment\|deleteComment\|moderateComment\|manage_engagement" --include="*.js" --include="*.ts" --include="*.py" .
   ```

4. **`catalog_management`** — creating/reading/updating/deleting product catalogues, product sets, or product feeds.
   ```
   grep -rn "product_catalog\|productCatalog\|\/products\b\|product_set\|product_feed" --include="*.js" --include="*.ts" --include="*.py" .
   ```

5. **`threads_business_basic`** — fetching a Threads account ID associated with an Instagram account (used as `threads_user_id` for Threads ads).
   ```
   grep -rn "threads_user_id\|threads_business\|ThreadsAccount" --include="*.js" --include="*.ts" --include="*.py" .
   ```

For each of the 5, also check whether any matching code is actually **registered as a callable MCP tool** (exposed in the server's tool list) versus dead code that exists but isn't reachable — a permission backed only by unreachable code should be reported as unused.

---

## Do NOT

- Do NOT remove, comment out, or modify any code — this is a read-only audit.
- Do NOT change any Meta app configuration or permission requests — Hedgehog will decide what to drop/keep based on your report.
- Do NOT commit anything.

---

## How to report back

For each of the 5 permissions, state clearly:

- **In use / not in use** (based on the grep results and tool registration check)
- If in use: the file(s) and function/line references backing that permission, and which MCP tool(s) expose it
- If not in use: confirm the grep found nothing relevant, and note the exact patterns tried so it's clear the absence is real rather than a search miss

No code changes, no verification steps beyond the report itself.

---

## Context

Hedgehog Marketing's Meta App Review request for this app was partially rejected — 8 permissions were approved (covering ads management, Page metadata, and leads) and 9 were rejected because the submitted screencast never demonstrated them in use. Before recording a new screencast to win these back, Hedgehog wants to confirm which are worth the effort versus which should simply be dropped from the next submission because the feature isn't built yet.
