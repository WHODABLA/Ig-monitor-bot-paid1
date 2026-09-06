# telegram_bot.py - FINAL WORKING VERSION
# Instagram Unban Monitor for Telegram
# Uses JobQueue for background checks, supports multiple owners, cooldown, and DB-first recovery marking.

import os
import re
import asyncio
import json
import time
from datetime import datetime, timezone
from collections import deque

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
import aiohttp
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

load_dotenv()

# ---------- Configuration ----------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
PORT = int(os.getenv("PORT", "8080"))

D1_WORKER_URL = os.getenv("D1_WORKER_URL", "").rstrip("/")
if D1_WORKER_URL and not D1_WORKER_URL.startswith(("http://", "https://")):
    D1_WORKER_URL = f"https://{D1_WORKER_URL}"
D1_API_KEY = os.getenv("D1_API_KEY", "")

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
STABLE_API_HOST = os.getenv("STABLE_API_HOST", "instagram-scraper-stable-api.p.rapidapi.com")
STABLE_API_URL = f"https://{STABLE_API_HOST}/ig_get_fb_profile.php"
INSTAGRAM120_HOST = os.getenv("INSTAGRAM120_HOST", "instagram120.p.rapidapi.com")
INSTAGRAM120_URL = f"https://{INSTAGRAM120_HOST}/api/instagram/profile"

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "apify~instagram-profile-scraper")
APIFY_URL = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/run-sync-get-dataset-items?token={APIFY_TOKEN}"

APIFY_POST_ACTOR = os.getenv("APIFY_POST_ACTOR", "apify~instagram-post-scraper")
APIFY_POST_URL = f"https://api.apify.com/v2/acts/{APIFY_POST_ACTOR}/run-sync-get-dataset-items?token={APIFY_TOKEN}"

# ---------- Owner IDs (support comma-separated list) ----------
OWNER_USER_IDS = set()
raw_owner = os.getenv("OWNER_USER_ID", "").strip()
if raw_owner:
    if "," in raw_owner:
        try:
            OWNER_USER_IDS = {int(x.strip()) for x in raw_owner.split(",") if x.strip()}
        except ValueError:
            print("ERROR: OWNER_USER_ID contains non‑integer values.", flush=True)
            raise SystemExit("Invalid OWNER_USER_ID")
    else:
        try:
            OWNER_USER_IDS = {int(raw_owner)}
        except ValueError:
            print("ERROR: OWNER_USER_ID is not a valid integer.", flush=True)
            raise SystemExit("Invalid OWNER_USER_ID")
else:
    print("ERROR: OWNER_USER_ID not set.", flush=True)
    raise SystemExit("OWNER_USER_ID must be set.")

# ---------- Cooldown to prevent duplicate notifications ----------
last_notified = {}  # username -> datetime
NOTIFICATION_COOLDOWN_SECONDS = 600  # 10 minutes

# ---------- Notification queue ----------
notification_queue = deque()
is_processing_notifications = False

# ---------- Keep-alive Flask ----------
keep_alive_app = Flask(__name__)

@keep_alive_app.route("/")
def home():
    return "Instagram Unban Monitor (Telegram) is running."

def run_keep_alive():
    keep_alive_app.run(host="0.0.0.0", port=PORT)

def start_keep_alive():
    t = Thread(target=run_keep_alive)
    t.daemon = True
    t.start()

# ---------- D1 API Helpers ----------
def _d1_headers():
    return {
        "Authorization": f"Bearer {D1_API_KEY}",
        "Content-Type": "application/json",
    }

async def api_get_tracked() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/tracked", headers=_d1_headers()) as resp:
            if resp.status != 200:
                body = (await resp.text())[:500]
                print(f"api_get_tracked failed: HTTP {resp.status} — {body}", flush=True)
                return {}
            rows = await resp.json()
            return {
                row["username"]: {
                    "start_time": row["start_time"],
                    "recovered": bool(row.get("recovered", False)),
                    "recovered_at": row.get("recovered_at"),
                    "track_type": row.get("track_type") or "recovery",
                    "banned": bool(row.get("banned", False)),
                    "banned_at": row.get("banned_at"),
                    "fail_count": row.get("fail_count", 0) or 0,
                    "post_stats": row.get("post_stats") or "",
                }
                for row in rows
            }

async def api_add_tracked(username: str, start_time: str, track_type: str = "recovery", post_stats: str = "") -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked",
                headers=_d1_headers(),
                json={"username": username, "start_time": start_time, "track_type": track_type, "post_stats": post_stats},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_add_tracked({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_add_tracked({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False

async def api_mark_recovered(username: str, recovered_at: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/update",
                headers=_d1_headers(),
                json={"username": username, "recovered_at": recovered_at},
            ) as resp:
                body = await resp.text()
                print(f"[api_mark_recovered] {username} -> {resp.status} {body}", flush=True)
                if resp.status != 200:
                    return False
                return True
    except Exception as e:
        print(f"[api_mark_recovered] exception: {e}", flush=True)
        return False

async def api_mark_banned(username: str, banned_at: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/update",
                headers=_d1_headers(),
                json={"username": username, "banned_at": banned_at},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_mark_banned({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_mark_banned({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False

async def api_set_fail_count(username: str, fail_count: int) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/update",
                headers=_d1_headers(),
                json={"username": username, "fail_count": fail_count},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_set_fail_count({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_set_fail_count({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False

async def api_remove_tracked(username: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/delete",
                headers=_d1_headers(),
                json={"username": username},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_remove_tracked({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_remove_tracked({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False

async def api_get_config() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/config", headers=_d1_headers()) as resp:
            if resp.status != 200:
                body = (await resp.text())[:500]
                print(f"api_get_config failed: HTTP {resp.status} — {body}", flush=True)
                return {}
            return await resp.json()

async def api_set_config(key: str, value) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/config", headers=_d1_headers(), json={key: value}
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_set_config({key}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_set_config({key}) failed: {type(e).__name__}: {e}", flush=True)
        return False

# ---------- Notification handling ----------
async def send_telegram_message(chat_id: int, text: str) -> bool:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await application.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
            return True
        except Exception as e:
            print(f"Failed to send message (attempt {attempt+1}): {e}", flush=True)
            if "retry after" in str(e).lower():
                import re
                match = re.search(r"retry after (\d+)", str(e))
                if match:
                    retry_after = int(match.group(1)) + 1
                    await asyncio.sleep(retry_after)
                else:
                    await asyncio.sleep(5)
            else:
                return False
    return False

async def process_notification_queue():
    global is_processing_notifications
    if is_processing_notifications:
        return
    is_processing_notifications = True
    try:
        while notification_queue:
            chat_id, text, label = notification_queue.popleft()
            success = await send_telegram_message(chat_id, text)
            if success:
                print(f"✅ Notification sent for {label}")
            else:
                print(f"❌ Failed to send notification for {label}")
            await asyncio.sleep(2)
    finally:
        is_processing_notifications = False

def queue_notification(chat_id: int, text: str, label: str):
    notification_queue.append((chat_id, text, label))
    asyncio.create_task(process_notification_queue())

# ---------- Instagram checks (same as Discord) ----------
async def _check_via_apify(username: str):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                APIFY_URL,
                headers={"Content-Type": "application/json"},
                json={
                    "inputUrl": f"https://www.instagram.com/{username}",
                    "usernames": [username],
                },
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                if resp.status not in (200, 201):
                    print(f"[apify] @{username}: HTTP {resp.status}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                if not data:
                    print(f"[apify] @{username}: empty response", flush=True)
                    return None
                if isinstance(data, list):
                    if len(data) == 0:
                        print(f"[apify] @{username}: empty dataset", flush=True)
                        return None
                    item = data[0]
                else:
                    item = data
                if isinstance(item, dict):
                    if "error" in item and item["error"]:
                        print(f"[apify] @{username}: error: {item['error']}", flush=True)
                        return None
                    if "errorDescription" in item and item["errorDescription"]:
                        print(f"[apify] @{username}: error: {item['errorDescription']}", flush=True)
                        return None
                    if item.get("status") == "error":
                        print(f"[apify] @{username}: error status", flush=True)
                        return None
                    if not item.get("username"):
                        print(f"[apify] @{username}: no username in response", flush=True)
                        return None
                    if item.get("followersCount") is None and item.get("postsCount") is None:
                        print(f"[apify] @{username}: null data fields", flush=True)
                        return None
                result_username = item.get("username") or item.get("user", {}).get("username")
                if not result_username:
                    print(f"[apify] @{username}: no username found", flush=True)
                    return None
                return {
                    "username": result_username,
                    "full_name": item.get("fullName") or item.get("full_name") or "",
                    "followers": item.get("followersCount") or item.get("followerCount") or 0,
                    "following": item.get("followsCount") or item.get("followingCount") or 0,
                    "posts": item.get("postsCount") or item.get("mediaCount") or 0,
                    "profile_pic_url": item.get("profilePicUrlHD") or item.get("profilePicUrl") or "",
                    "is_verified": bool(item.get("verified") or item.get("isVerified") or False),
                }
    except asyncio.TimeoutError:
        print(f"[apify] @{username}: TIMEOUT", flush=True)
        return None
    except Exception as e:
        print(f"[apify] @{username}: exception: {type(e).__name__}: {e}", flush=True)
        return None

async def _check_via_instagram120(username: str):
    headers = {
        "Content-Type": "application/json",
        "x-rapidapi-host": INSTAGRAM120_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INSTAGRAM120_URL,
                headers=headers,
                json={"username": username},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    print(f"[instagram120] @{username}: HTTP {resp.status}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                if not data:
                    print(f"[instagram120] @{username}: empty response", flush=True)
                    return None
                if data.get("error") or data.get("message"):
                    print(f"[instagram120] @{username}: error: {data.get('error') or data.get('message')}", flush=True)
                    return None
                result = data.get("result")
                if not result:
                    print(f"[instagram120] @{username}: no result", flush=True)
                    return None
                if "username" not in result or not result["username"]:
                    print(f"[instagram120] @{username}: no username", flush=True)
                    return None
                return {
                    "username": result.get("username", username),
                    "full_name": result.get("full_name") or "",
                    "followers": result.get("edge_followed_by", {}).get("count", 0),
                    "following": result.get("edge_follow", {}).get("count", 0),
                    "posts": result.get("edge_owner_to_timeline_media", {}).get("count", 0),
                    "profile_pic_url": result.get("profile_pic_url_hd") or result.get("profile_pic_url") or "",
                    "is_verified": bool(result.get("is_verified", False)),
                }
    except asyncio.TimeoutError:
        print(f"[instagram120] @{username}: TIMEOUT", flush=True)
        return None
    except Exception as e:
        print(f"[instagram120] @{username}: exception: {type(e).__name__}: {e}", flush=True)
        return None

async def check_instagram_status(username: str):
    print(f"🔍 Checking @{username}...", flush=True)
    info = await _check_via_apify(username)
    if info is not None:
        print(f"✅ @{username} found via Apify", flush=True)
        return info
    print(f"🔄 @{username}: Apify failed, trying instagram120...", flush=True)
    info = await _check_via_instagram120(username)
    if info is not None:
        print(f"✅ @{username} found via instagram120", flush=True)
        return info
    print(f"❌ @{username}: NOT FOUND (banned/suspended)", flush=True)
    return None

def _normalize_post_url(url: str) -> str:
    url = url.strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return f"https://www.instagram.com/p/{url}/"

async def check_post_status(url: str):
    if not APIFY_POST_ACTOR:
        print("check_post_status: APIFY_POST_ACTOR not set — cannot check posts.", flush=True)
        return None
    full_url = _normalize_post_url(url)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                APIFY_POST_URL,
                headers={"Content-Type": "application/json"},
                json={
                    "directUrls": [full_url],
                    "postUrls": [full_url],
                    "urls": [full_url],
                },
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                if resp.status not in (200, 201):
                    body = (await resp.text())[:300]
                    print(f"[apify-post] {full_url}: HTTP {resp.status} — {body}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                if not data:
                    print(f"[apify-post] {full_url}: empty response", flush=True)
                    return None
                item = data[0] if isinstance(data, list) else data
                if not isinstance(item, dict):
                    print(f"[apify-post] {full_url}: unexpected response shape — {data}", flush=True)
                    return None
                if item.get("status") == "unavailable" or item.get("error"):
                    print(f"[apify-post] {full_url}: reported unreachable — {item}", flush=True)
                    return {
                        "available": False,
                        "owner_username": item.get("author") or item.get("ownerUsername"),
                        "owner_full_name": item.get("authorFullName") or item.get("ownerFullName"),
                        "likes": item.get("likes") or 0,
                        "comments": item.get("comments") or 0,
                        "views": item.get("views") or 0,
                        "caption": item.get("caption") or "",
                        "shortcode": item.get("shortcode") or "",
                    }
                return {
                    "available": True,
                    "id": item.get("mediaId") or item.get("id"),
                    "shortcode": item.get("shortcode") or "",
                    "owner_username": item.get("author") or item.get("ownerUsername") or "",
                    "owner_full_name": item.get("authorFullName") or item.get("ownerFullName") or "",
                    "owner_id": item.get("authorId") or item.get("ownerId") or "",
                    "likes": item.get("likes") or 0,
                    "comments": item.get("comments") or 0,
                    "views": item.get("views") or 0,
                    "caption": item.get("caption") or "",
                    "is_video": item.get("isVideo", False),
                    "date_posted": item.get("datePosted") or "",
                    "author_url": item.get("authorUrl") or "",
                }
    except asyncio.TimeoutError:
        print(f"[apify-post] {full_url}: TIMEOUT", flush=True)
        return None
    except Exception as e:
        print(f"[apify-post] {full_url}: exception: {type(e).__name__}: {e}", flush=True)
        return None

# ---------- Helper formatting ----------
def format_elapsed(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)

def format_elapsed_long(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} hours, {minutes} minutes, {seconds} seconds"

def format_removed_time(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def build_recovery_text(info: dict, start_iso: str) -> str:
    lines = [
        f"🎉 *Account Recovered!*",
        f"",
        f"**@{info['username']}** is back! 🏆✅",
        f"",
        f"📊 *Stats:*",
        f"• Followers: {info['followers']:,}",
        f"• Following: {info['following']:,}",
        f"• Posts: {info['posts']:,}",
        f"• Verified: {'✅' if info['is_verified'] else '❌'}",
        f"",
        f"⏱️ *Time taken:* {format_elapsed_long(start_iso)}",
        f"",
        f"🔗 https://instagram.com/{info['username']}",
    ]
    return "\n".join(lines)

def build_ban_text(username: str, start_iso: str) -> str:
    lines = [
        f"🚫 *Account Banned!*",
        f"",
        f"**@{username}** has been banned! 🚫❌",
        f"",
        f"⏱️ *Banned after:* {format_elapsed_long(start_iso)}",
        f"",
        f"🔗 https://instagram.com/{username}",
    ]
    return "\n".join(lines)

def parse_post_stats(stats_str: str) -> dict:
    if not stats_str:
        return {}
    try:
        return json.loads(stats_str)
    except:
        return {}

def build_post_removed_text(url: str, start_iso: str, post_data: dict = None, stored_stats: dict = None) -> str:
    time_str = format_removed_time(start_iso)
    lines = [
        f"🚫 *Post/Story Removed!*",
        f"",
        f"This content is no longer available — likely removed or taken down. 🚫❌",
        f"",
        f"⏱️ *Removed in:* {time_str}",
        f"",
    ]
    stats_data = stored_stats if stored_stats else post_data
    if stats_data:
        owner = stats_data.get('owner_username') or stats_data.get('author')
        if owner:
            lines.append(f"👤 *Owner:* @{owner}")
        stats = []
        likes = stats_data.get('likes', 0)
        if likes:
            stats.append(f"❤️ {likes:,}")
        comments = stats_data.get('comments', 0)
        if comments:
            stats.append(f"💬 {comments:,}")
        views = stats_data.get('views', 0)
        if views:
            stats.append(f"👁️ {views:,}")
        if stats:
            lines.append(f"📊 *Stats:* {' | '.join(stats)}")
        caption = stats_data.get('caption', '')
        if caption:
            if len(caption) > 200:
                caption = caption[:197] + "..."
            lines.append(f"📝 *Caption:* {caption}")
    lines.append("")
    lines.append(f"🔗 {url}")
    return "\n".join(lines)

# ---------- Owner check ----------
def is_owner(update: Update) -> bool:
    if not update.effective_user:
        return False
    return update.effective_user.id in OWNER_USER_IDS

# ---------- Background check ----------
async def check_tracked_accounts(context: ContextTypes.DEFAULT_TYPE = None):
    """
    The main monitoring loop. Called by JobQueue every CHECK_INTERVAL_MINUTES minutes.
    If context is None (manual call), we run without it.
    """
    try:
        tracked = await api_get_tracked()
        if not tracked:
            print("check_tracked_accounts: nothing tracked, skipping.", flush=True)
            return

        config = await api_get_config()
        chat_id = config.get("notify_chat_id")
        if not chat_id:
            print("check_tracked_accounts: no notify chat set, skipping.", flush=True)
            return
        chat_id = int(chat_id)

        active_items = {}
        for username, meta in tracked.items():
            track_type = meta.get("track_type", "recovery")
            if track_type == "ban" and meta.get("banned", False):
                continue
            if track_type == "post" and meta.get("banned", False):
                continue
            if track_type == "recovery" and meta.get("recovered", False):
                continue
            active_items[username] = meta

        if not active_items:
            print("check_tracked_accounts: no active items to check, skipping.", flush=True)
            return

        print(f"🔍 Checking {len(active_items)} active item(s)...", flush=True)

        for username, meta in active_items.items():
            track_type = meta.get("track_type", "recovery")
            print(f"🔍 Checking {username} ({track_type})...", flush=True)

            info = None
            if track_type in ("ban", "recovery"):
                info = await check_instagram_status(username)

            # ---------- POST/STORY MONITORING ----------
            if track_type == "post":
                result = await check_post_status(username)
                if result is not None and result.get("available") is False:
                    fail_count = meta.get("fail_count", 0) + 1
                    print(f"📊 Post {username} appears removed (check {fail_count}/2)", flush=True)
                    if fail_count >= 2:
                        print(f"🚨 REMOVAL CONFIRMED for post {username}!", flush=True)
                        stored_stats = parse_post_stats(meta.get("post_stats", ""))
                        text = build_post_removed_text(username, meta["start_time"], result, stored_stats)
                        queue_notification(chat_id, text, username)
                        ok = await api_mark_banned(username, datetime.now(timezone.utc).isoformat())
                        if not ok:
                            print(f"⚠️ Sent removal notification but DB save failed for {username}", flush=True)
                    else:
                        await api_set_fail_count(username, fail_count)
                        print(f"⏳ Post {username} unreachable (check {fail_count}/2) — waiting for confirmation.", flush=True)
                elif result is not None and result.get("available") is True and meta.get("fail_count", 0) > 0:
                    await api_set_fail_count(username, 0)
                    print(f"✅ Post {username} is reachable again, reset fail_count to 0", flush=True)

            # ---------- BAN MONITORING ----------
            elif track_type == "ban":
                if info is None:
                    fail_count = meta.get("fail_count", 0) + 1
                    print(f"📊 @{username} unreachable (check {fail_count}/2)", flush=True)
                    if fail_count >= 2:
                        print(f"🚨 BAN CONFIRMED for @{username}!", flush=True)
                        text = build_ban_text(username, meta["start_time"])
                        queue_notification(chat_id, text, username)
                        ok = await api_mark_banned(username, datetime.now(timezone.utc).isoformat())
                        if ok:
                            print(f"✅ Banned notification sent for @{username}", flush=True)
                        else:
                            print(f"⚠️ Sent ban notification but DB save failed for @{username}", flush=True)
                    else:
                        await api_set_fail_count(username, fail_count)
                        print(f"⏳ @{username} unreachable (check {fail_count}/2) — waiting for confirmation.", flush=True)
                else:
                    if meta.get("fail_count", 0) > 0:
                        await api_set_fail_count(username, 0)
                        print(f"✅ @{username} is reachable again, reset fail_count to 0", flush=True)

            # ---------- RECOVERY MONITORING ----------
            elif track_type == "recovery":
                if info is not None and not meta.get("recovered", False):
                    # Cooldown check
                    now = datetime.now(timezone.utc)
                    last = last_notified.get(username)
                    if last and (now - last).total_seconds() < NOTIFICATION_COOLDOWN_SECONDS:
                        print(f"⏳ @{username} recently notified, skipping duplicate.", flush=True)
                        # Still try to mark recovered if not already
                        if not meta.get("recovered"):
                            await api_mark_recovered(username, now.isoformat())
                        continue

                    print(f"🎉 RECOVERY DETECTED for @{username}!", flush=True)

                    # 1. Mark recovered FIRST
                    ok = await api_mark_recovered(username, now.isoformat())
                    if ok:
                        print(f"✅ Marked @{username} as recovered in DB.", flush=True)
                    else:
                        print(f"⚠️ Failed to mark @{username} as recovered, but still sending notification.", flush=True)

                    # 2. Send notification (only once per cooldown)
                    last_notified[username] = now
                    text = build_recovery_text(info, meta["start_time"])
                    queue_notification(chat_id, text, username)

            await asyncio.sleep(1)

    except Exception as e:
        print(f"check_tracked_accounts: UNHANDLED ERROR: {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()

# ---------- Command Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    await update.message.reply_text("👋 Instagram Unban Monitor (Telegram)\nUse /help to see commands.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    help_text = (
        "*Available commands:*\n\n"
        "/track <username> - start tracking for recovery\n"
        "/untrack <username> - stop tracking recovery\n"
        "/list - show recovery tracked\n"
        "/ban <username> - monitor for ban\n"
        "/unban <username> - stop ban monitoring\n"
        "/banlist - show ban monitored\n"
        "/trackpost <url> - monitor post for removal\n"
        "/untrackpost <url> - stop post monitoring\n"
        "/postbanlist - show post removal monitored\n"
        "/checkpost <url> - debug check post status\n"
        "/checknow <username> - debug check account\n"
        "/setchannel - set this chat as notification destination\n"
        "/help - show this message"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def checknow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /checknow <username>")
        return
    username = context.args[0].lstrip("@").strip()
    await update.message.reply_text(f"🔍 Checking @{username}...")
    info = await check_instagram_status(username)
    if info:
        reply = (
            f"✅ *@{info['username']}* is LIVE — "
            f"Followers: {info['followers']:,} | "
            f"Posts: {info['posts']:,} | "
            f"Verified: {'✅' if info['is_verified'] else '❌'}"
        )
        await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ *@{username}* is NOT reachable (banned/suspended/not found).", parse_mode=ParseMode.MARKDOWN)

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /track <username>")
        return
    username = context.args[0].lstrip("@").strip()
    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        await update.message.reply_text(f"❌ `{username}` doesn't look like a valid Instagram username.")
        return
    tracked = await api_get_tracked()
    if username in tracked and not tracked[username].get("recovered"):
        await update.message.reply_text(f"⚠️ Already tracking @{username}.")
        return
    ok = await api_add_tracked(username, datetime.now(timezone.utc).isoformat(), track_type="recovery")
    if ok:
        await update.message.reply_text(f"⏱️ Started tracking *@{username}*. I'll post here when it's back.", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Failed to start tracking @{username} — database error.")

async def untrack(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /untrack <username>")
        return
    username = context.args[0].lstrip("@").strip()
    tracked = await api_get_tracked()
    if username in tracked:
        ok = await api_remove_tracked(username)
        if ok:
            await update.message.reply_text(f"✅ Stopped tracking @{username}.")
        else:
            await update.message.reply_text(f"❌ Failed to remove @{username} — database error.")
    else:
        await update.message.reply_text(f"❌ @{username} isn't being tracked.")

async def list_tracked(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    tracked = await api_get_tracked()
    recovery_only = {u: m for u, m in tracked.items() if m.get("track_type", "recovery") == "recovery"}
    if not recovery_only:
        await update.message.reply_text("📭 Nothing is being tracked for recovery right now.")
        return
    pending = {u: m for u, m in recovery_only.items() if not m.get("recovered")}
    recovered = {u: m for u, m in recovery_only.items() if m.get("recovered")}
    lines = [
        "📊 *Tracked Accounts (Recovery):*",
        f"Active: {len(pending)} | Recovered: {len(recovered)}",
        "─" * 20,
    ]
    if pending:
        lines.append("")
        lines.append("*Currently Tracking:*")
        for username, meta in pending.items():
            lines.append(f"`{username}` — ⏳ {format_elapsed(meta['start_time'])}")
    if recovered:
        lines.append("")
        lines.append("*Recovered:*")
        for username, meta in recovered.items():
            lines.append(f"`{username}` — ✅")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /ban <username>")
        return
    username = context.args[0].lstrip("@").strip()
    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        await update.message.reply_text(f"❌ `{username}` doesn't look like a valid Instagram username.")
        return
    tracked = await api_get_tracked()
    if username in tracked and not tracked[username].get("banned"):
        await update.message.reply_text(f"⚠️ Already monitoring @{username} for bans.")
        return
    ok = await api_add_tracked(username, datetime.now(timezone.utc).isoformat(), track_type="ban")
    if ok:
        await update.message.reply_text(f"🚫 Started monitoring *@{username}* for bans. I'll post here if it goes down.", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Failed to start monitoring @{username} — database error.")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /unban <username>")
        return
    username = context.args[0].lstrip("@").strip()
    tracked = await api_get_tracked()
    if username in tracked:
        ok = await api_remove_tracked(username)
        if ok:
            await update.message.reply_text(f"✅ Stopped monitoring @{username} for bans.")
        else:
            await update.message.reply_text(f"❌ Failed to remove @{username} — database error.")
    else:
        await update.message.reply_text(f"❌ @{username} isn't being tracked.")

async def banlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    tracked = await api_get_tracked()
    ban_only = {u: m for u, m in tracked.items() if m.get("track_type", "recovery") == "ban"}
    if not ban_only:
        await update.message.reply_text("📭 Nothing is being monitored for bans right now.")
        return
    active = {u: m for u, m in ban_only.items() if not m.get("banned")}
    banned = {u: m for u, m in ban_only.items() if m.get("banned")}
    lines = [
        "🚫 *Ban Monitoring:*",
        f"Active: {len(active)} | Banned: {len(banned)}",
        "─" * 20,
    ]
    if active:
        lines.append("")
        lines.append("*Currently Monitoring:*")
        for username, meta in active.items():
            lines.append(f"`{username}` — 📡 {format_elapsed(meta['start_time'])}")
    if banned:
        lines.append("")
        lines.append("*Banned:*")
        for username, meta in banned.items():
            lines.append(f"`{username}` — 🚫")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def checkpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /checkpost <url>")
        return
    url = context.args[0].strip()
    await update.message.reply_text(f"🔍 Checking post: {url}")
    result = await check_post_status(url)
    if result is None:
        await update.message.reply_text(
            f"⚠️ Couldn't determine status (check failed, not proof of removal): {url}\n"
            "If `APIFY_POST_ACTOR` isn't set yet, that's why — see the setup notes."
        )
    elif result.get("available") is True:
        stats = []
        if result.get('likes'):
            stats.append(f"❤️ {result['likes']:,}")
        if result.get('comments'):
            stats.append(f"💬 {result['comments']:,}")
        if result.get('views'):
            stats.append(f"👁️ {result['views']:,}")
        stats_msg = " | ".join(stats) if stats else "No stats available"
        owner = result.get('owner_username') or "Unknown"
        caption = result.get('caption', '')
        if caption and len(caption) > 100:
            caption = caption[:97] + "..."
        reply = f"✅ *Still up:* {url}\n"
        reply += f"👤 *Owner:* @{owner}\n"
        reply += f"📊 *Stats:* {stats_msg}"
        if caption:
            reply += f"\n📝 *Caption:* {caption}"
        await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)
    else:
        stats = []
        if result.get('likes'):
            stats.append(f"❤️ {result['likes']:,}")
        if result.get('comments'):
            stats.append(f"💬 {result['comments']:,}")
        if result.get('views'):
            stats.append(f"👁️ {result['views']:,}")
        stats_msg = " | ".join(stats) if stats else "No stats available"
        owner = result.get('owner_username') or "Unknown"
        reply = f"🚫 *Appears removed/unavailable:* {url}\n"
        reply += f"👤 *Owner:* @{owner}\n"
        reply += f"📊 *Last known stats:* {stats_msg}"
        await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)

async def trackpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /trackpost <url>")
        return
    url = context.args[0].strip()
    if not url.startswith(("http://", "https://")):
        await update.message.reply_text("❌ Please provide the full URL (starting with https://) of the post/reel/story.")
        return
    tracked = await api_get_tracked()
    if url in tracked and not tracked[url].get("banned"):
        await update.message.reply_text("⚠️ Already monitoring this content.")
        return
    await update.message.reply_text(f"📊 Fetching stats for post...")
    result = await check_post_status(url)
    stats_json = ""
    if result and result.get("available") is True:
        stats_json = json.dumps({
            "owner_username": result.get("owner_username", ""),
            "owner_full_name": result.get("owner_full_name", ""),
            "likes": result.get("likes", 0),
            "comments": result.get("comments", 0),
            "views": result.get("views", 0),
            "caption": result.get("caption", ""),
        })
        await update.message.reply_text(
            f"📊 Stats saved for this post:\n"
            f"👤 Owner: @{result.get('owner_username', 'Unknown')}\n"
            f"❤️ {result.get('likes', 0):,} | 💬 {result.get('comments', 0):,} | 👁️ {result.get('views', 0):,}"
        )
    else:
        await update.message.reply_text("⚠️ Couldn't fetch stats for this post. It may already be removed or unavailable.")
    ok = await api_add_tracked(url, datetime.now(timezone.utc).isoformat(), track_type="post", post_stats=stats_json)
    if ok:
        await update.message.reply_text(f"🚫 Started monitoring this content for removal. I'll post here if it goes down.")
    else:
        await update.message.reply_text("❌ Failed to start monitoring — database error.")

async def untrackpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /untrackpost <url>")
        return
    url = context.args[0].strip()
    tracked = await api_get_tracked()
    if url in tracked:
        ok = await api_remove_tracked(url)
        if ok:
            await update.message.reply_text("✅ Stopped monitoring this content.")
        else:
            await update.message.reply_text("❌ Failed to remove — database error.")
    else:
        await update.message.reply_text("❌ That URL isn't being tracked.")

async def postbanlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    tracked = await api_get_tracked()
    post_only = {u: m for u, m in tracked.items() if m.get("track_type", "recovery") == "post"}
    if not post_only:
        await update.message.reply_text("📭 Nothing is being monitored for post/story removal right now.")
        return
    active = {u: m for u, m in post_only.items() if not m.get("banned")}
    removed = {u: m for u, m in post_only.items() if m.get("banned")}
    lines = [
        "🚫 *Post/Story Removal Monitoring:*",
        f"Active: {len(active)} | Removed: {len(removed)}",
        "─" * 20,
    ]
    if active:
        lines.append("")
        lines.append("*Currently Monitoring:*")
        for url, meta in active.items():
            display_url = url
            if "instagram.com/p/" in url:
                shortcode = url.split("/p/")[1].split("/")[0].split("?")[0]
                display_url = f"instagram.com/p/{shortcode}"
            elif "instagram.com/reel/" in url:
                shortcode = url.split("/reel/")[1].split("/")[0].split("?")[0]
                display_url = f"instagram.com/reel/{shortcode}"
            lines.append(f"`{display_url}` — 📡 {format_elapsed(meta['start_time'])}")
    if removed:
        lines.append("")
        lines.append("*Removed:*")
        for url, meta in removed.items():
            display_url = url
            if "instagram.com/p/" in url:
                shortcode = url.split("/p/")[1].split("/")[0].split("?")[0]
                display_url = f"instagram.com/p/{shortcode}"
            elif "instagram.com/reel/" in url:
                shortcode = url.split("/reel/")[1].split("/")[0].split("?")[0]
                display_url = f"instagram.com/reel/{shortcode}"
            lines.append(f"`{display_url}` — 🚫")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def setchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("❌ Only the bot owner can use this bot.")
        return
    chat_id = update.effective_chat.id
    ok = await api_set_config("notify_chat_id", str(chat_id))
    if ok:
        config = await api_get_config()
        saved_id = config.get("notify_chat_id")
        if str(saved_id) == str(chat_id):
            await update.message.reply_text(f"✅ Notifications will be posted in this chat.")
        else:
            await update.message.reply_text(f"⚠️ Save request succeeded but database shows different value.")
    else:
        await update.message.reply_text("❌ Failed to save the chat — database error.")

# ---------- Main ----------
application = None

def main():
    global application
    if not TELEGRAM_TOKEN:
        raise SystemExit("TELEGRAM_TOKEN not set.")
    if not OWNER_USER_IDS:
        raise SystemExit("OWNER_USER_IDS is empty.")
    if not D1_WORKER_URL or not D1_API_KEY:
        raise SystemExit("D1_WORKER_URL and D1_API_KEY must be set.")
    if not RAPIDAPI_KEY:
        raise SystemExit("RAPIDAPI_KEY not set.")

    start_keep_alive()

    # Build application
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Clear webhook to avoid conflicts
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.bot.delete_webhook(drop_pending_updates=True))
    loop.close()
    time.sleep(2)

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("checknow", checknow))
    application.add_handler(CommandHandler("track", track))
    application.add_handler(CommandHandler("untrack", untrack))
    application.add_handler(CommandHandler("list", list_tracked))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    application.add_handler(CommandHandler("banlist", banlist))
    application.add_handler(CommandHandler("checkpost", checkpost))
    application.add_handler(CommandHandler("trackpost", trackpost))
    application.add_handler(CommandHandler("untrackpost", untrackpost))
    application.add_handler(CommandHandler("postbanlist", postbanlist))
    application.add_handler(CommandHandler("setchannel", setchannel))

    # Schedule background checks using JobQueue
    if application.job_queue:
        application.job_queue.run_repeating(
            check_tracked_accounts,
            interval=CHECK_INTERVAL_MINUTES * 60,
            first=0
        )
        print(f"✅ Scheduled checks every {CHECK_INTERVAL_MINUTES} minutes.", flush=True)
    else:
        print("⚠️ JobQueue not available – falling back to manual loop.", flush=True)
        # Fallback: create a background task manually
        import asyncio
        asyncio.create_task(check_tracked_accounts())

    # Start polling
    print("Bot started. Polling...", flush=True)
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()