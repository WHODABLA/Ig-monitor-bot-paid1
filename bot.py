"""
Instagram Unban Monitor — Discord Bot
--------------------------------------
Tracks Instagram accounts you flag as "banned" and posts a Discord
notification with full stats the moment they become reachable again.

- Only the server owner can use any command.
- Storage: Cloudflare Worker + D1 database (survives restarts).
- Instagram checking: via a RapidAPI Instagram profile-lookup service
  (instagram120) — no login, no proxy, no checkpoints. This service
  handles Instagram-side blocking on their end, so requests here are
  simple authenticated calls to RapidAPI itself.

  IMPORTANT: the free RapidAPI plan has a monthly request cap (check your
  specific plan). Set CHECK_INTERVAL_MINUTES generously enough that
  (30 days * 24h * 60min / CHECK_INTERVAL_MINUTES) * number_of_tracked_accounts
  stays under your monthly limit. E.g. with a 250/month cap and 1-2
  accounts tracked, checking every 3 hours (180 min) uses ~240/month.

Commands (slash commands, server owner only):
  /track username        - start tracking an account (starts the timer)
  /untrack username      - stop tracking an account
  /list                  - show everything currently being tracked
  /setchannel            - set the channel this bot posts recovery alerts to
  /checknow username     - debug: immediately check one account

Setup: see README.md
"""

import os
import asyncio
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiohttp
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "180"))
PORT = int(os.getenv("PORT", "8080"))

D1_WORKER_URL = os.getenv("D1_WORKER_URL", "").rstrip("/")
D1_API_KEY = os.getenv("D1_API_KEY", "")

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
STABLE_API_HOST = os.getenv("STABLE_API_HOST", "instagram-scraper-stable-api.p.rapidapi.com")
STABLE_API_URL = f"https://{STABLE_API_HOST}/ig_get_fb_profile.php"
INSTAGRAM120_HOST = os.getenv("INSTAGRAM120_HOST", "instagram120.p.rapidapi.com")
INSTAGRAM120_URL = f"https://{INSTAGRAM120_HOST}/api/instagram/profile"

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "apify~instagram-profile-scraper")
APIFY_URL = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/run-sync-get-dataset-items?token={APIFY_TOKEN}"

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


# ---------- owner-only check ----------

def is_owner():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message("This command only works in a server.", ephemeral=True)
            return False
        if interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message("❌ Only the server owner can use this bot.", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


# ---------- keep-alive web server (for Render free tier + UptimeRobot) ----------

keep_alive_app = Flask(__name__)


@keep_alive_app.route("/")
def home():
    return "Instagram Unban Monitor is running."


def run_keep_alive():
    keep_alive_app.run(host="0.0.0.0", port=PORT)


def start_keep_alive():
    t = Thread(target=run_keep_alive)
    t.daemon = True
    t.start()


# ---------- D1 storage (via Cloudflare Worker API) ----------

def _d1_headers():
    return {
        "Authorization": f"Bearer {D1_API_KEY}",
        "Content-Type": "application/json",
    }


async def api_get_tracked() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/tracked", headers=_d1_headers()) as resp:
            rows = await resp.json()
            return {
                row["username"]: {
                    "start_time": row["start_time"],
                    "recovered": bool(row["recovered"]),
                    "recovered_at": row.get("recovered_at"),
                }
                for row in rows
            }


async def api_add_tracked(username: str, start_time: str):
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{D1_WORKER_URL}/tracked",
            headers=_d1_headers(),
            json={"username": username, "start_time": start_time},
        )


async def api_mark_recovered(username: str, recovered_at: str):
    async with aiohttp.ClientSession() as session:
        await session.patch(
            f"{D1_WORKER_URL}/tracked/{username}",
            headers=_d1_headers(),
            json={"recovered_at": recovered_at},
        )


async def api_remove_tracked(username: str):
    async with aiohttp.ClientSession() as session:
        await session.delete(f"{D1_WORKER_URL}/tracked/{username}", headers=_d1_headers())


async def api_get_config() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/config", headers=_d1_headers()) as resp:
            return await resp.json()


async def api_set_config(key: str, value) -> bool:
    """Returns True only if the write actually succeeded — callers must
    check this instead of assuming success, since a silent failure here
    previously caused /setchannel to report success when nothing saved."""
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


# ---------- Instagram status check (Apify primary, dual RapidAPI as fallback) ----------

async def _check_via_apify(username: str):
    """Primary provider — real headless-browser-quality data, ~1600
    free checks/month on the $5 free credit. Falls back to RapidAPI
    providers below if this fails or credit runs out."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                APIFY_URL,
                headers={"Content-Type": "application/json"},
                json={"inputUrl": f"https://www.instagram.com/{username}"},
                timeout=aiohttp.ClientTimeout(total=45),  # Apify runs can take longer than a plain API call
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"[apify] @{username}: HTTP {resp.status} — {body}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                if not data or not isinstance(data, list) or len(data) == 0:
                    print(f"[apify] @{username}: 200 OK but empty dataset — {data}", flush=True)
                    return None
                item = data[0]
                if "username" not in item:
                    print(f"[apify] @{username}: no username in item — {item}", flush=True)
                    return None
                return {
                    "username": item.get("username", username),
                    "full_name": item.get("fullName") or "",
                    "followers": item.get("followersCount", 0),
                    "following": item.get("followsCount", 0),
                    "posts": item.get("postsCount", 0),
                    "profile_pic_url": item.get("profilePicUrlHD") or item.get("profilePicUrl") or "",
                    "is_verified": bool(item.get("verified", False)),
                }
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as e:
        print(f"[apify] @{username}: {type(e).__name__}: {e}", flush=True)
        return None


async def _check_via_stable_api(username: str):
    """Primary provider — handles regular/smaller accounts well, not just
    celebrities. Returns dict on success, None on any failure (never raises)."""
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-rapidapi-host": STABLE_API_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                STABLE_API_URL,
                headers=headers,
                data={"username_or_url": username, "data": "basic"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"[stable-api] @{username}: HTTP {resp.status} — {body}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                if not data or "username" not in data:
                    print(f"[stable-api] @{username}: 200 OK but no username in response — {data}", flush=True)
                    return None
                return {
                    "username": data.get("username", username),
                    "full_name": data.get("full_name") or "",
                    "followers": data.get("follower_count", 0),
                    "following": data.get("following_count", 0),
                    "posts": data.get("media_count", 0),
                    "profile_pic_url": (data.get("hd_profile_pic_url_info") or {}).get("url")
                        or data.get("profile_pic_url") or "",
                    "is_verified": bool(data.get("is_verified", False)),
                }
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as e:
        print(f"[stable-api] @{username}: {type(e).__name__}: {e}", flush=True)
        return None


async def _check_via_instagram120(username: str):
    """Backup provider — used only if the primary one fails."""
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
                    body = (await resp.text())[:300]
                    print(f"[instagram120] @{username}: HTTP {resp.status} — {body}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                result = (data or {}).get("result")
                if not result:
                    print(f"[instagram120] @{username}: 200 OK but no result — {data}", flush=True)
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
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as e:
        print(f"[instagram120] @{username}: {type(e).__name__}: {e}", flush=True)
        return None


async def check_instagram_status(username: str):
    """Returns None if the account is unreachable (banned/suspended/not
    found), or if ALL providers failed. Tries Apify first (best data
    quality, highest free quota), then falls back through the two
    RapidAPI providers if needed."""
    info = await _check_via_apify(username)
    if info is not None:
        return info
    print(f"Apify failed for @{username}, trying stable-api...", flush=True)
    info = await _check_via_stable_api(username)
    if info is not None:
        return info
    print(f"stable-api failed for @{username}, trying instagram120...", flush=True)
    return await _check_via_instagram120(username)


# ---------- timer UI ----------

def format_elapsed(start_iso: str) -> str:
    """Short-form elapsed timer, e.g. '2h 15m 30s'."""
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
    """Long-form elapsed timer, e.g. '0 hours, 14 minutes, 41 seconds'."""
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} hours, {minutes} minutes, {seconds} seconds"


def build_recovery_embed(info: dict, start_iso: str) -> discord.Embed:
    embed = discord.Embed(
        title="Monitoring Status",
        description=(
            f"Account Recovered | @{info['username']} 🏆✅ |\n"
            f"Followers: {info['followers']} |⏱️Time taken: {format_elapsed_long(start_iso)}"
        ),
        color=discord.Color.green(),
        url=f"https://instagram.com/{info['username']}",
    )
    return embed


# ---------- background loop ----------

@tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
async def check_tracked_accounts():
    try:
        tracked = await api_get_tracked()
        if not tracked:
            print("check_tracked_accounts: nothing tracked, skipping.", flush=True)
            return

        config = await api_get_config()
        channel_id = config.get("notify_channel_id")
        if not channel_id:
            print("check_tracked_accounts: no notify channel set, skipping.", flush=True)
            return

        channel = bot.get_channel(int(channel_id))
        if channel is None:
            # not in cache yet (can happen right after startup) — fetch directly
            try:
                channel = await bot.fetch_channel(int(channel_id))
            except discord.HTTPException as e:
                print(f"check_tracked_accounts: could not fetch channel {channel_id}: {e}", flush=True)
                return

        print(f"check_tracked_accounts: checking {len(tracked)} account(s)...", flush=True)
        for username, meta in tracked.items():
            if meta.get("recovered"):
                continue
            info = await check_instagram_status(username)
            if info is not None:
                embed = build_recovery_embed(info, meta["start_time"])
                await channel.send(embed=embed)
                await api_mark_recovered(username, datetime.now(timezone.utc).isoformat())
                print(f"✅ Recovered: @{username}", flush=True)
    except Exception as e:
        # CRITICAL: without this catch-all, any single error here would
        # silently kill the entire recurring loop forever — it would never
        # run again until the next manual redeploy, with no visible sign
        # anything was wrong.
        print(f"check_tracked_accounts: UNHANDLED ERROR (loop continues): {type(e).__name__}: {e}", flush=True)


@check_tracked_accounts.error
async def check_tracked_accounts_error(error):
    print(f"check_tracked_accounts: loop-level error: {type(error).__name__}: {error}", flush=True)


@check_tracked_accounts.before_loop
async def before_check():
    await bot.wait_until_ready()


# ---------- slash commands (server owner only) ----------

@bot.event
async def on_ready():
    await bot.tree.sync()
    check_tracked_accounts.start()
    print(f"Logged in as {bot.user} — checking every {CHECK_INTERVAL_MINUTES} min", flush=True)
    if not RAPIDAPI_KEY:
        print("WARNING: RAPIDAPI_KEY not set — checks will fail.", flush=True)
    if not APIFY_TOKEN:
        print("WARNING: APIFY_TOKEN not set — will skip straight to RapidAPI fallbacks.", flush=True)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        return
    print(f"[command error] /{interaction.command.name if interaction.command else '?'}: {error!r}", flush=True)
    message = f"⚠️ Something went wrong: `{error}`"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.tree.command(name="checknow", description="Debug: immediately check an account and show the raw result")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def checknow(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()
    await interaction.response.defer(thinking=True)
    info = await check_instagram_status(username)
    if info:
        await interaction.followup.send(
            f"✅ **@{username}** is LIVE — Followers: {info['followers']:,} | Posts: {info['posts']:,}"
        )
    else:
        await interaction.followup.send(
            f"❌ **@{username}** is NOT reachable — would be treated as still banned."
        )


@bot.tree.command(name="track", description="Start tracking an Instagram account for unban recovery")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def track(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()
    tracked = await api_get_tracked()
    if username in tracked and not tracked[username].get("recovered"):
        await interaction.response.send_message(f"⚠️ Already tracking @{username}.", ephemeral=True)
        return
    await api_add_tracked(username, datetime.now(timezone.utc).isoformat())
    await interaction.response.send_message(f"⏱️ Started tracking **@{username}**. I'll post here when it's back.")


@bot.tree.command(name="untrack", description="Stop tracking an Instagram account")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def untrack(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()
    tracked = await api_get_tracked()
    if username in tracked:
        await api_remove_tracked(username)
        await interaction.response.send_message(f"✅ Stopped tracking @{username}.")
    else:
        await interaction.response.send_message(f"❌ @{username} isn't being tracked.", ephemeral=True)


@bot.tree.command(name="list", description="List all tracked accounts")
@is_owner()
async def list_tracked(interaction: discord.Interaction):
    tracked = await api_get_tracked()
    if not tracked:
        await interaction.response.send_message("📭 Nothing is being tracked right now.")
        return

    pending = {u: m for u, m in tracked.items() if not m.get("recovered")}
    recovered = {u: m for u, m in tracked.items() if m.get("recovered")}

    lines = [
        "📊 **Tracked Accounts:**",
        f"Active: {len(pending)} | Recovered: {len(recovered)}",
        "─" * 32,
    ]

    if pending:
        lines.append("")
        lines.append("**Currently Tracking:**")
        for username, meta in pending.items():
            lines.append(f"`{username}` — ⏳ {format_elapsed(meta['start_time'])}")

    if recovered:
        lines.append("")
        lines.append("**Recovered:**")
        for username, meta in recovered.items():
            lines.append(f"`{username}` — ✅")

    await interaction.response.send_message("\n".join(lines))


@bot.tree.command(name="setchannel", description="Set this channel as the recovery notification channel")
@is_owner()
async def setchannel(interaction: discord.Interaction):
    ok = await api_set_config("notify_channel_id", str(interaction.channel_id))
    if ok:
        # verify by reading it straight back, not just trusting the write response
        config = await api_get_config()
        saved_id = config.get("notify_channel_id")
        if str(saved_id) == str(interaction.channel_id):
            await interaction.response.send_message(
                f"✅ Recovery notifications will be posted in {interaction.channel.mention}."
            )
        else:
            await interaction.response.send_message(
                f"⚠️ Save request succeeded but the database shows a different value "
                f"(`{saved_id}`) than expected (`{interaction.channel_id}`). Something's "
                f"wrong with the database — try again or check the Cloudflare Worker."
            )
    else:
        await interaction.response.send_message(
            "❌ Failed to save the channel — the database write did not succeed. "
            "Check Render logs for the exact error, and verify D1_WORKER_URL/D1_API_KEY "
            "are correct.",
            ephemeral=True,
        )


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN not set.")
    if not D1_WORKER_URL or not D1_API_KEY:
        raise SystemExit("D1_WORKER_URL and D1_API_KEY must be set. See README.md.")
    if not RAPIDAPI_KEY:
        raise SystemExit("RAPIDAPI_KEY not set. See README.md.")
    start_keep_alive()
    bot.run(DISCORD_TOKEN)
