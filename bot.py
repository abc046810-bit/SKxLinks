"""
SKxLinksBot (@SKxLinksBot)
- Admin: create posts (step + bulk), send to me / channel / share link
- Public: deep-link delivery after forced channel join
- Multi MongoDB URI storage
- Rich Message posts + 2-min auto-delete
Render free web service + UptimeRobot ready
"""

import os
import re
import json
import logging
import asyncio
import secrets
import string
from datetime import datetime, timezone
from threading import Thread
from flask import Flask
import httpx
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from telegram import (
    Update,
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    ChatMember,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode, ChatMemberStatus

# ================== CONFIG ==================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
MAIN_OWNER_ID = int(os.environ.get("OWNER_ID", "8723278238"))
PORT = int(os.environ.get("PORT", 8080))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "SKxLinksBot")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")  # optional render url
DELETE_AFTER_SEC = 120  # 2 minutes

DMCA_IMAGE_URL = "https://cdn.phototourl.com/member/2026-09-15-ca86640f-3059-499d-acc8-9d100738e3bc.png"
HOW_TO_DOWNLOAD_URL = "https://t.me/SKxMOVIES/614"

# Forced join — checked EVERY time before delivery
FORCE_CHANNELS = [
    {"username": "The_Sk08", "url": "https://t.me/The_Sk08", "title": "THE SK08"},
    {"username": "Movielink_08", "url": "https://t.me/Movielink_08", "title": "MOVIE LINKS"},
]

DEFAULT_QUALITIES = [
    "480p",
    "720p HEVC",
    "720p x264",
    "1080p HEVC",
    "1080p x264",
    "HQ-Rip 1080p",
    "HQ 1080p",
]

(
    SELECT_TYPE,
    WAITING_POSTER_URL,
    WAITING_TITLE,
    WAITING_SAMPLE,
    WAITING_TRAILER,
    WAITING_STREAM,
    WAITING_MORE_STREAM,
    WAITING_SEASON,
    WAITING_EPISODE,
    WAITING_SEASON_NUM,
    QUALITY_MENU,
    WAITING_SIZE,
    WAITING_LINK1,
    WAITING_LINK2,
    WAITING_NEW_QUALITY,
    WAITING_SCREENSHOTS,
    WAITING_HASHTAGS,
    CONFIRM_POST,
    BULK_POSTER,
    BULK_TEXT,
    BULK_CONFIRM,
) = range(21)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ADMINS_FILE = "admins.json"
CHANNEL_FILE = "channel.json"


# ================== ADMINS / CHANNEL CONFIG ==================
def load_admins():
    if os.path.exists(ADMINS_FILE):
        try:
            with open(ADMINS_FILE, "r") as f:
                return set(json.load(f).get("admins", []))
        except Exception:
            pass
    return set()


def save_admins(admins_set):
    with open(ADMINS_FILE, "w") as f:
        json.dump({"admins": list(admins_set)}, f, indent=2)


ADMINS = load_admins()
ADMINS.add(MAIN_OWNER_ID)


def is_admin(user_id: int) -> bool:
    return user_id in ADMINS or user_id == MAIN_OWNER_ID


def load_channel():
    if os.path.exists(CHANNEL_FILE):
        try:
            with open(CHANNEL_FILE, "r") as f:
                return json.load(f).get("channel_id")
        except Exception:
            pass
    return None


def save_channel(channel_id):
    with open(CHANNEL_FILE, "w") as f:
        json.dump({"channel_id": channel_id}, f, indent=2)


def clear_channel():
    if os.path.exists(CHANNEL_FILE):
        try:
            os.remove(CHANNEL_FILE)
        except Exception:
            pass


# ================== MULTI MONGO ==================
_mongo_clients = []  # list of (uri_label, client, db)


def _collect_mongo_uris():
    uris = []
    single = os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URI")
    if single:
        uris.append(("URI_1", single.strip()))
    for i in range(1, 11):
        u = os.environ.get(f"MONGO_URI_{i}")
        if u and u.strip():
            label = f"URI_{i}"
            if not any(x[0] == label for x in uris):
                uris.append((label, u.strip()))
    # dedupe by value
    seen = set()
    out = []
    for label, u in uris:
        if u not in seen:
            seen.add(u)
            out.append((label, u))
    return out


def init_mongo():
    global _mongo_clients
    _mongo_clients = []
    for label, uri in _collect_mongo_uris():
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=8000)
            client.admin.command("ping")
            db = client.get_default_database()
            if db is None or db.name == "test" or not db.name:
                # use explicit db name from path or default skxlinks
                db = client["skxlinks"]
            _mongo_clients.append((label, client, db))
            logger.info(f"Mongo connected: {label} db={db.name}")
        except Exception as e:
            logger.error(f"Mongo failed {label}: {e}")
    if not _mongo_clients:
        logger.warning("NO MongoDB connected — posts will fail until MONGO_URI is set")


def mongo_ready() -> bool:
    return len(_mongo_clients) > 0


def gen_code(n: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def save_post(post_doc: dict) -> str:
    """
    Write new post to first available Mongo.
    Returns short code.
    """
    if not _mongo_clients:
        raise RuntimeError("MongoDB not configured. Set MONGO_URI on Render.")

    code = gen_code(8)
    # ensure unique across all
    for _ in range(12):
        found = False
        for label, client, db in _mongo_clients:
            try:
                if db.posts.find_one({"code": code}):
                    found = True
                    break
            except Exception:
                pass
        if not found:
            break
        code = gen_code(8)

    doc = dict(post_doc)
    doc["code"] = code
    doc["created_at"] = datetime.now(timezone.utc).isoformat()

    last_err = None
    for label, client, db in _mongo_clients:
        try:
            db.posts.insert_one(doc)
            logger.info(f"Post {code} saved on {label}")
            return code
        except PyMongoError as e:
            last_err = e
            logger.warning(f"Write failed on {label}: {e}")
            continue
    raise RuntimeError(f"All Mongo URIs failed to write: {last_err}")


def get_post_by_code(code: str):
    """Search all Mongo URIs for code (old posts stay on old cluster)."""
    if not code:
        return None
    code = code.strip()
    for label, client, db in _mongo_clients:
        try:
            doc = db.posts.find_one({"code": code})
            if doc:
                doc.pop("_id", None)
                doc["_mongo_label"] = label
                return doc
        except Exception as e:
            logger.warning(f"Read failed {label}: {e}")
    return None


# ================== HELPERS ==================
def esc(s: str) -> str:
    if not s:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def get_user_data(context: ContextTypes.DEFAULT_TYPE):
    if "post" not in context.user_data:
        context.user_data["post"] = {
            "poster": None,
            "screenshots": [],
            "type": None,
            "title": None,
            "sample": None,
            "trailer": None,
            "streams": [],
            "season": None,
            "episode": None,
            "seasons": {},
            "qualities": {q: {"size": None, "links": []} for q in DEFAULT_QUALITIES},
            "current_quality": None,
            "current_season": None,
            "hashtags": None,
        }
    if "to_delete" not in context.user_data:
        context.user_data["to_delete"] = []
    return context.user_data["post"]


def track(context: ContextTypes.DEFAULT_TYPE, message):
    if message and hasattr(message, "message_id"):
        context.user_data.setdefault("to_delete", []).append(message.message_id)


def reset_post(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("post", None)
    context.user_data.pop("to_delete", None)
    context.user_data.pop("bulk", None)


def get_current_qualities(post):
    if post["type"] == "webseries" and post.get("current_season"):
        return post["seasons"][post["current_season"]]
    return post["qualities"]


def make_quality_keyboard(post):
    qualities = get_current_qualities(post)
    buttons = []
    row = []
    for q in qualities.keys():
        status = "✅" if qualities[q]["links"] else "⬜"
        row.append(InlineKeyboardButton(f"{status} {q}", callback_data=f"q_{q}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("➕ Add New Quality", callback_data="add_new_quality")])
    if post["type"] == "webseries":
        buttons.append([
            InlineKeyboardButton("✅ Season Done", callback_data="season_done"),
            InlineKeyboardButton("➕ Another Season", callback_data="add_another_season"),
        ])
        buttons.append([
            InlineKeyboardButton("🏁 Finish All Seasons", callback_data="finish"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
        ])
    else:
        buttons.append([
            InlineKeyboardButton("✅ Finish & Continue", callback_data="finish"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
        ])
    return InlineKeyboardMarkup(buttons)


def is_image_url(text: str) -> bool:
    text = text.strip().lower()
    if not text.startswith(("http://", "https://")):
        return False
    return any(ext in text for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]) or "tmdb.org" in text or "phototourl.com" in text


def deep_link(code: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={code}"


# ================== CAPTION / RICH HTML ==================
def build_rich_html_and_media(post: dict):
    media = []
    parts = []

    poster = post.get("poster")
    if poster:
        if isinstance(poster, str) and str(poster).startswith("http"):
            parts.append(f'<img src="{esc(poster)}"/>')
        else:
            media.append({"id": "poster", "media": {"type": "photo", "media": poster}})
            parts.append('<img src="tg://photo?id=poster"/>')
        parts.append("")

    parts.append("<h2>🎬 TITLE</h2>")
    parts.append(f'<p><b>"{esc(post.get("title") or "")}"</b></p>')

    if post.get("type") == "episode" and post.get("season") and post.get("episode"):
        parts.append(
            f'<p>📺 <b>Season {esc(str(post["season"]))} • Episode {esc(str(post["episode"]))}</b></p>'
        )

    if post.get("sample"):
        parts.append("<h3>🎞️ SAMPLE</h3>")
        parts.append(f'<p>🔗 <a href="{esc(post["sample"])}">Watch Sample</a></p>')

    if post.get("trailer"):
        parts.append("<h3>🎥 TRAILER</h3>")
        parts.append(f'<p>🔗 <a href="{esc(post["trailer"])}">Watch Trailer</a></p>')

    if post.get("streams"):
        parts.append("<h3>▶️ WATCH / STREAM</h3>")
        for idx, link in enumerate(post["streams"], 1):
            parts.append(f'<p>🔗 <a href="{esc(link)}">Stream Link {idx}</a></p>')
        parts.append("<p><i>⚠️ Online streams may contain ads</i></p>")

    parts.append("<hr/>")
    parts.append("<h3>📥 ALL QUALITY LINKS</h3>")
    parts.append("<p><i>🔵 Blue text = Clickable Download Link</i></p>")

    has_any = False
    if post.get("type") == "webseries" and post.get("seasons"):
        for season_num in sorted(
            post["seasons"].keys(),
            key=lambda x: int(x) if str(x).isdigit() else 0,
        ):
            qualities = post["seasons"][season_num]
            if not any(q.get("links") for q in qualities.values()):
                continue
            has_any = True
            parts.append(f"<h4>📺 SEASON {esc(str(season_num))}</h4>")
            for q_name, q_data in qualities.items():
                if q_data.get("links"):
                    parts.append(f"<p>🔹 <b>{esc(q_name)}</b></p>")
                    size = q_data.get("size") or "—"
                    parts.append(f"<p>📦 Size: <code>{esc(size)}</code></p>")
                    for idx, link in enumerate(q_data["links"], 1):
                        parts.append(
                            f'<p>🔗 <a href="{esc(link)}">Download Link {idx}</a></p>'
                        )
    else:
        for q_name, q_data in (post.get("qualities") or {}).items():
            if q_data.get("links"):
                has_any = True
                parts.append(f"<p>🔹 <b>{esc(q_name)}</b></p>")
                size = q_data.get("size") or "—"
                parts.append(f"<p>📦 Size: <code>{esc(size)}</code></p>")
                for idx, link in enumerate(q_data["links"], 1):
                    parts.append(
                        f'<p>🔗 <a href="{esc(link)}">Download Link {idx}</a></p>'
                    )

    if not has_any:
        parts.append("<p><i>No quality links added</i></p>")

    parts.append("<hr/>")

    screenshots = post.get("screenshots") or []
    if screenshots:
        for i, ss in enumerate(screenshots):
            media.append({"id": f"ss{i}", "media": {"type": "photo", "media": ss}})
        parts.append("<details>")
        parts.append(
            f"<summary>📸 <b>SCREENSHOTS ({len(screenshots)})</b> — Tap to open &amp; swipe</summary>"
        )
        parts.append(
            "<p><i>👆 Click above to expand • Swipe to view all screenshots</i></p>"
        )
        parts.append("<tg-slideshow>")
        for i in range(len(screenshots)):
            parts.append(f'<img src="tg://photo?id=ss{i}"/>')
        parts.append(
            f"<figcaption>Screenshots — {esc(post.get('title') or '')}</figcaption>"
        )
        parts.append("</tg-slideshow>")
        parts.append("</details>")
        parts.append("<hr/>")

    parts.append("<h3>📥 HOW TO DOWNLOAD</h3>")
    parts.append(
        f'<p>🔗 <a href="{HOW_TO_DOWNLOAD_URL}"><b>HOW TO DOWNLOAD MOVIE • WEB SERIES • SHOW BY LINK</b></a></p>'
    )
    parts.append("<hr/>")
    parts.append(
        f'<p>⚖️ <a href="{esc(DMCA_IMAGE_URL)}"><b>DMCA &amp; CONTENT DISCLAIMER</b></a></p>'
    )
    parts.append(
        "<p>We do not host any files. Takedown → @SKxMOVIES_RequestBot</p>"
    )
    parts.append("<p>🔔 <b>STAY CONNECTED • STAY UPDATED</b> 🚀</p>")
    if post.get("hashtags"):
        parts.append(f"<p>{esc(post['hashtags'])}</p>")

    return "\n".join(parts), media


def build_legacy_caption(post: dict) -> str:
    lines = []
    lines.append("🎬 <b>TITLE</b>")
    lines.append(f'<b>"{esc(post.get("title") or "")}"</b>')
    lines.append("")
    if post.get("type") == "episode" and post.get("season") and post.get("episode"):
        lines.append(
            f"📺 <b>Season {esc(str(post['season']))} • Episode {esc(str(post['episode']))}</b>"
        )
        lines.append("")
    if post.get("sample"):
        lines.append("🎞️ <b>SAMPLE</b>")
        lines.append(f'🔗 <a href="{esc(post["sample"])}">Watch Sample</a>')
        lines.append("")
    if post.get("trailer"):
        lines.append("🎥 <b>TRAILER</b>")
        lines.append(f'🔗 <a href="{esc(post["trailer"])}">Watch Trailer</a>')
        lines.append("")
    if post.get("streams"):
        lines.append("▶️ <b>WATCH / STREAM</b>")
        for idx, link in enumerate(post["streams"], 1):
            lines.append(f'🔗 <a href="{esc(link)}">Stream Link {idx}</a>')
        lines.append("")
        lines.append("⚠️ <i>Online streams may contain ads</i>")
        lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📥 <b>ALL QUALITY LINKS</b>")
    lines.append("<i>🔵 Blue text = Clickable Link</i>")
    lines.append("")
    if post.get("type") == "webseries" and post.get("seasons"):
        for season_num in sorted(
            post["seasons"].keys(),
            key=lambda x: int(x) if str(x).isdigit() else 0,
        ):
            qualities = post["seasons"][season_num]
            if not any(q.get("links") for q in qualities.values()):
                continue
            lines.append(f"📺 <b>SEASON {esc(str(season_num))}</b>")
            lines.append("")
            for q_name, q_data in qualities.items():
                if q_data.get("links"):
                    lines.append(f"🔹 <b>{esc(q_name)}</b>")
                    size = q_data.get("size") or "—"
                    lines.append(f"📦 Size: <code>{esc(size)}</code>")
                    for idx, link in enumerate(q_data["links"], 1):
                        lines.append(f'🔗 <a href="{esc(link)}">Download Link {idx}</a>')
                    lines.append("")
            lines.append("")
    else:
        for q_name, q_data in (post.get("qualities") or {}).items():
            if q_data.get("links"):
                lines.append(f"🔹 <b>{esc(q_name)}</b>")
                size = q_data.get("size") or "—"
                lines.append(f"📦 Size: <code>{esc(size)}</code>")
                for idx, link in enumerate(q_data["links"], 1):
                    lines.append(f'🔗 <a href="{esc(link)}">Download Link {idx}</a>')
                lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📥 <b>HOW TO DOWNLOAD</b>")
    lines.append(
        f'🔗 <a href="{HOW_TO_DOWNLOAD_URL}"><b>HOW TO DOWNLOAD MOVIE • WEB SERIES • SHOW BY LINK</b></a>'
    )
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append(
        f'⚖️ <a href="{esc(DMCA_IMAGE_URL)}"><b>DMCA &amp; CONTENT DISCLAIMER</b></a>'
    )
    lines.append("We do not host any files. Takedown → @SKxMOVIES_RequestBot")
    lines.append("")
    lines.append("🔔 <b>STAY CONNECTED • STAY UPDATED</b> 🚀")
    if post.get("hashtags"):
        lines.append("")
        lines.append(esc(post["hashtags"]))
    return "\n".join(lines)


def safe_html_truncate(html: str, max_len: int = 1000) -> str:
    if len(html) <= max_len:
        return html
    cut = html[:max_len]
    last_lt, last_gt = cut.rfind("<"), cut.rfind(">")
    if last_lt > last_gt:
        cut = cut[:last_lt]
    if cut.count("<a ") > cut.count("</a>"):
        idx = cut.rfind("<a ")
        if idx >= 0:
            cut = cut[:idx]
    return cut.rstrip() + "\n\n… <i>(truncated)</i>"


def build_preview_caption(post: dict) -> str:
    full = build_legacy_caption(post)
    if len(full) <= 1000:
        return full
    lines = [
        "🎬 <b>TITLE</b>",
        f'<b>"{esc(post.get("title") or "")}"</b>',
        "",
    ]
    if post.get("type") == "webseries" and post.get("seasons"):
        sc = sum(
            1
            for sn, qs in post["seasons"].items()
            if any(q.get("links") for q in qs.values())
        )
        lc = sum(
            len(q.get("links") or [])
            for qs in post["seasons"].values()
            for q in qs.values()
        )
        lines.append(f"📺 Web Series — {sc} season(s), {lc} link(s)")
    else:
        qcount = sum(1 for q in (post.get("qualities") or {}).values() if q.get("links"))
        lcount = sum(len(q.get("links") or []) for q in (post.get("qualities") or {}).values())
        lines.append(f"📥 {qcount} quality(ies), {lcount} link(s)")
    ss = len(post.get("screenshots") or [])
    if ss:
        lines.append(f"📸 Screenshots: {ss}")
    lines.append("")
    lines.append("<i>Preview shortened. Final post full hoga.</i>")
    return "\n".join(lines)


# ================== SEND RICH / CLASSIC ==================
async def send_rich_message(token: str, chat_id, html: str, media: list):
    payload = {
        "chat_id": chat_id,
        "rich_message": {"html": html, "skip_entity_detection": False},
    }
    if media:
        payload["rich_message"]["media"] = media
    url = f"https://api.telegram.org/bot{token}/sendRichMessage"
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, json=payload)
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description", str(data)))
        return data.get("result")


async def send_post_to_chat(context: ContextTypes.DEFAULT_TYPE, chat_id, post: dict):
    """Send rich post; fallback classic. Returns list of message ids sent."""
    token = context.bot.token
    html, media = build_rich_html_and_media(post)
    try:
        result = await send_rich_message(token, chat_id, html, media)
        mid = result.get("message_id") if isinstance(result, dict) else None
        return [mid] if mid else []
    except Exception as e:
        logger.warning(f"Rich failed, classic fallback: {e}")

    caption = safe_html_truncate(build_legacy_caption(post), 1000)
    poster = post.get("poster")
    screenshots = post.get("screenshots") or []
    ids = []
    try:
        if poster and screenshots:
            mg = [
                InputMediaPhoto(
                    media=poster, caption=caption, parse_mode=ParseMode.HTML
                )
            ]
            for ss in screenshots:
                mg.append(InputMediaPhoto(media=ss))
            msgs = await context.bot.send_media_group(chat_id=chat_id, media=mg)
            ids = [m.message_id for m in msgs]
        elif poster:
            m = await context.bot.send_photo(
                chat_id=chat_id,
                photo=poster,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            ids = [m.message_id]
        else:
            m = await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            ids = [m.message_id]
    except Exception as e2:
        logger.error(f"Classic send failed: {e2}")
        m = await context.bot.send_message(
            chat_id=chat_id, text=f"⚠️ Post send error: {e2}"
        )
        ids = [m.message_id]
    return ids


async def schedule_delete(bot, chat_id, message_ids, delay=DELETE_AFTER_SEC):
    await asyncio.sleep(delay)
    for mid in message_ids:
        if not mid:
            continue
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass


# ================== FORCE JOIN ==================
async def check_all_joins(bot, user_id: int) -> list:
    """Return list of channels user has NOT joined. Empty = all ok.
    Re-checked every delivery — left users must rejoin.
    """
    missing = []
    for ch in FORCE_CHANNELS:
        username = ch["username"]
        chat_id = f"@{username}"
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            status = member.status
            ok = status in (
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
                "member",
                "administrator",
                "creator",
            )
            if not ok:
                missing.append(ch)
        except Exception as e:
            logger.warning(f"getChatMember @{username}: {e}")
            # If bot can't check, treat as missing (safer)
            missing.append(ch)
    return missing


def join_keyboard():
    rows = []
    for ch in FORCE_CHANNELS:
        rows.append(
            [InlineKeyboardButton(f"📢 Join {ch['title']}", url=ch["url"])]
        )
    rows.append(
        [InlineKeyboardButton("✅ I Joined — Check Again", callback_data="check_join")]
    )
    return InlineKeyboardMarkup(rows)


# ================== START / WELCOME / DELIVERY ==================
WELCOME_TEXT = (
    "👋 *Welcome to SKxLinks*\n\n"
    "🎬 Movies • Web Series • Shows — instant access\n\n"
    "📌 *How it works*\n"
    "1. Open a share link from us\n"
    "2. Join required channels\n"
    "3. Receive the full post here\n"
    "4. Post auto-deletes in *2 minutes* — forward/save first\n\n"
    "🔒 Private delivery • Clean chat after 2 min\n\n"
    "Use the menu (☰) for commands if you are admin."
)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args or []

    # Deep link payload
    if args:
        code = args[0].strip()
        await deliver_post_flow(update, context, code)
        return

    # Plain start
    if is_admin(user.id):
        await update.message.reply_text(
            WELCOME_TEXT
            + "\n\n"
            "🛠 *Admin*\n"
            "`/new` — step-by-step post\n"
            "`/bulk` — paste caption post\n"
            "`/setchannel` `/getchannel`\n"
            "`/admins` `/help`",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.MARKDOWN)


async def deliver_post_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, code: str):
    """Force-join check then send post; schedule 2-min delete."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    msg = update.effective_message

    if not mongo_ready():
        await msg.reply_text("⚠️ Service temporarily unavailable. Try again later.")
        return

    post = get_post_by_code(code)
    if not post:
        await msg.reply_text(
            "❌ *Link invalid or expired.*\nPlease request a new link.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Store pending code for check_join callback
    context.user_data["pending_code"] = code

    missing = await check_all_joins(context.bot, user.id)
    if missing:
        names = ", ".join(c["title"] for c in missing)
        await msg.reply_text(
            "🔐 *Access Locked*\n\n"
            "To unlock this post, please join the required channel(s):\n"
            f"➡️ {names}\n\n"
            "After joining, tap *I Joined — Check Again*.\n"
            "_If you left a channel, you must join again every time._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=join_keyboard(),
        )
        return

    await send_delivery(context, chat_id, post, user.id)


async def send_delivery(context, chat_id, post, user_id):
    notice = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "⏳ *Preparing your post…*\n\n"
            "⚠️ This post will be *automatically deleted in 2 minutes*.\n"
            "Please *forward* it to Saved Messages or another chat now."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )

    ids = await send_post_to_chat(context, chat_id, post)
    warn = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🗑️ *Auto-delete in 2 minutes*\n"
            "Forward / save this post before it disappears.\n"
            "Thank you for staying with SKxLinks."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    all_ids = [notice.message_id, warn.message_id] + list(ids)
    asyncio.create_task(
        schedule_delete(context.bot, chat_id, all_ids, DELETE_AFTER_SEC)
    )


async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    code = context.user_data.get("pending_code")
    if not code:
        await query.edit_message_text(
            "Session expired. Open your share link again."
        )
        return

    post = get_post_by_code(code)
    if not post:
        await query.edit_message_text("❌ Link invalid or expired.")
        return

    missing = await check_all_joins(context.bot, user.id)
    if missing:
        names = ", ".join(c["title"] for c in missing)
        try:
            await query.edit_message_text(
                "❌ *Still not joined*\n\n"
                f"Missing: {names}\n\n"
                "Join *all* required channels, then tap check again.\n"
                "_Leaving a channel removes access until you rejoin._",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=join_keyboard(),
            )
        except Exception:
            await query.message.reply_text(
                "Still missing channels. Join all, then check again.",
                reply_markup=join_keyboard(),
            )
        return

    try:
        await query.edit_message_text("✅ Verified! Sending your post…")
    except Exception:
        pass
    await send_delivery(context, query.message.chat_id, post, user.id)


# ================== ADMIN COMMANDS ==================
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.MARKDOWN)
        return
    await update.message.reply_text(
        "🛠 *SKxLinks Admin Help*\n\n"
        "*Create post*\n"
        "`/new` — step-by-step\n"
        "`/bulk` — poster + caption paste\n\n"
        "*After create*\n"
        "• Send to Me\n"
        "• Send to Channel\n"
        "• *Get Share Link* (deep link for users)\n\n"
        "*Channel*\n"
        "`/setchannel @Channel`\n"
        "`/getchannel` `/removechannel`\n\n"
        "*Admins*\n"
        "`/addadmin <id>` `/removeadmin <id>` `/admins`\n\n"
        "*Users*\n"
        "Must join @The\\_Sk08 + @Movielink\\_08\n"
        "Post auto-deletes in 2 minutes\n"
        "`/cancel` — stop current flow",
        parse_mode=ParseMode.MARKDOWN,
    )


async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MAIN_OWNER_ID:
        await update.message.reply_text("⛔ Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
    try:
        new_id = int(context.args[0])
        ADMINS.add(new_id)
        save_admins(ADMINS)
        await update.message.reply_text(f"✅ Admin added: `{new_id}`", parse_mode=ParseMode.MARKDOWN)
    except ValueError:
        await update.message.reply_text("Invalid ID")


async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MAIN_OWNER_ID:
        await update.message.reply_text("⛔ Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removeadmin <user_id>")
        return
    try:
        rem = int(context.args[0])
        if rem == MAIN_OWNER_ID:
            await update.message.reply_text("Cannot remove owner.")
            return
        ADMINS.discard(rem)
        save_admins(ADMINS)
        await update.message.reply_text(f"✅ Removed: `{rem}`", parse_mode=ParseMode.MARKDOWN)
    except ValueError:
        await update.message.reply_text("Invalid ID")


async def list_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    text = "👑 *Admins*\n\n"
    for aid in sorted(ADMINS):
        text += f"`{aid}`" + (" (Owner)" if aid == MAIN_OWNER_ID else "") + "\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def set_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MAIN_OWNER_ID:
        await update.message.reply_text("⛔ Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/setchannel @Channel` or `-100id`", parse_mode=ParseMode.MARKDOWN)
        return
    raw = context.args[0].strip()
    if raw.startswith("@"):
        ch = raw
    else:
        try:
            ch = int(raw)
        except ValueError:
            await update.message.reply_text("Invalid channel.")
            return
    save_channel(ch)
    await update.message.reply_text(f"✅ Channel set: `{ch}`", parse_mode=ParseMode.MARKDOWN)


async def get_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    ch = load_channel()
    await update.message.reply_text(
        f"📢 Channel: `{ch}`" if ch else "No channel set. `/setchannel @Channel`",
        parse_mode=ParseMode.MARKDOWN,
    )


async def remove_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MAIN_OWNER_ID:
        await update.message.reply_text("⛔ Owner only.")
        return
    clear_channel()
    await update.message.reply_text("✅ Channel removed.")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id:
        to_delete = context.user_data.get("to_delete", [])
        if update.callback_query:
            to_delete.append(update.callback_query.message.message_id)
        for mid in to_delete:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
    reset_post(context)
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text("❌ Cancelled.")
        except Exception:
            pass
    else:
        await update.message.reply_text("❌ Cancelled. `/new` or `/bulk` to start again.")
    return ConversationHandler.END


# ================== CREATE POST FLOW (ADMIN) ==================
async def new_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return ConversationHandler.END
    context.user_data["to_delete"] = []
    track(context, update.message)
    post = get_user_data(context)
    post["poster"] = None
    post["screenshots"] = []
    kb = [
        [
            InlineKeyboardButton("🎬 Movie", callback_data="type_movie"),
            InlineKeyboardButton("📺 Web Series", callback_data="type_webseries"),
        ],
        [InlineKeyboardButton("🎞 Episode / Show", callback_data="type_episode")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    msg = await update.message.reply_text(
        "📝 Select type:", reply_markup=InlineKeyboardMarkup(kb)
    )
    track(context, msg)
    return SELECT_TYPE


async def photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    post = context.user_data.get("post")
    if post and post.get("title"):
        return await screenshot_photo(update, context)
    context.user_data["to_delete"] = []
    track(context, update.message)
    post = get_user_data(context)
    post["poster"] = update.message.photo[-1].file_id
    post["screenshots"] = []
    kb = [
        [
            InlineKeyboardButton("🎬 Movie", callback_data="type_movie"),
            InlineKeyboardButton("📺 Web Series", callback_data="type_webseries"),
        ],
        [InlineKeyboardButton("🎞 Episode / Show", callback_data="type_episode")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    msg = await update.message.reply_text(
        "✅ Poster saved. Select type:", reply_markup=InlineKeyboardMarkup(kb)
    )
    track(context, msg)
    return SELECT_TYPE


async def type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        return await cancel(update, context)
    post = get_user_data(context)
    post["type"] = {
        "type_movie": "movie",
        "type_webseries": "webseries",
        "type_episode": "episode",
    }.get(query.data, "movie")
    if not post.get("poster"):
        kb = [
            [InlineKeyboardButton("⏭ Skip Poster URL", callback_data="skip_poster")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        await query.edit_message_text(
            "🖼 Poster image URL (or Skip):",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return WAITING_POSTER_URL
    await query.edit_message_text("📝 Send *Title*:", parse_mode=ParseMode.MARKDOWN)
    return WAITING_TITLE


async def poster_url_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    text = update.message.text.strip()
    if is_image_url(text):
        post["poster"] = text
        msg = await update.message.reply_text(
            "✅ Poster URL saved.\n📝 Send *Title*:", parse_mode=ParseMode.MARKDOWN
        )
        track(context, msg)
        return WAITING_TITLE
    msg = await update.message.reply_text("⚠️ Invalid image URL. Try again or Skip.")
    track(context, msg)
    return WAITING_POSTER_URL


async def skip_poster(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    get_user_data(context)["poster"] = None
    await query.edit_message_text("📝 Send *Title*:", parse_mode=ParseMode.MARKDOWN)
    return WAITING_TITLE


async def title_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    get_user_data(context)["title"] = update.message.text.strip()
    kb = [
        [InlineKeyboardButton("⏭ Skip Sample", callback_data="skip_sample")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    msg = await update.message.reply_text(
        "🔗 Sample link (or Skip):", reply_markup=InlineKeyboardMarkup(kb)
    )
    track(context, msg)
    return WAITING_SAMPLE


async def sample_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    get_user_data(context)["sample"] = update.message.text.strip()
    return await ask_trailer(update, context)


async def skip_sample(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    get_user_data(context)["sample"] = None
    return await ask_trailer(update, context, from_callback=True)


async def ask_trailer(update, context, from_callback=False):
    kb = [
        [InlineKeyboardButton("⏭ Skip Trailer", callback_data="skip_trailer")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    text = "🎬 Trailer link (optional):"
    if from_callback:
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        msg = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))
        track(context, msg)
    return WAITING_TRAILER


async def trailer_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    get_user_data(context)["trailer"] = update.message.text.strip()
    return await ask_stream(update, context)


async def skip_trailer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    get_user_data(context)["trailer"] = None
    return await ask_stream(update, context, from_callback=True)


async def ask_stream(update, context, from_callback=False):
    kb = [
        [InlineKeyboardButton("⏭ Skip Stream", callback_data="skip_stream")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    text = "▶️ Watch/Stream link (optional, multiple ok):"
    if from_callback:
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        msg = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))
        track(context, msg)
    return WAITING_STREAM


async def stream_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    post["streams"].append(update.message.text.strip())
    kb = [
        [InlineKeyboardButton("➕ Another Stream", callback_data="add_more_stream")],
        [InlineKeyboardButton("✅ Done Streams", callback_data="streams_done")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    msg = await update.message.reply_text(
        f"✅ Stream {len(post['streams'])} added.",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    track(context, msg)
    return WAITING_MORE_STREAM


async def skip_stream(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    get_user_data(context)["streams"] = []
    return await after_stream(update, context, from_callback=True)


async def more_stream_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        return await cancel(update, context)
    if query.data == "add_more_stream":
        await query.edit_message_text("▶️ Next stream link:")
        return WAITING_STREAM
    return await after_stream(update, context, from_callback=True)


async def after_stream(update, context, from_callback=False):
    post = get_user_data(context)
    if post["type"] == "episode":
        text = "📺 Season number (e.g. 1):"
        if from_callback:
            await update.callback_query.edit_message_text(text)
        else:
            msg = await update.message.reply_text(text)
            track(context, msg)
        return WAITING_SEASON
    if post["type"] == "webseries":
        text = "📺 First season number (e.g. 1):"
        if from_callback:
            await update.callback_query.edit_message_text(text)
        else:
            msg = await update.message.reply_text(text)
            track(context, msg)
        return WAITING_SEASON_NUM
    text = "📥 Add quality links:"
    kb = make_quality_keyboard(post)
    if from_callback:
        await update.callback_query.edit_message_text(text, reply_markup=kb)
    else:
        msg = await update.message.reply_text(text, reply_markup=kb)
        track(context, msg)
    return QUALITY_MENU


async def season_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    get_user_data(context)["season"] = update.message.text.strip()
    msg = await update.message.reply_text("🎞 Episode number:")
    track(context, msg)
    return WAITING_EPISODE


async def episode_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    post["episode"] = update.message.text.strip()
    msg = await update.message.reply_text(
        "📥 Add quality links:", reply_markup=make_quality_keyboard(post)
    )
    track(context, msg)
    return QUALITY_MENU


async def season_num_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    sn = update.message.text.strip()
    if sn not in post["seasons"]:
        post["seasons"][sn] = {q: {"size": None, "links": []} for q in DEFAULT_QUALITIES}
    post["current_season"] = sn
    msg = await update.message.reply_text(
        f"📥 *Season {sn}* quality links:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=make_quality_keyboard(post),
    )
    track(context, msg)
    return QUALITY_MENU


async def quality_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    post = get_user_data(context)
    if data == "cancel":
        return await cancel(update, context)
    if data == "finish":
        kb = [
            [InlineKeyboardButton("⏭ Skip Screenshots", callback_data="skip_screenshots")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        await query.edit_message_text(
            "🖼 Send screenshots (optional). When done, tap Done.",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return WAITING_SCREENSHOTS
    if data in ("season_done", "add_another_season"):
        await query.edit_message_text("📺 Next season number (or Finish All):")
        return WAITING_SEASON_NUM
    if data == "add_new_quality":
        await query.edit_message_text("➕ New quality name (e.g. 2160p 4K):")
        return WAITING_NEW_QUALITY
    if data.startswith("q_"):
        post["current_quality"] = data[2:]
        await query.edit_message_text(
            f"📦 *{post['current_quality']}* size (or `skip`):",
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_SIZE
    return QUALITY_MENU


async def new_quality_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    nq = update.message.text.strip()
    qualities = get_current_qualities(post)
    if nq in qualities:
        msg = await update.message.reply_text("Already exists.")
        track(context, msg)
        return WAITING_NEW_QUALITY
    qualities[nq] = {"size": None, "links": []}
    msg = await update.message.reply_text(
        f"✅ `{nq}` added.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=make_quality_keyboard(post),
    )
    track(context, msg)
    return QUALITY_MENU


async def size_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    text = update.message.text.strip()
    q = post["current_quality"]
    qualities = get_current_qualities(post)
    qualities[q]["size"] = None if text.lower() == "skip" else text
    msg = await update.message.reply_text(
        f"🔗 *{q}* Download Link 1:", parse_mode=ParseMode.MARKDOWN
    )
    track(context, msg)
    return WAITING_LINK1


async def link1_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    q = post["current_quality"]
    get_current_qualities(post)[q]["links"] = [update.message.text.strip()]
    kb = [[InlineKeyboardButton("⏭ Skip Link 2", callback_data="skip_link2")]]
    msg = await update.message.reply_text(
        f"🔗 *{q}* Link 2 (optional):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(kb),
    )
    track(context, msg)
    return WAITING_LINK2


async def link2_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    q = post["current_quality"]
    link = update.message.text.strip()
    if link:
        get_current_qualities(post)[q]["links"].append(link)
    msg = await update.message.reply_text(
        f"✅ *{q}* updated.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=make_quality_keyboard(post),
    )
    track(context, msg)
    return QUALITY_MENU


async def skip_link2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    post = get_user_data(context)
    q = post["current_quality"]
    await query.edit_message_text(
        f"✅ *{q}* updated (1 link).",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=make_quality_keyboard(post),
    )
    return QUALITY_MENU


async def screenshot_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    post = get_user_data(context)
    fid = update.message.photo[-1].file_id
    if fid not in post["screenshots"]:
        post["screenshots"].append(fid)
    kb = [
        [InlineKeyboardButton("✅ Done Screenshots", callback_data="screenshots_done")],
        [InlineKeyboardButton("⏭ Skip", callback_data="skip_screenshots")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    msg = await update.message.reply_text(
        f"✅ Screenshot {len(post['screenshots'])}. More or Done:",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    track(context, msg)
    return WAITING_SCREENSHOTS


async def screenshots_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    post = get_user_data(context)
    if query.data == "cancel":
        return await cancel(update, context)
    if query.data == "skip_screenshots":
        post["screenshots"] = []
    kb = [
        [InlineKeyboardButton("⏭ Skip Hashtags", callback_data="skip_hashtags")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    await query.edit_message_text(
        "#️⃣ Hashtags (or Skip):", reply_markup=InlineKeyboardMarkup(kb)
    )
    return WAITING_HASHTAGS


async def hashtags_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    get_user_data(context)["hashtags"] = update.message.text.strip()
    return await show_preview(update, context)


async def skip_hashtags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    get_user_data(context)["hashtags"] = None
    return await show_preview(update, context, from_callback=True)


async def show_preview(update, context, from_callback=False):
    post = get_user_data(context)
    caption = build_preview_caption(post)
    poster = post.get("poster")
    extra = ""
    if post.get("screenshots"):
        extra = f"\n\n📸 {len(post['screenshots'])} screenshot(s)"
    try:
        if poster:
            if from_callback:
                preview = await update.callback_query.message.reply_photo(
                    photo=poster, caption=caption + extra, parse_mode=ParseMode.HTML
                )
                target = update.callback_query.message
            else:
                preview = await update.message.reply_photo(
                    photo=poster, caption=caption + extra, parse_mode=ParseMode.HTML
                )
                target = update.message
        else:
            if from_callback:
                preview = await update.callback_query.message.reply_text(
                    caption + extra, parse_mode=ParseMode.HTML
                )
                target = update.callback_query.message
            else:
                preview = await update.message.reply_text(
                    caption + extra, parse_mode=ParseMode.HTML
                )
                target = update.message
    except Exception as e:
        logger.error(f"preview: {e}")
        plain = f"Title: {post.get('title')}\n(Preview error — final still OK)"
        if from_callback:
            preview = await update.callback_query.message.reply_text(plain)
            target = update.callback_query.message
        else:
            preview = await update.message.reply_text(plain)
            target = update.message
    track(context, preview)

    ch = load_channel()
    kb = []
    if ch:
        kb.append([InlineKeyboardButton("📢 Send to Channel", callback_data="confirm_channel")])
    kb.append([
        InlineKeyboardButton("✅ Send to Me", callback_data="confirm_me"),
        InlineKeyboardButton("🔗 Get Share Link", callback_data="confirm_share"),
    ])
    kb.append([InlineKeyboardButton("✏️ Edit", callback_data="edit_again")])
    kb.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    cmsg = await target.reply_text(
        "👆 Preview\n\nChoose:\n"
        "• *Send to Me* — private\n"
        "• *Send to Channel* — channel post\n"
        "• *Get Share Link* — deep link for users (force-join + 2-min delete)",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(kb),
    )
    track(context, cmsg)
    return CONFIRM_POST


async def confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    post = get_user_data(context)
    if query.data == "cancel":
        return await cancel(update, context)
    if query.data == "edit_again":
        await query.edit_message_text(
            "✏️ Quality links:", reply_markup=make_quality_keyboard(post)
        )
        return QUALITY_MENU

    chat_id = query.message.chat_id

    if query.data == "confirm_me":
        await query.edit_message_text("⏳ Sending to you…")
        await send_post_to_chat(context, chat_id, post)
        _cleanup_admin(context, chat_id, query.message.message_id)
        reset_post(context)
        await context.bot.send_message(chat_id, "✅ Sent to you.\n/new or /bulk for next.")
        return ConversationHandler.END

    if query.data == "confirm_channel":
        ch = load_channel()
        if not ch:
            await query.edit_message_text("⚠️ Set channel first: /setchannel @Channel")
            return CONFIRM_POST
        await query.edit_message_text(f"⏳ Sending to channel `{ch}`…")
        try:
            await send_post_to_chat(context, ch, post)
            _cleanup_admin(context, chat_id, query.message.message_id)
            reset_post(context)
            await context.bot.send_message(
                chat_id,
                f"✅ Posted to channel `{ch}`.\n/new or /bulk for next.",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            await context.bot.send_message(chat_id, f"⚠️ Channel send failed: {e}")
        return ConversationHandler.END

    if query.data == "confirm_share":
        if not mongo_ready():
            await query.edit_message_text(
                "⚠️ MongoDB not connected. Set MONGO_URI on Render."
            )
            return CONFIRM_POST
        await query.edit_message_text("⏳ Saving & generating share link…")
        try:
            # serializable post (file_ids ok as strings)
            code = save_post(dict(post))
            link = deep_link(code)
            _cleanup_admin(context, chat_id, query.message.message_id)
            reset_post(context)
            await context.bot.send_message(
                chat_id,
                f"✅ *Share Link ready*\n\n"
                f"`{link}`\n\n"
                f"Users must join:\n"
                f"• @The\\_Sk08\n"
                f"• @Movielink\\_08\n\n"
                f"Post auto-deletes *2 min* after delivery.\n"
                f"Title: *{esc(post.get('title') or '')}*",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.exception("share link")
            await context.bot.send_message(chat_id, f"⚠️ Save failed: {e}")
        return ConversationHandler.END

    return CONFIRM_POST


def _cleanup_admin(context, chat_id, extra_mid=None):
    to_delete = context.user_data.get("to_delete", [])
    if extra_mid:
        to_delete.append(extra_mid)
    async def _run():
        for mid in to_delete:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
    asyncio.create_task(_run())


# ================== BULK ==================
_re_full_url = re.compile(r"^https?://[^\s<>\"']+$")


def bulk_footer_html() -> str:
    return "\n".join(
        [
            "<hr/>",
            "<p><b>📥 HOW TO DOWNLOAD</b></p>",
            f'<p>🔗 <a href="{HOW_TO_DOWNLOAD_URL}"><b>HOW TO DOWNLOAD MOVIE • WEB SERIES • SHOW BY LINK</b></a></p>',
            "<hr/>",
            f'<p>⚖️ <a href="{DMCA_IMAGE_URL}"><b>DMCA &amp; CONTENT DISCLAIMER</b></a></p>',
            "<p>We do not host any files. Takedown → @SKxMOVIES_RequestBot</p>",
            "<p>🔔 <b>STAY CONNECTED • STAY UPDATED</b> 🚀</p>",
        ]
    )


def bulk_text_to_html(text: str) -> str:
    if not text:
        return ""
    raw = text.strip().replace("DMCA & CONTENT", "DMCA &amp; CONTENT")
    lower = raw.lower()
    has_footer = "how to download" in lower or "dmca" in lower or "stay connected" in lower
    inject_note = "blue text" not in lower and "clickable" not in lower
    lines_out = []
    for line in raw.split("\n"):
        stripped = line.strip()
        if not stripped:
            lines_out.append("")
            continue
        if _re_full_url.match(stripped):
            if inject_note:
                lines_out.append("<p><i>🔵 Blue text = Clickable Download Link</i></p>")
                inject_note = False
            lines_out.append(f'🔗 <a href="{stripped}">Download Link</a>')
            continue
        if "<a href" in stripped.lower():
            lines_out.append(stripped)
            continue
        safe = esc(stripped)

        def _linkify(m):
            u = m.group(0).replace("&amp;", "&")
            return f'<a href="{u}">{m.group(0)}</a>'

        safe = re.sub(r"https?://[^\s<>\"']+", _linkify, safe)
        upper = stripped.upper()
        if "SEASON" in upper and len(stripped) < 50:
            if inject_note:
                lines_out.append("<p><i>🔵 Blue text = Clickable Download Link</i></p>")
                inject_note = False
            lines_out.append(f"<p><b>{safe}</b></p>")
        elif stripped.startswith(("📥", "📺", "⚖️", "🔔", "🎬")):
            lines_out.append(f"<p><b>{safe}</b></p>")
        else:
            lines_out.append(f"<p>{safe}</p>")
    body = "\n".join(lines_out)
    if not has_footer:
        body = body + "\n" + bulk_footer_html()
    return body


async def bulk_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return ConversationHandler.END
    context.user_data["bulk"] = {"poster": None, "text": None, "html": None}
    context.user_data["to_delete"] = []
    track(context, update.message)
    kb = [
        [InlineKeyboardButton("⏭ Skip Poster", callback_data="bulk_skip_poster")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    msg = await update.message.reply_text(
        "📦 *BULK MODE*\n\n"
        "1. Poster photo (or Skip)\n"
        "2. Paste TITLE + seasons + links only\n"
        "   (footer auto-added)\n"
        "3. Confirm → Me / Channel / Share Link",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(kb),
    )
    track(context, msg)
    return BULK_POSTER


async def bulk_poster_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    context.user_data.setdefault("bulk", {})["poster"] = update.message.photo[-1].file_id
    msg = await update.message.reply_text(
        "✅ Poster saved.\nPaste full caption (title + links):"
    )
    track(context, msg)
    return BULK_TEXT


async def bulk_skip_poster(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.setdefault("bulk", {})["poster"] = None
    await query.edit_message_text("Paste full caption (title + links):")
    return BULK_TEXT


async def bulk_text_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track(context, update.message)
    bulk = context.user_data.setdefault("bulk", {})
    text = (update.message.text or "").strip()
    if len(text) < 20:
        msg = await update.message.reply_text("Caption too short. Paste full text.")
        track(context, msg)
        return BULK_TEXT
    bulk["text"] = text
    bulk["html"] = bulk_text_to_html(text)
    # extract title heuristically for storage
    title = "Bulk Post"
    for line in text.split("\n")[:8]:
        t = line.strip().strip('"')
        if t and not t.startswith("http") and "TITLE" not in t.upper() and len(t) > 3:
            title = t[:120]
            break
    bulk["title"] = title

    ch = load_channel()
    kb = []
    if ch:
        kb.append([InlineKeyboardButton("📢 Send to Channel", callback_data="bulk_channel")])
    kb.append([
        InlineKeyboardButton("✅ Send to Me", callback_data="bulk_me"),
        InlineKeyboardButton("🔗 Get Share Link", callback_data="bulk_share"),
    ])
    kb.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    cmsg = await update.message.reply_text(
        f"✅ Caption received ({len(text)} chars).\nTitle guess: *{title}*\n\nChoose action:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(kb),
    )
    track(context, cmsg)
    return BULK_CONFIRM


async def bulk_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        return await cancel(update, context)
    bulk = context.user_data.get("bulk") or {}
    html = bulk.get("html") or bulk_text_to_html(bulk.get("text") or "")
    poster = bulk.get("poster")
    chat_id = query.message.chat_id
    title = bulk.get("title") or "Bulk Post"

    # Build a minimal post dict for rich send / storage
    post = {
        "poster": poster,
        "screenshots": [],
        "type": "movie",
        "title": title,
        "sample": None,
        "trailer": None,
        "streams": [],
        "season": None,
        "episode": None,
        "seasons": {},
        "qualities": {},
        "hashtags": None,
        "bulk_html": html,
        "is_bulk": True,
    }

    async def send_bulk_to(dest):
        media = []
        final_html = html
        if poster:
            media.append({"id": "poster", "media": {"type": "photo", "media": poster}})
            final_html = f'<img src="tg://photo?id=poster"/>\n{html}'
        try:
            await send_rich_message(context.bot.token, dest, final_html, media)
            return True
        except Exception as e:
            logger.warning(f"bulk rich: {e}")
            cap = (bulk.get("text") or "")[:1000]
            try:
                if poster:
                    await context.bot.send_photo(dest, photo=poster, caption=cap)
                else:
                    await context.bot.send_message(dest, text=cap)
                return True
            except Exception as e2:
                await context.bot.send_message(chat_id, f"⚠️ Error: {e2}")
                return False

    if query.data == "bulk_me":
        await query.edit_message_text("⏳ Sending…")
        await send_bulk_to(chat_id)
        _cleanup_admin(context, chat_id, query.message.message_id)
        reset_post(context)
        await context.bot.send_message(chat_id, "✅ Sent.\n/new or /bulk next.")
        return ConversationHandler.END

    if query.data == "bulk_channel":
        ch = load_channel()
        if not ch:
            await query.edit_message_text("⚠️ /setchannel first")
            return BULK_CONFIRM
        await query.edit_message_text("⏳ Channel…")
        await send_bulk_to(ch)
        _cleanup_admin(context, chat_id, query.message.message_id)
        reset_post(context)
        await context.bot.send_message(chat_id, f"✅ Channel `{ch}`", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    if query.data == "bulk_share":
        if not mongo_ready():
            await query.edit_message_text("⚠️ Set MONGO_URI on Render.")
            return BULK_CONFIRM
        await query.edit_message_text("⏳ Generating share link…")
        try:
            code = save_post(post)
            link = deep_link(code)
            _cleanup_admin(context, chat_id, query.message.message_id)
            reset_post(context)
            await context.bot.send_message(
                chat_id,
                f"✅ *Share Link*\n\n`{link}`\n\n"
                f"Join required: @The\\_Sk08 + @Movielink\\_08\n"
                f"Auto-delete: 2 min after delivery\n"
                f"Title: *{title}*",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception as e:
            await context.bot.send_message(chat_id, f"⚠️ {e}")
        return ConversationHandler.END

    return BULK_CONFIRM


# Override send for bulk posts stored with bulk_html
async def send_post_to_chat_wrapper(context, chat_id, post):
    if post.get("is_bulk") and post.get("bulk_html"):
        html = post["bulk_html"]
        media = []
        poster = post.get("poster")
        if poster:
            media.append({"id": "poster", "media": {"type": "photo", "media": poster}})
            html = f'<img src="tg://photo?id=poster"/>\n{html}'
        try:
            result = await send_rich_message(context.bot.token, chat_id, html, media)
            mid = result.get("message_id") if isinstance(result, dict) else None
            return [mid] if mid else []
        except Exception as e:
            logger.warning(f"bulk deliver rich fail: {e}")
            cap = (post.get("title") or "Post")[:200]
            try:
                if poster:
                    m = await context.bot.send_photo(chat_id, photo=poster, caption=cap)
                else:
                    m = await context.bot.send_message(chat_id, text=cap)
                return [m.message_id]
            except Exception:
                return []
    return await send_post_to_chat(context, chat_id, post)


# Patch delivery to use wrapper
_original_send_delivery = send_delivery


async def send_delivery(context, chat_id, post, user_id):
    notice = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "⏳ *Preparing your post…*\n\n"
            "⚠️ This post will be *automatically deleted in 2 minutes*.\n"
            "Please *forward* it to Saved Messages or another chat now."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    ids = await send_post_to_chat_wrapper(context, chat_id, post)
    warn = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🗑️ *Auto-delete in 2 minutes*\n"
            "Forward / save this post before it disappears.\n"
            "Thank you for staying with SKxLinks."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    all_ids = [notice.message_id, warn.message_id] + list(ids)
    asyncio.create_task(
        schedule_delete(context.bot, chat_id, all_ids, DELETE_AFTER_SEC)
    )


# ================== FLASK ==================
flask_app = Flask(__name__)


@flask_app.route("/")
def home():
    return "SKxLinks Bot is Alive ✅", 200


@flask_app.route("/health")
def health():
    return "OK", 200


def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


# ================== MAIN ==================
def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ Set BOT_TOKEN env var")
        return

    init_mongo()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    Thread(target=run_flask, daemon=True).start()
    print(f"✅ Flask on port {PORT}")

    async def post_init(application: Application):
        await application.bot.set_my_commands(
            [
                BotCommand("start", "Start / open share link"),
                BotCommand("new", "Admin: create post"),
                BotCommand("bulk", "Admin: bulk paste post"),
                BotCommand("cancel", "Cancel current flow"),
                BotCommand("setchannel", "Admin: set post channel"),
                BotCommand("getchannel", "Admin: show channel"),
                BotCommand("help", "Help"),
            ]
        )
        logger.info("Commands menu set")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Exception: {context.error}")
        if isinstance(update, Update) and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "⚠️ Technical error. /cancel and try again."
                )
            except Exception:
                pass

    app.add_error_handler(error_handler)

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, photo_received),
            CommandHandler("new", new_command),
        ],
        states={
            SELECT_TYPE: [CallbackQueryHandler(type_selected)],
            WAITING_POSTER_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, poster_url_received),
                CallbackQueryHandler(skip_poster, pattern="^skip_poster$"),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            WAITING_TITLE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, title_received)
            ],
            WAITING_SAMPLE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sample_received),
                CallbackQueryHandler(skip_sample, pattern="^skip_sample$"),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            WAITING_TRAILER: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, trailer_received),
                CallbackQueryHandler(skip_trailer, pattern="^skip_trailer$"),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            WAITING_STREAM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_received),
                CallbackQueryHandler(skip_stream, pattern="^skip_stream$"),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            WAITING_MORE_STREAM: [CallbackQueryHandler(more_stream_handler)],
            WAITING_SEASON: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, season_received)
            ],
            WAITING_EPISODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, episode_received)
            ],
            WAITING_SEASON_NUM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, season_num_received)
            ],
            QUALITY_MENU: [CallbackQueryHandler(quality_menu_handler)],
            WAITING_SIZE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, size_received)
            ],
            WAITING_LINK1: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, link1_received)
            ],
            WAITING_LINK2: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, link2_received),
                CallbackQueryHandler(skip_link2, pattern="^skip_link2$"),
            ],
            WAITING_NEW_QUALITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_quality_received)
            ],
            WAITING_SCREENSHOTS: [
                MessageHandler(filters.PHOTO, screenshot_photo),
                CallbackQueryHandler(screenshots_callback),
            ],
            WAITING_HASHTAGS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, hashtags_received),
                CallbackQueryHandler(skip_hashtags, pattern="^skip_hashtags$"),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            CONFIRM_POST: [CallbackQueryHandler(confirm_handler)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
        ],
        allow_reentry=False,
        per_message=False,
    )

    bulk_conv = ConversationHandler(
        entry_points=[CommandHandler("bulk", bulk_start)],
        states={
            BULK_POSTER: [
                MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, bulk_poster_photo),
                CallbackQueryHandler(bulk_skip_poster, pattern="^bulk_skip_poster$"),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            BULK_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bulk_text_received),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
            BULK_CONFIRM: [
                CallbackQueryHandler(
                    bulk_confirm, pattern="^bulk_(me|channel|share)$"
                ),
                CallbackQueryHandler(cancel, pattern="^cancel$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
        ],
        allow_reentry=False,
        per_message=False,
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("addadmin", add_admin))
    app.add_handler(CommandHandler("removeadmin", remove_admin))
    app.add_handler(CommandHandler("admins", list_admins))
    app.add_handler(CommandHandler("setchannel", set_channel))
    app.add_handler(CommandHandler("getchannel", get_channel))
    app.add_handler(CommandHandler("removechannel", remove_channel_cmd))
    app.add_handler(CallbackQueryHandler(check_join_callback, pattern="^check_join$"))
    app.add_handler(bulk_conv)
    app.add_handler(conv)
    app.add_handler(CommandHandler("cancel", cancel))

    print("✅ SKxLinksBot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
