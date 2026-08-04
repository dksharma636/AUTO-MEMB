#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MULTI-TENANT TELEGRAM CHANNEL MANAGEMENT PLATFORM
Deployable on Render / Heroku / Termux / VPS
FULLY REPAIRED WITH SQLITE LOCK PROTECTION
"""

from __future__ import annotations

import asyncio
import csv
import html
import io
import logging
import os
import re
import signal
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

# ---- Flask web server ----
from flask import Flask, jsonify

# ---- Telegram ----
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import httpx
from werkzeug.serving import make_server

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Message,
)
from telegram.constants import ParseMode, ChatAction, ChatType
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TelegramError,
    TimedOut,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",")
    if x.lstrip("-").isdigit()
}
DB_PATH = os.getenv("DB_PATH", "botdata.db")
LOG_CHANNEL = os.getenv("LOG_CHANNEL", "").strip()
SUPPORT_URL = os.getenv("SUPPORT_URL", "").strip()
BOT_NAME = os.getenv("BOT_NAME", "Channel Manager").strip()
WELCOME_DELAY = float(os.getenv("WELCOME_DELAY", "1"))
BROADCAST_WORKERS = max(1, int(os.getenv("BROADCAST_WORKERS", "5")))
BROADCAST_DELAY = float(os.getenv("BROADCAST_DELAY", "0.05"))
BROADCAST_MAX_RETRIES = int(os.getenv("BROADCAST_MAX_RETRIES", "3"))

# HTTP / Telegram timeouts
TG_CONNECT_TIMEOUT = float(os.getenv("TG_CONNECT_TIMEOUT", "30.0"))
TG_READ_TIMEOUT = float(os.getenv("TG_READ_TIMEOUT", "60.0"))
TG_WRITE_TIMEOUT = float(os.getenv("TG_WRITE_TIMEOUT", "60.0"))
TG_POOL_TIMEOUT = float(os.getenv("TG_POOL_TIMEOUT", "30.0"))
TG_POOL_SIZE = int(os.getenv("TG_POOL_SIZE", "50"))
TG_MAX_RETRIES = int(os.getenv("TG_MAX_RETRIES", "5"))

SCHEMA_VERSION = 2

DEFAULT_WELCOME = (
    "✨ Welcome, {mention}! ✨\n\n"
    "🎉 Aapki request {chat} ke liye approve kar di gayi hai.\n"
    "Ab aap hamari family ka hissa ho — dil se swagat hai! 🤝\n\n"
    "📌 Yahan aapko milega:\n"
    "• 🔥 Daily fresh updates\n"
    "• 💎 Premium & exclusive content\n"
    "• ⚡ Fast support\n\n"
    "🔔 Notifications ON kar lena, taaki koi update miss na ho.\n"
    "🚫 Spam / abuse strictly not allowed.\n\n"
    "Enjoy your stay! ❤️"
)

DEFAULT_PENDING_MSG = (
    "👋 Hi {mention}!\n\n"
    "Aapki join request {chat} ke liye mil gayi hai. ⏳\n"
    "Admin jaldi review karke approve kar denge. Thoda intezaar kijiye 🙏"
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("cmbot")

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()

# ----------------------------------------------------------------------------
# DATABASE WITH FULL LOCK & RETRY
# ----------------------------------------------------------------------------
_db_lock = threading.Lock()
DB_RETRY_ATTEMPTS = 5
DB_RETRY_DELAY = 0.1

def _db_connect():
    """Create a new database connection with pragmas."""
    con = sqlite3.connect(DB_PATH, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=30000")
    return con

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    con = _db_connect()
    try:
        yield con
    finally:
        con.close()

def db_execute(sql: str, params: tuple = ()) -> sqlite3.Cursor:
    """Execute a single SQL statement with retry on lock."""
    attempt = 0
    while attempt < DB_RETRY_ATTEMPTS:
        with _db_lock:
            try:
                with get_db_connection() as con:
                    cur = con.execute(sql, params)
                    con.commit()
                    return cur
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    attempt += 1
                    wait = DB_RETRY_DELAY * (2 ** attempt)
                    log.warning("DB locked, retry %d/%d in %.2fs", attempt, DB_RETRY_ATTEMPTS, wait)
                    time.sleep(wait)
                    continue
                raise
    raise sqlite3.OperationalError(f"Database locked after {DB_RETRY_ATTEMPTS} attempts")

def db_fetchall(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """Fetch all rows with retry."""
    attempt = 0
    while attempt < DB_RETRY_ATTEMPTS:
        with _db_lock:
            try:
                with get_db_connection() as con:
                    cur = con.execute(sql, params)
                    return cur.fetchall()
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    attempt += 1
                    wait = DB_RETRY_DELAY * (2 ** attempt)
                    log.warning("DB locked (fetch), retry %d/%d in %.2fs", attempt, DB_RETRY_ATTEMPTS, wait)
                    time.sleep(wait)
                    continue
                raise
    raise sqlite3.OperationalError(f"Database locked after {DB_RETRY_ATTEMPTS} attempts")

def db_fetchone(sql: str, params: tuple = ()) -> sqlite3.Row | None:
    rows = db_fetchall(sql, params)
    return rows[0] if rows else None

def db_executemany(sql: str, params_list: list[tuple]) -> None:
    """Execute many statements with retry."""
    attempt = 0
    while attempt < DB_RETRY_ATTEMPTS:
        with _db_lock:
            try:
                with get_db_connection() as con:
                    con.executemany(sql, params_list)
                    con.commit()
                    return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    attempt += 1
                    wait = DB_RETRY_DELAY * (2 ** attempt)
                    log.warning("DB locked (executemany), retry %d/%d in %.2fs", attempt, DB_RETRY_ATTEMPTS, wait)
                    time.sleep(wait)
                    continue
                raise
    raise sqlite3.OperationalError(f"Database locked after {DB_RETRY_ATTEMPTS} attempts")

# ---- schema and init ----
def db_init() -> None:
    """Create schema if missing and run safe migrations with full retry."""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    attempt = 0
    while attempt < DB_RETRY_ATTEMPTS:
        with _db_lock:
            try:
                with get_db_connection() as con:
                    con.executescript("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id INTEGER PRIMARY KEY,
                            first_name TEXT,
                            last_name TEXT,
                            username TEXT,
                            language TEXT,
                            is_premium INTEGER DEFAULT 0,
                            chat_id INTEGER,
                            chat_title TEXT,
                            requested_at TEXT,
                            approved INTEGER DEFAULT 0,
                            welcomed INTEGER DEFAULT 0,
                            blocked INTEGER DEFAULT 0,
                            source TEXT,
                            first_seen TEXT,
                            last_seen TEXT
                        );

                        CREATE TABLE IF NOT EXISTS settings (
                            key TEXT PRIMARY KEY,
                            value TEXT
                        );

                        CREATE TABLE IF NOT EXISTS channels (
                            channel_id INTEGER PRIMARY KEY,
                            title TEXT,
                            username TEXT,
                            owner_user_id INTEGER,
                            added_at TEXT,
                            status TEXT DEFAULT 'ACTIVE',
                            can_invite INTEGER DEFAULT 0,
                            ctype TEXT
                        );

                        CREATE TABLE IF NOT EXISTS channel_settings (
                            channel_id INTEGER PRIMARY KEY,
                            auto_approve INTEGER DEFAULT 1,
                            welcome_enabled INTEGER DEFAULT 1,
                            welcome_text TEXT,
                            pending_text TEXT,
                            broadcast_enabled INTEGER DEFAULT 1,
                            logging_enabled INTEGER DEFAULT 1,
                            scheduler_enabled INTEGER DEFAULT 1,
                            FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
                        );

                        CREATE TABLE IF NOT EXISTS channel_members (
                            channel_id INTEGER,
                            user_id INTEGER,
                            first_name TEXT,
                            username TEXT,
                            approved INTEGER DEFAULT 0,
                            blocked INTEGER DEFAULT 0,
                            joined_at TEXT,
                            PRIMARY KEY(channel_id, user_id)
                        );

                        CREATE TABLE IF NOT EXISTS channel_managers (
                            channel_id INTEGER,
                            user_id INTEGER,
                            added_at TEXT,
                            PRIMARY KEY(channel_id, user_id),
                            FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
                        );

                        CREATE TABLE IF NOT EXISTS channel_manager_permissions (
                            channel_id INTEGER,
                            user_id INTEGER,
                            permission TEXT,
                            PRIMARY KEY(channel_id, user_id, permission)
                        );

                        CREATE TABLE IF NOT EXISTS join_requests (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            channel_id INTEGER,
                            user_id INTEGER,
                            status TEXT,
                            created_at TEXT
                        );

                        CREATE TABLE IF NOT EXISTS broadcasts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            owner_user_id INTEGER,
                            channel_id INTEGER,
                            kind TEXT,
                            text TEXT,
                            file_id TEXT,
                            caption TEXT,
                            copy_chat_id INTEGER,
                            copy_msg_id INTEGER,
                            audience TEXT,
                            status TEXT DEFAULT 'DRAFT',
                            total INTEGER DEFAULT 0,
                            sent INTEGER DEFAULT 0,
                            failed INTEGER DEFAULT 0,
                            blocked INTEGER DEFAULT 0,
                            created_at TEXT,
                            started_at TEXT,
                            finished_at TEXT
                        );

                        CREATE TABLE IF NOT EXISTS broadcast_buttons (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            broadcast_id INTEGER,
                            row_idx INTEGER,
                            col_idx INTEGER,
                            text TEXT,
                            btype TEXT,
                            value TEXT,
                            FOREIGN KEY(broadcast_id) REFERENCES broadcasts(id) ON DELETE CASCADE
                        );

                        CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            broadcast_id INTEGER,
                            scheduled_at TEXT,
                            timezone TEXT,
                            status TEXT DEFAULT 'SCHEDULED',
                            FOREIGN KEY(broadcast_id) REFERENCES broadcasts(id) ON DELETE CASCADE
                        );

                        CREATE TABLE IF NOT EXISTS events (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            actor_user_id INTEGER,
                            channel_id INTEGER,
                            action TEXT,
                            metadata TEXT,
                            created_at TEXT
                        );

                        CREATE INDEX IF NOT EXISTS ix_channels_owner ON channels(owner_user_id);
                        CREATE INDEX IF NOT EXISTS ix_cmembers_channel ON channel_members(channel_id);
                        CREATE INDEX IF NOT EXISTS ix_cmembers_user ON channel_members(user_id);
                        CREATE INDEX IF NOT EXISTS ix_managers_channel ON channel_managers(channel_id);
                        CREATE INDEX IF NOT EXISTS ix_bcast_owner ON broadcasts(owner_user_id);
                        CREATE INDEX IF NOT EXISTS ix_bcast_channel ON broadcasts(channel_id);
                        CREATE INDEX IF NOT EXISTS ix_bcast_status ON broadcasts(status);
                        CREATE INDEX IF NOT EXISTS ix_btargets_bcast ON broadcast_buttons(broadcast_id);
                        CREATE INDEX IF NOT EXISTS ix_events_created ON events(created_at);
                        CREATE INDEX IF NOT EXISTS ix_sched_status ON scheduled_broadcasts(status);
                    """)

                    # Migrations
                    def col_exists(table, col):
                        res = con.execute(f"PRAGMA table_info({table})").fetchall()
                        return any(r["name"] == col for r in res)

                    if col_exists("events", "created_at") is False:
                        con.execute("ALTER TABLE events ADD COLUMN created_at TEXT")
                        con.execute("UPDATE events SET created_at=? WHERE created_at IS NULL", (utcnow_iso(),))
                    if col_exists("join_requests", "created_at") is False:
                        con.execute("ALTER TABLE join_requests ADD COLUMN created_at TEXT")
                        con.execute("UPDATE join_requests SET created_at=? WHERE created_at IS NULL", (utcnow_iso(),))
                    for col in ("first_seen", "last_seen"):
                        if col_exists("users", col) is False:
                            con.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
                    con.execute(
                        "UPDATE users SET first_seen=COALESCE(first_seen, requested_at), "
                        "last_seen=COALESCE(last_seen, requested_at) "
                        "WHERE first_seen IS NULL OR last_seen IS NULL"
                    )

                    ver = con.execute("PRAGMA user_version").fetchone()[0]
                    if ver < SCHEMA_VERSION:
                        con.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

                    # Default settings
                    con.execute(
                        "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
                        ("welcome_text", DEFAULT_WELCOME)
                    )
                    con.execute(
                        "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
                        ("pending_text", DEFAULT_PENDING_MSG)
                    )
                    con.execute(
                        "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
                        ("auto_approve", os.getenv("AUTO_APPROVE", "true").lower())
                    )

                    con.commit()
                    log.info("DB ready at %s (schema v%s)", DB_PATH, SCHEMA_VERSION)
                    return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    attempt += 1
                    wait = DB_RETRY_DELAY * (2 ** attempt)
                    log.warning("DB init locked, retry %d/%d in %.2fs", attempt, DB_RETRY_ATTEMPTS, wait)
                    time.sleep(wait)
                    continue
                raise
    raise sqlite3.OperationalError(f"Database locked after {DB_RETRY_ATTEMPTS} attempts during init")

# ---- settings ----
def get_setting(key: str, default: str = "") -> str:
    row = db_fetchone("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else default

def set_setting(key: str, value: str) -> None:
    db_execute(
        "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )

def set_default(key: str, value: str) -> None:
    db_execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))

# ---- users ----
def save_user(user, chat=None, *, approved=0, welcomed=0, source="join_request") -> None:
    now = utcnow_iso()
    db_execute(
        """
        INSERT INTO users (user_id, first_name, last_name, username, language,
            is_premium, chat_id, chat_title, requested_at,
            approved, welcomed, source, first_seen, last_seen)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            username = excluded.username,
            language = excluded.language,
            chat_id = COALESCE(excluded.chat_id, users.chat_id),
            chat_title = COALESCE(excluded.chat_title, users.chat_title),
            approved = MAX(users.approved, excluded.approved),
            welcomed = MAX(users.welcomed, excluded.welcomed),
            blocked = 0,
            last_seen = excluded.last_seen
        """,
        (
            user.id, user.first_name, user.last_name, user.username,
            user.language_code, int(bool(getattr(user, "is_premium", False))),
            chat.id if chat else None, chat.title if chat else None,
            now, approved, welcomed, source, now, now,
        ),
    )

def mark(user_id: int, field_: str, value: int = 1) -> None:
    if field_ not in {"approved", "welcomed", "blocked"}:
        return
    db_execute(f"UPDATE users SET {field_}=? WHERE user_id=?", (value, user_id))

def log_event(action: str, *, actor_user_id: int | None = None,
              channel_id: int | None = None, metadata: str = "") -> None:
    db_execute(
        "INSERT INTO events(actor_user_id,channel_id,action,metadata,created_at) "
        "VALUES(?,?,?,?,?)",
        (actor_user_id, channel_id, action, metadata, utcnow_iso()),
    )

# ----------------------------------------------------------------------------
# ROLES & OWNERSHIP
# ----------------------------------------------------------------------------
PERMISSIONS = ["broadcast", "welcome", "analytics", "users", "settings", "logs"]

def is_super_admin(user_id: int) -> bool:
    return bool(ADMIN_IDS) and user_id in ADMIN_IDS

def get_channel(channel_id: int) -> sqlite3.Row | None:
    return db_fetchone(
        "SELECT * FROM channels WHERE channel_id=? AND status!='REMOVED'",
        (channel_id,)
    )

def user_owns_channel(user_id: int, channel_id: int) -> bool:
    ch = get_channel(channel_id)
    return bool(ch and ch["owner_user_id"] == user_id)

def user_is_manager(user_id: int, channel_id: int) -> bool:
    r = db_fetchone(
        "SELECT 1 FROM channel_managers WHERE channel_id=? AND user_id=?",
        (channel_id, user_id)
    )
    return r is not None

def manager_permissions(user_id: int, channel_id: int) -> set[str]:
    rows = db_fetchall(
        "SELECT permission FROM channel_manager_permissions "
        "WHERE channel_id=? AND user_id=?",
        (channel_id, user_id)
    )
    return {r["permission"] for r in rows}

def can_access_channel(user_id: int, channel_id: int) -> bool:
    if is_super_admin(user_id):
        return True
    if user_owns_channel(user_id, channel_id):
        return True
    return user_is_manager(user_id, channel_id)

def has_permission(user_id: int, channel_id: int, permission: str) -> bool:
    if is_super_admin(user_id):
        return True
    if user_owns_channel(user_id, channel_id):
        return True
    if user_is_manager(user_id, channel_id):
        return permission in manager_permissions(user_id, channel_id)
    return False

def user_channels(user_id: int) -> list[sqlite3.Row]:
    if is_super_admin(user_id):
        return db_fetchall(
            "SELECT * FROM channels WHERE status!='REMOVED' ORDER BY added_at DESC"
        )
    return db_fetchall(
        """
        SELECT * FROM channels
        WHERE status!='REMOVED' AND (
            owner_user_id=? OR
            channel_id IN (SELECT channel_id FROM channel_managers WHERE user_id=?)
        )
        ORDER BY added_at DESC
        """,
        (user_id, user_id),
    )

# ---- channel + per-channel settings ----
def channel_upsert(*, channel_id: int, title: str, username: str | None,
                   owner_user_id: int, can_invite: int, ctype: str) -> None:
    db_execute(
        """
        INSERT INTO channels(channel_id,title,username,owner_user_id,added_at,
            status,can_invite,ctype)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(channel_id) DO UPDATE SET
            title=excluded.title,
            username=excluded.username,
            status='ACTIVE',
            can_invite=excluded.can_invite,
            ctype=excluded.ctype
        """,
        (channel_id, title, username, owner_user_id, utcnow_iso(),
         "ACTIVE", can_invite, ctype),
    )
    db_execute(
        "INSERT OR IGNORE INTO channel_settings(channel_id, welcome_text, pending_text) VALUES(?,?,?)",
        (channel_id, DEFAULT_WELCOME, DEFAULT_PENDING_MSG),
    )

def channel_settings(channel_id: int) -> sqlite3.Row:
    row = db_fetchone("SELECT * FROM channel_settings WHERE channel_id=?", (channel_id,))
    if row is None:
        db_execute(
            "INSERT OR IGNORE INTO channel_settings(channel_id,welcome_text,pending_text) VALUES(?,?,?)",
            (channel_id, DEFAULT_WELCOME, DEFAULT_PENDING_MSG),
        )
        row = db_fetchone("SELECT * FROM channel_settings WHERE channel_id=?", (channel_id,))
    return row

def set_channel_setting(channel_id: int, key: str, value) -> None:
    allowed = {
        "auto_approve", "welcome_enabled", "welcome_text", "pending_text",
        "broadcast_enabled", "logging_enabled", "scheduler_enabled",
    }
    if key not in allowed:
        return
    db_execute(f"UPDATE channel_settings SET {key}=? WHERE channel_id=?", (value, channel_id))

def channel_stats(channel_id: int) -> dict:
    with get_db_connection() as con:
        total = con.execute("SELECT COUNT() FROM channel_members WHERE channel_id=?", (channel_id,)).fetchone()[0]
        appr = con.execute("SELECT COUNT() FROM channel_members WHERE channel_id=? AND approved=1", (channel_id,)).fetchone()[0]
        blk = con.execute("SELECT COUNT() FROM channel_members WHERE channel_id=? AND blocked=1", (channel_id,)).fetchone()[0]
        pend = con.execute("SELECT COUNT() FROM join_requests WHERE channel_id=? AND status='PENDING'", (channel_id,)).fetchone()[0]
    return {"total": total, "approved": appr, "blocked": blk, "pending": pend}

def channel_member_upsert(channel_id, user, *, approved=0) -> None:
    db_execute(
        """
        INSERT INTO channel_members(channel_id,user_id,first_name,username,
            approved,joined_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(channel_id,user_id) DO UPDATE SET
            first_name=excluded.first_name,
            username=excluded.username,
            approved=MAX(channel_members.approved, excluded.approved),
            blocked=0
        """,
        (channel_id, user.id, user.first_name, user.username, approved, utcnow_iso()),
    )

def channel_member_mark(channel_id: int, user_id: int, field_: str, value: int = 1) -> None:
    if field_ not in {"approved", "blocked"}:
        return
    db_execute(
        f"UPDATE channel_members SET {field_}=? WHERE channel_id=? AND user_id=?",
        (value, channel_id, user_id),
    )

# ---- global stats ----
def stats() -> dict:
    with get_db_connection() as con:
        total = con.execute("SELECT COUNT() FROM users").fetchone()[0]
        appr = con.execute("SELECT COUNT() FROM users WHERE approved=1").fetchone()[0]
        welc = con.execute("SELECT COUNT() FROM users WHERE welcomed=1").fetchone()[0]
        blk = con.execute("SELECT COUNT() FROM users WHERE blocked=1").fetchone()[0]
        today = con.execute("SELECT COUNT() FROM users WHERE substr(requested_at,1,10)=?", (today_iso(),)).fetchone()[0]
        channels = con.execute("SELECT COUNT() FROM channels WHERE status!='REMOVED'").fetchone()[0]
        campaigns = con.execute("SELECT COUNT(*) FROM broadcasts").fetchone()[0]
    return {"total": total, "approved": appr, "welcomed": welc, "blocked": blk,
            "today": today, "channels": channels, "campaigns": campaigns}

# ----------------------------------------------------------------------------
# TEXT HELPERS
# ----------------------------------------------------------------------------
def mention_of(user) -> str:
    name = html.escape(user.first_name or "Friend")
    return f'{name}'

def mention_raw(user_id: int, name: str) -> str:
    return f'{html.escape(name or "Friend")}'

def render(template: str, user, chat_title: str = "") -> str:
    uname = ("@" + user.username) if getattr(user, "username", None) else "—"
    return (
        template.replace("{mention}", mention_of(user))
        .replace("{name}", html.escape(user.first_name or "Friend"))
        .replace("{username}", uname)
        .replace("{id}", str(user.id))
        .replace("{chat}", html.escape(chat_title or "our channel"))
        .replace("{date}", datetime.now(timezone.utc).strftime("%d %b %Y"))
    )

def validate_template(text: str) -> tuple[bool, str]:
    """Validate Telegram HTML template for balanced tags and allowed attributes."""
    if not text.strip():
        return False, "Template khaali nahi ho sakta."

    # Allowed tags and their attributes
    allowed_tags = {"b", "i", "u", "s", "code", "pre", "a"}
    allowed_attrs = {"a": {"href"}}

    # Simple tag extraction using regex
    tag_re = re.compile(r'</?([a-zA-Z]+)(?:\s+[^>]*)?>')
    stack = []
    pos = 0
    # Find all tags
    for match in tag_re.finditer(text):
        tag = match.group(1).lower()
        # Check if tag is allowed
        if tag not in allowed_tags:
            return False, f"Tag <{tag}> is not allowed."
        # Check attributes (only for <a>)
        if tag == "a" and not match.group().startswith("</"):
            attrs = re.findall(r'(\w+)\s*=\s*("[^"]*"|\'[^\']*\'|[^\s>]+)', match.group())
            # we only allow href
            href_found = False
            for attr_name, attr_value in attrs:
                if attr_name == "href":
                    href_found = True
                    # Ensure href is a valid URL or tg://
                    val = attr_value.strip('"\'').strip()
                    if not (val.startswith("http://") or val.startswith("https://") or val.startswith("tg://")):
                        return False, "href must be a valid http/https or tg:// URL"
                else:
                    return False, f"Attribute '{attr_name}' not allowed on <a>"
            if not href_found:
                return False, "<a> tag must have href attribute"
        # Check closing tags
        if match.group().startswith("</"):
            if not stack or stack[-1] != tag:
                return False, f"Closing tag </{tag}> does not match opening tag."
            stack.pop()
        else:
            stack.append(tag)

    if stack:
        return False, f"Unclosed tags: {', '.join(stack)}"

    return True, "ok"

URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)

def valid_url(u: str) -> bool:
    return bool(URL_RE.match(u.strip())) or u.strip().startswith("tg://")

def bar(pct: float, width: int = 15) -> str:
    filled = int(round(pct / 100 * width))
    return "█" * filled + "░" * (width - filled)

def parse_channel_input(text: str) -> tuple[str | None, str | None]:
    t = text.strip()
    m = re.search(r"t.me/([A-Za-z0-9_]{4,})", t)
    if m:
        return "@" + m.group(1), "username"
    if t.startswith("@") and len(t) > 1:
        return t, "username"
    if re.fullmatch(r"-100\d{5,}", t):
        return t, "id"
    if re.fullmatch(r"-?\d{5,}", t):
        return t, "id"
    if re.fullmatch(r"[A-Za-z0-9_]{4,}", t):
        return "@" + t, "username"
    return None, None

# ----------------------------------------------------------------------------
# UI HELPERS
# ----------------------------------------------------------------------------
def nav_row(back: str = "home") -> list[InlineKeyboardButton]:
    return [
        InlineKeyboardButton("🏠 Home", callback_data="home"),
        InlineKeyboardButton("⬅️ Back", callback_data=back),
    ]

def kb(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(rows)

async def ui_edit(update: Update, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text, parse_mode=ParseMode.HTML,
                reply_markup=markup, disable_web_page_preview=True,
            )
            return
        except BadRequest as e:
            if "not modified" in str(e).lower():
                return
    target = update.effective_message
    await target.reply_html(text, reply_markup=markup, disable_web_page_preview=True)

def home_text(user) -> str:
    s = stats()
    chans = len(user_channels(user.id))
    admin_line = "\n🛡️ You have SUPER ADMIN access." if is_super_admin(user.id) else ""
    return (
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        f" 🚀 {html.escape(BOT_NAME)}\n"
        " CHANNEL MANAGEMENT\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"👋 Welcome, {mention_of(user)}\n\n"
        "Manage your Telegram channels, broadcast campaigns, automation, "
        "join requests and analytics from one place.\n\n"
        f"📢 Your channels: {chans}\n"
        f"👥 Total users: {s['total']:,}\n"
        f"📣 Campaigns: {s['campaigns']:,}"
        f"{admin_line}"
    )

def home_kb(user) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("➕ ADD CHANNEL", callback_data="ch:add")],
        [InlineKeyboardButton("📢 MY CHANNELS", callback_data="channels"),
         InlineKeyboardButton("📣 BROADCAST", callback_data="channels?bc")],
        [InlineKeyboardButton("📊 ANALYTICS", callback_data="channels?an"),
         InlineKeyboardButton("👥 USERS", callback_data="channels?us")],
        [InlineKeyboardButton("⚙️ SETTINGS", callback_data="channels?se"),
         InlineKeyboardButton("❓ HELP", callback_data="help")],
    ]
    if is_super_admin(user.id):
        rows.append([InlineKeyboardButton("🛡️ ADMIN PANEL", callback_data="admin")])
    return kb(rows)

async def show_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    save_user(user, source="start")
    await ui_edit(update, home_text(user), home_kb(user))

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📖 Help & Commands\n\n"
        "This bot lets you connect your own Telegram channels and manage join "
        "requests, welcome messages, broadcasts, scheduling, analytics and "
        "managers — all from inline buttons.\n\n"
        "Quick start\n"
        "1. Add this bot as admin in your channel (with Invite Users).\n"
        "2. Tap ➕ Add Channel and send your channel @username or ID.\n"
        "3. Open the dashboard to broadcast, edit welcome, view analytics.\n\n"
        "Commands\n"
        "/start — open home\n"
        "/help — this help\n"
        "/id — your & chat ID\n\n"
        "Admin/legacy\n"
        "/panel /stats /users /export /broadcast /settings /pending\n"
        "/setwelcome /getwelcome /autoapprove\n\n"
        "Placeholders: {mention} {name} {username} {id} {chat} {date}\n"
        "HTML: <b> <i> <u> <code> <a href>"
    )
    await ui_edit(update, text, kb([nav_row()]))

# ----------------------------------------------------------------------------
# CHANNEL ONBOARDING
# ----------------------------------------------------------------------------
def _ud(context) -> dict:
    return context.user_data

HOW_TO_ADD = (
    "📖 How to add the bot to your channel\n\n"
    "1. Open your channel → Manage Channel → Administrators.\n"
    "2. Tap Add Admin and search this bot's username.\n"
    "3. Enable at least Add Members / Invite Users "
    "(needed to approve join requests).\n"
    "4. Save. Then come back and tap Add Channel → send your "
    "@username or channel ID."
)

async def ch_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _ud(context)["flow"] = "add_channel"
    text = (
        "➕ Add Channel — Step 1\n\n"
        "Please add this bot as administrator to your Telegram channel "
        "with Add Members / Invite Users permission.\n\n"
        "When done, send me the channel below in any of these forms:\n"
        "• @channelusername\n"
        "• https://t.me/channelusername\n"
        "• -100xxxxxxxxxx"
    )
    rows = [
        [InlineKeyboardButton("📖 How to Add Bot", callback_data="ch:how")],
        [InlineKeyboardButton("❌ Cancel", callback_data="ch:cancel")],
        nav_row(),
    ]
    await ui_edit(update, text, kb(rows))

async def ch_how(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ui_edit(update, HOW_TO_ADD, kb([
        [InlineKeyboardButton("⬅️ Back", callback_data="ch:add")],
        [InlineKeyboardButton("🏠 Home", callback_data="home")]]))

async def ch_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _ud(context).pop("flow", None)
    await show_home(update, context)

async def verify_and_store_channel(context, ref: str, owner_id: int) -> tuple[bool, str, dict | None]:
    try:
        chat = await context.bot.get_chat(ref)
    except (BadRequest, Forbidden) as e:
        return False, (
            "❌ Channel nahi mila ya bot ko access nahi hai.\n"
            "Make sure the bot is added as admin and the @username/ID is correct.\n"
            f"({html.escape(str(e))})"
        ), None
    except TelegramError as e:
        return False, f"❌ Telegram error: {html.escape(str(e))}", None

    if chat.type not in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
        return False, "❌ Ye ek channel/group nahi hai.", None

    try:
        me = await context.bot.get_me()
        member = await context.bot.get_chat_member(chat.id, me.id)
    except TelegramError as e:
        return False, f"❌ Bot admin status check nahi ho paaya: <i>{html.escape(str(e))}</i>", None

    if member.status not in ("administrator", "creator"):
        return False, (
            "⚠️ Bot abhi is channel me <b>admin nahi</b> hai.\n"
            "Pehle bot ko admin banao, phir <b>Check Again</b> dabao."
        ), None

    can_invite = int(bool(getattr(member, "can_invite_users", False)) or member.status == "creator")
    channel_upsert(
        channel_id=chat.id,
        title=chat.title or "Channel",
        username=("@" + chat.username) if chat.username else None,
        owner_user_id=owner_id,
        can_invite=can_invite,
        ctype=str(chat.type),
    )
    log_event("CHANNEL_ADDED", actor_user_id=owner_id, channel_id=chat.id, metadata=chat.title or "")
    info = {
        "id": chat.id,
        "title": chat.title or "Channel",
        "username": ("@" + chat.username) if chat.username else "—",
        "can_invite": can_invite,
    }
    return True, "ok", info

async def ch_finish_add(update: Update, context: ContextTypes.DEFAULT_TYPE, ref: str) -> None:
    user = update.effective_user
    ok, msg, info = await verify_and_store_channel(context, ref, user.id)
    if not ok:
        rows = [
            [InlineKeyboardButton("🔄 Check Again", callback_data="ch:recheck")],
            [InlineKeyboardButton("📖 How to Add Bot", callback_data="ch:how")],
            [InlineKeyboardButton("❌ Cancel", callback_data="ch:cancel")],
        ]
        _ud(context)["pending_ref"] = ref
        await update.effective_message.reply_html(msg, reply_markup=kb(rows))
        return

    _ud(context).pop("flow", None)
    _ud(context).pop("pending_ref", None)
    text = (
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        " ✅ <b>CHANNEL CONNECTED</b>\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"📢 Name: <b>{html.escape(info['title'])}</b>\n"
        f"🆔 ID: <code>{info['id']}</code>\n"
        f"👑 Owner: You\n"
        f"🛡 Bot Admin: {'YES' if info['can_invite'] else 'YES (limited)'}\n"
        f"🟢 Status: ACTIVE"
    )
    cid = info["id"]
    rows = [
        [InlineKeyboardButton("⚙️ Manage", callback_data=f"ch:{cid}")],
        [InlineKeyboardButton("📣 Broadcast", callback_data=f"bc:new:{cid}"),
         InlineKeyboardButton("📊 Analytics", callback_data=f"an:{cid}")],
        nav_row("channels"),
    ]
    await update.effective_message.reply_html(text, reply_markup=kb(rows))

async def ch_recheck(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ref = _ud(context).get("pending_ref")
    if not ref:
        await ch_add_start(update, context)
        return
    await ch_finish_add(update, context, ref)

# ----------------------------------------------------------------------------
# CHANNEL LIST / SWITCHER
# ----------------------------------------------------------------------------
async def show_channels(update: Update, context: ContextTypes.DEFAULT_TYPE,
                        action: str = "") -> None:
    user = update.effective_user
    chans = user_channels(user.id)
    if not chans:
        text = (
            "📢 My Channels\n\n"
            "Abhi koi channel connected nahi hai.\n"
            "Tap ➕ Add Channel to connect your first one."
        )
        await ui_edit(update, text, kb([
            [InlineKeyboardButton("➕ Add Channel", callback_data="ch:add")],
            nav_row(),
        ]))
        return

    titles = {"": "ch", "bc": "bc:new", "an": "an", "us": "us", "se": "se"}
    prefix = titles.get(action, "ch")
    header = {
        "": "📢 <b>SELECT CHANNEL</b>",
        "bc": "📣 <b>BROADCAST — select channel</b>",
        "an": "📊 <b>ANALYTICS — select channel</b>",
        "us": "👥 <b>USERS — select channel</b>",
        "se": "⚙️ <b>SETTINGS — select channel</b>",
    }.get(action, "📢 <b>SELECT CHANNEL</b>")

    rows = []
    for ch in chans:
        badge = "🟢" if ch["status"] == "ACTIVE" else "⚠️"
        rows.append([InlineKeyboardButton(
            f"{badge} {ch['title']}", callback_data=f"{prefix}:{ch['channel_id']}")])
    rows.append([InlineKeyboardButton("➕ Add Channel", callback_data="ch:add")])
    rows.append(nav_row())
    await ui_edit(update, header, kb(rows))

# ----------------------------------------------------------------------------
# CHANNEL DASHBOARD
# ----------------------------------------------------------------------------
async def show_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         channel_id: int) -> None:
    user = update.effective_user
    if not can_access_channel(user.id, channel_id):
        await deny(update, "Aapke paas is channel ka access nahi hai.")
        return
    ch = get_channel(channel_id)
    if not ch:
        await deny(update, "Channel nahi mila.")
        return
    _ud(context)["current_channel"] = channel_id
    s = channel_stats(channel_id)

    status_note = "" if ch["status"] != "ACTIVE" else f"\n⚠️ <b>Status:</b> {ch['status']} — recheck karein."
    text = (
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        "📢 <b>CHANNEL DASHBOARD</b>\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"📢 <b>{html.escape(ch['title'])}</b>\n"
        f"🆔 <code>{ch['channel_id']}</code>\n\n"
        "📊 <b>Overview</b>\n"
        f"👥 Users: <b>{s['total']:,}</b>\n"
        f"✅ Approved: <b>{s['approved']:,}</b>\n"
        f"⏳ Pending: <b>{s['pending']:,}</b>\n"
        f"🚫 Blocked: <b>{s['blocked']:,}</b>"
        f"{status_note}"
    )
    cid = channel_id
    rows = [
        [InlineKeyboardButton("📣 Broadcast", callback_data=f"bc:new:{cid}"),
         InlineKeyboardButton("📝 Welcome", callback_data=f"wel:{cid}"),
         InlineKeyboardButton("🔘 Buttons", callback_data=f"wbtn:{cid}")],
        [InlineKeyboardButton("👥 Users", callback_data=f"us:{cid}"),
         InlineKeyboardButton("📊 Analytics", callback_data=f"an:{cid}"),
         InlineKeyboardButton("📋 Logs", callback_data=f"logs:{cid}")],
        [InlineKeyboardButton("⏰ Scheduled", callback_data=f"sched:{cid}"),
         InlineKeyboardButton("⚙️ Settings", callback_data=f"se:{cid}")],
        [InlineKeyboardButton("👥 Managers", callback_data=f"mgr:{cid}"),
         InlineKeyboardButton("🗑 Remove", callback_data=f"ch:rm:{cid}")],
        nav_row("channels"),
    ]
    await ui_edit(update, text, kb(rows))

async def deny(update: Update, msg: str) -> None:
    text = f"⛔ Access denied\n\n{msg}"
    if update.callback_query:
        await update.callback_query.answer(msg, show_alert=True)
        try:
            await ui_edit(update, text, kb([nav_row()]))
        except Exception:
            pass
    else:
        await update.effective_message.reply_html(text, reply_markup=kb([nav_row()]))

# ----------------------------------------------------------------------------
# WELCOME BUILDER
# ----------------------------------------------------------------------------
async def show_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if not has_permission(user.id, cid, "welcome"):
        await deny(update, "Welcome edit permission nahi hai.")
        return
    cs = channel_settings(cid)
    enabled = bool(cs["welcome_enabled"])
    preview = (cs["welcome_text"] or DEFAULT_WELCOME)[:400]
    text = (
        "📝 WELCOME MESSAGE\n\n"
        f"State: {'🟢 Enabled' if enabled else '🔴 Disabled'}\n\n"
        "Current template:\n"
        f"{html.escape(preview)}\n\n"
        "Variables: {name} {mention} {username} {id} {chat} {date}"
    )
    rows = [
        [InlineKeyboardButton("✏️ Edit", callback_data=f"wel:edit:{cid}"),
         InlineKeyboardButton("👁 Preview", callback_data=f"wel:prev:{cid}")],
        [InlineKeyboardButton("🔄 Reset", callback_data=f"wel:reset:{cid}"),
         InlineKeyboardButton("🔴 Disable" if enabled else "🟢 Enable",
                              callback_data=f"wel:toggle:{cid}")],
        nav_row(f"ch:{cid}"),
    ]
    await ui_edit(update, text, kb(rows))

async def welcome_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    _ud(context)["flow"] = "set_welcome"
    _ud(context)["flow_cid"] = cid
    await ui_edit(update,
                  "✏️ Send the new welcome text\n\n"
                  "HTML allowed. Variables: {name} {mention} {username} {id} {chat} {date}\n\n"
                  "Send /cancel to abort.",
                  kb([nav_row(f"wel:{cid}")]))

async def welcome_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    cs = channel_settings(cid)
    user = update.effective_user
    ch = get_channel(cid)
    rendered = render(cs["welcome_text"] or DEFAULT_WELCOME, user, ch["title"] if ch else "")
    await ui_edit(update, "👁 Preview\n\n" + rendered, kb([nav_row(f"wel:{cid}")]))

# ----------------------------------------------------------------------------
# PER-CHANNEL SETTINGS
# ----------------------------------------------------------------------------
SETTING_TOGGLES = [
    ("auto_approve", "🔄 Auto Approve"),
    ("welcome_enabled", "📝 Welcome"),
    ("broadcast_enabled", "📣 Broadcast"),
    ("logging_enabled", "📋 Logging"),
    ("scheduler_enabled", "⏰ Scheduler"),
]

async def show_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if not has_permission(user.id, cid, "settings"):
        await deny(update, "Settings permission nahi hai.")
        return
    cs = channel_settings(cid)

    def dot(v): return "🟢" if v else "🔴"
    text = (
        "⚙️ <b>CHANNEL SETTINGS</b>\n\n"
        f"Auto Approve: {dot(cs['auto_approve'])}\n"
        f"Welcome: {dot(cs['welcome_enabled'])}\n"
        f"Broadcast: {dot(cs['broadcast_enabled'])}\n"
        f"Logging: {dot(cs['logging_enabled'])}\n"
        f"Scheduler: {dot(cs['scheduler_enabled'])}\n\n"
        "<i>Tap to toggle. These apply only to this channel.</i>"
    )
    rows = []
    for key, label in SETTING_TOGGLES:
        state = "🟢" if cs[key] else "🔴"
        rows.append([InlineKeyboardButton(f"{state} {label}", callback_data=f"se:tg:{key}:{cid}")])
    rows.append(nav_row(f"ch:{cid}"))
    await ui_edit(update, text, kb(rows))

async def settings_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          key: str, cid: int) -> None:
    user = update.effective_user
    if not has_permission(user.id, cid, "settings"):
        await deny(update, "Settings permission nahi hai.")
        return
    cs = channel_settings(cid)
    new = 0 if cs[key] else 1
    set_channel_setting(cid, key, new)
    log_event("SETTINGS_UPDATED", actor_user_id=user.id, channel_id=cid,
              metadata=f"{key}={new}")
    await show_settings(update, context, cid)

# ----------------------------------------------------------------------------
# MANAGERS + PERMISSIONS
# ----------------------------------------------------------------------------
async def show_managers(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if not (is_super_admin(user.id) or user_owns_channel(user.id, cid)):
        await deny(update, "Sirf channel owner managers add kar sakta hai.")
        return
    mgrs = db_fetchall(
        "SELECT user_id FROM channel_managers WHERE channel_id=?", (cid,)
    )
    lines = ["👥 MANAGERS\n"]
    if not mgrs:
        lines.append("Abhi koi manager nahi hai.")
    else:
        for m in mgrs:
            perms = manager_permissions(m["user_id"], cid)
            lines.append(
                f"• {m['user_id']} — {', '.join(sorted(perms)) or 'no permissions'}")
    rows = [
        [InlineKeyboardButton("➕ Add Manager", callback_data=f"mgr:add:{cid}")],
    ]
    for m in mgrs:
        rows.append([
            InlineKeyboardButton(f"🔧 {m['user_id']}", callback_data=f"mgr:perm:{m['user_id']}:{cid}"),
            InlineKeyboardButton("🗑", callback_data=f"mgr:del:{m['user_id']}:{cid}"),
        ])
    rows.append(nav_row(f"ch:{cid}"))
    await ui_edit(update, "\n".join(lines), kb(rows))

async def manager_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    _ud(context)["flow"] = "add_manager"
    _ud(context)["flow_cid"] = cid
    await ui_edit(update,
                  "➕ Add Manager\n\n"
                  "Send the user's numeric Telegram ID (they must have started the bot).\n"
                  "Send /cancel to abort.",
                  kb([nav_row(f"mgr:{cid}")]))

async def manager_perm_screen(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              mgr_id: int, cid: int) -> None:
    user = update.effective_user
    if not (is_super_admin(user.id) or user_owns_channel(user.id, cid)):
        await deny(update, "Sirf owner permissions change kar sakta hai.")
        return
    perms = manager_permissions(mgr_id, cid)
    text = (
        f"🔧 Permissions for {mgr_id}\n\n"
        "Tap to toggle each permission for this channel."
    )
    rows = []
    for p in PERMISSIONS:
        state = "🟢" if p in perms else "🔴"
        rows.append([InlineKeyboardButton(f"{state} {p}",
                                          callback_data=f"mgr:tg:{p}:{mgr_id}:{cid}")])
    rows.append(nav_row(f"mgr:{cid}"))
    await ui_edit(update, text, kb(rows))

async def manager_perm_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              perm: str, mgr_id: int, cid: int) -> None:
    user = update.effective_user
    if not (is_super_admin(user.id) or user_owns_channel(user.id, cid)):
        await deny(update, "Permission change denied.")
        return
    if perm not in PERMISSIONS:
        return
    perms = manager_permissions(mgr_id, cid)
    if perm in perms:
        db_execute(
            "DELETE FROM channel_manager_permissions "
            "WHERE channel_id=? AND user_id=? AND permission=?",
            (cid, mgr_id, perm)
        )
    else:
        db_execute(
            "INSERT OR IGNORE INTO channel_manager_permissions"
            "(channel_id,user_id,permission) VALUES(?,?,?)",
            (cid, mgr_id, perm)
        )
    await manager_perm_screen(update, context, mgr_id, cid)

async def manager_delete(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         mgr_id: int, cid: int) -> None:
    user = update.effective_user
    if not (is_super_admin(user.id) or user_owns_channel(user.id, cid)):
        await deny(update, "Delete denied.")
        return
    db_execute("DELETE FROM channel_managers WHERE channel_id=? AND user_id=?", (cid, mgr_id))
    db_execute("DELETE FROM channel_manager_permissions WHERE channel_id=? AND user_id=?", (cid, mgr_id))
    log_event("MANAGER_REMOVED", actor_user_id=user.id, channel_id=cid, metadata=str(mgr_id))
    await show_managers(update, context, cid)

# ----------------------------------------------------------------------------
# ANALYTICS
# ----------------------------------------------------------------------------
def _period_cutoff(period: str) -> str | None:
    now = datetime.now(timezone.utc)
    if period == "24H":
        return (now - timedelta(hours=24)).isoformat()
    if period == "7D":
        return (now - timedelta(days=7)).isoformat()
    if period == "30D":
        return (now - timedelta(days=30)).isoformat()
    return None

async def show_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         cid: int, period: str = "7D") -> None:
    user = update.effective_user
    if not has_permission(user.id, cid, "analytics"):
        await deny(update, "Analytics permission nahi hai.")
        return
    cutoff = _period_cutoff(period)
    with get_db_connection() as con:
        base = "SELECT COUNT(*) FROM channel_members WHERE channel_id=?"
        args = [cid]
        if cutoff:
            new_users = con.execute(base + " AND joined_at>=?", (cid, cutoff)).fetchone()[0]
        else:
            new_users = con.execute(base, (cid,)).fetchone()[0]
        total = con.execute(base, (cid,)).fetchone()[0]
        approved = con.execute(base + " AND approved=1", (cid,)).fetchone()[0]
        blocked = con.execute(base + " AND blocked=1", (cid,)).fetchone()[0]

        jr_base = "SELECT COUNT(*) FROM join_requests WHERE channel_id=?"
        if cutoff:
            joins = con.execute(jr_base + " AND created_at>=?", (cid, cutoff)).fetchone()[0]
            approvals = con.execute(
                jr_base + " AND status='APPROVED' AND created_at>=?", (cid, cutoff)
            ).fetchone()[0]
        else:
            joins = con.execute(jr_base, (cid,)).fetchone()[0]
            approvals = con.execute(jr_base + " AND status='APPROVED'", (cid,)).fetchone()[0]

        bc_base = "SELECT COUNT(*) FROM broadcasts WHERE channel_id=?"
        bsent = con.execute("SELECT COALESCE(SUM(sent),0), COALESCE(SUM(total),0) "
                            "FROM broadcasts WHERE channel_id=?", (cid,)).fetchone()
        if cutoff:
            bc_count = con.execute(bc_base + " AND created_at>=?", (cid, cutoff)).fetchone()[0]
        else:
            bc_count = con.execute(bc_base, (cid,)).fetchone()[0]
        success = (bsent[0] / bsent[1] * 100) if bsent[1] else 0.0
        active = total - blocked

    text = (
        "📊 <b>ANALYTICS</b>\n"
        f"Period: <b>{period}</b>\n\n"
        f"👥 Total users: <b>{total:,}</b>\n"
        f"🟢 Active users: <b>{active:,}</b>\n"
        f"🚫 Blocked users: <b>{blocked:,}</b>\n"
        f"📈 New (this period): <b>{new_users:,}</b>\n"
        f"📥 Join requests: <b>{joins:,}</b>\n"
        f"✅ Approved: <b>{approvals:,}</b>\n"
        f"📣 Broadcasts: <b>{bc_count:,}</b>\n"
        f"📊 Broadcast success rate: <b>{success:.1f}%</b>"
    )
    rows = [
        [InlineKeyboardButton("24H", callback_data=f"an:{cid}:24H"),
         InlineKeyboardButton("7D", callback_data=f"an:{cid}:7D"),
         InlineKeyboardButton("30D", callback_data=f"an:{cid}:30D"),
         InlineKeyboardButton("ALL", callback_data=f"an:{cid}:ALL")],
        nav_row(f"ch:{cid}"),
    ]
    await ui_edit(update, text, kb(rows))

# ----------------------------------------------------------------------------
# BROADCAST — composition, buttons, targets, preview
# ----------------------------------------------------------------------------
AUDIENCE_LABELS = {
    "ALL": "👥 All users",
    "ACTIVE": "🟢 Active users",
    "EXCLUDE_BLOCKED": "🚫 Exclude blocked",
    "TODAY": "📅 Today",
    "LAST7": "📅 Last 7 days",
    "OWNERS": "👑 Channel owners",
}

def create_broadcast(owner_user_id: int, channel_id: int | None) -> int:
    cur = db_execute(
        "INSERT INTO broadcasts(owner_user_id,channel_id,kind,audience,status,created_at) "
        "VALUES(?,?,?,?,?,?)",
        (owner_user_id, channel_id, "text", "EXCLUDE_BLOCKED", "DRAFT", utcnow_iso()),
    )
    bid = cur.lastrowid
    log_event("BROADCAST_CREATED", actor_user_id=owner_user_id, channel_id=channel_id,
              metadata=f"broadcast={bid}")
    return bid

def get_broadcast(bid: int) -> sqlite3.Row | None:
    return db_fetchone("SELECT * FROM broadcasts WHERE id=?", (bid,))

def owns_broadcast(user_id: int, bid: int) -> bool:
    b = get_broadcast(bid)
    if not b:
        return False
    if is_super_admin(user_id):
        return True
    if b["owner_user_id"] == user_id:
        return True
    if b["channel_id"] and has_permission(user_id, b["channel_id"], "broadcast"):
        return True
    return False

def update_broadcast(bid: int, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    db_execute(f"UPDATE broadcasts SET {cols} WHERE id=?", (*fields.values(), bid))

# ---- audience resolution ----
def resolve_audience_ids(b: sqlite3.Row) -> list[int]:
    aud = b["audience"] or "EXCLUDE_BLOCKED"
    cid = b["channel_id"]
    if cid:
        q = "SELECT user_id FROM channel_members WHERE channel_id=?"
        args = [cid]
        if aud in ("ACTIVE", "EXCLUDE_BLOCKED"):
            q += " AND blocked=0"
        elif aud == "TODAY":
            q += " AND substr(joined_at,1,10)=?"
            args.append(today_iso())
        elif aud == "LAST7":
            q += " AND joined_at>=?"
            args.append((datetime.now(timezone.utc) - timedelta(days=7)).isoformat())
        rows = db_fetchall(q, tuple(args))
    else:
        if aud == "OWNERS":
            rows = db_fetchall(
                "SELECT DISTINCT owner_user_id AS user_id FROM channels "
                "WHERE status!='REMOVED' AND owner_user_id IS NOT NULL"
            )
        else:
            q = "SELECT user_id FROM users"
            args = []
            if aud in ("ACTIVE", "EXCLUDE_BLOCKED"):
                q += " WHERE blocked=0"
            elif aud == "TODAY":
                q += " WHERE substr(requested_at,1,10)=?"
                args.append(today_iso())
            elif aud == "LAST7":
                q += " WHERE requested_at>=?"
                args.append((datetime.now(timezone.utc) - timedelta(days=7)).isoformat())
            rows = db_fetchall(q, tuple(args))
    return list({r["user_id"] for r in rows if r["user_id"]})

def audience_estimate(b: sqlite3.Row) -> tuple[int, int]:
    ids = resolve_audience_ids(b)
    cid = b["channel_id"]
    if cid:
        blocked = db_fetchone(
            "SELECT COUNT() FROM channel_members WHERE channel_id=? AND blocked=1",
            (cid,)
        )[0]
    else:
        blocked = db_fetchone("SELECT COUNT() FROM users WHERE blocked=1")[0]
    return len(ids), blocked

# ---- broadcast buttons ----
def broadcast_buttons(bid: int) -> list[sqlite3.Row]:
    return db_fetchall(
        "SELECT * FROM broadcast_buttons WHERE broadcast_id=? ORDER BY row_idx,col_idx",
        (bid,)
    )

def build_button_markup(bid: int) -> InlineKeyboardMarkup | None:
    rows_map: dict[int, list[InlineKeyboardButton]] = {}
    for b in broadcast_buttons(bid):
        if b["btype"] == "url":
            btn = InlineKeyboardButton(b["text"], url=b["value"])
        else:
            btn = InlineKeyboardButton(b["text"], callback_data=f"noop:{b['id']}")
        rows_map.setdefault(b["row_idx"], []).append(btn)
    if not rows_map:
        return None
    rows = [rows_map[k] for k in sorted(rows_map)]
    return InlineKeyboardMarkup(rows)

async def bc_new(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int | None) -> None:
    user = update.effective_user
    if cid is not None:
        if not has_permission(user.id, cid, "broadcast"):
            await deny(update, "Broadcast permission nahi hai.")
            return
        cs = channel_settings(cid)
        if not cs["broadcast_enabled"]:
            await deny(update, "Is channel me broadcast disabled hai.")
            return
    bid = create_broadcast(user.id, cid)
    _ud(context)["flow"] = "bc_content"
    _ud(context)["flow_bid"] = bid
    text = (
        "📣 NEW BROADCAST\n\n"
        "Send the content now — I accept any of:\n"
        "📝 Text · 🖼 Photo · 🎥 Video · 📄 Document · 🎵 Audio · 🎞 Animation/GIF\n\n"
        "For media, add a caption if you want (HTML allowed).\n"
        "Or forward/reply to a message and I'll copy it exactly.\n\n"
        "Send /cancel to abort."
    )
    await ui_edit(update, text, kb([nav_row(f"ch:{cid}" if cid else "home")]))

def _summ_kind(b) -> str:
    return {
        "text": "📝 Text", "photo": "🖼 Photo", "video": "🎥 Video",
        "document": "📄 Document", "audio": "🎵 Audio",
        "animation": "🎞 Animation", "copy": "📋 Copy message",
    }.get(b["kind"], b["kind"])

async def bc_show_compose(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = get_broadcast(bid)
    if not b:
        await deny(update, "Broadcast nahi mila.")
        return
    eligible, blocked = audience_estimate(b)
    btn_count = len(broadcast_buttons(bid))
    text = (
        "📣 BROADCAST — compose\n\n"
        f"Type: {_summ_kind(b)}\n"
        f"🎯 Audience: {AUDIENCE_LABELS.get(b['audience'], b['audience'])} "
        f"(~{eligible:,})\n"
        f"🔘 Buttons: {btn_count}\n\n"
        "Use the controls below, then Preview."
    )
    rows = [
        [InlineKeyboardButton("✏️ Edit content", callback_data=f"bc:edit:{bid}")],
        [InlineKeyboardButton("🎯 Audience", callback_data=f"bc:aud:{bid}"),
         InlineKeyboardButton("🔘 Buttons", callback_data=f"bc:btns:{bid}")],
        [InlineKeyboardButton("👁 Preview", callback_data=f"bc:prev:{bid}")],
        [InlineKeyboardButton("⏰ Schedule", callback_data=f"bc:sch:{bid}"),
         InlineKeyboardButton("❌ Cancel", callback_data=f"bc:del:{bid}")],
        nav_row(f"ch:{b['channel_id']}" if b["channel_id"] else "admin"),
    ]
    await ui_edit(update, text, kb(rows))

# ---- target selector ----
async def bc_audience(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = get_broadcast(bid)
    if not b:
        await deny(update, "Broadcast nahi mila.")
        return
    eligible, blocked = audience_estimate(b)
    est = max(0, eligible)
    text = (
        "🎯 SELECT AUDIENCE\n\n"
        f"Current: {AUDIENCE_LABELS.get(b['audience'], b['audience'])}\n\n"
        f"Total eligible: {eligible:,}\n"
        f"Blocked: {blocked:,}\n"
        f"Estimated delivery: {est:,}"
    )
    is_global = b["channel_id"] is None
    opts = (["ALL", "ACTIVE", "EXCLUDE_BLOCKED", "OWNERS"] if is_global
            else ["ALL", "ACTIVE", "EXCLUDE_BLOCKED", "TODAY", "LAST7"])
    rows = []
    for a in opts:
        mark_ = "✅ " if a == b["audience"] else ""
        rows.append([InlineKeyboardButton(mark_ + AUDIENCE_LABELS[a],
                                          callback_data=f"bc:setaud:{a}:{bid}")])
    rows.append([InlineKeyboardButton("➡️ Continue", callback_data=f"bc:compose:{bid}")])
    rows.append(nav_row(f"bc:compose:{bid}"))
    await ui_edit(update, text, kb(rows))

# ---- button builder ----
def get_button_builder_data(bid: int) -> tuple[str, InlineKeyboardMarkup]:
    btns = broadcast_buttons(bid)
    if btns:
        preview_rows = {}
        for b in btns:
            preview_rows.setdefault(b["row_idx"], []).append(f"[{b['text']}]")
        layout = "\n".join(" ".join(preview_rows[k]) for k in sorted(preview_rows))
    else:
        layout = "(no buttons yet)"
    text = (
        "🔘 BUTTON BUILDER\n\n"
        "Current layout:\n"
        f"{layout}\n\n"
        "URL buttons open a link. Callback buttons carry only a safe token — "
        "never sensitive data."
    )
    rows = [
        [InlineKeyboardButton("➕ Add Button", callback_data=f"bc:baddrow:{bid}"),
         InlineKeyboardButton("↔️ Add to last row", callback_data=f"bc:baddcol:{bid}")],
        [InlineKeyboardButton("🗑 Delete last", callback_data=f"bc:bdel:{bid}"),
         InlineKeyboardButton("👁 Preview", callback_data=f"bc:prev:{bid}")],
        [InlineKeyboardButton("✅ Done", callback_data=f"bc:compose:{bid}")],
        nav_row(f"bc:compose:{bid}"),
    ]
    return text, kb(rows)

async def bc_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    text, markup = get_button_builder_data(bid)
    await ui_edit(update, text, markup)

async def show_button_builder_message(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    text, markup = get_button_builder_data(bid)
    await update.effective_message.reply_html(text, reply_markup=markup)

async def bc_add_button_start(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              bid: int, new_row: bool) -> None:
    _ud(context)["flow"] = "bc_btn_text"
    _ud(context)["flow_bid"] = bid
    _ud(context)["btn_new_row"] = new_row
    await ui_edit(update,
                  "🔘 Add Button\n\nSend the button text.\n"
                  "Send /cancel to abort.",
                  kb([nav_row(f"bc:btns:{bid}")]))

def add_button(bid: int, text: str, btype: str, value: str, new_row: bool) -> None:
    maxrow = db_fetchone(
        "SELECT COALESCE(MAX(row_idx),-1) FROM broadcast_buttons WHERE broadcast_id=?",
        (bid,)
    )[0]
    if new_row or maxrow < 0:
        row_idx = maxrow + 1
        col_idx = 0
    else:
        row_idx = maxrow
        col_idx = db_fetchone(
            "SELECT COALESCE(MAX(col_idx),-1)+1 FROM broadcast_buttons "
            "WHERE broadcast_id=? AND row_idx=?", (bid, row_idx)
        )[0]
    db_execute(
        "INSERT INTO broadcast_buttons(broadcast_id,row_idx,col_idx,text,btype,value) "
        "VALUES(?,?,?,?,?,?)",
        (bid, row_idx, col_idx, text, btype, value)
    )

def delete_last_button(bid: int) -> None:
    last = db_fetchone(
        "SELECT id FROM broadcast_buttons WHERE broadcast_id=? "
        "ORDER BY row_idx DESC, col_idx DESC LIMIT 1", (bid,)
    )
    if last:
        db_execute("DELETE FROM broadcast_buttons WHERE id=?", (last["id"],))

# ----------------------------------------------------------------------------
# BROADCAST — preview, send delivery for one recipient
# ----------------------------------------------------------------------------
async def deliver_one(bot, b: sqlite3.Row, uid: int,
                      markup: InlineKeyboardMarkup | None) -> None:
    kind = b["kind"]
    if kind == "copy" and b["copy_chat_id"] and b["copy_msg_id"]:
        await bot.copy_message(chat_id=uid, from_chat_id=b["copy_chat_id"],
                               message_id=b["copy_msg_id"], reply_markup=markup)
        return
    cap = b["caption"] or None
    if kind == "text":
        await bot.send_message(uid, b["text"] or "", parse_mode=ParseMode.HTML,
                               reply_markup=markup, disable_web_page_preview=True)
    elif kind == "photo":
        await bot.send_photo(uid, b["file_id"], caption=cap,
                             parse_mode=ParseMode.HTML, reply_markup=markup)
    elif kind == "video":
        await bot.send_video(uid, b["file_id"], caption=cap,
                             parse_mode=ParseMode.HTML, reply_markup=markup)
    elif kind == "document":
        await bot.send_document(uid, b["file_id"], caption=cap,
                                parse_mode=ParseMode.HTML, reply_markup=markup)
    elif kind == "audio":
        await bot.send_audio(uid, b["file_id"], caption=cap,
                             parse_mode=ParseMode.HTML, reply_markup=markup)
    elif kind == "animation":
        await bot.send_animation(uid, b["file_id"], caption=cap,
                                 parse_mode=ParseMode.HTML, reply_markup=markup)
    else:
        await bot.send_message(uid, b["text"] or "(empty)", parse_mode=ParseMode.HTML,
                               reply_markup=markup)

async def bc_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = get_broadcast(bid)
    if not b:
        await deny(update, "Broadcast nahi mila.")
        return
    eligible, _ = audience_estimate(b)
    markup = build_button_markup(bid)

    chat_id = update.effective_chat.id
    try:
        await deliver_one(context.bot, b, chat_id, markup)
    except TelegramError as e:
        await update.effective_message.reply_html(
            f"⚠️ Preview render nahi ho paaya: <i>{html.escape(str(e))}</i>")
    text = (
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        " 📣 <b>PREVIEW</b>\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "☝️ That's exactly how it will look.\n\n"
        f"🎯 Audience: <b>{eligible:,}</b>\n"
        f"🔘 Buttons: <b>{len(broadcast_buttons(bid))}</b>"
    )
    rows = [
        [InlineKeyboardButton("✏️ Edit", callback_data=f"bc:edit:{bid}"),
         InlineKeyboardButton("🔘 Edit Buttons", callback_data=f"bc:btns:{bid}")],
        [InlineKeyboardButton("🎯 Audience", callback_data=f"bc:aud:{bid}"),
         InlineKeyboardButton("⏰ Schedule", callback_data=f"bc:sch:{bid}")],
        [InlineKeyboardButton("🚀 SEND NOW", callback_data=f"bc:sendask:{bid}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"bc:del:{bid}")],
        nav_row(f"bc:compose:{bid}"),
    ]
    await update.effective_message.reply_html(text, reply_markup=kb(rows))

async def bc_send_ask(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = get_broadcast(bid)
    if not b:
        await deny(update, "Broadcast nahi mila.")
        return
    eligible, _ = audience_estimate(b)
    text = (
        "⚠️ Are you absolutely sure?\n\n"
        f"This will send to about {eligible:,} users.\n"
        "This cannot be undone once started."
    )
    rows = [
        [InlineKeyboardButton("✅ CONFIRM SEND", callback_data=f"bc:send:{bid}")],
        [InlineKeyboardButton("❌ CANCEL", callback_data=f"bc:compose:{bid}")],
    ]
    await ui_edit(update, text, kb(rows))

# ----------------------------------------------------------------------------
# BROADCAST ENGINE
# ----------------------------------------------------------------------------
@dataclass
class Campaign:
    bid: int
    total: int = 0
    sent: int = 0
    failed: int = 0
    blocked: int = 0
    paused: bool = False
    stop: bool = False
    started: float = field(default_factory=time.monotonic)

RUNNING: dict[int, Campaign] = {}

async def _send_worker(bot, b: sqlite3.Row, markup, camp: Campaign,
                       queue: asyncio.Queue, sem: asyncio.Semaphore) -> None:
    while True:
        try:
            uid = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        while camp.paused and not camp.stop:
            await asyncio.sleep(0.5)
        if camp.stop:
            queue.task_done()
            continue

        async with sem:
            attempt = 0
            success = False
            while attempt < BROADCAST_MAX_RETRIES and not success:
                attempt += 1
                try:
                    await deliver_one(bot, b, uid, markup)
                    camp.sent += 1
                    success = True
                    break
                except RetryAfter as e:
                    wait = float(e.retry_after) + 0.5
                    log.warning("RetryAfter for %s: waiting %.1fs", uid, wait)
                    await asyncio.sleep(wait)
                    continue
                except (TimedOut, NetworkError, httpx.ReadTimeout, httpx.ConnectError) as e:
                    wait = min(2 ** (attempt + 1), 30)
                    log.warning("Network error for %s (attempt %d/%d): %s, retry in %ds",
                                uid, attempt, BROADCAST_MAX_RETRIES, e, wait)
                    await asyncio.sleep(wait)
                    continue
                except Forbidden:
                    camp.blocked += 1
                    mark(uid, "blocked", 1)
                    if b["channel_id"]:
                        channel_member_mark(b["channel_id"], uid, "blocked", 1)
                    success = True
                    break
                except BadRequest as e:
                    log.warning("BadRequest for %s: %s", uid, e)
                    camp.failed += 1
                    success = True
                    break
                except TelegramError as e:
                    log.warning("TelegramError for %s: %s", uid, e)
                    if attempt >= BROADCAST_MAX_RETRIES:
                        camp.failed += 1
                        success = True
            if not success:
                camp.failed += 1
            queue.task_done()
            await asyncio.sleep(BROADCAST_DELAY)

async def run_campaign(context: ContextTypes.DEFAULT_TYPE, bid: int,
                       status_chat_id: int | None = None,
                       status_msg_id: int | None = None,
                       scheduled_id: int | None = None) -> None:
    b = get_broadcast(bid)
    if not b:
        if scheduled_id:
            db_execute("UPDATE scheduled_broadcasts SET status='FAILED' WHERE id=?", (scheduled_id,))
        return
    ids = resolve_audience_ids(b)
    markup = build_button_markup(bid)

    camp = Campaign(bid=bid, total=len(ids))
    RUNNING[bid] = camp
    update_broadcast(bid, status="RUNNING", total=len(ids), started_at=utcnow_iso())
    if scheduled_id:
        db_execute("UPDATE scheduled_broadcasts SET status='RUNNING' WHERE id=?", (scheduled_id,))
    log_event("BROADCAST_STARTED", actor_user_id=b["owner_user_id"],
              channel_id=b["channel_id"], metadata=f"broadcast={bid} total={len(ids)}")

    queue: asyncio.Queue = asyncio.Queue()
    for uid in ids:
        queue.put_nowait(uid)
    sem = asyncio.Semaphore(BROADCAST_WORKERS)

    async def progress_reporter():
        if status_chat_id is None or status_msg_id is None:
            return
        while not queue.empty() and not camp.stop:
            await asyncio.sleep(3)
            done = camp.sent + camp.failed + camp.blocked
            pct = (done / camp.total * 100) if camp.total else 100
            elapsed = time.monotonic() - camp.started
            rate = done / elapsed if elapsed > 0 else 0
            remaining = camp.total - done
            eta = int(remaining / rate) if rate > 0 else 0
            txt = (
                "📣 <b>CAMPAIGN RUNNING</b>\n\n"
                f"{bar(pct)} {pct:.0f}%\n\n"
                f"Total: {camp.total:,}\n"
                f"Sent: {camp.sent:,}\n"
                f"Failed: {camp.failed:,}\n"
                f"Blocked: {camp.blocked:,}\n"
                f"ETA: {eta}s"
            )
            btns = kb([[InlineKeyboardButton("⏸ Pause", callback_data=f"bc:pause:{bid}"),
                        InlineKeyboardButton("🛑 Stop", callback_data=f"bc:stop:{bid}")]])
            try:
                await context.bot.edit_message_text(
                    txt, chat_id=status_chat_id, message_id=status_msg_id,
                    parse_mode=ParseMode.HTML, reply_markup=btns)
            except BadRequest:
                pass
            except TelegramError:
                pass

    workers = [asyncio.create_task(_send_worker(context.bot, b, markup, camp, queue, sem))
               for _ in range(BROADCAST_WORKERS)]
    reporter = asyncio.create_task(progress_reporter())
    await queue.join()
    camp.stop = True
    for w in workers:
        w.cancel()
    reporter.cancel()

    final_status = "CANCELLED" if camp.sent + camp.failed + camp.blocked < camp.total else "COMPLETED"
    update_broadcast(bid, status=final_status, sent=camp.sent,
                     failed=camp.failed, blocked=camp.blocked,
                     finished_at=utcnow_iso())
    if scheduled_id:
        sched_status = "COMPLETED" if final_status == "COMPLETED" else "FAILED"
        db_execute("UPDATE scheduled_broadcasts SET status=? WHERE id=?", (sched_status, scheduled_id))
    log_event("BROADCAST_COMPLETED" if final_status == "COMPLETED" else "BROADCAST_FAILED",
              actor_user_id=b["owner_user_id"], channel_id=b["channel_id"],
              metadata=f"broadcast={bid} sent={camp.sent} failed={camp.failed} blocked={camp.blocked}")
    RUNNING.pop(bid, None)

    if status_chat_id is not None and status_msg_id is not None:
        summary = (
            f"✅ <b>Broadcast {final_status.title()}</b>\n\n"
            f"Total: {camp.total:,}\n"
            f"✅ Sent: {camp.sent:,}\n"
            f"❌ Failed: {camp.failed:,}\n"
            f"🚫 Blocked: {camp.blocked:,}"
        )
        try:
            await context.bot.edit_message_text(
                summary, chat_id=status_chat_id, message_id=status_msg_id,
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("📣 History", callback_data=f"hist:{b['channel_id'] or 0}")],
                                 nav_row()]))
        except TelegramError:
            pass

async def bc_send_now(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = get_broadcast(bid)
    if not b:
        await deny(update, "Broadcast nahi mila.")
        return
    if bid in RUNNING:
        await update.callback_query.answer("Already running.", show_alert=True)
        return
    status = await context.bot.send_message(
        update.effective_chat.id, "📣 Starting campaign…", parse_mode=ParseMode.HTML)
    context.application.create_task(
        run_campaign(context, bid, status.chat_id, status.message_id))
    try:
        await update.callback_query.answer("🚀 Campaign started")
    except Exception:
        pass

async def bc_pause(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    camp = RUNNING.get(bid)
    if not camp:
        await update.callback_query.answer("Not running.", show_alert=True)
        return
    camp.paused = not camp.paused
    update_broadcast(bid, status="PAUSED" if camp.paused else "RUNNING")
    log_event("BROADCAST_PAUSED" if camp.paused else "BROADCAST_STARTED",
              channel_id=get_broadcast(bid)["channel_id"], metadata=f"broadcast={bid}")
    await update.callback_query.answer("⏸ Paused" if camp.paused else "▶️ Resumed")

async def bc_stop(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    camp = RUNNING.get(bid)
    if not camp:
        await update.callback_query.answer("Not running.", show_alert=True)
        return
    camp.stop = True
    camp.paused = False
    await update.callback_query.answer("🛑 Stopping…")

# ----------------------------------------------------------------------------
# SCHEDULED CAMPAIGNS
# ----------------------------------------------------------------------------
async def bc_schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    _ud(context)["flow"] = "bc_schedule"
    _ud(context)["flow_bid"] = bid
    await ui_edit(
        update,
        "⏰ Schedule Broadcast\n\n"
        "Send the date & time in this format (24h):\n"
        "YYYY-MM-DD HH:MM\n\n"
        "Example: 2026-08-10 18:30\n"
        "This is interpreted in UTC. Send /cancel to abort.",
        kb([nav_row(f"bc:compose:{bid}")]))

def schedule_broadcast(bid: int, when_utc: datetime) -> None:
    db_execute(
        "INSERT INTO scheduled_broadcasts(broadcast_id,scheduled_at,timezone,status) "
        "VALUES(?,?,?,?)",
        (bid, when_utc.isoformat(timespec="seconds"), "UTC", "SCHEDULED")
    )
    update_broadcast(bid, status="SCHEDULED")

async def scheduled_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = utcnow_iso()
    due = db_fetchall(
        "SELECT s.id AS sid, s.broadcast_id AS bid FROM scheduled_broadcasts s "
        "JOIN broadcasts b ON b.id=s.broadcast_id "
        "WHERE s.status='SCHEDULED' AND s.scheduled_at<=? AND b.status NOT IN "
        "('RUNNING','COMPLETED','CANCELLED')",
        (now,)
    )
    for r in due:
        b = get_broadcast(r["bid"])
        if not b:
            db_execute("UPDATE scheduled_broadcasts SET status='FAILED' WHERE id=?", (r["sid"],))
            continue
        # Start campaign and pass scheduled_id so it updates the scheduled record
        owner = b["owner_user_id"]
        status = None
        try:
            status = await context.bot.send_message(
                owner, f"⏰ Scheduled broadcast #{r['bid']} starting…",
                parse_mode=ParseMode.HTML)
        except TelegramError:
            pass
        # run_campaign will update scheduled status on completion
        context.application.create_task(run_campaign(
            context, r["bid"],
            status.chat_id if status else None,
            status.message_id if status else None,
            scheduled_id=r["sid"]))

async def show_scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if not has_permission(user.id, cid, "broadcast"):
        await deny(update, "Broadcast permission nahi hai.")
        return
    rows = db_fetchall(
        "SELECT s.*, b.status AS bstatus FROM scheduled_broadcasts s "
        "JOIN broadcasts b ON b.id=s.broadcast_id "
        "WHERE b.channel_id=? AND s.status='SCHEDULED' ORDER BY s.scheduled_at",
        (cid,)
    )
    if not rows:
        await ui_edit(update, "⏰ Scheduled\n\nKoi scheduled campaign nahi hai.",
                      kb([nav_row(f"ch:{cid}")]))
        return
    lines = ["⏰ Scheduled Campaigns\n"]
    kbrows = []
    for r in rows:
        lines.append(f"• #{r['broadcast_id']} at {r['scheduled_at']} UTC")
        kbrows.append([InlineKeyboardButton(f"🗑 Cancel #{r['broadcast_id']}",
                                            callback_data=f"sched:del:{r['id']}:{cid}")])
    kbrows.append(nav_row(f"ch:{cid}"))
    await ui_edit(update, "\n".join(lines), kb(kbrows))

async def scheduled_delete(update: Update, context: ContextTypes.DEFAULT_TYPE,
                           sid: int, cid: int) -> None:
    db_execute("UPDATE scheduled_broadcasts SET status='CANCELLED' WHERE id=?", (sid,))
    await show_scheduled(update, context, cid)

# ----------------------------------------------------------------------------
# CAMPAIGN HISTORY
# ----------------------------------------------------------------------------
async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if cid:
        if not has_permission(user.id, cid, "broadcast"):
            await deny(update, "Broadcast permission nahi hai.")
            return
        rows = db_fetchall(
            "SELECT * FROM broadcasts WHERE channel_id=? ORDER BY id DESC LIMIT 15",
            (cid,)
        )
    else:
        if not is_super_admin(user.id):
            await deny(update, "Admin only.")
            return
        rows = db_fetchall(
            "SELECT * FROM broadcasts ORDER BY id DESC LIMIT 15"
        )
    if not rows:
        await ui_edit(update, "📣 Broadcast History\n\nKoi campaign nahi.",
                      kb([nav_row(f"ch:{cid}" if cid else "admin")]))
        return
    lines = ["📣 Broadcast History\n"]
    kbrows = []
    for b in rows:
        lines.append(
            f"#{b['id']} · {b['status']} · 🎯{b['total']:,} "
            f"✅{b['sent']:,} ❌{b['failed']:,}")
        kbrows.append([
            InlineKeyboardButton(f"👁 #{b['id']}", callback_data=f"hist:view:{b['id']}"),
            InlineKeyboardButton("🔁", callback_data=f"hist:dup:{b['id']}"),
            InlineKeyboardButton("🗑", callback_data=f"hist:del:{b['id']}"),
        ])
    kbrows.append(nav_row(f"ch:{cid}" if cid else "admin"))
    await ui_edit(update, "\n".join(lines), kb(kbrows))

async def history_view(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    user = update.effective_user
    if not owns_broadcast(user.id, bid):
        await deny(update, "Access denied.")
        return
    b = get_broadcast(bid)
    text = (
        f"📣 Campaign #{b['id']}\n\n"
        f"Status: {b['status']}\n"
        f"Type: {_summ_kind(b)}\n"
        f"Audience: {AUDIENCE_LABELS.get(b['audience'], b['audience'])}\n"
        f"Total: {b['total']:,}\n"
        f"✅ Sent: {b['sent']:,}\n"
        f"❌ Failed: {b['failed']:,}\n"
        f"🚫 Blocked: {b['blocked']:,}\n"
        f"📅 Created: {(b['created_at'] or '')[:16].replace('T',' ')}"
    )
    await ui_edit(update, text, kb([
        [InlineKeyboardButton("🔁 Duplicate", callback_data=f"hist:dup:{bid}")],
        nav_row(f"hist:{b['channel_id'] or 0}")]))

async def history_duplicate(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    user = update.effective_user
    if not owns_broadcast(user.id, bid):
        await deny(update, "Access denied.")
        return
    src = get_broadcast(bid)
    new_bid = create_broadcast(user.id, src["channel_id"])
    update_broadcast(new_bid, kind=src["kind"], text=src["text"], file_id=src["file_id"],
                     caption=src["caption"], copy_chat_id=src["copy_chat_id"],
                     copy_msg_id=src["copy_msg_id"], audience=src["audience"])
    for btn in broadcast_buttons(bid):
        add_button(new_bid, btn["text"], btn["btype"], btn["value"],
                   new_row=(btn["col_idx"] == 0))
    await update.callback_query.answer(f"Duplicated as #{new_bid}")
    await bc_show_compose(update, context, new_bid)

async def history_delete(update: Update, context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    user = update.effective_user
    if not owns_broadcast(user.id, bid):
        await deny(update, "Access denied.")
        return
    if bid in RUNNING:
        await update.callback_query.answer("Running campaign delete nahi kar sakte.",
                                           show_alert=True)
        return
    cid = get_broadcast(bid)["channel_id"] or 0
    db_execute("DELETE FROM broadcasts WHERE id=?", (bid,))
    await show_history(update, context, cid)

# ----------------------------------------------------------------------------
# USER MANAGEMENT
# ----------------------------------------------------------------------------
async def show_users(update: Update, context: ContextTypes.DEFAULT_TYPE,
                     cid: int, flt: str = "ALL") -> None:
    user = update.effective_user
    if cid:
        if not has_permission(user.id, cid, "users"):
            await deny(update, "Users permission nahi hai.")
            return
    else:
        if not is_super_admin(user.id):
            await deny(update, "Admin only.")
            return

    if cid:
        q = "SELECT user_id, first_name, username, approved, blocked, joined_at " \
            "FROM channel_members WHERE channel_id=?"
        args = [cid]
    else:
        q = "SELECT user_id, first_name, username, approved, blocked, requested_at AS joined_at, " \
            "is_premium FROM users WHERE 1=1"
        args = []
    if flt == "ACTIVE":
        q += " AND blocked=0"
    elif flt == "BLOCKED":
        q += " AND blocked=1"
    elif flt == "PREMIUM" and not cid:
        q += " AND is_premium=1"
    elif flt == "TODAY":
        q += (" AND substr(joined_at,1,10)=?" if cid else " AND substr(requested_at,1,10)=?")
        args.append(today_iso())
    q += " ORDER BY joined_at DESC LIMIT 20"
    rows = db_fetchall(q, tuple(args))

    header = f"👥 <b>USER MANAGEMENT</b> — filter: {flt}\n"
    if not rows:
        body = "\nKoi user nahi mila."
    else:
        lines = []
        for r in rows:
            st = "🚫" if r["blocked"] else ("✅" if r["approved"] else "⏳")
            lines.append(
                f"{st} <b>{html.escape(r['first_name'] or '')}</b> "
                f"| @{r['username'] or '—'} | <code>{r['user_id']}</code>")
        body = "\n" + "\n".join(lines)
    text = header + body + "\n\n<i>Send /finduser &lt;id|@username|name&gt; to search.</i>"

    filters_row1 = [
        InlineKeyboardButton("ALL", callback_data=f"us:{cid}:ALL"),
        InlineKeyboardButton("ACTIVE", callback_data=f"us:{cid}:ACTIVE"),
        InlineKeyboardButton("BLOCKED", callback_data=f"us:{cid}:BLOCKED"),
    ]
    filters_row2 = [InlineKeyboardButton("TODAY", callback_data=f"us:{cid}:TODAY")]
    if not cid:
        filters_row2.append(InlineKeyboardButton("PREMIUM", callback_data=f"us:{cid}:PREMIUM"))
    rows_kb = [filters_row1, filters_row2, nav_row(f"ch:{cid}" if cid else "admin")]
    await ui_edit(update, text, kb(rows_kb))

async def user_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int) -> None:
    actor = update.effective_user
    u = db_fetchone("SELECT * FROM users WHERE user_id=?", (target_id,))
    owned = db_fetchone(
        "SELECT COUNT(*) FROM channels WHERE owner_user_id=? AND status!='REMOVED'",
        (target_id,)
    )[0]
    if not u:
        await ui_edit(update, "User nahi mila.", kb([nav_row()]))
        return
    text = (
        "👤 User Profile\n\n"
        f"👤 Name: {html.escape(u['first_name'] or '')}\n"
        f"🔗 Username: @{u['username'] or '—'}\n"
        f"🆔 ID: {u['user_id']}\n"
        f"📅 Joined: {(u['first_seen'] or u['requested_at'] or '')[:16].replace('T',' ')}\n"
        f"📢 Source: {u['source'] or '—'}\n"
        f"🟢 Status: {'🚫 Blocked' if u['blocked'] else 'Active'}\n"
        f"📢 Owned channels: {owned}"
    )
    rows = []
    if is_super_admin(actor.id):
        if u["blocked"]:
            rows.append([InlineKeyboardButton("✅ Unblock", callback_data=f"usr:unblock:{target_id}")])
        else:
            rows.append([InlineKeyboardButton("🚫 Block", callback_data=f"usr:block:{target_id}")])
    rows.append(nav_row())
    await ui_edit(update, text, kb(rows))

async def user_block_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE,
                            target_id: int, block: bool) -> None:
    actor = update.effective_user
    if not is_super_admin(actor.id):
        await deny(update, "Admin only.")
        return
    mark(target_id, "blocked", 1 if block else 0)
    log_event("SETTINGS_UPDATED", actor_user_id=actor.id,
              metadata=f"{'block' if block else 'unblock'} user={target_id}")
    await user_profile(update, context, target_id)

# ----------------------------------------------------------------------------
# LOGS
# ----------------------------------------------------------------------------
async def show_logs(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if cid and not has_permission(user.id, cid, "logs"):
        await deny(update, "Logs permission nahi hai.")
        return
    if not cid and not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    if cid:
        rows = db_fetchall(
            "SELECT action, metadata, created_at FROM events WHERE channel_id=? "
            "ORDER BY id DESC LIMIT 20", (cid,)
        )
    else:
        rows = db_fetchall(
            "SELECT action, metadata, created_at FROM events "
            "ORDER BY id DESC LIMIT 20"
        )
    if not rows:
        body = "Koi log entry nahi."
    else:
        body = "\n".join(
            f"• {(r['created_at'] or '')[11:16]} {r['action']} "
            f"{html.escape((r['metadata'] or '')[:40])}" for r in rows)
    await ui_edit(update, f"📋 LOGS\n\n{body}", kb([nav_row(f"ch:{cid}" if cid else "admin")]))

# ----------------------------------------------------------------------------
# REMOVE CHANNEL
# ----------------------------------------------------------------------------
async def channel_remove_ask(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if not (is_super_admin(user.id) or user_owns_channel(user.id, cid)):
        await deny(update, "Sirf owner channel remove kar sakta hai.")
        return
    ch = get_channel(cid)
    await ui_edit(update,
                  f"🗑 Remove channel {html.escape(ch['title'])}?\n\n"
                  "This unlinks it from the bot (data stays in DB).",
                  kb([[InlineKeyboardButton("✅ Confirm remove", callback_data=f"ch:rmok:{cid}")],
                      [InlineKeyboardButton("❌ Cancel", callback_data=f"ch:{cid}")]]))

async def channel_remove(update: Update, context: ContextTypes.DEFAULT_TYPE, cid: int) -> None:
    user = update.effective_user
    if not (is_super_admin(user.id) or user_owns_channel(user.id, cid)):
        await deny(update, "Remove denied.")
        return
    db_execute("UPDATE channels SET status='REMOVED' WHERE channel_id=?", (cid,))
    log_event("CHANNEL_REMOVED", actor_user_id=user.id, channel_id=cid)
    await show_channels(update, context)

# ----------------------------------------------------------------------------
# SUPER ADMIN CENTER
# ----------------------------------------------------------------------------
async def show_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Sirf super admin ke liye.")
        return
    s = stats()
    active = db_fetchone("SELECT COUNT(*) FROM channels WHERE status='ACTIVE'")[0]
    text = (
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        " 🛡️ ADMIN CENTER\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"👥 Users: {s['total']:,}\n"
        f"📢 Channels: {s['channels']:,}\n"
        f"📣 Campaigns: {s['campaigns']:,}\n"
        f"🟢 Active channels: {active:,}"
    )
    rows = [
        [InlineKeyboardButton("👥 Users", callback_data="us:0:ALL"),
         InlineKeyboardButton("📢 Channels", callback_data="admin:channels")],
        [InlineKeyboardButton("📣 Global Broadcast", callback_data="bc:global"),
         InlineKeyboardButton("📊 Global Analytics", callback_data="admin:analytics")],
        [InlineKeyboardButton("🚫 Blocked", callback_data="us:0:BLOCKED"),
         InlineKeyboardButton("📋 Logs", callback_data="logs:0")],
        [InlineKeyboardButton("💾 Backup", callback_data="admin:backup"),
         InlineKeyboardButton("📤 Export", callback_data="admin:export")],
        [InlineKeyboardButton("📣 History", callback_data="hist:0"),
         InlineKeyboardButton("🔄 Refresh", callback_data="admin")],
        nav_row(),
    ]
    await ui_edit(update, text, kb(rows))

async def admin_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    rows = db_fetchall(
        "SELECT channel_id, title, owner_user_id, status FROM channels "
        "WHERE status!='REMOVED' ORDER BY added_at DESC LIMIT 30"
    )
    lines = ["📢 All Channels\n"]
    kbrows = []
    for r in rows:
        lines.append(f"• {html.escape(r['title'])} "
                     f"(owner {r['owner_user_id']}) — {r['status']}")
        kbrows.append([InlineKeyboardButton(f"⚙️ {r['title'][:20]}",
                                            callback_data=f"ch:{r['channel_id']}")])
    kbrows.append(nav_row("admin"))
    await ui_edit(update, "\n".join(lines) if rows else "Koi channel nahi.", kb(kbrows))

async def admin_global_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    s = stats()
    sums = db_fetchone(
        "SELECT COALESCE(SUM(sent),0) s, COALESCE(SUM(total),0) t, "
        "COALESCE(SUM(failed),0) f FROM broadcasts"
    )
    rate = (sums["s"] / sums["t"] * 100) if sums["t"] else 0
    text = (
        "📊 GLOBAL ANALYTICS\n\n"
        f"👥 Total users: {s['total']:,}\n"
        f"🟢 Approved: {s['approved']:,}\n"
        f"🚫 Blocked: {s['blocked']:,}\n"
        f"📅 New today: {s['today']:,}\n"
        f"📢 Channels: {s['channels']:,}\n"
        f"📣 Campaigns: {s['campaigns']:,}\n"
        f"📤 Messages sent: {sums['s']:,}\n"
        f"📊 Overall success: {rate:.1f}%"
    )
    await ui_edit(update, text, kb([nav_row("admin")]))

async def admin_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    await update.effective_chat.send_action(ChatAction.UPLOAD_DOCUMENT)
    tmp = DB_PATH + ".backup"
    try:
        src = sqlite3.connect(DB_PATH)
        dst = sqlite3.connect(tmp)
        with dst:
            src.backup(dst)
        dst.close()
        src.close()
        with open(tmp, "rb") as f:
            data = f.read()
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    fname = f"backup_{datetime.now().strftime('%Y%m%d_%H%M')}.db"
    await context.bot.send_document(
        update.effective_chat.id,
        document=InputFile(io.BytesIO(data), filename=fname),
        caption="💾 Database backup.")
    if update.callback_query:
        await update.callback_query.answer("Backup sent ✅")

def _csv_bytes(header: list[str], rows: Iterable[Iterable]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    for r in rows:
        w.writerow(list(r))
    return buf.getvalue().encode("utf-8-sig")

async def admin_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    await ui_edit(update, "📤 Export\n\nChoose what to export as CSV:", kb([
        [InlineKeyboardButton("📤 Users CSV", callback_data="exp:users"),
         InlineKeyboardButton("📤 Channels CSV", callback_data="exp:channels")],
        [InlineKeyboardButton("📤 Broadcasts CSV", callback_data="exp:broadcasts"),
         InlineKeyboardButton("📤 Logs CSV", callback_data="exp:logs")],
        nav_row("admin")]))

async def admin_export_do(update: Update, context: ContextTypes.DEFAULT_TYPE, what: str) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    await update.effective_chat.send_action(ChatAction.UPLOAD_DOCUMENT)
    if what == "users":
        rows = db_fetchall("SELECT * FROM users ORDER BY requested_at DESC")
    elif what == "channels":
        rows = db_fetchall("SELECT * FROM channels ORDER BY added_at DESC")
    elif what == "broadcasts":
        rows = db_fetchall("SELECT * FROM broadcasts ORDER BY id DESC")
    else:
        rows = db_fetchall("SELECT * FROM events ORDER BY id DESC LIMIT 5000")
    if not rows:
        await context.bot.send_message(update.effective_chat.id, "Kuch export karne ko nahi.")
        return
    header = list(rows[0].keys())
    data = _csv_bytes(header, ([r[k] for k in header] for r in rows))
    fname = f"{what}{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await context.bot.send_document(
        update.effective_chat.id,
        document=InputFile(io.BytesIO(data), filename=fname),
        caption=f"📄 {len(rows)} {what} exported.")
    if update.callback_query:
        await update.callback_query.answer("Exported ✅")

# ----------------------------------------------------------------------------
# GLOBAL BROADCAST (super admin)
# ----------------------------------------------------------------------------
async def bc_global(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_super_admin(user.id):
        await deny(update, "Admin only.")
        return
    await bc_new(update, context, None)

# ----------------------------------------------------------------------------
# JOIN REQUEST HANDLING
# ----------------------------------------------------------------------------
def welcome_keyboard(chat_link: str | None) -> InlineKeyboardMarkup | None:
    rows = []
    if chat_link:
        rows.append([InlineKeyboardButton("📢 Open Channel", url=chat_link)])
    if SUPPORT_URL:
        rows.append([InlineKeyboardButton("💬 Support", url=SUPPORT_URL)])
    rows.append([InlineKeyboardButton("✅ Thanks!", callback_data="thanks")])
    return InlineKeyboardMarkup(rows)

async def notify_log(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    if not LOG_CHANNEL:
        return
    try:
        await context.bot.send_message(LOG_CHANNEL, text, parse_mode=ParseMode.HTML,
                                       disable_web_page_preview=True)
    except Exception as e:
        log.warning("log channel error: %s", e)

async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    req = update.chat_join_request
    user, chat = req.from_user, req.chat

    save_user(user, chat, approved=0, welcomed=0, source="join_request")
    channel_member_upsert(chat.id, user, approved=0)
    db_execute(
        "INSERT INTO join_requests(channel_id,user_id,status,created_at) VALUES(?,?,?,?)",
        (chat.id, user.id, "PENDING", utcnow_iso())
    )
    log_event("JOIN_REQUEST", actor_user_id=user.id, channel_id=chat.id,
              metadata=chat.title or str(chat.id))
    log.info("Join request: %s (%s) -> %s", user.first_name, user.id, chat.title)

    ch = get_channel(chat.id)
    if ch:
        cs = channel_settings(chat.id)
        auto = bool(cs["auto_approve"])
        welcome_enabled = bool(cs["welcome_enabled"])
        welcome_tpl = cs["welcome_text"] or DEFAULT_WELCOME
        pending_tpl = cs["pending_text"] or DEFAULT_PENDING_MSG
    else:
        auto = get_setting("auto_approve", "true") == "true"
        welcome_enabled = True
        welcome_tpl = get_setting("welcome_text", DEFAULT_WELCOME)
        pending_tpl = get_setting("pending_text", DEFAULT_PENDING_MSG)

    if WELCOME_DELAY > 0:
        await asyncio.sleep(WELCOME_DELAY)

    approved = False
    if auto:
        try:
            await context.bot.approve_chat_join_request(chat.id, user.id)
            approved = True
            mark(user.id, "approved", 1)
            channel_member_mark(chat.id, user.id, "approved", 1)
            db_execute(
                "UPDATE join_requests SET status='APPROVED' "
                "WHERE channel_id=? AND user_id=? AND status='PENDING'",
                (chat.id, user.id)
            )
            log_event("JOIN_APPROVED", actor_user_id=user.id, channel_id=chat.id, metadata="auto")
        except (BadRequest, Forbidden) as e:
            log.error("Approve failed for %s: %s", user.id, e)

    if welcome_enabled:
        template = welcome_tpl if approved else pending_tpl
        text = render(template, user, chat.title or "")
        link = req.invite_link.invite_link if req.invite_link else None
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=welcome_keyboard(link),
                disable_web_page_preview=True)
            mark(user.id, "welcomed", 1)
            log_event("WELCOME_SENT", actor_user_id=user.id, channel_id=chat.id)
        except (Forbidden, BadRequest) as e:
            mark(user.id, "blocked", 1)
            channel_member_mark(chat.id, user.id, "blocked", 1)
            log.warning("DM failed %s: %s", user.id, e)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)

    await notify_log(
        context,
        f"🆕 <b>New join request</b>\n"
        f"👤 {mention_of(user)} (<code>{user.id}</code>)\n"
        f"🔗 @{user.username or '—'}\n"
        f"📢 {html.escape(chat.title or '')}\n"
        f"✅ Approved: {'Yes' if approved else 'No'}"
    )

async def on_thanks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.answer("Welcome aboard! 🎉", show_alert=False)

async def on_noop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.answer()

# ----------------------------------------------------------------------------
# CHANNEL LIFECYCLE — bot's own membership changes
# ----------------------------------------------------------------------------
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cm = update.my_chat_member
    if not cm:
        return
    chat = cm.chat
    if chat.type not in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
        return
    new = cm.new_chat_member
    ch = get_channel(chat.id)
    if not ch:
        return
    if new.status in ("left", "kicked"):
        db_execute("UPDATE channels SET status='BOT_REMOVED' WHERE channel_id=?", (chat.id,))
        log_event("CHANNEL_UPDATED", channel_id=chat.id, metadata="bot_removed")
    elif new.status in ("administrator", "creator"):
        can_inv = int(bool(getattr(new, "can_invite_users", False)) or new.status == "creator")
        db_execute("UPDATE channels SET status='ACTIVE', can_invite=? WHERE channel_id=?",
                   (can_inv, chat.id))
    elif new.status == "member":
        db_execute("UPDATE channels SET status='PERMISSION_LOST' WHERE channel_id=?", (chat.id,))
        log_event("CHANNEL_UPDATED", channel_id=chat.id, metadata="permission_lost")

# ----------------------------------------------------------------------------
# CONVERSATION MESSAGE ROUTER
# ----------------------------------------------------------------------------
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or update.effective_chat.type != ChatType.PRIVATE:
        return
    flow = _ud(context).get("flow")
    text = (msg.text or msg.caption or "").strip()

    if text == "/cancel":
        _ud(context).clear()
        await msg.reply_html("❌ Cancelled.", reply_markup=home_kb(update.effective_user))
        return
    if not flow:
        return

    user = update.effective_user

    if flow == "add_channel":
        ref, kind = parse_channel_input(text)
        if not ref:
            await msg.reply_html("⚠️ Valid channel @username, link, ya -100 ID bhejo.")
            return
        await ch_finish_add(update, context, ref)
        return

    if flow == "set_welcome":
        cid = _ud(context).get("flow_cid")
        raw = msg.text_html or text
        ok, why = validate_template(raw)
        if not ok:
            await msg.reply_html(f"⚠️ {why}")
            return
        set_channel_setting(cid, "welcome_text", raw)
        _ud(context).clear()
        await msg.reply_html("✅ Welcome updated.\n\n<b>Preview:</b>")
        ch = get_channel(cid)
        await msg.reply_html(render(raw, user, ch["title"] if ch else ""))
        return

    if flow == "add_manager":
        cid = _ud(context).get("flow_cid")
        if not re.fullmatch(r"\d{5,}", text):
            await msg.reply_html("⚠️ Numeric Telegram user ID bhejo.")
            return
        mid = int(text)
        db_execute(
            "INSERT OR IGNORE INTO channel_managers(channel_id,user_id,added_at) VALUES(?,?,?)",
            (cid, mid, utcnow_iso())
        )
        log_event("MANAGER_ADDED", actor_user_id=user.id, channel_id=cid, metadata=str(mid))
        _ud(context).clear()
        await msg.reply_html(
            f"✅ Manager <code>{mid}</code> added. Ab permissions set karo.",
            reply_markup=kb([[InlineKeyboardButton("🔧 Set permissions", callback_data=f"mgr:perm:{mid}:{cid}")],
                             nav_row(f"mgr:{cid}")]))
        return

    if flow == "bc_content":
        bid = _ud(context).get("flow_bid")
        if not owns_broadcast(user.id, bid):
            _ud(context).clear()
            await msg.reply_html("⛔ Access denied.")
            return
        cap = msg.caption_html or None
        if msg.photo:
            update_broadcast(bid, kind="photo", file_id=msg.photo[-1].file_id, caption=cap)
        elif msg.video:
            update_broadcast(bid, kind="video", file_id=msg.video.file_id, caption=cap)
        elif msg.animation:
            update_broadcast(bid, kind="animation", file_id=msg.animation.file_id, caption=cap)
        elif msg.document:
            update_broadcast(bid, kind="document", file_id=msg.document.file_id, caption=cap)
        elif msg.audio:
            update_broadcast(bid, kind="audio", file_id=msg.audio.file_id, caption=cap)
        elif msg.text:
            update_broadcast(bid, kind="text", text=msg.text_html or msg.text)
        else:
            update_broadcast(bid, kind="copy", copy_chat_id=msg.chat_id, copy_msg_id=msg.message_id)
        _ud(context).clear()
        await bc_show_compose(update, context, bid)
        return

    if flow == "bc_btn_text":
        _ud(context)["btn_text"] = text
        _ud(context)["flow"] = "bc_btn_type"
        bid = _ud(context).get("flow_bid")
        await msg.reply_html(
            "Button type?",
            reply_markup=kb([[InlineKeyboardButton("🔗 URL", callback_data=f"bc:btype:url:{bid}"),
                              InlineKeyboardButton("⚡ Callback", callback_data=f"bc:btype:cb:{bid}")]]))
        return

    if flow == "bc_btn_url":
        bid = _ud(context).get("flow_bid")
        if not valid_url(text):
            await msg.reply_html("⚠️ Valid http(s) URL bhejo.")
            return
        add_button(bid, _ud(context).get("btn_text", "Button"), "url", text,
                   _ud(context).get("btn_new_row", True))
        _ud(context).clear()
        await msg.reply_html("✅ Button added.")
        await show_button_builder_message(update, context, bid)
        return

    if flow == "bc_schedule":
        bid = _ud(context).get("flow_bid")
        try:
            when = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except ValueError:
            await msg.reply_html("⚠️ Format: <code>YYYY-MM-DD HH:MM</code> (UTC).")
            return
        if when <= datetime.now(timezone.utc):
            await msg.reply_html("⚠️ Time future me honi chahiye.")
            return
        schedule_broadcast(bid, when)
        _ud(context).clear()
        await msg.reply_html(
            f"✅ Scheduled for <code>{when.isoformat(timespec='minutes')} UTC</code>.",
            reply_markup=home_kb(user))
        return

# ----------------------------------------------------------------------------
# CENTRAL CALLBACK ROUTER
# ----------------------------------------------------------------------------
def _int(s: str) -> int:
    return int(s)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    data = q.data or ""
    try:
        await q.answer()
    except Exception:
        pass

    try:
        if data == "home":
            return await show_home(update, context)
        if data == "help":
            return await show_help(update, context)
        if data == "channels":
            return await show_channels(update, context)
        if data == "admin":
            return await show_admin(update, context)
        if data == "thanks":
            return
        if data.startswith("noop:"):
            return
        if data.startswith("channels?"):
            return await show_channels(update, context, data.split("?", 1)[1])

        if data == "admin:channels":
            return await admin_channels(update, context)
        if data == "admin:analytics":
            return await admin_global_analytics(update, context)
        if data == "admin:backup":
            return await admin_backup(update, context)
        if data == "admin:export":
            return await admin_export(update, context)
        if data.startswith("exp:"):
            return await admin_export_do(update, context, data.split(":", 1)[1])

        if data == "ch:add":
            return await ch_add_start(update, context)
        if data == "ch:how":
            return await ch_how(update, context)
        if data == "ch:cancel":
            return await ch_cancel(update, context)
        if data == "ch:recheck":
            return await ch_recheck(update, context)

        if data.startswith("ch:rmok:"):
            return await channel_remove(update, context, _int(data.split(":")[2]))
        if data.startswith("ch:rm:"):
            return await channel_remove_ask(update, context, _int(data.split(":")[2]))
        if re.fullmatch(r"ch:-?\d+", data):
            return await show_dashboard(update, context, _int(data.split(":")[1]))

        if data.startswith("wel:edit:"):
            return await welcome_edit_start(update, context, _int(data.split(":")[2]))
        if data.startswith("wel:prev:"):
            return await welcome_preview(update, context, _int(data.split(":")[2]))
        if data.startswith("wel:reset:"):
            cid = _int(data.split(":")[2])
            set_channel_setting(cid, "welcome_text", DEFAULT_WELCOME)
            return await show_welcome(update, context, cid)
        if data.startswith("wel:toggle:"):
            cid = _int(data.split(":")[2])
            cs = channel_settings(cid)
            set_channel_setting(cid, "welcome_enabled", 0 if cs["welcome_enabled"] else 1)
            return await show_welcome(update, context, cid)
        if data.startswith("wel:"):
            return await show_welcome(update, context, _int(data.split(":")[1]))
        if data.startswith("wbtn:"):
            return await show_welcome(update, context, _int(data.split(":")[1]))

        if data.startswith("se:tg:"):
            _, _, key, cid = data.split(":")
            return await settings_toggle(update, context, key, _int(cid))
        if data.startswith("se:"):
            return await show_settings(update, context, _int(data.split(":")[1]))

        if data.startswith("mgr:add:"):
            return await manager_add_start(update, context, _int(data.split(":")[2]))
        if data.startswith("mgr:perm:"):
            _, _, mid, cid = data.split(":")
            return await manager_perm_screen(update, context, _int(mid), _int(cid))
        if data.startswith("mgr:tg:"):
            _, _, perm, mid, cid = data.split(":")
            return await manager_perm_toggle(update, context, perm, _int(mid), _int(cid))
        if data.startswith("mgr:del:"):
            _, _, mid, cid = data.split(":")
            return await manager_delete(update, context, _int(mid), _int(cid))
        if data.startswith("mgr:"):
            return await show_managers(update, context, _int(data.split(":")[1]))

        if data.startswith("an:"):
            parts = data.split(":")
            cid = _int(parts[1])
            period = parts[2] if len(parts) > 2 else "7D"
            return await show_analytics(update, context, cid, period)

        if data.startswith("us:"):
            parts = data.split(":")
            cid = _int(parts[1])
            flt = parts[2] if len(parts) > 2 else "ALL"
            return await show_users(update, context, cid, flt)
        if data.startswith("usr:block:"):
            return await user_block_toggle(update, context, _int(data.split(":")[2]), True)
        if data.startswith("usr:unblock:"):
            return await user_block_toggle(update, context, _int(data.split(":")[2]), False)
        if data.startswith("usr:"):
            return await user_profile(update, context, _int(data.split(":")[1]))

        if data.startswith("logs:"):
            return await show_logs(update, context, _int(data.split(":")[1]))

        if data.startswith("sched:del:"):
            _, _, sid, cid = data.split(":")
            return await scheduled_delete(update, context, _int(sid), _int(cid))
        if data.startswith("sched:"):
            return await show_scheduled(update, context, _int(data.split(":")[1]))

        if data.startswith("hist:view:"):
            return await history_view(update, context, _int(data.split(":")[2]))
        if data.startswith("hist:dup:"):
            return await history_duplicate(update, context, _int(data.split(":")[2]))
        if data.startswith("hist:del:"):
            return await history_delete(update, context, _int(data.split(":")[2]))
        if data.startswith("hist:"):
            return await show_history(update, context, _int(data.split(":")[1]))

        if data == "bc:global":
            return await bc_global(update, context)
        if data.startswith("bc:new:"):
            raw = data.split(":")[2]
            return await bc_new(update, context, _int(raw) if raw not in ("", "0") else None)
        if data.startswith("bc:compose:"):
            return await bc_show_compose(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:edit:"):
            bid = _int(data.split(":")[2])
            if not owns_broadcast(update.effective_user.id, bid):
                return await deny(update, "Access denied.")
            _ud(context)["flow"] = "bc_content"
            _ud(context)["flow_bid"] = bid
            return await ui_edit(update, "✏️ Send the new content now. /cancel to abort.",
                                 kb([nav_row(f"bc:compose:{bid}")]))
        if data.startswith("bc:aud:"):
            return await bc_audience(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:setaud:"):
            _, _, aud, bid = data.split(":")
            bid = _int(bid)
            if not owns_broadcast(update.effective_user.id, bid):
                return await deny(update, "Access denied.")
            if aud in AUDIENCE_LABELS:
                update_broadcast(bid, audience=aud)
            return await bc_audience(update, context, bid)
        if data.startswith("bc:btns:"):
            return await bc_buttons(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:baddrow:"):
            return await bc_add_button_start(update, context, _int(data.split(":")[2]), True)
        if data.startswith("bc:baddcol:"):
            return await bc_add_button_start(update, context, _int(data.split(":")[2]), False)
        if data.startswith("bc:bdel:"):
            bid = _int(data.split(":")[2])
            delete_last_button(bid)
            return await bc_buttons(update, context, bid)
        if data.startswith("bc:btype:"):
            _, _, bt, bid = data.split(":")
            bid = _int(bid)
            if bt == "url":
                _ud(context)["flow"] = "bc_btn_url"
                _ud(context)["flow_bid"] = bid
                return await ui_edit(update, "🔗 Send the URL (http/https). /cancel to abort.",
                                     kb([nav_row(f"bc:btns:{bid}")]))
            else:
                add_button(bid, _ud(context).get("btn_text", "Button"), "callback", "noop",
                           _ud(context).get("btn_new_row", True))
                _ud(context).clear()
                return await bc_buttons(update, context, bid)
        if data.startswith("bc:prev:"):
            return await bc_preview(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:sch:"):
            return await bc_schedule_start(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:sendask:"):
            return await bc_send_ask(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:send:"):
            bid = _int(data.split(":")[2])
            if not owns_broadcast(update.effective_user.id, bid):
                return await deny(update, "Access denied.")
            return await bc_send_now(update, context, bid)
        if data.startswith("bc:pause:"):
            return await bc_pause(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:stop:"):
            return await bc_stop(update, context, _int(data.split(":")[2]))
        if data.startswith("bc:del:"):
            bid = _int(data.split(":")[2])
            if owns_broadcast(update.effective_user.id, bid) and bid not in RUNNING:
                db_execute("UPDATE broadcasts SET status='CANCELLED' WHERE id=?", (bid,))
                _ud(context).clear()
                return await show_home(update, context)

        await q.answer("Unknown action.", show_alert=False)
    except Forbidden:
        pass
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            log.warning("callback BadRequest: %s", e)
    except Exception as e:
        log.error("callback error on %r: %s", data, e, exc_info=e)

# ----------------------------------------------------------------------------
# LEGACY / DIRECT COMMANDS (all preserved)
# ----------------------------------------------------------------------------
def legacy_is_admin(update: Update) -> bool:
    u = update.effective_user
    return bool(u and (not ADMIN_IDS or u.id in ADMIN_IDS))

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not legacy_is_admin(update):
            await update.message.reply_text("⛔ Sirf admin ke liye.")
            return
        return await func(update, context)
    return wrapper

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    save_user(update.effective_user, source="start")
    await update.message.reply_html(
        home_text(update.effective_user), reply_markup=home_kb(update.effective_user))

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_html(
        "📖 Commands\n\n"
        "/start — open home\n/help — this list\n/id — your & chat ID\n\n"
        "🛠 Admin\n/panel /stats /users /export /broadcast /settings /pending\n"
        "/setwelcome /getwelcome /autoapprove /finduser\n\n"
        "Primary interface is inline buttons — tap /start.\n\n"
        "Placeholders: {mention} {name} {username} {id} {chat} {date}",
        reply_markup=home_kb(update.effective_user))

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    c = update.effective_chat
    u = update.effective_user
    await update.message.reply_html(
        f"🆔 Your ID: {u.id}\n"
        f"💬 Chat ID: {c.id}\n"
        f"📂 Type: {c.type}")

@admin_only
async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    s = stats()
    auto = get_setting("auto_approve", "true")
    await update.message.reply_html(
        "🛠 Admin Panel\n\n"
        f"👥 Total users: {s['total']:,}\n"
        f"✅ Approved: {s['approved']:,}\n"
        f"💌 Welcomed: {s['welcomed']:,}\n"
        f"🚫 DM blocked: {s['blocked']:,}\n"
        f"📅 Today: {s['today']:,}\n"
        f"📢 Channels: {s['channels']:,}\n"
        f"📣 Campaigns: {s['campaigns']:,}\n"
        f"⚙️ Auto-approve (global): {'ON' if auto == 'true' else 'OFF'}\n"
        f"🗄 DB: {DB_PATH}",
        reply_markup=kb([[InlineKeyboardButton("🛡️ Open Admin Center", callback_data="admin")]]))

@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_panel(update, context)

@admin_only
async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = db_fetchall(
        "SELECT user_id, first_name, username, requested_at, approved "
        "FROM users ORDER BY requested_at DESC LIMIT 20"
    )
    if not rows:
        await update.message.reply_text("Abhi koi user nahi hai.")
        return
    lines = ["👥 Last 20 users\n"]
    for r in rows:
        lines.append(
            f"{'✅' if r['approved'] else '⏳'} {html.escape(r['first_name'] or '')} "
            f"| @{r['username'] or '—'} | {r['user_id']}\n"
            f" {(r['requested_at'] or '')[:16].replace('T', ' ')}")
    await update.message.reply_html("\n".join(lines))

@admin_only
async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_chat.send_action(ChatAction.UPLOAD_DOCUMENT)
    rows = db_fetchall("SELECT * FROM users ORDER BY requested_at DESC")
    if not rows:
        await update.message.reply_text("Koi user nahi.")
        return
    header = list(rows[0].keys())
    data = _csv_bytes(header, ([r[k] for k in header] for r in rows))
    fname = f"users{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await update.message.reply_document(
        document=InputFile(io.BytesIO(data), filename=fname),
        caption=f"📄 {len(rows)} users exported.")

@admin_only
async def cmd_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.partition(" ")[2].strip()
    if not text and update.message.reply_to_message:
        text = update.message.reply_to_message.text_html or ""
    if not text:
        await update.message.reply_text(
            "Use: /setwelcome <text>\nPlaceholders: {mention} {name} {chat} {id} {date}")
        return
    ok, why = validate_template(text)
    if not ok:
        await update.message.reply_html(f"⚠️ {why}")
        return
    set_setting("welcome_text", text)
    await update.message.reply_html("✅ Global welcome updated.\n\nPreview:")
    await update.message.reply_html(render(text, update.effective_user, update.effective_chat.title or ""))

@admin_only
async def cmd_getwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(get_setting("welcome_text", DEFAULT_WELCOME))

@admin_only
async def cmd_autoapprove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    arg = (context.args[0].lower() if context.args else "")
    if arg in {"on", "true", "1"}:
        set_setting("auto_approve", "true")
    elif arg in {"off", "false", "0"}:
        set_setting("auto_approve", "false")
    else:
        await update.message.reply_text("Use: /autoapprove on | /autoapprove off")
        return
    await update.message.reply_html(
        f"⚙️ Global auto-approve ab {'ON' if get_setting('auto_approve') == 'true' else 'OFF'} hai.")

@admin_only
async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = db_fetchall("SELECT key, value FROM settings")
    out = "\n".join(
        f"• {r['key']}: {html.escape((r['value'] or '')[:80])}" for r in rows)
    await update.message.reply_html(f"⚙️ Global Settings\n\n{out}")

@admin_only
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reply = update.message.reply_to_message
    text = update.message.text.partition(" ")[2].strip()
    if not text and not reply:
        await update.message.reply_text(
            "Use: /broadcast <text> ya kisi message pe reply karke /broadcast\n"
            "Tip: rich campaigns ke liye /start → 📣 Broadcast.")
        return
    rows = db_fetchall("SELECT user_id FROM users WHERE blocked=0")
    ids = [r["user_id"] for r in rows]
    status = await update.message.reply_html(f"📤 Broadcasting to {len(ids):,} users…")
    sent = failed = 0
    for i, uid in enumerate(ids, 1):
        try:
            if reply:
                await reply.copy(chat_id=uid)
            else:
                await context.bot.send_message(uid, text, parse_mode=ParseMode.HTML,
                                               disable_web_page_preview=True)
            sent += 1
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            failed += 1
        except (Forbidden, BadRequest):
            mark(uid, "blocked", 1)
            failed += 1
        except TimedOut:
            failed += 1
        await asyncio.sleep(BROADCAST_DELAY)
        if i % 25 == 0:
            try:
                await status.edit_text(f"📤 {i}/{len(ids)} — ✅ {sent} ❌ {failed}")
            except Exception:
                pass
    await status.edit_text(f"✅ Broadcast done.\nSent: {sent}\nFailed: {failed}")

@admin_only
async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = db_fetchall(
        "SELECT user_id, first_name, username FROM users WHERE approved=0 "
        "ORDER BY requested_at DESC LIMIT 30"
    )
    if not rows:
        await update.message.reply_text("🎉 Koi pending request nahi.")
        return
    out = "\n".join(
        f"⏳ {html.escape(r['first_name'] or '')} | @{r['username'] or '—'} | {r['user_id']}"
        for r in rows)
    await update.message.reply_html(f"Pending ({len(rows)})\n\n{out}")

@admin_only
async def cmd_finduser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = " ".join(context.args).strip() if context.args else ""
    if not query:
        await update.message.reply_text("Use: /finduser <id | @username | name>")
        return
    if query.isdigit():
        rows = db_fetchall("SELECT * FROM users WHERE user_id=?", (int(query),))
    elif query.startswith("@"):
        rows = db_fetchall("SELECT * FROM users WHERE username=?", (query[1:],))
    else:
        rows = db_fetchall(
            "SELECT * FROM users WHERE first_name LIKE ? OR username LIKE ? LIMIT 10",
            (f"%{query}%", f"%{query}%")
        )
    if not rows:
        await update.message.reply_text("Koi user nahi mila.")
        return
    out = "\n".join(
        f"👤 {html.escape(r['first_name'] or '')} | @{r['username'] or '—'} "
        f"| {r['user_id']} {'🚫' if r['blocked'] else ''}" for r in rows)
    await update.message.reply_html(f"🔎 Results\n\n{out}")

# ----------------------------------------------------------------------------
# ERROR HANDLER
# ----------------------------------------------------------------------------
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Update caused error: %s", context.error, exc_info=context.error)

# ----------------------------------------------------------------------------
# FLASK WEB SERVER (for Render) - improved with werkzeug server
# ----------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route('/')
def index():
    return "🤖 Bot is running. Use Telegram to interact."

@flask_app.route('/health')
def health():
    # Simple health check - can be expanded to check DB etc.
    return jsonify({"status": "ok", "service": "telegram-bot"}), 200

# Global reference to the server for shutdown
flask_server = None

def run_flask():
    global flask_server
    port = int(os.environ.get("PORT", 5000))
    # Use Werkzeug's make_server for better control
    flask_server = make_server('0.0.0.0', port, flask_app, threaded=True)
    log.info("Flask server starting on port %s", port)
    flask_server.serve_forever()

def stop_flask():
    global flask_server
    if flask_server:
        log.info("Shutting down Flask server...")
        flask_server.shutdown()
        flask_server = None

# ----------------------------------------------------------------------------
# MAIN — with retry and graceful shutdown
# ----------------------------------------------------------------------------
def build_application() -> Application:
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_YOUR_TOKEN_HERE":
        sys.exit("❌ BOT_TOKEN set karo: export BOT_TOKEN='123:ABC'")

    # Create HTTPXRequest with timeout parameters directly (PTB 20.6 API)
    http_client = HTTPXRequest(
        connect_timeout=TG_CONNECT_TIMEOUT,
        read_timeout=TG_READ_TIMEOUT,
        write_timeout=TG_WRITE_TIMEOUT,
        pool_timeout=TG_POOL_TIMEOUT,
        connection_pool_size=TG_POOL_SIZE,
        http_version="HTTP/1.1"
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .http_client(http_client)
        .concurrent_updates(True)
        .build()
    )

    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("setwelcome", cmd_setwelcome))
    app.add_handler(CommandHandler("getwelcome", cmd_getwelcome))
    app.add_handler(CommandHandler("autoapprove", cmd_autoapprove))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CommandHandler("finduser", cmd_finduser))

    app.add_handler(CallbackQueryHandler(on_thanks, pattern=r"^thanks$"))
    app.add_handler(CallbackQueryHandler(on_noop, pattern=r"^noop:"))
    app.add_handler(CallbackQueryHandler(on_callback))

    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_message))

    app.add_error_handler(on_error)

    if app.job_queue is not None:
        app.job_queue.run_repeating(scheduled_job, interval=30, first=10)
    else:
        log.warning("JobQueue unavailable — scheduled campaigns won't auto-fire. "
                    "Install python-telegram-bot[job-queue].")

    return app

async def bot_main() -> None:
    app = build_application()
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # Wait for shutdown signal
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()

    log.info("Shutting down gracefully...")
    # Stop polling and application
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    # Stop Flask server
    stop_flask()
    log.info("Shutdown complete.")

async def bot_main_with_retry() -> None:
    attempt = 0
    while True:
        try:
            await bot_main()
            break
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                NetworkError, TimedOut, TelegramError) as e:
            attempt += 1
            if attempt >= TG_MAX_RETRIES:
                log.critical("Bot failed to start after %d attempts. Exiting.", TG_MAX_RETRIES)
                sys.exit(1)
            wait = min(2 ** attempt, 60)
            log.warning("Bot start failed: %s. Retrying in %ds (attempt %d/%d)",
                        e, wait, attempt, TG_MAX_RETRIES)
            await asyncio.sleep(wait)
        except Exception as e:
            log.critical("Unexpected error in bot main: %s", e, exc_info=True)
            sys.exit(1)

def main() -> None:
    if not ADMIN_IDS:
        log.warning("⚠️ ADMIN_IDS khali hai — super-admin features off; "
                    "legacy admin commands sabke liye khule hain.")

    db_init()
    log.info("🤖 %s starting...", BOT_NAME)

    # Start Flask server in a separate thread (non-daemon so it can be stopped)
    flask_thread = threading.Thread(target=run_flask, daemon=False)
    flask_thread.start()
    log.info("Flask health server running on port %s", os.environ.get("PORT", 5000))

    try:
        asyncio.run(bot_main_with_retry())
    finally:
        # Ensure Flask server is stopped even if bot_main raises
        stop_flask()
        if flask_thread.is_alive():
            flask_thread.join(timeout=5)

if __name__ == "__main__":
    main()
