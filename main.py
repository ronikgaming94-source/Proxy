import subprocess, sys, importlib

def _pip(pkg):
    print(f"[setup] installing {pkg} ...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", pkg])

try:
    import pg8000.dbapi
except ImportError:
    _pip("pg8000"); import pg8000.dbapi

try:
    from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
except ImportError:
    _pip("python-telegram-bot==20.7")
    from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton

import os, re, asyncio, hashlib, time, random, string, traceback, threading, ssl as _ssl
try:
    import pg8000.exceptions as _pg8000exc
    _NET_ERRS = (BrokenPipeError, OSError, _pg8000exc.InterfaceError)
except Exception:
    _NET_ERRS = (BrokenPipeError, OSError)
from datetime import datetime
from urllib.parse import quote, urlparse, parse_qs
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes,
)

# ═══════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURE HERE
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN    = "8939528828:AAFtb-CoWHoP25qiRNf6BsZ2Qjwwr813QH4"
DATABASE_URL = "postgresql://neondb_owner:npg_BWTXNCu1nS4f@ep-broad-voice-adwzv8yt-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
ADMIN_IDS    = ["713914937"]
BOT_USERNAME = "ProxyReferBot"
# ═══════════════════════════════════════════════════════════════

PROXY_COST      = 10
REFERRAL_REWARD = 5

# ─── Markdown escape (v1) ────────────────────────────────────
def esc(text):
    """Escape Markdown v1 special chars in dynamic content."""
    if text is None:
        return ""
    for ch in ("_", "*", "`", "["):
        text = str(text).replace(ch, f"\\{ch}")
    return text


# ═══════════════════════════════════════════════════════════════
#  DATABASE  (pg8000 — pure Python, auto-reconnect)
# ═══════════════════════════════════════════════════════════════

def _pg_kw(url):
    r  = urlparse(url)
    qs = parse_qs(r.query)
    kw = {
        "host":     r.hostname,
        "port":     r.port or 5432,
        "database": r.path.lstrip("/"),
        "user":     r.username,
        "password": r.password,
    }
    if qs.get("sslmode", [""])[0] in ("require", "verify-ca", "verify-full"):
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = _ssl.CERT_NONE
        kw["ssl_context"]  = ctx
    return kw


_PG_KW = _pg_kw(DATABASE_URL)


def _new_conn():
    return pg8000.dbapi.connect(**_PG_KW)


class _PgPool:
    def __init__(self, maxconn=10):
        self._lock    = threading.Lock()
        self._free    = []
        self._maxconn = maxconn

    def getconn(self):
        with self._lock:
            if self._free:
                return self._free.pop()
        return _new_conn()

    def putconn(self, conn):
        try:
            conn.rollback()
        except Exception:
            return
        with self._lock:
            if len(self._free) < self._maxconn:
                self._free.append(conn)


_pool      = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = _PgPool()
    return _pool


def _drain_pool():
    """Discard ALL pooled connections (called after any network error)."""
    pool = _get_pool()
    with pool._lock:
        stale, pool._free = pool._free, []
    for c in stale:
        try: c.close()
        except Exception: pass


def _db(sql, params=None, fetch="none"):
    """Execute SQL — retries once on ANY network/connection error.

    pg8000 fetchone()  → [v1, v2, ...]          (single row as list)
    pg8000 fetchall()  → [[v1,v2,...], ...]      (list of row-lists)
    """
    for attempt in range(3):
        # Always use a fresh connection on retries
        conn = _new_conn() if attempt > 0 else _get_pool().getconn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params or [])
            conn.commit()
            result = None
            if fetch == "one":
                row = cur.fetchone()
                if row is not None:
                    cols   = [d[0] for d in cur.description]
                    result = dict(zip(cols, row))
            elif fetch == "all":
                rows = cur.fetchall() or []
                cols = [d[0] for d in cur.description]
                result = [dict(zip(cols, r)) for r in rows]
            _get_pool().putconn(conn)
            return result
        except _NET_ERRS:
            # Discard this connection + wipe entire pool (all conns are stale)
            try: conn.close()
            except Exception: pass
            _drain_pool()
            if attempt == 2:
                raise
        except Exception:
            try:
                conn.rollback()
                _get_pool().putconn(conn)
            except Exception:
                pass
            raise


async def _q(sql, params=None):
    return await asyncio.to_thread(_db, sql, params, "none")

async def _one(sql, params=None):
    return await asyncio.to_thread(_db, sql, params, "one")

async def _all(sql, params=None):
    return await asyncio.to_thread(_db, sql, params, "all")


# ── Schema ────────────────────────────────────────────────────
def _schema_sync():
    conn = _new_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL PRIMARY KEY,
                telegram_id     TEXT UNIQUE NOT NULL,
                username        TEXT,
                first_name      TEXT,
                last_name       TEXT,
                points          INTEGER NOT NULL DEFAULT 0,
                referral_code   TEXT UNIQUE NOT NULL,
                referred_by     TEXT,
                is_banned       BOOLEAN NOT NULL DEFAULT FALSE,
                is_admin        BOOLEAN NOT NULL DEFAULT FALSE,
                total_referrals INTEGER NOT NULL DEFAULT 0,
                claimed_rewards INTEGER NOT NULL DEFAULT 0,
                join_date       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_active     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS proxies (
                id          SERIAL PRIMARY KEY,
                username    TEXT NOT NULL,
                password    TEXT NOT NULL,
                server      TEXT NOT NULL,
                port        INTEGER NOT NULL,
                status      TEXT NOT NULL DEFAULT 'available',
                claimed_by  TEXT,
                claimed_at  TIMESTAMPTZ,
                point_cost  INTEGER NOT NULL DEFAULT 10,
                added_by    TEXT,
                notes       TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS referrals (
                id           SERIAL PRIMARY KEY,
                referrer_id  TEXT NOT NULL,
                referee_id   TEXT UNIQUE NOT NULL,
                reward_given BOOLEAN NOT NULL DEFAULT FALSE,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS claims (
                id              SERIAL PRIMARY KEY,
                telegram_id     TEXT NOT NULL,
                proxy_id        INTEGER NOT NULL,
                points_deducted INTEGER NOT NULL,
                proxy_server    TEXT NOT NULL,
                proxy_port      INTEGER NOT NULL,
                proxy_username  TEXT NOT NULL,
                proxy_password  TEXT NOT NULL,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id           SERIAL PRIMARY KEY,
                telegram_id  TEXT NOT NULL,
                type         TEXT NOT NULL,
                points       INTEGER NOT NULL,
                description  TEXT NOT NULL,
                reference_id TEXT,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS codes (
                id          SERIAL PRIMARY KEY,
                code        TEXT UNIQUE NOT NULL,
                points      INTEGER NOT NULL,
                max_uses    INTEGER NOT NULL DEFAULT 1,
                used_count  INTEGER NOT NULL DEFAULT 0,
                is_active   BOOLEAN NOT NULL DEFAULT TRUE,
                created_by  TEXT,
                expires_at  TIMESTAMPTZ,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS code_redeems (
                id           SERIAL PRIMARY KEY,
                code_id      INTEGER NOT NULL,
                telegram_id  TEXT NOT NULL,
                points_given INTEGER NOT NULL,
                redeemed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(code_id, telegram_id)
            );
            CREATE TABLE IF NOT EXISTS settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS channels (
                id            SERIAL PRIMARY KEY,
                channel_id    TEXT UNIQUE NOT NULL,
                channel_title TEXT,
                is_active     BOOLEAN NOT NULL DEFAULT TRUE,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS admin_logs (
                id          SERIAL PRIMARY KEY,
                admin_id    TEXT NOT NULL,
                action      TEXT NOT NULL,
                target_id   TEXT,
                details     TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS broadcasts (
                id          SERIAL PRIMARY KEY,
                admin_id    TEXT NOT NULL,
                message     TEXT NOT NULL,
                sent_count  INTEGER NOT NULL DEFAULT 0,
                fail_count  INTEGER NOT NULL DEFAULT 0,
                sent_at     TIMESTAMPTZ,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        conn.commit()
        print("[DB] Schema ready")
    finally:
        try: conn.close()
        except Exception: pass

async def init_schema():
    await asyncio.to_thread(_schema_sync)


# ═══════════════════════════════════════════════════════════════
#  ORM CLASSES
# ═══════════════════════════════════════════════════════════════

class Users:
    @staticmethod
    async def get(tid):
        return await _one("SELECT * FROM users WHERE telegram_id=%s", [str(tid)])

    @staticmethod
    async def create(data):
        return await _one(
            "INSERT INTO users (telegram_id,username,first_name,last_name,referral_code,referred_by,is_admin) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            [data["telegramId"], data.get("username"), data.get("firstName"),
             data.get("lastName"), data["referralCode"], data.get("referredBy"),
             data.get("isAdmin", False)]
        )

    @staticmethod
    async def touch(tid, username):
        await _q("UPDATE users SET last_active=NOW(),username=COALESCE(%s,username) WHERE telegram_id=%s",
                 [username, str(tid)])

    @staticmethod
    async def add_points(tid, pts):
        return await _one("UPDATE users SET points=points+%s WHERE telegram_id=%s RETURNING points",
                          [pts, str(tid)])

    @staticmethod
    async def deduct_points(tid, pts):
        return await _one(
            "UPDATE users SET points=points-%s,claimed_rewards=claimed_rewards+1 "
            "WHERE telegram_id=%s AND points>=%s RETURNING points",
            [pts, str(tid), pts]
        )

    @staticmethod
    async def set_points(tid, pts):
        await _q("UPDATE users SET points=%s WHERE telegram_id=%s", [pts, str(tid)])

    @staticmethod
    async def reset_points(tid):
        await _q("UPDATE users SET points=0 WHERE telegram_id=%s", [str(tid)])

    @staticmethod
    async def increment_referrals(tid):
        await _q("UPDATE users SET total_referrals=total_referrals+1 WHERE telegram_id=%s", [str(tid)])

    @staticmethod
    async def ban(tid):
        await _q("UPDATE users SET is_banned=TRUE WHERE telegram_id=%s", [str(tid)])

    @staticmethod
    async def unban(tid):
        await _q("UPDATE users SET is_banned=FALSE WHERE telegram_id=%s", [str(tid)])

    @staticmethod
    async def delete(tid):
        await _q("DELETE FROM users WHERE telegram_id=%s", [str(tid)])

    @staticmethod
    async def get_by_code(code):
        return await _one("SELECT * FROM users WHERE referral_code=%s", [code])

    @staticmethod
    async def list(limit=50, offset=0):
        return await _all("SELECT * FROM users ORDER BY created_at DESC LIMIT %s OFFSET %s", [limit, offset])

    @staticmethod
    async def search(term):
        return await _all(
            "SELECT * FROM users WHERE telegram_id=%s OR username ILIKE %s OR first_name ILIKE %s LIMIT 20",
            [term, f"%{term}%", f"%{term}%"]
        )

    @staticmethod
    async def count():
        return await _one("SELECT COUNT(*) AS c FROM users")

    @staticmethod
    async def count_banned():
        return await _one("SELECT COUNT(*) AS c FROM users WHERE is_banned=TRUE")

    @staticmethod
    async def count_active():
        return await _one("SELECT COUNT(*) AS c FROM users WHERE last_active>=NOW()-INTERVAL '24 hours'")

    @staticmethod
    async def get_all():
        return await _all("SELECT telegram_id FROM users WHERE is_banned=FALSE")


class Proxies:
    @staticmethod
    async def get_available():
        return await _one("SELECT * FROM proxies WHERE status='available' ORDER BY id ASC LIMIT 1")

    @staticmethod
    async def count_available():
        return await _one("SELECT COUNT(*) AS c FROM proxies WHERE status='available'")

    @staticmethod
    async def count():
        return await _one("SELECT COUNT(*) AS c FROM proxies")

    @staticmethod
    async def claim(proxy_id, tid):
        await _q("UPDATE proxies SET status='claimed',claimed_by=%s,claimed_at=NOW() WHERE id=%s",
                 [str(tid), proxy_id])

    @staticmethod
    async def add(data):
        return await _one(
            "INSERT INTO proxies (username,password,server,port,point_cost,notes,added_by) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            [data["username"], data["password"], data["server"], int(data["port"]),
             data.get("pointCost", 10), data.get("notes"), data.get("addedBy")]
        )

    @staticmethod
    async def delete(proxy_id):
        await _q("DELETE FROM proxies WHERE id=%s", [proxy_id])

    @staticmethod
    async def list(status=None):
        if status:
            return await _all("SELECT * FROM proxies WHERE status=%s ORDER BY created_at DESC", [status])
        return await _all("SELECT * FROM proxies ORDER BY created_at DESC")


class Referrals:
    @staticmethod
    async def create(referrer_id, referee_id):
        return await _one(
            "INSERT INTO referrals (referrer_id,referee_id,reward_given) VALUES (%s,%s,TRUE) RETURNING *",
            [str(referrer_id), str(referee_id)]
        )

    @staticmethod
    async def exists(referee_id):
        return await _one("SELECT id FROM referrals WHERE referee_id=%s", [str(referee_id)])

    @staticmethod
    async def list_by_referrer(referrer_id):
        return await _all(
            "SELECT r.*,u.username,u.first_name FROM referrals r "
            "LEFT JOIN users u ON r.referee_id=u.telegram_id "
            "WHERE r.referrer_id=%s ORDER BY r.created_at DESC",
            [str(referrer_id)]
        )

    @staticmethod
    async def list_all(limit=100):
        return await _all("SELECT * FROM referrals ORDER BY created_at DESC LIMIT %s", [limit])

    @staticmethod
    async def count():
        return await _one("SELECT COUNT(*) AS c FROM referrals")

    @staticmethod
    async def count_daily():
        return await _one("SELECT COUNT(*) AS c FROM referrals WHERE created_at>=NOW()-INTERVAL '24 hours'")


class Claims:
    @staticmethod
    async def create(data):
        await _q(
            "INSERT INTO claims (telegram_id,proxy_id,points_deducted,proxy_server,proxy_port,proxy_username,proxy_password) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            [data["telegramId"], data["proxyId"], data["pointsDeducted"],
             data["proxyServer"], data["proxyPort"], data["proxyUsername"], data["proxyPassword"]]
        )

    @staticmethod
    async def list_by_user(tid, limit=5):
        return await _all("SELECT * FROM claims WHERE telegram_id=%s ORDER BY created_at DESC LIMIT %s",
                          [str(tid), limit])

    @staticmethod
    async def count():
        return await _one("SELECT COUNT(*) AS c FROM claims")


class Transactions:
    @staticmethod
    async def create(tid, type_, points, description, reference_id=None):
        await _q(
            "INSERT INTO transactions (telegram_id,type,points,description,reference_id) VALUES (%s,%s,%s,%s,%s)",
            [str(tid), type_, points, description,
             str(reference_id) if reference_id is not None else None]
        )


class Codes:
    @staticmethod
    async def get(code):
        return await _one("SELECT * FROM codes WHERE code=%s AND is_active=TRUE", [code.upper()])

    @staticmethod
    async def create(data: dict):
        return await _one(
            "INSERT INTO codes (code,points,max_uses,created_by,expires_at) VALUES (%s,%s,%s,%s,%s) RETURNING *",
            [data["code"].upper(), data["points"], data.get("maxUses", 1),
             data.get("createdBy"), data.get("expiresAt")]
        )

    @staticmethod
    async def increment_use(code_id):
        await _q("UPDATE codes SET used_count=used_count+1 WHERE id=%s", [code_id])

    @staticmethod
    async def deactivate(code_id):
        await _q("UPDATE codes SET is_active=FALSE WHERE id=%s", [code_id])

    @staticmethod
    async def delete_by_code(code):
        await _q("DELETE FROM codes WHERE UPPER(code)=UPPER(%s)", [code])

    @staticmethod
    async def list_all():
        return await _all("SELECT * FROM codes ORDER BY created_at DESC")

    @staticmethod
    async def has_redeemed(code_id, tid):
        return await _one("SELECT id FROM code_redeems WHERE code_id=%s AND telegram_id=%s",
                          [code_id, str(tid)])

    @staticmethod
    async def record_redeem(code_id, tid, points):
        await _q("INSERT INTO code_redeems (code_id,telegram_id,points_given) VALUES (%s,%s,%s)",
                 [code_id, str(tid), points])


class Settings:
    @staticmethod
    async def get(key):
        return await _one("SELECT value FROM settings WHERE key=%s", [key])

    @staticmethod
    async def set(key, value):
        await _q(
            "INSERT INTO settings (key,value) VALUES (%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=%s,updated_at=NOW()",
            [key, value, value]
        )

    @staticmethod
    async def get_bool(key, fallback=False):
        row = await Settings.get(key)
        return (row["value"] == "true") if row else fallback

    @staticmethod
    async def get_string(key, fallback=""):
        row = await Settings.get(key)
        return row["value"] if row else fallback


class Channels:
    @staticmethod
    async def list_active():
        return await _all("SELECT * FROM channels WHERE is_active=TRUE")

    @staticmethod
    async def list_all():
        return await _all("SELECT * FROM channels ORDER BY created_at DESC")

    @staticmethod
    async def add(channel_id, title):
        await _q(
            "INSERT INTO channels (channel_id,channel_title) VALUES (%s,%s) "
            "ON CONFLICT (channel_id) DO NOTHING",
            [channel_id, title]
        )

    @staticmethod
    async def remove(cid):
        await _q("DELETE FROM channels WHERE id=%s", [cid])


class AdminLogs:
    @staticmethod
    async def create(admin_id, action, target_id=None, details=None):
        await _q(
            "INSERT INTO admin_logs (admin_id,action,target_id,details) VALUES (%s,%s,%s,%s)",
            [str(admin_id), action,
             str(target_id) if target_id is not None else None, details]
        )

    @staticmethod
    async def list_all(limit=20):
        return await _all("SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT %s", [limit])


class Broadcasts:
    @staticmethod
    async def create(admin_id, message):
        return await _one(
            "INSERT INTO broadcasts (admin_id,message) VALUES (%s,%s) RETURNING *",
            [str(admin_id), message]
        )

    @staticmethod
    async def mark_sent(bid, sent, fail):
        await _q("UPDATE broadcasts SET sent_at=NOW(),sent_count=%s,fail_count=%s WHERE id=%s",
                 [sent, fail, bid])

    @staticmethod
    async def list_all(limit=10):
        return await _all("SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT %s", [limit])


class Analytics:
    @staticmethod
    async def get_dashboard():
        (tu, bu, au, tr, dr, tc, ps) = await asyncio.gather(
            Users.count(), Users.count_banned(), Users.count_active(),
            Referrals.count(), Referrals.count_daily(),
            Claims.count(), Proxies.count_available(),
        )
        return {
            "totalUsers":     int(tu["c"]),
            "bannedUsers":    int(bu["c"]),
            "activeUsers":    int(au["c"]),
            "totalReferrals": int(tr["c"]),
            "dailyReferrals": int(dr["c"]),
            "totalClaims":    int(tc["c"]),
            "proxyStock":     int(ps["c"]),
        }


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def is_admin(uid):
    return str(uid) in ADMIN_IDS

def gen_ref_code(uid):
    raw = f"{uid}{int(time.time()*1000)}".encode()
    return hashlib.sha256(raw).hexdigest()[:10].upper()

def referral_link(code):
    return f"https://t.me/{BOT_USERNAME}?start={code}"

async def get_proxy_cost():
    val = await Settings.get_string("proxy_cost", "")
    try: return int(val) if val else PROXY_COST
    except ValueError: return PROXY_COST

async def get_referral_reward():
    val = await Settings.get_string("referral_reward", "")
    try: return int(val) if val else REFERRAL_REWARD
    except ValueError: return REFERRAL_REWARD

async def get_welcome_text(first_name, ref_link, points):
    """Build welcome message — uses custom template if set by admin."""
    pc  = await get_proxy_cost()
    rr  = await get_referral_reward()
    tpl = await Settings.get_string("welcome_message", "")
    if tpl:
        return (tpl
                .replace("{name}",   first_name)
                .replace("{points}", str(points))
                .replace("{link}",   ref_link)
                .replace("{cost}",   str(pc))
                .replace("{reward}", str(rr)))
    return (
        f"👋 Welcome, *{esc(first_name)}*!\n\n"
        "Earn points by referring friends and redeem them for premium proxies.\n\n"
        "📊 *How it works:*\n"
        f"• Share your referral link → friend joins\n"
        f"• You earn *{rr} points* instantly\n"
        f"• Redeem *{pc} points* for 1 proxy\n\n"
        f"💰 Your Points: *{points}*\n"
        f"🔗 Your link:\n`{ref_link}`\n\n"
        "Use the menu below to get started."
    )

async def get_or_create(from_user):
    user = await Users.get(str(from_user.id))
    if not user:
        user = await Users.create({
            "telegramId":   str(from_user.id),
            "username":     from_user.username,
            "firstName":    from_user.first_name,
            "lastName":     from_user.last_name,
            "referralCode": gen_ref_code(from_user.id),
            "isAdmin":      is_admin(from_user.id),
        })
    else:
        await Users.touch(str(from_user.id), from_user.username)
    return user

async def check_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    on = await Settings.get_bool("maintenance", False)
    if not on or is_admin(update.effective_user.id):
        return False
    msg = await Settings.get_string("maintenance_message",
                                    "🔧 Bot is under maintenance. Please try again later.")
    await update.effective_message.reply_text(msg)
    return True

async def check_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    channels = await Channels.list_active()
    if not channels:
        return True
    not_joined = []
    for ch in channels:
        try:
            m = await context.bot.get_chat_member(ch["channel_id"], update.effective_user.id)
            if m.status in ("left", "kicked"):
                not_joined.append(ch)
        except Exception:
            not_joined.append(ch)
    if not not_joined:
        return True
    btns = []
    for ch in not_joined:
        title = ch["channel_title"] or ch["channel_id"]
        cid   = ch["channel_id"].replace("@", "")
        btns.append([InlineKeyboardButton(f"📢 Join {title}", url=f"https://t.me/{cid}")])
    btns.append([InlineKeyboardButton("✅ I Joined — Check Again", callback_data="check_sub")])
    await update.effective_message.reply_text(
        "⚠️ *Please join our channel(s) to use this bot:*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return False


# ═══════════════════════════════════════════════════════════════
#  UI BUILDERS
# ═══════════════════════════════════════════════════════════════

def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Claim Proxy",   callback_data="menu_claim"),
         InlineKeyboardButton("👥 Refer Friends", callback_data="menu_refer")],
        [InlineKeyboardButton("📖 How To Use",    callback_data="menu_howto"),
         InlineKeyboardButton("⚠️ Disclaimer",    callback_data="menu_disclaimer")],
        [InlineKeyboardButton("👤 My Profile",    callback_data="menu_profile"),
         InlineKeyboardButton("🔗 My Referrals",  callback_data="menu_referrals")],
        [InlineKeyboardButton("🎁 My Rewards",    callback_data="menu_rewards"),
         InlineKeyboardButton("🎫 Redeem Code",   callback_data="menu_redeem")],
    ])

def admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Dashboard",  callback_data="adm_dashboard"),
         InlineKeyboardButton("👥 Users",      callback_data="adm_users")],
        [InlineKeyboardButton("🌐 Proxies",    callback_data="adm_proxies"),
         InlineKeyboardButton("🔗 Referrals",  callback_data="adm_referrals")],
        [InlineKeyboardButton("🎫 Codes",      callback_data="adm_codes"),
         InlineKeyboardButton("📢 Broadcast",  callback_data="adm_broadcast")],
        [InlineKeyboardButton("⚙️ Settings",   callback_data="adm_settings"),
         InlineKeyboardButton("📡 Channels",   callback_data="adm_channels")],
        [InlineKeyboardButton("📋 Admin Logs", callback_data="adm_logs"),
         InlineKeyboardButton("📈 Analytics",  callback_data="adm_analytics")],
    ])

def proxy_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Proxy",      callback_data="adm_prx_add"),
         InlineKeyboardButton("📥 Bulk Import",    callback_data="adm_prx_bulk")],
        [InlineKeyboardButton("📋 Available",      callback_data="adm_prx_avail"),
         InlineKeyboardButton("📦 Claimed",        callback_data="adm_prx_claimed")],
        [InlineKeyboardButton("🗑 Delete Proxy",   callback_data="adm_prx_del")],
        [InlineKeyboardButton("◀️ Back",           callback_data="adm_dashboard")],
    ])

def codes_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Quick Generate", callback_data="adm_gencode"),
         InlineKeyboardButton("➕ Create Code",    callback_data="adm_code_create")],
        [InlineKeyboardButton("📋 Active Codes",   callback_data="adm_codes_list"),
         InlineKeyboardButton("🗑 Delete Code",    callback_data="adm_code_del")],
        [InlineKeyboardButton("◀️ Back",           callback_data="adm_dashboard")],
    ])

BACK_BTN = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="adm_dashboard")]])

def build_dashboard_text(d):
    return (
        f"📊 *Admin Dashboard*\n\n"
        f"👥 *Users:* {d['totalUsers']} total | {d['activeUsers']} active | {d['bannedUsers']} banned\n"
        f"🔗 *Referrals:* {d['totalReferrals']} total | {d['dailyReferrals']} today\n"
        f"🌐 *Proxies:* {d['proxyStock']} available | {d['totalClaims']} claimed total"
    )


# ═══════════════════════════════════════════════════════════════
#  DECORATORS
# ═══════════════════════════════════════════════════════════════

def _safe(func):
    """Wrap callback in try/except — one crash never kills the bot."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            return await func(update, context)
        except Exception as e:
            print(f"[ERROR] {func.__name__}: {e}\n{traceback.format_exc()}")
            try:
                if update.callback_query:
                    await update.callback_query.answer("⚠️ An error occurred. Please try again.")
                elif update.effective_message:
                    await update.effective_message.reply_text("⚠️ An error occurred. Please try again.")
            except Exception:
                pass
    return wrapper

def _admin_guard(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            if update.callback_query:
                await update.callback_query.answer("⛔ Access denied.")
            return
        try:
            return await func(update, context)
        except Exception as e:
            print(f"[ERROR] {func.__name__}: {e}\n{traceback.format_exc()}")
            try:
                if update.callback_query:
                    await update.callback_query.answer("⚠️ Error. Check logs.")
                elif update.effective_message:
                    await update.effective_message.reply_text("⚠️ Error occurred.")
            except Exception:
                pass
    return wrapper


# ═══════════════════════════════════════════════════════════════
#  STATIC TEXTS
# ═══════════════════════════════════════════════════════════════

TEXT_HOW_TO_USE = (
    "📖 *How To Use a Proxy*\n\n"
    "1. Open *Telegram Settings*\n"
    "2. Select *Data and Storage*\n"
    "3. Open *Proxy Settings*\n"
    "4. Tap *Add Proxy*\n"
    "5. Enter *Server*, *Port*, *Username*, *Password*\n"
    "6. Save and turn proxy *ON*\n\n"
    "💡 If proxy is slow or not working, claim another one."
)

TEXT_DISCLAIMER = (
    "⚠️ *Disclaimer*\n\n"
    "Sometimes you may receive a dead or non-working proxy.\n\n"
    "If that happens, simply claim another proxy.\n\n"
    "Proxy availability may vary by region and server status.\n\n"
    "We are not responsible for proxy uptime or performance."
)


# ═══════════════════════════════════════════════════════════════
#  USER COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════

@_safe
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_maintenance(update, context):
        return
    parts     = update.message.text.split(" ", 1)
    payload   = parts[1].strip() if len(parts) > 1 else None
    from_user = update.effective_user
    my_id     = str(from_user.id)

    referred_by = None
    if payload:
        referrer = await Users.get_by_code(payload)
        if referrer and referrer["telegram_id"] != my_id:
            referred_by = referrer["telegram_id"]

    existing = await Users.get(my_id)
    is_new   = not existing
    user     = await get_or_create(from_user)

    if is_new and referred_by:
        if not await Referrals.exists(my_id):
            rr = await get_referral_reward()
            await Referrals.create(referred_by, my_id)
            await Users.add_points(referred_by, rr)
            await Users.increment_referrals(referred_by)
            await Transactions.create(
                referred_by, "earn_referral", rr,
                f"Referral from {('@' + from_user.username) if from_user.username else my_id}",
                my_id
            )
            try:
                ref = await Users.get(referred_by)
                if ref:
                    await context.bot.send_message(
                        referred_by,
                        f"🎉 *New Referral!*\n\n"
                        f"{esc(from_user.first_name or 'Someone')} joined using your link.\n\n"
                        f"💰 You earned *{rr} points*!\n"
                        f"🏦 New Balance: *{ref['points'] + rr} points*",
                        parse_mode="Markdown"
                    )
            except Exception:
                pass

    if not await check_subscribe(update, context):
        return

    welcome = await get_welcome_text(
        from_user.first_name or "User",
        referral_link(user["referral_code"]),
        user["points"]
    )
    await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=main_menu())


@_safe
async def cmd_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_maintenance(update, context):
        return
    parts = update.message.text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text("Usage: /redeem <CODE>")
        return

    code = parts[1].strip().upper()
    user = await get_or_create(update.effective_user)
    if user["is_banned"]:
        await update.message.reply_text("🚫 Your account is banned.")
        return

    code_record = await Codes.get(code)
    if not code_record:
        await update.message.reply_text("❌ Invalid or expired code.")
        return
    if code_record["used_count"] >= code_record["max_uses"]:
        await update.message.reply_text("❌ This code has reached its usage limit.")
        return
    if await Codes.has_redeemed(code_record["id"], str(update.effective_user.id)):
        await update.message.reply_text("❌ You have already redeemed this code.")
        return

    await Codes.increment_use(code_record["id"])
    await Codes.record_redeem(code_record["id"], str(update.effective_user.id), code_record["points"])
    await Users.add_points(str(update.effective_user.id), code_record["points"])
    await Transactions.create(
        str(update.effective_user.id), "code_redeem", code_record["points"],
        f"Redeemed code {code}", str(code_record["id"])
    )
    new_balance = user["points"] + code_record["points"]
    await update.message.reply_text(
        f"🎉 *Code Redeemed!*\n\n"
        f"🎫 Code: `{code}`\n"
        f"💰 Points Added: *+{code_record['points']}*\n"
        f"🏦 New Balance: *{new_balance} points*",
        parse_mode="Markdown",
        reply_markup=main_menu()
    )


# ═══════════════════════════════════════════════════════════════
#  USER CALLBACK HANDLERS
# ═══════════════════════════════════════════════════════════════

@_safe
async def cb_check_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await check_subscribe(update, context):
        return
    user    = await get_or_create(update.effective_user)
    welcome = await get_welcome_text(
        update.effective_user.first_name or "User",
        referral_link(user["referral_code"]), user["points"]
    )
    await query.edit_message_text(welcome, parse_mode="Markdown", reply_markup=main_menu())


@_safe
async def cb_menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user    = await get_or_create(update.effective_user)
    welcome = await get_welcome_text(
        update.effective_user.first_name or "User",
        referral_link(user["referral_code"]), user["points"]
    )
    await query.edit_message_text(welcome, parse_mode="Markdown", reply_markup=main_menu())


@_safe
async def cb_menu_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await check_maintenance(update, context):
        return
    user = await get_or_create(update.effective_user)
    if user["is_banned"]:
        await query.edit_message_text("🚫 Your account is banned.")
        return

    stock = int((await Proxies.count_available())["c"])
    cost  = await get_proxy_cost()

    if stock == 0:
        await query.edit_message_text(
            "⚠️ *No Proxy Stock*\n\nAll proxies are currently claimed. Check back later!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
        )
        return

    can_claim = user["points"] >= cost
    status_text = (
        "✅ You have enough points to claim."
        if can_claim
        else f"❌ You need *{cost - user['points']}* more points. Refer friends to earn!"
    )
    action_row = (
        [InlineKeyboardButton("✅ Claim Proxy Now", callback_data="do_claim")]
        if can_claim
        else [InlineKeyboardButton("👥 Refer & Earn Points", callback_data="menu_refer")]
    )
    await query.edit_message_text(
        f"🌐 *Claim a Proxy*\n\n"
        f"💰 Cost: *{cost} points*\n"
        f"💳 Your Balance: *{user['points']} points*\n"
        f"📦 Stock: *{stock} available*\n\n{status_text}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            action_row,
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")],
        ])
    )


@_safe
async def cb_do_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await check_maintenance(update, context):
        return

    # ── Quick animation ──────────────────────────────────────
    await query.edit_message_text("⏳ Checking your balance...")
    await asyncio.sleep(0.3)
    await query.edit_message_text("🔍 Finding an available proxy...")
    await asyncio.sleep(0.3)

    user = await get_or_create(update.effective_user)
    if user["is_banned"]:
        await query.edit_message_text("🚫 Your account is banned.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]]))
        return

    proxy = await Proxies.get_available()
    if not proxy:
        await query.edit_message_text(
            "⚠️ Proxy stock is empty. Please try again later.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
        )
        return

    cost = await get_proxy_cost()
    if user["points"] < cost:
        await query.edit_message_text(
            f"❌ *Insufficient Points*\n\nYou need *{cost}* points but have *{user['points']}*.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
        )
        return

    updated = await Users.deduct_points(str(update.effective_user.id), cost)
    if not updated:
        await query.edit_message_text(
            "❌ Failed to deduct points. Please try again.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
        )
        return

    await Proxies.claim(proxy["id"], str(update.effective_user.id))
    await Claims.create({
        "telegramId":     str(update.effective_user.id),
        "proxyId":        proxy["id"],
        "pointsDeducted": cost,
        "proxyServer":    proxy["server"],
        "proxyPort":      proxy["port"],
        "proxyUsername":  proxy["username"],
        "proxyPassword":  proxy["password"],
    })
    await Transactions.create(
        str(update.effective_user.id), "claim_proxy", -cost,
        f"Claimed proxy {proxy['server']}:{proxy['port']}", str(proxy["id"])
    )

    new_balance = user["points"] - cost
    await query.edit_message_text(
        f"✅ *Proxy Claimed!*\n\n"
        f"```\nServer:   {proxy['server']}\nPort:     {proxy['port']}\n"
        f"Username: {proxy['username']}\nPassword: {proxy['password']}\n```\n\n"
        f"💳 Points Used: *{cost}* | 💰 Left: *{new_balance}*\n\n"
        f"_Tap proxy info above to copy._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Claim Another", callback_data="menu_claim"),
             InlineKeyboardButton("◀️ Menu",          callback_data="menu_main")]
        ])
    )


@_safe
async def cb_menu_refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = await get_or_create(update.effective_user)
    rr   = await get_referral_reward()
    link = referral_link(user["referral_code"])
    await query.edit_message_text(
        f"👥 *Refer & Earn*\n\n"
        f"• Share your referral link with friends\n"
        f"• They start the bot using your link\n"
        f"• You earn *{rr} points* instantly!\n\n"
        f"🔗 *Your Link:*\n`{link}`\n\n"
        f"📊 Referrals: *{user['total_referrals']}* | Points Earned: *{user['total_referrals'] * rr}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Share",
                                  url=f"https://t.me/share/url?url={quote(link)}&text={quote('Get free proxies!')}")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")],
        ])
    )


@_safe
async def cb_menu_howto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        TEXT_HOW_TO_USE, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
    )


@_safe
async def cb_menu_disclaimer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        TEXT_DISCLAIMER, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
    )


@_safe
async def cb_menu_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    await query.answer()
    user     = await get_or_create(update.effective_user)
    join     = user.get("join_date") or user.get("created_at")
    join_str = join.strftime("%m/%d/%Y") if join else "—"
    uname    = f"@{esc(user['username'])}" if user["username"] else "—"
    await query.edit_message_text(
        f"👤 *My Profile*\n\n"
        f"🆔 ID: `{user['telegram_id']}`\n"
        f"👤 Username: {uname}\n"
        f"💰 Points: *{user['points']}*\n"
        f"🔗 Referrals: *{user['total_referrals']}*\n"
        f"🎁 Proxies Claimed: *{user['claimed_rewards']}*\n"
        f"📅 Joined: {join_str}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
    )


@_safe
async def cb_menu_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    user   = await get_or_create(update.effective_user)
    refs   = await Referrals.list_by_referrer(str(update.effective_user.id))
    rr     = await get_referral_reward()
    link   = referral_link(user["referral_code"])
    lines  = []
    for r in refs[:10]:
        name = esc(r.get("first_name") or (f"@{r['username']}" if r.get("username") else f"User {r['referee_id']}"))
        date = r["created_at"].strftime("%m/%d/%Y") if r.get("created_at") else ""
        lines.append(f"• {name} — {date}")
    body = "\n".join(lines) if lines else "_No referrals yet. Share your link!_"
    await query.edit_message_text(
        f"🔗 *My Referrals*\n\n"
        f"Total: *{len(refs)}* | Points Earned: *{len(refs) * rr}*\n\n"
        f"{body}\n\n🔗 `{link}`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={quote(link)}")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")],
        ])
    )


@_safe
async def cb_menu_rewards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    user   = await get_or_create(update.effective_user)
    claims = await Claims.list_by_user(str(update.effective_user.id), 5)
    pc     = await get_proxy_cost()
    lines  = []
    for c in claims:
        date = c["created_at"].strftime("%m/%d/%Y") if c.get("created_at") else ""
        lines.append(f"• `{esc(c['proxy_server'])}:{c['proxy_port']}` — {date}")
    body = "\n".join(lines) if lines else "_No proxies claimed yet._"
    await query.edit_message_text(
        f"🎁 *My Rewards*\n\n"
        f"💰 Points: *{user['points']}* | 📦 Claimed: *{user['claimed_rewards']}*\n\n"
        f"{body}\n\n💡 Each proxy costs *{pc} points*.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Claim Proxy", callback_data="menu_claim")],
            [InlineKeyboardButton("◀️ Back",        callback_data="menu_main")],
        ])
    )


@_safe
async def cb_menu_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "🎫 *Redeem a Code*\n\nSend your code using:\n/redeem CODE\n\nExample: `/redeem PROMO2024`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="menu_main")]])
    )


# ═══════════════════════════════════════════════════════════════
#  ADMIN COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    d = await Analytics.get_dashboard()
    await update.message.reply_text(
        build_dashboard_text(d), parse_mode="Markdown", reply_markup=admin_keyboard()
    )

async def cmd_finduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/finduser (.+)$", update.message.text)
    if not m: return
    users = await Users.search(m.group(1).strip())
    if not users:
        await update.message.reply_text("❌ User not found.")
        return
    u     = users[0]
    join  = (u.get("created_at") or u.get("join_date"))
    jstr  = join.strftime("%m/%d/%Y") if join else "—"
    await update.message.reply_text(
        f"👤 *User Info*\n\n"
        f"ID: `{u['telegram_id']}`\n"
        f"Username: @{esc(u['username'] or '-')}\n"
        f"Name: {esc(u['first_name'] or '')} {esc(u['last_name'] or '')}\n"
        f"Points: *{u['points']}* | Referrals: {u['total_referrals']} | Claims: {u['claimed_rewards']}\n"
        f"Banned: {'🚫 Yes' if u['is_banned'] else '✅ No'}\n"
        f"Joined: {jstr}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 Ban",      callback_data=f"adm_ban_{u['telegram_id']}"),
             InlineKeyboardButton("✅ Unban",    callback_data=f"adm_unban_{u['telegram_id']}")],
            [InlineKeyboardButton("➕ Add 5pts", callback_data=f"adm_add5_{u['telegram_id']}"),
             InlineKeyboardButton("🗑 Delete",  callback_data=f"adm_del_{u['telegram_id']}")],
        ])
    )

async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/ban (.+)$", update.message.text)
    if not m: return
    uid = m.group(1).strip()
    await Users.ban(uid)
    await AdminLogs.create(str(update.effective_user.id), "ban_user", uid)
    await update.message.reply_text(f"🚫 User `{uid}` banned.", parse_mode="Markdown")

async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/unban (.+)$", update.message.text)
    if not m: return
    uid = m.group(1).strip()
    await Users.unban(uid)
    await AdminLogs.create(str(update.effective_user.id), "unban_user", uid)
    await update.message.reply_text(f"✅ User `{uid}` unbanned.", parse_mode="Markdown")

async def cmd_addpoints(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/addpoints (\S+) (\d+)(.*)", update.message.text)
    if not m: return
    uid, pts = m.group(1), int(m.group(2))
    reason   = m.group(3).strip() or "Admin added points"
    user = await Users.get(uid)
    if not user:
        await update.message.reply_text("❌ User not found."); return
    await Users.add_points(uid, pts)
    await Transactions.create(uid, "admin_add", pts, reason, str(update.effective_user.id))
    await AdminLogs.create(str(update.effective_user.id), "add_points", uid, f"{pts} pts")
    await update.message.reply_text(f"✅ Added *{pts}* pts to `{uid}`.", parse_mode="Markdown")

async def cmd_removepoints(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/removepoints (\S+) (\d+)(.*)", update.message.text)
    if not m: return
    uid, pts = m.group(1), int(m.group(2))
    reason   = m.group(3).strip() or "Admin removed points"
    user = await Users.get(uid)
    if not user:
        await update.message.reply_text("❌ User not found."); return
    new_pts = max(0, user["points"] - pts)
    await Users.set_points(uid, new_pts)
    await Transactions.create(uid, "admin_remove", -pts, reason, str(update.effective_user.id))
    await AdminLogs.create(str(update.effective_user.id), "remove_points", uid, f"{pts} pts")
    await update.message.reply_text(f"✅ Removed *{pts}* pts from `{uid}`.", parse_mode="Markdown")

async def cmd_resetpoints(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/resetpoints (.+)$", update.message.text)
    if not m: return
    uid = m.group(1).strip()
    await Users.reset_points(uid)
    await AdminLogs.create(str(update.effective_user.id), "reset_points", uid)
    await update.message.reply_text(f"✅ Points reset for `{uid}`.", parse_mode="Markdown")

async def cmd_deleteuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/deleteuser (.+)$", update.message.text)
    if not m: return
    uid = m.group(1).strip()
    await Users.delete(uid)
    await AdminLogs.create(str(update.effective_user.id), "delete_user", uid)
    await update.message.reply_text(f"🗑 User `{uid}` deleted.", parse_mode="Markdown")

async def cmd_addproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/addproxy (\S+)", update.message.text)
    if not m: return
    parts = m.group(1).split(":")
    if len(parts) < 4:
        await update.message.reply_text("Format: /addproxy server:port:user:pass"); return
    server, port, username = parts[0], parts[1], parts[2]
    password = ":".join(parts[3:])
    proxy = await Proxies.add({
        "server": server, "port": port,
        "username": username, "password": password,
        "addedBy": str(update.effective_user.id)
    })
    await AdminLogs.create(str(update.effective_user.id), "add_proxy", str(proxy["id"]))
    await update.message.reply_text(f"✅ Proxy added: `{server}:{port}` (ID: {proxy['id']})",
                                    parse_mode="Markdown")

async def cmd_importproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    context.user_data["adm_state"] = "bulk_import"
    await update.message.reply_text(
        "📋 Reply to this message with your proxy list.\nFormat (one per line):\n`server:port:username:password`",
        parse_mode="Markdown"
    )

async def cmd_listproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m      = re.match(r"^/listproxies ?(.*)$", update.message.text)
    status = m.group(1).strip() if m and m.group(1).strip() else None
    proxies = await Proxies.list(status)
    if not proxies:
        await update.message.reply_text("No proxies found."); return
    lines = [f"[{p['id']}] `{esc(p['server'])}:{p['port']}` — {p['status']}" for p in proxies[:20]]
    await update.message.reply_text(
        f"🌐 *Proxies* ({len(proxies)} total)\n\n" + "\n".join(lines),
        parse_mode="Markdown"
    )

async def cmd_deleteproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/deleteproxy (\d+)$", update.message.text)
    if not m: return
    pid = int(m.group(1))
    await Proxies.delete(pid)
    await AdminLogs.create(str(update.effective_user.id), "delete_proxy", str(pid))
    await update.message.reply_text(f"🗑 Proxy #{pid} deleted.")

async def cmd_addcode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/addcode (\S+) (\d+)(.*)", update.message.text)
    if not m: return
    code, pts  = m.group(1), int(m.group(2))
    extra      = m.group(3).strip().split()
    max_uses   = int(extra[0]) if extra else 1
    c = await Codes.create({
        "code": code, "points": pts,
        "maxUses": max_uses, "createdBy": str(update.effective_user.id)
    })
    await AdminLogs.create(str(update.effective_user.id), "create_code", c["code"], f"{pts} pts")
    await update.message.reply_text(
        f"✅ Code `{c['code']}` — {pts} pts, max {max_uses} uses.",
        parse_mode="Markdown"
    )

async def cmd_deletecode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/deletecode (.+)$", update.message.text)
    if not m: return
    code = m.group(1).strip()
    await Codes.delete_by_code(code)
    await AdminLogs.create(str(update.effective_user.id), "delete_code", code)
    await update.message.reply_text(f"🗑 Code `{esc(code)}` deleted.", parse_mode="Markdown")

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/broadcast (.+)$", update.message.text, re.DOTALL)
    if not m:
        await update.message.reply_text("Usage: /broadcast <message>"); return
    message = m.group(1).strip()
    users   = await Users.get_all()
    bcast   = await Broadcasts.create(str(update.effective_user.id), message)
    sent = failed = 0
    await update.message.reply_text(f"📢 Broadcasting to {len(users)} users...")
    for u in users:
        try:
            await context.bot.send_message(u["telegram_id"], f"📢 *Announcement*\n\n{message}", parse_mode="Markdown")
            sent += 1
            await asyncio.sleep(0.035)
        except Exception:
            failed += 1
    await Broadcasts.mark_sent(bcast["id"], sent, failed)
    await AdminLogs.create(str(update.effective_user.id), "broadcast", None, f"Sent:{sent} Failed:{failed}")
    await update.message.reply_text(f"✅ Done: Sent {sent} | Failed {failed}")

async def cmd_broadcastlogs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    logs = await Broadcasts.list_all(10)
    if not logs:
        await update.message.reply_text("No broadcasts yet."); return
    lines = []
    for b in logs:
        date = b["created_at"].strftime("%m/%d/%Y") if b.get("created_at") else "—"
        lines.append(f"• {date} — Sent: {b['sent_count']} Failed: {b['fail_count']}")
    await update.message.reply_text("📢 *Recent Broadcasts*\n\n" + "\n".join(lines), parse_mode="Markdown")

async def cmd_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/maintenance (on|off)$", update.message.text)
    if not m:
        await update.message.reply_text("Usage: /maintenance on|off"); return
    toggle = m.group(1)
    await Settings.set("maintenance", "true" if toggle == "on" else "false")
    await AdminLogs.create(str(update.effective_user.id), f"maintenance_{toggle}")
    await update.message.reply_text(f"🔧 Maintenance *{'enabled' if toggle == 'on' else 'disabled'}*.",
                                    parse_mode="Markdown")

async def cmd_setmessage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/setmessage (.+)$", update.message.text, re.DOTALL)
    if not m:
        await update.message.reply_text("Usage: /setmessage <text>"); return
    await Settings.set("maintenance_message", m.group(1).strip())
    await update.message.reply_text("✅ Maintenance message updated.")

async def cmd_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/setwelcome (.+)$", update.message.text, re.DOTALL)
    if not m:
        await update.message.reply_text(
            "✏️ *Edit Welcome Message*\n\n"
            "Usage: `/setwelcome <your message>`\n\n"
            "*Placeholders:*\n"
            "`{name}` — user's first name\n"
            "`{points}` — user's points\n"
            "`{link}` — referral link\n"
            "`{cost}` — proxy cost\n"
            "`{reward}` — referral reward\n\n"
            "To reset to default: `/setwelcome DEFAULT`",
            parse_mode="Markdown"
        )
        return
    text = m.group(1).strip()
    if text.upper() == "DEFAULT":
        await Settings.set("welcome_message", "")
        await update.message.reply_text("✅ Welcome message reset to default.")
    else:
        await Settings.set("welcome_message", text)
        await AdminLogs.create(str(update.effective_user.id), "set_welcome")
        await update.message.reply_text("✅ Welcome message updated!")

async def cmd_addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/addchannel (\S+)(.*)", update.message.text)
    if not m: return
    cid   = m.group(1).strip()
    title = m.group(2).strip() or cid
    await Channels.add(cid, title)
    await AdminLogs.create(str(update.effective_user.id), "add_channel", cid)
    await update.message.reply_text(f"✅ Channel `{esc(cid)}` added.", parse_mode="Markdown")

async def cmd_removechannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    m = re.match(r"^/removechannel (\d+)$", update.message.text)
    if not m: return
    await Channels.remove(int(m.group(1)))
    await AdminLogs.create(str(update.effective_user.id), "remove_channel", m.group(1))
    await update.message.reply_text("✅ Channel removed.")

async def cmd_setcost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    parts = update.message.text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await update.message.reply_text("Usage: /setcost <points>"); return
    n = int(parts[1].strip())
    if n <= 0:
        await update.message.reply_text("❌ Must be > 0."); return
    await Settings.set("proxy_cost", str(n))
    await AdminLogs.create(str(update.effective_user.id), "set_proxy_cost", None, str(n))
    await update.message.reply_text(f"✅ Proxy cost set to *{n} points*.", parse_mode="Markdown")

async def cmd_setreward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    parts = update.message.text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await update.message.reply_text("Usage: /setreward <points>"); return
    n = int(parts[1].strip())
    if n <= 0:
        await update.message.reply_text("❌ Must be > 0."); return
    await Settings.set("referral_reward", str(n))
    await AdminLogs.create(str(update.effective_user.id), "set_referral_reward", None, str(n))
    await update.message.reply_text(f"✅ Referral reward set to *{n} points*.", parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
#  ADMIN CALLBACK HANDLERS
# ═══════════════════════════════════════════════════════════════

@_admin_guard
async def cb_adm_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    d = await Analytics.get_dashboard()
    await query.edit_message_text(build_dashboard_text(d), parse_mode="Markdown",
                                  reply_markup=admin_keyboard())


@_admin_guard
async def cb_adm_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    users = await Users.list(10)
    lines = []
    for u in users:
        name   = esc(u["username"] or u["first_name"] or "?")
        banned = " 🚫" if u["is_banned"] else ""
        lines.append(f"• `{u['telegram_id']}` {name} — {u['points']}pts{banned}")
    text = "\n".join(lines) if lines else "_No users_"
    await query.edit_message_text(
        f"👥 *Recent Users*\n\n{text}\n\n"
        "_Use:_ /finduser \\<id\\> /ban \\<id\\> /unban \\<id\\>\n"
        "/addpoints \\<id\\> \\<pts\\> /removepoints \\<id\\> \\<pts\\>",
        parse_mode="Markdown", reply_markup=BACK_BTN
    )


@_admin_guard
async def cb_adm_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    stock = int((await Proxies.count_available())["c"])
    total = int((await Proxies.count())["c"])
    await query.edit_message_text(
        f"🌐 *Proxy Manager*\n\n✅ Available: *{stock}* | 📦 Total: *{total}*\n\n"
        "Choose an action:",
        parse_mode="Markdown", reply_markup=proxy_keyboard()
    )


@_admin_guard
async def cb_adm_prx_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["adm_state"] = "add_proxy"
    await query.edit_message_text(
        "➕ *Add Proxy*\n\nSend the proxy in this format:\n"
        "`server:port:username:password`\n\nExample:\n`proxy.example.com:8080:user:pass`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_proxies")]])
    )


@_admin_guard
async def cb_adm_prx_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["adm_state"] = "bulk_import"
    await query.edit_message_text(
        "📥 *Bulk Import*\n\nSend one proxy per line:\n`server:port:username:password`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_proxies")]])
    )


@_admin_guard
async def cb_adm_prx_avail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    proxies = await Proxies.list("available")
    lines   = [f"[{p['id']}] `{esc(p['server'])}:{p['port']}`" for p in proxies[:15]]
    text    = "\n".join(lines) if lines else "_No available proxies_"
    await query.edit_message_text(
        f"📋 *Available Proxies* ({len(proxies)} total)\n\n{text}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="adm_proxies")]])
    )


@_admin_guard
async def cb_adm_prx_claimed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    proxies = await Proxies.list("claimed")
    lines   = [f"[{p['id']}] `{esc(p['server'])}:{p['port']}` → {p['claimed_by']}" for p in proxies[:15]]
    text    = "\n".join(lines) if lines else "_No claimed proxies_"
    await query.edit_message_text(
        f"📦 *Claimed Proxies* ({len(proxies)} total)\n\n{text}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="adm_proxies")]])
    )


@_admin_guard
async def cb_adm_prx_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["adm_state"] = "delete_proxy"
    await query.edit_message_text(
        "🗑 *Delete Proxy*\n\nSend the proxy ID to delete.\nUse 📋 Available to see IDs.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_proxies")]])
    )


@_admin_guard
async def cb_adm_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    refs  = await Referrals.list_all(10)
    lines = [f"• `{esc(r['referrer_id'])}` → `{esc(r['referee_id'])}`" for r in refs]
    text  = "\n".join(lines) if lines else "_No referrals_"
    await query.edit_message_text(
        f"🔗 *Recent Referrals*\n\n{text}",
        parse_mode="Markdown", reply_markup=BACK_BTN
    )


@_admin_guard
async def cb_adm_codes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    codes  = await Codes.list_all()
    active = [c for c in codes if c["is_active"] and c["used_count"] < c["max_uses"]]
    lines  = [f"• `{c['code']}` — {c['points']}pts — {c['used_count']}/{c['max_uses']}"
              for c in active[:8]]
    text = "\n".join(lines) if lines else "_No active codes_"
    await query.edit_message_text(
        f"🎫 *Reward Codes* ({len(active)} active)\n\n{text}",
        parse_mode="Markdown", reply_markup=codes_keyboard()
    )


@_admin_guard
async def cb_adm_codes_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    codes  = await Codes.list_all()
    active = [c for c in codes if c["is_active"] and c["used_count"] < c["max_uses"]]
    lines  = [f"• `{c['code']}` — {c['points']}pts — {c['used_count']}/{c['max_uses']} uses"
              for c in active[:20]]
    text = "\n".join(lines) if lines else "_No active codes_"
    await query.edit_message_text(
        f"📋 *Active Codes* ({len(active)})\n\n{text}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="adm_codes")]])
    )


@_admin_guard
async def cb_adm_code_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["adm_state"] = "create_code"
    await query.edit_message_text(
        "➕ *Create Code*\n\nSend in format:\n`CODE POINTS MAX_USES`\n\nExample:\n`PROMO100 100 50`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_codes")]])
    )


@_admin_guard
async def cb_adm_code_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["adm_state"] = "delete_code"
    await query.edit_message_text(
        "🗑 *Delete Code*\n\nSend the code name to delete.\nExample: `PROMO100`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_codes")]])
    )


@_admin_guard
async def cb_adm_gencode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⚡ Generating...")
    code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    pts  = 100
    uses = 10
    await Codes.create({
        "code":      code,
        "points":    pts,
        "maxUses":   uses,
        "createdBy": str(update.effective_user.id),
    })
    await AdminLogs.create(str(update.effective_user.id), "gencode", code, f"{pts}pts,{uses}uses")
    await query.edit_message_text(
        f"✅ *Code Generated!*\n\n"
        f"🎫 Code: `{code}`\n"
        f"💰 Points: *{pts}*\n"
        f"🔢 Max Uses: *{uses}*\n\n"
        f"Share with users: /redeem {code}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ Generate Another", callback_data="adm_gencode")],
            [InlineKeyboardButton("◀️ Back",             callback_data="adm_codes")],
        ])
    )


@_admin_guard
async def cb_adm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    logs  = await Broadcasts.list_all(5)
    lines = []
    for b in logs:
        date = b["created_at"].strftime("%m/%d/%Y") if b.get("created_at") else "—"
        lines.append(f"• {date} — Sent: {b['sent_count']} Failed: {b['fail_count']}")
    history = "\n".join(lines) if lines else "_No broadcasts yet_"
    await query.edit_message_text(
        f"📢 *Broadcast*\n\nUse: /broadcast \\<message\\>\n\n*Recent:*\n{history}",
        parse_mode="Markdown", reply_markup=BACK_BTN
    )


@_admin_guard
async def cb_adm_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query       = update.callback_query
    await query.answer()
    maintenance = await Settings.get_bool("maintenance", False)
    toggle      = "off" if maintenance else "on"
    btn_text    = "🔴 Turn Maintenance OFF" if maintenance else "🟢 Turn Maintenance ON"
    pc          = await get_proxy_cost()
    rr          = await get_referral_reward()
    custom_wlc  = bool(await Settings.get_string("welcome_message", ""))
    await query.edit_message_text(
        f"⚙️ *Settings*\n\n"
        f"🔧 Maintenance: {'🟢 ON' if maintenance else '🔴 OFF'}\n"
        f"💰 Proxy Cost: *{pc} points*\n"
        f"🎁 Referral Reward: *{rr} points*\n"
        f"✏️ Welcome Msg: {'Custom' if custom_wlc else 'Default'}\n\n"
        "_Commands:_ /setcost \\<n\\> /setreward \\<n\\> /setwelcome \\<text\\>",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(btn_text, callback_data=f"adm_maint_{toggle}")],
            [InlineKeyboardButton("✏️ Edit Welcome Message", callback_data="adm_setwelcome")],
            [InlineKeyboardButton("◀️ Back", callback_data="adm_dashboard")],
        ])
    )


@_admin_guard
async def cb_adm_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["adm_state"] = "set_welcome"
    await query.edit_message_text(
        "✏️ *Edit Welcome Message*\n\n"
        "Send your new welcome message.\n\n"
        "*Available placeholders:*\n"
        "`{name}` — user first name\n"
        "`{points}` — user points\n"
        "`{link}` — referral link\n"
        "`{cost}` — proxy cost\n"
        "`{reward}` — referral reward\n\n"
        "Send `DEFAULT` to reset to original.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_settings")]])
    )


@_admin_guard
async def cb_adm_maint_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await Settings.set("maintenance", "true")
    await AdminLogs.create(str(update.effective_user.id), "maintenance_on")
    await query.edit_message_text("🔧 Maintenance mode *enabled*.", parse_mode="Markdown", reply_markup=BACK_BTN)


@_admin_guard
async def cb_adm_maint_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await Settings.set("maintenance", "false")
    await AdminLogs.create(str(update.effective_user.id), "maintenance_off")
    await query.edit_message_text("✅ Maintenance mode *disabled*.", parse_mode="Markdown", reply_markup=BACK_BTN)


@_admin_guard
async def cb_adm_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    await query.answer()
    channels = await Channels.list_all()
    lines    = [f"• [{c['id']}] {esc(c['channel_title'] or c['channel_id'])} — {'🟢' if c['is_active'] else '🔴'}"
                for c in channels]
    text = "\n".join(lines) if lines else "_No channels configured_"
    await query.edit_message_text(
        f"📡 *Force\\-Subscribe Channels*\n\n{text}\n\n"
        "_Use:_ /addchannel @handle \\[Title\\]\n/removechannel \\<id\\>",
        parse_mode="Markdown", reply_markup=BACK_BTN
    )


@_admin_guard
async def cb_adm_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    d = await Analytics.get_dashboard()
    await query.edit_message_text(
        f"📈 *Analytics*\n\n"
        f"👥 Users: {d['totalUsers']} total | {d['activeUsers']} active | {d['bannedUsers']} banned\n"
        f"🔗 Referrals: {d['totalReferrals']} total | {d['dailyReferrals']} today\n"
        f"🌐 Proxy stock: {d['proxyStock']} | Claims: {d['totalClaims']}",
        parse_mode="Markdown", reply_markup=BACK_BTN
    )


@_admin_guard
async def cb_adm_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    logs  = await AdminLogs.list_all(20)
    lines = []
    for l in logs:
        action = str(l["action"] or "").replace("_", " ")
        target = f" → {l['target_id']}" if l.get("target_id") else ""
        date   = l["created_at"].strftime("%m/%d %H:%M") if l.get("created_at") else ""
        lines.append(f"• {action}{target}  {date}")
    text = "\n".join(lines) if lines else "No logs yet."
    # Send as plain text to avoid Markdown parse errors
    await query.edit_message_text(
        f"📋 Admin Logs (latest 20)\n\n{text}",
        reply_markup=BACK_BTN
    )


# Admin quick-action callbacks
@_safe
async def cb_adm_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(update.effective_user.id): await query.answer("⛔"); return
    await query.answer()
    uid = query.data[len("adm_ban_"):]
    await Users.ban(uid)
    await AdminLogs.create(str(update.effective_user.id), "ban_user", uid)
    await query.message.reply_text(f"🚫 User `{uid}` banned.", parse_mode="Markdown")


@_safe
async def cb_adm_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(update.effective_user.id): await query.answer("⛔"); return
    await query.answer()
    uid = query.data[len("adm_unban_"):]
    await Users.unban(uid)
    await AdminLogs.create(str(update.effective_user.id), "unban_user", uid)
    await query.message.reply_text(f"✅ User `{uid}` unbanned.", parse_mode="Markdown")


@_safe
async def cb_adm_add5(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(update.effective_user.id): await query.answer("⛔"); return
    await query.answer()
    uid  = query.data[len("adm_add5_"):]
    user = await Users.get(uid)
    if not user: await query.message.reply_text("User not found."); return
    await Users.add_points(uid, 5)
    await Transactions.create(uid, "admin_add", 5, "Admin quick-add", str(update.effective_user.id))
    await AdminLogs.create(str(update.effective_user.id), "add_points", uid, "5 pts")
    await query.message.reply_text(f"✅ Added *5* pts to `{uid}`. Balance: {user['points'] + 5}",
                                   parse_mode="Markdown")


@_safe
async def cb_adm_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(update.effective_user.id): await query.answer("⛔"); return
    await query.answer()
    uid = query.data[len("adm_del_"):]
    await Users.delete(uid)
    await AdminLogs.create(str(update.effective_user.id), "delete_user", uid)
    await query.message.reply_text(f"🗑 User `{uid}` deleted.", parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
#  TEXT / STATE HANDLER
# ═══════════════════════════════════════════════════════════════

@_safe
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if not is_admin(update.effective_user.id):
        return

    state = context.user_data.get("adm_state")

    # ── Bulk import via reply to importproxies message ─────────
    reply_to = update.message.reply_to_message
    if reply_to and reply_to.text and "Bulk Import" in (reply_to.text or ""):
        state = "bulk_import"

    if state == "bulk_import":
        context.user_data.pop("adm_state", None)
        lines  = [l.strip() for l in update.message.text.split("\n") if l.strip()]
        added = failed = 0
        for line in lines:
            parts = line.split(":")
            if len(parts) >= 4:
                try:
                    await Proxies.add({
                        "server": parts[0], "port": parts[1],
                        "username": parts[2], "password": ":".join(parts[3:]),
                        "addedBy": str(update.effective_user.id)
                    })
                    added += 1
                except Exception:
                    failed += 1
            else:
                failed += 1
        await AdminLogs.create(str(update.effective_user.id), "import_proxies", None, f"{added} added")
        await update.message.reply_text(
            f"✅ Import done: Added *{added}* | Failed: {failed}",
            parse_mode="Markdown"
        )
        return

    if state == "add_proxy":
        context.user_data.pop("adm_state", None)
        parts = update.message.text.strip().split(":")
        if len(parts) < 4:
            await update.message.reply_text("❌ Invalid format. Use: server:port:user:pass"); return
        try:
            proxy = await Proxies.add({
                "server": parts[0], "port": parts[1],
                "username": parts[2], "password": ":".join(parts[3:]),
                "addedBy": str(update.effective_user.id)
            })
            await AdminLogs.create(str(update.effective_user.id), "add_proxy", str(proxy["id"]))
            await update.message.reply_text(
                f"✅ Proxy added: `{esc(parts[0])}:{parts[1]}` (ID: {proxy['id']})",
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return

    if state == "delete_proxy":
        context.user_data.pop("adm_state", None)
        txt = update.message.text.strip()
        if not txt.isdigit():
            await update.message.reply_text("❌ Send a numeric proxy ID."); return
        pid = int(txt)
        await Proxies.delete(pid)
        await AdminLogs.create(str(update.effective_user.id), "delete_proxy", str(pid))
        await update.message.reply_text(f"🗑 Proxy #{pid} deleted.")
        return

    if state == "create_code":
        context.user_data.pop("adm_state", None)
        parts = update.message.text.strip().split()
        if len(parts) < 2 or not parts[1].isdigit():
            await update.message.reply_text("❌ Format: CODE POINTS [MAX_USES]"); return
        code     = parts[0].upper()
        pts      = int(parts[1])
        max_uses = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
        try:
            c = await Codes.create({
                "code": code, "points": pts,
                "maxUses": max_uses, "createdBy": str(update.effective_user.id)
            })
            await AdminLogs.create(str(update.effective_user.id), "create_code", c["code"])
            await update.message.reply_text(
                f"✅ Code `{c['code']}` — {pts} pts, max {max_uses} uses.",
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error (code may already exist): {e}")
        return

    if state == "delete_code":
        context.user_data.pop("adm_state", None)
        code = update.message.text.strip().upper()
        await Codes.delete_by_code(code)
        await AdminLogs.create(str(update.effective_user.id), "delete_code", code)
        await update.message.reply_text(f"🗑 Code `{esc(code)}` deleted.", parse_mode="Markdown")
        return

    if state == "set_welcome":
        context.user_data.pop("adm_state", None)
        text = update.message.text.strip()
        if text.upper() == "DEFAULT":
            await Settings.set("welcome_message", "")
            await update.message.reply_text("✅ Welcome message reset to default.")
        else:
            await Settings.set("welcome_message", text)
            await AdminLogs.create(str(update.effective_user.id), "set_welcome")
            await update.message.reply_text("✅ Welcome message updated!")
        return


# ═══════════════════════════════════════════════════════════════
#  GLOBAL ERROR HANDLER
# ═══════════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"[GLOBAL ERROR] {context.error}\n{traceback.format_exc()}")
    try:
        if isinstance(update, Update):
            if update.callback_query:
                await update.callback_query.answer("⚠️ Something went wrong. Please try again.")
            elif update.effective_message:
                await update.effective_message.reply_text("⚠️ An error occurred. Please try again.")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════

async def post_init(application: Application):
    print("[App] Connecting to database...")
    await init_schema()
    print("[Bot] Bot is running ✓")


def main():
    print("[Bot] Starting...")
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Global error handler
    application.add_error_handler(error_handler)

    # User commands
    application.add_handler(CommandHandler("start",  cmd_start))
    application.add_handler(CommandHandler("redeem", cmd_redeem))
    application.add_handler(CommandHandler("admin",  cmd_admin))

    # Admin text commands
    for pattern, handler in [
        (r"^/finduser ",       cmd_finduser),
        (r"^/ban ",            cmd_ban),
        (r"^/unban ",          cmd_unban),
        (r"^/addpoints ",      cmd_addpoints),
        (r"^/removepoints ",   cmd_removepoints),
        (r"^/resetpoints ",    cmd_resetpoints),
        (r"^/deleteuser ",     cmd_deleteuser),
        (r"^/addproxy ",       cmd_addproxy),
        (r"^/listproxies",     cmd_listproxies),
        (r"^/deleteproxy ",    cmd_deleteproxy),
        (r"^/addcode ",        cmd_addcode),
        (r"^/deletecode ",     cmd_deletecode),
        (r"^/setcost ",        cmd_setcost),
        (r"^/setreward ",      cmd_setreward),
        (r"^/setwelcome",      cmd_setwelcome),
        (r"^/broadcast ",      cmd_broadcast),
        (r"^/broadcastlogs",   cmd_broadcastlogs),
        (r"^/maintenance ",    cmd_maintenance),
        (r"^/setmessage ",     cmd_setmessage),
        (r"^/addchannel ",     cmd_addchannel),
        (r"^/removechannel ",  cmd_removechannel),
        (r"^/importproxies",   cmd_importproxies),
    ]:
        application.add_handler(MessageHandler(filters.Regex(pattern) & filters.TEXT, handler))

    # User callbacks
    for pattern, handler in [
        ("^check_sub$",      cb_check_sub),
        ("^menu_main$",      cb_menu_main),
        ("^menu_claim$",     cb_menu_claim),
        ("^do_claim$",       cb_do_claim),
        ("^menu_refer$",     cb_menu_refer),
        ("^menu_howto$",     cb_menu_howto),
        ("^menu_disclaimer$",cb_menu_disclaimer),
        ("^menu_profile$",   cb_menu_profile),
        ("^menu_referrals$", cb_menu_referrals),
        ("^menu_rewards$",   cb_menu_rewards),
        ("^menu_redeem$",    cb_menu_redeem),
    ]:
        application.add_handler(CallbackQueryHandler(handler, pattern=pattern))

    # Admin quick-action (MUST be before general adm_ patterns)
    application.add_handler(CallbackQueryHandler(cb_adm_ban,   pattern=r"^adm_ban_.+$"))
    application.add_handler(CallbackQueryHandler(cb_adm_unban, pattern=r"^adm_unban_.+$"))
    application.add_handler(CallbackQueryHandler(cb_adm_add5,  pattern=r"^adm_add5_.+$"))
    application.add_handler(CallbackQueryHandler(cb_adm_del,   pattern=r"^adm_del_.+$"))

    # Admin panel callbacks
    for pattern, handler in [
        ("^adm_dashboard$",   cb_adm_dashboard),
        ("^adm_users$",       cb_adm_users),
        ("^adm_proxies$",     cb_adm_proxies),
        ("^adm_prx_add$",     cb_adm_prx_add),
        ("^adm_prx_bulk$",    cb_adm_prx_bulk),
        ("^adm_prx_avail$",   cb_adm_prx_avail),
        ("^adm_prx_claimed$", cb_adm_prx_claimed),
        ("^adm_prx_del$",     cb_adm_prx_del),
        ("^adm_referrals$",   cb_adm_referrals),
        ("^adm_codes$",       cb_adm_codes),
        ("^adm_codes_list$",  cb_adm_codes_list),
        ("^adm_code_create$", cb_adm_code_create),
        ("^adm_code_del$",    cb_adm_code_del),
        ("^adm_gencode$",     cb_adm_gencode),
        ("^adm_broadcast$",   cb_adm_broadcast),
        ("^adm_settings$",    cb_adm_settings),
        ("^adm_setwelcome$",  cb_adm_setwelcome),
        ("^adm_maint_on$",    cb_adm_maint_on),
        ("^adm_maint_off$",   cb_adm_maint_off),
        ("^adm_channels$",    cb_adm_channels),
        ("^adm_analytics$",   cb_adm_analytics),
        ("^adm_logs$",        cb_adm_logs),
    ]:
        application.add_handler(CallbackQueryHandler(handler, pattern=pattern))

    # Text/state handler (must be last)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("[Bot] Starting polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
