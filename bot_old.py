"""
Enterprise OTP Bot v6 — Ultra-Optimized Edition.
Multi-API failover | SQLite WAL | Adaptive broadcast | OTP AI parser
Auto backup | Crash recovery | Analytics | Termux-max | Mobile-perfect
"""

import asyncio
import gc
import json
import os
import re
import shutil
import signal
import sqlite3
import sys
import time
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, User
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError, TimedOut, RetryAfter

# ═══════════════════════════════════════
#  COLORED TERMINAL LOGGING — reduced verbosity
# ═══════════════════════════════════════

class _C:
    G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"
    C = "\033[96m"; W = "\033[97m"; D = "\033[90m"
    B = "\033[1m";  E = "\033[0m"

class _Fmt(logging.Formatter):
    _M = {
        logging.INFO:    f"{_C.G}[✓]{_C.E}",
        logging.WARNING: f"{_C.Y}[!]{_C.E}",
        logging.ERROR:   f"{_C.R}[✗]{_C.E}",
        logging.DEBUG:   f"{_C.D}[-]{_C.E}",
    }
    def format(self, r):
        return f"{self._M.get(r.levelno, f'{_C.W}[?]{_C.E}')} {r.getMessage()}"

_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(_Fmt())
log = logging.getLogger("otp")
log.setLevel(logging.INFO)
log.addHandler(_h)
log.propagate = False

# ═══════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════

BOT_TOKEN   = os.getenv("BOT_TOKEN",   "8601304803:AAGwMcZ15a5O-jm2fDAvxRmQrLSYoes0jmo")
API_KEY     = os.getenv("API_KEY",     "nxa_e80d2108272d419495acd90e3972eab0b582a5a1")
ADMIN_ID    = int(os.getenv("ADMIN_ID", "6668016879"))
GROUP_ID    = os.getenv("GROUP_ID",    "-1003727266573")
GROUP_LINK  = os.getenv("GROUP_LINK",  "https://t.me/seven_otp")
BOT_UNAME   = os.getenv("BOT_USERNAME","number_xsayem_bot")
BASE_URL    = os.getenv("BASE_URL",    "http://63.141.255.227")
CHANNELS    = ["@fastaccountapk_official", "@fast_account_updates"]

CONFIG_FILE  = "config.json"
USERS_FILE   = "users.json"
DATA_FILE    = "bot_data.json"
PRESETS_FILE = "presets.json"
DB_FILE      = "bot.db"
BACKUP_DIR   = "backups"

HEADERS: Dict[str, str] = {"X-API-Key": API_KEY}

API_ENDPOINTS = [os.getenv("BASE_URL", "http://63.141.255.227")]

# ── Performance tuning ──
POLL_FAST           = 0.5
POLL_SLOW           = 2.0
POLL_IDLE           = 3.0
POLL_TIMEOUT        = 600
REQUEST_TIMEOUT     = 4
FETCH_TIMEOUT       = 8
MAX_RETRIES         = 2
BROADCAST_WORKERS   = 3
BROADCAST_DELAY     = 0.05
SAVE_INTERVAL       = 45
FLOOD_COOLDOWN      = 0.8
MAX_CONCURRENT      = 50
API_SEM_LIMIT       = 8
CLEANUP_INTERVAL    = 120
SUB_CACHE_TTL       = 900
BACKUP_INTERVAL     = 3600
DEDUP_TTL           = 300
MAX_ACTIVITY        = 20
DB_COMMIT_INTERVAL  = 20
RECONNECT_COOLDOWN  = 10
GC_INTERVAL         = 120
TASK_WATCHDOG_INT   = 60

REQUIRED_CHATS = CHANNELS + [GROUP_ID]

# Default presets
_DEFAULT_PRESETS: Dict[str, Dict] = {
    "TJ": {"name": "Tajikistan",  "code": "+992", "range": "99298XXX",  "flag": "\U0001f1f9\U0001f1ef"},
    "TG": {"name": "Togo",        "code": "+22", "range": "2289XXXXX", "flag": "\U0001f1f9\U0001f1ec"},
    "CM": {"name": "Cameroon",    "code": "+237", "range": "2376XXXXX", "flag": "\U0001f1e8\U0001f1f2"},
    "BD": {"name": "Bangladesh",  "code": "+880", "range": "88017XXXXX","flag": "\U0001f1e7\U0001f1e9"},
}

PRESETS: Dict[str, Dict] = {}
_active_preset: str = ""

# OTP patterns
OTP_PATTERNS = [
    re.compile(r'\b(\d{4,8})\b'),
    re.compile(r'(?:OTP|code|pin|verify|verification)\s*[:=\-]?\s*(\d{4,8})', re.I),
    re.compile(r'(\d{4,8})\s*(?:is your|is the|your)', re.I),
    re.compile(r'(?:use|enter|input)\s+(\d{4,8})', re.I),
]

# ═══════════════════════════════════════
#  GLOBAL STATE — compact
# ═══════════════════════════════════════

_session: Optional[aiohttp.ClientSession] = None
_api_sem: Optional[asyncio.Semaphore] = None
_db: Optional[sqlite3.Connection] = None
_bg_tasks: set = set()
_activity: deque = deque(maxlen=MAX_ACTIVITY)
_orders: Dict[str, Dict] = {}
_user_orders: Dict[int, str] = {}
_admin_state: Dict[int, str] = {}
_flood_cache: Dict[int, float] = {}

_broadcast_cancel: set = set()
_preset_temp: Dict[int, Dict] = {}

_api_status: str = "unknown"
_api_latencies: deque = deque(maxlen=15)
_api_consecutive_errs: int = 0
_api_index: int = 0
_api_healthy: Dict[int, bool] = {}
_api_last_reconnect: float = 0.0

_msg_dedup: Dict[int, float] = {}
_sub_cache: Dict[int, Tuple[float, bool]] = {}
_db_write_buf: List[Tuple] = []
_db_dirty: bool = False

config: Dict = {}
users_db: List[int] = []
users_set: set = set()
_users_dirty: bool = False
bot_data: Dict = {}
_data_dirty: bool = False
_otp_seen: Dict[str, set] = {}
_boot_time: float = 0.0
_warmup_done: bool = False

# ═══════════════════════════════════════
#  SQLITE — WAL + batched writes
# ═══════════════════════════════════════

def _db_init():
    global _db
    _db = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
    _db.execute("PRAGMA journal_mode=WAL")
    _db.execute("PRAGMA synchronous=NORMAL")
    _db.execute("PRAGMA cache_size=-512")        # 512KB cache
    _db.execute("PRAGMA temp_store=MEMORY")
    _db.execute("PRAGMA mmap_size=33554432")     # 32MB mmap
    _db.execute("PRAGMA busy_timeout=3000")
    _db.execute("PRAGMA optimize")
    _db.execute("""CREATE TABLE IF NOT EXISTS users (
        uid INTEGER PRIMARY KEY, first_seen TEXT, last_seen TEXT,
        otp_count INTEGER DEFAULT 0, name TEXT, username TEXT
    )""")
    _db.execute("""CREATE TABLE IF NOT EXISTS stats (
        key TEXT PRIMARY KEY, value TEXT
    )""")
    _db.execute("""CREATE TABLE IF NOT EXISTS activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, uid INTEGER, action TEXT, detail TEXT
    )""")
    _db.execute("CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen)")
    _db.execute("CREATE INDEX IF NOT EXISTS idx_activity_ts ON activity(ts)")
    _db.commit()

def _db_get_stat(key: str, default: str = "0") -> str:
    row = _db.execute("SELECT value FROM stats WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def _db_set_stat(key: str, value: str):
    global _db_dirty
    _db.execute("INSERT OR REPLACE INTO stats(key,value) VALUES(?,?)", (key, value))
    _db_dirty = True

def _db_inc_stat(key: str, amount: int = 1):
    cur = int(_db_get_stat(key, "0"))
    _db_set_stat(key, str(cur + amount))

def _db_record_user(uid: int, name: str, username: str):
    global _db_dirty
    now = datetime.now(timezone.utc).isoformat()
    _db.execute("""INSERT INTO users(uid,first_seen,last_seen,otp_count,name,username)
        VALUES(?,?,?,0,?,?)
        ON CONFLICT(uid) DO UPDATE SET last_seen=?,name=?,username=?""",
        (uid, now, now, name, username, now, name, username))
    _db_dirty = True

def _db_inc_otp(uid: int):
    global _db_dirty
    _db.execute("UPDATE users SET otp_count=otp_count+1 WHERE uid=?", (uid,))
    _db_dirty = True
    _db_inc_stat("total_otps")
    _db_inc_stat("today_otps")

def _db_log_activity(uid: int, action: str, detail: str = ""):
    global _db_dirty
    _db.execute("INSERT INTO activity(ts,uid,action,detail) VALUES(?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(), uid, action, detail))
    _db_dirty = True

def _db_top_users(limit: int = 5) -> List[Tuple]:
    return _db.execute(
        "SELECT username, otp_count FROM users ORDER BY otp_count DESC LIMIT ?",
        (limit,)
    ).fetchall()

def _db_user_count() -> int:
    return _db.execute("SELECT COUNT(*) FROM users").fetchone()[0]

def _db_active_today() -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    return _db.execute(
        "SELECT COUNT(*) FROM users WHERE last_seen LIKE ?", (f"{today}%",)
    ).fetchone()[0]

def _db_all_users(page: int = 0, per_page: int = 20) -> Tuple[List[Tuple], int]:
    total = _db_user_count()
    rows = _db.execute(
        "SELECT name, username, uid FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?",
        (per_page, page * per_page)
    ).fetchall()
    return rows, total

# ═══════════════════════════════════════
#  JSON I/O — atomic, compact
# ═══════════════════════════════════════

def _load(p: str, d):
    if not os.path.exists(p):
        return d
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return d

def _save(p: str, data) -> bool:
    t = p + ".tmp"
    try:
        with open(t, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(t, p)
        return True
    except Exception:
        try: os.remove(t)
        except OSError: pass
        return False

# ── Preset I/O ──
def _load_presets():
    global PRESETS, _active_preset
    data = _load(PRESETS_FILE, None)
    if data is None:
        PRESETS = dict(_DEFAULT_PRESETS)
        _save_presets()
    else:
        PRESETS = data.get("presets", {})
        _active_preset = data.get("active", "")
    if _active_preset and _active_preset in PRESETS:
        pre = PRESETS[_active_preset]
        config.update({"country": pre["name"], "code": pre["code"], "range": pre["range"]})

def _save_presets():
    _save(PRESETS_FILE, {"presets": PRESETS, "active": _active_preset})

def _preset_set_active(key: str):
    global _active_preset
    if key not in PRESETS:
        return False
    _active_preset = key
    pre = PRESETS[key]
    config.update({"country": pre["name"], "code": pre["code"], "range": pre["range"]})
    _save(CONFIG_FILE, config)
    _save_presets()
    return True

def _preset_add(key: str, name: str, code: str, range_: str, flag: str = "\U0001f30d"):
    flag = _validate_flag(flag)
    PRESETS[key] = {"name": name, "code": code, "range": range_, "flag": flag}
    _save_presets()

def _preset_delete(key: str):
    global _active_preset
    PRESETS.pop(key, None)
    if _active_preset == key:
        _active_preset = ""
    _save_presets()

def _preset_edit(key: str, name: str = None, code: str = None, range_: str = None, flag: str = None):
    if key not in PRESETS:
        return False
    if name is not None:
        PRESETS[key]["name"] = name
    if code is not None:
        PRESETS[key]["code"] = code
    if range_ is not None:
        PRESETS[key]["range"] = range_
    if flag is not None:
        PRESETS[key]["flag"] = _validate_flag(flag)
    _save_presets()
    if _active_preset == key:
        pre = PRESETS[key]
        config.update({"country": pre["name"], "code": pre["code"], "range": pre["range"]})
        _save(CONFIG_FILE, config)
    return True

def _mu():
    global _users_dirty
    _users_dirty = True

def _md():
    global _data_dirty
    _data_dirty = True

# ═══════════════════════════════════════
#  INIT — deferred, non-blocking
# ═══════════════════════════════════════

def _norm_stats():
    if not bot_data:
        return
    today = datetime.now(timezone.utc).date().isoformat()
    s = bot_data.setdefault("stats", {})
    if s.get("today_date") != today:
        s["today_date"] = today
        s["today_numbers"] = 0
        s["today_otps"] = 0
    if _db:
        _db_set_stat("today_date", today)

def _init():
    global config, users_db, users_set, bot_data
    config = _load(CONFIG_FILE, {"range": "99298XXX", "country": "Tajikistan", "code": "+992"})
    users_db = _load(USERS_FILE, [])
    users_set = set(users_db)
    bot_data = _load(DATA_FILE, {
        "users": {}, "banned": [],
        "stats": {
            "total_numbers": 0, "total_otps": 0, "requests": 0,
            "today_numbers": 0, "today_otps": 0,
            "today_date": datetime.now(timezone.utc).date().isoformat(),
        },
        "range": config.get("range", "99298XXX"),
        "country": config.get("country", "Tajikistan"),
    })
    _norm_stats()
    _load_presets()

_init()
_db_init()

# ═══════════════════════════════════════
#  HELPERS — optimized
# ═══════════════════════════════════════

def _esc(t: str) -> str:
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _validate_flag(text: str) -> str:
    if not text:
        return "\U0001f30d"
    text = text.strip()
    ri = [c for c in text if "\U0001f1e6" <= c <= "\U0001f1ff"]
    if len(ri) == 2:
        return "".join(ri)
    return "\U0001f30d"

def _fmt_phone(api_num: str, code: str) -> str:
    c = str(api_num).lstrip("+")
    cc = code.lstrip("+")
    return f"+{c}" if c.startswith(cc) else f"+{cc}{c}"

def _hide(p: str) -> str:
    return f"{p[:5]}***{p[-2:]}" if len(p) > 7 else p

def _reg(uid: int) -> bool:
    if uid not in users_set:
        users_set.add(uid)
        users_db.append(uid)
        _mu()
        return True
    return False

def _banned(uid: int) -> bool:
    bl = bot_data.get("banned", [])
    return str(uid) in bl if isinstance(bl, list) else False

def _ban(uid: int) -> bool:
    b = bot_data.setdefault("banned", [])
    s = str(uid)
    if s not in b:
        b.append(s); _md(); _save(DATA_FILE, bot_data)
        return True
    return False

def _unban(uid: int) -> bool:
    b = bot_data.setdefault("banned", [])
    s = str(uid)
    if s in b:
        b.remove(s); _md(); _save(DATA_FILE, bot_data)
        return True
    return False

def _user_info(u: User) -> Tuple[str, str]:
    un = f"@{u.username}" if u.username else "N/A"
    nm = (u.first_name or "User") + (f" {u.last_name}" if u.last_name else "")
    return _esc(nm), un

def _record(u: User) -> Dict:
    uid = str(u.id)
    users = bot_data.setdefault("users", {})
    nm, un = _user_info(u)
    r = users.setdefault(uid, {"name": nm, "username": un, "otp_count": 0, "last_seen": ""})
    r["name"] = nm; r["username"] = un
    r["last_seen"] = datetime.now(timezone.utc).isoformat()
    _db_record_user(u.id, nm, un)
    return r

def _track(u: User, action: str):
    _record(u)
    s = bot_data.setdefault("stats", {})
    if action == "request":
        s["requests"] = s.get("requests", 0) + 1
        _db_inc_stat("total_requests")
        _db_log_activity(u.id, "request")
    elif action == "number":
        s["total_numbers"] = s.get("total_numbers", 0) + 1
        s["today_numbers"] = s.get("today_numbers", 0) + 1
        _db_inc_stat("total_numbers")
        _db_inc_stat("today_numbers")
        _db_log_activity(u.id, "number")
    elif action == "otp":
        r = bot_data.get("users", {}).get(str(u.id), {})
        r["otp_count"] = int(r.get("otp_count", 0)) + 1
        s["total_otps"] = s.get("total_otps", 0) + 1
        s["today_otps"] = s.get("today_otps", 0) + 1
        _db_inc_otp(u.id)
        _db_log_activity(u.id, "otp")
    _md()

def _is_flood(uid: int) -> bool:
    now = time.monotonic()
    last = _flood_cache.get(uid, 0)
    if now - last < FLOOD_COOLDOWN:
        return True
    _flood_cache[uid] = now
    return False

def _avg_latency() -> float:
    if not _api_latencies:
        return 0.0
    return round(sum(_api_latencies) / len(_api_latencies), 3)

def _p95_latency() -> float:
    if not _api_latencies:
        return 0.0
    s = sorted(_api_latencies)
    idx = int(len(s) * 0.95)
    return round(s[min(idx, len(s)-1)], 3)

# ═══════════════════════════════════════
#  OTP AI PARSER
# ═══════════════════════════════════════

def _extract_otp(data: Dict) -> Optional[str]:
    otp = data.get("otp")
    if otp and str(otp).strip():
        return str(otp).strip()
    msg = data.get("message", "")
    if not msg:
        return None
    for pattern in OTP_PATTERNS:
        m = pattern.search(msg)
        if m:
            return m.group(1)
    return None

# ═══════════════════════════════════════
#  SUBSCRIPTION — cached, non-blocking start
# ═══════════════════════════════════════

async def _is_sub(bot, uid: int) -> bool:
    now = time.monotonic()
    cached = _sub_cache.get(uid)
    if cached and now - cached[0] < SUB_CACHE_TTL:
        return cached[1]
    result = True
    for chat in REQUIRED_CHATS:
        try:
            m = await bot.get_chat_member(chat_id=chat, user_id=uid)
            if m.status in ("left", "kicked"):
                result = False; break
        except (BadRequest, TelegramError):
            result = False; break
    _sub_cache[uid] = (now, result)
    return result

# ═══════════════════════════════════════
#  SESSION MANAGEMENT — cooldown reconnect
# ═══════════════════════════════════════

async def _ensure_session() -> bool:
    global _session, _api_consecutive_errs
    if _session and not _session.closed:
        return True
    try:
        connector = aiohttp.TCPConnector(
            limit=8, limit_per_host=4,
            enable_cleanup_closed=True, ttl_dns_cache=300,
            keepalive_timeout=20,
        )
        _session = aiohttp.ClientSession(connector=connector)
        _api_consecutive_errs = 0
        log.info(f"{_C.C}[API]{_C.E} Session created")
        return True
    except Exception as e:
        log.error(f"[ERR] Session: {e}")
        return False

async def _check_reconnect():
    global _api_consecutive_errs, _api_last_reconnect
    now = time.monotonic()
    if now - _api_last_reconnect < RECONNECT_COOLDOWN:
        return
    if _api_consecutive_errs >= 5:
        _api_last_reconnect = now
        log.warning(f"{_C.Y}[!]{_C.E} {_api_consecutive_errs}x errors — reconnecting")
        try:
            if _session and not _session.closed:
                await _session.close()
                _session = None
            await _ensure_session()
        except Exception as e:
            log.error(f"[ERR] reconnect: {e}")

# ═══════════════════════════════════════
#  MULTI-API ENGINE — failover + health
# ═══════════════════════════════════════

def _get_api_url() -> str:
    return API_ENDPOINTS[_api_index % len(API_ENDPOINTS)]

def _switch_api():
    global _api_index
    if len(API_ENDPOINTS) <= 1:
        return
    _api_healthy[_api_index % len(API_ENDPOINTS)] = False
    for i in range(len(API_ENDPOINTS)):
        idx = (_api_index + 1 + i) % len(API_ENDPOINTS)
        if _api_healthy.get(idx, True):
            _api_index = idx
            log.info(f"{_C.C}[API]{_C.E} Switched to endpoint {idx}")
            return
    _api_index = (_api_index + 1) % len(API_ENDPOINTS)
    log.warning(f"{_C.Y}[!]{_C.E} All endpoints unhealthy, cycling")

async def _api(method: str, ep: str, payload: Optional[Dict] = None, timeout: int = REQUEST_TIMEOUT) -> Optional[Dict]:
    global _api_status, _api_consecutive_errs
    if not await _ensure_session():
        _api_status = "offline"
        return None

    base = _get_api_url()
    url = f"{base}{ep}"
    ct = aiohttp.ClientTimeout(total=timeout)

    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            async with _api_sem:
                async with _session.request(method, url, json=payload, headers=HEADERS, timeout=ct) as r:
                    lat = round(time.monotonic() - t0, 3)
                    _api_latencies.append(lat)
                    if r.status != 200:
                        _api_status = f"err:{r.status}"
                        _api_consecutive_errs += 1
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(0.3)
                            continue
                        if _api_consecutive_errs >= 3:
                            _switch_api()
                        return None
                    _api_status = "ok"
                    _api_consecutive_errs = 0
                    _api_healthy[_api_index % len(API_ENDPOINTS)] = True
                    return await r.json(content_type=None)
        except asyncio.TimeoutError:
            _api_status = "timeout"
            _api_consecutive_errs += 1
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.3)
        except aiohttp.ClientError:
            _api_status = "conn_err"
            _api_consecutive_errs += 1
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.5)
        except Exception:
            _api_status = "error"
            _api_consecutive_errs += 1
            return None

    if _api_consecutive_errs >= 3:
        _switch_api()
    return None

async def _fetch_number(timeout: int = FETCH_TIMEOUT) -> Optional[Tuple[str, str, int]]:
    payload = {"range": config["range"], "format": "national"}
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        d = await _api("POST", "/api/v1/numbers/get", payload)
        if d and d.get("number_id"):
            nid = str(d["number_id"])
            num = str(d.get("number", ""))
            try:
                mins = int(float(d.get("expires_in", 1200))) // 60
            except (ValueError, TypeError):
                mins = 20
            log.info(f"{_C.C}[API]{_C.E} Number {nid} ({mins}m)")
            return num, nid, mins
        await asyncio.sleep(0.8)
    return None

async def _fetch_sms(nid: str) -> Optional[Dict]:
    return await _api("GET", f"/api/v1/numbers/{nid}/sms", timeout=4)

# ═══════════════════════════════════════
#  BORDER SMS UI — premium messages
# ═══════════════════════════════════════

def _get_flag() -> str:
    if _active_preset and _active_preset in PRESETS:
        return _validate_flag(PRESETS[_active_preset].get("flag", "\U0001f30d"))
    return "\U0001f30d"

def _msg_number(phone: str) -> str:
    flag = _get_flag()
    return (
        f"{'':>3}------\U0001f7e2New Number\U0001f7e2------\n"
        f"\u260e\ufe0f Number | <code>{_esc(phone)}</code> | {flag} {config['country']}\n\n"
        f"\u23f3 Waiting For OTP..."
    )

def _msg_otp(phone: str, otp: str) -> str:
    flag = _get_flag()
    return (
        f"{'':>3}------\u2705Received OTP\u2705------\n"
        f"\u260e\ufe0f Number | <code>{_esc(phone)}</code> | {flag} {config['country']}\n"
        f"\U0001f511 OTP | <code>{_esc(otp)}</code>"
    )

def _msg_searching() -> str:
    return (
        f"{'':>3}------\U0001f50dSearching\U0001f50d------\n"
        f"\U0001f4e1 Finding Available Number..."
    )

def _msg_no_number() -> str:
    return (
        f"{'':>3}------\u274cUnavailable\u274c------\n"
        f"No Numbers Available.\n\n"
        f"Try Again Later."
    )

def _msg_timeout(phone: str) -> str:
    return (
        f"{'':>3}------\u274cTimeout\u274c------\n"
        f"\u260e\ufe0f Number | <code>{_esc(phone)}</code>\n\n"
        f"Retry Again..."
    )

def _msg_banned() -> str:
    return (
        f"{'':>3}------\U0001f6ab Banned\U0001f6ab ------\n"
        f"Access Denied."
    )

def _msg_join() -> str:
    return (
        f"{'':>3}------\u26a0\ufe0f Join Required\u26a0\ufe0f------\n"
        f"Join All Channels First."
    )

def _msg_welcome(name: str) -> str:
    return (
        f"{'':>3}------\u26a1Welcome\u26a1------\n"
        f"<b>{_esc(name)}</b>\n\n"
        f"Tap Below To Get Number."
    )

def _msg_verified() -> str:
    return (
        f"{'':>3}------\u2705Verified\u2705------\n"
        f"Send /start To Begin."
    )

def _msg_no_active() -> str:
    return (
        f"{'':>3}------\u2139\ufe0f No Active\u2139\ufe0f------\n"
        f"No Active Request."
    )

def _msg_active(oid: str) -> str:
    return (
        f"{'':>3}------\u2705 Active \u2705------\n"
        f"Order: <code>{_esc(oid)}</code>"
    )

def _msg_busy() -> str:
    return (
        f"{'':>3}------\u26a0\ufe0f Busy \u26a0\ufe0f------\n"
        f"Server Busy. Try Again."
    )

# ── Admin messages ──

def _msg_admin_num(uid: int, name: str, uname: str, phone: str) -> str:
    return (
        f"{'':>3}------\U0001f7e2 New Number \U0001f7e2------\n"
        f"{name} | {uname}\n"
        f"\u260e\ufe0f <code>{_esc(phone)}</code>"
    )

def _msg_admin_otp(uid: int, name: str, uname: str, phone: str, otp: str) -> str:
    return (
        f"{'':>3}------\U0001f534 OTP Captured \U0001f534------\n"
        f"{name} | {uname}\n"
        f"\u260e\ufe0f <code>{_esc(phone)}</code>\n"
        f"\U0001f511 <code>{_esc(otp)}</code>"
    )

def _msg_group_otp(hidden: str, otp: str, api_msg: str) -> str:
    return (
        f"{'':>3}------\U0001f534 OTP Received \U0001f534------\n"
        f"\u260e\ufe0f <code>{_esc(hidden)}</code>\n"
        f"\U0001f511 <code>{_esc(otp)}</code>\n\n"
        f"\U0001f4ac <code>{_esc(api_msg)}</code>"
    )

def _msg_dashboard() -> str:
    s = bot_data.get("stats", {})
    top = _fmt_top()
    uptime = int(time.monotonic() - _boot_time)
    h, m = divmod(uptime // 60, 60)
    active = len(_orders)
    otp_t = int(_db_get_stat("total_otps", "0")) or s.get("total_otps", 0)
    num_t = int(_db_get_stat("total_numbers", "0")) or s.get("total_numbers", 0)
    rate = f"{(otp_t / num_t * 100):.0f}%" if num_t > 0 else "0%"
    avg_lat = _avg_latency()
    p95_lat = _p95_latency()
    errs = _api_consecutive_errs
    poll_spd = f"{POLL_FAST}s" if active > 0 else f"{POLL_IDLE}s"
    db_users = _db_user_count()
    active_today = _db_active_today()
    api_ep = _api_index % len(API_ENDPOINTS)
    healthy_count = sum(1 for v in _api_healthy.values() if v)
    mem_kb = _get_mem_usage()

    return (
        f"{'':>3}------\u2699\ufe0f Admin Panel \u2699\ufe0f------\n\n"
        f"\U0001f465 Users    | <code>{db_users}</code>\n"
        f"\U0001f4f2 Numbers  | <code>{num_t}</code>\n"
        f"\U0001f511 OTPs     | <code>{otp_t}</code>\n"
        f"\U0001f4ca Requests | <code>{s.get('requests', 0)}</code>\n"
        f"\U0001f4c8 Success  | <code>{rate}</code>\n"
        f"\U0001f504 Active   | <code>{active}</code>\n\n"
        f"\u23f0 <b>Today</b>\n"
        f"  Numbers  | <code>{s.get('today_numbers', 0)}</code>\n"
        f"  OTPs     | <code>{s.get('today_otps', 0)}</code>\n"
        f"  Active   | <code>{active_today}</code>\n\n"
        f"\U0001f4e1 <b>API</b>\n"
        f"  Status   | <code>{_api_status}</code>\n"
        f"  Endpoint | <code>#{api_ep}</code>\n"
        f"  Avg      | <code>{avg_lat}s</code>\n"
        f"  P95      | <code>{p95_lat}s</code>\n"
        f"  Errors   | <code>{errs}</code>\n"
        f"  Healthy  | <code>{healthy_count}/{len(API_ENDPOINTS)}</code>\n\n"
        f"\u2699\ufe0f <b>System</b>\n"
        f"  Poll     | <code>{poll_spd}</code>\n"
        f"  Memory   | <code>{mem_kb}MB</code>\n"
        f"  Tasks    | <code>{len(_bg_tasks)}</code>\n"
        f"  Uptime   | <code>{h}h {m}m</code>\n\n"
        f"\U0001f30d <b>{config['country']}</b>\n"
        f"  Code     | <code>{config['code']}</code>\n"
        f"  Range    | <code>{config['range']}</code>\n\n"
        f"\U0001f3c6 <b>Top Users</b>\n{top}"
    )

def _get_mem_usage() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 0

def _fmt_top() -> str:
    rows = _db_top_users(5)
    if not rows:
        return "  No records yet."
    return "\n".join(
        f"  {i}. {r[0] or 'N/A'} \u2014 {r[1]}"
        for i, r in enumerate(rows, 1)
    )

def _fmt_user_list(page: int = 0) -> Tuple[str, int]:
    per_page = 20
    rows, total = _db_all_users(page, per_page)
    pages = max(1, (total + per_page - 1) // per_page)
    if not rows:
        return f"{'':>3}------\U0001f465 All Users\U0001f465------\n\nNo users yet.", pages
    lines = [f"{_esc(r[0] or 'N/A')}|{_esc(r[1]) if r[1] else 'N/A'}|{r[2]}" for r in rows]
    return (
        f"{'':>3}------\U0001f465 All Users\U0001f465------\n"
        f"Total: {total} | Page {page + 1}/{pages}\n\n"
        + "\n".join(lines)
    ), pages

def _kb_user_list(page: int, total_pages: int):
    btns = []
    if total_pages > 1:
        row = []
        if page > 0:
            row.append(InlineKeyboardButton("\u25c0 Prev", callback_data=f"au_{page - 1}"))
        if page < total_pages - 1:
            row.append(InlineKeyboardButton("Next \u25b6", callback_data=f"au_{page + 1}"))
        if row:
            btns.append(row)
    btns.append([InlineKeyboardButton("\u21a9 Back", callback_data="ar")])
    return InlineKeyboardMarkup(btns)

# ═══════════════════════════════════════
#  KEYBOARDS — compact, merged duplicates
# ═══════════════════════════════════════

_KB_GET_OTP_ROW = [InlineKeyboardButton("\U0001f4f2 Get Number", callback_data="get"),
                    InlineKeyboardButton("\U0001f534 OTP Group", url=GROUP_LINK)]

def _kb_start():
    return InlineKeyboardMarkup([_KB_GET_OTP_ROW])

def _kb_join():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Channel 1", url="https://t.me/fastaccountapk_official")],
        [InlineKeyboardButton("Channel 2", url="https://t.me/fast_account_updates")],
        [InlineKeyboardButton("Group", url=GROUP_LINK)],
        [InlineKeyboardButton("\u2705 I Joined", callback_data="check_join")],
    ])

def _kb_waiting():
    return InlineKeyboardMarkup([_KB_GET_OTP_ROW])

def _kb_otp():
    return InlineKeyboardMarkup([_KB_GET_OTP_ROW])

def _kb_group():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Channel", url="https://t.me/fast_account_updates"),
         InlineKeyboardButton("OTP Bot", url=f"https://t.me/{BOT_UNAME}")]
    ])

def _kb_admin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Refresh", callback_data="ar"),
         InlineKeyboardButton("\u2699\ufe0f Preset", callback_data="preset_mgr")],
        [InlineKeyboardButton("\U0001f6ab Ban", callback_data="ab"),
         InlineKeyboardButton("\u2705 Unban", callback_data="aub")],
        [InlineKeyboardButton("\U0001f4dc Logs", callback_data="al"),
         InlineKeyboardButton("\U0001f4e2 Broadcast", callback_data="broadcast_panel")],
        [InlineKeyboardButton("\U0001f4c8 Analytics", callback_data="an"),
         InlineKeyboardButton("\U0001f465 All Users", callback_data="au")],
    ])

def _kb_presets():
    btns, row = [], []
    for k, v in PRESETS.items():
        active = " \u2705" if k == _active_preset else ""
        row.append(InlineKeyboardButton(f"{v['flag']} {v['name']}{active}", callback_data=f"sp_{k}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    btns.append([InlineKeyboardButton("\u2699\ufe0f Manager", callback_data="preset_mgr")])
    btns.append([InlineKeyboardButton("\u21a9 Back", callback_data="ar")])
    return InlineKeyboardMarkup(btns)

def _kb_preset_manager():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\u2795 Add Preset", callback_data="pm_add"),
         InlineKeyboardButton("\u270f\ufe0f Edit Preset", callback_data="pm_edit")],
        [InlineKeyboardButton("\U0001f5d1\ufe0f Delete Preset", callback_data="pm_del"),
         InlineKeyboardButton("\U0001f3af Active Preset", callback_data="pm_active")],
        [InlineKeyboardButton("\u270f\ufe0f Change Range", callback_data="pm_chrange"),
         InlineKeyboardButton("\U0001f522 Custom Range", callback_data="pm_range")],
        [InlineKeyboardButton("\u21a9 Back", callback_data="ap")],
    ])

def _kb_preset_list(cb_prefix: str):
    btns, row = [], []
    for k, v in PRESETS.items():
        row.append(InlineKeyboardButton(f"{v['flag']} {v['name']}", callback_data=f"{cb_prefix}_{k}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    btns.append([InlineKeyboardButton("\u21a9 Back", callback_data="preset_mgr")])
    return InlineKeyboardMarkup(btns)

def _kb_preset_active():
    btns, row = [], []
    for k, v in PRESETS.items():
        mark = " \u2705" if k == _active_preset else ""
        row.append(InlineKeyboardButton(f"{v['flag']} {v['name']}{mark}", callback_data=f"pa_{k}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    btns.append([InlineKeyboardButton("\u21a9 Back", callback_data="preset_mgr")])
    return InlineKeyboardMarkup(btns)

def _kb_preset_edit_fields(key: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f30d Name", callback_data=f"pe_name_{key}"),
         InlineKeyboardButton("\U0001f4de Code", callback_data=f"pe_code_{key}")],
        [InlineKeyboardButton("\U0001f522 Range", callback_data=f"pe_range_{key}"),
         InlineKeyboardButton("\U0001f3a8 Flag", callback_data=f"pe_flag_{key}")],
        [InlineKeyboardButton("\u21a9 Back", callback_data="pm_edit")],
    ])

def _kb_back(cb="ar"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("\u21a9 Back", callback_data=cb)]])

def _kb_cancel():
    return InlineKeyboardMarkup([[InlineKeyboardButton("\u274c Cancel", callback_data="cx")]])

def _kb_broadcast_cancel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\u274c Cancel Broadcast", callback_data="bc_cancel")],
    ])

# ═══════════════════════════════════════
#  ORDER MANAGEMENT
# ═══════════════════════════════════════

async def _cancel_order(ctx: ContextTypes.DEFAULT_TYPE, uid: int) -> bool:
    nid = _user_orders.pop(uid, None)
    if not nid:
        return False
    order = _orders.pop(nid, None)
    if not order:
        return False
    t = order.get("task")
    if t and not t.done():
        t.cancel()
    cid, mid = order.get("chat_id"), order.get("msg_id")
    if cid and mid:
        try: await ctx.bot.delete_message(cid, mid)
        except Exception: pass
    _otp_seen.pop(nid, None)
    return True

# ═══════════════════════════════════════
#  TELEGRAM SAFE WRAPPERS
# ═══════════════════════════════════════

async def _safe_edit(msg, text: str, **kw):
    try:
        await msg.edit_text(text, **kw)
    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.5)
        try: await msg.edit_text(text, **kw)
        except Exception: pass
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            pass
    except (TimedOut, TelegramError):
        pass
    except Exception:
        pass

async def _safe_send(bot, cid: int, text: str, **kw):
    try:
        return await bot.send_message(cid, text, **kw)
    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.5)
        try: return await bot.send_message(cid, text, **kw)
        except Exception: return None
    except (BadRequest, TimedOut):
        return None
    except Exception:
        return None

async def _safe_del(bot, cid: int, mid: int):
    try:
        await bot.delete_message(cid, mid)
    except (BadRequest, TimedOut, TelegramError):
        pass
    except Exception:
        pass

# ═══════════════════════════════════════
#  CORE — get number + adaptive OTP poll
# ═══════════════════════════════════════

async def _get_number(ctx: ContextTypes.DEFAULT_TYPE, user: User):
    if _is_flood(user.id):
        return

    _reg(user.id)

    if _banned(user.id):
        await _safe_send(ctx.bot, user.id, _msg_banned(), parse_mode=ParseMode.HTML)
        return

    if not await _is_sub(ctx.bot, user.id):
        await _safe_send(ctx.bot, user.id, _msg_join(), parse_mode=ParseMode.HTML, reply_markup=_kb_join())
        return

    if len(_orders) >= MAX_CONCURRENT:
        await _safe_send(ctx.bot, user.id, _msg_busy(), parse_mode=ParseMode.HTML)
        return

    _track(user, "request")
    await _cancel_order(ctx, user.id)

    # Smart loading: start API immediately, show searching only if slow
    api_task = asyncio.create_task(_fetch_number(FETCH_TIMEOUT))
    msg = None

    done, _ = await asyncio.wait({api_task}, timeout=0.5)

    if api_task in done:
        result = api_task.result()
    else:
        msg = await _safe_send(ctx.bot, user.id, _msg_searching(), parse_mode=ParseMode.HTML)
        result = await api_task

    if not result:
        if msg:
            await _safe_edit(msg, _msg_no_number(), parse_mode=ParseMode.HTML)
        else:
            await _safe_send(ctx.bot, user.id, _msg_no_number(), parse_mode=ParseMode.HTML)
        return

    api_num, nid, mins = result
    phone = _fmt_phone(api_num, config["code"])
    _track(user, "number")

    if msg:
        await _safe_edit(msg, _msg_number(phone), parse_mode=ParseMode.HTML, reply_markup=_kb_waiting())
    else:
        msg = await _safe_send(ctx.bot, user.id, _msg_number(phone), parse_mode=ParseMode.HTML, reply_markup=_kb_waiting())

    nm, un = _user_info(user)
    await _safe_send(ctx.bot, ADMIN_ID, _msg_admin_num(user.id, nm, un, phone), parse_mode=ParseMode.HTML)
    _activity.append(f"{un} {phone} {time.strftime('%H:%M')}")

    if msg:
        task = asyncio.create_task(_poll_otp(ctx, user, nid, phone, msg.message_id, msg.chat_id))
        _orders[nid] = {"task": task, "msg_id": msg.message_id, "user_id": user.id, "chat_id": msg.chat_id}
        _user_orders[user.id] = nid
        _otp_seen[nid] = set()


async def _poll_otp(ctx, user: User, nid: str, phone: str, msg_id: int, chat_id: int):
    """Adaptive OTP polling: fast at start, gradually slows down."""
    seen = _otp_seen.get(nid, set())
    poll_interval = POLL_FAST
    consecutive_empty = 0
    last_text = ""
    try:
        for tick in range(POLL_TIMEOUT):
            await asyncio.sleep(poll_interval)

            data = await _fetch_sms(nid)
            if not data:
                consecutive_empty += 1
                # Adaptive: slow down after 30 empty polls
                if consecutive_empty > 30 and poll_interval < POLL_SLOW:
                    poll_interval = min(poll_interval + 0.2, POLL_SLOW)
                continue

            consecutive_empty = 0
            poll_interval = POLL_FAST  # reset on data

            otp = _extract_otp(data)
            if not otp or otp in seen:
                continue

            seen.add(otp)
            api_msg = data.get("message", "")

            _track(user, "otp")
            log.info(f"{_C.Y}[OTP]{_C.E} {_hide(phone)} -> {otp}")

            await _safe_del(ctx.bot, chat_id, msg_id)
            await _safe_send(ctx.bot, user.id, _msg_otp(phone, otp),
                parse_mode=ParseMode.HTML, reply_markup=_kb_otp())

            _orders.pop(nid, None)
            _user_orders.pop(user.id, None)
            _otp_seen.pop(nid, None)

            nm, un = _user_info(user)
            await _safe_send(ctx.bot, ADMIN_ID, _msg_admin_otp(user.id, nm, un, phone, otp), parse_mode=ParseMode.HTML)

            asyncio.create_task(_safe_send(ctx.bot, GROUP_ID,
                _msg_group_otp(_hide(phone), otp, api_msg),
                parse_mode=ParseMode.HTML, reply_markup=_kb_group()))
            return

        _orders.pop(nid, None)
        _user_orders.pop(user.id, None)
        _otp_seen.pop(nid, None)
        await _safe_send(ctx.bot, user.id, _msg_timeout(phone), parse_mode=ParseMode.HTML)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"[ERR] poll: {e}")

# ═══════════════════════════════════════
#  SMART BROADCAST ENGINE
# ═══════════════════════════════════════

def _detect_media_type(msg) -> str:
    if msg.photo: return "photo"
    if msg.video: return "video"
    if msg.document and msg.document.mime_type and "gif" in msg.document.mime_type: return "animation"
    if msg.document: return "document"
    if msg.audio: return "audio"
    if msg.voice: return "voice"
    if msg.sticker: return "sticker"
    if msg.animation: return "animation"
    if msg.video_note: return "video_note"
    if msg.contact: return "contact"
    if msg.poll: return "poll"
    if msg.forward_origin or msg.forward_from or msg.forward_from_chat: return "forwarded"
    return "text"

async def _bc_send_one(bot, uid: int, chat_id: int, msg_id: int, mode: str):
    if mode == "forward":
        await bot.forward_message(uid, chat_id, msg_id)
    else:
        await bot.copy_message(uid, chat_id, msg_id)

async def _bc_worker(bot, uids: List[int], chat_id: int, msg_id: int,
                      mode: str, progress: Dict, delay: float,
                      cancel: set, admin_id: int):
    for uid in uids:
        if admin_id in cancel:
            return
        for attempt in range(3):
            try:
                await _bc_send_one(bot, uid, chat_id, msg_id, mode)
                progress["ok"] += 1
                break
            except RetryAfter as e:
                delay = min(delay * 1.5, 2.0)
                await asyncio.sleep(float(e.retry_after) + 0.5)
                continue
            except TimedOut:
                if attempt < 2:
                    await asyncio.sleep(1.0)
                continue
            except (BadRequest, TelegramError):
                progress["fail"] += 1
                break
            except Exception:
                progress["fail"] += 1
                break
        else:
            progress["fail"] += 1
        progress["done"] += 1
        await asyncio.sleep(delay)

async def _bc_progress_loop(bot, progress_msg, progress: Dict, total: int,
                             cancel: set, admin_id: int):
    start = time.time()
    while True:
        await asyncio.sleep(3)
        if admin_id in cancel:
            return
        done = progress["done"]
        if done >= total:
            return
        elapsed = time.time() - start
        speed = done / elapsed if elapsed > 0 else 0
        remaining = (total - done) / speed if speed > 0 else 0
        try:
            await _safe_edit(progress_msg,
                f"{'':>3}------\u26a1 Broadcasting \u26a1------\n"
                f"Total   | {total}\n"
                f"Sent    | {progress['ok']}\n"
                f"Failed  | {progress['fail']}\n"
                f"Speed   | {speed:.1f}/s\n"
                f"ETA     | {remaining:.0f}s\n"
                f"Workers | {BROADCAST_WORKERS}",
                parse_mode=ParseMode.HTML, reply_markup=_kb_broadcast_cancel())
        except Exception:
            pass

async def _broadcast_smart(bot, chat_id: int, msg_id: int, mode: str,
                            progress_msg=None, admin_id: int = 0):
    if not users_db:
        return 0, 0, 0.0

    _broadcast_cancel.discard(admin_id)
    total = len(users_db)
    delay = BROADCAST_DELAY
    progress = {"ok": 0, "fail": 0, "done": 0}

    chunk_size = max(1, total // BROADCAST_WORKERS)
    chunks = [users_db[i:i+chunk_size] for i in range(0, total, chunk_size)]

    start = time.time()

    progress_task = None
    if progress_msg:
        progress_task = asyncio.create_task(
            _bc_progress_loop(bot, progress_msg, progress, total,
                               _broadcast_cancel, admin_id))

    tasks = [
        _bc_worker(bot, chunk, chat_id, msg_id, mode, progress, delay,
                    _broadcast_cancel, admin_id)
        for chunk in chunks
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

    if progress_task and not progress_task.done():
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

    elapsed = time.time() - start
    return progress["ok"], progress["fail"], elapsed

# ═══════════════════════════════════════
#  HANDLERS
# ═══════════════════════════════════════

async def h_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    _reg(u.id)
    if _banned(u.id):
        return await _safe_send(ctx.bot, u.id, _msg_banned(), parse_mode=ParseMode.HTML)
    # INSTANT response — check sub in background, edit if needed
    msg = await _safe_send(ctx.bot, u.id, _msg_welcome(u.first_name or "User"),
        parse_mode=ParseMode.HTML, reply_markup=_kb_start())
    # Background subscription check — only edit if NOT subscribed
    if msg:
        try:
            if not await _is_sub(ctx.bot, u.id):
                await _safe_edit(msg, _msg_join(), parse_mode=ParseMode.HTML, reply_markup=_kb_join())
        except Exception:
            pass

async def h_get_number(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _get_number(ctx, update.effective_user)

async def h_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try: await q.answer()
    except Exception: pass
    d = q.data
    _is_admin = q.from_user.id == ADMIN_ID

    try:
        if d == "check_join":
            if await _is_sub(ctx.bot, q.from_user.id):
                if q.message:
                    await _safe_edit(q.message, _msg_verified(), parse_mode=ParseMode.HTML)
            else:
                try: await q.answer("\u26a0\ufe0f Join all chats first!", show_alert=True)
                except Exception: pass

        elif d == "get":
            await _get_number(ctx, q.from_user)

        elif d == "ar":
            await _admin_update(q.message, ctx, edit=True)

        elif d == "ap":
            if q.message:
                try: await q.message.edit_reply_markup(reply_markup=_kb_presets())
                except Exception: pass

        elif d.startswith("sp_"):
            key = d.split("_", 1)[1]
            if key in PRESETS:
                _preset_set_active(key)
                try: await q.answer(f"\u2705 {PRESETS[key]['name']}")
                except Exception: pass
                await _admin_update(q.message, ctx, edit=True)

        # ── Preset Manager (admin only) ──
        elif d == "preset_mgr" and _is_admin:
            if q.message:
                active_name = PRESETS[_active_preset]["name"] if _active_preset in PRESETS else "None"
                txt = (
                    f"{'':>3}------\u2699\ufe0f Preset Manager \u2699\ufe0f------\n\n"
                    f"Active  | <b>{active_name}</b>\n"
                    f"Total   | <code>{len(PRESETS)}</code> presets\n\n"
                    f"Manage your presets:"
                )
                await _safe_edit(q.message, txt, parse_mode=ParseMode.HTML,
                    reply_markup=_kb_preset_manager())

        elif d == "pm_add" and _is_admin:
            _admin_state[q.from_user.id] = "preset_add_name"
            _preset_temp[q.from_user.id] = {}
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\u2795 Add Preset \u2795------\n\n"
                    f"Step 1/4: Send preset <b>Name</b>\n"
                    f"Example: <code>Bangladesh</code>",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "pm_edit" and _is_admin:
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\u270f\ufe0f Edit Preset \u270f\ufe0f------\n\n"
                    f"Select preset to edit:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_kb_preset_list("ped"))

        elif d.startswith("ped_") and _is_admin:
            key = d.split("_", 1)[1]
            if key in PRESETS and q.message:
                pre = PRESETS[key]
                txt = (
                    f"{'':>3}------\u270f\ufe0f Edit: {pre['flag']} {pre['name']} \u270f\ufe0f------\n\n"
                    f"Name  | <code>{pre['name']}</code>\n"
                    f"Code  | <code>{pre['code']}</code>\n"
                    f"Range | <code>{pre['range']}</code>\n"
                    f"Flag  | {pre['flag']}\n\n"
                    f"Select field to edit:"
                )
                await _safe_edit(q.message, txt, parse_mode=ParseMode.HTML,
                    reply_markup=_kb_preset_edit_fields(key))

        elif d.startswith("pe_name_") and _is_admin:
            key = d.split("_", 2)[2]
            _admin_state[q.from_user.id] = f"preset_edit_name_{key}"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f30d Edit Name \U0001f30d------\n\n"
                    f"Current: <code>{PRESETS.get(key, {}).get('name', '?')}</code>\n\n"
                    f"Send new name:",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d.startswith("pe_code_") and _is_admin:
            key = d.split("_", 2)[2]
            _admin_state[q.from_user.id] = f"preset_edit_code_{key}"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f4de Edit Code \U0001f4de------\n\n"
                    f"Current: <code>{PRESETS.get(key, {}).get('code', '?')}</code>\n\n"
                    f"Send new code (e.g. <code>+880</code>):",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d.startswith("pe_range_") and _is_admin:
            key = d.split("_", 2)[2]
            _admin_state[q.from_user.id] = f"preset_edit_range_{key}"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f522 Edit Range \U0001f522------\n\n"
                    f"Current: <code>{PRESETS.get(key, {}).get('range', '?')}</code>\n\n"
                    f"Send new range (e.g. <code>88017XXXXX</code>):",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d.startswith("pe_flag_") and _is_admin:
            key = d.split("_", 2)[2]
            _admin_state[q.from_user.id] = f"preset_edit_flag_{key}"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f3a8 Edit Flag \U0001f3a8------\n\n"
                    f"Current: {PRESETS.get(key, {}).get('flag', '?')}\n\n"
                    f"Send new flag emoji:",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "pm_del" and _is_admin:
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f5d1\ufe0f Delete Preset \U0001f5d1\ufe0f------\n\n"
                    f"Select preset to delete:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_kb_preset_list("pdl"))

        elif d.startswith("pdl_") and _is_admin:
            key = d.split("_", 1)[1]
            if key in PRESETS:
                name = PRESETS[key]["name"]
                _preset_delete(key)
                try: await q.answer(f"\U0001f5d1 Deleted {name}")
                except Exception: pass
                if q.message:
                    await _safe_edit(q.message,
                        f"{'':>3}------\U0001f5d1\ufe0f Delete Preset \U0001f5d1\ufe0f------\n\n"
                        f"Deleted: <b>{name}</b>\n"
                        f"Remaining: <code>{len(PRESETS)}</code> presets",
                        parse_mode=ParseMode.HTML,
                        reply_markup=_kb_preset_list("pdl"))

        elif d == "pm_active" and _is_admin:
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f3af Active Preset \U0001f3af------\n\n"
                    f"Current: <b>{PRESETS.get(_active_preset, {}).get('name', 'None')}</b>\n\n"
                    f"Select new active preset:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_kb_preset_active())

        elif d.startswith("pa_") and _is_admin:
            key = d.split("_", 1)[1]
            if key in PRESETS:
                _preset_set_active(key)
                try: await q.answer(f"\u2705 Active: {PRESETS[key]['name']}")
                except Exception: pass
                await _admin_update(q.message, ctx, edit=True)

        elif d == "pm_chrange" and _is_admin:
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\u270f\ufe0f Change Range \u270f\ufe0f------\n\n"
                    f"Select preset to change range:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_kb_preset_list("chr"))

        elif d.startswith("chr_") and _is_admin:
            key = d.split("_", 1)[1]
            if key in PRESETS and q.message:
                pre = PRESETS[key]
                _admin_state[q.from_user.id] = f"preset_chrange_{key}"
                await _safe_edit(q.message,
                    f"{'':>3}------\u270f\ufe0f Change Range \u270f\ufe0f------\n\n"
                    f"{pre['flag']} <b>{pre['name']}</b>\n\n"
                    f"Current Range:\n<code>{pre['range']}</code>\n\n"
                    f"Send New Range:\n"
                    f"Example: <code>22896XXXXX</code>",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "pm_range" and _is_admin:
            _admin_state[q.from_user.id] = "preset_range_country"
            _preset_temp[q.from_user.id] = {}
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f522 Custom Range \U0001f522------\n\n"
                    f"Step 1/3: Send <b>Country Name</b>\n"
                    f"Example: <code>Bangladesh</code>",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "al":
            logs = "\n".join(_activity) or "No logs yet."
            txt = f"{'':>3}------\U0001f4dc Activity Log \U0001f4dc------\n\n<code>{logs}</code>"
            if q.message:
                await _safe_edit(q.message, txt, parse_mode=ParseMode.HTML, reply_markup=_kb_back())

        elif d == "an":
            await _admin_analytics(q.message, ctx)

        elif d == "ab":
            _admin_state[q.from_user.id] = "ban"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f6ab Ban User \U0001f6ab------\nSend User ID:",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "aub":
            _admin_state[q.from_user.id] = "unban"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\u2705 Unban User \u2705------\nSend User ID:",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "broadcast_panel":
            _admin_state[q.from_user.id] = "broadcast"
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\U0001f4e2 Broadcast \U0001f4e2------\n\n"
                    f"Send anything to broadcast.\n\n"
                    f"<b>Supported:</b>\n"
                    f"Text, Photo, Video, Document,\n"
                    f"Audio, Voice, Sticker, GIF,\n"
                    f"Contact, Poll, Forwarded msgs\n\n"
                    f"<i>Auto-detects copy vs forward.</i>",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

        elif d == "bc_cancel":
            _broadcast_cancel.add(q.from_user.id)
            _admin_state.pop(q.from_user.id, None)
            if q.message:
                await _safe_edit(q.message,
                    f"{'':>3}------\u23f9 Broadcast Cancelled \u23f9------\n\n"
                    f"Cancelling... remaining sends will stop.",
                    parse_mode=ParseMode.HTML, reply_markup=_kb_back())

        elif d == "au" and _is_admin:
            txt, pages = _fmt_user_list(0)
            if q.message:
                await _safe_edit(q.message, txt, parse_mode=ParseMode.HTML,
                    reply_markup=_kb_user_list(0, pages))

        elif d.startswith("au_") and _is_admin:
            try:
                page = int(d.split("_", 1)[1])
            except ValueError:
                page = 0
            txt, pages = _fmt_user_list(page)
            if q.message:
                await _safe_edit(q.message, txt, parse_mode=ParseMode.HTML,
                    reply_markup=_kb_user_list(page, pages))

        elif d == "cx":
            _admin_state.pop(q.from_user.id, None)
            _broadcast_cancel.discard(q.from_user.id)
            _preset_temp.pop(q.from_user.id, None)
            await _admin_update(q.message, ctx, edit=True)

    except Exception as e:
        log.error(f"[ERR] callback: {e}")

# ═══════════════════════════════════════
#  ADMIN PANEL + ANALYTICS
# ═══════════════════════════════════════

async def h_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    msg = await _safe_send(ctx.bot, ADMIN_ID,
        f"{'':>3}------\u2699\ufe0f Loading \u2699\ufe0f------", parse_mode=ParseMode.HTML)
    if msg:
        await _admin_update(msg, ctx, edit=True)

async def _admin_update(message, ctx, edit=False):
    txt = _msg_dashboard()
    kb = _kb_admin()
    if edit and message:
        await _safe_edit(message, txt, parse_mode=ParseMode.HTML, reply_markup=kb)
    elif message:
        await _safe_send(ctx.bot, message.chat_id, txt, parse_mode=ParseMode.HTML, reply_markup=kb)

async def _admin_analytics(message, ctx):
    s = bot_data.get("stats", {})
    otp_t = int(_db_get_stat("total_otps", "0"))
    num_t = int(_db_get_stat("total_numbers", "0"))
    rate = f"{(otp_t / num_t * 100):.1f}%" if num_t > 0 else "0%"
    avg_lat = _avg_latency()
    p95_lat = _p95_latency()

    recent = _db.execute(
        "SELECT action, COUNT(*) FROM activity GROUP BY action"
    ).fetchall()
    activity_summary = "\n".join(f"  {r[0]} | {r[1]}" for r in recent) or "  No data."

    txt = (
        f"{'':>3}------\U0001f4c9 Analytics \U0001f4c9------\n\n"
        f"\U0001f4ca <b>Performance</b>\n"
        f"  Avg Latency | <code>{avg_lat}s</code>\n"
        f"  P95 Latency | <code>{p95_lat}s</code>\n"
        f"  Success Rate| <code>{rate}</code>\n"
        f"  Endpoints   | <code>{len(API_ENDPOINTS)}</code>\n\n"
        f"\U0001f4c8 <b>Lifetime</b>\n"
        f"  Numbers     | <code>{num_t}</code>\n"
        f"  OTPs        | <code>{otp_t}</code>\n"
        f"  Users       | <code>{_db_user_count()}</code>\n\n"
        f"\U0001f4cb <b>Activity Breakdown</b>\n{activity_summary}"
    )
    if message:
        await _safe_edit(message, txt, parse_mode=ParseMode.HTML, reply_markup=_kb_back())

# ═══════════════════════════════════════
#  ADMIN MESSAGE HANDLER
# ═══════════════════════════════════════

async def h_admin_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return
    state = _admin_state.pop(update.effective_user.id, None)
    if not state or not update.message:
        return

    if state == "broadcast":
        source_msg = update.message.reply_to_message if update.message.reply_to_message else update.message
        source_chat = source_msg.chat_id
        source_mid = source_msg.message_id

        # Smart auto-detection
        is_forwarded = bool(source_msg.forward_origin or source_msg.forward_from or source_msg.forward_from_chat)
        mode = "forward" if is_forwarded else "copy"
        mode_label = "Forward" if mode == "forward" else "Copy"
        media_type = _detect_media_type(source_msg)

        msg = await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u26a1 Broadcasting \u26a1------\n"
            f"Mode   | {mode_label}\n"
            f"Type   | {media_type}\n"
            f"Workers| {BROADCAST_WORKERS}\n"
            f"Users  | {len(users_db)}\n"
            f"Sending...",
            parse_mode=ParseMode.HTML, reply_markup=_kb_broadcast_cancel())

        ok, fail, elapsed = await _broadcast_smart(ctx.bot, source_chat, source_mid, mode,
                                                    progress_msg=msg, admin_id=ADMIN_ID)

        if msg:
            await _safe_edit(msg,
                f"{'':>3}------\u2705 Broadcast Done \u2705------\n"
                f"Mode   | {mode_label}\n"
                f"Type   | {media_type}\n"
                f"Sent   | {ok}/{len(users_db)}\n"
                f"Failed | {fail}\n"
                f"Time   | {elapsed:.1f}s\n"
                f"Workers| {BROADCAST_WORKERS}",
                parse_mode=ParseMode.HTML)
        return

    if state in ("ban", "unban"):
        if not update.message.text:
            return
        target = None
        for tok in reversed(update.message.text.strip().split()):
            if tok.lstrip("-").isdigit():
                target = int(tok); break
        if not target:
            return await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u274c Invalid ID \u274c------", parse_mode=ParseMode.HTML)
        if state == "ban":
            if _ban(target):
                await _safe_send(ctx.bot, ADMIN_ID,
                    f"{'':>3}------\U0001f6ab Banned \U0001f6ab------\nUser: <code>{target}</code>",
                    parse_mode=ParseMode.HTML)
            else:
                await _safe_send(ctx.bot, ADMIN_ID,
                    f"{'':>3}------\u26a0\ufe0f Already Banned \u26a0\ufe0f------\nUser: <code>{target}</code>",
                    parse_mode=ParseMode.HTML)
        else:
            if _unban(target):
                await _safe_send(ctx.bot, ADMIN_ID,
                    f"{'':>3}------\u2705 Unbanned \u2705------\nUser: <code>{target}</code>",
                    parse_mode=ParseMode.HTML)
            else:
                await _safe_send(ctx.bot, ADMIN_ID,
                    f"{'':>3}------\u26a0\ufe0f Not Banned \u26a0\ufe0f------\nUser: <code>{target}</code>",
                    parse_mode=ParseMode.HTML)
        return

    # ── Preset Add Flow (4 steps) ──
    if state == "preset_add_name":
        tmp = _preset_temp.get(update.effective_user.id, {})
        tmp["name"] = update.message.text.strip()
        _preset_temp[update.effective_user.id] = tmp
        _admin_state[update.effective_user.id] = "preset_add_code"
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2795 Add Preset \u2795------\n\n"
            f"Step 2/4: Send <b>Country Code</b>\n"
            f"Example: <code>+880</code>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())
        return

    if state == "preset_add_code":
        tmp = _preset_temp.get(update.effective_user.id, {})
        code = update.message.text.strip()
        if not code.startswith("+"):
            code = "+" + code
        tmp["code"] = code
        _preset_temp[update.effective_user.id] = tmp
        _admin_state[update.effective_user.id] = "preset_add_range"
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2795 Add Preset \u2795------\n\n"
            f"Step 3/4: Send <b>Number Range</b>\n"
            f"Example: <code>88017XXXXX</code>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())
        return

    if state == "preset_add_range":
        tmp = _preset_temp.get(update.effective_user.id, {})
        tmp["range"] = update.message.text.strip()
        _preset_temp[update.effective_user.id] = tmp
        _admin_state[update.effective_user.id] = "preset_add_flag"
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2795 Add Preset \u2795------\n\n"
            f"Step 4/4: Send <b>Flag Emoji</b>\n"
            f"Example: \U0001f1e7\U0001f1e9",
            parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())
        return

    if state == "preset_add_flag":
        tmp = _preset_temp.pop(update.effective_user.id, {})
        flag = _validate_flag(update.message.text)
        name = tmp.get("name", "")
        code = tmp.get("code", "")
        range_ = tmp.get("range", "")
        if not name or not code or not range_:
            return await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u274c Missing Data \u274c------\nPlease try again.",
                parse_mode=ParseMode.HTML)
        key = name[:2].upper()
        counter = 1
        base_key = key
        while key in PRESETS:
            key = f"{base_key}{counter}"
            counter += 1
        _preset_add(key, name, code, range_, flag)
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2705 Preset Added \u2705------\n\n"
            f"Key   | <code>{key}</code>\n"
            f"Name  | {flag} <b>{name}</b>\n"
            f"Code  | <code>{code}</code>\n"
            f"Range | <code>{range_}</code>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_preset_manager())
        return

    # ── Preset Edit Flows ──
    if state.startswith("preset_edit_name_"):
        key = state.split("_", 3)[3]
        new_name = update.message.text.strip()
        if key in PRESETS:
            _preset_edit(key, name=new_name)
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u2705 Name Updated \u2705------\n"
                f"Key  | <code>{key}</code>\n"
                f"Name | <b>{new_name}</b>",
                parse_mode=ParseMode.HTML, reply_markup=_kb_preset_edit_fields(key))
        return

    if state.startswith("preset_edit_code_"):
        key = state.split("_", 3)[3]
        new_code = update.message.text.strip()
        if not new_code.startswith("+"):
            new_code = "+" + new_code
        if key in PRESETS:
            _preset_edit(key, code=new_code)
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u2705 Code Updated \u2705------\n"
                f"Key  | <code>{key}</code>\n"
                f"Code | <code>{new_code}</code>",
                parse_mode=ParseMode.HTML, reply_markup=_kb_preset_edit_fields(key))
        return

    if state.startswith("preset_edit_range_"):
        key = state.split("_", 3)[3]
        new_range = update.message.text.strip()
        if key in PRESETS:
            _preset_edit(key, range_=new_range)
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u2705 Range Updated \u2705------\n"
                f"Key   | <code>{key}</code>\n"
                f"Range | <code>{new_range}</code>",
                parse_mode=ParseMode.HTML, reply_markup=_kb_preset_edit_fields(key))
        return

    if state.startswith("preset_edit_flag_"):
        key = state.split("_", 3)[3]
        new_flag = _validate_flag(update.message.text)
        if key in PRESETS:
            _preset_edit(key, flag=new_flag)
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u2705 Flag Updated \u2705------\n"
                f"Key  | <code>{key}</code>\n"
                f"Flag | {new_flag}",
                parse_mode=ParseMode.HTML, reply_markup=_kb_preset_edit_fields(key))
        return

    # ── Change Range Flow ──
    if state.startswith("preset_chrange_"):
        key = state.split("_", 2)[2]
        new_range = update.message.text.strip()
        if key in PRESETS:
            pre = PRESETS[key]
            old_range = pre.get("range", "?")
            _preset_edit(key, range_=new_range)
            if key == _active_preset:
                config["range"] = new_range
                _save(CONFIG_FILE, config)
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u2705 Range Updated \u2705------\n\n"
                f"{pre['flag']} <b>{pre['name']}</b>\n\n"
                f"Old:\n<code>{old_range}</code>\n\n"
                f"New:\n<code>{new_range}</code>",
                parse_mode=ParseMode.HTML, reply_markup=_kb_preset_manager())
        else:
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u274c Preset Not Found \u274c------",
                parse_mode=ParseMode.HTML, reply_markup=_kb_preset_manager())
        return

    # ── Custom Range Flow (3 steps) ──
    if state == "preset_range_country":
        tmp = _preset_temp.get(update.effective_user.id, {})
        tmp["name"] = update.message.text.strip()
        _preset_temp[update.effective_user.id] = tmp
        _admin_state[update.effective_user.id] = "preset_range_code"
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\U0001f522 Custom Range \U0001f522------\n\n"
            f"Step 2/3: Send <b>Country Code</b>\n"
            f"Example: <code>+880</code>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())
        return

    if state == "preset_range_code":
        tmp = _preset_temp.get(update.effective_user.id, {})
        code = update.message.text.strip()
        if not code.startswith("+"):
            code = "+" + code
        tmp["code"] = code
        _preset_temp[update.effective_user.id] = tmp
        _admin_state[update.effective_user.id] = "preset_range_range"
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\U0001f522 Custom Range \U0001f522------\n\n"
            f"Step 3/3: Send <b>Number Range</b>\n"
            f"Example: <code>88017XXXXX</code>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())
        return

    if state == "preset_range_range":
        tmp = _preset_temp.pop(update.effective_user.id, {})
        range_ = update.message.text.strip()
        name = tmp.get("name", "")
        code = tmp.get("code", "")
        if not name or not code:
            return await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u274c Missing Data \u274c------\nPlease try again.",
                parse_mode=ParseMode.HTML)
        config.update({"country": name, "code": code, "range": range_})
        _save(CONFIG_FILE, config)
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2705 Custom Range Set \u2705------\n\n"
            f"Country | <b>{name}</b>\n"
            f"Code    | <code>{code}</code>\n"
            f"Range   | <code>{range_}</code>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_preset_manager())
        return

async def h_set_config(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = ctx.args
    if len(args) < 3:
        return await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u274c Usage \u274c------\n"
            f"/set \U0001f1f9\U0001f1ef Togo +22 22897XXXXX\n\n"
            f"Format: /set {chr(127481)}{chr(127487)} Country Code Range",
            parse_mode=ParseMode.HTML)
    try:
        flag = _validate_flag(args[0])
        if flag != "\U0001f30d":
            if len(args) < 4:
                return await _safe_send(ctx.bot, ADMIN_ID,
                    f"{'':>3}------\u274c Usage \u274c------\n"
                    f"/set \U0001f1f9\U0001f1ef Togo +22 22897XXXXX",
                    parse_mode=ParseMode.HTML)
            nco, nc, nr = args[1], args[2], args[3]
        else:
            flag = "\U0001f30d"
            nr, nc = args[-1], args[-2]
            nco = " ".join(args[:-2])
        if not nc.startswith("+"):
            nc = "+" + nc
        config.update({"country": nco, "code": nc, "range": nr})
        _save(CONFIG_FILE, config)
        if _active_preset and _active_preset in PRESETS:
            _preset_edit(_active_preset, name=nco, code=nc, range_=nr, flag=flag)
        else:
            key = nco[:2].upper()
            c = 1
            bk = key
            while key in PRESETS:
                key = f"{bk}{c}"; c += 1
            _preset_add(key, nco, nc, nr, flag)
            _preset_set_active(key)
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2705 Config Updated \u2705------\n"
            f"{flag} {nco}\n\U0001f4de {nc}\n\U0001f522 {nr}",
            parse_mode=ParseMode.HTML)
        log.info(f"{_C.C}[CFG]{_C.E} {flag} {nco} {nc} {nr}")
    except Exception as e:
        log.error(f"[ERR] set_config: {e}")

async def h_ban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        return await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\U0001f6ab Usage \U0001f6ab------\n/ban user_id",
            parse_mode=ParseMode.HTML)
    try:
        tid = int(ctx.args[0])
        if _ban(tid):
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\U0001f6ab Banned \U0001f6ab------\n<code>{tid}</code>",
                parse_mode=ParseMode.HTML)
        else:
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u26a0\ufe0f Already Banned \u26a0\ufe0f------\n<code>{tid}</code>",
                parse_mode=ParseMode.HTML)
    except ValueError:
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u274c Invalid ID \u274c------", parse_mode=ParseMode.HTML)

async def h_unban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        return await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u2705 Usage \u2705------\n/unban user_id",
            parse_mode=ParseMode.HTML)
    try:
        tid = int(ctx.args[0])
        if _unban(tid):
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u2705 Unbanned \u2705------\n<code>{tid}</code>",
                parse_mode=ParseMode.HTML)
        else:
            await _safe_send(ctx.bot, ADMIN_ID,
                f"{'':>3}------\u26a0\ufe0f Not Banned \u26a0\ufe0f------\n<code>{tid}</code>",
                parse_mode=ParseMode.HTML)
    except ValueError:
        await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\u274c Invalid ID \u274c------", parse_mode=ParseMode.HTML)

async def h_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    if _banned(u.id):
        return await _safe_send(ctx.bot, u.id, _msg_banned(), parse_mode=ParseMode.HTML)
    oid = _user_orders.get(u.id)
    if not oid or oid not in _orders:
        return await _safe_send(ctx.bot, u.id, _msg_no_active(), parse_mode=ParseMode.HTML)
    await _safe_send(ctx.bot, u.id, _msg_active(oid), parse_mode=ParseMode.HTML)

async def h_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not update.message.reply_to_message and not ctx.args:
        _admin_state[ADMIN_ID] = "broadcast"
        return await _safe_send(ctx.bot, ADMIN_ID,
            f"{'':>3}------\U0001f4e2 Broadcast \U0001f4e2------\n\n"
            f"Send anything to broadcast.\n\n"
            f"<b>Supported:</b>\n"
            f"Text, Photo, Video, Document,\n"
            f"Audio, Voice, Sticker, GIF,\n"
            f"Contact, Poll, Forwarded msgs\n\n"
            f"<i>Auto-detects copy vs forward.</i>",
            parse_mode=ParseMode.HTML, reply_markup=_kb_cancel())

    source_msg = update.message.reply_to_message if update.message.reply_to_message else update.message
    source_chat = source_msg.chat_id
    source_mid = source_msg.message_id

    is_forwarded = bool(source_msg.forward_origin or source_msg.forward_from or source_msg.forward_from_chat)
    mode = "forward" if is_forwarded else "copy"
    mode_label = "Forward" if mode == "forward" else "Copy"
    media_type = _detect_media_type(source_msg)

    msg = await _safe_send(ctx.bot, ADMIN_ID,
        f"{'':>3}------\u26a1 Broadcasting \u26a1------\n"
        f"Mode   | {mode_label}\n"
        f"Type   | {media_type}\n"
        f"Workers| {BROADCAST_WORKERS}\n"
        f"Users  | {len(users_db)}\n"
        f"Sending...",
        parse_mode=ParseMode.HTML, reply_markup=_kb_broadcast_cancel())

    ok, fail, elapsed = await _broadcast_smart(ctx.bot, source_chat, source_mid, mode,
                                                progress_msg=msg, admin_id=ADMIN_ID)

    if msg:
        await _safe_edit(msg,
            f"{'':>3}------\u2705 Broadcast Done \u2705------\n"
            f"Mode   | {mode_label}\n"
            f"Type   | {media_type}\n"
            f"Sent   | {ok}/{len(users_db)}\n"
            f"Failed | {fail}\n"
            f"Time   | {elapsed:.1f}s\n"
            f"Workers| {BROADCAST_WORKERS}",
            parse_mode=ParseMode.HTML)

async def h_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    err = ctx.error
    if isinstance(err, (TimedOut, RetryAfter)):
        return
    if isinstance(err, BadRequest) and "Message is not modified" in str(err):
        return
    if isinstance(err, TelegramError) and "Message to delete not found" in str(err):
        return
    log.error(f"[ERR] {type(err).__name__}: {err}")

# ═══════════════════════════════════════
#  BACKGROUND SYSTEMS — optimized loops
# ═══════════════════════════════════════

async def _save_loop():
    global _users_dirty, _data_dirty, _db_dirty
    while True:
        await asyncio.sleep(SAVE_INTERVAL)
        try:
            if _users_dirty:
                await asyncio.to_thread(_save, USERS_FILE, users_db)
                _users_dirty = False
            if _data_dirty:
                await asyncio.to_thread(_save, DATA_FILE, bot_data)
                _data_dirty = False
            if _db and _db_dirty:
                await asyncio.to_thread(_db.commit)
                _db_dirty = False
        except Exception as e:
            log.error(f"[ERR] save_loop: {e}")

async def _db_commit_loop():
    """Separate DB commit loop for batched writes."""
    global _db_dirty
    while True:
        await asyncio.sleep(DB_COMMIT_INTERVAL)
        try:
            if _db and _db_dirty:
                await asyncio.to_thread(_db.commit)
                _db_dirty = False
        except Exception as e:
            log.error(f"[ERR] db_commit: {e}")

async def _cleanup_loop():
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        try:
            now = time.monotonic()
            # Flood cache — batch delete
            if len(_flood_cache) > 200:
                stale = [k for k, v in _flood_cache.items() if now - v > 600]
                for k in stale:
                    del _flood_cache[k]
            # Sub cache
            if len(_sub_cache) > 100:
                stale_sub = [k for k, v in _sub_cache.items() if now - v[0] > SUB_CACHE_TTL * 2]
                for k in stale_sub:
                    del _sub_cache[k]
            # Dedup cache
            if len(_msg_dedup) > 100:
                stale_dedup = [k for k, v in _msg_dedup.items() if now - v > DEDUP_TTL]
                for k in stale_dedup:
                    del _msg_dedup[k]
            # Done tasks
            _bg_tasks.difference_update(t for t in _bg_tasks if t.done())
            # Reconnect check
            await _check_reconnect()
            # Commit DB
            if _db and _db_dirty:
                await asyncio.to_thread(_db.commit)
                _db_dirty = False
        except Exception as e:
            log.error(f"[ERR] cleanup_loop: {e}")

async def _gc_loop():
    """Periodic garbage collection for low-memory environments."""
    while True:
        await asyncio.sleep(GC_INTERVAL)
        try:
            gc.collect()
        except Exception:
            pass

async def _task_watchdog():
    """Monitor and clean up orphan/stuck tasks."""
    while True:
        await asyncio.sleep(TASK_WATCHDOG_INT)
        try:
            # Clean completed bg tasks
            _bg_tasks.difference_update(t for t in _bg_tasks if t.done())
            # Check for stuck orders (no activity for 15+ minutes)
            now = time.monotonic()
            stuck = []
            for nid, order in _orders.items():
                t = order.get("task")
                if t and t.done():
                    stuck.append(nid)
            for nid in stuck:
                _orders.pop(nid, None)
                _otp_seen.pop(nid, None)
            # Clean stale user_orders references
            stale_users = [uid for uid, nid in _user_orders.items() if nid not in _orders]
            for uid in stale_users:
                _user_orders.pop(uid, None)
        except Exception as e:
            log.error(f"[ERR] watchdog: {e}")

async def _health_loop():
    while True:
        await asyncio.sleep(60)
        try:
            if not _orders:
                await _ensure_session()
        except Exception as e:
            log.error(f"[ERR] health_loop: {e}")

async def _backup_loop():
    while True:
        await asyncio.sleep(BACKUP_INTERVAL)
        try:
            os.makedirs(BACKUP_DIR, exist_ok=True)
            ts = time.strftime("%Y%m%d_%H%M")
            for f in (DB_FILE, USERS_FILE, DATA_FILE, PRESETS_FILE):
                if os.path.exists(f):
                    dst = os.path.join(BACKUP_DIR, f"{ts}_{os.path.basename(f)}")
                    await asyncio.to_thread(shutil.copy2, f, dst)
            # Rotate: keep last 10 per file type
            try:
                for fname in os.listdir(BACKUP_DIR):
                    full = os.path.join(BACKUP_DIR, fname)
                    if os.path.getmtime(full) < time.time() - BACKUP_INTERVAL * 10:
                        os.remove(full)
            except OSError:
                pass
            log.info(f"{_C.D}[BAK]{_C.E} Backup saved")
        except Exception as e:
            log.error(f"[ERR] backup: {e}")

# ═══════════════════════════════════════
#  CRASH RECOVERY
# ═══════════════════════════════════════

def _global_excepthook(exc_type, exc_val, exc_tb):
    log.error(f"[FATAL] {exc_type.__name__}: {exc_val}")

sys.excepthook = _global_excepthook

# ═══════════════════════════════════════
#  LIFECYCLE
# ═══════════════════════════════════════

async def _on_start(app: Application):
    global _boot_time, _api_sem
    _boot_time = time.monotonic()
    _api_sem = asyncio.Semaphore(API_SEM_LIMIT)
    await _ensure_session()

    # Launch all background tasks
    for coro in (_save_loop, _cleanup_loop, _health_loop, _backup_loop,
                 _gc_loop, _task_watchdog, _db_commit_loop):
        t = asyncio.create_task(coro())
        _bg_tasks.add(t)
        t.add_done_callback(_bg_tasks.discard)

    log.info(f"{_C.G}[✓]{_C.E} Bot Online")
    log.info(f"{_C.C}[API]{_C.E} {len(API_ENDPOINTS)} endpoint(s)")
    log.info(f"{_C.C}[CFG]{_C.E} {config['country']} {config['code']} {config['range']}")
    log.info(f"{_C.C}[SYS]{_C.E} Sem={API_SEM_LIMIT} Workers={BROADCAST_WORKERS} Poll={POLL_FAST}s")
    log.info(f"{_C.C}[DB]{_C.E} SQLite WAL ({DB_FILE})")

async def _on_shutdown(app: Application):
    global _session
    try:
        for o in _orders.values():
            t = o.get("task")
            if t and not t.done():
                t.cancel()
        _orders.clear()
    except Exception as e:
        log.error(f"[ERR] shutdown orders: {e}")
    try:
        if _session and not _session.closed:
            await _session.close()
            _session = None
    except Exception as e:
        log.error(f"[ERR] shutdown session: {e}")
    try:
        if _users_dirty:
            _save(USERS_FILE, users_db)
        if _data_dirty:
            _save(DATA_FILE, bot_data)
        _save_presets()
    except Exception as e:
        log.error(f"[ERR] shutdown save: {e}")
    try:
        if _db:
            _db.commit()
            _db.close()
    except Exception as e:
        log.error(f"[ERR] shutdown db: {e}")
    log.info(f"{_C.G}[✓]{_C.E} Bot Stopped")

# ═══════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════

def main():
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda s, f: sys.exit(0))
        except (OSError, ValueError):
            pass

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(8)
        .write_timeout(8)
        .pool_timeout(6)
        .get_updates_read_timeout(8)
        .post_init(_on_start)
        .post_shutdown(_on_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", h_start))
    app.add_handler(CommandHandler("admin", h_admin))
    app.add_handler(CommandHandler("dashboard", h_admin))
    app.add_handler(CommandHandler("set", h_set_config))
    app.add_handler(CommandHandler("ban", h_ban))
    app.add_handler(CommandHandler("unban", h_unban))
    app.add_handler(CommandHandler("status", h_status))
    app.add_handler(CommandHandler("broadcast", h_broadcast))
    app.add_handler(MessageHandler(filters.Regex("^\U0001f4f2 Get Number$"), h_get_number))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, h_admin_msg))
    app.add_handler(CallbackQueryHandler(h_callback))
    app.add_error_handler(h_error)

    log.info(f"{_C.G}[✓]{_C.E} Polling Active")
    app.run_polling(drop_pending_updates=True, poll_interval=0.3)


if __name__ == "__main__":
    main()
