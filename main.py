import asyncio
import json
import os
import logging
import aiohttp
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    User
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError

# ================= LOGGING SETUP =================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIGURATION =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8601304803:AAGwMcZ15a5O-jm2fDAvxRmQrLSYoes0jmo")
API_KEY = os.getenv("API_KEY", "nxa_1a440d9fd9df7c320e4f61f8b221fe8663ffdd40")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6668016879"))
GROUP_ID = os.getenv("GROUP_ID", "-1003727266573")
GROUP_LINK = os.getenv("GROUP_LINK", "https://t.me/seven_otp")
BOT_USERNAME = os.getenv("BOT_USERNAME", "number_xsayem_bot")
BASE_URL = os.getenv("BASE_URL", "http://185.190.142.81")
CHANNELS = ["@fastaccountapk_official", "@fast_account_updates"]
CONFIG_FILE = "config.json"
USERS_FILE = "users.json"
DATA_FILE = "bot_data.json"

HEADERS = {"X-API-Key": API_KEY}
POLL_INTERVAL = 2
POLL_TIMEOUT = 600
REQUEST_TIMEOUT = 10
INITIAL_FETCH_TIMEOUT = 10
MAX_API_RETRIES = 3
BROADCAST_DELAY = 0.05

# ================= GLOBAL STATE =================
bot_session: Optional[aiohttp.ClientSession] = None
background_tasks: set = set()
user_activity: List[str] = []
active_orders: Dict[str, Dict] = {}
user_orders: Dict[int, str] = {}
admin_state: Dict[int, str] = {}

config: Dict = {}
users_db: List[int] = []
users_set: set = set()
users_dirty: bool = False
bot_data: Dict = {}
data_dirty: bool = False

REQUIRED_CHATS = CHANNELS + [GROUP_ID]

# ================= DATA HELPERS =================
def load_json(file: str, default):
    if not os.path.exists(file):
        return default
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"Loaded {file}: {len(data) if isinstance(data, (list, dict)) else 'data'}")
            return data
    except Exception as e:
        logger.error(f"Error loading {file}: {e}")
        return default

def save_json(file: str, data):
    temp_file = f"{file}.tmp"
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, file)
        logger.info(f"Saved {file}")
        return True
    except Exception as e:
        logger.error(f"Error saving {file}: {e}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass
        return False

# ================= INITIALIZATION =================
def normalize_stats() -> None:
    global bot_data
    if not bot_data:
        return

    current_date = datetime.now(timezone.utc).date().isoformat()
    stats = bot_data.setdefault("stats", {})
    last_date = stats.get("today_date")
    if last_date != current_date:
        stats["today_date"] = current_date
        stats["today_numbers"] = 0
        stats["today_otps"] = 0
        save_json(DATA_FILE, bot_data)


def init_data():
    global config, users_db, users_set, bot_data
    config = load_json(CONFIG_FILE, {"range": "99298XXX", "country": "Tajikistan", "code": "+992"})
    users_db = load_json(USERS_FILE, [])
    users_set = set(users_db)
    bot_data = load_json(DATA_FILE, {
        "users": {},
        "banned": [],
        "stats": {
            "total_numbers": 0,
            "total_otps": 0,
            "requests": 0,
            "today_numbers": 0,
            "today_otps": 0,
            "today_date": datetime.now(timezone.utc).date().isoformat()
        },
        "range": config.get("range", "99298XXX"),
        "country": config.get("country", "Tajikistan"),
        "active_requests": {}
    })
    users_set = set(users_db)
    normalize_stats()
    logger.info(f"Initialized: {len(users_db)} users, Country: {config['country']}")

init_data()

# ================= COUNTRY PRESETS =================
PRESETS = {
    "TJ": {"name": "Tajikistan", "code": "+992", "range": "99298XXX", "flag": "🇹🇯"},
    "CM": {"name": "Cameroon", "code": "+237", "range": "2376XXXXX", "flag": "🇨🇲"},
    "BD": {"name": "Bangladesh", "code": "+880", "range": "88017XXXXX", "flag": "🇧🇩"},
    "RU": {"name": "Russia", "code": "+7", "range": "79XXXXXXX", "flag": "🇷🇺"},
}

# ================= SUBSCRIPTION CHECKER =================
async def is_subscribed(bot, user_id: int) -> bool:
    for chat in REQUIRED_CHATS:
        try:
            member = await bot.get_chat_member(chat_id=chat, user_id=user_id)
            if member.status in ['left', 'kicked']:
                return False
        except BadRequest:
            return False
        except TelegramError as e:
            logger.error(f"Error checking subscription for {user_id} in {chat}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking subscription: {e}")
            return False
    return True

# ================= USER MANAGEMENT =================
def register_user(user_id: int) -> bool:
    global users_dirty
    if user_id not in users_set:
        users_set.add(user_id)
        users_db.append(user_id)
        users_dirty = True
        logger.info(f"New user registered: {user_id}")
        return True
    return False

def get_user_info_string(user: User) -> Tuple[str, str]:
    username = f"@{user.username}" if user.username else "None"
    full_name = user.first_name or "Unknown"
    if user.last_name:
        full_name += f" {user.last_name}"
    full_name = full_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return full_name, username

def is_banned(user_id: int) -> bool:
    return str(user_id) in [str(x) for x in bot_data.get("banned", [])]

def ban_user(user_id: int) -> bool:
    global data_dirty
    banned = bot_data.setdefault("banned", [])
    user_str = str(user_id)
    if user_str not in banned:
        banned.append(user_str)
        data_dirty = True
        save_json(DATA_FILE, bot_data)
        return True
    return False

def unban_user(user_id: int) -> bool:
    global data_dirty
    banned = bot_data.setdefault("banned", [])
    user_str = str(user_id)
    if user_str in banned:
        banned.remove(user_str)
        data_dirty = True
        save_json(DATA_FILE, bot_data)
        return True
    return False

def get_user_record(user: User) -> Dict:
    uid = str(user.id)
    users = bot_data.setdefault("users", {})
    full_name, username = get_user_info_string(user)
    record = users.setdefault(uid, {
        "name": full_name,
        "username": username,
        "otp_count": 0,
        "last_seen": datetime.now(timezone.utc).isoformat()
    })
    record["name"] = full_name
    record["username"] = username
    record["last_seen"] = datetime.now(timezone.utc).isoformat()
    return record

def record_user_activity(user: User, action: str) -> None:
    global data_dirty
    record = get_user_record(user)
    stats = bot_data.setdefault("stats", {})
    if action == "request":
        stats["requests"] = stats.get("requests", 0) + 1
        data_dirty = True
    elif action == "number":
        stats["total_numbers"] = stats.get("total_numbers", 0) + 1
        stats["today_numbers"] = stats.get("today_numbers", 0) + 1
        data_dirty = True
    elif action == "otp":
        record["otp_count"] = int(record.get("otp_count", 0)) + 1
        stats["total_otps"] = stats.get("total_otps", 0) + 1
        stats["today_otps"] = stats.get("today_otps", 0) + 1
        data_dirty = True

async def background_save_task():
    global users_dirty, data_dirty
    while True:
        await asyncio.sleep(15)
        if users_dirty:
            try:
                await asyncio.to_thread(save_json, USERS_FILE, users_db)
                users_dirty = False
                logger.info(f"Batch saved {len(users_db)} users to disk")
            except Exception as e:
                logger.error(f"Error saving users: {e}")
        if data_dirty:
            try:
                await asyncio.to_thread(save_json, DATA_FILE, bot_data)
                data_dirty = False
                logger.info("Batch saved bot data to disk")
            except Exception as e:
                logger.error(f"Error saving bot data: {e}")

# ================= KEYBOARD BUILDERS =================
def get_start_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📲 Get Number", callback_data="get_new_number"),
            InlineKeyboardButton("⭕ OTP - Group", url=GROUP_LINK)
        ]
    ])

def get_join_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Channel 1", url="https://t.me/fastaccountapk_official")],
        [InlineKeyboardButton("Channel 2", url="https://t.me/fast_account_updates")],
        [InlineKeyboardButton("Group", url=GROUP_LINK)],
        [InlineKeyboardButton("✅ I Joined", callback_data="check_join")]
    ])

def get_waiting_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📲 New Number", callback_data="get_new_number"),
            InlineKeyboardButton("⭕ OTP - Group", url=GROUP_LINK)
        ]
    ])

def get_otp_received_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📲 New Number", callback_data="get_new_number"),
            InlineKeyboardButton("⭕ OTP - Group", url=GROUP_LINK)
        ]
    ])

def get_group_otp_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Channel", url="https://t.me/fast_account_updates"),
         InlineKeyboardButton("OTP - BOT", url=f"https://t.me/{BOT_USERNAME}")]
    ])

def get_admin_dashboard_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Refresh Stats", callback_data="admin_refresh")],
        [InlineKeyboardButton("⚙️ Quick Config Setup", callback_data="admin_presets")],
        [InlineKeyboardButton("� Ban User", callback_data="admin_ban"), InlineKeyboardButton("✅ Unban User", callback_data="admin_unban")],
        [InlineKeyboardButton("�📜 View User Logs", callback_data="admin_logs")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")]
    ])

def get_presets_kb() -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for key, data in PRESETS.items():
        row.append(InlineKeyboardButton(
            f"{data['flag']} {data['name']}",
            callback_data=f"setpreset_{key}"
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("🔙 Back to Dashboard", callback_data="admin_refresh")])
    return InlineKeyboardMarkup(buttons)

# ================= PHONE NUMBER FORMATTING =================
def format_phone_number(api_number: str, country_code: str) -> str:
    clean_number = str(api_number).lstrip("+")
    clean_code = country_code.lstrip("+")
    if clean_number.startswith(clean_code):
        return "+" + clean_number
    else:
        return "+" + clean_code + clean_number

def hide_phone_number(phone: str) -> str:
    if len(phone) > 7:
        return phone[:5] + "***" + phone[-2:]
    return phone

def sanitize_html(text: str) -> str:
    return (str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;"))

async def api_request(method: str, endpoint: str, json_payload: Optional[Dict] = None, timeout_sec: int = REQUEST_TIMEOUT) -> Optional[Dict]:
    if not bot_session:
        logger.error("Bot session not initialized")
        return None

    url = f"{BASE_URL}{endpoint}"
    req_timeout = aiohttp.ClientTimeout(total=timeout_sec)

    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            async with bot_session.request(
                method,
                url,
                json=json_payload,
                headers=HEADERS,
                timeout=req_timeout
            ) as r:
                raw_text = await r.text()
                if r.status != 200:
                    logger.warning("API %s %s returned %s: %s", method, url, r.status, raw_text)
                    if attempt < MAX_API_RETRIES:
                        await asyncio.sleep(2)
                        continue
                    return None

                try:
                    return json.loads(raw_text)
                except Exception as e:
                    logger.error("Invalid JSON response from %s: %s | %s", url, e, raw_text)
                    return None

        except asyncio.TimeoutError:
            logger.warning("Request timeout on %s %s attempt %d", method, url, attempt)
        except aiohttp.ClientError as e:
            logger.warning("API client error on %s %s attempt %d: %s", method, url, attempt, e)
        except Exception as e:
            logger.error("Unexpected API error on %s %s: %s", method, url, e)

        if attempt < MAX_API_RETRIES:
            await asyncio.sleep(2)

    return None

async def cancel_active_user_order(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    num_id = user_orders.pop(user_id, None)
    if not num_id:
        return False

    order = active_orders.pop(str(num_id), None)
    if not order:
        return False

    task = order.get("task")
    if task and not task.done():
        task.cancel()

    chat_id = order.get("chat_id")
    msg_id = order.get("msg_id")
    if chat_id and msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except BadRequest:
            pass
        except Exception as e:
            logger.warning(f"Unable to delete old number message: {e}")

    return True

# ================= API OPERATIONS =================
async def fetch_available_number(timeout_sec: int = INITIAL_FETCH_TIMEOUT) -> Optional[Tuple[str, str, int]]:
    payload = {"range": config["range"], "format": "national"}
    start_time = time.time()

    while time.time() - start_time < timeout_sec:
        data = await api_request("POST", "/api/v1/numbers/get", json_payload=payload, timeout_sec=REQUEST_TIMEOUT)
        if not data:
            await asyncio.sleep(2)
            continue

        num_id = data.get("number_id")
        if not num_id:
            logger.warning("API response missing number_id: %s", data)
            await asyncio.sleep(2)
            continue

        api_number = str(data.get("number") or "")
        expires_in = data.get("expires_in", 1200)
        try:
            expires_mins = int(float(expires_in)) // 60
        except (ValueError, TypeError):
            expires_mins = 20

        logger.info(f"Number fetched: {num_id}, expires in {expires_mins} mins")
        return api_number, num_id, expires_mins

    return None

async def fetch_otp_sms(num_id: str) -> Optional[Dict]:
    return await api_request("GET", f"/api/v1/numbers/{num_id}/sms", timeout_sec=5)

# ================= MESSAGE BUILDERS =================
def build_number_message(phone: str) -> str:
    return f"✅ Number - <code>{sanitize_html(phone)}</code>"

def build_otp_message(phone: str, otp: str) -> str:
    return (
        f"✅ Number - <code>{sanitize_html(phone)}</code>\n"
        f"⭕ OTP - <code>{sanitize_html(otp)}</code>"
    )


def build_admin_number_log(user_id: int, full_name: str, username: str, phone: str, country: str) -> str:
    return (
        f"🔰 {full_name} | {username}\n"
        f"🔰 Number - <code>{sanitize_html(phone)}</code>"
    )

def build_admin_otp_log(user_id: int, full_name: str, username: str, phone: str, otp: str, api_msg: str, service: str) -> str:
    return (
        f"🔰 {full_name} | {username}\n"
        f"🔰 Number - <code>{sanitize_html(phone)}</code>\n"
        f"⭕ OTP - <code>{sanitize_html(otp)}</code>"
    )

def build_group_otp_message(full_name: str, user_id: int, hidden_phone: str, otp: str, api_msg: str, service: str) -> str:
    safe_msg = sanitize_html(api_msg)
    return (
        f"✅ Number - <code>{sanitize_html(hidden_phone)}</code>\n"
        f"⭕ OTP - <code>{sanitize_html(otp)}</code>\n\n"
        f"🗨️ <code>{safe_msg}</code>"
    )

# ================= CORE: GET NUMBER & POLL =================
async def do_get_number(context: ContextTypes.DEFAULT_TYPE, user: User):
    """Shared logic to fetch a number and start OTP polling"""
    register_user(user.id)

    if is_banned(user.id):
        await context.bot.send_message(
            user.id,
            "❌ <b>Access Denied!</b>\n\nYou are banned from using this bot.",
            parse_mode=ParseMode.HTML
        )
        return

    if not await is_subscribed(context.bot, user.id):
        await context.bot.send_message(
            user.id,
            "⚠️ Join channels first!",
            reply_markup=get_join_kb(),
            parse_mode=ParseMode.HTML
        )
        return

    record_user_activity(user, "request")
    await cancel_active_user_order(context, user.id)

    search_msg = await context.bot.send_message(
        user.id,
        "⏳ <i>Searching for an available number...</i>",
        parse_mode=ParseMode.HTML
    )

    try:
        result = await fetch_available_number(INITIAL_FETCH_TIMEOUT)

        if not result:
            try:
                await context.bot.delete_message(chat_id=search_msg.chat_id, message_id=search_msg.message_id)
            except Exception:
                pass
            await context.bot.send_message(
                user.id,
                "❌ <b>No available numbers right now. Please try again later.</b>",
                parse_mode=ParseMode.HTML
            )
            return

        api_number, num_id, expires_mins = result
        phone = format_phone_number(api_number, config["code"])

        record_user_activity(user, "number")

        # Send number info to user
        try:
            await context.bot.delete_message(chat_id=search_msg.chat_id, message_id=search_msg.message_id)
        except Exception:
            pass
        msg = build_number_message(phone)
        number_msg = await context.bot.send_message(
            user.id,
            msg,
            parse_mode=ParseMode.HTML,
            reply_markup=get_waiting_kb()
        )

        # Notify admin
        full_name, username = get_user_info_string(user)
        admin_log = build_admin_number_log(user.id, full_name, username, phone, config["country"])
        await context.bot.send_message(ADMIN_ID, admin_log, parse_mode=ParseMode.HTML)

        user_activity.append(
            f"UID: {user.id} | User: {username} | Num: {phone} at {time.strftime('%H:%M:%S')}"
        )

        # Start OTP polling task
        task = asyncio.create_task(
            poll_otp(context, user, num_id, phone, number_msg.message_id, number_msg.chat_id)
        )
        active_orders[str(num_id)] = {
            "task": task,
            "msg_id": number_msg.message_id,
            "user_id": user.id,
            "chat_id": number_msg.chat_id
        }
        user_orders[user.id] = str(num_id)

    except Exception as e:
        logger.error(f"Error in do_get_number: {e}")
        await context.bot.send_message(
            user.id,
            "❌ <b>Connection error.</b> Failed to reach the API.",
            parse_mode=ParseMode.HTML
        )

async def poll_otp(context, user: User, num_id: str, phone: str, msg_id: int, chat_id: int):
    seen_otps = set()

    try:
        for _ in range(POLL_TIMEOUT):  # 600 * 2s = ~20 mins
            await asyncio.sleep(POLL_INTERVAL)

            data = await fetch_otp_sms(num_id)
            if not data:
                continue

            otp = data.get("otp")
            if not otp or otp in seen_otps:
                continue

            seen_otps.add(otp)
            service = data.get("service", "Unknown")
            api_msg = data.get("message", "N/A")

            record_user_activity(user, "otp")

            msg = build_otp_message(phone, otp)
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=msg,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_otp_received_kb()
                )
            except Exception as e:
                logger.warning(f"Failed to edit OTP message for user {user.id}: {e}")
                try:
                    await context.bot.send_message(
                        user.id,
                        msg,
                        parse_mode=ParseMode.HTML,
                        reply_markup=get_otp_received_kb()
                    )
                except Exception as send_err:
                    logger.error(f"Failed to send OTP to user {user.id}: {send_err}")

            # Clean up active order
            if str(num_id) in active_orders:
                del active_orders[str(num_id)]
            user_orders.pop(user.id, None)

            # Notify admin
            full_name, username = get_user_info_string(user)
            admin_log = build_admin_otp_log(user.id, full_name, username, phone, otp, api_msg, service)
            await context.bot.send_message(ADMIN_ID, admin_log, parse_mode=ParseMode.HTML)

            # Send to group (with hidden phone)
            hidden_phone = hide_phone_number(phone)
            group_msg = build_group_otp_message(full_name, user.id, hidden_phone, otp, api_msg, service)

            async def send_group_msg():
                try:
                    await context.bot.send_message(
                        chat_id=GROUP_ID,
                        text=group_msg,
                        parse_mode=ParseMode.HTML,
                        reply_markup=get_group_otp_kb()
                    )
                except Exception as e:
                    logger.error(f"Group forward failed: {e}")

            asyncio.create_task(send_group_msg())
            return  # OTP received, stop polling

        # Timeout reached - no OTP in 20 mins
        if str(num_id) in active_orders:
            del active_orders[str(num_id)]
        user_orders.pop(user.id, None)

        await context.bot.send_message(
            user.id,
            f"❌ <b>OTP Timeout</b>\nNumber: <code>{phone}</code>\nStatus: No SMS received in 20 mins.",
            parse_mode=ParseMode.HTML
        )

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in poll_otp: {e}")

# ================= HANDLERS =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    register_user(user.id)
    if is_banned(user.id):
        return await update.message.reply_text(
            "❌ <b>Access Denied!</b>\n\nYou are banned from using this bot.",
            parse_mode=ParseMode.HTML
        )

    if not await is_subscribed(context.bot, user.id):
        return await update.message.reply_text(
            "❌ <b>Access Denied!</b>\n\nYou must join all required channels and the group to use this bot.",
            reply_markup=get_join_kb(),
            parse_mode=ParseMode.HTML
        )

    await update.message.reply_text(
        f"🚀 <b>Welcome {sanitize_html(user.first_name or 'User')}!</b>\n\nClick the button below to get an OTP number.",
        reply_markup=get_start_kb(),
        parse_mode=ParseMode.HTML
    )

async def get_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await do_get_number(context, user)

# ================= CALLBACK HANDLER =================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    try:
        if data == "check_join":
            if await is_subscribed(context.bot, query.from_user.id):
                if query.message:
                    await query.edit_message_text("✅ Verification successful! Send /start to begin.")
                else:
                    await context.bot.send_message(
                        chat_id=query.from_user.id,
                        text="✅ Verification successful! Send /start to begin.",
                        parse_mode=ParseMode.HTML
                    )
            else:
                await query.answer("⚠️ Please join all required chats first!", show_alert=True)

        elif data == "get_new_number":
            user = query.from_user
            await do_get_number(context, user)

        elif data == "admin_refresh":
            await admin_panel_update(query.message, context, edit=True)

        elif data == "admin_presets":
            if query.message:
                await query.edit_message_reply_markup(reply_markup=get_presets_kb())

        elif data.startswith("setpreset_"):
            key = data.split("_")[1]
            if key not in PRESETS:
                return
            pre = PRESETS[key]
            config.update({"country": pre["name"], "code": pre["code"], "range": pre["range"]})
            save_json(CONFIG_FILE, config)
            await query.answer(f"✅ Config updated to {pre['name']}")
            await admin_panel_update(query.message, context, edit=True)

        elif data == "admin_logs":
            logs = "\n".join(user_activity[-10:]) if user_activity else "No recent logs."
            msg = f"📜 <b>Recent Activity (Top 10):</b>\n\n<code>{logs}</code>"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_refresh")]])
            await query.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

        elif data == "admin_ban":
            admin_state[query.from_user.id] = "waiting_for_ban"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")]])
            await query.edit_message_text(
                "🚫 <b>Send the user ID to ban:</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )

        elif data == "admin_unban":
            admin_state[query.from_user.id] = "waiting_for_unban"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")]])
            await query.edit_message_text(
                "✅ <b>Send the user ID to unban:</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )

        elif data == "admin_broadcast":
            admin_state[query.from_user.id] = "waiting_for_broadcast"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")]])
            await query.edit_message_text(
                "📢 <b>Send the message (Text, Photo, File, Video) you want to broadcast:</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )

        elif data == "cancel_broadcast":
            if query.from_user.id in admin_state:
                del admin_state[query.from_user.id]
            await admin_panel_update(query.message, context, edit=True)

    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        try:
            await query.answer("❌ An error occurred", show_alert=True)
        except Exception:
            pass

# ================= ADMIN DASHBOARD =================
def format_top_users() -> str:
    users = bot_data.get("users", {})
    ranked = sorted(
        users.values(),
        key=lambda item: int(item.get("otp_count", 0)),
        reverse=True
    )[:5]
    if not ranked:
        return "No OTP records yet."

    lines = []
    for position, record in enumerate(ranked, start=1):
        username = record.get("username", "None")
        otp_count = record.get("otp_count", 0)
        lines.append(f"{position}. {username} — {otp_count} OTPs")
    return "\n".join(lines)

async def get_dashboard_text() -> str:
    stats = bot_data.get("stats", {})
    total_numbers = stats.get("total_numbers", 0)
    total_otps = stats.get("total_otps", 0)
    requests = stats.get("requests", 0)
    today_numbers = stats.get("today_numbers", 0)
    today_otps = stats.get("today_otps", 0)
    active_users = len(users_db)
    top_users = format_top_users()

    return (
        f"🛠 <b>Professional Admin Dashboard</b> 🛠\n\n"
        f"👥 <b>Total Users:</b> <code>{active_users}</code>\n"
        f"📊 <b>Total Numbers Taken:</b> <code>{total_numbers}</code>\n"
        f"🔑 <b>Total OTPs Received:</b> <code>{total_otps}</code>\n"
        f"🕒 <b>Total Requests:</b> <code>{requests}</code>\n\n"
        f"📈 <b>Today Stats:</b>\n"
        f"✅ Numbers: <code>{today_numbers}</code>\n"
        f"⭕ OTPs: <code>{today_otps}</code>\n\n"
        f"📌 <b>Active Configuration:</b>\n"
        f"🌍 Country: <b>{config['country']}</b>\n"
        f"📞 Code: <code>{config['code']}</code>\n"
        f"🔢 Range: <code>{config['range']}</code>\n\n"
        f"<b>Top Users Ranking:</b>\n{top_users}"
    )

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    msg = await update.message.reply_text("🔄 <i>Loading dashboard...</i>", parse_mode=ParseMode.HTML)
    await admin_panel_update(msg, context, edit=True)

async def admin_panel_update(message, context, edit: bool = False):
    text = await get_dashboard_text()
    kb = get_admin_dashboard_kb()
    try:
        if edit and message is not None:
            await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif message is not None:
            await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif hasattr(context, 'bot'):
            await context.bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception as e:
        logger.error(f"Error updating admin panel: {e}")

# ================= MESSAGE HANDLER =================
async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        return

    state = admin_state.get(user_id)
    if not state:
        return

    if state == "waiting_for_broadcast":
        admin_state.pop(user_id, None)
        if not update.message:
            return

        msg = await update.message.reply_text("⏳ <i>Starting broadcast...</i>", parse_mode=ParseMode.HTML)
        success = 0
        failed = 0

        for uid in users_db:
            try:
                if update.message.reply_to_message:
                    await context.bot.copy_message(
                        chat_id=uid,
                        from_chat_id=update.message.chat_id,
                        message_id=update.message.reply_to_message.message_id
                    )
                elif update.message.text and not any([
                    update.message.photo,
                    update.message.document,
                    update.message.video,
                    update.message.animation,
                    update.message.audio,
                    update.message.voice,
                    update.message.sticker,
                    update.message.video_note,
                ]):
                    await context.bot.send_message(
                        chat_id=uid,
                        text=update.message.text,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await context.bot.copy_message(
                        chat_id=uid,
                        from_chat_id=update.message.chat_id,
                        message_id=update.message.message_id
                    )
                success += 1
                await asyncio.sleep(BROADCAST_DELAY)
            except Exception as e:
                failed += 1
                logger.warning(f"Failed to broadcast to {uid}: {e}")

        await msg.edit_text(
            f"✅ <b>Broadcast Complete!</b>\n"
            f"✅ Sent: <code>{success}/{len(users_db)}</code>\n"
            f"❌ Failed: <code>{failed}</code>",
            parse_mode=ParseMode.HTML
        )
        return

    if state == "waiting_for_ban":
        admin_state.pop(user_id, None)
        if not update.message or not update.message.text:
            return
        target_id = None
        for token in reversed(update.message.text.strip().split()):
            if token.lstrip("-").isdigit():
                target_id = int(token)
                break
        if not target_id:
            return await update.message.reply_text(
                "❌ <b>Invalid user ID.</b> Send a numeric ID to ban.",
                parse_mode=ParseMode.HTML
            )
        if ban_user(target_id):
            await update.message.reply_text(
                f"✅ <b>User banned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"⚠️ <b>User is already banned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
        return

    if state == "waiting_for_unban":
        admin_state.pop(user_id, None)
        if not update.message or not update.message.text:
            return
        target_id = None
        for token in reversed(update.message.text.strip().split()):
            if token.lstrip("-").isdigit():
                target_id = int(token)
                break
        if not target_id:
            return await update.message.reply_text(
                "❌ <b>Invalid user ID.</b> Send a numeric ID to unban.",
                parse_mode=ParseMode.HTML
            )
        if unban_user(target_id):
            await update.message.reply_text(
                f"✅ <b>User unbanned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"⚠️ <b>User was not banned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
        return

# ================= CONFIG COMMANDS =================
async def admin_set_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    args = context.args
    if len(args) < 3:
        usage_msg = (
            "❌ <b>Invalid Format!</b>\n\n"
            "<b>Usage:</b> <code>/set &lt;Country&gt; &lt;Code&gt; &lt;Range&gt;</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/set Bangladesh +880 88017XXXXX</code>"
        )
        return await update.message.reply_text(usage_msg, parse_mode=ParseMode.HTML)

    try:
        new_range = args[-1]
        new_code = args[-2]
        new_country = " ".join(args[:-2])

        if not new_code.startswith("+"):
            new_code = "+" + new_code

        config.update({"country": new_country, "code": new_code, "range": new_range})
        save_json(CONFIG_FILE, config)

        success_msg = (
            "✅ <b>Configuration Updated Successfully!</b>\n\n"
            f"🌍 <b>Country:</b> <code>{new_country}</code>\n"
            f"📞 <b>Code:</b> <code>{new_code}</code>\n"
            f"🔢 <b>Range:</b> <code>{new_range}</code>\n\n"
            "<i>All new user requests will now use this config.</i>"
        )
        await update.message.reply_text(success_msg, parse_mode=ParseMode.HTML)
        logger.info(f"Config updated: {new_country}")

    except Exception as e:
        logger.error(f"Error setting config: {e}")
        await update.message.reply_text(
            f"❌ <b>Error occurred:</b> {e}",
            parse_mode=ParseMode.HTML
        )

async def admin_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        return await update.message.reply_text(
            "⚠️ <b>Usage:</b> <code>/ban &lt;user_id&gt;</code>",
            parse_mode=ParseMode.HTML
        )
    try:
        target_id = int(context.args[0])
        if ban_user(target_id):
            await update.message.reply_text(
                f"✅ <b>User banned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"⚠️ <b>User is already banned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
    except ValueError:
        await update.message.reply_text(
            "❌ <b>Invalid user ID.</b>",
            parse_mode=ParseMode.HTML
        )

async def admin_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        return await update.message.reply_text(
            "⚠️ <b>Usage:</b> <code>/unban &lt;user_id&gt;</code>",
            parse_mode=ParseMode.HTML
        )
    try:
        target_id = int(context.args[0])
        if unban_user(target_id):
            await update.message.reply_text(
                f"✅ <b>User unbanned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"⚠️ <b>User was not banned:</b> <code>{target_id}</code>",
                parse_mode=ParseMode.HTML
            )
    except ValueError:
        await update.message.reply_text(
            "❌ <b>Invalid user ID.</b>",
            parse_mode=ParseMode.HTML
        )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    if is_banned(user.id):
        return await update.message.reply_text(
            "❌ <b>You are banned from using this bot.</b>",
            parse_mode=ParseMode.HTML
        )
    order_id = user_orders.get(user.id)
    if not order_id:
        return await update.message.reply_text(
            "ℹ️ <b>No active number request found.</b>",
            parse_mode=ParseMode.HTML
        )
    order = active_orders.get(str(order_id))
    if not order:
        return await update.message.reply_text(
            "ℹ️ <b>No active number request found.</b>",
            parse_mode=ParseMode.HTML
        )
    return await update.message.reply_text(
        f"✅ <b>Active Request:</b> <code>{order_id}</code>",
        parse_mode=ParseMode.HTML
    )

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    message = " ".join(context.args) if context.args else ""
    if not message and not update.message.reply_to_message:
        return await update.message.reply_text(
            "⚠️ <b>Usage:</b> <code>/broadcast &lt;text&gt;</code> or reply to a message with <code>/broadcast</code>",
            parse_mode=ParseMode.HTML
        )

    msg = await update.message.reply_text("⏳ <i>Starting broadcast...</i>", parse_mode=ParseMode.HTML)
    success = 0
    failed = 0

    for uid in users_db:
        try:
            if update.message.reply_to_message:
                await context.bot.copy_message(
                    chat_id=uid,
                    from_chat_id=update.message.chat_id,
                    message_id=update.message.reply_to_message.message_id
                )
            else:
                await context.bot.send_message(
                    chat_id=uid,
                    text=message,
                    parse_mode=ParseMode.HTML
                )
            success += 1
            await asyncio.sleep(BROADCAST_DELAY)
        except Exception as e:
            failed += 1
            logger.warning(f"Failed to broadcast to {uid}: {e}")

    await msg.edit_text(
        f"✅ <b>Broadcast Complete!</b>\n"
        f"✅ Sent: <code>{success}/{len(users_db)}</code>\n"
        f"❌ Failed: <code>{failed}</code>",
        parse_mode=ParseMode.HTML
    )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)

# ================= LIFECYCLE HOOKS =================
async def post_init(application: Application):
    global bot_session
    try:
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        bot_session = aiohttp.ClientSession(connector=connector)
        logger.info("Bot session initialized")

        task = asyncio.create_task(background_save_task())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        logger.info("Background tasks started")
    except Exception as e:
        logger.error(f"Error in post_init: {e}")

async def post_shutdown(application: Application):
    global users_dirty, bot_session
    try:
        # Cancel all active polling tasks
        for order in active_orders.values():
            task = order.get("task")
            if task and not task.done():
                task.cancel()
        active_orders.clear()

        if bot_session:
            await bot_session.close()
            logger.info("Bot session closed")

        if users_dirty:
            save_json(USERS_FILE, users_db)
            logger.info("Final user data saved")
    except Exception as e:
        logger.error(f"Error in post_shutdown: {e}")

# ================= MAIN =================
def main():
    try:
        app = (Application.builder()
            .token(BOT_TOKEN)
            .read_timeout(30)
            .write_timeout(30)
            .post_init(post_init)
            .post_shutdown(post_shutdown)
            .build())

        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("admin", admin_panel))
        app.add_handler(CommandHandler("dashboard", admin_panel))
        app.add_handler(CommandHandler("set", admin_set_config))
        app.add_handler(CommandHandler("ban", admin_ban))
        app.add_handler(CommandHandler("unban", admin_unban))
        app.add_handler(CommandHandler("status", status))
        app.add_handler(CommandHandler("broadcast", broadcast))
        app.add_handler(MessageHandler(filters.Regex("^📲 Get Number$"), get_number))
        app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_admin_messages))
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_error_handler(error_handler)

        logger.info("=" * 50)
        logger.info("🚀 Bot is running...")
        logger.info("=" * 50)
        app.run_polling(drop_pending_updates=True)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
