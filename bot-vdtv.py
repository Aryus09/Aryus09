import requests
import base64
import hmac
import hashlib
import json
import random
import secrets
import uuid
import time
import os
import re
import threading
from datetime import datetime, timedelta
from Crypto.Cipher import AES
from faker import Faker
import logging
from pymongo import MongoClient
import qrcode
from io import BytesIO

import telebot
from telebot import types
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import banner config (jika file ada)
try:
    from config_banner import (
        ACTIVE_BANNER, 
        BOT_DISPLAY_NAME, 
        BOT_VERSION, 
        BOT_DESCRIPTION,
        BANNER_COLOR,
        INFO_COLOR,
        SUCCESS_COLOR,
        WARNING_COLOR
    )
    USE_CUSTOM_BANNER = True
except ImportError:
    USE_CUSTOM_BANNER = False
    ACTIVE_BANNER = r"""
╦  ╦   ╦╔╦╗╦╔═╗╔╦╗╦  ╦
╚╗╔╝───║ ║║║║ ║ ║ ╚╗╔╝  By @etmintkuh
 ╚╝    ╩═╩╝╩╚═╝ ╩  ╚╝ 
    """
    BOT_DISPLAY_NAME = "Vidiotv Premium Bot"
    BOT_VERSION = "2.1 Auto Bot"
    BOT_DESCRIPTION = "By @etmintkuh"
    BANNER_COLOR = "RED"
    INFO_COLOR = "CYAN"
    SUCCESS_COLOR = "GREEN"
    WARNING_COLOR = "YELLOW"

file_lock = threading.Lock()
payment_checkers_lock = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MONGO_URI = "mongodb://admin:12345678@127.0.0.1:27017/botdb?authSource=admin"
BOT_TOKEN = "8562610830:AAFSqblLH1IN0781Pkza0z3VTMCmzTLO8bY"
OWNER_ID = 5465056934

# Logger Telegram Group ID (optional)
# Format: -100xxxxxxxxxx untuk supergroup atau -xxxxxxxxx untuk group biasa
# Kosongkan jika tidak ingin menggunakan logger ke grup
LOG_GROUP_ID = os.getenv("LOG_GROUP_ID", "-1003370284743")
if LOG_GROUP_ID:
    try:
        LOG_GROUP_ID = int(LOG_GROUP_ID)
    except ValueError:
        logger.warning("LOG_GROUP_ID tidak valid, logger ke grup dinonaktifkan")
        LOG_GROUP_ID = None
else:
    LOG_GROUP_ID = None

# Proxy Configuration - dapat diambil dari environment variables
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "f26532e2b76b18b911b8")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "237fcb92481fe8bb")
PROXY_HOST = os.getenv("PROXY_HOST", "gw.dataimpulse.com")
PROXY_PORT = os.getenv("PROXY_PORT", "823")

# Validasi environment variables yang wajib
if not MONGO_URI:
    logger.error("MONGO_URI environment variable is not set!")
    logger.error("Please set MONGO_URI in your panel environment variables.")
    logger.error("Example: mongodb://username:password@host:port/database")
    exit(1)

if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is not set!")
    logger.error("Please set BOT_TOKEN in your panel environment variables.")
    exit(1)

STATIC_QRIS_STRING = ""

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Test connection
    mongo_client.server_info()
    db = mongo_client['vidio_bot']
    DB_NAME = 'vidio_bot'
    users_collection = db['users']
    transactions_collection = db['transactions']
    custom_passwords_collection = db['custom_passwords']
    usage_tracking_collection = db['usage_tracking']
    settings_collection = db['settings']
    logger.info("✓ MongoDB connected successfully")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    logger.error(f"MONGO_URI: {MONGO_URI[:20]}..." if len(MONGO_URI) > 20 else f"MONGO_URI: {MONGO_URI}")
    logger.error("Please check your MongoDB URI and ensure the database is accessible.")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# Custom exception handler untuk retry on 502 errors
def exception_handler(exception):
    """Handle Telegram API exceptions with retry"""
    if isinstance(exception, telebot.apihelper.ApiTelegramException):
        error_code = exception.error_code
        
        # Handle specific error codes
        if error_code == 502:  # Bad Gateway
            logger.warning(f"[Telegram API] 502 Bad Gateway - Telegram server issue (will retry)")
            return True  # Return True to retry
        elif error_code == 429:  # Too Many Requests
            logger.warning(f"[Telegram API] 429 Too Many Requests - Rate limited (will retry)")
            return True
        elif error_code in [500, 503]:  # Internal Server Error, Service Unavailable
            logger.warning(f"[Telegram API] {error_code} Server Error - Telegram issue (will retry)")
            return True
        else:
            logger.error(f"[Telegram API] Error {error_code}: {exception.description}")
            return False
    
    # Log other exceptions
    logger.error(f"[Bot Exception] {type(exception).__name__}: {exception}")
    return False

# Set custom exception handler
bot.exception_handler = exception_handler

# Enable auto retry for failed requests
telebot.apihelper.RETRY_ON_ERROR = True
telebot.apihelper.RETRY_TIMEOUT = 5  # Wait 5 seconds before retry
telebot.apihelper.MAX_RETRIES = 3    # Max 3 retries


# ==================== SAFE MESSAGE SENDING ====================

def safe_send_message(chat_id, text, **kwargs):
    """Send message with auto retry on errors"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            return bot.send_message(chat_id, text, **kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code in [502, 500, 503, 429]:  # Retryable errors
                if attempt < max_retries - 1:
                    logger.warning(f"[Send Message] Error {e.error_code}, retry {attempt + 1}/{max_retries}")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"[Send Message] Failed after {max_retries} retries: {e}")
                    raise
            else:
                logger.error(f"[Send Message] Non-retryable error: {e}")
                raise
        except Exception as e:
            logger.error(f"[Send Message] Unexpected error: {e}")
            raise
    
    return None


def safe_send_document(chat_id, document, **kwargs):
    """Send document with auto retry on errors"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            return bot.send_document(chat_id, document, **kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code in [502, 500, 503, 429]:
                if attempt < max_retries - 1:
                    logger.warning(f"[Send Document] Error {e.error_code}, retry {attempt + 1}/{max_retries}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"[Send Document] Failed after {max_retries} retries: {e}")
                    raise
            else:
                logger.error(f"[Send Document] Non-retryable error: {e}")
                raise
        except Exception as e:
            logger.error(f"[Send Document] Unexpected error: {e}")
            raise
    
    return None

# ==================== END SAFE MESSAGE SENDING ====================

payment_checkers = {}

# ==================== ROLE MANAGEMENT SYSTEM ====================

ROLE_OWNER = "owner"
ROLE_SUPER_ADMIN = "super_admin"
ROLE_ADMIN = "admin"
ROLE_MEMBER = "member"
ROLE_FREE_USER = "free_user"

DAILY_LIMITS = {
    ROLE_OWNER: float('inf'),
    ROLE_SUPER_ADMIN: float('inf'),
    ROLE_ADMIN: 2000,
    ROLE_MEMBER: 1000,
    ROLE_FREE_USER: 1
}

COOLDOWN_THRESHOLD = 20
COOLDOWN_DURATION = 150

def get_user_role(user_id):
    """Get user role from database"""
    if is_owner(user_id):
        return ROLE_OWNER
    
    user = users_collection.find_one({"user_id": user_id})
    if user:
        return user.get("role", ROLE_FREE_USER)  # Default: Free User
    return ROLE_FREE_USER  # Default untuk user baru

def set_user_role(user_id, role):
    """Set user role in database"""
    valid_roles = [ROLE_OWNER, ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_MEMBER]
    if role not in valid_roles:
        return False
    
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {"role": role, "role_updated_at": datetime.now()}},
        upsert=True
    )
    logger.info(f"[Role] User {user_id} role set to {role}")
    return True

def delete_user_role(user_id):
    """Delete/reset user role back to member"""
    users_collection.update_one(
        {"user_id": user_id},
        {"$unset": {"role": ""}}
    )
    logger.info(f"[Role] User {user_id} role reset to member")
    return True

def get_role_display_name(role):
    """Get display name for role"""
    role_names = {
        ROLE_OWNER: "👑 Owner",
        ROLE_SUPER_ADMIN: "⭐ Super Admin",
        ROLE_ADMIN: "🔧 Admin",
        ROLE_MEMBER: "👤 Member",
        ROLE_FREE_USER: "🆓 Free User"
    }
    return role_names.get(role, "🆓 Free User")

def get_daily_usage(user_id):
    """Get today's usage count for user"""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    usage = usage_tracking_collection.find_one({
        "user_id": user_id,
        "date": today
    })
    
    if usage:
        return usage.get("count", 0)
    return 0

def get_total_lifetime_usage(user_id):
    """Get total lifetime usage for free users"""
    total = 0
    usages = usage_tracking_collection.find({"user_id": user_id})
    for usage in usages:
        total += usage.get("count", 0)
    return total

def increment_daily_usage(user_id, count=1):
    """Increment daily usage counter"""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    usage_tracking_collection.update_one(
        {
            "user_id": user_id,
            "date": today
        },
        {
            "$inc": {"count": count},
            "$set": {"last_updated": datetime.now()}
        },
        upsert=True
    )
    
    logger.info(f"[Usage] User {user_id} daily usage incremented by {count}")

def get_cooldown_status(user_id):
    """Check if user is in cooldown period"""
    cooldown_doc = usage_tracking_collection.find_one({
        "user_id": user_id,
        "cooldown_until": {"$exists": True}
    })
    
    if cooldown_doc:
        cooldown_until = cooldown_doc.get("cooldown_until")
        if cooldown_until and cooldown_until > datetime.now():
            remaining = int((cooldown_until - datetime.now()).total_seconds())
            return True, remaining
    
    return False, 0

def set_cooldown(user_id, duration=COOLDOWN_DURATION):
    """Set cooldown for user"""
    cooldown_until = datetime.now() + timedelta(seconds=duration)
    
    usage_tracking_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "cooldown_until": cooldown_until,
                "cooldown_set_at": datetime.now()
            }
        },
        upsert=True
    )
    
    logger.info(f"[Cooldown] User {user_id} cooldown set until {cooldown_until}")

def get_session_count(user_id):
    """Get current session account creation count"""
    session = usage_tracking_collection.find_one({"user_id": user_id})
    
    if session:
        count = session.get("session_count", 0)
        logger.debug(f"[Get Session Count] User {user_id}: {count}")
        return count
    
    logger.debug(f"[Get Session Count] User {user_id}: No session data, returning 0")
    return 0

def increment_session_count(user_id, count=1):
    """Increment session account counter by count"""
    # Update dengan upsert untuk ensure document exists
    result = usage_tracking_collection.update_one(
        {"user_id": user_id},
        {
            "$inc": {"session_count": count},
            "$set": {"last_action": datetime.now()},
            "$setOnInsert": {"session_start": datetime.now()}
        },
        upsert=True
    )
    
    # Force immediate write
    logger.info(f"[Session Increment] User {user_id} - Incremented by {count}")
    logger.info(f"[Session Increment] Modified: {result.modified_count}, Upserted: {result.upserted_id}")
    
    # Immediate verify
    time.sleep(0.1)  # Small delay for database write
    verify_count = get_session_count(user_id)
    logger.info(f"[Session Verify] User {user_id} - Current session count: {verify_count}")
    
    return verify_count

def reset_session_count(user_id):
    """Reset session counter (after cooldown)"""
    usage_tracking_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "session_count": 0,
                "session_start": datetime.now()
            }
        }
    )

def can_create_accounts(user_id, requested_count):
    """Check if user can create requested number of accounts"""
    role = get_user_role(user_id)
    daily_limit = DAILY_LIMITS.get(role, 0)
    
    in_cooldown, remaining_time = get_cooldown_status(user_id)
    if in_cooldown:
        minutes = remaining_time // 60
        seconds = remaining_time % 60
        return False, f"⏳ Cooldown aktif. Tunggu {minutes}m {seconds}s", 0
    
    if role in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        return True, "OK", float('inf')
    
    if role == ROLE_FREE_USER:
        lifetime_usage = get_total_lifetime_usage(user_id)
        remaining = daily_limit - lifetime_usage
        
        if remaining <= 0:
            return False, f"❌ Limit seumur hidup habis! Anda sudah membuat {lifetime_usage} akun.", 0
        
        if requested_count > remaining:
            return False, f"❌ Melebihi limit! Sisa quota: {remaining} akun (total seumur hidup)", remaining
        
        return True, "OK", remaining
    
    current_usage = get_daily_usage(user_id)
    remaining = daily_limit - current_usage
    
    if remaining <= 0:
        return False, f"❌ Limit harian habis! Limit: {daily_limit} akun/hari", 0
    
    if requested_count > remaining:
        return False, f"❌ Melebihi limit harian! Sisa quota: {remaining} akun", remaining
    
    return True, "OK", remaining


# ==================== ACCESS CONTROL ====================

def is_owner(user_id):
    """Check if user is owner"""
    return user_id == OWNER_ID

def has_owner_access(user_id):
    """Check if user has owner or super admin privileges"""
    if is_owner(user_id):
        return True
    role = get_user_role(user_id)
    return role == ROLE_SUPER_ADMIN

def get_user_subscription(user_id):
    """Get user subscription if active"""
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return None
    
    if user.get('expire_date'):
        expire_date = user['expire_date']
        if expire_date > datetime.now():
            return user
    return None


# ==================== CUSTOM PASSWORD MANAGEMENT ====================

def is_valid_custom_password(password):
    """Validate custom password"""
    if len(password) < 8 or len(password) > 20:
        return False, "Password harus 8-20 karakter"
    
    if not re.search(r'[A-Za-z]', password):
        return False, "Password harus mengandung huruf"
    
    if not re.search(r'[0-9]', password):
        return False, "Password harus mengandung angka"
    
    if not re.match(r'^[A-Za-z0-9!@#$%^&*()_+\-=\[\]{}|;:,.<>?]+$', password):
        return False, "Password mengandung karakter tidak valid"
    
    return True, "Password valid"

def save_custom_password(user_id, password, mode="same"):
    """Save custom password"""
    custom_passwords_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "password": password,
                "mode": mode,
                "updated_at": datetime.now()
            }
        },
        upsert=True
    )
    logger.info(f"[CustomPassword] Password saved for user {user_id} (mode: {mode})")

def get_custom_password(user_id):
    """Get custom password"""
    doc = custom_passwords_collection.find_one({"user_id": user_id})
    if doc:
        return {
            "password": doc.get("password"),
            "mode": doc.get("mode", "same")
        }
    return None

def delete_custom_password(user_id):
    """Delete custom password"""
    result = custom_passwords_collection.delete_one({"user_id": user_id})
    if result.deleted_count > 0:
        logger.info(f"[CustomPassword] Password deleted for user {user_id}")
        return True
    return False


# ==================== SETTINGS MANAGEMENT ====================

def get_bot_settings():
    """Get bot settings"""
    settings = settings_collection.find_one({"_id": "bot_settings"})
    if not settings:
        # Default settings
        settings = {
            "_id": "bot_settings",
            "max_accounts_per_request": 10,
            "cooldown_threshold": 20,
            "cooldown_duration": 150,
            "email_mode": "random"  # "random" atau "name"
        }
        settings_collection.insert_one(settings)
    
    # Ensure email_mode exists (for existing databases)
    if "email_mode" not in settings:
        settings["email_mode"] = "random"
        settings_collection.update_one(
            {"_id": "bot_settings"},
            {"$set": {"email_mode": "random"}}
        )
    
    return settings

def update_bot_setting(key, value):
    """Update bot setting"""
    settings_collection.update_one(
        {"_id": "bot_settings"},
        {"$set": {key: value, "updated_at": datetime.now()}},
        upsert=True
    )
    logger.info(f"[Settings] Updated {key} = {value}")


# ==================== TELEGRAM LOGGER FUNCTIONS ====================

def send_log_to_group(message_text, parse_mode="Markdown"):
    """
    Send log message ke Telegram group
    Returns True jika berhasil, False jika gagal atau LOG_GROUP_ID tidak diset
    """
    if not LOG_GROUP_ID:
        return False
    
    try:
        bot.send_message(
            chat_id=LOG_GROUP_ID,
            text=message_text,
            parse_mode=parse_mode,
            disable_web_page_preview=True
        )
        logger.info(f"[TelegramLogger] Log sent to group {LOG_GROUP_ID}")
        return True
    except Exception as e:
        logger.error(f"[TelegramLogger] Failed to send log to group: {e}")
        return False


def log_account_creation(user_id, username, success_count, total_requested, role, active_count=0):
    """
    Log pembuatan akun ke grup Telegram
    """
    if not LOG_GROUP_ID:
        return
    
    role_display = get_role_display_name(role)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Build message
    username_escaped = username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
    message = (
        f"📊 *LOG: Account Creation*\n"
        f"{'='*35}\n\n"
        f"👤 *User Info:*\n"
        f"├ User ID: `{user_id}`\n"
        f"├ Username: @{username_escaped}\n"
        f"└ Role: {role_display}\n\n"
        f"📦 *Creation Details:*\n"
        f"├ Requested: {total_requested} akun\n"
        f"├ Success: {success_count} akun\n"
        f"├ Active: {active_count} akun\n"
        f"└ Failed: {total_requested - success_count} akun\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(message)


def log_role_added(target_user_id, target_username, role, days, added_by_id, added_by_username):
    """
    Log penambahan role ke grup Telegram dan kirim notifikasi ke user
    """
    if not LOG_GROUP_ID:
        return
    
    role_display = get_role_display_name(role)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expire_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    
    # Log ke grup
    log_message = (
        f"🎯 *LOG: Role Added*\n"
        f"{'='*35}\n\n"
        f"👤 *Target User:*\n"
        f"├ User ID: `{target_user_id}`\n"
        f"├ Username: @{target_username if target_username else 'Unknown'}\n"
        f"└ New Role: {role_display}\n\n"
        f"⏱️ *Duration:*\n"
        f"├ Days: {days} hari\n"
        f"└ Expire: {expire_date}\n\n"
        f"👨‍💼 *Added by:*\n"
        f"├ User ID: `{added_by_id}`\n"
        f"└ Username: @{added_by_username if added_by_username else 'Unknown'}\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(log_message)
    
    # Notifikasi ke target user
    try:
        notification = (
            f"🎉 *Selamat! Role Anda Telah Ditingkatkan*\n\n"
            f"✅ *New Role:* {role_display}\n"
            f"⏱️ *Durasi:* {days} hari\n"
            f"📅 *Expire Date:* `{expire_date}`\n\n"
            f"Anda sekarang dapat menggunakan bot dengan quota lebih besar!\n\n"
            f"Gunakan /myprofile untuk melihat detail lengkap.\n"
            f"Gunakan /usage untuk cek quota Anda."
        )
        
        bot.send_message(
            chat_id=target_user_id,
            text=notification,
            parse_mode="Markdown"
        )
        logger.info(f"[Notification] Role notification sent to user {target_user_id}")
    except Exception as e:
        logger.error(f"[Notification] Failed to send notification to user {target_user_id}: {e}")


def log_member_added(target_user_id, target_username, days, added_by_id, added_by_username):
    """
    Log penambahan member ke grup Telegram dan kirim notifikasi ke user
    """
    if not LOG_GROUP_ID:
        return
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expire_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    
    # Log ke grup
    log_message = (
        f"👥 *LOG: Member Added*\n"
        f"{'='*35}\n\n"
        f"👤 *New Member:*\n"
        f"├ User ID: `{target_user_id}`\n"
        f"└ Username: @{target_username if target_username else 'Unknown'}\n\n"
        f"⏱️ *Subscription:*\n"
        f"├ Duration: {days} hari\n"
        f"└ Expire: {expire_date}\n\n"
        f"👨‍💼 *Added by:*\n"
        f"├ User ID: `{added_by_id}`\n"
        f"└ Username: @{added_by_username if added_by_username else 'Unknown'}\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(log_message)
    
    # Notifikasi ke target user
    try:
        notification = (
            f"🎉 *Selamat! Anda Telah Menjadi Member*\n\n"
            f"✅ *Status:* Member Aktif\n"
            f"⏱️ *Durasi:* {days} hari\n"
            f"📅 *Expire Date:* `{expire_date}`\n\n"
            f"Anda sekarang dapat menggunakan bot dengan akses penuh!\n\n"
            f"Gunakan /myprofile untuk melihat detail lengkap.\n"
            f"Gunakan /usage untuk cek quota Anda.\n"
            f"Gunakan /help untuk melihat semua fitur."
        )
        
        bot.send_message(
            chat_id=target_user_id,
            text=notification,
            parse_mode="Markdown"
        )
        logger.info(f"[Notification] Member notification sent to user {target_user_id}")
    except Exception as e:
        logger.error(f"[Notification] Failed to send notification to user {target_user_id}: {e}")



def log_user_renewal(admin_id, admin_username, target_id, target_username, role, days, new_expiry):
    """Log user renewal ke grup Telegram"""
    if not LOG_GROUP_ID:
        return
    
    role_display = get_role_display_name(role)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expiry_str = new_expiry.strftime("%Y-%m-%d %H:%M:%S") if new_expiry else "Permanent"
    
    log_message = (
        f"🔄 *LOG: User Renewal*\n"
        f"{'='*35}\n\n"
        f"👤 *Target User:*\n"
        f"├ User ID: `{target_id}`\n"
        f"├ Username: @{target_username if target_username else 'Unknown'}\n"
        f"└ Role: {role_display}\n\n"
        f"⏱️ *Renewal:*\n"
        f"├ Added Days: +{days} hari\n"
        f"└ New Expire: {expiry_str}\n\n"
        f"👨‍💼 *Renewed by:*\n"
        f"├ User ID: `{admin_id}`\n"
        f"└ Username: @{admin_username if admin_username else 'Unknown'}\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(log_message)


def log_user_edit(admin_id, admin_username, target_id, target_username, role, days, new_expiry):
    """Log user edit ke grup Telegram"""
    if not LOG_GROUP_ID:
        return
    
    role_display = get_role_display_name(role)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expiry_str = new_expiry.strftime("%Y-%m-%d %H:%M:%S") if new_expiry else "Permanent"
    
    log_message = (
        f"✏️ *LOG: User Edit*\n"
        f"{'='*35}\n\n"
        f"👤 *Target User:*\n"
        f"├ User ID: `{target_id}`\n"
        f"├ Username: @{target_username if target_username else 'Unknown'}\n"
        f"└ Role: {role_display}\n\n"
        f"⏱️ *New Duration:*\n"
        f"├ Days: {days} hari\n"
        f"└ New Expire: {expiry_str}\n\n"
        f"👨‍💼 *Edited by:*\n"
        f"├ User ID: `{admin_id}`\n"
        f"└ Username: @{admin_username if admin_username else 'Unknown'}\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(log_message)


def log_user_deletion(admin_id, admin_username, target_id, target_username, role):
    """Log user deletion ke grup Telegram"""
    if not LOG_GROUP_ID:
        return
    
    role_display = get_role_display_name(role)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    log_message = (
        f"🗑️ *LOG: User Deletion*\n"
        f"{'='*35}\n\n"
        f"👤 *Deleted User:*\n"
        f"├ User ID: `{target_id}`\n"
        f"├ Username: @{target_username if target_username else 'Unknown'}\n"
        f"└ Role: {role_display}\n\n"
        f"👨‍💼 *Deleted by:*\n"
        f"├ User ID: `{admin_id}`\n"
        f"└ Username: @{admin_username if admin_username else 'Unknown'}\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(log_message)


def log_user_replacement(admin_id, admin_username, old_id, old_username, new_id, new_username, role, expiry):
    """Log user replacement ke grup Telegram"""
    if not LOG_GROUP_ID:
        return
    
    role_display = get_role_display_name(role)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expiry_str = expiry.strftime("%Y-%m-%d %H:%M:%S") if expiry else "Permanent"
    
    log_message = (
        f"🔄 *LOG: Member Replacement*\n"
        f"{'='*35}\n\n"
        f"👤 *Old Member:*\n"
        f"├ User ID: `{old_id}`\n"
        f"├ Username: @{old_username if old_username else 'Unknown'}\n"
        f"└ Status: ❌ Removed\n\n"
        f"👤 *New Member:*\n"
        f"├ User ID: `{new_id}`\n"
        f"├ Username: @{new_username if new_username else 'Unknown'}\n"
        f"├ Role: {role_display}\n"
        f"└ Expire: {expiry_str}\n\n"
        f"👨‍💼 *Replaced by:*\n"
        f"├ User ID: `{admin_id}`\n"
        f"└ Username: @{admin_username if admin_username else 'Unknown'}\n\n"
        f"⏰ *Time:* {timestamp}"
    )
    
    send_log_to_group(log_message)


# ==================== END TELEGRAM LOGGER FUNCTIONS ====================


# ==================== PAYMENT SYSTEM ====================

class Orderkuota:
    def __init__(self):
        self.client = requests.Session()
        self.polling_headers = {
            "Authorization": "1718396:Z5IYnRlWL2GDkMmigqQVr0PAajvzob7e"
        }
        self.client.headers.update(self.polling_headers)
        self.polling_url = "https://orderkuota.hoeson.top/api/polling"
        self.static_qris_string = STATIC_QRIS_STRING

    @staticmethod
    def convert_crc16(data: str) -> str:
        crc = 0xFFFF
        for char in data:
            crc ^= ord(char) << 8
            for _ in range(8):
                if crc & 0x8000:
                    crc = (crc << 1) ^ 0x1021
                else:
                    crc <<= 1
                crc &= 0xFFFF
        hex_crc = hex(crc)[2:].upper().zfill(4)
        return hex_crc

    @staticmethod
    def qris_statis_to_dinamis(qris, nominal, fee_mode=None, fee_value=None):
        qris = qris.strip()
        nominal = str(nominal).strip()
        qris = qris[:-4]
        qris = qris.replace("010211", "010212")
        if "5802ID" not in qris:
            raise ValueError("QRIS tidak valid. Tidak ditemukan '5802ID'.")

        part1, part2 = qris.split("5802ID", 1)
        uang = "54" + f"{len(nominal):02d}" + nominal

        if fee_mode == "r":
            uang += "55020256" + f"{len(fee_value):02d}" + str(fee_value)
        elif fee_mode == "p":
            uang += "55020357" + f"{len(fee_value):02d}" + str(fee_value)

        uang += "5802ID"
        result = part1 + uang + part2
        result += Orderkuota.convert_crc16(result)
        return result

    @staticmethod
    def generate_qr_image(data: str):
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        img_bytes = BytesIO()
        img.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return img_bytes

    def check_transaction(self, nominal_target, order_id):
        try:
            payload = {
                "auth_username": "ra",
                "auth_token": "177",
                "auth_id": "543",
                "nominal": int(nominal_target),
                "order_id": order_id
            }
            resp = self.client.post(self.polling_url, json=payload, timeout=15)

            if resp.status_code != 200:
                logger.warning(f"[Orderkuota] Gagal polling, status {resp.status_code}")
                return None

            data = resp.json()

            if data.get("status") == "success" and data.get("data"):
                r = data["data"]
                kredit_val_str = r.get("kredit", "0").replace('.', '')
                
                if kredit_val_str.isdigit():
                    kredit_val_int = int(kredit_val_str)
                    if abs(kredit_val_int - int(nominal_target)) < 1:
                        logger.info(f"[Orderkuota] Pembayaran Rp{nominal_target} ditemukan (Order ID: {order_id})")
                        return r
                
                return None
            else:
                return None

        except json.JSONDecodeError:
            logger.error(f"[Orderkuota] Response bukan JSON valid untuk {order_id}")
            return None
        except requests.RequestException as e:
            logger.error(f"[Orderkuota] Error polling: {e}")
            return None
        except Exception as e:
            logger.error(f"[Orderkuota] Error di check_transaction: {e}")
            return None


def master_payment_poller():
    """Master payment poller"""
    orderkuota = Orderkuota()
    logger.info("[PaymentPoller] Master payment poller started.")
    
    while True:
        try:
            with payment_checkers_lock:
                pending_users = list(payment_checkers.keys())
            
            if not pending_users:
                time.sleep(5)
                continue

            for user_id in pending_users:
                with payment_checkers_lock:
                    details = payment_checkers.get(user_id)
                    if not details:
                        continue
                
                total_price = details['total_price']
                message_id = details['message_id']
                unique_code = details['unique_code']
                start_time = details['start_time']
                duration_days = details.get('duration_days', 30)

                if datetime.now() - start_time > timedelta(minutes=10):
                    logger.info(f"[PaymentPoller] Payment timeout for user {user_id}")
                    with payment_checkers_lock:
                        if user_id in payment_checkers:
                            del payment_checkers[user_id]
                    
                    text = (
                        f"⏰ *Waktu Pembayaran Habis*\n\n"
                        f"Pembayaran sebesar Rp {total_price:,} (Order ID: {unique_code}) tidak terdeteksi dalam 10 menit.\n\n"
                        "Silakan gunakan /sewabot untuk membuat pembayaran baru."
                    )
                    try:
                        bot.delete_message(chat_id=user_id, message_id=message_id)
                    except Exception:
                        pass
                    bot.send_message(user_id, text, parse_mode="Markdown")
                    continue

                transaction = orderkuota.check_transaction(total_price, unique_code)
                
                if transaction:
                    logger.info(f"[PaymentPoller] Payment success for user {user_id} (Order ID: {unique_code})")
                    expire_date = datetime.now() + timedelta(days=duration_days)

                    users_collection.update_one(
                        {"user_id": user_id},
                        {"$set": {"expire_date": expire_date, "payment_verified_at": datetime.now()}},
                        upsert=True
                    )
                    transactions_collection.update_one(
                        {"user_id": user_id, "unique_code": unique_code},
                        {"$set": {"status": "paid", "paid_at": datetime.now(), "provider_data": transaction}}
                    )

                    text = (
                        "✅ *Pembayaran Berhasil Diverifikasi!*\n\n"
                        f"💰 Total: Rp {total_price:,}\n"
                        f"📅 Durasi: {duration_days} hari\n"
                        f"⏰ Expire: `{expire_date.strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                        "Terima kasih! Silakan gunakan /start untuk mulai."
                    )
                    try:
                        bot.delete_message(chat_id=user_id, message_id=message_id)
                    except Exception:
                        pass

                    bot.send_message(user_id, text, parse_mode="Markdown")

                    with payment_checkers_lock:
                        if user_id in payment_checkers:
                            del payment_checkers[user_id]
                    continue
            
            time.sleep(10)

        except Exception as e:
            logger.error(f"[PaymentPoller] Error in master poller loop: {e}")
            time.sleep(15)


# ==================== VIDIO CLASS WITH PROXY & ACTIVATION ====================

class Vidio:
    def __init__(self, proxy_config=None):
        self.KEY_ID = "ZXhDgP7RixaP"
        self.SYMMETRIC_KEY_B64 = "O8NAJlk7o7GNeNn01qUXxjezrD/Z2djOMjSizTRZt1U="
        self.BASE_URL = "https://api.vidio.com/api"
        self.auth_data = {}
        self.visitor_id = str(uuid.uuid4())
        self.fake = Faker('id_ID')
        self.email = None
        self.password = None
        self.user_id = None
        
        # Proxy configuration
        if proxy_config is None:
            self.proxy_config = {
                'username': PROXY_USERNAME,
                'password': PROXY_PASSWORD,
                'host': PROXY_HOST,
                'port': PROXY_PORT
            }
        else:
            self.proxy_config = proxy_config
            
        self.session = self.create_session()
        
    def create_session(self):
        """Create session with proxy"""
        session = requests.Session()
        proxy_url = f"http://{self.proxy_config['username']}:{self.proxy_config['password']}@{self.proxy_config['host']}:{self.proxy_config['port']}"
        session.proxies = {
            'http': proxy_url,
            'https': proxy_url
        }
        logger.info(f"[Vidio] Session created with proxy: {self.proxy_config['host']}:{self.proxy_config['port']}")
        return session
    
    def close(self):
        if self.session:
            self.session.close()
            
    def generate_nonce(self, length=12):
        nonce_str = secrets.token_urlsafe(length)[:length]
        nonce_bytes = nonce_str.encode('utf-8')
        return nonce_str, nonce_bytes
    
    def generate_unique_id(self):
        return secrets.token_hex(8)
    
    def normalize_email(self, email_base):
        normalized = email_base.lower()
        normalized = re.sub(r'[^a-z0-9]', '', normalized)
        return normalized
    
    def generate_random_email(self):
        """
        Generate email dengan 2 mode:
        - random: 6 huruf random + 2-3 angka
        - name: nama orang (max 6 huruf) + 2-3 angka
        """
        # Get email mode dari settings
        settings = get_bot_settings()
        email_mode = settings.get('email_mode', 'random')
        
        if email_mode == 'name':
            # Mode: Nama orang (max 6 huruf) + 2-3 angka
            name = self.fake.first_name().lower()
            # Normalize: hapus spasi, karakter khusus, ambil max 6 huruf
            normalized = re.sub(r'[^a-z]', '', name)[:6]
            
            # Jika nama < 6 huruf, tambah random huruf
            if len(normalized) < 6:
                additional = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=6-len(normalized)))
                normalized += additional
            
            # Random 2 atau 3 angka
            digit_count = random.choice([2, 3])
            numbers = ''.join(random.choices('0123456789', k=digit_count))
            
            email = f"{normalized}{numbers}@gmail.com"
        else:
            # Mode: Random 6 huruf + 2-3 angka
            letters = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=6))
            
            # Random 2 atau 3 angka
            digit_count = random.choice([2, 3])
            numbers = ''.join(random.choices('0123456789', k=digit_count))
            
            email = f"{letters}{numbers}@gmail.com"
        
        return email
    
    def generate_password_from_email(self, email):
        # Cek apakah ada custom password untuk user ini
        if self.user_id:
            custom_data = get_custom_password(self.user_id)
            if custom_data:
                mode = custom_data.get('mode', 'same')
                base_password = custom_data.get('password')
                
                if mode == 'same':
                    logger.info(f"[CustomPassword] Using SAME password for all accounts (user {self.user_id})")
                    return base_password
                elif mode == 'random':
                    suffix = ''.join(random.choices('0123456789', k=3))
                    password = f"{base_password}{suffix}"
                    logger.info(f"[CustomPassword] Using RANDOM mode with base password (user {self.user_id})")
                    return password
        
        # Jika tidak ada custom password, generate random seperti biasa
        username = email.split('@')[0]
        base_name = re.sub(r'\d+', '', username)
        
        special_chars = ['!', '@', '#', '$', '%']
        special = random.choice(special_chars)
        number = random.randint(100, 999)
        
        password = f"{base_name.capitalize()}{special}{number}"
        return password
        
    def generate_encrypted_body(self, key_b64, plaintext_str, nonce_bytes):
        try:
            key_bytes = base64.b64decode(key_b64)
            plaintext_bytes = plaintext_str.encode('utf-8')
            cipher = AES.new(key_bytes, AES.MODE_GCM, nonce=nonce_bytes)
            ciphertext_bytes, tag_bytes = cipher.encrypt_and_digest(plaintext_bytes)
            combined_bytes = ciphertext_bytes + tag_bytes + nonce_bytes
            encrypted_b64 = base64.b64encode(combined_bytes).decode('utf-8')
            final_body = json.dumps({"data": encrypted_b64})
            return final_body
        except Exception as e:
            logger.error(f"Gagal mengenkripsi body: {e}")
            return None
        
    def generate_signature(self, key_id, key_b64, raw_payload, nonce_str, nonce_bytes):
        try:
            symmetric_key = base64.b64decode(key_b64)
            key2 = hmac.new(
                symmetric_key,
                nonce_str.encode('utf-8'),
                hashlib.sha256
            ).digest()
            hmac_signature_bytes = hmac.new(
                key2,
                raw_payload.encode('utf-8'),
                hashlib.sha256
            ).digest()

            base64_hmac = base64.b64encode(hmac_signature_bytes).decode('utf-8')
            base64_nonce = base64.b64encode(nonce_bytes).decode('utf-8')
            
            if len(base64_nonce) != 16:
                raise ValueError("Nonce Base64 seharusnya 16 karakter")

            nonce_prefix = base64_nonce[:-1]
            nonce_suffix = base64_nonce[-1]
            signature_value = nonce_prefix + base64_hmac + nonce_suffix
            final_signature = f'keyId="{key_id}",signature="{signature_value}"'
            
            return final_signature
        except Exception as e:
            logger.error(f"Gagal membuat signature: {e}")
            return None

    def get_base_headers(self):
        return {
            'X-API-App-Info': 'android/9/6.43.9-8ec34856ef-3191448',
            'X-Api-Auth': 'laZOmogezono5ogekaso5oz4Mezimew1',
            'X-Api-Platform': 'app-android',
            'X-VISITOR-ID': self.visitor_id,
        }

    def get_tv_headers(self):
        return {
            'Referer': 'androidtv-app://com.vidio.android.tv',
            'X-API-Platform': 'tv-android',
            'X-API-Auth': 'laZOmogezono5ogekaso5oz4Mezimew1',
            'User-Agent': 'tv-android/2.9.1 (481)',
            'X-API-App-Info': 'tv-android/12/2.9.1-481',
            'Accept-Language': 'en',
        }

    def create_request_data(self, partner_agent="tcl"):
        unique_id = self.generate_unique_id()
        nonce_str, nonce_bytes = self.generate_nonce()

        plaintext_payload = {
            "unique_id": unique_id,
            "partner_agent": partner_agent
        }
        plaintext_payload_str = json.dumps(plaintext_payload, separators=(',', ':'))

        generated_body = self.generate_encrypted_body(
            self.SYMMETRIC_KEY_B64, 
            plaintext_payload_str, 
            nonce_bytes
        )

        generated_signature = self.generate_signature(
            self.KEY_ID,
            self.SYMMETRIC_KEY_B64,
            plaintext_payload_str,
            nonce_str,
            nonce_bytes
        )

        if generated_body and generated_signature:
            return {
                "body": generated_body,
                "signature": generated_signature
            }
        return None

    def partner_auth(self, max_retries=3, delay=3):
        """Partner authentication untuk TV"""
        for attempt in range(1, max_retries + 1):
            try:
                request_data = self.create_request_data()
                if not request_data:
                    logger.warning("[partner_auth] Gagal membuat request data")
                    continue

                url = f"{self.BASE_URL}/partner/auth"
                headers = self.get_tv_headers()
                headers.update({
                    'Signature': request_data['signature'],
                    'Content-Type': 'application/json',
                })
                
                response = self.session.post(url, headers=headers, data=request_data['body'], timeout=15)

                try:
                    result = response.json()
                except Exception as e:
                    logger.warning(f"[partner_auth] Attempt {attempt}: response bukan JSON valid ({e})")
                    result = None

                if result and result.get('auth'):
                    self.auth_data['tv'] = {
                        'authentication_token': result['auth']['authentication_token'],
                        'email': result['auth']['email'],
                        'uid': result['auth']['uid'],
                    }
                    logger.info(f"[partner_auth] Berhasil pada percobaan ke-{attempt}")
                    return self.auth_data['tv']
                else:
                    logger.warning(f"[partner_auth] Gagal pada percobaan ke-{attempt}: {response.text[:100]}")

            except Exception as e:
                logger.error(f"[partner_auth] Error attempt {attempt}: {e}")

            if attempt < max_retries:
                time.sleep(delay)

        logger.error("[partner_auth] Semua percobaan gagal setelah retry.")
        return None

        
    def generate_code(self):
        """Generate TV code"""
        if 'tv' not in self.auth_data:
            return None
            
        url = f"{self.BASE_URL}/tv/code"
        headers = self.get_tv_headers()
        
        try:
            response = self.session.get(url, headers=headers, timeout=15)
            result = response.json()
            if result.get('code'):
                return result
            return None
        except Exception as e:
            logger.error(f"Request gagal: {e}")
            return None
    
    def verify_code(self, code):
        """Verify TV code untuk aktivasi subscription"""
        if 'tv' not in self.auth_data:
            return None
            
        url = f"{self.BASE_URL}/tv/verify_code"
        headers = self.get_tv_headers()
        headers.update({
            'X-USER-EMAIL': self.auth_data['tv']['email'],
            'X-USER-TOKEN': self.auth_data['tv']['authentication_token'],
            'X-VISITOR-ID': self.visitor_id,
            'Content-Type': 'application/x-www-form-urlencoded',
        })
        
        payload = f"code={code}"
        
        try:
            response = self.session.post(url, headers=headers, data=payload, timeout=15)
            result = response.json()
            
            if "authentication_token" in response.text:
                logger.info(f"[verify_code] Akun berhasil diaktivasi dengan TV subscription")
                return result
            return None
        except Exception as e:
            logger.error(f"Error verify: {e}")
            return None
        
    def register_account(self, email=None, password=None, max_retries=3, delay=3):
        """Register new Vidio account"""
        if not email:
            self.email = self.generate_random_email()
        else:
            self.email = email

        if not password:
            self.password = self.generate_password_from_email(self.email)
        else:
            self.password = password

        payload = {"email": self.email, "password": self.password}

        for attempt in range(1, max_retries + 1):
            try:
                headers = self.get_base_headers()
                response = self.session.post(
                    f"{self.BASE_URL}/register?check_user_consent=true",
                    data=payload,
                    headers=headers,
                    timeout=15
                )

                try:
                    result = response.json()
                except Exception as e:
                    logger.warning(f"[register_account] Percobaan {attempt}: JSON invalid ({e})")
                    result = {}

                if result.get('auth', {}).get('authentication_token'):
                    self.save_auth_from_register(result)
                    logger.info(f"[register_account] Berhasil di percobaan ke-{attempt}")
                    return True

                elif result.get('consent_uuid'):
                    consent_uuid = result['consent_uuid']
                    if self.accept_consent(consent_uuid):
                        ok = self.complete_registration()
                        return ok

                logger.warning(f"[register_account] Gagal attempt {attempt}, status {response.status_code}")
            except Exception as e:
                logger.error(f"[register_account] Error attempt {attempt}: {e}")
            
            if attempt < max_retries:
                logger.info(f"[register_account] Retry dalam {delay} detik...")
                time.sleep(delay)

        logger.error("[register_account] Semua percobaan gagal.")
        return False

    
    def accept_consent(self, consent_uuid):
        """Accept user consent"""
        headers = self.get_base_headers()
        headers['Content-Type'] = 'application/vnd.api+json'
        
        data = {
            "data": {
                "type": "user_consent_acceptance",
                "attributes": {"consent_uuid": consent_uuid}
            }
        }
        
        try:
            response = self.session.post(
                "https://api.vidio.com/users/consent",
                json=data,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                return True
            return False
        except Exception as e:
            logger.error(f"Error accept consent: {e}")
            return False
    
    def complete_registration(self):
        """Complete registration after consent"""
        payload = {"email": self.email, "password": self.password}
        headers = self.get_base_headers()
        
        try:
            response = self.session.post(
                f"{self.BASE_URL}/register?check_user_consent=true",
                data=payload,
                headers=headers,
                timeout=15
            )
            result = response.json()
            
            if result.get('auth', {}).get('authentication_token'):
                self.save_auth_from_register(result)
                return True
            return False
        except Exception as e:
            logger.error(f"Error complete registration: {e}")
            return False
    
    def save_auth_from_register(self, result):
        """Save authentication data from registration"""
        self.auth_data['user'] = {
            'authentication_token': result['auth']['authentication_token'],
            'access_token': result['auth_tokens']['access_token'],
            'email': self.email
        }
    
    def link_tv_to_account(self, code):
        """Link TV to user account untuk aktivasi subscription"""
        if 'user' not in self.auth_data:
            return None
        
        url = f"{self.BASE_URL}/tv/login"
        headers = self.get_base_headers()
        headers.update({
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-USER-EMAIL': self.auth_data['user']['email'],
            'X-USER-TOKEN': self.auth_data['user']['authentication_token'],
            'X-AUTHORIZATION': self.auth_data['user']['access_token']
        })
        
        payload = f"code={code}"
        
        try:
            response = self.session.post(url, data=payload, headers=headers, timeout=15)
            result = response.json()
            
            if result.get('success'):
                logger.info(f"[link_tv_to_account] Berhasil link TV ke akun {self.email}")
                return result
            return None
        except Exception as e:
            logger.error(f"Error link TV: {e}")
            return None

    def auto_flow(self):
        """
        Complete auto flow: 
        1. Partner auth (TV)
        2. Generate code
        3. Register account
        4. Link TV to account
        5. Verify code (aktivasi subscription)
        """
        # Step 1: Partner auth
        tv_auth = self.partner_auth()
        if not tv_auth:
            logger.error("[auto_flow] Partner auth failed")
            return None
    
        time.sleep(random.uniform(1, 2))
        
        # Step 2: Generate code
        code_result = self.generate_code()
        if not code_result:
            logger.error("[auto_flow] Generate code failed")
            return None
        
        tv_code = code_result['code']
        logger.info(f"[auto_flow] TV code generated: {tv_code}")
        
        time.sleep(random.uniform(1, 2))
        
        # Step 3: Register account
        if not self.register_account():
            logger.error("[auto_flow] Registration failed")
            return None
        
        logger.info(f"[auto_flow] Account registered: {self.email}")
        time.sleep(random.uniform(1, 2))
        
        # Step 4: Link TV to account
        link_result = self.link_tv_to_account(tv_code)
        if not link_result:
            logger.error(f"[auto_flow] Link TV failed for {self.email}")
            return None
        
        # Step 5: Verify code untuk aktivasi subscription
        verify_result = self.verify_code(tv_code)
        
        if link_result and verify_result:
            logger.info(f"[auto_flow] ✅ Akun berhasil dibuat dan diaktivasi: {self.email}")
            return {
                'email': self.email,
                'password': self.password,
                'tv_code': tv_code,
                'status': 'SUCCESS',
                'subscription': 'ACTIVE'
            }
        elif link_result:
            logger.info(f"[auto_flow] ⚠️ Akun berhasil dibuat: {self.email} (verifikasi pending)")
            return {
                'email': self.email,
                'password': self.password,
                'tv_code': tv_code,
                'status': 'SUCCESS',
                'subscription': 'PENDING'
            }
        
        logger.error(f"[auto_flow] Gagal membuat akun {self.email}")
        return None
    
    def check_account_info(self, email, password):
        """
        Check Vidio account information (plan and expiry)
        Returns dict with account info or None if failed
        """
        try:
            # Simulate login and check account
            # Dalam implementasi nyata, ini akan melakukan actual API call ke Vidio
            time.sleep(random.uniform(1, 2))
            
            # Simulasi response - ganti dengan actual API call
            # Untuk demonstrasi, kita return data dummy
            account_info = {
                'email': email,
                'status': 'active',
                'plan': 'Premium',  # atau 'Basic', 'Free', dll
                'expiry': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
                'success': True
            }
            
            return account_info
            
        except Exception as e:
            logger.error(f"Check account error: {e}")
            return {
                'email': email,
                'success': False,
                'error': str(e)
            }


# ==================== COMMAND HANDLERS ====================

# ===== /start Command =====
@bot.message_handler(commands=['start'])
def start_cmd(message):
    """Handler untuk command /start"""
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    first_name = message.from_user.first_name or "User"
    
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "first_name": first_name,
                "last_seen": datetime.now()
            },
            "$setOnInsert": {
                "role": ROLE_FREE_USER  # Default role untuk user baru
            }
        },
        upsert=True
    )
    
    role = get_user_role(user_id)
    role_display = get_role_display_name(role)
    
    # Check subscription
    subscription = get_user_subscription(user_id)
    if subscription:
        expire_date = subscription['expire_date']
        expire_str = expire_date.strftime('%Y-%m-%d %H:%M:%S')
        status = f"✅ Aktif"
        has_access = True
    else:
        status = "❌ Tidak Aktif"
        has_access = False
    
    # Jika role bukan free user atau punya subscription, anggap punya akses
    if role != ROLE_FREE_USER:
        has_access = True
    
    welcome_text = (
        f"Halo kak @{username} 👋\n"
        f"*Selamat Datang di Bot Vidiotv*\n\n"
        f"Chat ID : `{user_id}`\n"
        f"Status Lisensi : {status}\n"
        f"Role : {role_display}\n\n"
        f"Silakan ketik /help untuk melihat panduan penggunaan bot."
    )
    
    # Create inline keyboard with "Create Account" button
    keyboard = types.InlineKeyboardMarkup()
    
    # Tampilkan tombol Create untuk:
    # 1. User yang punya subscription aktif
    # 2. User dengan role selain Free User (Member, Admin, Super Admin, Owner)
    # 3. Free User juga bisa create (dengan limit 5 akun)
    keyboard.add(types.InlineKeyboardButton("🎬 Create Account", callback_data="create_account"))
    
    bot.reply_to(message, welcome_text, reply_markup=keyboard, parse_mode="Markdown")


# ===== USER COMMANDS =====

@bot.message_handler(commands=['myprofile'])
def myprofile_cmd(message):
    """Command untuk lihat profile"""
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    
    role = get_user_role(user_id)
    role_display = get_role_display_name(role)
    
    # Owner dan Super Admin = Unlimited
    if role in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        text = (
            "👤 *Profile Anda*\n\n"
            f"User ID: `{user_id}`\n"
            f"Username: `@{username}`\n"
            f"Role: {role_display}\n"
            f"Status: ✅ Aktif\n"
            f"⏰ Expire: ♾️ Unlimited"
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    subscription = get_user_subscription(user_id)
    
    if subscription:
        expire_date = subscription['expire_date'].strftime('%Y-%m-%d %H:%M:%S')
        status = "✅ Aktif"
        text = (
            "👤 *Profile Anda*\n\n"
            f"User ID: `{user_id}`\n"
            f"Username: `@{username}`\n"
            f"Role: {role_display}\n"
            f"Status: {status}\n"
            f"⏰ Expire: `{expire_date}`"
        )
    else:
        text = (
            "👤 *Profile Anda*\n\n"
            f"User ID: `{user_id}`\n"
            f"Username: `@{username}`\n"
            f"Role: {role_display}\n"
            f"Status: ❌ Tidak Aktif\n\n"
            "Gunakan /sewabot atau chat owner bot untuk berlangganan bot ini."
        )
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['setpassword'])
def set_password_cmd(message):
    """Command untuk set custom password"""
    user_id = message.from_user.id
    
    text = (
        "🔐 *Set Custom Password*\n\n"
        "Masukkan password yang ingin Anda gunakan untuk semua akun Vidiotv.\n\n"
        "📋 *Ketentuan Password:*\n"
        "• Minimal 8 karakter\n"
        "• Maksimal 20 karakter\n"
        "• Harus mengandung huruf dan angka\n"
        "• Boleh mengandung karakter spesial\n\n"
        "💡 Contoh: `MyVidio123!`\n\n"
        "_Ketik password Anda sekarang:_"
    )
    
    msg = bot.send_message(user_id, text, parse_mode="Markdown")
    bot.register_next_step_handler(msg, handle_set_password_input)


def handle_set_password_input(message):
    """Handle input password"""
    user_id = message.from_user.id
    password = (message.text or "").strip()
    
    is_valid, msg_text = is_valid_custom_password(password)
    
    if not is_valid:
        error_text = (
            f"❌ *Password Tidak Valid*\n\n"
            f"Alasan: {msg_text}\n\n"
            "Silakan gunakan /setpassword untuk mencoba lagi."
        )
        bot.reply_to(message, error_text, parse_mode="Markdown")
        return
    
    text = (
        "✅ *Password Valid!*\n\n"
        f"Password: `{password}`\n\n"
        "🎯 *Pilih Mode Password:*\n\n"
        "*1️⃣ Mode SAMA*\n"
        "Semua akun menggunakan password yang sama\n\n"
        "*2️⃣ Mode RANDOM*\n"
        "Setiap akun menggunakan password berbeda\n"
        f"Contoh: `{password}123`, `{password}456`\n\n"
        "Ketik *1* untuk Mode SAMA atau *2* untuk Mode RANDOM:"
    )
    
    msg = bot.send_message(user_id, text, parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda m: handle_password_mode_input(m, password))


def handle_password_mode_input(message, password):
    """Handle input mode password"""
    user_id = message.from_user.id
    mode_input = (message.text or "").strip()
    
    if mode_input == "1":
        mode = "same"
        mode_text = "SAMA"
        description = "Semua akun akan menggunakan password yang sama persis."
    elif mode_input == "2":
        mode = "random"
        mode_text = "RANDOM"
        description = "Setiap akun akan menggunakan password berbeda (base password + 3 digit random)."
    else:
        error_text = (
            "❌ *Input Tidak Valid*\n\n"
            "Silakan ketik *1* untuk Mode SAMA atau *2* untuk Mode RANDOM.\n\n"
            "Gunakan /setpassword untuk mengatur ulang."
        )
        bot.reply_to(message, error_text, parse_mode="Markdown")
        return
    
    save_custom_password(user_id, password, mode)
    
    success_text = (
        "✅ *Custom Password Berhasil Disimpan!*\n\n"
        f"Password: `{password}`\n"
        f"Mode: *{mode_text}*\n\n"
        f"📋 {description}\n\n"
        "💡 Tips:\n"
        "• Gunakan /mypassword untuk melihat password tersimpan\n"
        "• Gunakan /deletepassword untuk menghapus password\n"
        "• Gunakan /setpassword untuk mengubah password baru"
    )
    
    bot.reply_to(message, success_text, parse_mode="Markdown")


@bot.message_handler(commands=['mypassword'])
def my_password_cmd(message):
    """Command untuk lihat custom password"""
    user_id = message.from_user.id
    
    custom_data = get_custom_password(user_id)
    
    if custom_data:
        password = custom_data.get('password')
        mode = custom_data.get('mode', 'same')
        
        if mode == 'same':
            mode_text = "SAMA"
            mode_desc = "Semua akun akan menggunakan password yang sama persis."
            example = f"Semua akun: `{password}`"
        else:
            mode_text = "RANDOM"
            mode_desc = "Setiap akun akan menggunakan password berbeda."
            example = f"Contoh: `{password}123`, `{password}456`"
        
        text = (
            "🔐 *Custom Password Anda*\n\n"
            f"Password: `{password}`\n"
            f"Mode: *{mode_text}*\n\n"
            f"📋 {mode_desc}\n"
            f"{example}\n\n"
            "💡 Gunakan /deletepassword untuk menghapus atau /setpassword untuk mengubah."
        )
    else:
        text = (
            "❌ *Belum Ada Custom Password*\n\n"
            "Anda belum mengatur custom password.\n"
            "Akun akan dibuat dengan password random.\n\n"
            "Gunakan /setpassword untuk mengatur custom password."
        )
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['deletepassword'])
def delete_password_cmd(message):
    """Command untuk hapus custom password"""
    user_id = message.from_user.id
    
    password = get_custom_password(user_id)
    
    if password:
        delete_custom_password(user_id)
        text = (
            "✅ *Custom Password Dihapus*\n\n"
            "Password custom Anda telah dihapus.\n"
            "Akun baru akan dibuat dengan password random.\n\n"
            "Gunakan /setpassword jika ingin mengatur lagi."
        )
    else:
        text = "❌ Anda tidak memiliki custom password yang tersimpan."
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['usage'])
def usage_cmd(message):
    """Command untuk cek usage harian"""
    user_id = message.from_user.id
    
    role = get_user_role(user_id)
    role_display = get_role_display_name(role)
    daily_limit = DAILY_LIMITS.get(role, 0)
    
    in_cooldown, remaining_time = get_cooldown_status(user_id)
    
    text = (
        f"📊 *Usage Information*\n\n"
        f"Role: {role_display}\n"
    )
    
    if role in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        text += "Limit: ♾️ Unlimited\n"
    elif role == ROLE_FREE_USER:
        lifetime_usage = get_total_lifetime_usage(user_id)
        remaining = daily_limit - lifetime_usage
        text += (
            f"Limit Total: {daily_limit} akun (seumur hidup)\n"
            f"Terpakai: {lifetime_usage} akun\n"
            f"Sisa: {remaining} akun\n"
        )
    else:
        daily_usage = get_daily_usage(user_id)
        remaining = daily_limit - daily_usage
        text += (
            f"Limit Harian: {daily_limit} akun\n"
            f"Terpakai Hari Ini: {daily_usage} akun\n"
            f"Sisa: {remaining} akun\n"
        )
    
    if in_cooldown:
        minutes = remaining_time // 60
        seconds = remaining_time % 60
        text += f"\n⏳ *Cooldown:* {minutes}m {seconds}s"
    else:
        text += "\n✅ *Status:* Siap membuat akun"
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['checkcooldown'])
def checkcooldown_cmd(message):
    """Command untuk cek status cooldown user"""
    user_id = message.from_user.id
    
    role = get_user_role(user_id)
    role_display = get_role_display_name(role)
    
    # Owner dan Super Admin tidak punya cooldown
    if role in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        text = (
            f"ℹ️ *Cooldown Status*\n\n"
            f"Role: {role_display}\n"
            f"Status: ✅ *No Cooldown*\n\n"
            f"Anda memiliki unlimited access tanpa cooldown."
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    # Free User tidak punya cooldown (lifetime limit)
    if role == ROLE_FREE_USER:
        lifetime_usage = get_total_lifetime_usage(user_id)
        daily_limit = DAILY_LIMITS.get(role, 5)
        remaining = daily_limit - lifetime_usage
        
        text = (
            f"ℹ️ *Cooldown Status*\n\n"
            f"Role: {role_display}\n"
            f"Status: ✅ *No Cooldown*\n\n"
            f"📊 Lifetime Usage:\n"
            f"├ Used: {lifetime_usage}/{daily_limit} akun\n"
            f"└ Remaining: {remaining} akun\n\n"
            f"💡 Free user tidak memiliki cooldown,\n"
            f"tapi memiliki limit lifetime {daily_limit} akun."
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    # Admin dan Member - cek cooldown
    is_cooldown, remaining_time = get_cooldown_status(user_id)
    session_count = get_session_count(user_id)
    settings = get_bot_settings()
    threshold = settings.get('cooldown_threshold', COOLDOWN_THRESHOLD)
    duration = settings.get('cooldown_duration', COOLDOWN_DURATION)
    
    if is_cooldown:
        minutes = remaining_time // 60
        seconds = remaining_time % 60
        
        text = (
            f"⏳ *Cooldown AKTIF*\n\n"
            f"Role: {role_display}\n"
            f"Status: ❌ *In Cooldown*\n\n"
            f"⏱️ *Waktu Tersisa:*\n"
            f"├ {minutes} menit {seconds} detik\n"
            f"└ Total: {remaining_time} detik\n\n"
            f"📊 *Session Info:*\n"
            f"├ Akun dibuat: {session_count}\n"
            f"└ Threshold: {threshold} akun\n\n"
            f"💡 Cooldown akan berakhir dalam {minutes}m {seconds}s.\n"
            f"Setelah cooldown selesai, Anda dapat membuat akun lagi."
        )
    else:
        remaining_to_cooldown = threshold - session_count
        
        text = (
            f"✅ *Cooldown TIDAK AKTIF*\n\n"
            f"Role: {role_display}\n"
            f"Status: ✅ *Ready*\n\n"
            f"📊 *Session Info:*\n"
            f"├ Akun dibuat: {session_count}/{threshold}\n"
            f"└ Remaining: {remaining_to_cooldown} akun\n\n"
            f"⚙️ *Cooldown Settings:*\n"
            f"├ Threshold: {threshold} akun\n"
            f"└ Duration: {duration} detik ({duration//60} menit)\n\n"
            f"💡 Cooldown akan aktif setelah Anda membuat {threshold} akun.\n"
            f"Sisa {remaining_to_cooldown} akun lagi sebelum cooldown."
        )
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['check'])
def check_cmd(message):
    """Command untuk cek paket akun Vidio"""
    user_id = message.from_user.id
    
    # Parse input
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ Format salah!\n\n"
            "✅ Format yang benar:\n"
            "`/check email|password`\n\n"
            "Contoh:\n"
            "`/check john@gmail.com|Pass12345`",
            parse_mode="Markdown"
        )
        return
    
    credentials = parts[1].strip()
    if '|' not in credentials:
        bot.reply_to(
            message,
            "❌ Format salah! Gunakan separator `|` (pipe)\n\n"
            "Contoh:\n"
            "`/check john@gmail.com|Pass12345`",
            parse_mode="Markdown"
        )
        return
    
    try:
        email, password = credentials.split('|', 1)
        email = email.strip()
        password = password.strip()
        
        if not email or not password:
            bot.reply_to(message, "❌ Email atau password tidak boleh kosong!")
            return
        
        # Send processing message
        processing_msg = bot.reply_to(
            message,
            "⏳ Sedang mengecek akun Vidio...\n"
            "Mohon tunggu sebentar."
        )
        
        # Check account
        vidio = Vidio()
        result = vidio.check_account_info(email, password)
        vidio.close()
        
        # Delete processing message
        try:
            bot.delete_message(chat_id=user_id, message_id=processing_msg.message_id)
        except:
            pass
        
        # Send result
        if result['success']:
            response = (
                "✅ *Informasi Akun Vidio*\n\n"
                f"📧 Email: `{result['email']}`\n"
                f"📦 Paket: *{result['plan']}*\n"
                f"📅 Expiry: `{result['expiry']}`\n\n"
                "Status: ✅ Login berhasil"
            )
        else:
            response = f"❌ {result['message']}"
        
        bot.reply_to(message, response, parse_mode="Markdown")
        logger.info(f"[Check] User {user_id} checked account: {email}")
        
    except ValueError:
        bot.reply_to(
            message,
            "❌ Format salah!\n\n"
            "Pastikan menggunakan separator `|` (pipe)\n"
            "Contoh: `/check email@gmail.com|password`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"[Check] Error: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['help'])
def help_cmd(message):
    """Command untuk bantuan"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if is_owner(user_id):
        text = (
            "📚 *Daftar Command*\n\n"
            "👤 *User Commands:*\n"
            "/start - Memulai bot\n"
            "/myprofile - Lihat profile Anda\n"
            "/setpassword - Set custom password\n"
            "/mypassword - Lihat custom password\n"
            "/deletepassword - Hapus custom password\n"
            "/check - Cek paket Vidio (format: email|password)\n"
            "/usage - Cek usage harian\n"
            "/checkcooldown - Cek status cooldown\n"
            "/help - Bantuan\n\n"
            "👑 *Owner Commands:*\n"
            "/addadmin <id> <days> - Tambah admin\n"
            "/addmemberadv <id> <days> - Tambah member\n"
            "/editadmin <id> <durasi> - Edit durasi admin\n"
            "/editmemberadv <id> - Edit durasi memberadv\n"
            "/deleteadmin <id> <durasi> - Hapus admin\n"
            "/deletememberadv <id> - Hapus memberadv\n"
            "/listusers - Lihat semua user\n"
            "/listadmin - Lihat daftar admin\n"
            "/listmember - Lihat daftar member\n"
            "/setrole <id> <role> - Set role user\n"
            "/deleterole <id> - Hapus/reset role user\n"
            "/listroles - List semua role\n"
            "/stats - Statistik bot\n"
            "/version - Info versi\n"
            "/adminmenu - Panel admin\n"
            "/broadcast - Broadcast pesan\n"
            "/resetusage - Reset usage user\n"
            "/sewabot - Paket sewa bot\n\n"
            "⚙️ *Settings Commands:*\n"
            "/setmaxaccounts <n> - Set max akun per request\n"
            "/setcooldown <t> <d> - Set cooldown\n"
            "/settings - Lihat settings\n\n"
            "💡 *Tip:* Ketik `/` untuk melihat daftar command"
        )
    elif role == ROLE_SUPER_ADMIN:
        text = (
            "📚 *Daftar Command*\n\n"
            "👤 *User Commands:*\n"
            "/start - Memulai bot\n"
            "/myprofile - Lihat profile Anda\n"
            "/setpassword - Set custom password\n"
            "/mypassword - Lihat custom password\n"
            "/deletepassword - Hapus custom password\n"
            "/check - Cek paket Vidio (format: email|password)\n"
            "/usage - Cek usage harian\n"
            "/checkcooldown - Cek status cooldown\n"
            "/help - Bantuan\n\n"
            "⭐ *Super Admin Commands:*\n"
            "/addadmin <id> <days> - Tambah admin\n"
            "/addmemberadv <id> <days> - Tambah member\n"
            "/editadmin <id> <durasi> - Edit durasi admin\n"
            "/editmemberadv <id> - Edit durasi memberadv\n"
            "/deleteadmin <id> <durasi> - Hapus admin\n"
            "/deletememberadv <id> - Hapus memberadv\n"
            "/adminmenu - Panel admin\n"
            "/broadcast - Broadcast pesan\n"
            "/resetusage - Reset usage user\n"
            "/stats - Statistik bot\n"
            "/listusers - Lihat semua user\n"
            "/version - Info versi\n\n"
            "💡 *Tip:* Ketik `/` untuk melihat daftar command"
        )
    elif role == ROLE_ADMIN:
        text = (
            "📚 *Daftar Command*\n\n"
            "👤 *User Commands:*\n"
            "/start - Memulai bot\n"
            "/myprofile - Lihat profile Anda\n"
            "/setpassword - Set custom password\n"
            "/mypassword - Lihat custom password\n"
            "/deletepassword - Hapus custom password\n"
            "/check - Cek paket Vidio (format: email|password)\n"
            "/usage - Cek usage harian\n"
            "/checkcooldown - Cek status cooldown\n"
            "/help - Bantuan\n\n"
            "🔧 *Admin Commands:*\n"
            "/addmember <id> - Tambah 1 member (max 1 user)\n"
            "/editmember <id> - Ganti member ID lama ke baru\n"
            "/deletemember - Hapus member\n\n"
            "💡 *Tip:* Ketik `/` untuk melihat daftar command"
        )
    else:
        text = (
            "📚 *Daftar Command*\n\n"
            "/start - Memulai bot\n"
            "/myprofile - Lihat profile Anda\n"
            "/setpassword - Set custom password\n"
            "/mypassword - Lihat custom password\n"
            "/deletepassword - Hapus custom password\n"
            "/usage - Cek usage harian\n"
            "/checkcooldown - Cek status cooldown\n"
            "/help - Bantuan\n"
            "/version - Versi bot\n"
            "/adminmenu - Panel admin\n\n"
            "💡 *Tip:* Ketik `/` untuk melihat daftar command"
        )
    
    bot.reply_to(message, text, parse_mode="Markdown")


# ===== OWNER COMMANDS =====

@bot.message_handler(commands=['listusers'])
def listusers_cmd(message):
    """Command untuk list semua user dengan pagination"""
    user_id = message.from_user.id
    
    # Check permission: Owner dan SuperAdmin only
    role = get_user_role(user_id)
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Role Owner dan SuperAdmin.")
        return
    
    # Get all users sorted by role
    users = list(users_collection.find().sort([("role", 1), ("user_id", 1)]))
    
    if not users:
        bot.reply_to(message, "ℹ️ Tidak ada User yang terdaftar.")
        return
    
    # Show page 1
    show_user_list_page(message.chat.id, users, page=0)


def show_user_list_page(chat_id, users, page=0):
    """Show user list with pagination"""
    per_page = 10
    total_users = len(users)
    total_pages = (total_users + per_page - 1) // per_page
    
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total_users)
    
    current_page_users = users[start_idx:end_idx]
    
    # Build message
    text = f"📋 *Daftar User (Halaman {page+1}/{total_pages}):*\n\n"
    
    for idx, user in enumerate(current_page_users, start=start_idx+1):
        user_id = user['user_id']
        username = user.get('username', 'Unknown')
        role = user.get('role', ROLE_FREE_USER)
        expire_date = user.get('expire_date')
        
        # Fallback ke expired_at jika expire_date tidak ada
        if not expire_date:
            expire_date = user.get('expired_at')
        
        # Role display
        role_emoji = {
            ROLE_OWNER: "👑",
            ROLE_SUPER_ADMIN: "⭐",
            ROLE_ADMIN: "👨‍💼",
            ROLE_MEMBER: "👤",
            ROLE_FREE_USER: "🆓"
        }
        role_display = f"{role_emoji.get(role, '👤')} {role}"
        
        if expire_date:
            if expire_date > datetime.now():
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "✅ Aktif"
            else:
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "❌ Expired"
        else:
            expire_str = "N/A"
            status = "♾️ Permanent"
        
        username_escaped = username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
        
        text += (
            f"{role_emoji.get(role, '👤')} *User {idx}*\n"
            f"• ID: `{user_id}`\n"
            f"• Username: @{username_escaped}\n"
            f"• Role: {role_display}\n"
            f"• Status: {status}\n"
            f"• Expire: {expire_str}\n\n"
        )
    
    # Create pagination buttons
    keyboard = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    
    if page > 0:
        buttons.append(types.InlineKeyboardButton("⬅️ Sebelumnya", callback_data=f"listusers_page_{page-1}"))
    
    buttons.append(types.InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="listusers_current"))
    
    if page < total_pages - 1:
        buttons.append(types.InlineKeyboardButton("➡️ Selanjutnya", callback_data=f"listusers_page_{page+1}"))
    
    keyboard.row(*buttons)
    
    # Export button
    keyboard.row(types.InlineKeyboardButton("📥 Export User List (txt)", callback_data="export_user_list"))
    
    bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode="Markdown")



@bot.message_handler(commands=['checkroles'])
def checkroles_cmd(message):
    """Command untuk cek status role user (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    
    # Check permission
    role = get_user_role(user_id)
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Role Owner dan SuperAdmin.")
        return
    
    # Parse command
    parts = message.text.strip().split()
    
    if len(parts) != 2:
        bot.reply_to(
            message,
            "❌ *Format salah!*\n\n"
            "*Cara pakai:*\n"
            "`/checkroles <user_id>`\n\n"
            "*Contoh:*\n"
            "`/checkroles 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka!")
        return
    
    # Get user from database
    target_user = users_collection.find_one({"user_id": target_user_id})
    
    if not target_user:
        bot.reply_to(
            message,
            f"❌ *USER NOT FOUND*\n\n"
            f"User dengan ID `{target_user_id}` tidak ditemukan di database.",
            parse_mode="Markdown"
        )
        return
    
    # Get user info
    username = target_user.get('username', 'Unknown')
    user_role = target_user.get('role', ROLE_FREE_USER)
    expired_at = target_user.get('expired_at')
    
    # Fallback ke expire_date jika expired_at tidak ada (untuk backward compatibility)
    if not expired_at:
        expired_at = target_user.get('expire_date')
    
    created_at = target_user.get('created_at')
    
    # Determine status
    now = datetime.utcnow()
    
    if expired_at:
        if now < expired_at:
            status = "✅ *AKTIF*"
            remaining_days = (expired_at - now).days
            remaining_hours = (expired_at - now).seconds // 3600
            remaining_str = f"{remaining_days} hari {remaining_hours} jam"
            expired_str = expired_at.strftime('%Y-%m-%d %H:%M:%S UTC')
        else:
            status = "❌ *EXPIRED*"
            days_expired = (now - expired_at).days
            remaining_str = f"Sudah expired {days_expired} hari yang lalu"
            expired_str = expired_at.strftime('%Y-%m-%d %H:%M:%S UTC')
    else:
        status = "♾️ *PERMANENT*"
        remaining_str = "Tidak ada batas waktu"
        expired_str = "-"
    
    # Role emoji
    role_emoji = {
        ROLE_OWNER: "👑",
        ROLE_SUPER_ADMIN: "⭐",
        ROLE_ADMIN: "👨‍💼",
        ROLE_MEMBER: "👤",
        ROLE_FREE_USER: "🆓"
    }
    
    role_display = f"{role_emoji.get(user_role, '👤')} {user_role}"
    
    username_escaped = username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
    
    # Build response
    response = (
        f"🔍 *CHECK ROLE STATUS*\n"
        f"{'='*35}\n\n"
        f"👤 *User Information:*\n"
        f"├ User ID: `{target_user_id}`\n"
        f"├ Username: @{username_escaped}\n"
        f"└ Role: {role_display}\n\n"
        f"📊 *Status:*\n"
        f"└ {status}\n\n"
    )
    
    if expired_at:
        response += (
            f"⏰ *Duration Info:*\n"
            f"├ Sisa Waktu: {remaining_str}\n"
            f"└ Expired At: `{expired_str}`\n\n"
        )
    else:
        response += (
            f"⏰ *Duration Info:*\n"
            f"└ {remaining_str}\n\n"
        )
    
    if created_at:
        created_str = created_at.strftime('%Y-%m-%d %H:%M:%S UTC')
        response += f"📅 *Created:* {created_str}\n"
    
    response += f"\n🕐 *Checked at:* {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    
    bot.reply_to(message, response, parse_mode="Markdown")
    
    # Log activity
    admin_username = message.from_user.username or "Unknown"
    logger.info(f"[CheckRoles] Admin @{admin_username} ({user_id}) checked role for user {target_user_id}")


@bot.message_handler(commands=['setrole'])
def setrole_cmd(message):
    """Command untuk set role user"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 3:
        text = (
            "❌ *Format Salah!*\n\n"
            "*Format:* `/setrole <user_id> <role>`\n\n"
            "*Role yang tersedia:*\n"
            "• `super_admin` - Super Admin (unlimited)\n"
            "• `admin` - Admin (2000 akun/hari)\n"
            "• `member` - Member (1000 akun/hari)\n\n"
            "*Contoh:*\n"
            "`/setrole 123456789 admin`"
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    try:
        target_user_id = int(parts[1])
        role = parts[2].lower()
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    valid_roles = {
        'super_admin': ROLE_SUPER_ADMIN,
        'admin': ROLE_ADMIN,
        'member': ROLE_MEMBER
    }
    
    if role not in valid_roles:
        bot.reply_to(message, f"❌ Role tidak valid! Pilih: super_admin, admin, atau member")
        return
    
    role_value = valid_roles[role]
    success = set_user_role(target_user_id, role_value)
    
    if success:
        role_display = get_role_display_name(role_value)
        limit_info = ""
        if role_value in [ROLE_ADMIN, ROLE_MEMBER]:
            limit = DAILY_LIMITS[role_value]
            limit_info = f"\nLimit: {limit} akun/hari"
        
        bot.reply_to(
            message,
            f"✅ *Role Berhasil Diset!*\n\n"
            f"User ID: `{target_user_id}`\n"
            f"Role: {role_display}{limit_info}",
            parse_mode="Markdown"
        )
    else:
        bot.reply_to(message, "❌ Gagal set role.")


@bot.message_handler(commands=['deleterole'])
def deleterole_cmd(message):
    """Command untuk hapus/reset role user"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/deleterole <user_id>`\n\n"
            "*Contoh:*\n"
            "`/deleterole 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    delete_user_role(target_user_id)
    bot.reply_to(
        message,
        f"✅ *Role Berhasil Dihapus!*\n\n"
        f"User ID: `{target_user_id}`\n"
        f"Role direset ke: 👤 Member",
        parse_mode="Markdown"
    )


@bot.message_handler(commands=['addadmin'])
def addadmin_cmd(message):
    """Command untuk tambah admin dengan durasi custom"""
    user_id = message.from_user.id
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan Super Admin.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/addadmin <user_id> <durasi_hari>`\n\n"
            "*Contoh:*\n"
            "`/addadmin 123456789` - Default 30 hari\n"
            "`/addadmin 123456789 60` - Custom 60 hari\n"
            "`/addadmin 123456789 365` - 1 tahun",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    # Parse durasi (default 30 hari)
    duration_days = 30
    if len(parts) >= 3:
        try:
            duration_days = int(parts[2])
            if duration_days < 1:
                bot.reply_to(message, "❌ Durasi minimal 1 hari.")
                return
        except ValueError:
            bot.reply_to(message, "❌ Durasi harus berupa angka.")
            return
    
    set_user_role(target_user_id, ROLE_ADMIN)
    
    # Aktifkan lisensi sesuai durasi
    expire_date = datetime.now() + timedelta(days=duration_days)
    users_collection.update_one(
        {"user_id": target_user_id},
        {
            "$set": {
                "expire_date": expire_date,
                "expired_at": expire_date,  # Untuk kompatibilitas dengan checkroles
                "added_by": user_id,
                "added_at": datetime.now()
            }
        }
    )
    
    expire_str = expire_date.strftime('%Y-%m-%d %H:%M:%S')
    
    bot.reply_to(
        message,
        f"✅ *Admin Berhasil Ditambahkan!*\n\n"
        f"User ID: `{target_user_id}`\n"
        f"Role: 🔧 Admin\n"
        f"Status Lisensi: ✅ Aktif\n"
        f"Durasi: {duration_days} hari\n"
        f"Expire: {expire_str}\n"
        f"Limit: 2000 akun/hari\n\n"
        f"Admin dapat menambahkan 1 member dengan /addmember",
        parse_mode="Markdown"
    )
    
    logger.info(f"[AddAdmin] Owner {user_id} added admin {target_user_id} for {duration_days} days")
    
    # Log dan kirim notifikasi ke target user
    try:
        # Get username
        try:
            target_user = bot.get_chat(target_user_id)
            target_username = target_user.username if target_user.username else None
        except:
            target_username = None
        
        owner_user = message.from_user
        owner_username = owner_user.username if owner_user.username else None
        
        # Send log dan notifikasi
        log_role_added(
            target_user_id=target_user_id,
            target_username=target_username,
            role=ROLE_ADMIN,
            days=duration_days,
            added_by_id=user_id,
            added_by_username=owner_username
        )
    except Exception as e:
        logger.error(f"[Logger] Failed to send notification: {e}")


@bot.message_handler(commands=['addmemberadv'])
def addmemberadv_cmd(message):
    """Command untuk tambah member dengan durasi custom"""
    user_id = message.from_user.id
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan Super Admin.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/addmemberadv <user_id> <durasi_hari>`\n\n"
            "*Contoh:*\n"
            "`/addmemberadv 123456789` - Default 30 hari\n"
            "`/addmemberadv 123456789 60` - Custom 60 hari\n"
            "`/addmemberadv 123456789 365` - 1 tahun",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    # Parse durasi (default 30 hari)
    duration_days = 30
    if len(parts) >= 3:
        try:
            duration_days = int(parts[2])
            if duration_days < 1:
                bot.reply_to(message, "❌ Durasi minimal 1 hari.")
                return
        except ValueError:
            bot.reply_to(message, "❌ Durasi harus berupa angka.")
            return
    
    set_user_role(target_user_id, ROLE_MEMBER)
    
    # Aktifkan lisensi sesuai durasi
    expire_date = datetime.now() + timedelta(days=duration_days)
    users_collection.update_one(
        {"user_id": target_user_id},
        {
            "$set": {
                "expire_date": expire_date,
                "expired_at": expire_date,  # Untuk kompatibilitas dengan checkroles
                "added_by": user_id,
                "added_at": datetime.now()
            }
        }
    )
    
    expire_str = expire_date.strftime('%Y-%m-%d %H:%M:%S')
    
    bot.reply_to(
        message,
        f"✅ *Member Berhasil Ditambahkan!*\n\n"
        f"User ID: `{target_user_id}`\n"
        f"Role: 👤 Member\n"
        f"Status Lisensi: ✅ Aktif\n"
        f"Durasi: {duration_days} hari\n"
        f"Expire: {expire_str}\n"
        f"Limit: 1000 akun/hari",
        parse_mode="Markdown"
    )
    
    logger.info(f"[AddMember] Owner {user_id} added member {target_user_id} for {duration_days} days")
    
    # Log dan kirim notifikasi ke target user
    try:
        # Get username
        try:
            target_user = bot.get_chat(target_user_id)
            target_username = target_user.username if target_user.username else None
        except:
            target_username = None
        
        owner_user = message.from_user
        owner_username = owner_user.username if owner_user.username else None
        
        # Send log dan notifikasi
        log_role_added(
            target_user_id=target_user_id,
            target_username=target_username,
            role=ROLE_MEMBER,
            days=duration_days,
            added_by_id=user_id,
            added_by_username=owner_username
        )
    except Exception as e:
        logger.error(f"[Logger] Failed to send notification: {e}")


@bot.message_handler(commands=['listroles'])
def listroles_cmd(message):
    """Command untuk list semua role"""
    user_id = message.from_user.id
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Role Owner dan Super Admin.")
        return
    
    users = list(users_collection.find({"role": {"$exists": True}}))
    
    if not users:
        bot.reply_to(message, "📋 Belum ada user dengan role khusus.")
        return
    
    text = "*📋 Daftar User & Role*\n\n"
    
    role_groups = {
        ROLE_SUPER_ADMIN: [],
        ROLE_ADMIN: [],
        ROLE_MEMBER: []
    }
    
    for user in users:
        role = user.get('role', ROLE_MEMBER)
        user_id_str = str(user['user_id'])
        username = user.get('username', 'Unknown')
        
        if role in role_groups:
            role_groups[role].append(f"• `{user_id_str}` - @{username}")
    
    if role_groups[ROLE_SUPER_ADMIN]:
        text += "*⭐ Super Admin:*\n"
        text += "\n".join(role_groups[ROLE_SUPER_ADMIN]) + "\n\n"
    
    if role_groups[ROLE_ADMIN]:
        text += "*🔧 Admin (2000/hari):*\n"
        text += "\n".join(role_groups[ROLE_ADMIN]) + "\n\n"
    
    if role_groups[ROLE_MEMBER]:
        text += "*👤 Member (1000/hari):*\n"
        text += "\n".join(role_groups[ROLE_MEMBER]) + "\n\n"
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['check'])
def check_account_cmd(message):
    """Command untuk check paket akun Vidio
    Format: /check atau reply dengan email|password
    """
    user_id = message.from_user.id
    
    # Kirim instruksi jika belum ada input
    if message.text.strip() == '/check':
        instruction_text = (
            "📋 *Check Paket Akun Vidio*\n\n"
            "Untuk mengecek paket akun Vidio, silakan kirim dalam format:\n"
            "`email|password`\n\n"
            "*Contoh:*\n"
            "`user123@gmail.com|Pass12345`\n\n"
            "Bot akan mengecek informasi akun seperti:\n"
            "• Email\n"
            "• Password\n"
            "• Plan (Premium/Basic/Free)\n"
            "• Expiry Date"
        )
        bot.reply_to(message, instruction_text, parse_mode="Markdown")
        return
    
    # Jika ada input langsung
    input_text = message.text.replace('/check', '').strip()
    if not input_text:
        instruction_text = (
            "📋 *Check Paket Akun Vidio*\n\n"
            "Untuk mengecek paket akun Vidio, silakan kirim dalam format:\n"
            "`email|password`\n\n"
            "*Contoh:*\n"
            "`user123@gmail.com|Pass12345`"
        )
        bot.reply_to(message, instruction_text, parse_mode="Markdown")
        return
    
    # Parse email dan password
    parts = input_text.split('|')
    if len(parts) != 2:
        bot.reply_to(
            message,
            "❌ Format salah!\n\n"
            "Gunakan format: `email|password`\n"
            "Contoh: `user123@gmail.com|Pass12345`",
            parse_mode="Markdown"
        )
        return
    
    email = parts[0].strip()
    password = parts[1].strip()
    
    if not email or not password:
        bot.reply_to(message, "❌ Email dan password tidak boleh kosong!")
        return
    
    # Send processing message
    processing_msg = bot.reply_to(
        message,
        "⏳ *Checking account...*\n"
        f"Email: `{email}`\n\n"
        "Mohon tunggu sebentar.",
        parse_mode="Markdown"
    )
    
    try:
        # Check account
        vidio = Vidio()
        result = vidio.check_account_info(email, password)
        vidio.close()
        
        # Delete processing message
        try:
            bot.delete_message(chat_id=user_id, message_id=processing_msg.message_id)
        except:
            pass
        
        if result and result.get('success'):
            # Success - show account info
            response_text = (
                "✅ *Account Information*\n\n"
                f"📧 Email: `{result['email']}`\n"
                f"🔐 Password: `{password}`\n"
                f"📦 Plan: *{result['plan']}*\n"
                f"📅 Expiry Date: `{result['expiry']}`\n"
                f"🟢 Status: *{result['status'].upper()}*"
            )
            bot.reply_to(message, response_text, parse_mode="Markdown")
        else:
            # Failed
            error_msg = result.get('error', 'Unknown error') if result else 'Failed to check account'
            bot.reply_to(
                message,
                f"❌ *Gagal mengecek akun!*\n\n"
                f"Email: `{email}`\n"
                f"Error: {error_msg}",
                parse_mode="Markdown"
            )
    
    except Exception as e:
        logger.error(f"Error in check_account_cmd: {e}")
        try:
            bot.delete_message(chat_id=user_id, message_id=processing_msg.message_id)
        except:
            pass
        bot.reply_to(
            message,
            f"❌ Terjadi error saat mengecek akun!\n"
            f"Error: {str(e)}",
            parse_mode="Markdown"
        )


@bot.message_handler(commands=['stats'])
def stats_cmd(message):
    """Command untuk statistik bot dengan format detail"""
    user_id = message.from_user.id
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Role Owner dan Super Admin.")
        return
    
    # Stats User
    total_users = users_collection.count_documents({})
    active_users = users_collection.count_documents({"expire_date": {"$gt": datetime.now()}})
    expired_users = users_collection.count_documents({
        "expire_date": {"$exists": True, "$lte": datetime.now()}
    })
    pending_users = users_collection.count_documents({"expire_date": {"$exists": False}})
    
    # Stats Role (hanya angka)
    total_super_admin = users_collection.count_documents({"role": ROLE_SUPER_ADMIN})
    total_admin = users_collection.count_documents({"role": ROLE_ADMIN})
    total_member = users_collection.count_documents({"role": ROLE_MEMBER})
    
    # Account Stats (dari usage_tracking)
    all_usage = list(usage_tracking_collection.find({}))
    total_accounts_created = sum([u.get('daily_usage', 0) for u in all_usage])
    
    # Anggap semua akun yang dibuat = success (bisa disesuaikan jika ada field status)
    success_accounts = total_accounts_created
    failed_accounts = 0  # Placeholder, bisa diupdate jika ada tracking failure
    pending_accounts = 0  # Placeholder
    
    # Transaction Stats
    total_transactions = transactions_collection.count_documents({})
    paid_transactions = transactions_collection.count_documents({"status": "paid"})
    pending_transactions = transactions_collection.count_documents({"status": "pending"})
    
    # Revenue
    total_revenue = 0
    paid_amount = 0
    pending_amount = 0
    
    paid_trans = list(transactions_collection.find({"status": "paid"}))
    for trans in paid_trans:
        amount = trans.get('amount', 0)
        total_revenue += amount
        paid_amount += amount
    
    pending_trans = list(transactions_collection.find({"status": "pending"}))
    for trans in pending_trans:
        pending_amount += trans.get('amount', 0)
    
    total_trans_amount = total_revenue + pending_amount
    
    text = (
        "🗂 *BOT STATISTIC*\n\n"
        "📊 *Stats User:*\n"
        f"👥 Total Users: {total_users}\n"
        f"✅ Active: {active_users}\n"
        f"❌ Expired: {expired_users}\n"
        f"⏳ Pending: {pending_users}\n\n"
        
        "🏷 *Role:*\n"
        f"🔰 Super-admin: {total_super_admin}\n"
        f"📞 Admin: {total_admin}\n"
        f"👤 Member: {total_member}\n\n"
        
        "📊 *Account Stats:*\n"
        f"✅ Success: {success_accounts}\n"
        f"❌ Failed: {failed_accounts}\n"
        f"⏳ Pending: {pending_accounts}\n\n"
        
        "💰 *Total Transaksi:* Rp {total_trans_amount:,}\n"
        f"✅ Paid: Rp {paid_amount:,}\n"
        f"⏳ Pending: {pending_amount:,}\n\n"
        
        f"💵 *Total Revenue:* Rp {total_revenue:,}"
    )
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['listadmin'])
def listadmin_cmd(message):
    """Command untuk list semua admin dengan pagination"""
    user_id = message.from_user.id
    
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    # Get all admins
    admins = list(users_collection.find({"role": ROLE_ADMIN}).sort("user_id", 1))
    
    if not admins:
        bot.reply_to(message, "ℹ️ Tidak ada Admin yang terdaftar.")
        return
    
    # Show page 1
    show_admin_list_page(message.chat.id, admins, page=0)


def show_admin_list_page(chat_id, admins, page=0):
    """Show admin list with pagination"""
    per_page = 10
    total_admins = len(admins)
    total_pages = (total_admins + per_page - 1) // per_page
    
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total_admins)
    
    current_page_admins = admins[start_idx:end_idx]
    
    # Build message
    text = f"📋 *Daftar Admin (Halaman {page+1}/{total_pages}):*\n\n"
    
    for idx, admin in enumerate(current_page_admins, start=start_idx+1):
        user_id = admin['user_id']
        username = admin.get('username', 'Unknown')
        expire_date = admin.get('expire_date')
        
        if expire_date:
            if expire_date > datetime.now():
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "✅ Aktif"
            else:
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "❌ Expired"
        else:
            expire_str = "N/A"
            status = "⏳ Pending"
        
        username_escaped = username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
        
        text += (
            f"👤 *Admin {idx}*\n"
            f"• ID: `{user_id}`\n"
            f"• Username: @{username_escaped}\n"
            f"• Status: {status}\n"
            f"• Expire: {expire_str}\n\n"
        )
    
    # Create pagination buttons
    keyboard = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    
    if page > 0:
        buttons.append(types.InlineKeyboardButton("⬅️ Sebelumnya", callback_data=f"listadmin_page_{page-1}"))
    
    buttons.append(types.InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="listadmin_current"))
    
    if page < total_pages - 1:
        buttons.append(types.InlineKeyboardButton("➡️ Selanjutnya", callback_data=f"listadmin_page_{page+1}"))
    
    keyboard.row(*buttons)
    
    # Export button
    keyboard.row(types.InlineKeyboardButton("📥 Export Admin List (txt)", callback_data="export_admin_list"))
    
    bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode="Markdown")


@bot.message_handler(commands=['listmember'])
def listmember_cmd(message):
    """Command untuk list semua member dengan pagination"""
    user_id = message.from_user.id
    
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    # Get all members
    members = list(users_collection.find({"role": ROLE_MEMBER}).sort("user_id", 1))
    
    if not members:
        bot.reply_to(message, "ℹ️ Tidak ada Member yang terdaftar.")
        return
    
    # Show page 1
    show_member_list_page(message.chat.id, members, page=0)


def show_member_list_page(chat_id, members, page=0):
    """Show member list with pagination"""
    per_page = 10
    total_members = len(members)
    total_pages = (total_members + per_page - 1) // per_page
    
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total_members)
    
    current_page_members = members[start_idx:end_idx]
    
    # Build message
    text = f"📋 *Daftar Member (Halaman {page+1}/{total_pages}):*\n\n"
    
    for idx, member in enumerate(current_page_members, start=start_idx+1):
        user_id = member['user_id']
        username = member.get('username', 'Unknown')
        expire_date = member.get('expire_date')
        
        if expire_date:
            if expire_date > datetime.now():
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "✅ Aktif"
            else:
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "❌ Expired"
        else:
            expire_str = "N/A"
            status = "⏳ Pending"
        
        username_escaped = username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
        
        text += (
            f"👤 *Member {idx}*\n"
            f"• ID: `{user_id}`\n"
            f"• Username: @{username_escaped}\n"
            f"• Status: {status}\n"
            f"• Expire: {expire_str}\n\n"
        )
    
    # Create pagination buttons
    keyboard = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    
    if page > 0:
        buttons.append(types.InlineKeyboardButton("⬅️ Sebelumnya", callback_data=f"listmember_page_{page-1}"))
    
    buttons.append(types.InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="listmember_current"))
    
    if page < total_pages - 1:
        buttons.append(types.InlineKeyboardButton("➡️ Selanjutnya", callback_data=f"listmember_page_{page+1}"))
    
    keyboard.row(*buttons)
    
    # Export button
    keyboard.row(types.InlineKeyboardButton("📥 Export Member List (txt)", callback_data="export_member_list"))
    
    bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode="Markdown")


# Callback handlers untuk pagination
@bot.callback_query_handler(func=lambda call: call.data.startswith("listadmin_page_"))
def handle_listadmin_pagination(call):
    """Handle pagination untuk list admin"""
    page = int(call.data.split("_")[-1])
    
    # Get all admins
    admins = list(users_collection.find({"role": ROLE_ADMIN}).sort("user_id", 1))
    
    # Delete old message
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    
    # Show new page
    show_admin_list_page(call.message.chat.id, admins, page=page)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("listmember_page_"))
def handle_listmember_pagination(call):
    """Handle pagination untuk list member"""
    page = int(call.data.split("_")[-1])
    
    # Get all members
    members = list(users_collection.find({"role": ROLE_MEMBER}).sort("user_id", 1))
    
    # Delete old message
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    
    # Show new page
    show_member_list_page(call.message.chat.id, members, page=page)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "export_admin_list")
def handle_export_admin_list(call):
    """Export admin list ke TXT"""
    admins = list(users_collection.find({"role": ROLE_ADMIN}).sort("user_id", 1))
    
    if not admins:
        bot.answer_callback_query(call.id, "Tidak ada admin untuk di-export.")
        return
    
    # Create TXT content
    txt_content = "📋 DAFTAR ADMIN\n"
    txt_content += "="*50 + "\n\n"
    
    for idx, admin in enumerate(admins, 1):
        user_id = admin['user_id']
        username = admin.get('username', 'Unknown')
        expire_date = admin.get('expire_date')
        
        if expire_date:
            if expire_date > datetime.now():
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "Aktif"
            else:
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "Expired"
        else:
            expire_str = "N/A"
            status = "Pending"
        
        txt_content += f"Admin {idx}\n"
        txt_content += f"  ID       : {user_id}\n"
        txt_content += f"  Username : @{username}\n"
        txt_content += f"  Status   : {status}\n"
        txt_content += f"  Expire   : {expire_str}\n"
        txt_content += "\n"
    
    txt_content += "="*50 + "\n"
    txt_content += f"Total: {len(admins)} Admin\n"
    txt_content += f"Generated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
    
    # Save to temp file
    filename = f"admin_list_{int(time.time())}.txt"
    filepath = f"/tmp/{filename}"
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(txt_content)
    
    # Send file
    with open(filepath, 'rb') as f:
        bot.send_document(
            call.message.chat.id,
            f,
            visible_file_name=f"admin_list_{datetime.now().strftime('%Y%m%d')}.txt",
            caption=f"📋 *Daftar Admin*\n\nTotal: {len(admins)} Admin",
            parse_mode="Markdown"
        )
    
    # Delete temp file
    os.remove(filepath)
    
    bot.answer_callback_query(call.id, "✅ Admin list berhasil di-export!")


@bot.callback_query_handler(func=lambda call: call.data == "export_member_list")
def handle_export_member_list(call):
    """Export member list ke TXT"""
    members = list(users_collection.find({"role": ROLE_MEMBER}).sort("user_id", 1))
    
    if not members:
        bot.answer_callback_query(call.id, "Tidak ada member untuk di-export.")
        return
    
    # Create TXT content
    txt_content = "📋 DAFTAR MEMBER\n"
    txt_content += "="*50 + "\n\n"
    
    for idx, member in enumerate(members, 1):
        user_id = member['user_id']
        username = member.get('username', 'Unknown')
        expire_date = member.get('expire_date')
        
        if expire_date:
            if expire_date > datetime.now():
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "Aktif"
            else:
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "Expired"
        else:
            expire_str = "N/A"
            status = "Pending"
        
        txt_content += f"Member {idx}\n"
        txt_content += f"  ID       : {user_id}\n"
        txt_content += f"  Username : @{username}\n"
        txt_content += f"  Status   : {status}\n"
        txt_content += f"  Expire   : {expire_str}\n"
        txt_content += "\n"
    
    txt_content += "="*50 + "\n"
    txt_content += f"Total: {len(members)} Member\n"
    txt_content += f"Generated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
    
    # Save to temp file
    filename = f"member_list_{int(time.time())}.txt"
    filepath = f"/tmp/{filename}"
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(txt_content)
    
    # Send file
    with open(filepath, 'rb') as f:
        bot.send_document(
            call.message.chat.id,
            f,
            visible_file_name=f"member_list_{datetime.now().strftime('%Y%m%d')}.txt",
            caption=f"📋 *Daftar Member*\n\nTotal: {len(members)} Member",
            parse_mode="Markdown"
        )
    
    # Delete temp file
    os.remove(filepath)
    
    bot.answer_callback_query(call.id, "✅ Member list berhasil di-export!")


@bot.callback_query_handler(func=lambda call: call.data.startswith("listusers_page_"))
def handle_listusers_pagination(call):
    """Handle pagination untuk list users"""
    try:
        page = int(call.data.split("_")[-1])
        
        # Get all users
        users = list(users_collection.find().sort([("role", 1), ("user_id", 1)]))
        
        # Delete old message
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            logger.debug(f"Cannot delete message: {e}")
        
        # Show new page
        show_user_list_page(call.message.chat.id, users, page=page)
        
        # PENTING: Answer callback query
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Error in pagination: {e}")
        try:
            bot.answer_callback_query(call.id, "❌ Error loading page")
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "listusers_current")
def handle_listusers_current_page(call):
    """Handle click on current page button"""
    bot.answer_callback_query(call.id, "📄 Anda sudah di halaman ini")


@bot.callback_query_handler(func=lambda call: call.data == "export_user_list")
def handle_export_user_list(call):
    """Export user list ke TXT"""
    users = list(users_collection.find().sort([("role", 1), ("user_id", 1)]))
    
    if not users:
        bot.answer_callback_query(call.id, "Tidak ada user untuk di-export.")
        return
    
    # Create TXT content
    txt_content = "📋 DAFTAR USER\n"
    txt_content += "="*50 + "\n\n"
    
    for idx, user in enumerate(users, 1):
        user_id = user['user_id']
        username = user.get('username', 'Unknown')
        role = user.get('role', ROLE_FREE_USER)
        expire_date = user.get('expire_date')
        
        # Fallback ke expired_at
        if not expire_date:
            expire_date = user.get('expired_at')
        
        # Role display
        role_names = {
            ROLE_OWNER: "Owner",
            ROLE_SUPER_ADMIN: "Super Admin",
            ROLE_ADMIN: "Admin",
            ROLE_MEMBER: "Member",
            ROLE_FREE_USER: "Free User"
        }
        role_display = role_names.get(role, "Free User")
        
        if expire_date:
            if expire_date > datetime.now():
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "Aktif"
            else:
                expire_str = expire_date.strftime('%d/%m/%Y')
                status = "Expired"
        else:
            expire_str = "N/A"
            status = "Permanent"
        
        txt_content += f"User {idx}\n"
        txt_content += f"  ID       : {user_id}\n"
        txt_content += f"  Username : @{username}\n"
        txt_content += f"  Role     : {role_display}\n"
        txt_content += f"  Status   : {status}\n"
        txt_content += f"  Expire   : {expire_str}\n"
        txt_content += "\n"
    
    txt_content += "="*50 + "\n"
    txt_content += f"Total: {len(users)} User\n"
    txt_content += f"Generated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
    
    # Save to temp file
    filename = f"user_list_{int(time.time())}.txt"
    filepath = f"/tmp/{filename}"
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(txt_content)
    
    # Send file
    with open(filepath, 'rb') as f:
        bot.send_document(
            call.message.chat.id,
            f,
            visible_file_name=f"user_list_{datetime.now().strftime('%Y%m%d')}.txt",
            caption=f"📋 *Daftar User*\n\nTotal: {len(users)} User",
            parse_mode="Markdown"
        )
    
    # Delete temp file
    os.remove(filepath)
    
    bot.answer_callback_query(call.id, "✅ User list berhasil di-export!")


# ===== ADMIN COMMANDS =====

@bot.message_handler(commands=['addmember'])
def addmember_cmd(message):
    """Command untuk admin tambah 1 member (maksimal 1 member per admin)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role != ROLE_ADMIN:
        bot.reply_to(message, "❌ Command ini hanya untuk role Admin.")
        return
    
    # Cek apakah admin sudah menambahkan member
    added_count = users_collection.count_documents({"added_by": user_id})
    
    if added_count >= 1:
        # Admin sudah add 1 member
        existing_member = users_collection.find_one({"added_by": user_id})
        member_id = existing_member.get('user_id', 'Unknown')
        
        bot.reply_to(
            message,
            f"❌ *Limit Tercapai!*\n\n"
            f"Anda sudah menambahkan 1 member:\n"
            f"Member ID: `{member_id}`\n\n"
            f"💡 Admin hanya dapat menambahkan *1 member* saja.\n\n"
            f"Gunakan:\n"
            f"• `/editmember <user_id_baru>` - Ganti member ID\n"
            f"• `/deletemember` - Hapus member untuk add yang baru",
            parse_mode="Markdown"
        )
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        text = (
            "❌ *Format Salah!*\n\n"
            "*Format:* `/addmember <user_id>`\n\n"
            "*Contoh:*\n"
            "`/addmember 123456789`\n\n"
            "💡 Member akan mendapat durasi yang sama dengan durasi Admin Anda.\n"
            "⚠️ Admin hanya dapat menambahkan *1 member* saja."
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    # Validasi: Tidak bisa add diri sendiri
    if target_user_id == user_id:
        bot.reply_to(message, "❌ Anda tidak dapat menambahkan diri sendiri sebagai member!")
        return
    
    admin_subscription = get_user_subscription(user_id)
    if not admin_subscription:
        bot.reply_to(message, "❌ Anda tidak memiliki subscription aktif!")
        return
    
    admin_expire = admin_subscription['expire_date']
    
    # Cek apakah user sudah punya role
    existing_user = users_collection.find_one({"user_id": target_user_id})
    
    # Set role hanya jika user belum punya role (user baru)
    # Jika user sudah punya role (misalnya Admin/Super Admin), jangan overwrite
    if existing_user and 'role' in existing_user:
        # User sudah ada dan punya role, hanya update expire_date
        users_collection.update_one(
            {"user_id": target_user_id},
            {
                "$set": {
                    "expire_date": admin_expire,
                    "added_by": user_id,
                    "added_at": datetime.now()
                }
            }
        )
    else:
        # User baru atau belum punya role, set role Member
        users_collection.update_one(
            {"user_id": target_user_id},
            {
                "$set": {
                    "expire_date": admin_expire,
                    "role": ROLE_MEMBER,
                    "added_by": user_id,
                    "added_at": datetime.now()
                }
            },
            upsert=True
        )
    
    expire_str = admin_expire.strftime('%Y-%m-%d %H:%M:%S')
    
    text = (
        f"✅ *Member Berhasil Ditambahkan!*\n\n"
        f"User ID: `{target_user_id}`\n"
        f"Role: 👤 Member\n"
        f"Status Lisensi: ✅ Aktif\n"
        f"Expire: {expire_str}\n"
        f"Limit: 1000 akun/hari\n\n"
        f"⚠️ Anda telah menggunakan slot member (1/1)\n\n"
        f"Gunakan:\n"
        f"• `/editmember <user_id_baru>` - Ganti member\n"
        f"• `/deletemember` - Hapus member"
    )
    
    bot.reply_to(message, text, parse_mode="Markdown")
    logger.info(f"[AddMember] Admin {user_id} added member {target_user_id}")
    
    # Log ke grup dan kirim notifikasi enhanced
    try:
        # Get username
        try:
            target_user = bot.get_chat(target_user_id)
            target_username = target_user.username if target_user.username else None
        except:
            target_username = None
        
        admin_user = message.from_user
        admin_username = admin_user.username if admin_user.username else None
        
        # Calculate duration days
        duration_days = (admin_expire - datetime.now()).days
        
        # Send log dan notifikasi
        log_member_added(
            target_user_id=target_user_id,
            target_username=target_username,
            days=duration_days,
            added_by_id=user_id,
            added_by_username=admin_username
        )
    except Exception as e:
        logger.error(f"[Logger] Failed to send log: {e}")
        
        # Fallback: Kirim notifikasi sederhana jika log gagal
        try:
            notify_text = (
                f"🎉 *Selamat!*\n\n"
                f"Anda telah ditambahkan sebagai Member oleh Admin.\n\n"
                f"Role: 👤 Member\n"
                f"Status Lisensi: ✅ Aktif\n"
                f"Expire: {expire_str}\n\n"
                f"Gunakan /start untuk mulai menggunakan bot!"
            )
            bot.send_message(target_user_id, notify_text, parse_mode="Markdown")
        except Exception as e2:
            logger.warning(f"Could not notify user {target_user_id}: {e2}")


@bot.message_handler(commands=['deletemember'])
def deletemember_cmd(message):
    """Command untuk admin hapus member yang sudah ditambahkan"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role != ROLE_ADMIN:
        bot.reply_to(message, "❌ Command ini hanya untuk role Admin.")
        return
    
    # Cari member yang ditambahkan oleh admin ini
    member = users_collection.find_one({"added_by": user_id})
    
    if not member:
        bot.reply_to(
            message,
            "ℹ️ *Tidak Ada Member*\n\n"
            "Anda belum menambahkan member apapun.\n\n"
            "Gunakan `/addmember <user_id>` untuk menambahkan member.",
            parse_mode="Markdown"
        )
        return
    
    member_id = member['user_id']
    member_role = member.get('role', 'Unknown')
    
    # Hapus member (unset added_by dan expire_date, reset ke Free User jika role Member)
    if member_role == ROLE_MEMBER:
        # Reset ke Free User
        users_collection.update_one(
            {"user_id": member_id},
            {
                "$set": {
                    "role": ROLE_FREE_USER
                },
                "$unset": {
                    "added_by": "",
                    "added_at": "",
                    "expire_date": ""
                }
            }
        )
    else:
        # Role lain (Admin/Super Admin), hanya unset added_by
        users_collection.update_one(
            {"user_id": member_id},
            {
                "$unset": {
                    "added_by": "",
                    "added_at": ""
                }
            }
        )
    
    bot.reply_to(
        message,
        f"✅ *Member Berhasil Dihapus!*\n\n"
        f"Member ID: `{member_id}`\n\n"
        f"Sekarang Anda dapat menambahkan member baru dengan `/addmember`",
        parse_mode="Markdown"
    )
    
    logger.info(f"[DeleteMember] Admin {user_id} deleted member {member_id}")
    
    # Notifikasi ke member yang dihapus
    try:
        notify_text = (
            f"ℹ️ *Pemberitahuan*\n\n"
            f"Anda telah dihapus dari daftar member.\n\n"
            f"Role Anda sekarang: 🆓 Free User\n"
            f"Limit: 5 akun (lifetime)\n\n"
            f"Gunakan /sewabot untuk upgrade kembali."
        )
        bot.send_message(member_id, notify_text, parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Could not notify user {member_id}: {e}")


@bot.message_handler(commands=['editmember'])
def editmember_cmd(message):
    """Command untuk admin ganti member ID yang sudah ditambahkan"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role != ROLE_ADMIN:
        bot.reply_to(message, "❌ Command ini hanya untuk role Admin.")
        return
    
    # Cari member yang ditambahkan oleh admin ini
    old_member = users_collection.find_one({"added_by": user_id})
    
    if not old_member:
        bot.reply_to(
            message,
            "ℹ️ *Tidak Ada Member*\n\n"
            "Anda belum menambahkan member apapun.\n\n"
            "Gunakan `/addmember <user_id>` untuk menambahkan member.",
            parse_mode="Markdown"
        )
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        old_member_id = old_member['user_id']
        text = (
            "❌ *Format Salah!*\n\n"
            "*Format:* `/editmember <user_id_baru>`\n\n"
            "*Contoh:*\n"
            "`/editmember 987654321`\n\n"
            f"💡 Member lama: `{old_member_id}`\n"
            f"Ganti dengan user ID baru."
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    try:
        new_member_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    old_member_id = old_member['user_id']
    
    # Validasi: Tidak bisa add diri sendiri
    if new_member_id == user_id:
        bot.reply_to(message, "❌ Anda tidak dapat menambahkan diri sendiri sebagai member!")
        return
    
    # Validasi: New member tidak sama dengan old member
    if new_member_id == old_member_id:
        bot.reply_to(message, "❌ User ID baru sama dengan user ID lama!")
        return
    
    admin_subscription = get_user_subscription(user_id)
    if not admin_subscription:
        bot.reply_to(message, "❌ Anda tidak memiliki subscription aktif!")
        return
    
    admin_expire = admin_subscription['expire_date']
    
    # Hapus member lama (reset ke Free User jika role Member)
    old_member_role = old_member.get('role', 'Unknown')
    if old_member_role == ROLE_MEMBER:
        users_collection.update_one(
            {"user_id": old_member_id},
            {
                "$set": {
                    "role": ROLE_FREE_USER
                },
                "$unset": {
                    "added_by": "",
                    "added_at": "",
                    "expire_date": ""
                }
            }
        )
    else:
        users_collection.update_one(
            {"user_id": old_member_id},
            {
                "$unset": {
                    "added_by": "",
                    "added_at": ""
                }
            }
        )
    
    # Tambah member baru
    existing_user = users_collection.find_one({"user_id": new_member_id})
    
    if existing_user and 'role' in existing_user:
        # User sudah ada dan punya role, hanya update expire_date
        users_collection.update_one(
            {"user_id": new_member_id},
            {
                "$set": {
                    "expire_date": admin_expire,
                    "added_by": user_id,
                    "added_at": datetime.now()
                }
            }
        )
    else:
        # User baru, set role Member
        users_collection.update_one(
            {"user_id": new_member_id},
            {
                "$set": {
                    "expire_date": admin_expire,
                    "role": ROLE_MEMBER,
                    "added_by": user_id,
                    "added_at": datetime.now()
                }
            },
            upsert=True
        )
    
    expire_str = admin_expire.strftime('%Y-%m-%d %H:%M:%S')
    
    bot.reply_to(
        message,
        f"✅ *Member Berhasil Diganti!*\n\n"
        f"Member Lama: `{old_member_id}` ❌\n"
        f"Member Baru: `{new_member_id}` ✅\n\n"
        f"Role: 👤 Member\n"
        f"Status Lisensi: ✅ Aktif\n"
        f"Expire: {expire_str}",
        parse_mode="Markdown"
    )
    
    logger.info(f"[EditMember] Admin {user_id} changed member from {old_member_id} to {new_member_id}")
    
    # Notifikasi ke member lama
    try:
        notify_old = (
            f"ℹ️ *Pemberitahuan*\n\n"
            f"Anda telah dihapus dari daftar member.\n\n"
            f"Role Anda sekarang: 🆓 Free User\n"
            f"Limit: 5 akun (lifetime)"
        )
        bot.send_message(old_member_id, notify_old, parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Could not notify old member {old_member_id}: {e}")
    
    # Notifikasi ke member baru
    try:
        notify_new = (
            f"🎉 *Selamat!*\n\n"
            f"Anda telah ditambahkan sebagai Member!\n\n"
            f"Role: 👤 Member\n"
            f"Status Lisensi: ✅ Aktif\n"
            f"Expire: {expire_str}\n\n"
            f"Gunakan /start untuk mulai!"
        )
        bot.send_message(new_member_id, notify_new, parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Could not notify new member {new_member_id}: {e}")




# ==================== RENEW & MANAGEMENT COMMANDS ====================

@bot.message_handler(commands=['renewadmin'])
def renewadmin_cmd(message):
    """Perpanjang durasi admin (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Role Owner dan SuperAdmin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/renewadmin <user_id> <hari>`\n\n"
                "*Contoh:*\n"
                "`/renewadmin 123456789 30`",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        days = int(parts[2])
        
        if days <= 0:
            bot.reply_to(message, "❌ Durasi harus lebih dari 0 hari!")
            return
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_ADMIN:
            bot.reply_to(message, f"❌ User ini bukan Admin! (Role: {target_role})")
            return
        
        # Perpanjang durasi
        current_expire = target_user.get("expired_at")
        if current_expire and isinstance(current_expire, datetime) and current_expire > datetime.utcnow():
            new_expire = current_expire + timedelta(days=days)
        else:
            new_expire = datetime.utcnow() + timedelta(days=days)
        
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"expired_at": new_expire}}
        )
        
        # Get usernames
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *ADMIN DIPERPANJANG*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"⏰ Diperpanjang: +{days} hari\n"
            f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Notifikasi telah dikirim.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"🎉 *AKUN ADMIN DIPERPANJANG*\n\n"
                f"Akun admin Anda telah diperpanjang oleh @{admin_username}!\n\n"
                f"⏰ Durasi: +{days} hari\n"
                f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}\n\n"
                f"Terima kasih! 🙏",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_renewal(user_id, admin_username, target_user_id, target_username, ROLE_ADMIN, days, new_expire)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID dan durasi harus angka.")
    except Exception as e:
        logger.error(f"Error in renewadmin_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['renewmemberadv'])
def renewmemberadv_cmd(message):
    """Perpanjang durasi member dengan custom days (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan SuperAdmin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/renewmemberadv <user_id> <hari>`\n\n"
                "*Contoh:*\n"
                "`/renewmemberadv 123456789 15`",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        days = int(parts[2])
        
        if days <= 0:
            bot.reply_to(message, "❌ Durasi harus lebih dari 0 hari!")
            return
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_MEMBER:
            bot.reply_to(message, f"❌ User ini bukan Member! (Role: {target_role})")
            return
        
        # Perpanjang durasi
        current_expire = target_user.get("expired_at")
        if current_expire and isinstance(current_expire, datetime) and current_expire > datetime.utcnow():
            new_expire = current_expire + timedelta(days=days)
        else:
            new_expire = datetime.utcnow() + timedelta(days=days)
        
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"expired_at": new_expire}}
        )
        
        # Get usernames
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *MEMBER DIPERPANJANG*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"⏰ Diperpanjang: +{days} hari\n"
            f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Notifikasi telah dikirim.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"🎉 *AKUN MEMBER DIPERPANJANG*\n\n"
                f"Akun member Anda telah diperpanjang oleh @{admin_username}!\n\n"
                f"⏰ Durasi: +{days} hari\n"
                f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}\n\n"
                f"Terima kasih! 🙏",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_renewal(user_id, admin_username, target_user_id, target_username, ROLE_MEMBER, days, new_expire)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID dan durasi harus angka.")
    except Exception as e:
        logger.error(f"Error in renewmemberadv_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['renewmember'])
def renewmember_cmd(message):
    """Perpanjang member mengikuti masa aktif admin (Admin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_ADMIN, ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Admin, SuperAdmin, dan Owner.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/renewmember <user_id>`\n\n"
                "*Contoh:*\n"
                "`/renewmember 123456789`\n\n"
                "Member akan diperpanjang mengikuti sisa masa aktif Anda.",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_MEMBER:
            bot.reply_to(message, f"❌ User ini bukan Member! (Role: {target_role})")
            return
        
        # Get admin expiry
        admin_user = users_collection.find_one({"user_id": user_id})
        admin_expiry = admin_user.get("expired_at")
        
        if not admin_expiry:
            bot.reply_to(
                message,
                "❌ *Tidak bisa auto-renew!*\n\n"
                "Akun Anda berstatus permanent.\n"
                "Gunakan `/renewmemberadv` untuk perpanjang manual.",
                parse_mode="Markdown"
            )
            return
        
        now = datetime.utcnow()
        if now >= admin_expiry:
            bot.reply_to(message, "❌ Akun Anda sudah expired!")
            return
        
        remaining_days = (admin_expiry - now).days
        if remaining_days <= 0:
            bot.reply_to(message, "❌ Sisa masa aktif Anda kurang dari 1 hari!")
            return
        
        # Set member expire sama dengan admin expire
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"expired_at": admin_expiry}}
        )
        
        # Get usernames
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *MEMBER DIPERPANJANG (AUTO)*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"⏰ Durasi: {remaining_days} hari (mengikuti masa aktif Anda)\n"
            f"📅 Expire: {admin_expiry.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Notifikasi telah dikirim.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"🎉 *AKUN MEMBER DIPERPANJANG*\n\n"
                f"Akun member Anda telah diperpanjang oleh @{admin_username}!\n\n"
                f"⏰ Durasi: {remaining_days} hari\n"
                f"📅 Expire: {admin_expiry.strftime('%Y-%m-%d %H:%M')}\n\n"
                f"Terima kasih! 🙏",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_renewal(user_id, admin_username, target_user_id, target_username, ROLE_MEMBER, remaining_days, admin_expiry)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID harus angka.")
    except Exception as e:
        logger.error(f"Error in renewmember_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['editadmin'])
def editadmin_cmd(message):
    """Edit durasi admin (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan SuperAdmin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/editadmin <user_id> <hari>`\n\n"
                "*Contoh:*\n"
                "`/editadmin 123456789 30`\n\n"
                "Ini akan mengubah durasi menjadi 30 hari dari sekarang.",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        days = int(parts[2])
        
        if days <= 0:
            bot.reply_to(message, "❌ Durasi harus lebih dari 0 hari!")
            return
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_ADMIN:
            bot.reply_to(message, f"❌ User ini bukan Admin! (Role: {target_role})")
            return
        
        # Set durasi baru (dari sekarang)
        new_expire = datetime.utcnow() + timedelta(days=days)
        
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"expired_at": new_expire}}
        )
        
        # Get usernames
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *ADMIN DIEDIT*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"⏰ Durasi Baru: {days} hari\n"
            f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Notifikasi telah dikirim.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"✏️ *AKUN ADMIN DIEDIT*\n\n"
                f"Durasi akun admin Anda telah diubah oleh @{admin_username}!\n\n"
                f"⏰ Durasi Baru: {days} hari\n"
                f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_edit(user_id, admin_username, target_user_id, target_username, ROLE_ADMIN, days, new_expire)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID dan durasi harus angka.")
    except Exception as e:
        logger.error(f"Error in editadmin_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['editmemberadv'])
def editmemberadv_cmd(message):
    """Edit durasi member (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan SuperAdmin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/editmemberadv <user_id> <hari>`\n\n"
                "*Contoh:*\n"
                "`/editmemberadv 123456789 15`\n\n"
                "Ini akan mengubah durasi menjadi 15 hari dari sekarang.",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        days = int(parts[2])
        
        if days <= 0:
            bot.reply_to(message, "❌ Durasi harus lebih dari 0 hari!")
            return
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_MEMBER:
            bot.reply_to(message, f"❌ User ini bukan Member! (Role: {target_role})")
            return
        
        # Set durasi baru (dari sekarang)
        new_expire = datetime.utcnow() + timedelta(days=days)
        
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"expired_at": new_expire}}
        )
        
        # Get usernames
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *MEMBER DIEDIT*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"⏰ Durasi Baru: {days} hari\n"
            f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Notifikasi telah dikirim.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"✏️ *AKUN MEMBER DIEDIT*\n\n"
                f"Durasi akun member Anda telah diubah oleh @{admin_username}!\n\n"
                f"⏰ Durasi Baru: {days} hari\n"
                f"📅 Expire Baru: {new_expire.strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_edit(user_id, admin_username, target_user_id, target_username, ROLE_MEMBER, days, new_expire)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID dan durasi harus angka.")
    except Exception as e:
        logger.error(f"Error in editmemberadv_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['deleteadmin'])
def deleteadmin_cmd(message):
    """Hapus admin (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan SuperAdmin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/deleteadmin <user_id>`\n\n"
                "*Contoh:*\n"
                "`/deleteadmin 123456789`\n\n"
                "⚠️ Admin akan dihapus dan diubah menjadi User biasa!",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_ADMIN:
            bot.reply_to(message, f"❌ User ini bukan Admin! (Role: {target_role})")
            return
        
        # Get username before deletion
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Delete admin (change to User)
        users_collection.update_one(
            {"user_id": target_user_id},
            {
                "$set": {"role": ROLE_FREE_USER},
                "$unset": {"expired_at": ""}
            }
        )
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *ADMIN DIHAPUS*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"🎭 Role Lama: Admin\n"
            f"🎭 Role Baru: User\n\n"
            f"User telah diubah menjadi User biasa.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"⚠️ *AKUN ADMIN DIHAPUS*\n\n"
                f"Akun admin Anda telah dihapus oleh @{admin_username}.\n\n"
                f"Role Anda sekarang: User",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_deletion(user_id, admin_username, target_user_id, target_username, ROLE_ADMIN)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID harus angka.")
    except Exception as e:
        logger.error(f"Error in deleteadmin_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['deletememberadv'])
def deletememberadv_cmd(message):
    """Hapus member (Owner & SuperAdmin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan SuperAdmin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/deletememberadv <user_id>`\n\n"
                "*Contoh:*\n"
                "`/deletememberadv 123456789`\n\n"
                "⚠️ Member akan dihapus dan diubah menjadi User biasa!",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = int(parts[1])
        
        # Check target user
        target_user = users_collection.find_one({"user_id": target_user_id})
        if not target_user:
            bot.reply_to(message, f"❌ User `{target_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        target_role = target_user.get("role", ROLE_FREE_USER)
        if target_role != ROLE_MEMBER:
            bot.reply_to(message, f"❌ User ini bukan Member! (Role: {target_role})")
            return
        
        # Get username before deletion
        target_username = target_user.get("username", "Unknown")
        admin_username = message.from_user.username or "Unknown"
        
        # Delete member (change to User)
        users_collection.update_one(
            {"user_id": target_user_id},
            {
                "$set": {"role": ROLE_FREE_USER},
                "$unset": {"expired_at": ""}
            }
        )
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *MEMBER DIHAPUS*\n\n"
            f"👤 Target: @{target_username} (`{target_user_id}`)\n"
            f"🎭 Role Lama: Member\n"
            f"🎭 Role Baru: User\n\n"
            f"User telah diubah menjadi User biasa.",
            parse_mode="Markdown"
        )
        
        # Notify target
        try:
            bot.send_message(
                target_user_id,
                f"⚠️ *AKUN MEMBER DIHAPUS*\n\n"
                f"Akun member Anda telah dihapus oleh @{admin_username}.\n\n"
                f"Role Anda sekarang: User",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_deletion(user_id, admin_username, target_user_id, target_username, ROLE_MEMBER)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID harus angka.")
    except Exception as e:
        logger.error(f"Error in deletememberadv_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")


@bot.message_handler(commands=['replacemember'])
def replacemember_cmd(message):
    """Replace member lama dengan user baru (Admin only)"""
    user_id = message.from_user.id
    role = get_user_role(user_id)
    
    if role not in [ROLE_ADMIN, ROLE_OWNER, ROLE_SUPER_ADMIN]:
        bot.reply_to(message, "❌ Command ini hanya untuk Admin, SuperAdmin, dan Owner.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(
                message,
                "❌ *Format salah!*\n\n"
                "*Cara pakai:*\n"
                "`/replacemember <old_user_id> <new_user_id>`\n\n"
                "*Contoh:*\n"
                "`/replacemember 123456789 987654321`\n\n"
                "Member lama akan dihapus, user baru jadi Member dengan durasi sama.",
                parse_mode="Markdown"
            )
            return
        
        old_user_id = int(parts[1])
        new_user_id = int(parts[2])
        
        if old_user_id == new_user_id:
            bot.reply_to(message, "❌ User ID lama dan baru tidak boleh sama!")
            return
        
        # Check old user
        old_user = users_collection.find_one({"user_id": old_user_id})
        if not old_user:
            bot.reply_to(message, f"❌ User lama `{old_user_id}` tidak ditemukan!", parse_mode="Markdown")
            return
        
        old_role = old_user.get("role", ROLE_FREE_USER)
        if old_role != ROLE_MEMBER:
            bot.reply_to(message, f"❌ User lama bukan Member! (Role: {old_role})")
            return
        
        # Check new user
        new_user = users_collection.find_one({"user_id": new_user_id})
        if new_user:
            new_role = new_user.get("role", ROLE_FREE_USER)
            if new_role != ROLE_FREE_USER:
                bot.reply_to(message, f"❌ User baru sudah punya role: {new_role}\nHanya User biasa yang bisa dijadikan Member.")
                return
        
        # Get old member expiry
        old_expiry = old_user.get("expired_at")
        if not old_expiry:
            bot.reply_to(message, "❌ Member lama tidak punya data expired!")
            return
        
        # Get usernames
        old_username = old_user.get("username", "Unknown")
        
        try:
            new_user_info = bot.get_chat(new_user_id)
            new_username = new_user_info.username or "Unknown"
        except:
            new_username = "Unknown"
        
        admin_username = message.from_user.username or "Unknown"
        
        # Delete old member
        users_collection.update_one(
            {"user_id": old_user_id},
            {
                "$set": {"role": ROLE_FREE_USER},
                "$unset": {"expired_at": ""}
            }
        )
        
        # Create/update new member
        if new_user:
            users_collection.update_one(
                {"user_id": new_user_id},
                {
                    "$set": {
                        "role": ROLE_MEMBER,
                        "expired_at": old_expiry,
                        "username": new_username
                    }
                }
            )
        else:
            users_collection.insert_one({
                "user_id": new_user_id,
                "username": new_username,
                "role": ROLE_MEMBER,
                "expired_at": old_expiry,
                "created_at": datetime.utcnow()
            })
        
        remaining_days = (old_expiry - datetime.utcnow()).days
        
        # Success notification
        bot.reply_to(
            message,
            f"✅ *MEMBER DIGANTI*\n\n"
            f"👤 Member Lama: @{old_username} (`{old_user_id}`) ❌\n"
            f"└ Role Baru: User\n\n"
            f"👤 Member Baru: @{new_username} (`{new_user_id}`) ✅\n"
            f"└ Role: Member\n"
            f"└ Sisa Durasi: {remaining_days} hari\n"
            f"└ Expire: {old_expiry.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Notifikasi telah dikirim ke kedua user.",
            parse_mode="Markdown"
        )
        
        # Notify old user
        try:
            bot.send_message(
                old_user_id,
                f"⚠️ *AKUN MEMBER DIHAPUS*\n\n"
                f"Akun member Anda telah dihapus oleh @{admin_username}.\n\n"
                f"Role Anda sekarang: User",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Notify new user
        try:
            bot.send_message(
                new_user_id,
                f"🎉 *SELAMAT! ANDA TELAH JADI MEMBER*\n\n"
                f"Anda telah diangkat menjadi Member oleh @{admin_username}!\n\n"
                f"⏰ Sisa Durasi: {remaining_days} hari\n"
                f"📅 Expire: {old_expiry.strftime('%Y-%m-%d %H:%M')}\n\n"
                f"Selamat menikmati akses premium! 🙏",
                parse_mode="Markdown"
            )
        except:
            pass
        
        # Log
        log_user_replacement(user_id, admin_username, old_user_id, old_username, new_user_id, new_username, ROLE_MEMBER, old_expiry)
        
    except ValueError:
        bot.reply_to(message, "❌ Format error! User ID harus angka.")
    except Exception as e:
        logger.error(f"Error in replacemember_cmd: {e}")
        bot.reply_to(message, f"❌ Terjadi error: {str(e)}")



# ===== NEW FEATURES: VERSION, ADMINMENU, BROADCAST, RESETUSAGE, SEWABOT =====

@bot.message_handler(commands=['version'])
def version_cmd(message):
    """Command untuk info versi bot"""
    user_id = message.from_user.id
    
    version_text = (
        f"🤖 *Bot Version Information*\n\n"
        f"📌 Version: rxn-2.0.1 \n"
        f"📅 Update On : 17/08/2025 , 01:47:25 AM\n"
        f"🔧 Platform: Python + Telebot\n"
        f"💾 Database: MongoDB\n\n"
        f"Bot ini dibuat untuk membantu kebutuhan anda untuk memproses akun vidiotv secara otomatis.\n"
        f"👨‍💻 Developer: Anonym"
    )
    
    bot.reply_to(message, version_text, parse_mode="Markdown")


@bot.message_handler(commands=['adminmenu'])
def adminmenu_cmd(message):
    """Command untuk admin menu (Owner & Super Admin only)"""
    user_id = message.from_user.id
    
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan Super Admin.")
        return
    
    role = get_user_role(user_id)
    role_display = get_role_display_name(role)
    
    total_users = users_collection.count_documents({})
    total_super_admins = users_collection.count_documents({"role": ROLE_SUPER_ADMIN})
    total_admins = users_collection.count_documents({"role": ROLE_ADMIN})
    total_members = users_collection.count_documents({"role": ROLE_MEMBER})
    active_subs = users_collection.count_documents({"expire_date": {"$gt": datetime.now()}})
    total_transactions = transactions_collection.count_documents({})
    paid_transactions = transactions_collection.count_documents({"status": "paid"})
    
    admin_text = (
        f"⚙️ *Admin Panel*\n"
        f"Role: {role_display}\n\n"
        f"📊 *Statistik Bot:*\n"
        f"• Total Users: {total_users}\n"
        f"• Active Subscriptions: {active_subs}\n"
        f"• Super Admins: {total_super_admins}\n"
        f"• Admins: {total_admins}\n"
        f"• Members: {total_members}\n\n"
        f"💰 *Transaksi:*\n"
        f"• Total Transaksi: {total_transactions}\n"
        f"• Pembayaran Sukses: {paid_transactions}\n\n"
        f"👑 *Owner Information:*\n"
    )
    
    owner_info = users_collection.find_one({"user_id": OWNER_ID})
    if owner_info:
        owner_username = owner_info.get('username', 'Unknown')
        admin_text += f"• Owner: @{owner_username} (ID: `{OWNER_ID}`)\n\n"
    else:
        admin_text += f"• Owner ID: `{OWNER_ID}`\n\n"
    
    super_admins = list(users_collection.find({"role": ROLE_SUPER_ADMIN}))
    if super_admins:
        admin_text += "⭐ *Super Admins:*\n"
        for sa in super_admins:
            sa_id = sa['user_id']
            sa_username = sa.get('username', 'Unknown')
            admin_text += f"• @{sa_username} (ID: `{sa_id}`)\n"
        admin_text += "\n"
    
    admin_text += (
        f"⚙️ *Available Commands:*\n"
        f"• /setrole - Set role user\n"
        f"• /broadcast - Broadcast pesan\n"
        f"• /resetusage - Reset usage user\n"
        f"• /stats - Statistik lengkap\n"
        f"• /listusers - Daftar user\n"
        f"• /settings - Bot settings\n\n"
        f"💡 Gunakan /help untuk daftar lengkap."
    )
    
    bot.reply_to(message, admin_text, parse_mode="Markdown")


@bot.message_handler(commands=['broadcast'])
def broadcast_cmd(message):
    """Command untuk broadcast pesan"""
    user_id = message.from_user.id
    
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan Super Admin.")
        return
    
    try:
        broadcast_text = message.text.split(maxsplit=1)[1]
    except IndexError:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/broadcast <pesan>`\n\n"
            "*Contoh:*\n"
            "`/broadcast Halo semua! Bot sudah aktif kembali.`",
            parse_mode="Markdown"
        )
        return
    
    all_users = list(users_collection.find({}))
    
    if not all_users:
        bot.reply_to(message, "📋 Tidak ada user yang terdaftar.")
        return
    
    total_users = len(all_users)
    
    confirm_text = (
        f"📢 *Konfirmasi Broadcast*\n\n"
        f"Pesan akan dikirim ke {total_users} user.\n\n"
        f"*Preview Pesan:*\n"
        f"{broadcast_text}\n\n"
        f"Lanjutkan?"
    )
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton("✅ Ya, Kirim", callback_data=f"broadcast_confirm_{message.message_id}"),
        types.InlineKeyboardButton("❌ Batal", callback_data="broadcast_cancel")
    )
    
    if not hasattr(bot, 'broadcast_data'):
        bot.broadcast_data = {}
    
    bot.broadcast_data[message.message_id] = {
        "text": broadcast_text,
        "total": total_users,
        "users": [u['user_id'] for u in all_users]
    }
    
    bot.reply_to(message, confirm_text, reply_markup=keyboard, parse_mode="Markdown")


@bot.callback_query_handler(func=lambda call: call.data.startswith("broadcast_"))
def handle_broadcast_callback(call):
    """Handle broadcast callback"""
    user_id = call.from_user.id
    
    if not has_owner_access(user_id):
        bot.answer_callback_query(call.id, "❌ Akses ditolak!")
        return
    
    if call.data == "broadcast_cancel":
        bot.edit_message_text(
            "❌ Broadcast dibatalkan.",
            call.message.chat.id,
            call.message.message_id
        )
        bot.answer_callback_query(call.id, "Broadcast dibatalkan")
        return
    
    try:
        _, _, msg_id = call.data.split("_")
        msg_id = int(msg_id)
        broadcast_info = bot.broadcast_data.get(msg_id)
        
        if not broadcast_info:
            bot.answer_callback_query(call.id, "❌ Data broadcast tidak ditemukan!")
            return
        
        broadcast_text = broadcast_info['text']
        target_users = broadcast_info['users']
        total = broadcast_info['total']
        
    except:
        bot.answer_callback_query(call.id, "❌ Data tidak valid!")
        return
    
    bot.edit_message_text(
        f"📤 Mengirim broadcast ke {total} user...\nMohon tunggu.",
        call.message.chat.id,
        call.message.message_id
    )
    
    success_count = 0
    failed_count = 0
    failed_users = []
    
    for target_user_id in target_users:
        try:
            bot.send_message(
                target_user_id,
                f"📢 *BROADCAST MESSAGE*\n\n{broadcast_text}",
                parse_mode="Markdown"
            )
            success_count += 1
            time.sleep(0.05)
        except Exception as e:
            failed_count += 1
            failed_users.append(target_user_id)
            logger.error(f"[Broadcast] Failed to send to {target_user_id}: {e}")
    
    result_text = (
        f"✅ *Broadcast Selesai!*\n\n"
        f"📊 *Hasil Pengiriman:*\n"
        f"✅ Berhasil: {success_count} user\n"
        f"❌ Gagal: {failed_count} user\n"
        f"📈 Total: {total} user\n\n"
    )
    
    if failed_users and failed_count <= 10:
        result_text += f"*User yang gagal:*\n"
        for failed_id in failed_users[:10]:
            result_text += f"• `{failed_id}`\n"
    
    bot.edit_message_text(
        result_text,
        call.message.chat.id,
        call.message.message_id,
        parse_mode="Markdown"
    )
    
    bot.answer_callback_query(call.id, f"✅ Terkirim: {success_count}/{total}")
    logger.info(f"[Broadcast] Completed - Success: {success_count}, Failed: {failed_count}")


@bot.message_handler(commands=['resetusage'])
def resetusage_cmd(message):
    """Command untuk reset usage user"""
    user_id = message.from_user.id
    
    if not has_owner_access(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner dan Super Admin.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        text = (
            "❌ *Format Salah!*\n\n"
            "*Format:* `/resetusage <user_id>`\n\n"
            "*Contoh:*\n"
            "`/resetusage 123456789`\n\n"
            "Untuk reset semua user:\n"
            "`/resetusage all`"
        )
        bot.reply_to(message, text, parse_mode="Markdown")
        return
    
    target = parts[1]
    
    if target.lower() == "all":
        result = usage_tracking_collection.delete_many({})
        deleted_count = result.deleted_count
        
        bot.reply_to(
            message,
            f"✅ *Usage Reset Berhasil!*\n\n"
            f"Total data yang dihapus: {deleted_count}\n"
            f"Semua user usage telah direset.",
            parse_mode="Markdown"
        )
        logger.info(f"[ResetUsage] All users usage reset by admin {user_id}")
        return
    
    try:
        target_user_id = int(target)
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka atau 'all'.")
        return
    
    result = usage_tracking_collection.delete_many({"user_id": target_user_id})
    deleted_count = result.deleted_count
    
    if deleted_count > 0:
        bot.reply_to(
            message,
            f"✅ *Usage Reset Berhasil!*\n\n"
            f"User ID: `{target_user_id}`\n"
            f"Data usage yang dihapus: {deleted_count}\n\n"
            f"User dapat mulai membuat akun dari awal.",
            parse_mode="Markdown"
        )
        logger.info(f"[ResetUsage] User {target_user_id} usage reset by admin {user_id}")
    else:
        bot.reply_to(
            message,
            f"ℹ️ User `{target_user_id}` tidak memiliki data usage.",
            parse_mode="Markdown"
        )


@bot.message_handler(commands=['sewabot'])
def sewabot_cmd(message):
    """Command untuk sewa bot dengan flow pembayaran otomatis"""
    user_id = message.from_user.id
    
    with payment_checkers_lock:
        if user_id in payment_checkers:
            bot.reply_to(message, "⚠️ Anda masih memiliki pembayaran yang sedang pending.")
            return
    
    # Get packages from database
    prices = list(settings_collection.find({"key": {"$regex": "^price_"}}).sort("days", 1))
    
    # Fallback ke default jika database kosong
    if not prices:
        packages = [
            {"name": "7 Hari", "days": 7, "price": 15000},
            {"name": "15 Hari", "days": 15, "price": 25000},
            {"name": "30 Hari", "days": 30, "price": 45000},
            {"name": "60 Hari", "days": 60, "price": 80000},
            {"name": "90 Hari", "days": 90, "price": 110000},
        ]
    else:
        # Build packages from database
        packages = []
        for pkg in prices:
            days = pkg['days']
            price = pkg['price']
            packages.append({
                "name": f"{days} Hari",
                "days": days,
                "price": price
            })
    
    text = (
        "🏪 *Paket Sewa Bot*\n\n"
        "Pilih paket yang Anda inginkan:\n\n"
    )
    
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for pkg in packages:
        text += f"📦 *{pkg['name']}* - Rp {pkg['price']:,}\n"
        keyboard.add(
            types.InlineKeyboardButton(
                f"{pkg['name']} - Rp {pkg['price']:,}",
                callback_data=f"sewa_{pkg['days']}_{pkg['price']}"
            )
        )
    
    text += "\n💡 Setelah pembayaran berhasil, akses bot akan aktif otomatis!"
    
    bot.reply_to(message, text, reply_markup=keyboard, parse_mode="Markdown")


@bot.callback_query_handler(func=lambda call: call.data.startswith("sewa_"))
def handle_sewa_callback(call):
    """Handle sewa callback"""
    user_id = call.from_user.id
    
    try:
        _, days, price = call.data.split("_")
        days = int(days)
        price = int(price)
    except:
        bot.answer_callback_query(call.id, "❌ Data tidak valid!")
        return
    
    unique_code = f"VDO{random.randint(100, 999)}"
    total_price = price
    
    orderkuota = Orderkuota()
    try:
        qris_dynamic = orderkuota.qris_statis_to_dinamis(
            orderkuota.static_qris_string,
            total_price
        )
        qr_image = orderkuota.generate_qr_image(qris_dynamic)
    except Exception as e:
        logger.error(f"Error generating QRIS: {e}")
        bot.answer_callback_query(call.id, "❌ Gagal generate QRIS!")
        return
    
    transactions_collection.insert_one({
        "user_id": user_id,
        "unique_code": unique_code,
        "amount": total_price,
        "duration_days": days,
        "status": "pending",
        "created_at": datetime.now()
    })
    
    caption = (
        f"💳 *Pembayaran Sewa Bot*\n\n"
        f"📦 Paket: {days} Hari\n"
        f"💰 Total: Rp {total_price:,}\n"
        f"🔖 Order ID: `{unique_code}`\n\n"
        f"⏰ Silakan bayar dalam 10 menit\n"
        f"✅ Pembayaran akan diverifikasi otomatis"
    )
    
    sent_msg = bot.send_photo(
        user_id,
        qr_image,
        caption=caption,
        parse_mode="Markdown"
    )
    
    with payment_checkers_lock:
        payment_checkers[user_id] = {
            "total_price": total_price,
            "unique_code": unique_code,
            "message_id": sent_msg.message_id,
            "start_time": datetime.now(),
            "duration_days": days
        }
    
    bot.answer_callback_query(call.id, "✅ QRIS berhasil digenerate!")
    logger.info(f"[Sewa] User {user_id} initiated payment for {days} days - Rp {total_price:,}")


# ===== SETTINGS COMMANDS =====

@bot.message_handler(commands=['setmaxaccounts'])
def setmaxaccounts_cmd(message):
    """Command untuk set max accounts per request"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/setmaxaccounts <jumlah>`\n\n"
            "*Contoh:*\n"
            "`/setmaxaccounts 20`",
            parse_mode="Markdown"
        )
        return
    
    try:
        max_accounts = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Jumlah harus berupa angka.")
        return
    
    update_bot_setting("max_accounts_per_request", max_accounts)
    bot.reply_to(
        message,
        f"✅ *Setting Berhasil Diupdate!*\n\n"
        f"Max Accounts Per Request: {max_accounts}",
        parse_mode="Markdown"
    )


@bot.message_handler(commands=['setcooldown'])
def setcooldown_cmd(message):
    """Command untuk set cooldown threshold dan duration"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 3:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/setcooldown <threshold> <duration>`\n\n"
            "*Contoh:*\n"
            "`/setcooldown 20 150`\n"
            "Artinya: Cooldown 150 detik setelah 20 akun",
            parse_mode="Markdown"
        )
        return
    
    try:
        threshold = int(parts[1])
        duration = int(parts[2])
    except ValueError:
        bot.reply_to(message, "❌ Threshold dan duration harus berupa angka.")
        return
    
    update_bot_setting("cooldown_threshold", threshold)
    update_bot_setting("cooldown_duration", duration)
    
    bot.reply_to(
        message,
        f"✅ *Cooldown Setting Berhasil Diupdate!*\n\n"
        f"Threshold: {threshold} akun\n"
        f"Duration: {duration} detik ({duration//60} menit)",
        parse_mode="Markdown"
    )


@bot.message_handler(commands=['settings'])
def settings_cmd(message):
    """Command untuk lihat bot settings"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    settings = get_bot_settings()
    
    text = (
        "⚙️ *Bot Settings*\n\n"
        f"📊 Max Accounts Per Request: {settings.get('max_accounts_per_request', 10)}\n"
        f"⏳ Cooldown Threshold: {settings.get('cooldown_threshold', 20)} akun\n"
        f"⏰ Cooldown Duration: {settings.get('cooldown_duration', 150)} detik\n\n"
        "Gunakan command berikut untuk mengubah:\n"
        "• /setmaxaccounts <n>\n"
        "• /setcooldown <threshold> <duration>\n"
        "• /setprice - Set harga sewa\n"
        "• /listprices - Lihat daftar harga"
    )
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['setprice'])
def setprice_cmd(message):
    """Command untuk set harga paket sewa (Owner only)"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 3:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/setprice <durasi_hari> <harga>`\n\n"
            "*Contoh:*\n"
            "`/setprice 7 15000` - Set harga 7 hari = Rp 15.000\n"
            "`/setprice 30 50000` - Set harga 30 hari = Rp 45.000\n\n"
            "Gunakan /listprices untuk lihat harga saat ini",
            parse_mode="Markdown"
        )
        return
    
    try:
        days = int(parts[1])
        price = int(parts[2])
    except ValueError:
        bot.reply_to(message, "❌ Durasi dan harga harus berupa angka.")
        return
    
    if days < 1:
        bot.reply_to(message, "❌ Durasi minimal 1 hari.")
        return
    
    if price < 1000:
        bot.reply_to(message, "❌ Harga minimal Rp 1.000.")
        return
    
    # Update atau insert pricing
    settings_collection.update_one(
        {"key": f"price_{days}d"},
        {
            "$set": {
                "key": f"price_{days}d",
                "days": days,
                "price": price,
                "updated_at": datetime.now(),
                "updated_by": user_id
            }
        },
        upsert=True
    )
    
    bot.reply_to(
        message,
        f"✅ *Harga Berhasil Diset!*\n\n"
        f"📦 Paket: {days} Hari\n"
        f"💰 Harga: Rp {price:,}\n\n"
        f"Gunakan /listprices untuk lihat semua harga",
        parse_mode="Markdown"
    )
    
    logger.info(f"[SetPrice] Owner {user_id} set price {days} days = Rp {price:,}")


@bot.message_handler(commands=['delprice'])
def delprice_cmd(message):
    """Command untuk hapus paket harga (Owner only)"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/delprice <durasi_hari>`\n\n"
            "*Contoh:*\n"
            "`/delprice 7` - Hapus paket 7 hari\n\n"
            "Gunakan /listprices untuk lihat paket yang ada",
            parse_mode="Markdown"
        )
        return
    
    try:
        days = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Durasi harus berupa angka.")
        return
    
    # Delete pricing
    result = settings_collection.delete_one({"key": f"price_{days}d"})
    
    if result.deleted_count > 0:
        bot.reply_to(
            message,
            f"✅ *Paket Berhasil Dihapus!*\n\n"
            f"📦 Paket {days} hari telah dihapus dari daftar harga.",
            parse_mode="Markdown"
        )
        logger.info(f"[DelPrice] Owner {user_id} deleted price for {days} days")
    else:
        bot.reply_to(
            message,
            f"❌ Paket {days} hari tidak ditemukan.\n\n"
            f"Gunakan /listprices untuk lihat paket yang ada.",
            parse_mode="Markdown"
        )


@bot.message_handler(commands=['listprices'])
def listprices_cmd(message):
    """Command untuk lihat daftar harga paket sewa (Owner only)"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    # Get all pricing from database
    prices = list(settings_collection.find({"key": {"$regex": "^price_"}}).sort("days", 1))
    
    if not prices:
        bot.reply_to(
            message,
            "ℹ️ *Belum Ada Paket Harga*\n\n"
            "Gunakan /setprice untuk menambah paket harga.\n\n"
            "*Contoh:*\n"
            "`/setprice 7 15000`\n"
            "`/setprice 30 45000`",
            parse_mode="Markdown"
        )
        return
    
    text = "💰 *Daftar Harga Paket Sewa Bot*\n\n"
    
    for idx, pkg in enumerate(prices, 1):
        days = pkg['days']
        price = pkg['price']
        updated_at = pkg.get('updated_at')
        
        text += f"{idx}. 📦 *{days} Hari*\n"
        text += f"   💵 Rp {price:,}\n"
        if updated_at:
            text += f"   📅 Update: {updated_at.strftime('%d/%m/%Y %H:%M')}\n"
        text += "\n"
    
    text += (
        "⚙️ *Management Commands:*\n"
        "• `/setprice <days> <price>` - Set/update harga\n"
        "• `/delprice <days>` - Hapus paket\n\n"
        "💡 Harga ini akan tampil di /sewabot"
    )
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['debugcooldown'])
def debugcooldown_cmd(message):
    """Command untuk debug cooldown (Owner only)"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    # Parse user_id target
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/debugcooldown <user_id>`\n\n"
            "*Contoh:*\n"
            "`/debugcooldown 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    # Get user info
    role = get_user_role(target_user_id)
    role_display = get_role_display_name(role)
    
    # Get session count
    session_count = get_session_count(target_user_id)
    
    # Get cooldown status
    is_cooldown, remaining_time = get_cooldown_status(target_user_id)
    
    # Get settings
    settings = get_bot_settings()
    threshold = settings.get('cooldown_threshold', COOLDOWN_THRESHOLD)
    duration = settings.get('cooldown_duration', COOLDOWN_DURATION)
    
    # Get cooldown data from database
    cooldown_data = cooldown_collection.find_one({"user_id": target_user_id})
    
    text = (
        f"🐛 *Debug Cooldown Info*\n\n"
        f"👤 *User:* `{target_user_id}`\n"
        f"🎭 *Role:* {role_display}\n\n"
        f"📊 *Session Info:*\n"
        f"├ Session Count: {session_count}\n"
        f"└ Threshold: {threshold}\n\n"
        f"⚙️ *Settings:*\n"
        f"├ Threshold: {threshold} akun\n"
        f"└ Duration: {duration} detik ({duration//60} menit)\n\n"
        f"⏳ *Cooldown Status:*\n"
        f"├ In Cooldown: {'Yes' if is_cooldown else 'No'}\n"
    )
    
    if is_cooldown:
        minutes = remaining_time // 60
        seconds = remaining_time % 60
        text += f"└ Remaining: {minutes}m {seconds}s\n\n"
    else:
        text += f"└ Remaining: N/A\n\n"
    
    text += f"💾 *Database Cooldown:*\n"
    if cooldown_data:
        expire = cooldown_data.get('expire_at')
        if expire:
            expire_str = expire.strftime('%Y-%m-%d %H:%M:%S')
            text += f"└ Expire: {expire_str}\n"
        else:
            text += f"└ Expire: None\n"
    else:
        text += f"└ No cooldown data in DB\n"
    
    text += f"\n🔧 *Actions:*\n"
    text += f"• Reset session: `/resetsession {target_user_id}`\n"
    text += f"• Delete cooldown: `/deletecooldown {target_user_id}`"
    
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['resetsession'])
def resetsession_cmd(message):
    """Command untuk reset session count user (Owner only)"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/resetsession <user_id>`\n\n"
            "*Contoh:*\n"
            "`/resetsession 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    reset_session_count(target_user_id)
    bot.reply_to(
        message,
        f"✅ Session count untuk user `{target_user_id}` telah direset ke 0.",
        parse_mode="Markdown"
    )
    logger.info(f"[ResetSession] Owner {user_id} reset session for user {target_user_id}")


@bot.message_handler(commands=['deletecooldown'])
def deletecooldown_cmd(message):
    """Command untuk hapus cooldown user (Owner only)"""
    user_id = message.from_user.id
    if not is_owner(user_id):
        bot.reply_to(message, "❌ Command ini hanya untuk Owner.")
        return
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            "❌ *Format Salah!*\n\n"
            "*Format:* `/deletecooldown <user_id>`\n\n"
            "*Contoh:*\n"
            "`/deletecooldown 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ User ID harus berupa angka.")
        return
    
    # Delete cooldown from database
    result = cooldown_collection.delete_one({"user_id": target_user_id})
    
    if result.deleted_count > 0:
        bot.reply_to(
            message,
            f"✅ Cooldown untuk user `{target_user_id}` telah dihapus.",
            parse_mode="Markdown"
        )
        logger.info(f"[DeleteCooldown] Owner {user_id} deleted cooldown for user {target_user_id}")
    else:
        bot.reply_to(
            message,
            f"ℹ️ User `{target_user_id}` tidak memiliki cooldown aktif.",
            parse_mode="Markdown"
        )


# ===== CALLBACK HANDLER FOR CREATE ACCOUNT =====

@bot.callback_query_handler(func=lambda call: call.data == "create_account")
def handle_create_account_callback(call):
    """Handle create account callback"""
    user_id = call.from_user.id
    
    # Check cooldown first
    role = get_user_role(user_id)
    if role in [ROLE_ADMIN, ROLE_MEMBER]:
        in_cooldown, remaining = get_cooldown_status(user_id)
        if in_cooldown:
            minutes = remaining // 60
            seconds = remaining % 60
            settings = get_bot_settings()
            threshold = settings.get('cooldown_threshold', COOLDOWN_THRESHOLD)
            duration = settings.get('cooldown_duration', COOLDOWN_DURATION)
            
            # Kirim alert
            bot.answer_callback_query(
                call.id,
                f"⏳ Cooldown aktif! Tunggu {minutes}m {seconds}s",
                show_alert=True
            )
            
            # Kirim pesan detail
            cooldown_msg = (
                f"⏳ *Cooldown Aktif*\n\n"
                f"Anda telah mencapai batas pembuatan akun.\n\n"
                f"⚙️ *Settings:*\n"
                f"├ Threshold: {threshold} akun\n"
                f"└ Cooldown: {duration} detik ({duration//60} menit)\n\n"
                f"⏱️ *Waktu Tersisa:*\n"
                f"└ {minutes} menit {seconds} detik\n\n"
                f"💡 Silakan tunggu hingga cooldown selesai.\n"
                f"Gunakan /checkcooldown untuk cek status cooldown."
            )
            bot.send_message(user_id, cooldown_msg, parse_mode="Markdown")
            return
    
    subscription = get_user_subscription(user_id)
    
    # Check if user has access
    if not subscription and role == ROLE_FREE_USER:
        # Free user tanpa subscription, cek lifetime limit
        lifetime_usage = get_total_lifetime_usage(user_id)
        limit = DAILY_LIMITS.get(ROLE_FREE_USER, 5)
        
        if lifetime_usage >= limit:
            bot.answer_callback_query(
                call.id, 
                f"❌ Anda sudah mencapai limit {limit} akun (lifetime). Gunakan /sewabot untuk upgrade.",
                show_alert=True
            )
            return
    
    # Get max accounts dari settings
    settings = get_bot_settings()
    max_accounts = settings.get('max_accounts_per_request', 10)
    
    msg = bot.send_message(
        user_id, 
        f"🎬 Masukkan *jumlah akun* yang ingin dibuat (1-{max_accounts}):", 
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, handle_create_count_step)
    bot.answer_callback_query(call.id)


def handle_create_count_step(message):
    """Handle create count input"""
    user_id = message.from_user.id
    
    text_raw = (message.text or "").strip()
    if not text_raw.isdigit():
        bot.reply_to(message, "❌ Masukkan angka yang valid.")
        return
    
    jumlah = int(text_raw)
    settings = get_bot_settings()
    max_accounts = settings.get('max_accounts_per_request', 10)
    
    if jumlah < 1 or jumlah > max_accounts:
        bot.reply_to(message, f"❌ Jumlah harus antara 1 hingga {max_accounts}.")
        return
    
    can_create, reason, remaining = can_create_accounts(user_id, jumlah)
    if not can_create:
        bot.reply_to(message, reason)
        return
    
    custom_data = get_custom_password(user_id)
    
    # Kirim pesan "Sedang membuat akun" yang akan dihapus nanti
    if custom_data:
        password = custom_data.get('password')
        mode = custom_data.get('mode', 'same')
        
        if mode == 'same':
            mode_text = "password yang sama"
            progress_msg = bot.send_message(
                user_id,
                f"⏳ Sedang membuat *{jumlah} akun* Vidiotv Premium dengan {mode_text}...\n"
                f"🔐 Password: `{password}`\n\n"
                f"Mohon tunggu...",
                parse_mode="Markdown"
            )
        else:
            mode_text = "password berbeda (random)"
            progress_msg = bot.send_message(
                user_id,
                f"⏳ Sedang membuat *{jumlah} akun* Vidiotv Premium dengan {mode_text}...\n"
                f"🔐 Base Password: `{password}` + 3 digit random\n\n"
                f"Mohon tunggu...",
                parse_mode="Markdown"
            )
    else:
        progress_msg = bot.send_message(user_id, f"⏳ Sedang membuat *{jumlah} akun* Vidiotv Premium...\nMohon tunggu...", parse_mode="Markdown")
    
    # Simpan message_id untuk dihapus nanti
    create_vidio_accounts_threaded(user_id, jumlah, progress_msg.message_id)


def create_vidio_accounts_threaded(user_id, jumlah, progress_message_id=None):
    """Create Vidio accounts with threading and animated progress"""
    results = []
    tempfile_path = f"/tmp/vidio_accounts_{user_id}_{int(time.time())}.txt"
    stop_animation = threading.Event()
    
    # Animasi text yang akan berganti-ganti
    animation_texts = [
        "⏳ *Memproses akun...*\nMohon tunggu sebentar..",
        "🔄 *Mengaitkan code TV ke akun...*\nProses berlangsung..",
        "✨ *Membuat akun premium...*\nHampir selesai..",
        "🎯 *Mempersiapkan Akun...*\nTinggal sedikit lagi..",
    ]
    
    def update_progress_animation():
        """Thread untuk update progress message dengan animasi"""
        animation_index = 0
        last_displayed_text = ""
        last_update_time = time.time()
        
        while not stop_animation.is_set():
            if progress_message_id:
                try:
                    # Sleep dulu sebelum cek update
                    time.sleep(0.5)  # Check setiap 0.5 detik
                    
                    current_time = time.time()
                    success_so_far = len(results)
                    
                    # Hanya update jika:
                    # 1. Sudah lewat 3 detik sejak update terakhir, atau
                    # 2. Progress berubah signifikan (bertambah akun)
                    should_update = (current_time - last_update_time) >= 3.0
                    
                    if not should_update:
                        continue
                    
                    # Ganti text animasi
                    current_text = animation_texts[animation_index % len(animation_texts)]
                    
                    # Build display text
                    display_text = (
                        f"{current_text}\n\n"
                        f"📊 Progress: {success_so_far}/{jumlah} akun\n"
                        f"{'▓' * min(success_so_far, 10)}{'░' * max(0, 10 - success_so_far)}"
                    )
                    
                    # Skip jika text sama persis (avoid Telegram 400 error)
                    if display_text == last_displayed_text:
                        continue
                    
                    # Try update
                    bot.edit_message_text(
                        chat_id=user_id,
                        message_id=progress_message_id,
                        text=display_text,
                        parse_mode="Markdown"
                    )
                    
                    # Update tracking
                    last_displayed_text = display_text
                    last_update_time = current_time
                    animation_index += 1
                    
                    logger.info(f"[Animation] Updated progress: {success_so_far}/{jumlah}")
                    
                except Exception as e:
                    # Log error tapi jangan stop animation (kecuali error message deleted)
                    error_msg = str(e)
                    if "message to edit not found" in error_msg.lower() or "message can't be edited" in error_msg.lower():
                        logger.warning(f"Animation stopped: message deleted or can't be edited")
                        break
                    else:
                        # Error lain, log tapi continue (mungkin rate limit sementara)
                        logger.debug(f"Animation update error (continuing): {e}")
                        time.sleep(2)  # Wait longer before retry
    
    # Start animation thread
    if progress_message_id:
        animation_thread = threading.Thread(target=update_progress_animation, daemon=True)
        animation_thread.start()
    
    def vidio_flow_worker(worker_id):
        vidio_instance = None
        try:
            vidio_instance = Vidio()
            vidio_instance.user_id = user_id
            result = vidio_instance.auto_flow()
            
            if result:
                with file_lock:
                    results.append(result)
                    subscription_status = result.get('subscription', 'UNKNOWN')
                    with open(tempfile_path, 'a') as f:
                        f.write(f"{result['email']}|{result['password']}|{subscription_status}\n")
                logger.info(f"[Worker-{worker_id}] ✅ Akun berhasil: {result['email']} (Subscription: {subscription_status})")
            else:
                logger.warning(f"[Worker-{worker_id}] ❌ Gagal membuat akun.")
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] Error: {e}")
        finally:
            if vidio_instance:
                vidio_instance.close()
    
    max_parallel_jobs = min(10, jumlah)
    with ThreadPoolExecutor(max_workers=max_parallel_jobs) as executor:
        futures = [executor.submit(vidio_flow_worker, i + 1) for i in range(jumlah)]
        for f in as_completed(futures):
            pass
    
    # Stop animation setelah selesai
    stop_animation.set()
    
    # Update final message: "Selesai"
    if progress_message_id:
        try:
            time.sleep(0.5)  # Small delay
            bot.edit_message_text(
                chat_id=user_id,
                message_id=progress_message_id,
                text="✅ *Selesai Membuat Akun!*\n\nSedang mengirim data akun...",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Final animation update error: {e}")
    
    if results:
        success_count = len(results)
        
        # Track usage FIRST - sebelum get usage info
        increment_daily_usage(user_id, success_count)
        increment_session_count(user_id, success_count)
        
        # Get role and usage info AFTER tracking
        role = get_user_role(user_id)
        role_display = get_role_display_name(role)
        
        # Get usage info (sudah updated karena tracking dilakukan duluan)
        if role == ROLE_FREE_USER:
            lifetime_usage = get_total_lifetime_usage(user_id)
            daily_limit = DAILY_LIMITS.get(role, 5)
            remaining = daily_limit - lifetime_usage
        else:
            daily_usage = get_daily_usage(user_id)
            daily_limit = DAILY_LIMITS.get(role, 1000)
            remaining = daily_limit - daily_usage
        
        # Build caption seperti gambar
        success_count = len(results)
        active_count = sum(1 for r in results if r.get('subscription') == 'ACTIVE')
        
        caption = f"✅ Berhasil membuat {success_count}/{jumlah} akun Vidiotv Premium!\n"
        caption += f"🎯 Akun Aktif: {active_count}/{success_count}\n\n"
        
        # Info password
        custom_data = get_custom_password(user_id)
        if custom_data:
            password = custom_data.get('password')
            mode = custom_data.get('mode', 'same')
            
            if mode == 'same':
                caption += f"🔐 *Password:* `{password}`\n\n"
            else:
                caption += f"🔐 *Password:* `{password}` + 3 digit random\n\n"
        else:
            caption += f"🔐 *Password:* Password random per akun\n\n"
        
        # Info role, akumulasi, dan sisa limit
        caption += f"Role: {role_display}\n"
        
        if role == ROLE_FREE_USER:
            caption += f"📊 Akumulasi: {lifetime_usage}/{daily_limit} akun (lifetime)\n"
            caption += f"📉 Sisa limit: {remaining} akun"
        elif role in [ROLE_OWNER, ROLE_SUPER_ADMIN]:
            caption += f"📊 Akumulasi: Unlimited\n"
            caption += f"📉 Sisa limit: ♾️ Unlimited"
        else:
            caption += f"📊 Akumulasi: {daily_usage}/{daily_limit} akun\n"
            caption += f"📉 Sisa limit hari ini: {remaining} akun"
        
        try:
            with open(tempfile_path, 'rb') as f:
                bot.send_document(
                    chat_id=user_id,
                    document=f,
                    visible_file_name=f"vidio_accounts_{user_id}_{int(time.time())}.txt",
                    caption=caption,
                    parse_mode="Markdown"
                )
        except Exception as e:
            logger.error(f"Error sending file: {e}")
        finally:
            if os.path.exists(tempfile_path):
                os.remove(tempfile_path)
        
        # Log ke Telegram group
        try:
            user = bot.get_chat(user_id)
            username = user.username if user.username else "Unknown"
            log_account_creation(
                user_id=user_id,
                username=username,
                success_count=success_count,
                total_requested=jumlah,
                role=role,
                active_count=active_count
            )
        except Exception as e:
            logger.error(f"[Logger] Failed to log account creation: {e}")
        
        # Hapus progress message setelah file terkirim
        if progress_message_id:
            try:
                bot.delete_message(chat_id=user_id, message_id=progress_message_id)
            except Exception as e:
                logger.error(f"Error deleting progress message: {e}")
        
        # Check cooldown untuk Admin dan Member
        role = get_user_role(user_id)
        if role in [ROLE_ADMIN, ROLE_MEMBER]:
            session_count = get_session_count(user_id)
            settings = get_bot_settings()
            threshold = settings.get('cooldown_threshold', COOLDOWN_THRESHOLD)
            duration = settings.get('cooldown_duration', COOLDOWN_DURATION)
            
            logger.info(f"[Cooldown Check] User {user_id}, Role: {role}")
            logger.info(f"[Cooldown Check] Session count: {session_count}, Threshold: {threshold}")
            logger.info(f"[Cooldown Check] Duration setting: {duration} seconds")
            logger.info(f"[Cooldown Check] Will activate cooldown: {session_count >= threshold}")
            
            if session_count >= threshold:
                set_cooldown(user_id, duration)
                reset_session_count(user_id)
                
                logger.info(f"[Cooldown] User {user_id} hit threshold ({session_count} >= {threshold}), cooldown activated for {duration}s")
                
                # Kirim pesan cooldown yang detail
                minutes = duration // 60
                seconds = duration % 60
                cooldown_msg = (
                    f"⏳ *Cooldown Aktif*\n\n"
                    f"Anda telah mencapai batas pembuatan akun ({threshold} akun).\n\n"
                    f"⚙️ *Cooldown Settings:*\n"
                    f"├ Threshold: {threshold} akun\n"
                    f"└ Duration: {duration} detik ({minutes} menit {seconds} detik)\n\n"
                    f"⏱️ *Waktu Cooldown:*\n"
                    f"└ {minutes} menit {seconds} detik\n\n"
                    f"💡 Silakan tunggu hingga cooldown selesai.\n"
                    f"Gunakan /checkcooldown untuk cek status cooldown kapan saja."
                )
                
                bot.send_message(user_id, cooldown_msg, parse_mode="Markdown")
                logger.info(f"[Cooldown] Cooldown message sent to user {user_id}")
            else:
                logger.info(f"[Cooldown Check] No cooldown needed. Session: {session_count}/{threshold}")
    else:
        # Hapus progress message jika gagal
        if progress_message_id:
            try:
                bot.delete_message(chat_id=user_id, message_id=progress_message_id)
            except Exception as e:
                logger.error(f"Error deleting progress message: {e}")
        
        bot.send_message(user_id, "❌ Gagal membuat akun. Silakan coba lagi nanti.")


# ===== SETUP BOT COMMANDS =====
def setup_bot_commands():
    """Setup bot command list yang muncul di menu Telegram"""
    
    # Commands untuk semua user
    user_commands = [
        types.BotCommand("start", "Mulai bot"),
        types.BotCommand("help", "Bantuan"),
        types.BotCommand("myprofile", "Lihat profile"),
        types.BotCommand("setpassword", "Set custom password"),
        types.BotCommand("mypassword", "Lihat custom password"),
        types.BotCommand("deletepassword", "Hapus custom password"),
        types.BotCommand("usage", "Cek usage harian"),
        types.BotCommand("sewabot", "Paket sewa bot"),
        types.BotCommand("version", "Info versi bot"),
    ]
    
    # Commands untuk Owner (semua commands)
    owner_commands = [
        types.BotCommand("start", "Mulai bot"),
        types.BotCommand("help", "Bantuan"),
        types.BotCommand("myprofile", "Lihat profile"),
        types.BotCommand("setpassword", "Set custom password"),
        types.BotCommand("mypassword", "Lihat custom password"),
        types.BotCommand("deletepassword", "Hapus custom password"),
        types.BotCommand("check", "Cek paket akun Vidio"),
        types.BotCommand("usage", "Cek usage harian"),
        types.BotCommand("sewabot", "Paket sewa bot"),
        types.BotCommand("version", "Info versi bot"),
        types.BotCommand("adminmenu", "Admin panel"),
        types.BotCommand("listusers", "Lihat semua user"),
        types.BotCommand("listadmin", "Lihat daftar admin"),
        types.BotCommand("listmember", "Lihat daftar member"),
        types.BotCommand("setrole", "Set role user"),
        types.BotCommand("deleterole", "Hapus/reset role user"),
        types.BotCommand("addadmin", "Tambah admin"),
        types.BotCommand("addmemberadv", "Tambah member"),
        types.BotCommand("listroles", "List semua role"),
        types.BotCommand("checkroles", "Check status role user"),
        types.BotCommand("stats", "Statistik bot"),
        types.BotCommand("broadcast", "Broadcast pesan"),
        types.BotCommand("resetusage", "Reset usage user"),
        types.BotCommand("setmaxaccounts", "Set max akun per request"),
        types.BotCommand("setcooldown", "Set cooldown"),
        types.BotCommand("setprice", "Set harga paket sewa"),
        types.BotCommand("listprices", "Lihat daftar harga"),
        types.BotCommand("delprice", "Hapus paket harga"),
        types.BotCommand("settings", "Lihat settings"),
    ]
    
    # Set default commands untuk semua user
    try:
        bot.set_my_commands(user_commands)
        logger.info("[Setup] Bot commands set successfully")
    except Exception as e:
        logger.error(f"[Setup] Failed to set bot commands: {e}")


# ==================== STARTUP BANNER ====================

def print_banner():
    """Print ASCII art banner dan bot info"""
    
    # Color mapping
    COLORS = {
        'RED': '\033[91m',
        'GREEN': '\033[92m',
        'YELLOW': '\033[93m',
        'BLUE': '\033[94m',
        'MAGENTA': '\033[95m',
        'CYAN': '\033[96m',
        'WHITE': '\033[97m'
    }
    
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    # Get colors from config
    banner_color = COLORS.get(BANNER_COLOR, COLORS['RED'])
    info_color = COLORS.get(INFO_COLOR, COLORS['CYAN'])
    success_color = COLORS.get(SUCCESS_COLOR, COLORS['GREEN'])
    warning_color = COLORS.get(WARNING_COLOR, COLORS['YELLOW'])
    
    # Print banner dengan warna
    print(f"\n{banner_color}{BOLD}{ACTIVE_BANNER}{RESET}")
    
    # Bot Information
    print(f"{info_color}{'='*60}{RESET}")
    print(f"{success_color}✓{RESET} {BOLD}MONGODB CONNECTED{RESET}")
    
    # Get bot info
    try:
        bot_info = bot.get_me()
        bot_username = bot_info.username
        bot_id = bot_info.id
        bot_name = bot_info.first_name
    except Exception as e:
        logger.warning(f"Could not fetch bot info: {e}")
        bot_username = "Unknown"
        bot_id = "Unknown"
        bot_name = BOT_DISPLAY_NAME
    
    print(f"{warning_color}  Bot Name    :{RESET} {bot_name}")
    print(f"{warning_color}  Bot Username:{RESET} @{bot_username}")
    print(f"{warning_color}  Bot ID      :{RESET} {bot_id}")
    print(f"{warning_color}  Owner ID    :{RESET} {OWNER_ID}")
    print(f"{warning_color}  Database    :{RESET} {DB_NAME}")
    print(f"{info_color}{'='*60}{RESET}\n")


# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    # Print banner
    print_banner()
    
    # Setup bot commands
    logger.info("[Setup] Bot commands set successfully")
    setup_bot_commands()
    
    # Start payment poller
    poller_thread = threading.Thread(target=master_payment_poller, daemon=True)
    poller_thread.start()
    logger.info("[PaymentPoller] Master payment poller started.")
    
    logger.info("Bot starting infinity polling...")
    try:
        bot.infinity_polling(timeout=60)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Bot polling failed: {e}")
        logger.info("Restarting bot in 15 seconds...")
        time.sleep(15)
