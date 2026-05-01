import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import sqlite3
import re
from datetime import datetime, timedelta
import time
import sys

# ========== КОНФИГУРАЦИЯ ==========
TOKEN = "vk1.a.60TiG34Ak-IuPrXeea-FJu7bHSah4rH6gEevT3D3eq6dHGjTwxwlJY2Hu1RgMoGjyPo1JvAwGiddX2LOiyZ5ZLXRXuU33FtT2XspZKqGLux_wVEhJ-gxrGuwkLAMuxWk8lYfGldUUaqwk0dAGHaL0O1VNWHhs0kuMtGwCEzKnX2uptvMwTESOU1lecsLTNdXe4vBLJXnjvToMoMxAA6E9Q"
GROUP_ID = 237479702
OWNER_ID = 600551888  # ТВОЙ ID (разработчик)

# ========== СЛОВАРЬ РОЛЕЙ ==========
ROLES = {
    1: "Без должности",
    2: "Следящий",
    3: "Заместитель главного следящего",
    4: "Главный следящий",
    5: "Заместитель главного следящего за хелперами",
    6: "Куратор",
    7: "Заместитель ГА",
    8: "Главный администратор",
    9: "Технический администратор",
    10: "Заместитель главного технического администратора",
    11: "Заместитель специального администратора",
    12: "Специальный администратор",
    13: "Владелец"
}
ROLE_ALIASES = {
    "Без должности": 1,
    "Следящий": 2,
    "Заместитель главного следящего": 3,
    "Главный следящий": 4,
    "Заместитель главного следящего за хелперами": 5,
    "Куратор": 6,
    "Заместитель ГА": 7,
    "Главный администратор": 8,
    "Технический администратор": 9,
    "Заместитель главного технического администратора": 10,
    "Главный технический администратор": 10,
    "Заместитель специального администратора": 11,
    "Специальный администратор": 12,
    "Разработчик": 11,
    "Заместитель основателя": 12,
    "Основатель": 12,
    "Руководитель": 12,
    "Заместитель владельца": 12,
    "Владелец": 13,
    "dev": 13
}

# ========== БАЗА ДАННЫХ ==========
def init_db():
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS bans (user_id INTEGER PRIMARY KEY, reason TEXT, admin_id INTEGER, timestamp TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS filters (word TEXT PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS forbidden_mentions (user_id INTEGER PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, action TEXT, target_id INTEGER, timestamp TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS warnings_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        peer_id INTEGER,
        admin_id INTEGER,
        reason TEXT,
        timestamp TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS vigors_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        peer_id INTEGER,
        admin_id INTEGER,
        reason TEXT,
        timestamp TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS mutes (
        user_id INTEGER,
        peer_id INTEGER,
        until TEXT,
        reason TEXT,
        PRIMARY KEY (user_id, peer_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS message_history (
        user_id INTEGER,
        peer_id INTEGER,
        timestamp REAL,
        PRIMARY KEY (user_id, peer_id, timestamp)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_activity (
        user_id INTEGER,
        peer_id INTEGER,
        join_time TEXT,
        last_message_time TEXT,
        PRIMARY KEY (user_id, peer_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS chat_config (
        peer_id INTEGER PRIMARY KEY,
        silence BOOLEAN DEFAULT 0,
        antiflood BOOLEAN DEFAULT 0,
        flood_warn BOOLEAN DEFAULT 0,
        block_links BOOLEAN DEFAULT 0,
        block_invite_links BOOLEAN DEFAULT 0,
        kick_on_leave BOOLEAN DEFAULT 1,
        kick_on_invite BOOLEAN DEFAULT 1,
        block_group_invite BOOLEAN DEFAULT 0,
        games_enabled BOOLEAN DEFAULT 1,
        mention_all BOOLEAN DEFAULT 0,
        block_forward BOOLEAN DEFAULT 0,
        max_length INT DEFAULT 0,
        ban_voice BOOLEAN DEFAULT 0,
        ban_sticker BOOLEAN DEFAULT 0,
        ban_photo BOOLEAN DEFAULT 0,
        ban_video BOOLEAN DEFAULT 0,
        ban_graffiti BOOLEAN DEFAULT 0,
        ban_poll BOOLEAN DEFAULT 0,
        ban_audio BOOLEAN DEFAULT 0,
        ban_doc BOOLEAN DEFAULT 0,
        ban_wall BOOLEAN DEFAULT 0,
        welcome_message TEXT,
        public_to_check INTEGER DEFAULT 0,
        flood_count INTEGER DEFAULT 5,
        flood_seconds INTEGER DEFAULT 10,
        active BOOLEAN DEFAULT 0
    )''')
    c.execute("PRAGMA table_info(chat_config)")
    existing_cols = {col[1] for col in c.fetchall()}
    new_cols = {
        'public_to_check': 'INTEGER DEFAULT 0',
        'flood_count': 'INTEGER DEFAULT 5',
        'flood_seconds': 'INTEGER DEFAULT 10',
        'active': 'BOOLEAN DEFAULT 0'
    }
    for col, coltype in new_cols.items():
        if col not in existing_cols:
            try:
                c.execute(f"ALTER TABLE chat_config ADD COLUMN {col} {coltype}")
            except:
                pass
    c.execute('''CREATE TABLE IF NOT EXISTS chat_members (
        user_id INTEGER,
        peer_id INTEGER,
        level INTEGER DEFAULT 1,
        role_name TEXT,
        nickname TEXT,
        warnings INTEGER DEFAULT 0,
        vigors INTEGER DEFAULT 0,
        reg_date TEXT,
        messages_count INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, peer_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS confirmations (
        peer_id INTEGER,
        user_id INTEGER,
        command TEXT,
        timestamp REAL,
        PRIMARY KEY (peer_id, user_id)
    )''')
    try:
        c.execute("SELECT * FROM users LIMIT 1")
        c.execute("DROP TABLE IF EXISTS users")
    except:
        pass
    conn.commit()
    conn.close()

init_db()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
vk_session = vk_api.VkApi(token=TOKEN)
vk = vk_session.get_api()

def get_user_level(user_id, peer_id):
    if user_id == OWNER_ID:
        conn = sqlite3.connect('bot_database.db')
        c = conn.cursor()
        c.execute("SELECT level FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
        row = c.fetchone()
        conn.close()
        if row:
            return max(row[0], 13)
        else:
            return 13
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT level FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 1

def get_user_role_name(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT role_name, level FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    if row and row[0]:
        return row[0]
    if row:
        return ROLES.get(row[1], "Без должности")
    return "Без должности"

def set_user_role(user_id, peer_id, level, role_name):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO chat_members (user_id, peer_id, level, role_name) VALUES (?, ?, ?, ?)",
              (user_id, peer_id, level, role_name))
    conn.commit()
    conn.close()
    log_action(user_id, f"role_set_to_{level}_{role_name}", user_id)

def remove_user_role(user_id, peer_id):
    set_user_role(user_id, peer_id, 1, "Без должности")

def add_warning(user_id, peer_id, admin_id, reason):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("UPDATE chat_members SET warnings = COALESCE(warnings,0)+1 WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    if c.rowcount == 0:
        c.execute("INSERT INTO chat_members (user_id, peer_id, warnings) VALUES (?, ?, 1)", (user_id, peer_id))
    c.execute("INSERT INTO warnings_log (user_id, peer_id, admin_id, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
              (user_id, peer_id, admin_id, reason, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def remove_warning(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM warnings_log WHERE id = (SELECT id FROM warnings_log WHERE user_id=? AND peer_id=? ORDER BY timestamp DESC LIMIT 1)",
              (user_id, peer_id))
    c.execute("UPDATE chat_members SET warnings = warnings-1 WHERE user_id=? AND peer_id=? AND warnings>0", (user_id, peer_id))
    conn.commit()
    conn.close()

def get_warnings(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT warnings FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def get_warnings_log(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT admin_id, reason, timestamp FROM warnings_log WHERE user_id=? AND peer_id=? ORDER BY timestamp DESC", (user_id, peer_id))
    rows = c.fetchall()
    conn.close()
    return rows

def add_vigor(user_id, peer_id, admin_id, reason):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("UPDATE chat_members SET vigors = COALESCE(vigors,0)+1 WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    if c.rowcount == 0:
        c.execute("INSERT INTO chat_members (user_id, peer_id, vigors) VALUES (?, ?, 1)", (user_id, peer_id))
    c.execute("INSERT INTO vigors_log (user_id, peer_id, admin_id, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
              (user_id, peer_id, admin_id, reason, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def remove_vigor(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM vigors_log WHERE id = (SELECT id FROM vigors_log WHERE user_id=? AND peer_id=? ORDER BY timestamp DESC LIMIT 1)",
              (user_id, peer_id))
    c.execute("UPDATE chat_members SET vigors = vigors-1 WHERE user_id=? AND peer_id=? AND vigors>0", (user_id, peer_id))
    conn.commit()
    conn.close()

def get_vigors(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT vigors FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def get_vigors_log(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT admin_id, reason, timestamp FROM vigors_log WHERE user_id=? AND peer_id=? ORDER BY timestamp DESC", (user_id, peer_id))
    rows = c.fetchall()
    conn.close()
    return rows

def set_mute(user_id, peer_id, seconds, reason=""):
    until = (datetime.now() + timedelta(seconds=seconds)).isoformat()
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO mutes (user_id, peer_id, until, reason) VALUES (?, ?, ?, ?)",
              (user_id, peer_id, until, reason))
    conn.commit()
    conn.close()

def remove_mute(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM mutes WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    conn.commit()
    conn.close()

def is_muted(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT until FROM mutes WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    if row and row[0]:
        return datetime.now() < datetime.fromisoformat(row[0])
    return False

def update_activity(user_id, peer_id, is_message=True):
    now = datetime.now().isoformat()
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    if is_message:
        c.execute("INSERT OR REPLACE INTO user_activity (user_id, peer_id, last_message_time, join_time) VALUES (?, ?, ?, COALESCE((SELECT join_time FROM user_activity WHERE user_id=? AND peer_id=?), ?))",
                  (user_id, peer_id, now, user_id, peer_id, now))
    else:
        c.execute("INSERT OR REPLACE INTO user_activity (user_id, peer_id, join_time, last_message_time) VALUES (?, ?, ?, COALESCE((SELECT last_message_time FROM user_activity WHERE user_id=? AND peer_id=?), ?))",
                  (user_id, peer_id, now, user_id, peer_id, now))
    conn.commit()
    conn.close()

def get_join_time(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT join_time FROM user_activity WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def get_last_activity(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT last_message_time FROM user_activity WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def check_flood(user_id, peer_id, cfg):
    if not cfg['antiflood']:
        return False
    now = time.time()
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT timestamp FROM message_history WHERE user_id=? AND peer_id=? ORDER BY timestamp DESC LIMIT ?",
              (user_id, peer_id, cfg['flood_count']))
    rows = c.fetchall()
    timestamps = [r[0] for r in rows]
    c.execute("INSERT INTO message_history (user_id, peer_id, timestamp) VALUES (?, ?, ?)", (user_id, peer_id, now))
    c.execute("DELETE FROM message_history WHERE user_id=? AND peer_id=? AND timestamp < ?",
              (user_id, peer_id, now - cfg['flood_seconds']))
    conn.commit()
    conn.close()
    if len(timestamps) >= cfg['flood_count'] - 1:
        if timestamps and (now - timestamps[-1]) <= cfg['flood_seconds']:
            return True
    return False

def get_chat_config(peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT * FROM chat_config WHERE peer_id=?", (peer_id,))
    row = c.fetchone()
    conn.close()
    if row:
        columns = ['peer_id','silence','antiflood','flood_warn','block_links','block_invite_links',
                   'kick_on_leave','kick_on_invite','block_group_invite','games_enabled','mention_all',
                   'block_forward','max_length','ban_voice','ban_sticker','ban_photo','ban_video',
                   'ban_graffiti','ban_poll','ban_audio','ban_doc','ban_wall','welcome_message','public_to_check',
                   'flood_count','flood_seconds','active']
        return dict(zip(columns, row))
    else:
        return {
            'peer_id': peer_id,
            'silence': False,
            'antiflood': False,
            'flood_warn': False,
            'block_links': False,
            'block_invite_links': False,
            'kick_on_leave': True,
            'kick_on_invite': True,
            'block_group_invite': False,
            'games_enabled': True,
            'mention_all': False,
            'block_forward': False,
            'max_length': 0,
            'ban_voice': False,
            'ban_sticker': False,
            'ban_photo': False,
            'ban_video': False,
            'ban_graffiti': False,
            'ban_poll': False,
            'ban_audio': False,
            'ban_doc': False,
            'ban_wall': False,
            'welcome_message': None,
            'public_to_check': 0,
            'flood_count': 5,
            'flood_seconds': 10,
            'active': False
        }

def set_chat_config(peer_id, key, value):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT 1 FROM chat_config WHERE peer_id=?", (peer_id,))
    if not c.fetchone():
        c.execute("INSERT INTO chat_config (peer_id) VALUES (?)", (peer_id,))
    c.execute(f"UPDATE chat_config SET {key} = ? WHERE peer_id = ?", (value, peer_id))
    conn.commit()
    conn.close()

def ban_user(user_id, reason, admin_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO bans (user_id, reason, admin_id, timestamp) VALUES (?, ?, ?, ?)",
              (user_id, reason, admin_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    log_action(admin_id, f"ban_{user_id}", user_id)

def unban_user(user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM bans WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def is_banned(user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row is not None

def get_ban_reason(user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT reason FROM bans WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def add_filter(word):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO filters (word) VALUES (?)", (word.lower(),))
    conn.commit()
    conn.close()

def remove_filter(word):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM filters WHERE word=?", (word.lower(),))
    conn.commit()
    conn.close()

def get_filters():
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT word FROM filters")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def forbid_mention(user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO forbidden_mentions (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()

def allow_mention(user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM forbidden_mentions WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def is_mention_forbidden(user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT 1 FROM forbidden_mentions WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row is not None

def increment_messages(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("UPDATE chat_members SET messages_count = COALESCE(messages_count,0)+1 WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    if c.rowcount == 0:
        c.execute("INSERT INTO chat_members (user_id, peer_id, messages_count) VALUES (?, ?, 1)", (user_id, peer_id))
    conn.commit()
    conn.close()

def get_messages_count(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT messages_count FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def set_reg_date(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("UPDATE chat_members SET reg_date = ? WHERE user_id=? AND peer_id=?", (datetime.now().isoformat(), user_id, peer_id))
    if c.rowcount == 0:
        c.execute("INSERT INTO chat_members (user_id, peer_id, reg_date) VALUES (?, ?, ?)", (user_id, peer_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_reg_date(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT reg_date FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "Не зарегистрирован"

def set_nickname(user_id, peer_id, nick):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO chat_members (user_id, peer_id, nickname) VALUES (?, ?, ?)", (user_id, peer_id, nick))
    conn.commit()
    conn.close()

def get_nickname(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT nickname FROM chat_members WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def remove_nickname(user_id, peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("UPDATE chat_members SET nickname = NULL WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    conn.commit()
    conn.close()

def log_action(admin_id, action, target_id=None):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT INTO logs (user_id, action, target_id, timestamp) VALUES (?, ?, ?, ?)",
              (admin_id, action, target_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_logs(user_id=None, limit=50):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    if user_id:
        c.execute("SELECT user_id, action, target_id, timestamp FROM logs WHERE user_id=? OR target_id=? ORDER BY timestamp DESC LIMIT ?", (user_id, user_id, limit))
    else:
        c.execute("SELECT user_id, action, target_id, timestamp FROM logs ORDER BY timestamp DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_user_by_mention(text):
    m = re.search(r'\[id(\d+)\|.*?\]', text)
    if m:
        return int(m.group(1))
    if text.isdigit():
        return int(text)
    return None

def get_user_display_link(user_id, peer_id):
    nick = get_nickname(user_id, peer_id)
    if nick:
        return f"[id{user_id}|{nick}]"
    return f"[id{user_id}|{user_id}]"

def get_user_display_name(user_id, peer_id):
    nick = get_nickname(user_id, peer_id)
    if nick:
        return nick
    try:
        info = vk.users.get(user_ids=user_id)[0]
        return f"{info['first_name']} {info['last_name']}"
    except:
        return f"id{user_id}"

def send_message(peer_id, text, reply_to=None):
    try:
        vk.messages.send(peer_id=peer_id, message=text[:4000], random_id=0, reply_to=reply_to)
    except Exception as e:
        print(f"Ошибка отправки: {e}")

def kick_from_chat(peer_id, user_id):
    if peer_id > 2000000000:
        try:
            vk.messages.removeChatUser(chat_id=peer_id-2000000000, user_id=user_id)
            return True
        except:
            pass
    return False

def get_target_from_event(event, args, peer_id):
    if args:
        user_id = get_user_by_mention(args)
        if user_id:
            return user_id, args
    reply_msg = event.obj.message.get('reply_message')
    if reply_msg:
        return reply_msg['from_id'], args
    return None, args

def kick_on_three_warnings(user_id, peer_id, admin_id, reason=""):
    warns = get_warnings(user_id, peer_id)
    if warns >= 3:
        display_link = get_user_display_link(user_id, peer_id)
        if kick_from_chat(peer_id, user_id):
            send_message(peer_id, f"🚪 {display_link} кикнут за 3 предупреждения. Причина: {reason if reason else 'Автоматически'}")
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("UPDATE chat_members SET warnings = 0 WHERE user_id=? AND peer_id=?", (user_id, peer_id))
            conn.commit()
            conn.close()
            log_action(admin_id, f"auto_kick_3_warns_{user_id}", user_id)
        else:
            send_message(peer_id, f"❌ Не удалось кикнуть {display_link} (бот не админ).")
    return False

def kick_on_three_vigors(user_id, peer_id, admin_id, reason=""):
    vigs = get_vigors(user_id, peer_id)
    if vigs >= 3:
        display_link = get_user_display_link(user_id, peer_id)
        if kick_from_chat(peer_id, user_id):
            send_message(peer_id, f"🚪 {display_link} кикнут за 3 выговора. Причина: {reason if reason else 'Автоматически'}")
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("UPDATE chat_members SET vigors = 0 WHERE user_id=? AND peer_id=?", (user_id, peer_id))
            conn.commit()
            conn.close()
            log_action(admin_id, f"auto_kick_3_vigors_{user_id}", user_id)
        else:
            send_message(peer_id, f"❌ Не удалось кикнуть {display_link} (бот не админ).")
    return False

def set_confirmation(peer_id, user_id, command):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO confirmations (peer_id, user_id, command, timestamp) VALUES (?, ?, ?, ?)",
              (peer_id, user_id, command, time.time()))
    conn.commit()
    conn.close()

def check_confirmation(peer_id, user_id, command):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("SELECT timestamp FROM confirmations WHERE peer_id=? AND user_id=? AND command=?", (peer_id, user_id, command))
    row = c.fetchone()
    conn.close()
    if row and (time.time() - row[0]) < 60:
        return True
    return False

def clear_confirmation(peer_id, user_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM confirmations WHERE peer_id=? AND user_id=?", (peer_id, user_id))
    conn.commit()
    conn.close()

def extract_public_id_from_arg(arg):
    if arg.isdigit():
        return int(arg)
    match = re.search(r'(?:public|club)(\d+)', arg)
    if match:
        return int(match.group(1))
    try:
        response = vk.utils.resolveScreenName(screen_name=arg)
        if response and response['type'] in ('group', 'page'):
            return response['object_id']
    except:
        pass
    return None

def clear_chat_data(peer_id):
    conn = sqlite3.connect('bot_database.db')
    c = conn.cursor()
    c.execute("DELETE FROM chat_config WHERE peer_id = ?", (peer_id,))
    c.execute("DELETE FROM mutes WHERE peer_id = ?", (peer_id,))
    c.execute("DELETE FROM user_activity WHERE peer_id = ?", (peer_id,))
    c.execute("DELETE FROM message_history WHERE peer_id = ?", (peer_id,))
    c.execute("DELETE FROM chat_members WHERE peer_id = ?", (peer_id,))
    c.execute("DELETE FROM warnings_log WHERE peer_id = ?", (peer_id,))
    c.execute("DELETE FROM vigors_log WHERE peer_id = ?", (peer_id,))
    conn.commit()
    conn.close()

def clear_user_messages(peer_id, user_id):
    try:
        count = 0
        offset = 0
        while True:
            history = vk.messages.getHistory(peer_id=peer_id, count=200, offset=offset)
            items = history['items']
            if not items:
                break
            msg_ids = []
            for msg in items:
                if msg['from_id'] == -GROUP_ID:
                    text = msg.get('text', '')
                    reply_msg = msg.get('reply_message')
                    if f'[id{user_id}|' in text or (reply_msg and reply_msg['from_id'] == user_id):
                        msg_ids.append(msg['id'])
            if msg_ids:
                vk.messages.delete(message_ids=msg_ids, delete_for_all=1)
                count += len(msg_ids)
            if len(items) < 200:
                break
            offset += 200
            time.sleep(0.3)
        return count
    except Exception as e:
        print(f"Ошибка при очистке сообщений: {e}")
        return -1

# ========== ОСНОВНОЙ БОТ ==========
longpoll = VkBotLongPoll(vk_session, GROUP_ID)
print("✅ Бот запущен. Жду команды...")

processed_messages = {}

for event in longpoll.listen():
    if event.type == VkBotEventType.MESSAGE_NEW and (event.from_user or event.from_chat):
        msg = event.obj.message
        peer_id = msg['peer_id']
        from_id = msg['from_id']
        msg_id = msg['id']
        text = msg.get('text', '').strip()
        text = re.sub(r'[\u200b\u2060\uFEFF]', '', text)
        
        msg_key = f"{peer_id}_{msg_id}"
        if msg_key in processed_messages and time.time() - processed_messages[msg_key] < 2:
            continue
        processed_messages[msg_key] = time.time()
        for key in list(processed_messages.keys()):
            if time.time() - processed_messages[key] > 5:
                del processed_messages[key]
        
        cfg = get_chat_config(peer_id)
        
        # Если чат ещё не активирован
        if not cfg['active']:
            # Специальная обработка слова "активировать" (без слеша)
            if text.lower() == 'активировать':
                set_chat_config(peer_id, 'active', True)
                send_message(peer_id, "✅ Бот активирован! Теперь можно использовать команды.")
                continue
            else:
                # Пропускаем все остальные сообщения, не обрабатываем команды
                continue
        
        # Далее – стандартный поток, если чат активен
        if is_muted(from_id, peer_id):
            send_message(peer_id, "⏰ Вы в муте в этой беседе.", reply_to=msg['id'])
            continue
        
        action = msg.get('action')
        if action:
            action_type = action.get('type')
            invited_id = action.get('member_id')
            # Бота пригласили
            if action_type in ('chat_invite_user', 'chat_invite_user_by_link') and invited_id == -GROUP_ID:
                inviter_id = from_id
                try:
                    inviter_info = vk.users.get(user_ids=inviter_id)
                    inviter_name = inviter_info[0]['first_name'] + ' ' + inviter_info[0]['last_name']
                except:
                    inviter_name = f"id{inviter_id}"
                clear_chat_data(peer_id)
                set_user_role(inviter_id, peer_id, 13, "Владелец")
                welcome_msg = (
                    "💞 Спасибо за добавление :3\n\n"
                    "⚙ Для полноценной работы бота, нужно нажать на название беседы и кликнуть по кнопке «Назначить администратором» возле бота\n\n"
                    "📝 Команды бота: /help\n\n"
                    "потом что бы сделать начать работать должны написать активировать"
                )
                send_message(peer_id, welcome_msg)
                set_chat_config(peer_id, 'active', False)  # ждём активации
                continue
            
            # Приглашение нового пользователя (для приветствия)
            if action_type == 'chat_invite_user' and invited_id and invited_id > 0:
                update_activity(invited_id, peer_id, is_message=False)
                welcome = cfg.get('welcome_message')
                if welcome:
                    send_message(peer_id, f"{get_user_display_link(invited_id, peer_id)}, {welcome}", reply_to=None)
            
            if action_type == 'chat_invite_user' and cfg['kick_on_invite']:
                invited_id = action.get('member_id')
                inviter_id = from_id
                if invited_id and invited_id > 0 and invited_id != -GROUP_ID and get_user_level(inviter_id, peer_id) < 2:
                    kick_from_chat(peer_id, invited_id)
                    send_message(peer_id, f"🚫 {get_user_display_link(invited_id, peer_id)} кикнут, т.к. пригласил не модератор.")
            if action_type == 'chat_invite_user' and cfg['block_group_invite']:
                invited_id = action.get('member_id')
                if invited_id and invited_id < 0:
                    kick_from_chat(peer_id, invited_id)
                    send_message(peer_id, f"🚫 Сообщество {get_user_display_link(invited_id, peer_id)} кикнуто (запрещено добавление сообществ).")
                    add_warning(from_id, peer_id, from_id, "Попытка добавить сообщество")
        
        update_activity(from_id, peer_id, is_message=True)
        increment_messages(from_id, peer_id)
        if get_reg_date(from_id, peer_id) == "Не зарегистрирован":
            set_reg_date(from_id, peer_id)
        
        if cfg['silence'] and get_user_level(from_id, peer_id) < 8:
            send_message(peer_id, "🔇 Режим тишины, писать могут только администраторы (уровень 8+).", reply_to=msg['id'])
            continue
        
        if cfg['max_length'] > 0 and len(text) > cfg['max_length']:
            send_message(peer_id, f"❌ Превышена максимальная длина сообщения ({cfg['max_length']} символов).", reply_to=msg['id'])
            continue
        
        if cfg['block_forward'] and msg.get('fwd_messages'):
            send_message(peer_id, "❌ Пересылка сообщений запрещена в этой беседе.", reply_to=msg['id'])
            continue
        
        if cfg['block_links'] and re.search(r'https?://\S+', text, re.I):
            send_message(peer_id, "❌ Отправка ссылок запрещена.", reply_to=msg['id'])
            continue
        
        if cfg['block_invite_links'] and re.search(r'(vk\.me/join|vk\.com/join)', text, re.I):
            send_message(peer_id, "❌ Отправка ссылок на приглашение в беседу запрещена.", reply_to=msg['id'])
            continue
        
        if not cfg['mention_all'] and ('@all' in text.lower() or '@все' in text.lower()):
            send_message(peer_id, "❌ Упоминание всех (@all) запрещено в этой беседе.", reply_to=msg['id'])
            continue
        
        attachments = msg.get('attachments', [])
        for att in attachments:
            att_type = att['type']
            if att_type == 'audio_message' and cfg['ban_voice']:
                send_message(peer_id, "❌ Голосовые сообщения запрещены.", reply_to=msg['id'])
                break
            if att_type == 'sticker' and cfg['ban_sticker']:
                send_message(peer_id, "❌ Стикеры запрещены.", reply_to=msg['id'])
                break
            if att_type == 'photo' and cfg['ban_photo']:
                send_message(peer_id, "❌ Фотографии запрещены.", reply_to=msg['id'])
                break
            if att_type == 'video' and cfg['ban_video']:
                send_message(peer_id, "❌ Видеозаписи запрещены.", reply_to=msg['id'])
                break
            if att_type == 'graffiti' and cfg['ban_graffiti']:
                send_message(peer_id, "❌ Граффити запрещены.", reply_to=msg['id'])
                break
            if att_type == 'poll' and cfg['ban_poll']:
                send_message(peer_id, "❌ Опросы запрещены.", reply_to=msg['id'])
                break
            if att_type == 'audio' and cfg['ban_audio']:
                send_message(peer_id, "❌ Аудиозаписи запрещены.", reply_to=msg['id'])
                break
            if att_type == 'doc' and cfg['ban_doc']:
                send_message(peer_id, "❌ Документы запрещены.", reply_to=msg['id'])
                break
            if att_type == 'wall' and cfg['ban_wall']:
                send_message(peer_id, "❌ Записи на стене запрещены.", reply_to=msg['id'])
                break
        
        if check_flood(from_id, peer_id, cfg):
            if cfg['flood_warn']:
                add_warning(from_id, peer_id, from_id, "Флуд")
                send_message(peer_id, f"⚠️ {get_user_display_link(from_id, peer_id)} флудит! Выдано предупреждение. Всего: {get_warnings(from_id, peer_id)} | Причина: Флуд", reply_to=msg['id'])
                kick_on_three_warnings(from_id, peer_id, from_id, "Флуд")
            else:
                set_mute(from_id, peer_id, 60, "Флуд")
                send_message(peer_id, f"🔇 {get_user_display_link(from_id, peer_id)} замучен на 60 секунд за флуд.", reply_to=msg['id'])
            continue
        
        for word in get_filters():
            if word in text.lower():
                send_message(peer_id, f"⚠️ Обнаружено запрещённое слово: {word}. Вы получили предупреждение.", reply_to=msg['id'])
                add_warning(from_id, peer_id, from_id, f"Запрещённое слово: {word}")
                log_action(from_id, "filter_violation", None)
                kick_on_three_warnings(from_id, peer_id, from_id, f"Запрещённое слово: {word}")
                break
        
        for uid in re.findall(r'\[id(\d+)\|.*?\]', text):
            uid = int(uid)
            if is_mention_forbidden(uid):
                send_message(peer_id, f"❌ Упоминание пользователя {get_user_display_link(uid, peer_id)} запрещено. Вы получили предупреждение.", reply_to=msg['id'])
                add_warning(from_id, peer_id, from_id, f"Упоминание запрещённого пользователя {uid}")
                log_action(from_id, "forbidden_mention", uid)
                kick_on_three_warnings(from_id, peer_id, from_id, f"Упоминание запрещённого пользователя")
                break
        
        if not text.startswith('/'):
            continue
        
        parts = text.split(maxsplit=1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ''
        
        user_lvl = get_user_level(from_id, peer_id)
        
        # ---------- /help ----------
        if cmd == '/help':
            if user_lvl < 2:
                send_message(peer_id, "❌ У вас нет прав для использования команд.")
                continue
            help_text = """📖 **Доступные команды (уровень 2+):**

🛡 **Команды модератора:**
/kick - исключить пользователя из беседы
/ban - заблокировать пользователя (глобально)
/unban - снять блокировку
/banlist - список заблокированных
/getban [пользователь] - узнать, забанен ли пользователь и причина
/warn [пользователь] [причина] - выдать предупреждение
/unwarn [пользователь] - снять предупреждение
/getwarn [пользователь] - список предупреждений с причинами
/warnlist - список предупреждений (количество)
/admins - список персонала с ролями
/mute [пользователь] [сек] [причина] - замутить
/unmute [пользователь] - снять мут
/online - список администраторов онлайн
/stats [пользователь] - статистика
/reg [пользователь] - дата входа в беседу
/reglist - список дат входа в беседу
/setnick [пользователь] [ник] [роль] - установить ник и роль
/rnick [пользователь] - удалить ник
/getnick [пользователь] - показать ник
/nicklist - администраторы по ролям
/nonicks - пользователи без ников
/koreshki - топ старейших и активных участников
/invate [пользователь] - пригласить пользователя в чат
/ticket [текст] - сообщить о баге или предложении (напишет разработчику)
/pin (ответом на сообщение) - закрепить сообщение
/unpin - открепить закреплённое сообщение

🌟 **Команды администратора (уровень 8+):**
/set_admin [пользователь] [роль] - назначить роль
/removerole [пользователь] - снять все роли
/roles - список ролей
/вызов [причина] - вызов администрации
/silence - включить/выключить режим тишины
/vig [пользователь] [причина] - выговор
/unvig [пользователь] - снять выговор
/getvig [пользователь] - список выговоров с причинами
/viglist - список выговоров (количество)
/clear [N] - удалить N сообщений бота

✏ **Команды спец. администратора (уровень 11+):**
/stitle [название] - изменить название беседы
/rtitle - сбросить название
/filter [слово] - добавить фильтр
/rfilter [слово] - удалить фильтр
/flist - список фильтров
/logs [пользователь] - логи действий
/mention [пользователь] - запретить упоминание
/unmention [пользователь] - разрешить упоминание
/rkick - кикнуть добавленных за 24 часа

👑 **Команды владельца (уровень 13):**
/жив - проверка работы бота
/приветствие [текст] - установить приветствие
/settings - настройки беседы
/setpublic [ссылка или id] - привязать паблик для проверки
/removepublic - отвязать паблик
/checkpublic - проверить подписку и кикнуть, если не подписан
/mtop [N] - топ по сообщениям
/editowner [пользователь] - передать права владельца
/editcmd - настройка прав команд (в разработке)
/inactive - кикнуть неактивных (2 дня)
/rec - перезагрузить бота (только для разработчика)

---
❗ **Встретил баг или есть предложение?** – используй команду `/ticket [текст]` или напиши разработчику: @srusskiy6"""
            send_message(peer_id, help_text)
        
        # ---------- /rec ----------
        elif cmd == '/rec':
            if from_id != OWNER_ID:
                send_message(peer_id, "❌ Эта команда доступна только разработчику бота.")
                continue
            send_message(peer_id, "🔄 Перезагрузка бота...")
            sys.exit(0)
        
        # ---------- /ticket ----------
        elif cmd == '/ticket':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            if not args:
                send_message(peer_id, "❌ Укажите текст сообщения: /ticket [описание проблемы]")
                continue
            user_link = get_user_display_link(from_id, peer_id)
            ticket_text = f"📩 **Новый тикет от {user_link}** (id{from_id}):\n\n{args}\n\n⚠️ *Беседа: {peer_id}*"
            try:
                vk.messages.send(peer_id=OWNER_ID, message=ticket_text[:4000], random_id=0)
                send_message(peer_id, "✅ Ваше сообщение отправлено разработчику в личные сообщения. Спасибо за обратную связь!")
                log_action(from_id, f"ticket_sent: {args[:50]}", None)
            except Exception as e:
                send_message(peer_id, f"❌ Ошибка при отправке тикета: {e}")
            continue
        
        # ---------- /pin ----------
        elif cmd == '/pin':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            reply_msg = msg.get('reply_message')
            if not reply_msg:
                send_message(peer_id, "❌ Ответьте на сообщение, которое нужно закрепить.")
                continue
            target_msg_id = reply_msg['id']
            try:
                vk.messages.pin(peer_id=peer_id, message_id=target_msg_id)
                send_message(peer_id, f"✅ Сообщение закреплено.")
                log_action(from_id, f"pin_message_{target_msg_id}", None)
            except Exception as e:
                send_message(peer_id, f"❌ Ошибка при закреплении: {e}")
            continue
        
        # ---------- /unpin ----------
        elif cmd == '/unpin':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            try:
                vk.messages.unpin(peer_id=peer_id)
                send_message(peer_id, f"✅ Закреплённое сообщение откреплено.")
                log_action(from_id, f"unpin_message", None)
            except Exception as e:
                send_message(peer_id, f"❌ Ошибка при откреплении: {e}")
            continue
        
        # ---------- /invate ----------
        elif cmd == '/invate':
            if user_lvl < 7:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 7+)")
                continue
            if not args:
                send_message(peer_id, "❌ Укажите пользователя: /invate @user")
                continue
            target_id = get_user_by_mention(args)
            if not target_id:
                send_message(peer_id, "❌ Не удалось определить пользователя.")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            chat_id = peer_id - 2000000000
            try:
                vk.messages.addChatUser(chat_id=chat_id, user_id=target_id)
                send_message(peer_id, f"✅ Пользователь {get_user_display_link(target_id, peer_id)} приглашён в беседу.")
                log_action(from_id, f"invite_{target_id}", target_id)
            except Exception as e:
                error_msg = str(e)
                if "15" in error_msg or "cant add user" in error_msg.lower():
                    send_message(peer_id, f"❌ Не удалось пригласить пользователя. Возможно, он уже в беседе или бот не администратор.")
                else:
                    send_message(peer_id, f"❌ Ошибка при приглашении: {error_msg}")
            continue
        
        # ---------- /приветствие ----------
        elif cmd == '/приветствие':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            if not args:
                current = cfg.get('welcome_message')
                if current:
                    send_message(peer_id, f"📝 Текущее приветствие: {current}")
                else:
                    send_message(peer_id, "Приветствие не установлено. /приветствие [текст]")
            else:
                set_chat_config(peer_id, 'welcome_message', args)
                send_message(peer_id, f"✅ Приветствие установлено.")
        
        # ---------- /koreshki ----------
        elif cmd == '/koreshki':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, join_time FROM user_activity WHERE peer_id=? AND join_time IS NOT NULL ORDER BY join_time ASC LIMIT 5", (peer_id,))
            oldest = c.fetchall()
            c.execute("SELECT user_id, last_message_time FROM user_activity WHERE peer_id=? AND last_message_time IS NOT NULL ORDER BY last_message_time DESC LIMIT 5", (peer_id,))
            active = c.fetchall()
            conn.close()
            msg = "👥 **Корешки (старейшины):**\n"
            if oldest:
                for uid, t in oldest:
                    name = get_user_display_name(uid, peer_id)
                    dt = t[:19] if t else "неизвестно"
                    msg += f"• {name} (id{uid}) — с {dt}\n"
            else:
                msg += "Нет данных\n"
            msg += "\n🔥 **Самые активные (недавно писали):**\n"
            if active:
                for uid, t in active:
                    name = get_user_display_name(uid, peer_id)
                    dt = t[:19] if t else "никогда"
                    msg += f"• {name} (id{uid}) — {dt}\n"
            else:
                msg += "Нет данных\n"
            send_message(peer_id, msg)
        
        # ---------- /myrole ----------
        elif cmd == '/myrole':
            role = get_user_role_name(from_id, peer_id)
            level = user_lvl
            send_message(peer_id, f"Ваша роль: {role} (уровень {level})")
        
        # ---------- /roles ----------
        elif cmd == '/roles':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            msg = "📋 **Доступные роли и уровни:**\n\n"
            grouped = {}
            for name, lvl in ROLE_ALIASES.items():
                if lvl not in grouped:
                    grouped[lvl] = []
                grouped[lvl].append(name)
            for lvl in sorted(grouped.keys()):
                msg += f"**Уровень {lvl}:**\n"
                for name in grouped[lvl]:
                    msg += f"• {name}\n"
                msg += "\n"
            send_message(peer_id, msg)
        
        # ---------- /set_admin ----------
        elif cmd == '/set_admin':
            if user_lvl not in (7,8,11,12,13):
                send_message(peer_id, "❌ Недостаточно прав. Требуется уровень 7,8,11,12 или 13.")
                continue
            if not args:
                send_message(peer_id, "❌ Использование: /set_admin [пользователь] [роль/уровень]")
                continue
            parts_admin = args.split(maxsplit=1)
            if len(parts_admin) < 2:
                send_message(peer_id, "❌ Укажите пользователя и роль.")
                continue
            target_id = get_user_by_mention(parts_admin[0])
            if not target_id:
                send_message(peer_id, "❌ Не удалось определить пользователя.")
                continue
            role_input = parts_admin[1].strip()
            if role_input.isdigit():
                level = int(role_input)
                role_name = None
                for name, lvl in ROLE_ALIASES.items():
                    if lvl == level:
                        role_name = name
                        break
                if not role_name:
                    role_name = ROLES.get(level, "Неизвестно")
            else:
                level = ROLE_ALIASES.get(role_input)
                if level is None:
                    send_message(peer_id, f"❌ Роль '{role_input}' не найдена. Список ролей: /roles")
                    continue
                role_name = role_input
            if target_id == OWNER_ID:
                send_message(peer_id, "❌ Нельзя изменить роль владельца.")
                continue
            set_user_role(target_id, peer_id, level, role_name)
            send_message(peer_id, f"✅ {get_user_display_link(target_id, peer_id)} назначена роль: {role_name} (уровень {level})")
        
        # ---------- /removerole ----------
        elif cmd == '/removerole':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя.")
                continue
            if target_id == OWNER_ID:
                send_message(peer_id, "❌ Нельзя снять права с владельца.")
                continue
            remove_user_role(target_id, peer_id)
            send_message(peer_id, f"✅ Сняты права с {get_user_display_link(target_id, peer_id)} (теперь 'Без должности').")
        
        # ---------- /admins ----------
        elif cmd == '/admins':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, level, role_name FROM chat_members WHERE peer_id=? AND level > 1 ORDER BY level DESC", (peer_id,))
            rows = c.fetchall()
            conn.close()
            staff = []
            seen = set()
            for uid, lvl, rname in rows:
                if uid not in seen:
                    staff.append((uid, lvl, rname))
                    seen.add(uid)
            staff.sort(key=lambda x: x[1], reverse=True)
            if not staff:
                send_message(peer_id, "Нет персонала.")
            else:
                msg = "🌟 **Admin состав: Veil**\n\n"
                for i, (uid, lvl, rname) in enumerate(staff, 1):
                    role_name = rname if rname else get_user_role_name(uid, peer_id)
                    display_name = get_user_display_name(uid, peer_id)
                    msg += f"{i}. {display_name} (id{uid}) - {role_name}\n"
                send_message(peer_id, msg)
        
        # ---------- /nicklist ----------
        elif cmd == '/nicklist':
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, level, role_name FROM chat_members WHERE peer_id=? AND level > 1 ORDER BY level DESC", (peer_id,))
            rows = c.fetchall()
            conn.close()
            if not rows:
                send_message(peer_id, "Нет пользователей с ролями.")
            else:
                grouped = {}
                for uid, lvl, rname in rows:
                    role = rname if rname else get_user_role_name(uid, peer_id)
                    if role not in grouped:
                        grouped[role] = []
                    grouped[role].append(uid)
                msg = "📋 **Список администраторов по ролям:**\n\n"
                for role, uids in grouped.items():
                    msg += f"**{role}:**\n"
                    for uid in uids:
                        msg += f"• {get_user_display_name(uid, peer_id)} (id{uid})\n"
                    msg += "\n"
                send_message(peer_id, msg)
        
        # ---------- /setnick ----------
        elif cmd == '/setnick':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            parts_nick = args.split(maxsplit=2)
            if len(parts_nick) < 3:
                send_message(peer_id, "❌ Использование: /setnick [пользователь] [ник] [роль]")
                continue
            target_id = get_user_by_mention(parts_nick[0])
            if not target_id:
                send_message(peer_id, "❌ Не удалось определить пользователя.")
                continue
            nickname = parts_nick[1].strip()
            if not nickname:
                send_message(peer_id, "❌ Ник не может быть пустым.")
                continue
            role_input = parts_nick[2].strip()
            if role_input.isdigit():
                level = int(role_input)
                role_name = None
                for name, lvl in ROLE_ALIASES.items():
                    if lvl == level:
                        role_name = name
                        break
                if not role_name:
                    role_name = ROLES.get(level, "Неизвестно")
            else:
                level = ROLE_ALIASES.get(role_input)
                if level is None:
                    send_message(peer_id, f"❌ Роль '{role_input}' не найдена. Список ролей: /roles")
                    continue
                role_name = role_input
            if target_id == OWNER_ID:
                send_message(peer_id, "❌ Нельзя изменить роль владельца.")
                continue
            set_nickname(target_id, peer_id, nickname)
            set_user_role(target_id, peer_id, level, role_name)
            send_message(peer_id, f"✅ Пользователю {get_user_display_link(target_id, peer_id)} установлен ник '{nickname}' и роль {role_name} (уровень {level}).")
        
        # ---------- /rnick ----------
        elif cmd == '/rnick':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя.")
                continue
            remove_nickname(target_id, peer_id)
            send_message(peer_id, f"✅ Ник удалён у {get_user_display_link(target_id, peer_id)}")
        
        # ---------- /getnick ----------
        elif cmd == '/getnick':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                target_id = from_id
            nick = get_nickname(target_id, peer_id)
            send_message(peer_id, f"📛 Ник {get_user_display_link(target_id, peer_id)}: {nick if nick else 'не установлен'}")
        
        # ---------- /nonicks ----------
        elif cmd == '/nonicks':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id FROM chat_members WHERE peer_id=? AND (nickname IS NULL OR nickname='') AND level=1", (peer_id,))
            rows = c.fetchall()
            conn.close()
            if rows:
                msg = "👤 Без ников:\n" + "\n".join([f"• {get_user_display_name(uid, peer_id)} (id{uid})" for (uid,) in rows[:20]])
                send_message(peer_id, msg)
            else:
                send_message(peer_id, "Все пользователи имеют ники.")
        
        # ---------- /kick ----------
        elif cmd == '/kick':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, new_args = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            if get_user_level(target_id, peer_id) >= user_lvl and target_id != OWNER_ID:
                send_message(peer_id, "❌ Нельзя кикнуть пользователя с равным или высшим уровнем.")
                continue
            reason = new_args if new_args else "Не указана"
            display_link = get_user_display_link(target_id, peer_id)
            if kick_from_chat(peer_id, target_id):
                send_message(peer_id, f"🚪 {display_link} исключён. Причина: {reason}")
                log_action(from_id, f"kick_{target_id}", target_id)
            else:
                send_message(peer_id, f"❌ Не удалось исключить {display_link}. Бот должен быть администратором беседы.")
        
        # ---------- /ban ----------
        elif cmd == '/ban':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, new_args = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            if get_user_level(target_id, peer_id) >= user_lvl and target_id != OWNER_ID:
                send_message(peer_id, "❌ Нельзя забанить пользователя с равным или высшим уровнем.")
                continue
            reason = new_args if new_args else "Не указана"
            display_link = get_user_display_link(target_id, peer_id)
            ban_user(target_id, reason, from_id)
            kick_from_chat(peer_id, target_id)
            send_message(peer_id, f"🔨 {display_link} забанен. Причина: {reason}")
        
        # ---------- /unban ----------
        elif cmd == '/unban':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            if is_banned(target_id):
                unban_user(target_id)
                send_message(peer_id, f"✅ {get_user_display_link(target_id, peer_id)} разбанен.")
            else:
                send_message(peer_id, "❌ Пользователь не в бане.")
        
        # ---------- /banlist ----------
        elif cmd == '/banlist':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, reason FROM bans")
            rows = c.fetchall()
            conn.close()
            if rows:
                msg = "📋 Забаненные:\n" + "\n".join([f"• {get_user_display_link(uid, peer_id)} - {reason}" for uid, reason in rows[:10]])
                send_message(peer_id, msg)
            else:
                send_message(peer_id, "Список блокировок пуст.")
        
        # ---------- /getban ----------
        elif cmd == '/getban':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            if is_banned(target_id):
                reason = get_ban_reason(target_id)
                send_message(peer_id, f"🔒 {get_user_display_link(target_id, peer_id)} забанен. Причина: {reason}")
            else:
                send_message(peer_id, f"✅ {get_user_display_link(target_id, peer_id)} не забанен.")
        
        # ---------- /warn ----------
        elif cmd == '/warn':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, new_args = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            if get_user_level(target_id, peer_id) >= user_lvl and target_id != OWNER_ID:
                send_message(peer_id, "❌ Нельзя выдать предупреждение пользователю с равным или высшим уровнем.")
                continue
            reason = new_args if new_args else "Не указана"
            add_warning(target_id, peer_id, from_id, reason)
            warns = get_warnings(target_id, peer_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"⚠️ {display_link} | Предупреждение. Всего: {warns} | Причина: {reason}")
            log_action(from_id, f"warn_{target_id}", target_id)
            kick_on_three_warnings(target_id, peer_id, from_id, reason)
        
        # ---------- /unwarn ----------
        elif cmd == '/unwarn':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            remove_warning(target_id, peer_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"✅ {display_link} | Снято последнее предупреждение. Осталось: {get_warnings(target_id, peer_id)}")
        
        # ---------- /getwarn ----------
        elif cmd == '/getwarn':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                target_id = from_id
            cnt = get_warnings(target_id, peer_id)
            logs = get_warnings_log(target_id, peer_id)
            if logs:
                msg = f"⚠️ **Предупреждения {get_user_display_link(target_id, peer_id)}** - всего {cnt}:\n"
                for admin_id, reason, ts in logs:
                    admin_name = get_user_display_name(admin_id, peer_id)
                    msg += f"• {ts[:19]}: {reason} (выдал: {admin_name})\n"
                send_message(peer_id, msg)
            else:
                send_message(peer_id, f"⚠️ У {get_user_display_link(target_id, peer_id)} нет предупреждений.")
        
        # ---------- /warnlist ----------
        elif cmd == '/warnlist':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, warnings FROM chat_members WHERE peer_id=? AND warnings>0 ORDER BY warnings DESC", (peer_id,))
            rows = c.fetchall()
            conn.close()
            if rows:
                msg = "⚠️ Список предупреждений (количество):\n" + "\n".join([f"• {get_user_display_link(uid, peer_id)} - {w}" for uid, w in rows[:10]])
                send_message(peer_id, msg)
            else:
                send_message(peer_id, "Нет пользователей с предупреждениями.")
        
        # ---------- /mute ----------
        elif cmd == '/mute':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            if not args:
                send_message(peer_id, "❌ Использование: /mute [пользователь] [секунды] [причина]")
                continue
            parts_mute = args.split(maxsplit=2)
            if len(parts_mute) < 2:
                send_message(peer_id, "❌ Укажите пользователя, время в секундах и причину.")
                continue
            target_id = get_user_by_mention(parts_mute[0])
            if not target_id:
                send_message(peer_id, "❌ Не удалось определить пользователя.")
                continue
            try:
                seconds = int(parts_mute[1])
            except:
                send_message(peer_id, "❌ Время должно быть числом (секунды).")
                continue
            reason = parts_mute[2] if len(parts_mute) > 2 else "Не указана"
            if get_user_level(target_id, peer_id) >= user_lvl and target_id != OWNER_ID:
                send_message(peer_id, "❌ Нельзя замутить пользователя с равным или высшим уровнем.")
                continue
            set_mute(target_id, peer_id, seconds, reason)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"🔇 {display_link} замучен на {seconds} сек. Причина: {reason}")
            log_action(from_id, f"mute_{target_id}_{seconds}", target_id)
        
        # ---------- /unmute ----------
        elif cmd == '/unmute':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            remove_mute(target_id, peer_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"✅ Снят мут с {display_link} в этой беседе.")
        
        # ---------- /online ----------
        elif cmd == '/online':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id FROM chat_members WHERE peer_id=? AND level>=2", (peer_id,))
            admins = c.fetchall()
            conn.close()
            if admins:
                mentions = " ".join([get_user_display_link(uid, peer_id) for (uid,) in admins])
                send_message(peer_id, f"🟢 Администраторы онлайн: {mentions}")
            else:
                send_message(peer_id, "Нет администраторов.")
        
        # ---------- /stats ----------
        elif cmd == '/stats':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                target_id = from_id
            msgs = get_messages_count(target_id, peer_id)
            warns = get_warnings(target_id, peer_id)
            vigors = get_vigors(target_id, peer_id)
            level = get_user_level(target_id, peer_id)
            role = get_user_role_name(target_id, peer_id)
            send_message(peer_id, f"📊 Статистика {get_user_display_link(target_id, peer_id)}:\nСообщений: {msgs}\nПредупреждений: {warns}\nВыговоров: {vigors}\nРоль: {role} (уровень {level})")
        
        # ---------- /reg ----------
        elif cmd == '/reg':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                target_id = from_id
            join_time = get_join_time(target_id, peer_id)
            if join_time:
                send_message(peer_id, f"📅 {get_user_display_link(target_id, peer_id)} присоединился к беседе: {join_time}")
            else:
                send_message(peer_id, f"❌ Данные о времени входа для {get_user_display_link(target_id, peer_id)} не найдены.")
        
        # ---------- /reglist ----------
        elif cmd == '/reglist':
            if user_lvl < 2:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 2+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, join_time FROM user_activity WHERE peer_id=? AND join_time IS NOT NULL ORDER BY join_time DESC LIMIT 30", (peer_id,))
            rows = c.fetchall()
            conn.close()
            if rows:
                msg = "📜 **Даты входа в беседу (последние 30):**\n"
                for uid, join_time in rows:
                    msg += f"• {get_user_display_name(uid, peer_id)} (id{uid}) — {join_time}\n"
                send_message(peer_id, msg)
            else:
                send_message(peer_id, "Нет данных о входе пользователей.")
        
        # ---------- /вызов ----------
        elif cmd == '/вызов':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id FROM chat_members WHERE peer_id=? AND level>=2", (peer_id,))
            admins = c.fetchall()
            conn.close()
            if admins:
                mentions = " @all " + " ".join([get_user_display_link(uid, peer_id) for (uid,) in admins])
                send_message(peer_id, f"🚨 ВЫЗОВ АДМИНИСТРАЦИИ! {mentions}\nПричина: {args if args else 'Не указана'}")
            else:
                send_message(peer_id, "Нет администраторов для вызова.")
        
        # ---------- /silence ----------
        elif cmd == '/silence':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            current = cfg['silence']
            new_val = not current
            set_chat_config(peer_id, 'silence', new_val)
            send_message(peer_id, f"🔇 Режим тишины {'включён' if new_val else 'выключен'}.")
        
        # ---------- /vig ----------
        elif cmd == '/vig' or cmd == '/last':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            target_id, new_args = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            if get_user_level(target_id, peer_id) >= user_lvl and target_id != OWNER_ID:
                send_message(peer_id, "❌ Нельзя выдать выговор пользователю с равным или высшим уровнем.")
                continue
            reason = new_args if new_args else "Не указана"
            add_vigor(target_id, peer_id, from_id, reason)
            vigs = get_vigors(target_id, peer_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"⚠️ {display_link} | Выговор. Всего: {vigs} | Причина: {reason}")
            log_action(from_id, f"vig_{target_id}", target_id)
            kick_on_three_vigors(target_id, peer_id, from_id, reason)
        
        # ---------- /unvig ----------
        elif cmd == '/unvig':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            remove_vigor(target_id, peer_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"✅ {display_link} | Снят последний выговор. Осталось: {get_vigors(target_id, peer_id)}")
        
        # ---------- /getvig ----------
        elif cmd == '/getvig':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                target_id = from_id
            cnt = get_vigors(target_id, peer_id)
            logs = get_vigors_log(target_id, peer_id)
            if logs:
                msg = f"⚠️ **Выговоры {get_user_display_link(target_id, peer_id)}** - всего {cnt}:\n"
                for admin_id, reason, ts in logs:
                    admin_name = get_user_display_name(admin_id, peer_id)
                    msg += f"• {ts[:19]}: {reason} (выдал: {admin_name})\n"
                send_message(peer_id, msg)
            else:
                send_message(peer_id, f"⚠️ У {get_user_display_link(target_id, peer_id)} нет выговоров.")
        
        # ---------- /viglist ----------
        elif cmd == '/viglist':
            if user_lvl < 8:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 8+)")
                continue
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, vigors FROM chat_members WHERE peer_id=? AND vigors>0 ORDER BY vigors DESC", (peer_id,))
            rows = c.fetchall()
            conn.close()
            if rows:
                msg = "⚠️ Список выговоров (количество):\n" + "\n".join([f"• {get_user_display_link(uid, peer_id)} - {v}" for uid, v in rows[:10]])
                send_message(peer_id, msg)
            else:
                send_message(peer_id, "Нет пользователей с выговорами.")
        
        # ---------- /stitle ----------
        elif cmd == '/stitle':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            if not args:
                send_message(peer_id, "❌ /stitle [название]")
                continue
            try:
                vk.messages.editChat(chat_id=peer_id-2000000000, title=args[:100])
                send_message(peer_id, f"✅ Название изменено: {args}")
            except Exception as e:
                send_message(peer_id, f"❌ Ошибка: {e}")
        
        # ---------- /rtitle ----------
        elif cmd == '/rtitle':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            try:
                vk.messages.editChat(chat_id=peer_id-2000000000, title="Беседа")
                send_message(peer_id, "✅ Название сброшено.")
            except Exception as e:
                send_message(peer_id, f"❌ Ошибка: {e}")
        
        # ---------- /filter ----------
        elif cmd == '/filter':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            if not args:
                send_message(peer_id, "❌ /filter [слово]")
                continue
            add_filter(args)
            send_message(peer_id, f"✅ Слово '{args}' добавлено в фильтр.")
        
        # ---------- /rfilter ----------
        elif cmd == '/rfilter':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            if not args:
                send_message(peer_id, "❌ /rfilter [слово]")
                continue
            remove_filter(args)
            send_message(peer_id, f"✅ Слово '{args}' удалено из фильтра.")
        
        # ---------- /flist ----------
        elif cmd == '/flist':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            words = get_filters()
            if words:
                send_message(peer_id, "📋 Фильтры: " + ", ".join(words))
            else:
                send_message(peer_id, "Фильтры не установлены.")
        
        # ---------- /logs ----------
        elif cmd == '/logs':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            if args:
                target_id = get_user_by_mention(args)
                if not target_id:
                    send_message(peer_id, "❌ Не удалось определить пользователя.")
                    continue
                logs = get_logs(target_id, 20)
                if logs:
                    msg = f"📜 Логи для {get_user_display_link(target_id, peer_id)}:\n" + "\n".join([f"{ts}: {action}" for _, action, _, ts in logs])
                    send_message(peer_id, msg[:4000])
                else:
                    send_message(peer_id, "Логов не найдено.")
            else:
                logs = get_logs(None, 20)
                if logs:
                    msg = "📜 Последние логи:\n" + "\n".join([f"{ts}: {action} (админ {get_user_display_name(aid, peer_id)})" for aid, action, _, ts in logs])
                    send_message(peer_id, msg[:4000])
                else:
                    send_message(peer_id, "Логов не найдено.")
        
        # ---------- /mention ----------
        elif cmd == '/mention':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            forbid_mention(target_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"🔇 Упоминание {display_link} запрещено.")
        
        # ---------- /unmention ----------
        elif cmd == '/unmention':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            target_id, _ = get_target_from_event(event, args, peer_id)
            if not target_id:
                send_message(peer_id, "❌ Укажите пользователя (упоминанием или ответом).")
                continue
            allow_mention(target_id)
            display_link = get_user_display_link(target_id, peer_id)
            send_message(peer_id, f"✅ Упоминание {display_link} разрешено.")
        
        # ---------- /rkick ----------
        elif cmd == '/rkick':
            if user_lvl < 11:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 11+)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            now = datetime.now()
            limit = now - timedelta(hours=24)
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id FROM user_activity WHERE peer_id=? AND join_time > ?", (peer_id, limit.isoformat()))
            rows = c.fetchall()
            conn.close()
            kicked = 0
            for (uid,) in rows:
                if get_user_level(uid, peer_id) < 2 and uid != OWNER_ID:
                    if kick_from_chat(peer_id, uid):
                        kicked += 1
            send_message(peer_id, f"🚪 Кикнуто {kicked} пользователей, присоединившихся за последние 24 часа.")
        
        # ---------- /inactive ----------
        elif cmd == '/inactive':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            now = datetime.now()
            limit = now - timedelta(days=2)
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id FROM user_activity WHERE peer_id=? AND last_message_time < ?", (peer_id, limit.isoformat()))
            rows = c.fetchall()
            conn.close()
            kicked = 0
            for (uid,) in rows:
                if get_user_level(uid, peer_id) < 2 and uid != OWNER_ID:
                    if kick_from_chat(peer_id, uid):
                        kicked += 1
            send_message(peer_id, f"🚪 Кикнуто {kicked} неактивных пользователей (не писали 2 дня).")
        
        # ---------- /жив ----------
        elif cmd == '/жив':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            send_message(peer_id, "✅ Бот жив и работает!")
        
        # ---------- /settings ----------
        elif cmd == '/settings':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            if peer_id <= 2000000000:
                send_message(peer_id, "❌ Команда работает только в беседах.")
                continue
            if not args:
                msg = "⚙ **Настройки беседы:**\n\n"
                msg += f"1. Режим тишины - {'✅' if cfg['silence'] else '❌'}\n"
                msg += f"2. Антифлуд - {'✅' if cfg['antiflood'] else '❌'}\n"
                msg += f"3. Приветствие - {'✅' if cfg['welcome_message'] else '❌'}\n"
                msg += f"4. Предупреждение о флуде - {'✅' if cfg['flood_warn'] else '❌'}\n"
                msg += f"5. Запрет на отправку ссылок - {'✅' if cfg['block_links'] else '❌'}\n"
                msg += f"6. Запрет на отправку ссылок на беседу - {'✅' if cfg['block_invite_links'] else '❌'}\n"
                msg += f"7. Кик при выходе из беседы - {'✅' if cfg['kick_on_leave'] else '❌'}\n"
                msg += f"8. Кик при приглашении пользователя (Не модератор) - {'✅' if cfg['kick_on_invite'] else '❌'}\n"
                msg += f"9. Запрет на добавление сообществ в беседу - {'✅' if cfg['block_group_invite'] else '❌'}\n"
                msg += f"10. Игры - {'✅' if cfg['games_enabled'] else '❌'}\n"
                msg += f"11. Упоминание всех [Через @all] - {'✅' if cfg['mention_all'] else '❌'}\n"
                msg += f"12. Пересылка сообщений (Кроме ответа) - {'✅' if cfg['block_forward'] else '❌'}\n"
                msg += f"13. Максимальное количество символов в сообщении - {cfg['max_length'] if cfg['max_length']>0 else '∞'}\n\n"
                msg += "🔐 **Запреты беседы:**\n"
                msg += f"1. Голосовые сообщения - {'✅' if cfg['ban_voice'] else '❌'}\n"
                msg += f"2. Стикеры - {'✅' if cfg['ban_sticker'] else '❌'}\n"
                msg += f"3. Фотографии - {'✅' if cfg['ban_photo'] else '❌'}\n"
                msg += f"4. Видеозаписи - {'✅' if cfg['ban_video'] else '❌'}\n"
                msg += f"5. Граффити - {'✅' if cfg['ban_graffiti'] else '❌'}\n"
                msg += f"6. Опросы - {'✅' if cfg['ban_poll'] else '❌'}\n"
                msg += f"7. Аудиозаписи - {'✅' if cfg['ban_audio'] else '❌'}\n"
                msg += f"8. Документы - {'✅' if cfg['ban_doc'] else '❌'}\n"
                msg += f"9. Записи на стене - {'✅' if cfg['ban_wall'] else '❌'}\n\n"
                msg += "✏ **Редактирование настроек:**\n"
                msg += "/settings antiflood - включить/выключить антифлуд\n"
                msg += "/settings floodwarn - предупреждения вместо мута\n"
                msg += "/settings leave - кик при выходе\n"
                msg += "/settings kickmode - кик при приглашении\n"
                msg += "/settings link - запрет ссылок\n"
                msg += "/settings joinlink - запрет ссылок на беседу\n"
                msg += "/settings group - запрет добавления сообществ\n"
                msg += "/settings games - игры (заглушка)\n"
                msg += "/settings mention - @all\n"
                msg += "/settings forward - пересылка\n"
                msg += "/settings length [число] - макс. символов\n"
                msg += "/settings voice_m - голосовые\n"
                msg += "/settings stickers - стикеры\n"
                msg += "/settings photo - фото\n"
                msg += "/settings video - видео\n"
                msg += "/settings graffiti - граффити\n"
                msg += "/settings polls - опросы\n"
                msg += "/settings audio - аудио\n"
                msg += "/settings doc - документы\n"
                msg += "/settings wall - записи на стене"
                send_message(peer_id, msg)
            else:
                cmdset = args.split()
                key = cmdset[0].lower()
                if key == 'length':
                    if len(cmdset) < 2 or not cmdset[1].isdigit():
                        send_message(peer_id, "❌ /settings length [число]")
                        continue
                    val = int(cmdset[1])
                    set_chat_config(peer_id, 'max_length', val)
                    send_message(peer_id, f"✅ Максимальная длина сообщения установлена: {val if val>0 else 'без ограничений'}")
                elif key in ('antiflood', 'floodwarn', 'leave', 'kickmode', 'link', 'joinlink', 'group', 'games', 'mention', 'forward',
                             'voice_m', 'stickers', 'photo', 'video', 'graffiti', 'polls', 'audio', 'doc', 'wall'):
                    mapping = {
                        'antiflood': 'antiflood',
                        'floodwarn': 'flood_warn',
                        'leave': 'kick_on_leave',
                        'kickmode': 'kick_on_invite',
                        'link': 'block_links',
                        'joinlink': 'block_invite_links',
                        'group': 'block_group_invite',
                        'games': 'games_enabled',
                        'mention': 'mention_all',
                        'forward': 'block_forward',
                        'voice_m': 'ban_voice',
                        'stickers': 'ban_sticker',
                        'photo': 'ban_photo',
                        'video': 'ban_video',
                        'graffiti': 'ban_graffiti',
                        'polls': 'ban_poll',
                        'audio': 'ban_audio',
                        'doc': 'ban_doc',
                        'wall': 'ban_wall'
                    }
                    db_key = mapping.get(key)
                    if db_key:
                        current = cfg[db_key]
                        new_val = not current
                        set_chat_config(peer_id, db_key, new_val)
                        send_message(peer_id, f"✅ Настройка '{key}' изменена: {'включена' if new_val else 'выключена'}")
                else:
                    send_message(peer_id, f"❌ Неизвестная настройка: {key}. Список: /settings")
        
        # ---------- /setpublic ----------
        elif cmd == '/setpublic':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            if not args:
                send_message(peer_id, "❌ /setpublic [ссылка или id]")
                continue
            public_id = extract_public_id_from_arg(args)
            if public_id is None:
                send_message(peer_id, "❌ Не удалось определить ID паблика по ссылке или имени.")
                continue
            set_chat_config(peer_id, 'public_to_check', public_id)
            send_message(peer_id, f"✅ Паблик ID {public_id} установлен для проверки.")
        
        # ---------- /removepublic ----------
        elif cmd == '/removepublic':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            set_chat_config(peer_id, 'public_to_check', 0)
            send_message(peer_id, "✅ Паблик для проверки удалён.")
        
        # ---------- /checkpublic ----------
        elif cmd == '/checkpublic':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            public_id = cfg.get('public_to_check', 0)
            if not public_id:
                send_message(peer_id, "❌ Паблик не установлен. Используйте /setpublic")
                continue
            try:
                is_member = vk.groups.isMember(group_id=public_id, user_id=from_id)
                if not is_member:
                    if kick_from_chat(peer_id, from_id):
                        send_message(peer_id, f"❌ Вы не подписаны на https://vk.com/public{public_id} и были кикнуты.")
                    else:
                        send_message(peer_id, f"❌ Вы не подписаны на паблик, но бот не может вас кикнуть (нет прав).")
                else:
                    send_message(peer_id, "✅ Вы подписаны на паблик.")
            except Exception as e:
                error_code = getattr(e, 'error', {}).get('error_code', 0)
                if error_code == 203 or 'access to the group members is denied' in str(e):
                    send_message(peer_id, "❌ Не удалось проверить подписку: у паблика скрыт список подписчиков. Проверьте настройки сообщества.")
                else:
                    send_message(peer_id, f"❌ Ошибка проверки: {e}")
        
        # ---------- /mtop ----------
        elif cmd == '/mtop':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            n = 10
            if args and args.isdigit():
                n = min(int(args), 20)
            conn = sqlite3.connect('bot_database.db')
            c = conn.cursor()
            c.execute("SELECT user_id, messages_count FROM chat_members WHERE peer_id=? ORDER BY messages_count DESC LIMIT ?", (peer_id, n))
            rows = c.fetchall()
            conn.close()
            if rows:
                msg = "🏆 Топ по сообщениям:\n" + "\n".join([f"{i}. {get_user_display_name(uid, peer_id)} (id{uid}) - {cnt}" for i, (uid, cnt) in enumerate(rows, 1)])
                send_message(peer_id, msg)
            else:
                send_message(peer_id, "Нет данных.")
        
        # ---------- /editowner ----------
        elif cmd == '/editowner':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            if not args:
                send_message(peer_id, "❌ /editowner [user]")
                continue
            new_owner_id = get_user_by_mention(args)
            if not new_owner_id:
                send_message(peer_id, "❌ Не удалось определить пользователя.")
                continue
            set_user_role(new_owner_id, peer_id, 13, "Владелец")
            send_message(peer_id, f"👑 Права владельца переданы {get_user_display_link(new_owner_id, peer_id)}. Перезапустите бота.")
            log_action(from_id, f"editowner_{new_owner_id}", new_owner_id)
        
        # ---------- /editcmd ----------
        elif cmd == '/editcmd':
            if user_lvl < 13:
                send_message(peer_id, "❌ Недостаточно прав (нужен уровень 13)")
                continue
            send_message(peer_id, "⚙ Функция изменения прав команд в разработке.")
        
        else:
            send_message(peer_id, f"Неизвестная команда: {cmd}. Введите /help")