"""
=============================================================
  SALES BOT — Полная версия с воронкой продаж
=============================================================
Оплата: Центр Кредит по номеру телефона
Воронка: группа → квалификация → прайс → оплата
CRM: /leads /stats /paid /ban /unban /report /broadcast
=============================================================
"""

import asyncio
import time
import logging
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta

import aiosqlite
from telethon import TelegramClient, events
import google.generativeai as genai

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  НАСТРОЙКИ — всё в одном месте
# ─────────────────────────────────────────────
API_ID     = 22666943
API_HASH   = "8acfd4df4ae031e1bfaf12abc4ac331f"
ADMIN      = "Coldry1"
GEMINI_KEY = "AIzaSyApF6auxaX5_ZGHMpufjIj41nQ1_BSQ9pI"

# ─────────────────────────────────────────────
#  КЛИЕНТЫ
# ─────────────────────────────────────────────
client = TelegramClient("session", API_ID, API_HASH)
genai.configure(api_key=GEMINI_KEY)
gemini_model = genai.GenerativeModel(
    model_name="gemini-1.5-pro",
    generation_config=genai.GenerationConfig(temperature=0.7),
)

# ─────────────────────────────────────────────
#  ОПЛАТА — ЦЕНТР КРЕДИТ
# ─────────────────────────────────────────────
PAYMENT_PHONE  = "+77071580492"
PAYMENT_BANK   = "Центр Кредит (CenterCredit Bank)"
PAYMENT_HOLDER = "Виктор Ремпель"

PACKAGE_BASE = 40_000
PACKAGE_PRO  = 80_000

PAYMENT_TEXT = f"""
Смотри 👇

Сейчас в работе 2 проекта — осталось 1 свободное место.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1 ТАРИФ — БАЗА
Цена: {PACKAGE_BASE:,} тенге

Простая онлайн-запись клиентов через бота.
Идеально для мастеров, клиник, студий и сервисов
где важно чтобы клиент записался быстро — без звонков и ожидания.

Что входит:
  Бот принимает запись 24/7 без участия человека
  Клиент выбирает удобное время из свободных слотов
  Автоматическое подтверждение и напоминание за час до записи
  Защита от накруток и случайных записей
  Простая админ-панель — видишь все записи в одном месте
  Подключение к Telegram-каналу или группе
  Поддержка 2 недели после запуска

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

2 ТАРИФ — ПРО
Цена: {PACKAGE_PRO:,} тенге

Полноценная система продаж и работы с клиентами.
Для бизнеса который хочет не просто принимать заявки —
а автоматически продавать, удерживать и возвращать клиентов.

Что входит:
  Всё из тарифа БАЗА +
  Воронка продаж внутри бота — бот сам ведёт клиента до оплаты
  Подключение оплаты через Kaspi (для ИП) — клиент платит прямо в боте
  Защита от слива клиентов — бот фиксирует каждый контакт
  Интеграция с CRM — все данные клиентов в одной системе
  Авторассылки и прогрев — бот сам догревает тех кто не купил
  Аналитика — сколько пришло, сколько купило, сколько ушло
  Приоритетная поддержка 1 месяц после запуска

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Оплата: Центр Кредит (CenterCredit Bank)

Номер телефона: {PAYMENT_PHONE}
Получатель: {PAYMENT_HOLDER}

Как перевести:
  1. Открой приложение ЦентрКредит
  2. Переводы — По номеру телефона
  3. Номер: {PAYMENT_PHONE}
  4. Сумма: 40 000 или 80 000 тенге
  5. Комментарий: БОТ

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

После оплаты напиши мне в таком формате:

ОПЛАТИЛ
ТАРИФ: (БАЗА или ПРО)
ТИП БОТА: (что хочешь автоматизировать)
НИША: (чем занимаешься)

Начну работу в течение 24 часов
""".strip()

def parse_payment(text):
    t = text.lower()
    return "оплатил" in t and ("тариф:" in t or "пакет:" in t) and "ниша:" in t

def parse_payment_incomplete(text):
    return "оплатил" in text.lower() and not parse_payment(text)

def extract_payment_fields(text):
    tariff = bot_type = niche = ""
    for line in text.splitlines():
        l = line.lower().strip()
        if l.startswith("тариф:") or l.startswith("пакет:"):
            tariff   = line.split(":", 1)[-1].strip()
        elif l.startswith("тип бота:"):
            bot_type = line.split(":", 1)[-1].strip()
        elif l.startswith("ниша:"):
            niche    = line.split(":", 1)[-1].strip()
    return tariff, bot_type, niche

# ─────────────────────────────────────────────
#  БД
# ─────────────────────────────────────────────
DB_PATH = "leads.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                first_name TEXT,
                niche TEXT,
                chat TEXT,
                status TEXT DEFAULT 'new',
                score INTEGER DEFAULT 0,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                role TEXT,
                content TEXT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                tariff TEXT,
                bot_type TEXT,
                niche TEXT,
                raw TEXT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS blacklist (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await db.commit()
    log.info("БД инициализирована")

async def upsert_lead(user_id, username, first_name, chat, score, status="new"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO leads (user_id, username, first_name, chat, score, status)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                score = MAX(leads.score, excluded.score),
                updated = CURRENT_TIMESTAMP
        """, (user_id, username, first_name, chat, score, status))
        await db.commit()

async def set_lead_status(user_id, status, niche=None):
    async with aiosqlite.connect(DB_PATH) as db:
        if niche:
            await db.execute(
                "UPDATE leads SET status=?, niche=?, updated=CURRENT_TIMESTAMP WHERE user_id=?",
                (status, niche, user_id)
            )
        else:
            await db.execute(
                "UPDATE leads SET status=?, updated=CURRENT_TIMESTAMP WHERE user_id=?",
                (status, user_id)
            )
        await db.commit()

async def get_lead_status(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT status FROM leads WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

async def get_leads_list(limit=10):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT username, first_name, niche, status, score, date FROM leads ORDER BY date DESC LIMIT ?",
            (limit,)
        ) as cur:
            return await cur.fetchall()

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        result = {}
        for s in ["new", "qualifying", "qualified", "price_sent", "paid"]:
            async with db.execute("SELECT COUNT(*) FROM leads WHERE status=?", (s,)) as cur:
                row = await cur.fetchone()
                result[s] = row[0] if row else 0
        async with db.execute("SELECT COUNT(*) FROM payments") as cur:
            row = await cur.fetchone()
            result["payments"] = row[0] if row else 0
        async with db.execute("SELECT COUNT(*) FROM leads WHERE date >= datetime('now', '-1 day')") as cur:
            row = await cur.fetchone()
            result["today"] = row[0] if row else 0
    return result

async def save_payment(user_id, username, tariff, bot_type, niche, raw):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO payments (user_id, username, tariff, bot_type, niche, raw) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, username, tariff, bot_type, niche, raw)
        )
        await db.commit()

async def save_message(user_id, role, content):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO messages (user_id, role, content) VALUES (?, ?, ?)",
            (user_id, role, content)
        )
        await db.commit()

async def get_history(user_id, limit=8):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT role, content FROM messages WHERE user_id=? ORDER BY date DESC LIMIT ?",
            (user_id, limit)
        ) as cur:
            rows = await cur.fetchall()
    return [{"role": r[0], "content": r[1]} for r in reversed(rows)]

async def ban_user(user_id, reason=""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO blacklist (user_id, reason) VALUES (?, ?)", (user_id, reason))
        await db.commit()

async def unban_user(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blacklist WHERE user_id=?", (user_id,))
        await db.commit()

async def is_banned(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM blacklist WHERE user_id=?", (user_id,)) as cur:
            return await cur.fetchone() is not None

# ─────────────────────────────────────────────
#  АНТИСПАМ
# ─────────────────────────────────────────────
class AntiSpam:
    def __init__(self, max_actions=5, window=60):
        self.max_actions = max_actions
        self.window = window
        self._buckets = defaultdict(deque)

    def can_send(self, user_id):
        now = time.time()
        dq = self._buckets[user_id]
        while dq and now - dq[0] > self.window:
            dq.popleft()
        if len(dq) < self.max_actions:
            dq.append(now)
            return True
        return False

spam = AntiSpam()
replied_users = {}

def can_reply(user_id, cooldown=7200):
    now = time.time()
    if now - replied_users.get(user_id, 0) > cooldown:
        replied_users[user_id] = now
        return True
    return False

# ─────────────────────────────────────────────
#  ДОЖИМЫ
# ─────────────────────────────────────────────
push_tasks = {}
ALMATY_TZ = timezone(timedelta(hours=5))

def is_working_hour():
    return 9 <= datetime.now(ALMATY_TZ).hour < 22

async def try_send_dm(user_id, text):
    try:
        await client.send_message(user_id, text)
        return True
    except Exception as e:
        log.warning(f"ЛС не отправлено {user_id}: {e}")
        return False

async def push_sequence(user_id):
    pushes = [
        (15 * 60,   "Кстати, у меня сейчас последнее свободное место. Много желающих."),
        (2 * 3600,  "Хочу уточнить — ты рассматриваешь запуск бота в этом месяце?"),
        (24 * 3600, "Пока ты думаешь — конкуренты уже автоматизируют продажи. Потом догонять дороже."),
        (3 * 86400, "Последний раз пишу — если интересно, просто ответь да и продолжим."),
    ]
    for delay, msg in pushes:
        await asyncio.sleep(delay)
        while not is_working_hour():
            await asyncio.sleep(600)
        await try_send_dm(user_id, msg)

def schedule_pushes(user_id):
    if user_id in push_tasks:
        push_tasks[user_id].cancel()
    push_tasks[user_id] = asyncio.create_task(push_sequence(user_id))

def cancel_pushes(user_id):
    if user_id in push_tasks:
        push_tasks[user_id].cancel()
        push_tasks.pop(user_id, None)

# ─────────────────────────────────────────────
#  HOT WORDS
# ─────────────────────────────────────────────
HOT_WORDS = [
    "нет клиентов", "мало клиентов", "нужны клиенты", "ищу клиентов",
    "нет заявок", "мало заявок", "нет продаж", "мало продаж",
    "сколько стоит", "цена", "прайс",
    "как привлечь", "как найти клиентов", "где брать клиентов",
    "помогите с клиентами", "хочу больше клиентов",
    "нет лидов", "мало лидов",
]

def score_message(text):
    t = text.lower()
    return sum(3 for w in HOT_WORDS if w in t)

# ─────────────────────────────────────────────
#  AI FILTER
# ─────────────────────────────────────────────
async def ai_filter(text):
    try:
        prompt = (
            "Определи — хочет ли человек больше клиентов, заявок или продаж. "
            "Ответь ТОЛЬКО одной цифрой: 0 — нет, 1 — возможно, 2 — явно хочет.\n\n"
            f"Сообщение: {text}"
        )
        response = await asyncio.to_thread(gemini_model.generate_content, prompt)
        raw = response.text.strip()
        return max(0, min(2, int(raw[0])))
    except Exception as e:
        log.warning(f"ai_filter ошибка: {e}")
        return 0

# ─────────────────────────────────────────────
#  ВОЗРАЖЕНИЯ
# ─────────────────────────────────────────────
OBJECTIONS = {
    "дорого":    "Человек считает цену высокой. Покажи что ежемесячные потери без бота больше. Приведи пример под его нишу. Предложи начать с БАЗЫ.",
    "подумаю":   "Человек говорит подумаю. Создай мягкий дефицит — скажи что осталось одно место. Спроси: что мешает решить прямо сейчас?",
    "не сейчас": "Человек говорит не сейчас. Спроси когда планирует. Предложи зарезервировать место бесплатно. Намекни что цена может вырасти.",
    "позже":     "Человек хочет позже. Создай срочность — место закрывается сегодня.",
}

def detect_objection(text):
    t = text.lower()
    for phrase, instruction in OBJECTIONS.items():
        if phrase in t:
            return instruction
    return ""

# ─────────────────────────────────────────────
#  AI REPLY
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """
Ты — опытный менеджер по продажам чат-ботов для бизнеса в Казахстане.
Твоя цель: довести человека до оплаты пакета БАЗА (40 000 тг) или ПРО (80 000 тг).

Правила:
- Пиши коротко: 2-4 предложения
- Говори на языке собеседника (русский или казахский)
- Показывай конкретную боль — сколько клиентов/денег человек теряет прямо сейчас
- Используй цифры и примеры под конкретную нишу
- Не давай лишних технических деталей — только ценность и результат
- Каждый ответ должен продвигать к следующему шагу воронки
- Не используй Markdown-разметку
"""

async def ai_reply(user_id, text, extra=""):
    await save_message(user_id, "user", text)
    history = await get_history(user_id)

    system = SYSTEM_PROMPT
    objection_hint = detect_objection(text)
    if objection_hint:
        system += f"\n\nВАЖНО: {objection_hint}"
    if extra:
        system += f"\n\nДОПОЛНИТЕЛЬНО: {extra}"

    try:
        history_text = ""
        for msg in history:
            role_label = "Менеджер" if msg["role"] == "assistant" else "Клиент"
            history_text += f"{role_label}: {msg['content']}\n"

        full_prompt = f"{system}\n\nИстория диалога:\n{history_text}\nКлиент: {text}\nМенеджер:"
        response = await asyncio.to_thread(gemini_model.generate_content, full_prompt)
        reply = response.text.strip()
        await save_message(user_id, "assistant", reply)
        return reply
    except Exception as e:
        log.error(f"ai_reply ошибка: {e}")
        return "Напиши мне — расскажу подробнее."

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def safe_username(sender):
    return f"@{sender.username}" if getattr(sender, "username", None) else f"id:{sender.id}"

def safe_first_name(sender):
    return getattr(sender, "first_name", None) or "Привет"

# ─────────────────────────────────────────────
#  ОБРАБОТЧИК ГРУПП
# ─────────────────────────────────────────────
@client.on(events.NewMessage)
async def group_handler(event):
    if not event.is_group:
        return
    text = event.raw_text
    if not text or len(text.strip()) < 5:
        return
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return

    user_id    = sender.id
    uname      = safe_username(sender)
    first_name = safe_first_name(sender)

    if await is_banned(user_id):
        return

    cancel_pushes(user_id)

    score    = score_message(text)
    ai_score = await ai_filter(text)
    total    = score + ai_score

    log.info(f"Группа <- {uname} | score={score} ai={ai_score} total={total}")

    if total < 3:
        return
    if not spam.can_send(user_id):
        return
    if not can_reply(user_id):
        return

    chat_title = getattr(event.chat, "title", "unknown")

    if total >= 5:
        hook = f"{first_name}, вижу проблему — это решается быстро. Пишу в личку 👇"
        await event.reply(hook)
        dm_ok = await try_send_dm(user_id, (
            f"Привет, {first_name}! Видел твоё сообщение.\n\n"
            "Чат-бот закрывает именно эту проблему — работает 24/7, "
            "обрабатывает заявки, квалифицирует клиентов.\n\n"
            "Скажи: чем занимаешься и сколько клиентов хочешь в месяц?"
        ))
        status = "qualifying" if dm_ok else "new"
    else:
        reply = await ai_reply(user_id, text)
        await event.reply(reply)
        status = "new"

    await upsert_lead(user_id, uname, first_name, chat_title, total, status)
    await client.send_message(
        ADMIN,
        f"ЛИД | total={total}\n{uname} ({first_name})\nЧат: {chat_title}\n\n{text}"
    )
    schedule_pushes(user_id)

# ─────────────────────────────────────────────
#  ОБРАБОТЧИК ЛС
# ─────────────────────────────────────────────
@client.on(events.NewMessage(func=lambda e: e.is_private))
async def dm_handler(event):
    text = event.raw_text
    if not text or len(text.strip()) < 2:
        return
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return

    user_id    = sender.id
    uname      = safe_username(sender)
    first_name = safe_first_name(sender)

    if getattr(sender, "username", "") == ADMIN:
        await handle_admin(event, text)
        return

    if await is_banned(user_id):
        return

    cancel_pushes(user_id)

    if parse_payment_incomplete(text):
        await event.reply(
            "Вижу что ты оплатил — отлично!\n\n"
            "Чтобы я мог начать работу, напиши пожалуйста в таком формате:\n\n"
            "ОПЛАТИЛ\n"
            "ТАРИФ: (БАЗА или ПРО)\n"
            "ТИП БОТА: (какой бот тебе нужен)\n"
            "НИША: (чем занимаешься)"
        )
        return

    if parse_payment(text):
        tariff, bot_type, niche = extract_payment_fields(text)
        now_str = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y %H:%M")
        await event.reply(
            f"Принял оплату! Спасибо, {first_name}\n\n"
            f"Тариф: {tariff}\nТип бота: {bot_type or 'уточним'}\nНиша: {niche}\n\n"
            "Свяжусь с тобой в течение нескольких часов и начнём работу"
        )
        await save_payment(user_id, uname, tariff, bot_type, niche, text)
        await set_lead_status(user_id, "paid", niche)
        amount = PACKAGE_PRO if "про" in tariff.lower() else PACKAGE_BASE
        await client.send_message(
            ADMIN,
            f"ОПЛАТА! {now_str}\n━━━━━━━━━━━━━━━━\n"
            f"Клиент:   {uname} ({first_name})\n"
            f"Тариф:    {tariff} — {amount:,} тг\n"
            f"Тип бота: {bot_type or 'не указан'}\nНиша: {niche}\n"
            f"━━━━━━━━━━━━━━━━\n{text}"
        )
        return

    status = await get_lead_status(user_id)

    if status == "qualifying":
        niche = text.strip()
        await set_lead_status(user_id, "qualified", niche)
        hook = await ai_reply(
            user_id, text,
            extra=(
                f"Ниша человека: {niche}. "
                "Скажи 1-2 предложения как конкретно бот поможет в этой нише. "
                "Затем скажи что отправляешь детали и стоимость."
            )
        )
        await event.reply(hook)
        await asyncio.sleep(1)
        await event.reply(PAYMENT_TEXT)
        await set_lead_status(user_id, "price_sent")
        await client.send_message(ADMIN, f"Прайс отправлен\n{uname}\nНиша: {niche}")
        schedule_pushes(user_id)
        return

    if status == "price_sent":
        t_low = text.lower()
        if "база" in t_low or "про" in t_low:
            chosen = "ПРО" if "про" in t_low else "БАЗА"
            amount = PACKAGE_PRO if chosen == "ПРО" else PACKAGE_BASE
            await event.reply(
                f"Отличный выбор — тариф {chosen}!\n\n"
                f"Переводи {amount:,} тг через Центр Кредит:\n"
                f"Номер: {PAYMENT_PHONE}\nПолучатель: {PAYMENT_HOLDER}\n\n"
                f"После оплаты напиши:\nОПЛАТИЛ\nТАРИФ: {chosen}\n"
                f"ТИП БОТА: (какой бот нужен)\nНИША: (чем занимаешься)"
            )
            return
        reply = await ai_reply(
            user_id, text,
            extra=(
                "Человек уже видел прайс. Твоя задача — закрыть сделку прямо сейчас. "
                "Ответь на его сообщение, сними возражение и в конце напомни: "
                f"оплата через Центр Кредит, номер {PAYMENT_PHONE}, получатель {PAYMENT_HOLDER}."
            )
        )
        await event.reply(reply)
        if not any(w in t_low for w in ["оплат", "перевод", "цент", "кредит"]):
            await asyncio.sleep(2)
            await event.reply(
                f"Реквизиты:\nЦентр Кредит -> По номеру телефона\n"
                f"{PAYMENT_PHONE} ({PAYMENT_HOLDER})\n\n"
                f"После оплаты напиши:\nОПЛАТИЛ / ТАРИФ / ТИП БОТА / НИША"
            )
        return

    if not status:
        await upsert_lead(user_id, uname, first_name, "direct_dm", 0, "qualifying")
        await event.reply(
            f"Привет, {first_name}!\n\n"
            "Расскажи — чем занимаешься и какая сейчас главная проблема с клиентами?"
        )
        return

    if not spam.can_send(user_id):
        return
    reply = await ai_reply(user_id, text)
    await event.reply(reply)

# ─────────────────────────────────────────────
#  ADMIN КОМАНДЫ
# ─────────────────────────────────────────────
STATUS_EMOJI = {
    "new": "Новый", "qualifying": "Квалификация",
    "qualified": "Квалифицирован", "price_sent": "Прайс отправлен", "paid": "Оплатил"
}

async def handle_admin(event, text):
    parts = text.strip().split()
    cmd   = parts[0].lower()

    if cmd == "/leads":
        n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10
        rows = await get_leads_list(n)
        if not rows:
            await event.reply("Лидов пока нет.")
            return
        lines = [f"Последние {n} лидов:\n"]
        for r in rows:
            username, fname, niche, status, score, date = r
            lines.append(f"{username or fname} | {niche or '—'} | {STATUS_EMOJI.get(status, status)} | {date[:10]}")
        await event.reply("\n".join(lines))

    elif cmd == "/stats":
        s = await get_stats()
        total = sum(s.get(k, 0) for k in ["new", "qualifying", "qualified", "price_sent", "paid"])
        pct = round((s["price_sent"] + s["paid"]) / total * 100) if total else 0
        await event.reply(
            f"Статистика воронки:\n\nНовых: {s['new']}\nВ диалоге: {s['qualifying'] + s['qualified']}\n"
            f"Прайс отправлен: {s['price_sent']}\nОплатили: {s['paid']}\n"
            f"Конверсия: {pct}%\nСегодня: {s['today']}\nТранзакций: {s['payments']}"
        )

    elif cmd == "/paid" and len(parts) > 1:
        target = parts[1].lstrip("@")
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM leads WHERE username LIKE ?", (f"%{target}%",)) as cur:
                row = await cur.fetchone()
        if row:
            await set_lead_status(row[0], "paid")
            await try_send_dm(row[0], "Твоя оплата подтверждена! Начинаем работу — скоро свяжусь с тобой.")
            await event.reply(f"@{target} отмечен как оплативший.")
        else:
            await event.reply(f"@{target} не найден.")

    elif cmd == "/ban" and len(parts) > 1:
        target = parts[1].lstrip("@")
        reason = " ".join(parts[2:]) if len(parts) > 2 else ""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM leads WHERE username LIKE ?", (f"%{target}%",)) as cur:
                row = await cur.fetchone()
        if row:
            await ban_user(row[0], reason)
            await event.reply(f"@{target} заблокирован.")
        else:
            await event.reply(f"@{target} не найден.")

    elif cmd == "/unban" and len(parts) > 1:
        target = parts[1].lstrip("@")
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM leads WHERE username LIKE ?", (f"%{target}%",)) as cur:
                row = await cur.fetchone()
        if row:
            await unban_user(row[0])
            await event.reply(f"@{target} разблокирован.")
        else:
            await event.reply(f"@{target} не найден.")

    elif cmd == "/broadcast" and len(parts) > 1:
        message = text[len("/broadcast"):].strip()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM leads WHERE status != 'paid'") as cur:
                rows = await cur.fetchall()
        sent, failed = 0, 0
        for (uid,) in rows:
            if await try_send_dm(uid, message): sent += 1
            else: failed += 1
            await asyncio.sleep(2)
        await event.reply(f"Рассылка: {sent} отправлено, {failed} ошибок.")

    elif cmd == "/report":
        s = await get_stats()
        total = sum(s.get(k, 0) for k in ["new", "qualifying", "qualified", "price_sent", "paid"])
        now = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y %H:%M")
        await event.reply(
            f"Отчёт ({now}):\n\nЛидов сегодня: {s['today']}\nВсего: {total}\n"
            f"Прайс отправлен: {s['price_sent']}\nОплатили: {s['paid']}\nТранзакций: {s['payments']}"
        )

    else:
        await event.reply(
            "Команды:\n/leads [N]\n/stats\n/paid @username\n"
            "/ban @username\n/unban @username\n/broadcast текст\n/report"
        )

# ─────────────────────────────────────────────
#  ЕЖЕДНЕВНЫЙ ОТЧЁТ
# ─────────────────────────────────────────────
async def daily_report_loop():
    while True:
        now = datetime.now(ALMATY_TZ)
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        s = await get_stats()
        total = sum(s.get(k, 0) for k in ["new", "qualifying", "qualified", "price_sent", "paid"])
        await try_send_dm(
            ADMIN,
            f"Утренний отчёт {datetime.now(ALMATY_TZ).strftime('%d.%m.%Y')}:\n\n"
            f"Всего лидов: {total}\nВ диалоге: {s['qualifying'] + s['qualified']}\n"
            f"Прайс отправлен: {s['price_sent']}\nОплатили: {s['paid']}\n\nУдачного дня!"
        )

# ─────────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────────
async def main():
    await init_db()
    await client.start()
    me = await client.get_me()
    log.info(f"Бот запущен как @{me.username}")
    asyncio.create_task(daily_report_loop())
    try:
        await client.run_until_disconnected()
    finally:
        log.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
