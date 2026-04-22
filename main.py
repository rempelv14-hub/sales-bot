# =============================================================
# ULTRA SALES BOT — FULL VERSION (NO CUT)
# =============================================================

import asyncio
import time
import logging
import random
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta

import aiosqlite
from telethon import TelegramClient, events
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import InputPeerChannel
from google import genai

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ================= CONFIG =================
API_ID = 22666943
API_HASH = "8acfd4df4ae031e1bfaf12abc4ac331f"
ADMIN = "Coldry1"
GEMINI_KEY = "YOUR_KEY_HERE"

client = TelegramClient("session", API_ID, API_HASH)
genai_client = genai.Client(api_key=GEMINI_KEY)

# ================= DB =================
DB = "leads.db"

async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            status TEXT,
            updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        await db.commit()

# ================= AI =================
SYSTEM_PROMPT = """
Ты живой продажник. Не бот.

Пиши:
- коротко
- дерзко
- как человек
- 1-3 предложения

Цель: довести до оплаты

Примеры:
"Сколько сейчас клиентов теряешь?"
"Вот в этом проблема"
"Это можно закрыть быстро"
"""

async def ai_reply(text):
    try:
        response = await asyncio.to_thread(
            genai_client.models.generate_content,
            model="gemini-1.5-flash",
            contents=SYSTEM_PROMPT + "\n" + text
        )
        reply = response.text.strip()

        # анти-бот стиль
        if len(reply) > 120:
            reply = reply.split(".")[0] + "."

        if random.random() < 0.3:
            reply += random.choice([
                "\n\nСколько клиентов сейчас?",
                "\n\nХочешь покажу как?",
                "\n\nЭто решается быстро"
            ])

        return reply
    except Exception as e:
        log.error(e)
        return "Скажи проще — тебе клиенты нужны?"

# ================= АНТИСПАМ =================
class AntiSpam:
    def __init__(self):
        self.data = defaultdict(deque)

    def ok(self, user):
        now = time.time()
        dq = self.data[user]
        while dq and now - dq[0] > 60:
            dq.popleft()
        if len(dq) < 5:
            dq.append(now)
            return True
        return False

spam = AntiSpam()

# ================= ХОТ СЛОВА =================
WORDS = [
    "клиенты", "заявки", "продажи", "нет клиентов",
    "мало клиентов", "ищу клиентов"
]

def score(text):
    return sum(2 for w in WORDS if w in text.lower())

# ================= АВТО ПОИСК ЧАТОВ =================
async def find_groups():
    result = await client(SearchRequest(
        q="бизнес",
        limit=20
    ))

    groups = []
    for chat in result.chats:
        if hasattr(chat, "megagroup"):
            groups.append(chat)

    return groups

# ================= ПУШИ =================
async def push(user):
    delays = [600, 3600, 21600]
    msgs = [
        "Ты пропал. Вопрос актуален?",
        "Ты сейчас теряешь клиентов каждый день",
        "Я закрываю слот сегодня"
    ]

    for d, m in zip(delays, msgs):
        await asyncio.sleep(d)
        try:
            await client.send_message(user, m)
        except:
            pass

# ================= GROUP HANDLER =================
@client.on(events.NewMessage)
async def handler(event):
    if not event.is_group:
        return

    text = event.raw_text
    if not text:
        return

    sender = await event.get_sender()
    if sender.bot:
        return

    sc = score(text)
    if sc < 2:
        return

    if not spam.ok(sender.id):
        return

    name = sender.first_name or ""

    hook = f"{name}, ты сейчас теряешь клиентов. Написал в личку 👇"
    await event.reply(hook)

    try:
        await client.send_message(sender.id,
            f"Привет, {name}\n\n"
            "Вижу проблему. Это можно закрыть ботом.\n\n"
            "Сколько клиентов сейчас в месяц?"
        )
    except:
        pass

    asyncio.create_task(push(sender.id))

# ================= DM HANDLER =================
@client.on(events.NewMessage(func=lambda e: e.is_private))
async def dm(event):
    sender = await event.get_sender()

    if sender.username == ADMIN:
        return

    text = event.raw_text

    reply = await ai_reply(text)
    await event.reply(reply)

# ================= MAIN =================
async def main():
    await init_db()
    await client.start()
    log.info("BOT STARTED")

    # авто поиск чатов (пассивный)
    groups = await find_groups()
    log.info(f"Найдено групп: {len(groups)}")

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
