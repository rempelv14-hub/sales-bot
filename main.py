# =============================================================
# SALES BOT X10 — ФИНАЛЬНАЯ ВЕРСИЯ
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
from telethon.tl.functions.messages import GetHistoryRequest
import google.generativeai as genai

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
API_ID = 22666943
API_HASH = "8acfd4df4ae031e1bfaf12abc4ac331f"
ADMIN = "Coldry1"
GEMINI_KEY = "ТВОЙ_API_KEY"

client = TelegramClient("session", API_ID, API_HASH)
genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-1.5-pro")

# ─────────────────────────────────────────────
# LOG
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect("db.sqlite") as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS leads(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            status TEXT,
            score INTEGER
        )
        """)
        await db.commit()

# ─────────────────────────────────────────────
# HUMAN DELAY
# ─────────────────────────────────────────────
async def human_delay(text):
    await asyncio.sleep(min(len(text) * 0.03, 2))

# ─────────────────────────────────────────────
# AI REPLY
# ─────────────────────────────────────────────
SYSTEM = """
Ты живой продажник.
Пиши коротко.
Дави на боль.
Веди к оплате.
"""

async def ai_reply(text):
    prompt = SYSTEM + "\n\n" + text
    r = await asyncio.to_thread(model.generate_content, prompt)
    reply = r.text.strip()

    if len(reply) > 150:
        reply = reply[:150]

    if random.random() < 0.3:
        reply += random.choice([" 👇", " 🤝", ""])

    return reply

# ─────────────────────────────────────────────
# LEAD CLASS
# ─────────────────────────────────────────────
def classify(score):
    if score >= 6:
        return "hot"
    if score >= 4:
        return "warm"
    return "cold"

# ─────────────────────────────────────────────
# SCORE
# ─────────────────────────────────────────────
WORDS = ["клиенты", "заявки", "продажи", "цена", "прайс"]

def score(text):
    t = text.lower()
    return sum(2 for w in WORDS if w in t)

# ─────────────────────────────────────────────
# GROUP SEARCH (SAFE)
# ─────────────────────────────────────────────
KEYWORDS = ["бизнес", "маркетинг", "клиенты"]

async def activity(chat):
    try:
        h = await client(GetHistoryRequest(peer=chat, limit=20, offset_date=None,
                                           offset_id=0, max_id=0, min_id=0,
                                           add_offset=0, hash=0))
        now = datetime.now(timezone.utc)
        return len([m for m in h.messages if (now - m.date).seconds < 86400])
    except:
        return 0

async def find_groups():
    res_all = []
    for kw in KEYWORDS:
        res = await client(SearchRequest(q=kw, limit=10))
        for c in res.chats:
            if getattr(c, "megagroup", False):
                act = await activity(c)
                if act > 5:
                    res_all.append((c.title, c.username, act))
    return res_all

async def send_groups():
    while True:
        g = await find_groups()
        if g:
            txt = "🔥 ГРУППЫ:\n\n"
            for t,u,a in g[:5]:
                link = f"https://t.me/{u}" if u else "-"
                txt += f"{t}\n{link}\n{a}\n\n"
            await client.send_message(ADMIN, txt)
        await asyncio.sleep(3600)

# ─────────────────────────────────────────────
# PUSH
# ─────────────────────────────────────────────
async def push(user):
    await asyncio.sleep(600)
    await client.send_message(user, "Ты пропал или думаешь?")

# ─────────────────────────────────────────────
# GROUP HANDLER
# ─────────────────────────────────────────────
@client.on(events.NewMessage)
async def handler(e):
    if not e.is_group:
        return

    text = e.raw_text
    if not text:
        return

    s = score(text)
    lead = classify(s)

    if s < 4:
        return

    user = await e.get_sender()

    if lead == "hot":
        await e.reply(
            "У тебя заявки есть\n"
            "но ты их теряешь\n\n"
            "могу показать как закрыть"
        )
        await asyncio.sleep(2)
        await e.reply("если интересно — напиши да")

        await client.send_message(
            ADMIN,
            f"🔥 ЛИД\n{user.username}\n{text}"
        )

# ─────────────────────────────────────────────
# DM HANDLER
# ─────────────────────────────────────────────
@client.on(events.NewMessage(func=lambda e: e.is_private))
async def dm(e):
    text = e.raw_text
    user = await e.get_sender()

    if user.username == ADMIN:
        return

    reply = await ai_reply(text)
    await human_delay(reply)
    await e.reply(reply)

    asyncio.create_task(push(user.id))

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    await init_db()
    await client.start()
    log.info("STARTED")

    asyncio.create_task(send_groups())

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
