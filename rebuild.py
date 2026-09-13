import asyncio
import json
import logging
from datetime import timezone

import discord

from storage import D1

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("rebuild")

with open("config.json", encoding="utf-8") as f:
    cfg = json.load(f)

MAMMOTH_UNICODE = cfg["mammoth_unicode"]
REQUIRED_STATUS = cfg["required_status"]
TARGET_CHANNEL_ID = int(cfg["target_channel_id"])
ALLOWED = {int(i) for i in cfg["allowed_user_ids"]}
STRICT = cfg.get("strict_mammoth_only", False)

db = D1(
    cfg["cloudflare"]["account_id"],
    cfg["cloudflare"]["database_id"],
    cfg["cloudflare"]["api_token"],
)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)


def count_mammoths(content: str) -> int:
    return (content or "").count(MAMMOTH_UNICODE)


def is_only(content: str) -> bool:
    cleaned = "".join(ch for ch in (content or "") if not ch.isspace())
    return bool(cleaned) and all(ch == MAMMOTH_UNICODE for ch in cleaned)


def is_mammoth_message(m: discord.Message) -> bool:
    if m.attachments or m.embeds or m.stickers:
        return False
    if STRICT:
        return is_only(m.content)
    return count_mammoths(m.content) > 0


def has_status(member: discord.Member) -> bool:
    if not member.activities:
        return False
    for a in member.activities:
        if a.type == discord.ActivityType.custom and a.state and REQUIRED_STATUS in a.state:
            return True
    return False


@client.event
async def on_ready():
    logger.info(f"Вошёл как {client.user}")
    ch = client.get_channel(TARGET_CHANNEL_ID)
    if not ch:
        logger.error("Целевой канал не найден")
        await client.close()
        return
    guild = ch.guild

    valid = set(ALLOWED)
    logger.info("Собираю участников со статусом...")
    async for m in guild.fetch_members(limit=None):
        if has_status(m):
            valid.add(m.id)
    logger.info(f"Валидных пользователей: {len(valid)}")

    await db.reset()
    logger.info("D1 очищена, начинаю обход истории...")

    batch = []
    counted = 0
    newest = 0
    async for m in ch.history(limit=None, oldest_first=True):
        if m.id > newest:
            newest = m.id
        if m.author.bot:
            continue
        if not is_mammoth_message(m):
            continue
        if m.author.id not in valid:
            continue
        ts = m.created_at.astimezone(timezone.utc).isoformat()
        batch.append((str(m.id), str(m.author.id), 1, ts))
        counted += 1
        if len(batch) >= 50:
            await db.record_batch(batch)
            batch.clear()
            if counted % 500 == 0:
                logger.info(f"Учтено {counted}...")

    if batch:
        await db.record_batch(batch)

    if newest:
        with open("data/last_msg.txt", "w") as f:
            f.write(str(newest))

    stats = await db.get_stats()
    top = await db.get_top(10)
    logger.info(f"ГОТОВО. Учтено сообщений: {counted}, всего мамонтов: {stats.get('total')}")
    for row in top:
        logger.info(f"  {row['user_id']}: {row['total']}")

    await db.close()
    await client.close()


client.run(cfg["token"])
