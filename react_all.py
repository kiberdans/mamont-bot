import asyncio
import json
from urllib.parse import quote

import aiohttp

with open("config.json", encoding="utf-8") as f:
    cfg = json.load(f)

TOKEN = cfg["token"]
CH = int(cfg["target_channel_id"])
EMOJI = quote(cfg["mammoth_unicode"], safe="")
BASE = "https://discord.com/api/v10"


async def add_reaction(session, mid):
    url = f"{BASE}/channels/{CH}/messages/{mid}/reactions/{EMOJI}/@me"
    while True:
        async with session.put(url) as r:
            if r.status == 429:
                data = await r.json()
                await asyncio.sleep(float(data.get("retry_after", 1)) + 0.2)
                continue
            if r.status >= 400:
                body = await r.text()
                print(f"ERR {mid}: {r.status} {body[:120]}", flush=True)
            await asyncio.sleep(0.25)
            return


async def main():
    headers = {"Authorization": f"Bot {TOKEN}"}
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as s:
        before = None
        total = 0
        while True:
            params = {"limit": 100}
            if before:
                params["before"] = before
            async with s.get(f"{BASE}/channels/{CH}/messages", params=params) as r:
                if r.status == 429:
                    data = await r.json()
                    await asyncio.sleep(float(data.get("retry_after", 1)) + 0.2)
                    continue
                msgs = await r.json()
            if not isinstance(msgs, list) or not msgs:
                break
            for m in msgs:
                await add_reaction(s, m["id"])
                total += 1
                if total % 10 == 0:
                    print(f"reacted {total}...", flush=True)
            before = msgs[-1]["id"]
    print(f"DONE: reacted on {total} messages")


asyncio.run(main())
