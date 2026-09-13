import asyncio
import json
from urllib.parse import quote

import aiohttp

with open("config.json", encoding="utf-8") as f:
    cfg = json.load(f)

TOKEN = cfg["token"]
CH = int(cfg["target_channel_id"])
ALLOWED = {e.replace("\ufe0f", "") for e in cfg.get("allowed_reactions", [cfg["mammoth_unicode"]])}
BASE = "https://discord.com/api/v10"


def norm(e):
    return str(e).replace("\ufe0f", "")


async def main():
    headers = {"Authorization": f"Bot {TOKEN}"}
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as s:
        before = None
        checked = 0
        removed = 0
        errors = 0
        while True:
            params = {"limit": 100}
            if before:
                params["before"] = before
            async with s.get(f"{BASE}/channels/{CH}/messages", params=params) as r:
                if r.status == 429:
                    d = await r.json()
                    await asyncio.sleep(float(d.get("retry_after", 1)) + 0.2)
                    continue
                msgs = await r.json()
            if not isinstance(msgs, list) or not msgs:
                break
            for m in msgs:
                checked += 1
                for rx in m.get("reactions", []):
                    emoji = rx["emoji"]
                    if emoji.get("id"):
                        ident = f"{emoji['name']}:{emoji['id']}"
                    else:
                        ident = emoji["name"]
                    if norm(emoji.get("name")) in ALLOWED or norm(ident) in ALLOWED:
                        continue
                    url = f"{BASE}/channels/{CH}/messages/{m['id']}/reactions/{quote(ident, safe='')}"
                    while True:
                        async with s.delete(url) as r2:
                            if r2.status == 429:
                                d = await r2.json()
                                await asyncio.sleep(float(d.get("retry_after", 1)) + 0.2)
                                continue
                            if r2.status >= 400:
                                errors += 1
                                txt = await r2.text()
                                print(f"ERR {m['id']} {ident} {r2.status} {txt[:100]}", flush=True)
                            else:
                                removed += 1
                            break
                    await asyncio.sleep(0.25)
            before = msgs[-1]["id"]
            print(f"checked {checked}, removed {removed}", flush=True)
    print(f"DONE checked={checked} removed={removed} errors={errors}")


asyncio.run(main())
