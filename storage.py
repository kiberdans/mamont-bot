import asyncio
import json
import logging
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)


class D1:
    def __init__(self, account_id: str, database_id: str, token: str, queue_file: str = "data/queue.jsonl"):
        self.url = (
            f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
            f"/d1/database/{database_id}/query"
        )
        self.token = token
        self.queue_file = Path(queue_file)
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()
        self._queue: list[dict] = self._load_queue()

    # ---------- низкий уровень ----------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def query(self, sql: str, params: list | None = None, retries: int = 5) -> dict:
        payload = {"sql": sql, "params": params or []}
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        session = await self._get_session()
        last_err = None
        for attempt in range(retries):
            try:
                async with session.post(self.url, json=payload, headers=headers) as resp:
                    data = await resp.json()
                if data.get("success"):
                    return data["result"][0]
                last_err = data.get("errors")
            except Exception as e:  # noqa: BLE001
                last_err = e
            await asyncio.sleep(min(2 ** attempt, 15))
        raise RuntimeError(f"D1 query failed: {last_err}")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------- запись ----------
    async def record_message(self, message_id: int, user_id: int, count: int, ts: str) -> bool:
        """Возвращает True, если сообщение учтено впервые."""
        async with self._lock:
            try:
                res = await self.query(
                    "INSERT OR IGNORE INTO messages(message_id,user_id,count,ts) VALUES(?,?,?,?)",
                    [str(message_id), str(user_id), int(count), ts],
                )
                inserted = res.get("meta", {}).get("changes", 0) > 0
                if self._queue:
                    await self._flush_locked()
                return inserted
            except Exception:  # noqa: BLE001
                logger.warning("D1 недоступен — сообщение в офлайн-очередь")
                self._enqueue({"message_id": str(message_id), "user_id": str(user_id), "count": int(count), "ts": ts})
                return False

    async def record_batch(self, records: list[tuple]) -> int:
        """records: [(message_id, user_id, count, ts), ...]. Возвращает число учтённых строк."""
        if not records:
            return 0
        total = 0
        chunk_size = 20
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            values = ",".join("(?,?,?,?)" for _ in chunk)
            params = [x for r in chunk for x in r]
            res = await self.query(
                f"INSERT OR IGNORE INTO messages(message_id,user_id,count,ts) VALUES {values}",
                params,
            )
            total += res.get("meta", {}).get("changes", 0)
        return total

    async def delete_user(self, user_id: int):
        await self.query("DELETE FROM messages WHERE user_id=?", [str(user_id)])
        await self.query("DELETE FROM users WHERE user_id=?", [str(user_id)])

    async def reset(self):
        await self.query("DELETE FROM messages; DELETE FROM users;")

    # ---------- офлайн-очередь ----------
    def _load_queue(self) -> list[dict]:
        if self.queue_file.exists():
            out = []
            for line in self.queue_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            return out
        return []

    def _enqueue(self, record: dict):
        self._queue.append(record)
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        with self.queue_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    async def _flush_locked(self):
        if not self._queue:
            return
        records = [
            (r["message_id"], r["user_id"], r["count"], r["ts"]) for r in self._queue
        ]
        try:
            await self.record_batch(records)
        except Exception:  # noqa: BLE001
            return
        self._queue.clear()
        self.queue_file.unlink(missing_ok=True)
        logger.info("Офлайн-очередь отправлена в D1")

    async def flush_queue(self):
        async with self._lock:
            await self._flush_locked()

    # ---------- чтение ----------
    async def get_user_total(self, user_id: int) -> int:
        res = await self.query("SELECT total FROM users WHERE user_id=?", [str(user_id)])
        rows = res.get("results", [])
        return int(rows[0]["total"]) if rows else 0

    async def get_user(self, user_id: int) -> dict | None:
        res = await self.query("SELECT * FROM users WHERE user_id=?", [str(user_id)])
        rows = res.get("results", [])
        return rows[0] if rows else None

    async def get_top(self, limit: int = 10) -> list[dict]:
        res = await self.query(
            "SELECT user_id,total,last_seen FROM users WHERE total>0 ORDER BY total DESC LIMIT ?",
            [int(limit)],
        )
        return res.get("results", [])

    async def get_totals(self) -> dict[int, int]:
        res = await self.query("SELECT user_id,total FROM users WHERE total>0")
        return {int(r["user_id"]): int(r["total"]) for r in res.get("results", [])}

    async def get_all_users(self) -> list[dict]:
        res = await self.query("SELECT user_id,total,last_seen FROM users")
        return res.get("results", [])

    async def get_stats(self) -> dict:
        res = await self.query(
            "SELECT COALESCE(SUM(total),0) AS total, COUNT(*) AS users FROM users WHERE total>0"
        )
        rows = res.get("results", [])
        return rows[0] if rows else {"total": 0, "users": 0}

    # ---------- meta ----------
    async def get_meta(self, key: str) -> str | None:
        res = await self.query("SELECT value FROM meta WHERE key=?", [key])
        rows = res.get("results", [])
        return rows[0]["value"] if rows else None

    async def set_meta(self, key: str, value: str):
        await self.query(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [key, str(value)],
        )
