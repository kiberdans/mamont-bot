import discord
from discord import app_commands
from discord.ext import commands, tasks
import asyncio, hashlib, json, logging, os, time, traceback
from datetime import datetime, timezone
from pathlib import Path

from storage import D1

# --- Загрузка конфига ---
with open("config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

# --- Настройки (глобальные переменные) ---
ALLOWED_USER_IDS = {int(i) for i in cfg["allowed_user_ids"]}
MAMMOTH_UNICODE = cfg["mammoth_unicode"]
REQUIRED_STATUS = cfg["required_status"]
TARGET_CHANNEL_ID = int(cfg["target_channel_id"])
QUICK_LIMIT = cfg["history_quick_limit"]
DEEP_LIMIT = cfg["history_deep_limit"]
BATCH_SIZE = cfg["deep_scan_batch_size"]
BATCH_DELAY = cfg["deep_scan_batch_delay_sec"]
SCAN_QUICK_INTERVAL = cfg["scan_quick_interval_sec"]
SCAN_DEEP_INTERVAL_MIN = cfg["scan_deep_interval_min"]
STATUS_CHECK_HOURS = cfg["status_check_interval_hours"]
ROLE_THRESHOLDS = {int(k): v for k, v in cfg["mammoth_role_thresholds"].items()}
STRICT_MAMMOTH_ONLY = cfg.get("strict_mammoth_only", False)
ALLOWED_REACTIONS = list(cfg.get("allowed_reactions", [MAMMOTH_UNICODE]))


def normalize_emoji(e) -> str:
    return str(e).replace("\ufe0f", "")


ALLOWED_REACTIONS_NORM = {normalize_emoji(e) for e in ALLOWED_REACTIONS}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

START_TIME = time.time()

db = D1(
    cfg["cloudflare"]["account_id"],
    cfg["cloudflare"]["database_id"],
    cfg["cloudflare"]["api_token"],
)

LAST_MSG_FILE = DATA_DIR / "last_msg.txt"
last_processed_message_id = 0
_last_msg_dirty = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)


# ==================== Утилиты ====================
def save_config():
    cfg["allowed_user_ids"] = list(ALLOWED_USER_IDS)
    cfg["required_status"] = REQUIRED_STATUS
    cfg["target_channel_id"] = TARGET_CHANNEL_ID
    cfg["history_quick_limit"] = QUICK_LIMIT
    cfg["history_deep_limit"] = DEEP_LIMIT
    cfg["deep_scan_batch_size"] = BATCH_SIZE
    cfg["deep_scan_batch_delay_sec"] = BATCH_DELAY
    cfg["scan_quick_interval_sec"] = SCAN_QUICK_INTERVAL
    cfg["scan_deep_interval_min"] = SCAN_DEEP_INTERVAL_MIN
    cfg["status_check_interval_hours"] = STATUS_CHECK_HOURS
    cfg["mammoth_role_thresholds"] = {str(k): v for k, v in ROLE_THRESHOLDS.items()}
    cfg["strict_mammoth_only"] = STRICT_MAMMOTH_ONLY
    with open("config.json", "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def update_last_message_id(msg_id: int):
    global last_processed_message_id, _last_msg_dirty
    if msg_id > last_processed_message_id:
        last_processed_message_id = msg_id
        _last_msg_dirty = True
        try:
            with open(LAST_MSG_FILE, "w") as f:
                f.write(str(msg_id))
        except OSError:
            pass


# ==================== Проверки ====================
def count_mammoths(msg: discord.Message) -> int:
    return (msg.content or "").count(MAMMOTH_UNICODE)


def is_only_mammoth(msg: discord.Message) -> bool:
    if msg.attachments or msg.embeds or msg.stickers:
        return False
    content = msg.content or ""
    cleaned = "".join(ch for ch in content if not ch.isspace())
    return bool(cleaned) and all(ch == MAMMOTH_UNICODE for ch in cleaned)


def is_mammoth_message(msg: discord.Message) -> bool:
    if STRICT_MAMMOTH_ONLY:
        return is_only_mammoth(msg)
    return count_mammoths(msg) > 0


def has_required_status(member: discord.Member) -> bool:
    if not member.activities:
        return False
    for a in member.activities:
        if a.type == discord.ActivityType.custom:
            if a.state and REQUIRED_STATUS in a.state:
                return True
    return False


async def try_delete(msg: discord.Message):
    try:
        await msg.delete()
    except (discord.Forbidden, discord.NotFound, asyncio.TimeoutError):
        pass


async def try_react(msg: discord.Message):
    try:
        await msg.add_reaction(MAMMOTH_UNICODE)
    except (discord.Forbidden, discord.NotFound, discord.HTTPException, asyncio.TimeoutError):
        pass


async def update_user_roles(member: discord.Member, count: int):
    if not member.guild:
        return
    sorted_thr = sorted(ROLE_THRESHOLDS.items(), key=lambda x: -x[1])
    target = next((rid for rid, th in sorted_thr if count >= th), None)
    if target:
        role = member.guild.get_role(target)
        if role and role not in member.roles:
            try:
                await member.add_roles(role, reason=f"🦣 Порог: {count}")
                logger.info(f"Выдана роль {role.name} -> {member.display_name}")
            except discord.HTTPException as e:
                logger.error(f"Ошибка выдачи роли {role.name}: {e}")
    else:
        for rid in ROLE_THRESHOLDS:
            r = member.guild.get_role(rid)
            if r and r in member.roles:
                try:
                    await member.remove_roles(r, reason=f"🦣 Ниже порога: {count}")
                except discord.HTTPException as e:
                    logger.error(f"Ошибка снятия роли {r.name}: {e}")


# ==================== Главный обработчик ====================
async def process_message(msg: discord.Message, guild: discord.Guild):
    if msg.id <= last_processed_message_id:
        return

    if msg.author.bot:
        update_last_message_id(msg.id)
        return

    if not is_mammoth_message(msg):
        await try_delete(msg)
        update_last_message_id(msg.id)
        return

    await try_react(msg)

    try:
        member = guild.get_member(msg.author.id) or await guild.fetch_member(msg.author.id)
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут получения участника {msg.author.id}")
        return
    except Exception as e:
        logger.error(f"Ошибка получения участника {msg.author.id}: {e}")
        return

    is_whitelisted = member.id in ALLOWED_USER_IDS
    if is_whitelisted or has_required_status(member):
        ts = msg.created_at.astimezone(timezone.utc).isoformat()
        inserted = await db.record_message(msg.id, member.id, 1, ts)
        if inserted:
            total = await db.get_user_total(member.id)
            await update_user_roles(member, total)
    else:
        for rid in ROLE_THRESHOLDS:
            r = guild.get_role(rid)
            if r and r in member.roles:
                try:
                    await member.remove_roles(r, reason="🦣 Статус не найден")
                except Exception:
                    pass

    update_last_message_id(msg.id)


# ==================== Сканирование истории ====================
async def run_scan(channel, limit: int, batch_mode: bool = False, ignore_last_id: bool = False):
    guild = channel.guild
    collected = []
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            async for m in channel.history(limit=limit):
                if not ignore_last_id and m.id <= last_processed_message_id:
                    break
                collected.append(m)
                if batch_mode and len(collected) >= BATCH_SIZE:
                    for msg in collected:
                        await process_message(msg, guild)
                    collected.clear()
                    await asyncio.sleep(BATCH_DELAY)
            if collected:
                for msg in collected:
                    await process_message(msg, guild)
            return
        except (asyncio.TimeoutError, discord.HTTPException, ConnectionError) as e:
            logger.warning(f"Сетевая ошибка при сканировании (попытка {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(5)
            else:
                logger.error("Сканирование прервано после нескольких попыток")
                raise


async def full_rebuild(channel) -> int:
    """Полный пересбор статистики из всей истории канала. Возвращает число учтённых сообщений."""
    guild = channel.guild
    await db.reset()

    valid_ids = set(ALLOWED_USER_IDS)
    if guild:
        try:
            async for member in guild.fetch_members(limit=None):
                if has_required_status(member):
                    valid_ids.add(member.id)
        except Exception as e:
            logger.warning(f"Не удалось получить участников: {e}")

    batch = []
    counted = 0
    newest_id = 0

    async for m in channel.history(limit=None, oldest_first=True):
        if m.id > newest_id:
            newest_id = m.id
        if m.author.bot:
            continue
        if not is_mammoth_message(m):
            continue
        if m.author.id not in valid_ids:
            continue
        ts = m.created_at.astimezone(timezone.utc).isoformat()
        batch.append((str(m.id), str(m.author.id), 1, ts))
        counted += 1
        if len(batch) >= 50:
            await db.record_batch(batch)
            batch.clear()

    if batch:
        await db.record_batch(batch)

    if newest_id:
        update_last_message_id(newest_id)

    logger.info(f"Ребилд завершён: учтено {counted} сообщений")
    return counted


# ==================== Фоновые задачи ====================
@tasks.loop(seconds=SCAN_QUICK_INTERVAL)
async def scan_quick():
    try:
        ch = bot.get_channel(TARGET_CHANNEL_ID)
        if ch:
            await run_scan(ch, limit=QUICK_LIMIT, batch_mode=False)
    except Exception:
        logger.error(f"Ошибка в scan_quick:\n{traceback.format_exc()}")


@tasks.loop(minutes=SCAN_DEEP_INTERVAL_MIN)
async def scan_deep():
    try:
        ch = bot.get_channel(TARGET_CHANNEL_ID)
        if ch:
            await run_scan(ch, limit=DEEP_LIMIT, batch_mode=True)
    except Exception:
        logger.error(f"Ошибка в scan_deep:\n{traceback.format_exc()}")


@tasks.loop(hours=STATUS_CHECK_HOURS)
async def status_verification():
    ch = bot.get_channel(TARGET_CHANNEL_ID)
    if not ch:
        return
    guild = ch.guild
    for row in await db.get_all_users():
        uid = int(row["user_id"])
        if uid in ALLOWED_USER_IDS:
            continue
        try:
            member = await guild.fetch_member(uid)
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут проверки статуса {uid}")
            continue
        except discord.NotFound:
            continue
        except Exception as e:
            logger.warning(f"Ошибка проверки статуса {uid}: {e}")
            continue

        if not has_required_status(member):
            for rid in ROLE_THRESHOLDS:
                r = guild.get_role(rid)
                if r and r in member.roles:
                    try:
                        await member.remove_roles(r, reason="🦣 Статус утерян")
                    except Exception:
                        pass
        await asyncio.sleep(0.5)


@tasks.loop(seconds=30)
async def flush_state_loop():
    global _last_msg_dirty
    if _last_msg_dirty:
        try:
            await db.set_meta("last_msg", str(last_processed_message_id))
            _last_msg_dirty = False
        except Exception as e:
            logger.warning(f"Не удалось сохранить чекпоинт: {e}")
    try:
        await db.set_meta("heartbeat", str(int(time.time())))
    except Exception:
        pass
    await db.flush_queue()


# ==================== Группа команд /mammoth ====================
mammoth_group = app_commands.Group(name="mammoth", description="Управление мамонтами")


# ---------- whitelist ----------
@mammoth_group.command(name="whitelist", description="Управление белым списком")
@app_commands.describe(action="Действие: add / remove / list", user="Пользователь (для add/remove)")
@app_commands.choices(action=[
    app_commands.Choice(name="Добавить", value="add"),
    app_commands.Choice(name="Удалить", value="remove"),
    app_commands.Choice(name="Список", value="list"),
])
async def whitelist(interaction: discord.Interaction, action: str, user: discord.User = None):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return

    if action == "add":
        if user is None:
            await interaction.response.send_message("❌ Укажите пользователя.", ephemeral=True)
            return
        if user.id in ALLOWED_USER_IDS:
            await interaction.response.send_message(f"ℹ️ {user.mention} уже в белом списке.", ephemeral=True)
        else:
            ALLOWED_USER_IDS.add(user.id)
            save_config()
            await interaction.response.send_message(f"✅ {user.mention} добавлен в белый список.", ephemeral=True)
            logger.info(f"Whitelist add: {user} ({user.id})")

    elif action == "remove":
        if user is None:
            await interaction.response.send_message("❌ Укажите пользователя.", ephemeral=True)
            return
        if user.id not in ALLOWED_USER_IDS:
            await interaction.response.send_message(f"⚠️ {user.mention} не найден в белом списке.", ephemeral=True)
        else:
            ALLOWED_USER_IDS.discard(user.id)
            save_config()
            ch = bot.get_channel(TARGET_CHANNEL_ID)
            if ch and ch.guild:
                member = ch.guild.get_member(user.id)
                if member:
                    for rid in ROLE_THRESHOLDS:
                        r = ch.guild.get_role(rid)
                        if r and r in member.roles:
                            try:
                                await member.remove_roles(r, reason="🦣 Удалён из whitelist")
                            except Exception:
                                pass
            await db.delete_user(user.id)
            await interaction.response.send_message(f"🚫 {user.mention} удалён из белого списка.", ephemeral=True)
            logger.info(f"Whitelist remove: {user} ({user.id})")

    elif action == "list":
        if not ALLOWED_USER_IDS:
            await interaction.response.send_message("📭 Белый список пуст.", ephemeral=True)
        else:
            mentions = []
            for uid in ALLOWED_USER_IDS:
                u = bot.get_user(uid) or await bot.fetch_user(uid)
                mentions.append(f"• {u.mention if u else uid}")
            await interaction.response.send_message("📋 **Белый список:**\n" + "\n".join(mentions), ephemeral=True)
    else:
        await interaction.response.send_message("❓ Неизвестное действие.", ephemeral=True)


# ---------- stats и user ----------
async def build_stats_embed(guild: discord.Guild | None) -> discord.Embed:
    stats = await db.get_stats()
    top = await db.get_top(10)

    embed = discord.Embed(title="🦣 Общая статистика", color=0xb19c7c)
    embed.add_field(name="Всего мамонтов", value=str(stats.get("total", 0)), inline=True)
    embed.add_field(name="Участников с мамонтами", value=str(stats.get("users", 0)), inline=True)

    if top:
        top_text = []
        for row in top:
            uid = int(row["user_id"])
            member = guild.get_member(uid) if guild else None
            name = member.display_name if member else str(uid)
            last = row.get("last_seen")
            last_str = f" (последний {last[:10]})" if last else ""
            top_text.append(f"`{int(row['total']):>5}` {name}{last_str}")
        embed.add_field(name="🏆 Топ-10", value="\n".join(top_text), inline=False)
    else:
        embed.add_field(name="🏆 Топ-10", value="Пока нет данных", inline=False)

    return embed


class StatsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🦣 Статистика",
        style=discord.ButtonStyle.primary,
        custom_id="mammoth_stats_btn",
    )
    async def stats_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await asyncio.wait_for(interaction.response.defer(ephemeral=True), timeout=2.5)
        except (asyncio.TimeoutError, discord.NotFound):
            return
        await interaction.followup.send(embed=await build_stats_embed(interaction.guild), ephemeral=True)


@mammoth_group.command(name="stats", description="Общая статистика мамонтов на сервере")
async def mammoth_stats(interaction: discord.Interaction):
    try:
        await asyncio.wait_for(interaction.response.defer(ephemeral=True), timeout=2.5)
    except (asyncio.TimeoutError, discord.NotFound):
        return
    await interaction.followup.send(embed=await build_stats_embed(interaction.guild), ephemeral=True)


@mammoth_group.command(name="panel", description="Отправить в чат кнопку статистики")
async def mammoth_panel(interaction: discord.Interaction):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    if not interaction.channel:
        await interaction.response.send_message("❌ Канал недоступен.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    embed = discord.Embed(
        title="🦣 Мамонты",
        description="Нажми кнопку, чтобы посмотреть статистику.",
        color=0xb19c7c,
    )
    try:
        await interaction.channel.send(embed=embed, view=StatsView())
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ Нет прав писать в этот канал. Дай боту право «Отправлять сообщения» "
            "или запусти команду в канале 🦣.",
            ephemeral=True,
        )
        return

    await interaction.followup.send("✅ Кнопка отправлена в чат.", ephemeral=True)


@mammoth_group.command(name="user", description="Статистика конкретного пользователя")
@app_commands.describe(user="Участник")
async def mammoth_user(interaction: discord.Interaction, user: discord.User):
    try:
        await asyncio.wait_for(interaction.response.defer(ephemeral=True), timeout=2.5)
    except (asyncio.TimeoutError, discord.NotFound):
        return

    uid = user.id
    data = await db.get_user(uid)
    cnt = int(data["total"]) if data else 0
    last = data["last_seen"] if data else None

    embed = discord.Embed(title=f"🦣 Статистика {user.display_name}", color=0xb19c7c)
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="Мамонтов", value=str(cnt), inline=True)
    embed.add_field(name="Последний", value=last[:19] if last else "—", inline=True)

    status_ok = False
    whitelisted = uid in ALLOWED_USER_IDS
    if interaction.guild:
        member = interaction.guild.get_member(uid)
        if member:
            status_ok = has_required_status(member)

    if whitelisted:
        status_text = "⭐ Белый список"
    elif status_ok:
        status_text = "✅ Статус активен"
    else:
        status_text = "❌ Нет статуса"
    embed.add_field(name="Статус", value=status_text, inline=True)

    if interaction.guild:
        member = interaction.guild.get_member(uid)
        if member:
            roles = []
            for rid in ROLE_THRESHOLDS:
                r = interaction.guild.get_role(rid)
                if r and r in member.roles:
                    roles.append(r.mention)
            embed.add_field(name="🎖 Роли", value=", ".join(roles) if roles else "Нет", inline=False)
        else:
            embed.add_field(name="🎖 Роли", value="Нет на сервере", inline=False)
    else:
        embed.add_field(name="🎖 Роли", value="Недоступно", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


@mammoth_group.command(name="roles", description="Пересчитать роли по текущей статистике")
async def mammoth_roles(interaction: discord.Interaction):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await recalculate_all_roles()
    await interaction.followup.send("✅ Роли пересчитаны по статистике.", ephemeral=True)


@mammoth_group.command(name="cleanreactions", description="Убрать все реакции, кроме разрешённых")
async def mammoth_cleanreactions(interaction: discord.Interaction):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    ch = bot.get_channel(TARGET_CHANNEL_ID)
    if not ch:
        await interaction.followup.send("❌ Целевой канал не найден.", ephemeral=True)
        return

    await interaction.followup.send("🧹 Чищу реакции...", ephemeral=True)

    async def run():
        checked = 0
        removed = 0
        errors = 0
        try:
            async for msg in ch.history(limit=None):
                checked += 1
                for reaction in list(msg.reactions):
                    if normalize_emoji(reaction.emoji) in ALLOWED_REACTIONS_NORM:
                        continue
                    try:
                        await msg.clear_reaction(reaction.emoji)
                        removed += 1
                    except discord.Forbidden:
                        errors += 1
                    except discord.HTTPException:
                        pass
                if checked % 25 == 0:
                    await asyncio.sleep(1)
            text = f"✅ Проверено сообщений: {checked}, убрано реакций: {removed}."
            if errors:
                text += f"\n⚠️ Ошибок доступа: {errors} — нужно право «Управлять сообщениями»."
            await interaction.followup.send(text, ephemeral=True)
        except Exception as e:
            logger.error(f"Ошибка очистки реакций: {e}\n{traceback.format_exc()}")
            await interaction.followup.send("❌ Ошибка во время очистки. Смотри лог.", ephemeral=True)

    asyncio.create_task(run())


# ---------- config группа ----------
config_group = app_commands.Group(name="config", description="Изменить настройки бота на лету", parent=mammoth_group)


def restart_task(task, new_interval, interval_type):
    task.cancel()
    task.change_interval(**{interval_type: new_interval})
    task.start()


@config_group.command(name="status", description="Изменить требуемый статус")
@app_commands.describe(text="Новый текст статуса")
async def config_status(interaction: discord.Interaction, text: str):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global REQUIRED_STATUS
    REQUIRED_STATUS = text
    save_config()
    await interaction.response.send_message(f"✅ Требуемый статус изменён на `{text}`", ephemeral=True)


@config_group.command(name="strict", description="Включить/выключить строгий режим (только мамонты)")
@app_commands.describe(mode="on / off")
@app_commands.choices(mode=[
    app_commands.Choice(name="Включить", value="on"),
    app_commands.Choice(name="Выключить", value="off"),
])
async def config_strict(interaction: discord.Interaction, mode: str):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global STRICT_MAMMOTH_ONLY
    STRICT_MAMMOTH_ONLY = (mode == "on")
    save_config()
    await interaction.response.send_message(f"✅ Строгий режим: **{'включен' if STRICT_MAMMOTH_ONLY else 'выключен'}**", ephemeral=True)


@config_group.command(name="quick_limit", description="Лимит сообщений быстрого сканирования")
@app_commands.describe(limit="Количество сообщений (1-500)")
async def config_quick_limit(interaction: discord.Interaction, limit: int):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global QUICK_LIMIT
    QUICK_LIMIT = max(1, min(500, limit))
    save_config()
    await interaction.response.send_message(f"✅ Быстрый лимит: {QUICK_LIMIT}", ephemeral=True)


@config_group.command(name="deep_limit", description="Лимит сообщений глубокого сканирования")
@app_commands.describe(limit="Количество сообщений (1-2000)")
async def config_deep_limit(interaction: discord.Interaction, limit: int):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global DEEP_LIMIT
    DEEP_LIMIT = max(1, min(2000, limit))
    save_config()
    await interaction.response.send_message(f"✅ Глубокий лимит: {DEEP_LIMIT}", ephemeral=True)


@config_group.command(name="quick_interval", description="Интервал быстрого цикла (секунды)")
@app_commands.describe(seconds="Новый интервал (минимум 5)")
async def config_quick_interval(interaction: discord.Interaction, seconds: int):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global SCAN_QUICK_INTERVAL
    SCAN_QUICK_INTERVAL = max(5, seconds)
    save_config()
    restart_task(scan_quick, SCAN_QUICK_INTERVAL, interval_type="seconds")
    await interaction.response.send_message(f"✅ Быстрый интервал: {SCAN_QUICK_INTERVAL} сек.", ephemeral=True)


@config_group.command(name="deep_interval", description="Интервал глубокого цикла (минуты)")
@app_commands.describe(minutes="Новый интервал (минимум 1)")
async def config_deep_interval(interaction: discord.Interaction, minutes: int):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global SCAN_DEEP_INTERVAL_MIN
    SCAN_DEEP_INTERVAL_MIN = max(1, minutes)
    save_config()
    restart_task(scan_deep, SCAN_DEEP_INTERVAL_MIN, interval_type="minutes")
    await interaction.response.send_message(f"✅ Глубокий интервал: {SCAN_DEEP_INTERVAL_MIN} мин.", ephemeral=True)


@config_group.command(name="status_check", description="Интервал проверки статусов (часы)")
@app_commands.describe(hours="Новый интервал (минимум 1)")
async def config_status_check(interaction: discord.Interaction, hours: int):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    global STATUS_CHECK_HOURS
    STATUS_CHECK_HOURS = max(1, hours)
    save_config()
    restart_task(status_verification, STATUS_CHECK_HOURS, interval_type="hours")
    await interaction.response.send_message(f"✅ Проверка статусов: каждые {STATUS_CHECK_HOURS} час(ов)", ephemeral=True)


@config_group.command(name="threshold", description="Изменить порог для роли")
@app_commands.describe(role="Роль из списка", count="Новое количество мамонтов для этой роли")
async def config_threshold(interaction: discord.Interaction, role: discord.Role, count: int):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return
    if role.id not in ROLE_THRESHOLDS:
        await interaction.response.send_message(f"❌ Роль {role.mention} не отслеживается.", ephemeral=True)
        return
    ROLE_THRESHOLDS[role.id] = count
    save_config()
    await interaction.response.send_message(f"✅ Порог для {role.mention}: {count} мамонтов", ephemeral=True)
    asyncio.create_task(recalculate_all_roles())


@config_group.command(name="rebuild", description="Пересобрать статистику из всей истории канала")
async def config_rebuild(interaction: discord.Interaction):
    if interaction.user.id not in ALLOWED_USER_IDS:
        await interaction.response.send_message("❌ Недостаточно прав.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    ch = bot.get_channel(TARGET_CHANNEL_ID)
    if not ch:
        await interaction.followup.send("❌ Целевой канал не найден.", ephemeral=True)
        return

    await interaction.followup.send("🔄 Начинаю полный пересбор статистики из истории...", ephemeral=True)

    async def run():
        try:
            counted = await full_rebuild(ch)
            await interaction.followup.send(f"✅ Готово. Учтено сообщений: {counted}", ephemeral=True)
            asyncio.create_task(recalculate_all_roles())
        except Exception as e:
            logger.error(f"Ошибка ребилда: {e}\n{traceback.format_exc()}")
            await interaction.followup.send("❌ Ошибка во время пересбора. Смотри лог.", ephemeral=True)

    asyncio.create_task(run())


async def recalculate_all_roles():
    ch = bot.get_channel(TARGET_CHANNEL_ID)
    if not ch or not ch.guild:
        return
    guild = ch.guild
    totals = await db.get_totals()
    for uid, cnt in totals.items():
        member = guild.get_member(uid)
        if not member:
            try:
                member = await guild.fetch_member(uid)
            except Exception:
                continue
        await update_user_roles(member, cnt)


# ==================== /status (доступен всем) ====================
@bot.tree.command(name="status", description="Общая статистика и состояние бота")
async def bot_status(interaction: discord.Interaction):
    uptime_sec = int(time.time() - START_TIME)
    days, rem = divmod(uptime_sec, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    uptime_str = f"{days}д {hours}ч {mins}м {secs}с"

    stats = await db.get_stats()

    embed = discord.Embed(title="🦣 Статус бота", color=0xb19c7c)
    embed.add_field(name="Аптайм", value=uptime_str, inline=True)
    embed.add_field(name="Всего мамонтов", value=str(stats.get("total", 0)), inline=True)
    embed.add_field(name="Пользователей", value=str(stats.get("users", 0)), inline=True)
    embed.add_field(name="Белый список", value=str(len(ALLOWED_USER_IDS)), inline=True)
    embed.add_field(name="Хранилище", value="Cloudflare D1", inline=False)
    embed.set_footer(text=f"Запросил {interaction.user.display_name}")

    await interaction.response.send_message(embed=embed, ephemeral=True)


bot.tree.add_command(mammoth_group)


# ==================== События ====================
@bot.event
async def on_ready():
    await bot.wait_until_ready()
    logger.info(f"Бот {bot.user} запущен")

    bot.add_view(StatsView())

    global last_processed_message_id
    meta_last = None
    try:
        meta_last = await db.get_meta("last_msg")
    except Exception as e:
        logger.warning(f"Не удалось прочитать чекпоинт из D1: {e}")
    if meta_last:
        last_processed_message_id = int(meta_last)
        logger.info(f"Загружен last_processed_message_id (D1): {last_processed_message_id}")
    elif LAST_MSG_FILE.exists():
        try:
            content = LAST_MSG_FILE.read_text().strip()
            if content:
                last_processed_message_id = int(content)
                logger.info(f"Загружен last_processed_message_id (файл): {last_processed_message_id}")
        except (ValueError, OSError) as e:
            logger.error(f"Ошибка чтения last_msg.txt: {e}")

    await db.flush_queue()

    ch = bot.get_channel(TARGET_CHANNEL_ID)
    if ch and ch.guild:
        logger.info("🔍 Стартовая проверка статусов...")
        for row in await db.get_all_users():
            uid = int(row["user_id"])
            if uid in ALLOWED_USER_IDS:
                continue
            try:
                member = await ch.guild.fetch_member(uid)
                if not has_required_status(member):
                    for rid in ROLE_THRESHOLDS:
                        r = ch.guild.get_role(rid)
                        if r and r in member.roles:
                            try:
                                await member.remove_roles(r, reason="🦣 Старт: статус отсутствует")
                            except Exception:
                                pass
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(f"⚠️ {uid}: {e}")
            await asyncio.sleep(0.5)
        logger.info("✅ Стартовая проверка завершена.")

    scan_quick.start()
    scan_deep.start()
    status_verification.start()
    flush_state_loop.start()

    try:
        commands_hash = hashlib.sha256(
            json.dumps(
                [c.to_dict(bot.tree) for c in bot.tree.get_commands()],
                sort_keys=True,
                ensure_ascii=False,
                default=str,
            ).encode()
        ).hexdigest()
        old_hash = await db.get_meta("commands_hash") or ""
        if commands_hash != old_hash:
            synced = await bot.tree.sync()
            await db.set_meta("commands_hash", commands_hash)
            logger.info(f"Синхронизировано {len(synced)} команд")
        else:
            logger.info("Команды не изменились — синхронизация пропущена")
    except Exception as e:
        logger.error(f"Ошибка синхронизации: {e}")


@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user or message.channel.id != TARGET_CHANNEL_ID:
        return
    await process_message(message, message.guild)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if after.author.bot or after.channel.id != TARGET_CHANNEL_ID:
        return
    if not is_mammoth_message(after):
        await try_delete(after)
    else:
        await try_react(after)


async def shutdown():
    logger.info("Сохраняем очередь...")
    await db.flush_queue()
    await db.close()
    await bot.close()


async def main():
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60)
    session = aiohttp.ClientSession(timeout=timeout)
    bot.http._HTTPClient__session = session
    try:
        await bot.start(cfg["token"])
    finally:
        await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(shutdown())
