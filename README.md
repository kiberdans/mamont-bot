# 🦣 mamont-bot

Discord-бот для учёта «мамонтов»: считает сообщения с эмодзи 🦣 в заданном канале, удаляет всё лишнее, ставит реакцию и выдаёт роли по порогам. Статистика хранится в **Cloudflare D1**, сам бот работает на **GitHub Actions** — Termux/VPS не нужен.

## Возможности

- **Учёт сообщений**: каждое сообщение, состоящее только из 🦣 (плюс пробелы и переносы строк), даёт +1 мамонт автору. Считаются все, независимо от статуса.
- **Автоудаление**: сообщения с любым другим содержимым (текст, вложения, эмбеды, стикеры) удаляются.
- **Реакция**: бот ставит 🦣 под каждым прошедшим сообщением.
- **Пороги ролей**: роли выдаются/снимаются автоматически по количеству мамонтов.
- **Флаг статуса**: наличие 🦣 в кастомном статусе записывается в БД (`users.has_status`), но на учёт не влияет и роли не снимает.
- **Очистка реакций**: команда убирает все реакции, кроме разрешённых.
- **Статистика**: общая, по пользователю, топ-10, кнопка-панель.
- **Надёжность**: идемпотентный журнал сообщений в D1 (без двойного счёта), офлайн-очередь, чекпоинт и heartbeat в D1.

## Архитектура

```
Discord  <—gateway—  GitHub Actions (mamont.py)  —HTTP—>  Cloudflare D1
```

- **GitHub Actions** запускает бота job'ом на ~6 часов; расписание каждые 5 часов и `concurrency` обеспечивают непрерывную работу.
- **Cloudflare D1** — источник истины: таблицы `messages` (журнал), `users` (агрегаты), `meta` (чекпоинт `last_msg`, `commands_hash`, `heartbeat`).
- Токены хранятся в **GitHub Actions Secrets** (`CONFIG_JSON`) и в репозиторий не попадают.

## Команды

| Команда | Описание |
| --- | --- |
| `/status` | Аптайм, всего мамонтов, пользователей, белый список |
| `/mammoth stats` | Общая статистика и топ-10 |
| `/mammoth user <user>` | Статистика пользователя |
| `/mammoth panel` | Отправить в канал кнопку статистики |
| `/mammoth roles` | Пересчитать роли по текущей статистике |
| `/mammoth cleanreactions` | Убрать реакции, кроме разрешённых (в текущем канале) |
| `/mammoth whitelist add/remove/list` | Управление белым списком |
| `/mammoth config status <текст>` | Изменить требуемый статус |
| `/mammoth config strict on/off` | Строгий режим (только 🦣) |
| `/mammoth config quick_limit`, `deep_limit` | Лимиты сканирования |
| `/mammoth config quick_interval`, `deep_interval` | Интервалы сканирования |
| `/mammoth config status_check <часы>` | Интервал проверки статусов |
| `/mammoth config threshold <role> <count>` | Порог роли |
| `/mammoth config rebuild` | Пересобрать статистику из всей истории |

## Конфигурация

Файл `config.json` (не коммитится, шаблон — `config.example.json`):

```json
{
  "token": "DISCORD_BOT_TOKEN",
  "target_channel_id": 0,
  "allowed_user_ids": [],
  "mammoth_unicode": "🦣",
  "required_status": "🦣",
  "history_quick_limit": 50,
  "history_deep_limit": 500,
  "scan_quick_interval_sec": 5,
  "scan_deep_interval_min": 5,
  "status_check_interval_hours": 1,
  "mammoth_role_thresholds": {},
  "strict_mammoth_only": true,
  "allowed_reactions": ["🤝", "⭐", "🦣"],
  "cloudflare": {
    "account_id": "CLOUDFLARE_ACCOUNT_ID",
    "database_id": "CLOUDFLARE_D1_DATABASE_ID",
    "api_token": "CLOUDFLARE_API_TOKEN"
  }
}
```

### Секреты

- `CONFIG_JSON` — содержимое `config.json` целиком (Repository → Settings → Secrets and variables → Actions).

## Развёртывание

1. Создать D1-базу и применить схему:
   ```sql
   CREATE TABLE IF NOT EXISTS messages (
     message_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
     count INTEGER NOT NULL, ts TEXT NOT NULL
   );
   CREATE TABLE IF NOT EXISTS users (
     user_id TEXT PRIMARY KEY, total INTEGER NOT NULL DEFAULT 0, last_seen TEXT
   );
   CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);

   CREATE TRIGGER IF NOT EXISTS trg_messages_insert AFTER INSERT ON messages
   BEGIN
     INSERT INTO users(user_id,total,last_seen) VALUES (NEW.user_id, NEW.count, NEW.ts)
     ON CONFLICT(user_id) DO UPDATE SET total = total + NEW.count, last_seen = NEW.ts;
   END;
   ```
2. Положить `config.json` в секрет `CONFIG_JSON`.
3. Запустить workflow `mamont-bot` (вручную или по расписанию).
4. Права роли бота в Discord: **Управлять сообщениями**, **Управлять ролями**, **Добавлять реакции**.

## Файлы

| Файл | Назначение |
| --- | --- |
| `mamont.py` | Основной бот |
| `storage.py` | Клиент Cloudflare D1 (запись/чтение, офлайн-очередь) |
| `rebuild.py` | Полный пересбор статистики из истории канала |
| `react_all.py` | Проставить 🦣 на все сообщения канала |
| `clean_reactions.py` | Удалить запрещённые реакции из канала |
| `start.sh` / `stop.sh` | Запуск/остановка локально (Termux) |
| `.github/workflows/bot.yml` | Запуск бота на GitHub Actions |
| `.github/workflows/heartbeat.yml` | Ежемесячный коммит, чтобы расписания не отключались |

## Мониторинг

- Вкладка **Actions** репозитория — статус запусков.
- В D1: `SELECT value FROM meta WHERE key='heartbeat'` — время последнего «сердцебиения» бота.

## Лицензия

ISC
