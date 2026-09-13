#!/data/data/com.termux/files/usr/bin/bash
cd "$(dirname "$0")"
if [ -f data/bot.pid ]; then
  PID="$(cat data/bot.pid)"
  kill "$PID" 2>/dev/null && echo "Остановлен (PID $PID)" || echo "Процесс не найден"
  rm -f data/bot.pid
else
  pkill -f "python3 mamont.py" && echo "Остановлен" || echo "Процесс не найден"
fi
