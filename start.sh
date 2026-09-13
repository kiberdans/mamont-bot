#!/data/data/com.termux/files/usr/bin/bash
cd "$(dirname "$0")"
if [ -f data/bot.pid ] && kill -0 "$(cat data/bot.pid)" 2>/dev/null; then
  echo "Уже запущен (PID $(cat data/bot.pid))"
  exit 0
fi
termux-wake-lock
nohup python3 mamont.py >> data/bot.log 2>&1 &
echo $! > data/bot.pid
echo "Запущен (PID $(cat data/bot.pid))"
