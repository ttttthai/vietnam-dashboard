#!/bin/bash
# Run daily by launchd (com.vietnamdashboard.backup). Asks before backing up when ≥ 14 days since the last backup.
# "Để sau" (Later) → asks again tomorrow. No answer within 1 hour → treated as "Later".
PROJECT="/Users/hoangthai/CLAUDECODE/VIETNAM DASHBOARD"
BK="$PROJECT/BACKUP"
PY="$PROJECT/.venv/bin/python"
INTERVAL_DAYS=14
TODAY=$(date +%Y-%m-%d)

# Snoozed until a later date?
if [ -f "$BK/.snooze_until" ] && [[ "$TODAY" < "$(cat "$BK/.snooze_until")" ]]; then exit 0; fi

# Days since last successful backup (manifest written only on success)
DAYS=999
if [ -f "$BK/manifest.json" ]; then
  LAST=$("$PY" -c "import json,sys; print(json.load(open(sys.argv[1]))['last_backup'][:10])" "$BK/manifest.json" 2>/dev/null)
  if [ -n "$LAST" ]; then
    DAYS=$(( ( $(date -j -f %Y-%m-%d "$TODAY" +%s) - $(date -j -f %Y-%m-%d "$LAST" +%s) ) / 86400 ))
  fi
fi
[ "$DAYS" -lt "$INTERVAL_DAYS" ] && exit 0

MSG="Đã $DAYS ngày kể từ lần sao lưu gần nhất.\n\nSao lưu dữ liệu Vietnam Dashboard (giá chứng khoán, file hệ thống) vào thư mục BACKUP ngay bây giờ?\n\nMất khoảng 2–5 phút; máy cần kết nối Internet."
[ "$DAYS" -ge 999 ] && MSG="Chưa có bản sao lưu nào.\n\nSao lưu dữ liệu Vietnam Dashboard vào thư mục BACKUP ngay bây giờ?\n\nLần đầu tải toàn bộ lịch sử giá: ~15–20 phút, ~300 MB."
ANSWER=$(osascript -e "display dialog \"$MSG\" with title \"Vietnam Dashboard — Sao lưu\" buttons {\"Để sau\", \"Sao lưu ngay\"} default button \"Sao lưu ngay\" giving up after 3600" 2>/dev/null)

if [[ "$ANSWER" != *"Sao lưu ngay"* ]]; then
  date -v+1d +%Y-%m-%d > "$BK/.snooze_until"
  exit 0
fi
rm -f "$BK/.snooze_until"
cd "$PROJECT" || exit 1
"$PY" backup.py --yes > "$BK/last_run.log" 2>&1
if [ $? -eq 0 ]; then
  osascript -e 'display notification "Sao lưu hoàn tất — xem BACKUP/manifest.json" with title "Vietnam Dashboard"'
else
  osascript -e 'display notification "Sao lưu lỗi — xem BACKUP/last_run.log" with title "Vietnam Dashboard"'
fi
