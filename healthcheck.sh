#!/bin/bash
# MonadPulse health check — runs via cron every 5 minutes
# Checks: nginx, API, collector (testnet + mainnet), PostgreSQL
# Sends Telegram alert on failure, with anti-spam (1 alert per issue per hour)

BOT_TOKEN=$(grep TELEGRAM_BOT_TOKEN /opt/monadpulse/.env | cut -d= -f2-)
CHANNEL="@monadpulse_alerts"
STATE_DIR="/opt/monadpulse/.health-state"
mkdir -p "$STATE_DIR"

FAILURES=""

tg_alert() {
  local key="$1" msg="$2"
  local lockfile="$STATE_DIR/$key.sent"
  # Anti-spam: skip if alert sent in last 60 minutes
  if [ -f "$lockfile" ] && [ $(($(date +%s) - $(stat -c %Y "$lockfile"))) -lt 3600 ]; then
    return
  fi
  curl -s "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d "chat_id=${CHANNEL}" \
    -d "text=🚨 <b>MonadPulse DOWN</b>%0A${msg}" \
    -d "parse_mode=HTML" \
    -d "disable_web_page_preview=true" > /dev/null 2>&1
  touch "$lockfile"
}

clear_alert() {
  local key="$1"
  local lockfile="$STATE_DIR/$key.sent"
  if [ -f "$lockfile" ]; then
    rm -f "$lockfile"
    # Send recovery notification
    curl -s "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
      -d "chat_id=${CHANNEL}" \
      -d "text=✅ <b>MonadPulse recovered</b>%0A$2" \
      -d "parse_mode=HTML" > /dev/null 2>&1
  fi
}

# 1. Nginx
if ! systemctl is-active --quiet nginx; then
  FAILURES="${FAILURES}nginx "
  tg_alert "nginx" "Nginx is down"
else
  clear_alert "nginx" "Nginx is back up"
fi

# 2. API — check /ping
API_RESP=$(curl -s --max-time 5 -o /dev/null -w "%{http_code}" http://127.0.0.1:8890/ping 2>/dev/null)
if [ "$API_RESP" != "200" ]; then
  FAILURES="${FAILURES}api "
  tg_alert "api" "API not responding (HTTP $API_RESP)"
  # Try restart
  sudo systemctl restart monadpulse-api
else
  clear_alert "api" "API is responding"
fi

# 3. HTTPS endpoint
HTTPS_RESP=$(curl -s --max-time 10 -o /dev/null -w "%{http_code}" https://monadpulse.xyz/api/ping 2>/dev/null)
if [ "$HTTPS_RESP" != "200" ]; then
  FAILURES="${FAILURES}https "
  tg_alert "https" "HTTPS endpoint unreachable (HTTP $HTTPS_RESP)"
else
  clear_alert "https" "HTTPS endpoint is reachable"
fi

# 4. Testnet collector
if ! systemctl is-active --quiet monadpulse-collector; then
  FAILURES="${FAILURES}collector-testnet "
  tg_alert "collector-testnet" "Testnet collector is down"
  sudo systemctl restart monadpulse-collector
else
  clear_alert "collector-testnet" "Testnet collector is running"
fi

# 5. Mainnet collector
if ! systemctl is-active --quiet monadpulse-collector-mainnet; then
  FAILURES="${FAILURES}collector-mainnet "
  tg_alert "collector-mainnet" "Mainnet collector is down"
  sudo systemctl restart monadpulse-collector-mainnet
else
  clear_alert "collector-mainnet" "Mainnet collector is running"
fi

# 6. PostgreSQL
if ! systemctl is-active --quiet postgresql; then
  FAILURES="${FAILURES}postgresql "
  tg_alert "postgresql" "PostgreSQL is down"
else
  clear_alert "postgresql" "PostgreSQL is running"
fi

# 7. Data freshness — check if blocks are being written (last 10 min)
FRESH=$(sudo -u postgres psql monadpulse -t -c "SELECT COUNT(*) FROM blocks WHERE timestamp > NOW() - INTERVAL '10 minutes' AND network='testnet';" 2>/dev/null | xargs)
if [ -z "$FRESH" ] || [ "$FRESH" -lt 1 ]; then
  FAILURES="${FAILURES}stale-data "
  tg_alert "stale-data" "No new testnet blocks in 10 minutes"
else
  clear_alert "stale-data" "Block ingestion resumed ($FRESH blocks in last 10min)"
fi

# 8. SSL certificate expiry (warn if < 14 days)
EXPIRY=$(echo | openssl s_client -connect monadpulse.xyz:443 -servername monadpulse.xyz 2>/dev/null | openssl x509 -noout -enddate 2>/dev/null | cut -d= -f2)
if [ -n "$EXPIRY" ]; then
  EXPIRY_EPOCH=$(date -d "$EXPIRY" +%s 2>/dev/null)
  NOW_EPOCH=$(date +%s)
  DAYS_LEFT=$(( (EXPIRY_EPOCH - NOW_EPOCH) / 86400 ))
  if [ "$DAYS_LEFT" -lt 14 ]; then
    tg_alert "ssl" "SSL certificate expires in ${DAYS_LEFT} days"
  else
    clear_alert "ssl" "SSL certificate OK (${DAYS_LEFT} days left)"
  fi
fi

# Log
if [ -n "$FAILURES" ]; then
  echo "$(date -u '+%Y-%m-%d %H:%M UTC') FAIL: $FAILURES" >> /tmp/monadpulse-health.log
else
  echo "$(date -u '+%Y-%m-%d %H:%M UTC') OK" >> /tmp/monadpulse-health.log
fi
