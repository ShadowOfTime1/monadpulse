#!/bin/bash
# MonadPulse — daily PostgreSQL backup.
# Writes a compressed custom-format dump to /var/backups/monadpulse/
# and keeps the last 14 dumps (≈2 weeks).
set -euo pipefail

BACKUP_DIR=/var/backups/monadpulse
KEEP=14
DATE=$(date -u +%Y%m%d-%H%M)
DUMP_FILE="$BACKUP_DIR/monadpulse-$DATE.dump"

mkdir -p "$BACKUP_DIR"
chown postgres:postgres "$BACKUP_DIR"
chmod 0750 "$BACKUP_DIR"

sudo -u postgres pg_dump \
  --format=custom \
  --compress=9 \
  --no-owner --no-privileges \
  monadpulse \
  --file="$DUMP_FILE"

chown postgres:postgres "$DUMP_FILE"
chmod 0640 "$DUMP_FILE"

# Rotation — drop all but the newest $KEEP dumps
cd "$BACKUP_DIR"
ls -1t monadpulse-*.dump 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f

# Summary
count=$(ls -1 monadpulse-*.dump 2>/dev/null | wc -l)
newest=$(ls -lh "$DUMP_FILE" | awk '{print $5}')
echo "OK: $DUMP_FILE ($newest), $count dumps total"
