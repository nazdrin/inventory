#!/bin/bash

set -euo pipefail

# DO NOT MODIFY WITHOUT UNDERSTANDING:
# - GDrive upload
# - notify_backup
# - retention logic

BACKUP_DIR="/root/inventory/backups"
DB_NAME="inventory_db"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
BACKUP_FILE="${BACKUP_DIR}/backup_${TIMESTAMP}.sql.gz"
TMP_FILE="${BACKUP_FILE}.tmp"
RETENTION_DAYS=7

log() {
  printf '[%s] [backup_db] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

cleanup() {
  rm -f "${TMP_FILE}"
}

trap cleanup EXIT
trap '/root/inventory/.venv/bin/python /root/inventory/scripts/backup/notify_backup.py error "backup failed"; exit 1' ERR

mkdir -p "${BACKUP_DIR}"

log "Starting PostgreSQL backup for database ${DB_NAME}"

pg_dump -U postgres "${DB_NAME}" | gzip > "${TMP_FILE}"
if [[ ! -s "${TMP_FILE}" ]]; then
  log "ERROR: pg_dump did not create a non-empty temporary backup file"
  exit 1
fi
mv "${TMP_FILE}" "${BACKUP_FILE}"

log "Backup created: ${BACKUP_FILE}"

find "${BACKUP_DIR}" -type f -name 'backup_*.sql.gz' -mtime +${RETENTION_DAYS} -print -delete || true

if [[ -n "${BACKUP_REMOTE_HOST:-}" && -n "${BACKUP_REMOTE_PATH:-}" ]]; then
  log "Copying backup to remote storage ${BACKUP_REMOTE_HOST}:${BACKUP_REMOTE_PATH}"
  scp "${BACKUP_FILE}" "${BACKUP_REMOTE_HOST}:${BACKUP_REMOTE_PATH}/"
  log "Offsite copy completed"
else
  log "Offsite copy skipped: BACKUP_REMOTE_HOST or BACKUP_REMOTE_PATH is not set"
fi

log "Backup finished successfully"

/root/inventory/.venv/bin/python /root/inventory/scripts/backup/upload_to_gdrive.py "${BACKUP_FILE}" || true
/root/inventory/.venv/bin/python /root/inventory/scripts/backup/notify_backup.py success "${BACKUP_FILE}" || true
