#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[%s] [restore_db] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

usage() {
  echo "Usage: $0 /path/to/backup.sql.gz target_db"
}

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

BACKUP_PATH="$1"
TARGET_DB="$2"

if [[ ! -f "${BACKUP_PATH}" ]]; then
  log "ERROR: backup file not found: ${BACKUP_PATH}"
  exit 1
fi

trap 'log "ERROR: restore failed"; exit 1' ERR

if psql -U "${PGUSER:-postgres}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${TARGET_DB}'" | grep -q 1; then
  log "Target database exists: ${TARGET_DB}"
else
  log "Target database does not exist, creating: ${TARGET_DB}"
  createdb -U "${PGUSER:-postgres}" "${TARGET_DB}"
fi

log "Starting restore into database ${TARGET_DB} from ${BACKUP_PATH}"

case "${BACKUP_PATH}" in
  *.sql.gz)
    gunzip -c "${BACKUP_PATH}" | psql -U "${PGUSER:-postgres}" -d "${TARGET_DB}"
    ;;
  *.sql)
    psql -U "${PGUSER:-postgres}" -d "${TARGET_DB}" < "${BACKUP_PATH}"
    ;;
  *)
    log "ERROR: unsupported backup format. Expected .sql.gz or .sql"
    exit 1
    ;;
esac

log "Restore finished successfully"
