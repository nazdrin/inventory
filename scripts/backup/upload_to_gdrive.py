#!/usr/bin/env python3

import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


ROOT_DIR = Path(__file__).resolve().parents[2]
CREDENTIALS_PATH = ROOT_DIR / "google_set" / "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
BACKUP_NAME_RE = re.compile(r"^backup_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.sql\.gz$")


def _get_retention_count() -> int:
    raw_value = os.getenv("GOOGLE_DRIVE_BACKUP_RETENTION_COUNT", "5").strip()
    try:
        retention_count = int(raw_value)
    except ValueError:
        print(f"Invalid GOOGLE_DRIVE_BACKUP_RETENTION_COUNT='{raw_value}', fallback to 5")
        return 5
    return max(retention_count, 1)


def _cleanup_old_backups(service, folder_id: str, retention_count: int) -> None:
    response = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name)",
        pageSize=1000,
    ).execute()

    backup_files = [
        file_info
        for file_info in response.get("files", [])
        if BACKUP_NAME_RE.match(str(file_info.get("name", "")))
    ]
    backup_files.sort(key=lambda item: item["name"], reverse=True)

    files_to_keep = backup_files[:retention_count]
    files_to_delete = backup_files[retention_count:]

    print(
        f"GDrive retention: found={len(backup_files)} keep={len(files_to_keep)} delete={len(files_to_delete)}"
    )

    for file_info in files_to_delete:
        service.files().delete(fileId=file_info["id"]).execute()
        print(f"GDrive retention deleted: {file_info['name']}")


def main() -> int:
    load_dotenv(ROOT_DIR / ".env")

    if len(sys.argv) != 2:
        print("Usage: upload_to_gdrive.py /path/to/backup.sql.gz")
        return 1

    file_path = Path(sys.argv[1]).resolve()
    folder_id = os.getenv("GOOGLE_DRIVE_BACKUP_FOLDER_ID")
    retention_count = _get_retention_count()

    if not file_path.exists():
        print(f"ERROR: file not found: {file_path}")
        return 1

    if not CREDENTIALS_PATH.exists():
        print(f"ERROR: credentials not found: {CREDENTIALS_PATH}")
        return 1

    if not folder_id:
        print("ERROR: GOOGLE_DRIVE_BACKUP_FOLDER_ID is not set")
        return 1

    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(CREDENTIALS_PATH),
            scopes=SCOPES,
        )
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)

        media = MediaFileUpload(str(file_path), resumable=False)
        metadata = {
            "name": file_path.name,
            "parents": [folder_id],
        }

        created = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,name",
        ).execute()

        print(f"Upload OK: {created.get('name')} ({created.get('id')})")
        _cleanup_old_backups(service, folder_id, retention_count)
        return 0
    except Exception as exc:
        print(f"Upload FAILED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
