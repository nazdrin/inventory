#!/usr/bin/env python3

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


ROOT_DIR = Path(__file__).resolve().parents[2]
CREDENTIALS_PATH = ROOT_DIR / "google_set" / "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def main() -> int:
    load_dotenv(ROOT_DIR / ".env")

    if len(sys.argv) != 2:
        print("Usage: upload_to_gdrive.py /path/to/backup.sql.gz")
        return 1

    file_path = Path(sys.argv[1]).resolve()
    folder_id = os.getenv("GOOGLE_DRIVE_BACKUP_FOLDER_ID")

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
        return 0
    except Exception as exc:
        print(f"Upload FAILED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
