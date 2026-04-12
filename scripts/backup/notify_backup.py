#!/usr/bin/env python3

import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.notification_service import send_notification


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: notify_backup.py success /path/to/file | error 'message'")
        return 1

    mode = sys.argv[1]

    try:
        if mode == "success":
            file_path = Path(sys.argv[2]).resolve()
            if not file_path.exists():
                print(f"ERROR: backup file not found: {file_path}")
                return 1

            size_mb = os.path.getsize(file_path) / 1024 / 1024
            message = f"✅ Backup OK\n{file_path.name}\n{size_mb:.2f} MB"
            send_notification(message)
            print(f"Notification sent: success for {file_path.name}")
            return 0

        if mode == "error":
            error_message = " ".join(sys.argv[2:]).strip()
            message = f"❌ Backup FAILED\n{error_message}"
            send_notification(message)
            print(f"Notification sent: error '{error_message}'")
            return 0

        print(f"ERROR: unsupported mode: {mode}")
        return 1
    except Exception as exc:
        print(f"Notification FAILED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
