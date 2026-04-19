from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
TEMP_DIR = BASE_DIR / "temp"
STATE_CACHE_DIR = BASE_DIR / "state_cache"
BACKUPS_DIR = BASE_DIR / "backups"
LOGS_DIR = BASE_DIR / "logs"
