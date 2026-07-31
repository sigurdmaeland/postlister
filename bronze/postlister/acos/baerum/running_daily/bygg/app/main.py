"""Byggesaker (Bærum 2015-nå) - daglig endringslogg.

Se common/scraper_lib.py for bakgrunn om mekanismen (dato-vindu-basert
CaseOnly+DocOnly-sammenslåing, auto-backfill, "seen"-dedup) - denne filen er
bare konfigurasjon + inngangspunkt + egen loggfil.
"""
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "bygg"
HERE = Path(__file__).parent
STATE_FILE = HERE / "state.json"
OUTPUT_DIR = HERE / "output"
LOG_FILE = HERE / "run.log"


def log(msg):
    line = f"[{datetime.now(ZoneInfo('Europe/Oslo')).isoformat(timespec='seconds')}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


if __name__ == "__main__":
    day_back = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    try:
        run_daily(KILDE, STATE_FILE, OUTPUT_DIR, day_back=day_back, log_fn=log)
    except Exception:  # noqa: BLE001
        sys.exit(1)
