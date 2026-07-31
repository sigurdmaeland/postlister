"""Byggesaker (Oslo) - daglig endringslogg fra eInnsyn.

Se oslo/common/scraper_lib.py for bakgrunn om mekanismen (datovindu +
seen-dedup i state.json, auto-backfill) - denne filen er bare konfigurasjon
+ inngangspunkt + egen loggfil.
"""
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

HERE = Path(__file__).parent
OUTPUT_DIR = HERE / "output"
STATE_FILE = HERE / "state.json"
LOG_FILE = HERE / "run.log"


def log(msg):
    line = f"[{datetime.now(ZoneInfo('Europe/Oslo')).isoformat(timespec='seconds')}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


if __name__ == "__main__":
    day_back = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    try:
        run_daily(OUTPUT_DIR, STATE_FILE, day_back=day_back, log_fn=log)
    except Exception:  # noqa: BLE001
        sys.exit(1)
