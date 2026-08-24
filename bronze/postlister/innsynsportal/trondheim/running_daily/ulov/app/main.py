"""Ulovlighetssaker (Trondheim) - daglig endringslogg.

Se ../bygg/app/main.py og common/scraper_lib.py for bakgrunn - denne filen
er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "ulov"

if __name__ == "__main__":
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run_daily(KILDE, day_back=db)
