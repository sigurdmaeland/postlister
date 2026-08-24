"""Tilsynssaker (Tromsø) - daglig endringslogg.

Se running_daily/bygg/app/main.py for bakgrunn - denne filen er bare
konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import TYPE_IDS, run_daily  # noqa: E402

SAKSTYPE = "Tilsynssak"

if __name__ == "__main__":
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run_daily(TYPE_IDS[SAKSTYPE], SAKSTYPE, day_back=db)
