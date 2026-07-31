"""Byggesaker (Trondheim) - daglig endringslogg.

Se common/scraper_lib.py for bakgrunn om mekanismen (ekte datofelt, dato-
vindu WINDOW_DAYS - ingen snapshot trengs) - denne filen er bare
konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "bygg"
OUTPUT_DIR = Path(__file__).parent / "output"

if __name__ == "__main__":
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run_daily(KILDE, OUTPUT_DIR, day_back=db)
