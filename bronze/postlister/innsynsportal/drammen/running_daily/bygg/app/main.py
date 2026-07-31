"""Byggesaker (Drammen, aktiv lista) - daglig endringslogg.

Se drammen/common/scraper_lib.py (LOOKBACK_DAYS + seen_journalpost_ids.json)
for bakgrunn om mekanismen - denne filen er bare konfigurasjon +
inngangspunkt. Følger kun den aktive lista - det historiske arkivet er i
praksis lukket og trenger ikke daglig oppfølging.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "bygg"
HERE = Path(__file__).parent
OUTPUT_DIR = HERE / "output"
SEEN_IDS_FILE = HERE / "seen_journalpost_ids.json"

if __name__ == "__main__":
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run_daily(KILDE, OUTPUT_DIR, SEEN_IDS_FILE, day_back=db)
