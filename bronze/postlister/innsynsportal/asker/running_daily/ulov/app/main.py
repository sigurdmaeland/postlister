"""Ulovlighetssaker (Asker) - daglig endringslogg.

Se ../bygg/app/main.py og asker/common/scraper_lib.py for bakgrunn - denne
filen er bare konfigurasjon + inngangspunkt. Egen seen_journalpost_ids.json,
separat fra bygg sin.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "ulov"
HERE = Path(__file__).parent
OUTPUT_DIR = HERE / "output"
SEEN_IDS_FILE = HERE / "seen_journalpost_ids.json"

if __name__ == "__main__":
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run_daily(KILDE, OUTPUT_DIR, SEEN_IDS_FILE, day_back=db)
