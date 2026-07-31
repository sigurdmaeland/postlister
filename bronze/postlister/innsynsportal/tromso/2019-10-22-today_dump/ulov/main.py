"""Ulovlighetssaker (Tromsø) - engangs historisk dump.

Se bygg/main.py for bakgrunn - denne filen er bare konfigurasjon +
inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import TYPE_IDS, run_full_dump  # noqa: E402

SAKSTYPE = "Ulovlighetssak"
OUTPUT_FILE = Path(__file__).parent / "tromso_ulov.json"

if __name__ == "__main__":
    from datetime import date
    if len(sys.argv) == 3:
        run_full_dump(OUTPUT_FILE, TYPE_IDS[SAKSTYPE], SAKSTYPE,
                      start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    else:
        run_full_dump(OUTPUT_FILE, TYPE_IDS[SAKSTYPE], SAKSTYPE)
