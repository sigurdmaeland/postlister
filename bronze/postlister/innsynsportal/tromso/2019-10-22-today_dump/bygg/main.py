"""Byggesaker (Tromsø) - engangs historisk dump.

All faktisk skrape- og ryddelogikk bor i det delte biblioteket
tromso/common/scraper_lib.py - denne filen er bare konfigurasjon +
inngangspunkt. Se den filen for bakgrunn om plattformen (innsynsportal.no,
samme GraphQL-API som Trondheim/Asker) og hvorfor START_DATE=2019-10-22.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import TYPE_IDS, run_full_dump  # noqa: E402

SAKSTYPE = "Byggesak"
OUTPUT_FILE = Path(__file__).parent / "tromso_bygg.json"

if __name__ == "__main__":
    from datetime import date
    # Valgfrie argumenter:
    #   python3 main.py                        -> full dump fra 2019-10-22
    #   python3 main.py 2023-01-01 2023-03-31   -> eksplisitt datointervall
    if len(sys.argv) == 3:
        run_full_dump(OUTPUT_FILE, TYPE_IDS[SAKSTYPE], SAKSTYPE,
                      start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    else:
        run_full_dump(OUTPUT_FILE, TYPE_IDS[SAKSTYPE], SAKSTYPE)
