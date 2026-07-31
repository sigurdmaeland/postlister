"""Henvendelser (Asker) - engangs historisk dump.

Ingen egen GraphQL-type finnes for dette - henter samme rådata som
../bygg/main.py og filtrerer til saker uten eiendomsreferanse, se
asker/common/scraper_lib.py (modul-docstring + run_full_dump) for detaljer.
Denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "henv"
OUTPUT_FILE = Path(__file__).parent / "asker_henv.json"

if __name__ == "__main__":
    from datetime import date, datetime, timedelta
    from zoneinfo import ZoneInfo
    if len(sys.argv) > 2:
        run_full_dump(KILDE, OUTPUT_FILE,
                      start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    elif len(sys.argv) > 1:
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        run_full_dump(KILDE, OUTPUT_FILE, start=end - timedelta(days=int(sys.argv[1]) - 1), end=end)
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
