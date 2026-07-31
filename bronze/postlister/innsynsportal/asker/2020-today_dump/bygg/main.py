"""Byggesaker (Asker) - engangs historisk dump.

All faktisk skrape-/ryddelogikk bor i asker/common/scraper_lib.py - denne
filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "bygg"
OUTPUT_FILE = Path(__file__).parent / "asker.json"

if __name__ == "__main__":
    from datetime import date, datetime, timedelta
    from zoneinfo import ZoneInfo
    # Valgfrie argumenter:
    #   python3 main.py                        -> full dump fra sakstypens startdato
    #   python3 main.py 30                      -> kun siste 30 dager
    #   python3 main.py 2023-01-01 2023-03-31   -> eksplisitt datointervall
    if len(sys.argv) > 2:
        run_full_dump(KILDE, OUTPUT_FILE,
                      start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    elif len(sys.argv) > 1:
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        run_full_dump(KILDE, OUTPUT_FILE, start=end - timedelta(days=int(sys.argv[1]) - 1), end=end)
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
