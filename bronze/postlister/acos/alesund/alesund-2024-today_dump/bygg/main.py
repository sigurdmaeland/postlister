"""Byggesaker (Ålesund fra 2024-nå) - engangs historisk dump.

Eneste av de seks Ålesund-kildene som fortsatt får nye saker - se
../../running_daily/bygg/app/main.py for daglig endringslogg.

Kjøremåter:
  main.py            - full dump (gjenopptakbar)
  main.py N           - kun de N nyeste sakene (smoketest)
  main.py FRA TIL     - kun saker med dato i [FRA, TIL] (YYYY-MM-DD)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "alesund_2024"
OUTPUT_FILE = Path(__file__).parent / "alesund_bygg.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
