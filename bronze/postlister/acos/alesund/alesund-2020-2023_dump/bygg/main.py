"""Byggesaker (Ålesund 2020-2023 + fellesnemnd 2017-2019) - engangs
historisk dump.

Lukket arkiv fra før den nyeste plattformoppgraderingen i 2024 - INGEN
running_daily (arkivet får aldri nye saker).

Kjøremåter: se ../../alesund-2024-today_dump/bygg/main.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "alesund_2020_2023"
OUTPUT_FILE = Path(__file__).parent / "alesund_2020_2023_bygg.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
