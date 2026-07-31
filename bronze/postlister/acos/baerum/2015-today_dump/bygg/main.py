"""Byggesaker (Bærum 2015-nå) - engangs historisk dump.

Se common/scraper_lib.py for bakgrunn - denne filen er bare
konfigurasjon + inngangspunkt.

Kjøremåter:
  python3 main.py                        -> hele arkivet (2015-nå)
  python3 main.py 30                      -> kun siste 30 dager
  python3 main.py 2023-01-01 2023-03-31   -> eksplisitt datointervall (start slutt)
"""
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "bygg"
OUTPUT_FILE = Path(__file__).parent / "baerum.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE,
                      from_date=date.fromisoformat(args[0]), to_date=date.fromisoformat(args[1]))
    elif len(args) == 1:
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        start = end - timedelta(days=int(args[0]) - 1)
        run_full_dump(KILDE, OUTPUT_FILE, from_date=start, to_date=end)
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
