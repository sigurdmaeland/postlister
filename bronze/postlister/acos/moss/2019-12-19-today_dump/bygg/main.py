"""Byggesaker (Moss) - engangs historisk dump.

Se common/scraper_lib.py for bakgrunn - denne filen er bare
konfigurasjon + inngangspunkt.

Kjøremåter:
  python3 main.py                     -> hele arkivet (2019-12-19 til i dag)
  python3 main.py 10                  -> kun de 10 nyeste sakene (smoketest)
  python3 main.py 2020-01-01 2020-01-31  -> kun saker med dato i det intervallet
                                             (test/delkjøring - se run_full_dump
                                             sin from_date/to_date-docstring)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "bygg"
OUTPUT_FILE = Path(__file__).parent / "moss_bygg.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
