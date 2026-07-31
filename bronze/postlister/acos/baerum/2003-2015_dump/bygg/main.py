"""Byggesaker (Bærum, historisk portal 2003-2015) - engangs historisk dump.

Frosset arkiv (nyeste dokument mars 2015, portalen får aldri ny aktivitet) -
ingen egen running_daily. Se common/scraper_lib.py for bakgrunn.

Kjøremåter:
  python3 main.py                        -> full dump (hele arkivet)
  python3 main.py 2010-01-01 2010-03-31   -> eksplisitt datointervall (start slutt)
"""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "bygg_hist"
OUTPUT_FILE = Path(__file__).parent / "baerum.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE,
                      from_date=date.fromisoformat(args[0]), to_date=date.fromisoformat(args[1]))
    elif len(args) == 1:
        print("Feil: dette er et frosset arkiv (2003-2015) - 'N dager tilbake' gir ikke mening.")
        print("Bruk enten ingen argumenter (full dump) eller to datoer: python3 main.py START SLUTT")
        sys.exit(1)
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
