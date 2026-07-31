"""Tilsynssaker (Larvik) - engangs historisk dump.

Se common/scraper_lib.py for bakgrunn - denne kilden har KUN 3 saker totalt
i hele arkivet (04.01.2018-18.12.2019), tatt med for fullstendighet siden de
fleste reelle tilsyn-/ulovlighetssaker i Larvik ligger som vanlige byggesaker
i stedet (se "bygg"-kilden og modul-docstringen for detaljer).

Kjøremåter:
  python3 main.py                     -> hele arkivet (2018-nå)
  python3 main.py 10                  -> kun de 10 nyeste sakene (smoketest)
  python3 main.py 2020-01-01 2020-01-31  -> kun saker med dato i det intervallet
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "tilsyn"
OUTPUT_FILE = Path(__file__).parent / "larvik_tilsyn.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
