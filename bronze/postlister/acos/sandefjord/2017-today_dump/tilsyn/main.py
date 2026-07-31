"""Byggetilsynssaker (Sandefjord fra 2017-nå) - engangs historisk dump.

"Byggetilsynssak" er Sandefjord sitt eget navn på tilsyn/oppfølging av
mulig ulovlige tiltak (se common/scraper_lib.py sin modul-docstring) -
funksjonelt tilsvarende "Tilsynssak"/"Ulovlighetssak" andre steder i
prosjektet. MED vedlegg (samme datakilde som bygg).

Kjøremåter: se bygg/main.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "tilsyn_ny"
OUTPUT_FILE = Path(__file__).parent / "sandefjord_tilsyn.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
