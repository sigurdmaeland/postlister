"""Byggesaker (gamle Sandefjord kommune, 2001-2016) - engangs historisk dump.

Lukket arkiv (forrige saksbehandlingssystem, før dagens innsynsportal) -
INGEN running_daily (får aldri nye saker), og INGEN vedlegg (vedlegg i
dette arkivet må bestilles manuelt hos kommunen, er ikke ment å være
maskinelt nedlastbare - se common/scraper_lib.py sin modul-docstring).
Kun sak- og journalpost-metadata hentes.

Kjøremåter: se sandefjord/2017-today_dump/bygg/main.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "bygg_2001_2016"
OUTPUT_FILE = Path(__file__).parent / "sandefjord_bygg_2001_2016.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
