"""Byggesaker (tidligere Stokke kommune, 2012-2016) - engangs historisk dump.

Lukket arkiv fra før sammenslåingen med Sandefjord 01.01.2017 - INGEN
running_daily, INGEN vedlegg (må bestilles manuelt - se
common/scraper_lib.py sin modul-docstring). Kun sak- og
journalpost-metadata hentes.

NB: Stokke 1998-2011 (en femte, enda eldre datakilde i samme portal) er
BEVISST UTELATT etter eksplisitt beskjed fra brukeren.

Kjøremåter: se sandefjord/2017-today_dump/bygg/main.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "stokke"
OUTPUT_FILE = Path(__file__).parent / "stokke_bygg.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
