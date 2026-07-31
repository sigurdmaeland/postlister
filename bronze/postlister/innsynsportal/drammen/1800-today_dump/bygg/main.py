"""Byggesaker (Drammen, historisk arkiv 1800-2020) - engangs historisk dump.

Seks hardkodede sub-arkiv fra Drammen + tidligere Nedre Eiker/Svelvik, egen
LIST_ID ("bygg_hist") - se drammen/common/scraper_lib.py (modul-docstring,
ARKIVER_HIST) for detaljer. I praksis lukket arkiv, ingen daglig
endringslogg. Denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump_hist  # noqa: E402

OUTPUT_FILE = Path(__file__).parent / "drammen_historisk.json"

if __name__ == "__main__":
    from datetime import date
    # Arkivene er for store til å hente i én kjøring - kjør i årsbolker som
    # bygger opp samme fil (merge=True er default):
    #   python3 main.py             -> full dump fra 1800 til i dag, alle 6 arkiver
    #   python3 main.py 2015        -> kun fra 2015 til i dag
    #   python3 main.py 1900 1950   -> kun årene 1900-1950
    if len(sys.argv) > 2:
        run_full_dump_hist(OUTPUT_FILE, start=date(int(sys.argv[1]), 1, 1), end=date(int(sys.argv[2]), 12, 31))
    elif len(sys.argv) > 1:
        run_full_dump_hist(OUTPUT_FILE, start=date(int(sys.argv[1]), 1, 1))
    else:
        run_full_dump_hist(OUTPUT_FILE)
