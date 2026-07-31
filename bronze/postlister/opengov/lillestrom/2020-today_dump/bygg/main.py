"""Byggesaker (Lillestrøm) - engangs historisk dump.

All faktisk skrape- og ryddelogikk bor i det delte biblioteket
lillestrom/common/scraper_lib.py (deles med henv/ulov/tilsyn, og med
running_daily) - denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

SAKSNUMMER_PREFIKS = "BYGG-"   # brukes i "q="-søket for å hente ALLE byggesaker
OUTPUT_FILE = Path(__file__).parent / "lillestrom.json"

if __name__ == "__main__":
    # Valgfritt argument: antall nyeste saker å hente (for rask testing/delkjøring).
    #   python3 main.py        -> full dump (alle saker)
    #   python3 main.py 200    -> kun de 200 nyeste sakene
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_full_dump(OUTPUT_FILE, SAKSNUMMER_PREFIKS, limit)
