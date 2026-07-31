"""Ulovlighetsoppfølging (Sandnes) - engangs historisk dump.

All faktisk skrape- og ryddelogikk bor i det delte biblioteket
sandnes/common/scraper_lib.py (deles med bygg/henv, og med running_daily) -
denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

SAKSNUMMER_PREFIKS = "ULOV-"   # brukes i "q="-søket for å hente ALLE ulovlighetssaker
OUTPUT_FILE = Path(__file__).parent / "sandnes_ulov.json"

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_full_dump(OUTPUT_FILE, SAKSNUMMER_PREFIKS, limit)
