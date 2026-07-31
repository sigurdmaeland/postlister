"""Byggesaker (Moss) - daglig endringslogg.

Se common/scraper_lib.py for bakgrunn om mekanismen (liste-så-filtrer,
30-dagersvindu, "seen"-dedup - samme mønster som Gjøvik) - denne filen er
bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "bygg"
HERE = Path(__file__).parent
STATE_FILE = HERE / "state.json"
OUTPUT_DIR = HERE / "output"

if __name__ == "__main__":
    run_daily(KILDE, STATE_FILE, OUTPUT_DIR)
