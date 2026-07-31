"""Byggetilsynssaker (Tønsberg fra 2020-nå) - daglig endringslogg.

Se running_daily/bygg/app/main.py for bakgrunn - denne filen er bare
konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "tilsyn_ny"
HERE = Path(__file__).parent
STATE_FILE = HERE / "state.json"
OUTPUT_DIR = HERE / "output"

if __name__ == "__main__":
    run_daily(KILDE, STATE_FILE, OUTPUT_DIR)
