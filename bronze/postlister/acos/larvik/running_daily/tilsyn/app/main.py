"""Tilsynssaker (Larvik) - daglig endringslogg.

Se common/scraper_lib.py for bakgrunn - denne kilden har svært få saker
totalt (se 2018-today_dump/tilsyn), så daglige kjøringer vil stort sett ikke
finne noe nytt. Tatt med for fullstendighet/konsistens med "bygg"-kilden.
state.json må finnes (bygget via build_state_from_dump()) før første
kjøring.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "tilsyn"
HERE = Path(__file__).parent
STATE_FILE = HERE / "state.json"
OUTPUT_DIR = HERE / "output"

if __name__ == "__main__":
    run_daily(KILDE, STATE_FILE, OUTPUT_DIR)
