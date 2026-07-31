"""Byggesaker (Ålesund fra 2024-nå) - daglig endringslogg.

KUN alesund_2024 - de fem andre datakildene (alesund_2020_2023,
alesund_2010_2019, skodje_hist, orskog_hist, sandoy_hist) er lukkede
historiske arkiv som aldri får nye saker.

`state.json` må bygges fra en fullført historisk dump
(alesund-2024-today_dump/bygg/main.py) via
`scraper_lib.build_state_from_dump(...)` før første kjøring - se
common/scraper_lib.py for bakgrunn om mekanismen (liste-så-filtrer,
30-dagersvindu, "seen"-dedup - samme mønster som Gjøvik/Moss/Sandefjord/
Tønsberg).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

KILDE = "alesund_2024"
HERE = Path(__file__).parent
STATE_FILE = HERE / "state.json"
OUTPUT_DIR = HERE / "output"

if __name__ == "__main__":
    run_daily(KILDE, STATE_FILE, OUTPUT_DIR)
