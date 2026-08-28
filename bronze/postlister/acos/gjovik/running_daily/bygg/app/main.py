"""Byggesaker (Gjøvik) - daglig endringslogg (wheel-pakket).

Se app/lib/scraper_lib.py for bakgrunn om mekanismen (liste-så-filtrer,
30-dagersvindu, "seen"-dedup) - denne filen er bare konfigurasjon +
inngangspunkt. state.json må finnes (bygget via build_state_from_dump())
før første kjøring. Endringsloggen skrives ALDRI til lokal disk - kun til
Azure, se write_changelog()/upload_til_azure() i scraper_lib.py.

Lokal testkjøring (fra denne pakkens rot, IKKE fra app/ - relativ import
krever at `app` importeres som pakke):

    cd bronze/postlister/acos/gjovik/running_daily/bygg
    python3 -c "from app.main import trigger; trigger()"
"""
from pathlib import Path

from .lib.scraper_lib import run_daily

KILDE = "bygg"
HERE = Path(__file__).parent
STATE_FILE = HERE / "state.json"


def trigger():
    """Entry point for console_scripts / Databricks Python wheel-task."""
    run_daily(KILDE, STATE_FILE)


if __name__ == "__main__":
    trigger()
