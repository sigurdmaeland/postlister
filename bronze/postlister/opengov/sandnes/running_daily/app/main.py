"""Sandnes - daglig endringslogg, alle fire sakstyper (wheel-pakket).

Byggesak/Henvendelse/Ulovlighetssak/Tilsynssak skrapes fra samme OpenGov-
portal med 100% identisk logikk (se app/lib/scraper_lib.py) - eneste
forskjell er søkeprefikset, se SAKSTYPER under.

Lokal testkjøring (fra denne pakkens rot, IKKE fra app/ - relativ import
krever at `app` importeres som pakke):

    cd bronze/postlister/opengov/sandnes/running_daily
    python3 -c "from app.main import trigger_bygg; trigger_bygg()"

    # eller med en grense for antall saker (rask test av én sakstype):
    python3 -c "from app.main import main; main('bygg', limit=30)"
"""
from pathlib import Path

from .lib.scraper_lib import run_daily

HERE = Path(__file__).parent

# Sakstype-nøkkel -> (søkeprefiks, sakstype-navn slik det brukes i
# scraper_lib/Azure-stier). Legg til en ny sakstype her + én trigger_*()
# funksjon + én linje i pyproject.toml/setup.py - IKKE et helt nytt
# wheel-prosjekt.
SAKSTYPER = {
    "bygg": {"prefiks": "BYGG-", "sakstype": "Byggesak"},
    "henv": {"prefiks": "HENV-", "sakstype": "Henvendelse"},
    "ulov": {"prefiks": "ULOV-", "sakstype": "Ulovlighetssak"},
    "tilsyn": {"prefiks": "TILSYN-", "sakstype": "Tilsynssak"},
}


def _snapshot_file(sakstype_key):
    # Én snapshot-fil per sakstype (lokal fallback/cache - Azure er primær,
    # se load_snapshot_azure). Navngitt per sakstype siden alle fire nå deler
    # samme app/-mappe (tidligere lå de naturlig adskilt i hver sin mappe).
    return HERE / f"snapshot_{sakstype_key}.json"


def main(sakstype_key, limit=None):
    """sakstype_key: en av nøklene i SAKSTYPER ('bygg'/'henv'/'ulov'/'tilsyn').
    limit: valgfritt antall nyeste saker å hente (kun for testing - full
    sweep ved None). Se run_daily()-docstringen i scraper_lib.py for hvorfor
    limit ALDRI påvirker Azure eller snapshotet."""
    cfg = SAKSTYPER[sakstype_key]
    run_daily(_snapshot_file(sakstype_key), cfg["prefiks"], cfg["sakstype"], limit)


def trigger_bygg():
    """Entry point for console_scripts / Databricks Python wheel-task (byggesaker)."""
    main("bygg")


def trigger_henv():
    """Entry point for console_scripts / Databricks Python wheel-task (henvendelser)."""
    main("henv")


def trigger_ulov():
    """Entry point for console_scripts / Databricks Python wheel-task (ulovlighetssaker)."""
    main("ulov")


def trigger_tilsyn():
    """Entry point for console_scripts / Databricks Python wheel-task (tilsynssaker)."""
    main("tilsyn")


if __name__ == "__main__":
    import sys
    # Valgfrie argumenter: sakstype-nøkkel, og evt. antall nyeste saker.
    #   python3 -m app.main bygg        -> full sweep for byggesaker
    #   python3 -m app.main bygg 30      -> kun de 30 nyeste byggesakene
    sakstype_key = sys.argv[1] if len(sys.argv) > 1 else "bygg"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    main(sakstype_key, limit=limit)
