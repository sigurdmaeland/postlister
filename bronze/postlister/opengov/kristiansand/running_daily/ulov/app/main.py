"""Ulovlighetssaker (Kristiansand) - daglig endringslogg (wheel-pakket).

Kristiansand sin OpenGov-portal har ingen datoer, API eller feed, så vi kan
ikke spørre "hva er endret siste døgn" slik Bergen/Stavanger gjør. I stedet
gjøres et fullt sveip av ALLE ulovlighetssakene hver dag, og resultatet
diffes mot gårsdagens snapshot av journalpost-ID-er (lest fra/skrevet til
Azure Blob som kilde til sannhet - se README.md).

Denne filen er pakket som en wheel for Databricks (matcher konvensjonen fra
easy-pipelines/Trondheim): `app/lib/scraper_lib.py` er en selvstendig kopi av
../../common/scraper_lib.py (delt med bygg/henv/tilsyn, og med 2020-today_dump -
oppdater alle kopiene samtidig ved endringer i kjernelogikken).

Lokal testkjøring (fra denne pakkens rot, IKKE fra app/ - relativ import
krever at `app` importeres som pakke):

    cd bronze/postlister/opengov/kristiansand/running_daily/ulov
    python3 -c "from app.main import trigger; trigger()"

    # eller med et grense for antall saker (rask test):
    python3 -c "from app.main import main; main(limit=30)"
"""
from pathlib import Path

from .lib.scraper_lib import run_daily

SAKSNUMMER_PREFIKS = "ULOV-"
SAKSTYPE = "Ulovlighetssak"

HERE = Path(__file__).parent
SNAPSHOT_FILE = HERE / "snapshot.json"   # {document_id: [journalpost_id, ...]} - lokal fallback/cache, Azure er primær
OUTPUT_DIR = HERE / "output"             # dagens endringslogg havner her (kun lokalt, informativt)


def main(limit=None):
    """limit: valgfritt antall nyeste saker å hente (kun for testing - full sweep ved None)."""
    run_daily(SNAPSHOT_FILE, OUTPUT_DIR, SAKSNUMMER_PREFIKS, SAKSTYPE, limit)


def trigger():
    """Entry point for console_scripts / Databricks Python wheel-task."""
    main()


if __name__ == "__main__":
    trigger()
