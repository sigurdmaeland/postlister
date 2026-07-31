"""Byggesaker (Sandnes) - daglig endringslogg.

Sandnes' OpenGov-portal har ingen datoer, API eller feed, så vi kan ikke
spørre "hva er endret siste døgn". I stedet gjøres et fullt sveip av ALLE
byggesakene hver dag, og resultatet diffes mot gårsdagens snapshot av
journalpost-ID-er - selve mekanikken (full_sweep, snapshot, diff,
endringslogg, Azure-hook) bor i det delte biblioteket
sandnes/common/scraper_lib.py (deles med ulov/henv, og med
2022-today_dump) - denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

SAKSNUMMER_PREFIKS = "BYGG-"
SAKSTYPE = "Byggesak"

HERE = Path(__file__).parent
SNAPSHOT_FILE = HERE / "snapshot.json"   # {document_id: [journalpost_id, ...]}
OUTPUT_DIR = HERE / "output"             # dagens endringslogg havner her

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_daily(SNAPSHOT_FILE, OUTPUT_DIR, SAKSNUMMER_PREFIKS, SAKSTYPE, limit)
