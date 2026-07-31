"""Henvendelser (Kristiansand) - daglig endringslogg.

Kristiansand sin OpenGov-portal har ingen datoer, API eller feed, så vi kan
ikke spørre "hva er endret siste døgn" slik Bergen/Stavanger gjør. I stedet
gjøres et fullt sveip av ALLE henvendelsene hver dag, og resultatet diffes mot
gårsdagens snapshot av journalpost-ID-er - selve mekanikken (full_sweep,
snapshot, diff, endringslogg, Azure-hook) bor i det delte biblioteket
kristiansand/common/scraper_lib.py (deles med bygg/ulov/tilsyn, og med
2020-today_dump) - denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

SAKSNUMMER_PREFIKS = "HENV-"
SAKSTYPE = "Henvendelse"

HERE = Path(__file__).parent
SNAPSHOT_FILE = HERE / "snapshot.json"   # {document_id: [journalpost_id, ...]}
OUTPUT_DIR = HERE / "output"             # dagens endringslogg havner her

if __name__ == "__main__":
    # Valgfritt argument: antall nyeste saker å hente (for rask testing).
    #   python3 main.py        -> full sweep (alle saker)
    #   python3 main.py 30      -> kun de 30 nyeste sakene
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_daily(SNAPSHOT_FILE, OUTPUT_DIR, SAKSNUMMER_PREFIKS, SAKSTYPE, limit)
