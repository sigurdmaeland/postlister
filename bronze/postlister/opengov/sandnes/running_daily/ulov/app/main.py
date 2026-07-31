"""Ulovlighetsoppfølging (Sandnes) - daglig endringslogg.

Se running_daily/bygg/app/main.py for bakgrunn om mekanismen (full sveip +
snapshot-diff) - denne filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from scraper_lib import run_daily  # noqa: E402

SAKSNUMMER_PREFIKS = "ULOV-"
SAKSTYPE = "Ulovlighetssak"

HERE = Path(__file__).parent
SNAPSHOT_FILE = HERE / "snapshot.json"
OUTPUT_DIR = HERE / "output"

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_daily(SNAPSHOT_FILE, OUTPUT_DIR, SAKSNUMMER_PREFIKS, SAKSTYPE, limit)
