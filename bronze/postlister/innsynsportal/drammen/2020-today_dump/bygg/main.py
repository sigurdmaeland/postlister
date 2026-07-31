"""Byggesaker (Drammen, aktiv lista) - engangs historisk dump.

All faktisk skrape-/ryddelogikk bor i drammen/common/scraper_lib.py - denne
filen er bare konfigurasjon + inngangspunkt. Se der for det historiske
arkivet (1800-today_dump), som er en helt egen LIST_ID/kilde ("bygg_hist").
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "bygg"
OUTPUT_FILE = Path(__file__).parent / "drammen.json"

if __name__ == "__main__":
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    # Valgfritt argument: antall dager bakover fra i dag (for rask testing).
    #   python3 main.py       -> full dump fra 2020-01-01
    #   python3 main.py 30    -> kun siste 30 dager
    if len(sys.argv) > 1:
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        run_full_dump(KILDE, OUTPUT_FILE, start=end - timedelta(days=int(sys.argv[1]) - 1), end=end)
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
