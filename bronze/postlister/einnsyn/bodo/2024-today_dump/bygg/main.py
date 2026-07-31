"""Byggesaker (Bodø) - engangs historisk dump fra eInnsyn.

All faktisk skrape-/ryddelogikk bor i bodo/common/scraper_lib.py - denne
filen er bare konfigurasjon + inngangspunkt.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

HERE = Path(__file__).parent
OUTPUT_FILE = HERE / "bodo_bygg.json"
CHECKPOINT_FILE = HERE / "bodo_bygg_checkpoint.jsonl"  # JSONL, én sak per linje - trygt å avbryte/gjenoppta

if __name__ == "__main__":
    from datetime import date, timedelta

    # python3 main.py                          -> full dump fra START_DATE (2024-06-01)
    # python3 main.py 30                       -> kun siste 30 dager
    # python3 main.py 2024-06-01 2024-12-31    -> eksplisitt datointervall
    # python3 main.py --limit 500              -> maks 500 saker denne økten
    # python3 main.py --resume                 -> hopp over saker som alt er hentet.
    #     Billig for en flerårs-backfill, men oppdaterer da IKKE journalpost-
    #     lista på saker som allerede ligger i sjekkpunktfilen.
    args = sys.argv[1:]
    limit = None
    if "--limit" in args:
        i = args.index("--limit")
        limit = int(args[i + 1])
        args = args[:i] + args[i + 2:]
    resume = "--resume" in args
    args = [a for a in args if a != "--resume"]

    if len(args) == 2:
        run_full_dump(OUTPUT_FILE, CHECKPOINT_FILE,
                      start=date.fromisoformat(args[0]), end=date.fromisoformat(args[1]),
                      max_saker=limit, resume=resume)
    elif len(args) == 1:
        slutt = date.today()
        start = slutt - timedelta(days=int(args[0]) - 1)
        run_full_dump(OUTPUT_FILE, CHECKPOINT_FILE, start=start, end=slutt,
                      max_saker=limit, resume=resume)
    else:
        run_full_dump(OUTPUT_FILE, CHECKPOINT_FILE, max_saker=limit, resume=resume)
