"""Byggesaker (tidligere Re kommune, 2006-2019) - engangs historisk dump.

Lukket arkiv fra før sammenslåingen med Tønsberg 01.01.2020 - eget separat
arkiv fra en helt annen tidligere kommune enn tonsberg-2006-2019_dump,
selv om periodene overlapper (se common/scraper_lib.py sin modul-
docstring). INGEN running_daily (arkivet får aldri nye saker). Har
bekreftet offentlig nedlastbare vedlegg - hentes med hent_filer=True.

Kjøremåter: se ../2020-today_dump/bygg/main.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import run_full_dump  # noqa: E402

KILDE = "re_hist"
OUTPUT_FILE = Path(__file__).parent / "re_hist_bygg.json"

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        run_full_dump(KILDE, OUTPUT_FILE, from_date=args[0], to_date=args[1])
    elif len(args) == 1:
        run_full_dump(KILDE, OUTPUT_FILE, limit=int(args[0]))
    else:
        run_full_dump(KILDE, OUTPUT_FILE)
