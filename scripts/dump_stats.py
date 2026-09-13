#!/usr/bin/env python3
"""Teller eiendomsreferanse-dekning i en eller flere postliste-dump-JSON-filer.

For hver fil: totalt antall saker, antall/andel med utfylt "adresse",
antall/andel med utfylt "gnr_bnr", og antall saker med INGEN eiendomsreferanse
i det hele tatt (adresse, gnr_bnr og matrikkelnr alle null/tomme).

Bruk:
    python3 scripts/dump_stats.py <fil1.json> [<fil2.json> ...]
    python3 scripts/dump_stats.py --scan   # finner automatisk alle "master"-
                                            # dump-filer under bronze/postlister/
                                            # (ekskluderer running_daily/output,
                                            # state.json, seen_*.json, connection*.json)

Output: én rad per fil, tabelform, pluss en JSON-fil (dump_stats.json) med
maskinlesbare tall du kan lime rett inn i HANDOVER.md.
"""
import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BRONZE = REPO_ROOT / "bronze" / "postlister"

# Filnavn/mønstre som IKKE er en autoritativ full-dump-fil, selv om de ligger
# et sted under bronze/postlister/ og ender på .json.
EXCLUDE_NAME_PATTERNS = (
    "state.json", "seen_", "connection", ".example.json", "config.json",
)
EXCLUDE_PATH_PARTS = ("running_daily", ".claude", "__pycache__")


def _is_master_dump(path: Path) -> bool:
    if any(p in path.parts for p in EXCLUDE_PATH_PARTS):
        return False
    if any(pat in path.name for pat in EXCLUDE_NAME_PATTERNS):
        return False
    return True


def find_master_dumps():
    return sorted(p for p in BRONZE.rglob("*.json") if _is_master_dump(p))


def _truthy(v):
    """None, tom streng, tom liste = "ikke utfylt". Alt annet = utfylt."""
    if v is None:
        return False
    if isinstance(v, str) and not v.strip():
        return False
    if isinstance(v, (list, dict)) and len(v) == 0:
        return False
    return True


def stats_for_file(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        return {"path": str(path), "error": str(e)}

    if isinstance(data, dict):
        records = list(data.values())
    elif isinstance(data, list):
        records = data
    else:
        return {"path": str(path), "error": f"uventet toppnivå-type: {type(data)}"}

    n = len(records)
    if n == 0:
        return {"path": str(path), "n": 0}

    n_adresse = sum(1 for r in records if isinstance(r, dict) and _truthy(r.get("adresse")))
    n_gnr_bnr = sum(1 for r in records if isinstance(r, dict) and _truthy(r.get("gnr_bnr")))
    n_matrikkelnr = sum(1 for r in records if isinstance(r, dict) and _truthy(r.get("matrikkelnr")))
    n_ingen = sum(
        1 for r in records
        if isinstance(r, dict)
        and not _truthy(r.get("adresse"))
        and not _truthy(r.get("gnr_bnr"))
        and not _truthy(r.get("matrikkelnr"))
    )

    return {
        "path": str(path.relative_to(REPO_ROOT)),
        "n": n,
        "n_adresse": n_adresse,
        "pct_adresse": round(100 * n_adresse / n, 1),
        "n_gnr_bnr": n_gnr_bnr,
        "pct_gnr_bnr": round(100 * n_gnr_bnr / n, 1),
        "n_matrikkelnr": n_matrikkelnr,
        "pct_matrikkelnr": round(100 * n_matrikkelnr / n, 1),
        "n_ingen_eiendomsreferanse": n_ingen,
        "pct_ingen_eiendomsreferanse": round(100 * n_ingen / n, 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="*", help="Enkeltfiler å telle")
    ap.add_argument("--scan", action="store_true", help="Finn alle master-dump-filer automatisk")
    ap.add_argument("--out", default=str(REPO_ROOT / "scripts" / "dump_stats.json"))
    args = ap.parse_args()

    if args.scan:
        files = find_master_dumps()
    elif args.files:
        files = [Path(f) for f in args.files]
    else:
        ap.error("gi enten filer eller --scan")

    results = [stats_for_file(f) for f in files]

    print(f"{'fil':<70} {'n':>7} {'adresse':>9} {'gnr/bnr':>9} {'matrikkel':>10} {'ingen ref':>10}")
    for r in results:
        if "error" in r:
            print(f"{r['path']:<70} FEIL: {r['error']}")
            continue
        if r.get("n", 0) == 0:
            print(f"{r['path']:<70} {'0 (tom)':>7}")
            continue
        print(f"{r['path']:<70} {r['n']:>7} "
              f"{r['n_adresse']:>4} ({r['pct_adresse']:>4.1f}%) "
              f"{r['n_gnr_bnr']:>4} ({r['pct_gnr_bnr']:>4.1f}%) "
              f"{r['n_matrikkelnr']:>5} ({r['pct_matrikkelnr']:>4.1f}%) "
              f"{r['n_ingen_eiendomsreferanse']:>5} ({r['pct_ingen_eiendomsreferanse']:>4.1f}%)")

    Path(args.out).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSkrev maskinlesbare tall til {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
