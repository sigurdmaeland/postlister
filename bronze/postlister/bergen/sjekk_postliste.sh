#!/bin/bash
# Sjekk Bergens postliste direkte i terminalen (JSON-API, ingen skraping, ingen pip).
#
# Bruk:
#   bash sjekk_postliste.sh "Marmorneset"          # kort liste
#   bash sjekk_postliste.sh "Marmorneset" --filer  # + journalposter/filer
#   bash sjekk_postliste.sh "Fjellveien 15" --json # full JSON i terminalen
#
# Søketeksten sendes til /api/saker?tekst=... (adresse, gnr/bnr, saksnr, tittel).

python3 - "$@" <<'PY'
import sys, json, subprocess, base64, urllib.parse, datetime

API = "https://www.bergen.kommune.no/innsynplanogbyggesak/api"
ROWS = 15

def norm(s):
    # normaliser «krøll-anførselstegn» og trim, i tilfelle teksten er kopiert
    for a, b in [("“", ""), ("”", ""), ("‘", ""), ("’", ""), ('"', "")]:
        s = s.replace(a, b)
    return s.strip()

args = sys.argv[1:]
mode = "list"
terms = []
for a in args:
    if a == "--filer": mode = "filer"
    elif a == "--json": mode = "json"
    else: terms.append(norm(a))
q = " ".join(t for t in terms if t)

if not q:
    print('Bruk: bash sjekk_postliste.sh "<søketekst>" [--json | --filer]')
    sys.exit(0)

def curl(url):
    r = subprocess.run(["curl", "-s", "--fail", url], capture_output=True, text=True)
    return r.stdout

def ms(t):
    return datetime.date.fromtimestamp(t / 1000).isoformat() if t else None

raw = curl(f"{API}/saker?tekst={urllib.parse.quote(q)}&rows={ROWS}&orderBy=statusDato&asc=false")
if not raw:
    print("Fikk ikke kontakt med Bergen-API-et (curl returnerte tomt). Sjekk nett/proxy.",
          file=sys.stderr)
    sys.exit(1)
try:
    search = json.loads(raw)
except json.JSONDecodeError:
    print("Uventet svar fra API-et (ikke JSON).", file=sys.stderr)
    sys.exit(1)

items = search.get("items", [])

if not items:
    print(f"Ingen treff for '{q}'.", file=sys.stderr)
    sys.exit(0)

if mode != "json":
    print(f"Treff for '{q}': {search.get('numTotal')} totalt (viser {len(items)})")

def build_jp(saksnr):
    docs = json.loads(curl(f"{API}/dokumenter?saksnr={urllib.parse.quote(saksnr)}")).get("items", [])
    out = []
    for d in docs:
        filer = []
        for f in d.get("filer", []):
            fn = f.get("filnavn", "") or ""
            enc = urllib.parse.quote(fn.replace(".", "_").replace(",", "_").replace("/", "_"))
            p = base64.b64encode(f"/saksinnsyn/sak/{saksnr}".encode()).decode()
            url = (f"{API}/fil/{d.get('jnr')}/{f.get('kildeid')}/{enc}?p={p}"
                   if f.get("publisert") else None)
            filer.append({"filnavn": fn, "filtype": f.get("filtype"),
                          "publisert": f.get("publisert"), "url": url})
        out.append({"jp_nummer": d.get("dokumentnr"), "tittel": d.get("tittel"),
                    "type": d.get("type"), "dokdato": ms(d.get("dokdato")),
                    "journaldato": ms(d.get("journaldato")),
                    "avsendere": d.get("avsendere"), "mottakere": d.get("mottakere"),
                    "dokumenter": filer})
    return out

for it in items:
    saksnr = it.get("saksnr")
    if mode == "list":
        print()
        print("=" * 70)
        print(saksnr, "|", it.get("sakstypenavn"))
        print(it.get("tittel"))
        print("adresse:", ", ".join(it.get("adresse") or []), "| status:", it.get("status"))
    elif mode == "filer":
        print()
        print("=" * 70)
        print(saksnr, "-", it.get("tittel"))
        for jp in build_jp(saksnr):
            print("  •", jp["jp_nummer"], "-", jp["tittel"])
            for f in jp["dokumenter"]:
                print("      -", f["filnavn"], "(" + str(f["filtype"]) + ")")
    elif mode == "json":
        rec = {"kommune": "Bergen", "kommune_nr": "4601", "saksnr": saksnr,
               "sakstype": it.get("sakstypenavn"), "sakstittel": it.get("tittel"),
               "adresse": it.get("adresse"), "gnrBnr": it.get("gnrBnr"),
               "status": it.get("status"), "statusDato": ms(it.get("statusDato")),
               "soker": it.get("soker"), "tiltakshaver": it.get("tiltakshaver"),
               "journalposter": build_jp(saksnr)}
        rec["n_jp"] = len(rec["journalposter"])
        print(json.dumps(rec, ensure_ascii=False, indent=2))
PY
