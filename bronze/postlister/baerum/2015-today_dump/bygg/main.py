import json
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Full-dump av Bærum-byggesaker fra portalen 2015-i dag (Acos Innsynpluss).
#
# Modell A:
#   1) CaseOnly  – alle saker (paginert)
#   2) DocOnly   – alle dokumenter (paginert), grupper på parentIdentifier
#   3) Vedlegg   – filer per dokument (/dokument/<id>)
#   4) Matrikkel – gnr/bnr fra adressen via Geonorge (Acos-API-et har det ikke)

BASE_URL = "https://innsynpluss.onacos.no"
API = BASE_URL + "/api/presentation/v2/nye-innsyn"
PORTAL_URL = BASE_URL + "/barum-gard/sok-i-plan-og-byggesaker-fra-2015/"

# Portal 2015-i dag
PORTAL_ID = "100"
MENYPUNKT_ID = "1631"
SAKSTYPE = "at-8de09341__cd12__4975__9460__e28961550ab5-BS!r1IrDy"

KOMMUNE = "Bærum"
KOMMUNE_NR = 3201      # Bærum (Akershus fra 2024; var 3024 i Viken 2020-2023)

# Geonorge adresse-API: geokoder adresse -> gnr/bnr (matrikkel finnes ikke i Acos-API-et)
GEONORGE_URL = "https://ws.geonorge.no/adresser/v1/sok"

HERE = Path(__file__).parent
OUTPUT_FILE = HERE / "baerum.json"

PAGE_SIZE = 100
MAX_WORKERS = 10       # parallelle vedlegg-/geokodings-kall
TIMEOUT = 60
RETRIES = 4
WITH_VEDLEGG = True    # sett False for rask metadata-only dump (uten fil-lenker)
WITH_MATRIKKEL = True  # slå opp gnr/bnr fra adressen via Geonorge
WITH_KORRESPONDANSE = True  # hent avsender+mottaker per journalpost via details (ett kall/sak)


def make_session():
    """Session med browser-UA + cookies + Acos-headere."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    })
    s.get(PORTAL_URL, timeout=TIMEOUT)   # session-cookies (anti-xsrf)
    s.headers.update({
        "PortalID": PORTAL_ID,
        "MenypunktID": MENYPUNKT_ID,
        "SprakID": "1",
        "X-ANTI-CSRF": "1",
        "Content-Type": "application/json",
        "Referer": PORTAL_URL,
        "Origin": BASE_URL,
    })
    return s


def _post(session, path, body):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.post(API + path, data=json.dumps(body), timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def _get(session, path):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.get(API + path, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def fetch_overview(session, filter_content, page, page_size=PAGE_SIZE, from_iso=None, to_iso=None):
    """Én side fra /overview for gitt FilterContent (CaseOnly / DocOnly)."""
    if from_iso and to_iso:
        dato_kv = [
            {"key": "Dato", "value": "Other"},
            {"key": "FromDate", "value": from_iso},
            {"key": "ToDate", "value": to_iso},
        ]
    else:
        dato_kv = [{"key": "Dato", "value": "AllCases"}]
    body = {"type": 0, "keyValues": dato_kv + [
        {"key": "FilterContent", "value": filter_content},
        {"key": "Sakstype", "value": SAKSTYPE},
        {"key": "page", "value": str(page)},
        {"key": "pageSize", "value": str(page_size)},
    ]}
    return _post(session, "/overview", body)["content"]["searchItems"]


def fetch_all_overview(session, filter_content, max_pages=None, from_iso=None, to_iso=None):
    """Paginer gjennom alle sider av /overview."""
    first = fetch_overview(session, filter_content, page=1, from_iso=from_iso, to_iso=to_iso)
    items = list(first["items"])
    page_count = first["pageCount"]
    if max_pages:
        page_count = min(page_count, max_pages)
    print(f"  {filter_content}: {first['totalItemCount']} totalt, {page_count} sider")
    for page in range(2, page_count + 1):
        items.extend(fetch_overview(session, filter_content, page=page, from_iso=from_iso, to_iso=to_iso)["items"])
        if page % 20 == 0:
            print(f"    {filter_content} side {page}/{page_count}")
    return items


def fetch_case_details(session, identifier):
    """Full sak fra /details/<id> – gir bl.a. fra/til (avsender/mottaker) per dokument."""
    try:
        return _get(session, "/details/" + quote(identifier, safe="")).get("content", {}).get("sak")
    except Exception:  # noqa: BLE001
        return None


def fetch_vedlegg(session, dokument_identifier):
    """Hent vedlegg (filer) for ett dokument via /dokument/<id>."""
    path = "/dokument/" + quote(dokument_identifier, safe="") + "?fromMeeting=false"
    try:
        model = (_get(session, path).get("content") or {}).get("model") or {}
    except Exception:  # noqa: BLE001
        return []
    vedlegg = []
    for gruppe in model.get("vedleggGruppe") or []:
        for v in gruppe.get("vedlegg") or []:
            fil_url = v.get("fileUrl") or ""
            vedlegg.append({
                "tittel": v.get("title"),
                "kategori": gruppe.get("title"),      # Hoveddokument / Vedlegg til saken
                "type": v.get("type"),                # H / V
                "filtype": v.get("filtype"),
                "filstorrelse": v.get("filstorrelseFormatted"),
                "synlighet": v.get("visibilityText"),
                "url": (BASE_URL + fil_url) if fil_url else None,
            })
    return vedlegg


# --------------------------------------------------------------------------- #
# Adresseparsing
# --------------------------------------------------------------------------- #
# Adressen står først i tittelen, skilt med ' - ' ELLER '–' (Acos bruker begge
# om hverandre). Herdet mot kjente Acos-Bærum-mønstre (samme type regex-basert
# rensing som Trondheim-scraperen bruker i sin extract_adresse):
#   - "34 A" -> "34A" (Geonorge takler begge, men vi normaliserer for konsistens)
#   - "–" (lang tankestrek) som skilletegn, ikke bare vanlig "-"
#   - "Bygg D"/"leilighetsbygg B"-etterheng fjernes før geokoding
#   - "Gate 3 og 7" / "Gate 2, 4, 6 og 8" -> flere enkelt-adresser, ikke ett
#     udelelig søk (Geonorge finner aldri "Gate 3 og 7" som helhet)
#   - "Gnr 41 bnr 80"-titler og kommunenavn ("Hurum kommune ...") gjenkjennes
#     som IKKE en gateadresse -> geokoding hoppes over, og gnr/bnr hentes i
#     stedet direkte fra teksten der det står eksplisitt (extract_gbnr_fra_tittel,
#     brukt som fallback i enrich_matrikkel)
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_TRAILING_BYGG = re.compile(r"\s+(?:leilighets)?bygg\s+[A-Za-zÆØÅæøå0-9]+\s*$", re.IGNORECASE)
_TRAILING_PAREN = re.compile(r"(?:\s*\([^)]*\))+\s*$")
_TRAILING_MFL = re.compile(r"\s+(?:m\.?\s*fl\.?|med\s+flere)\s*$", re.IGNORECASE)
_TRAILING_SNR = re.compile(r"[.,]?\s*snr\.?\s*\d+\s*$", re.IGNORECASE)
_KOMMUNE_NAVN = re.compile(r"^[A-ZÆØÅ][a-zæøå]+\s+kommune$", re.IGNORECASE)
_STARTER_MED_GBNR = re.compile(r"^g[/.]?(?:nr|bnr)\b", re.IGNORECASE)
_JUNK_LABELS = {"henvendelse", "tilsyn", "generell sak", "ingen adresse", "ingen registrering", "diverse"}
_GBNR_MØNSTER = re.compile(
    r"g[/.]?bnr\.?\s*(\d+)\s*/\s*(\d+)"
    r"|gnr\.?\s*(\d+)\s*b\.?nr\.?\s*(\d+)",
    re.IGNORECASE,
)

# Ett "husnummer-token": tall + evt. bokstav-suffiks, evt. bindestrek-spenn der
# andre enden enten er et nytt tall+bokstav ("13-25") ELLER bare en bokstav
# ("118B-N", flere oppganger/enheter på samme husnummer). Gatenavn+tall krever
# mellomrom foran tallet, slik at vi ikke kutter midt i et sammensatt gatenavn.
_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN = re.compile(rf"^{_TOKEN}$")
_BARE_LETTER = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_STREET_AND_NUMBER = re.compile(rf"^(.+?)\s+({_TOKEN})$")
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")
_HAR_TALL = re.compile(r"\d")
_KOMPONERT_SUFFIKS = re.compile(r"^(.+?)(vei|plass|gate|torg|sving|ring|alle|brygge|bakke|tun)$", re.IGNORECASE)
_RANGE_TOKEN = re.compile(r"^(\d+[A-Za-zæøåÆØÅ]?)\s*-\s*\d+[A-Za-zæøåÆØÅ]?$")
_LETTER_RANGE_TOKEN = re.compile(r"^(\d+[A-Za-zæøåÆØÅ])\s*-\s*[A-Za-zæøåÆØÅ]$")
# "Gate 36 - 38" (MELLOMROM rundt streken) er to distinkte eiendommer, i
# motsetning til et sammensatt tallspenn som "13-25" (ingen mellomrom) som kan
# dekke mange enheter og derfor IKKE splittes opp (se _RANGE_TOKEN-fallback i
# geocode_adresse i stedet).
_SPACED_PAIR = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+-\s+(\d+[A-Za-zæøåÆØÅ]?)$")


def _normaliser_nr_bokstav(s):
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _finn_hode(tittel):
    """Del tittelen ved FØRSTE '-'/'–' som ikke er del av et tallspenn ('13-25')
    eller en lengre tall-liste ('51 - 53 - 55 - 61'). Mellomrom rundt streken
    kan mangle på den ene siden (Acos er inkonsekvent: '58F -spørsmål',
    '51- spørsmål' forekommer begge). Ved en LENGRE tall-liste (3+ tall)
    beholdes bare gate+FØRSTE tall (samme forenkling som ved et rent tallspenn
    - vi geokoder én adresse, ikke alle i rekka); et enkelt tallspenn (kun to
    tall) beholdes intakt siden geocode_adresse allerede har egen fallback for det."""
    i = 0
    forste_slutt = None
    range_dash_count = 0
    pos = -1
    while True:
        pos_dash = tittel.find("-", i)
        pos_en = tittel.find("–", i)
        kandidater = [p for p in (pos_dash, pos_en) if p != -1]
        if not kandidater:
            pos = -1
            break
        pos = min(kandidater)
        left, right = tittel[:pos], tittel[pos + 1:]
        left_m = re.search(r"(\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ])\s*$", left)
        right_m = re.match(r"\s*(\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ])\b", right)
        if left_m and right_m:
            if forste_slutt is None:
                forste_slutt = left_m.end(1)
            range_dash_count += 1
            i = pos + 1
            continue
        break
    if pos == -1:
        return tittel.strip(), ""
    if range_dash_count >= 2:
        return tittel[:forste_slutt].strip(), tittel[pos:]
    return tittel[:pos].strip(), tittel[pos + 1:]


def _fjern_hengende_beskrivelse(head):
    head = _TRAILING_BYGG.sub("", head)
    head = _TRAILING_PAREN.sub("", head).strip()
    head = _TRAILING_MFL.sub("", head).strip()
    head = _TRAILING_SNR.sub("", head).strip()
    return head


def _er_soppel(head):
    """Kjente ikke-adresse-mønstre: kommunenavn, gnr/bnr-titler, rene stikkord."""
    if not head:
        return True
    if _KOMMUNE_NAVN.match(head) or _STARTER_MED_GBNR.match(head):
        return True
    if head.lower() in _JUNK_LABELS:
        return True
    return False


def _split_flere_adresser(candidate):
    """Del opp 'Gate 3 og 7' / 'Gate 2, 4, 6 og 8' i enkelt-adresser. Bindestrek-
    spenn ('13-25') beholdes som ett element. Bare tall/bokstav-elementer arver
    gatenavn (og evt. siste tall) fra forrige fulle element."""
    entries = []
    current_street, last_number = None, None
    tokens = _PART_SPLIT.split(candidate)
    parts = [(None, tokens[0])] + list(zip(tokens[1::2], tokens[2::2]))
    for sep, part in parts:
        part = part.strip()
        if not part:
            continue
        if _BARE_LETTER.match(part):
            if current_street is None or last_number is None:
                break
            entries.append(f"{current_street} {last_number}{part}")
            continue
        if _BARE_TOKEN.match(part):
            if current_street is None:
                break
            entries.append(f"{current_street} {part}")
            m = re.match(r"^(\d+)", part)
            if m:
                last_number = m.group(1)
            continue
        sm = _STREET_AND_NUMBER.match(part)
        if sm and (sep is None or _STARTS_UPPER.match(sm.group(1).strip())):
            current_street = sm.group(1).strip()
            entries.append(f"{current_street} {sm.group(2)}")
            m = re.match(r"^(\d+)", sm.group(2))
            if m:
                last_number = m.group(1)
            continue
        break
    seen, uniq = set(), []
    for e in entries:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq


def extract_adresser(sakstittel):
    """Liste med adresse-kandidater (flere ved 'og'/komma-oppramsing, eller ved
    et mellomrom-adskilt tallpar som "36 - 38"), eller [] hvis tittelen ikke
    inneholder noen ekte gateadresse."""
    if not sakstittel:
        return []
    head, _ = _finn_hode(sakstittel)
    head = _fjern_hengende_beskrivelse(head)
    if not head or _er_soppel(head) or not _HAR_TALL.search(head):
        return []
    candidate = _normaliser_nr_bokstav(head)
    par_m = _SPACED_PAIR.match(candidate)
    if par_m and _STARTS_UPPER.match(par_m.group(1).strip()):
        gate = par_m.group(1).strip()
        return [f"{gate} {par_m.group(2)}", f"{gate} {par_m.group(3)}"]
    return _split_flere_adresser(candidate)


def extract_adresse(sakstittel):
    """Enkelt-felt (bakoverkompatibelt): kandidatene slått sammen med '; '."""
    kandidater = extract_adresser(sakstittel)
    return "; ".join(kandidater) if kandidater else None


def extract_gbnr_fra_tittel(sakstittel):
    """Fanger opp 'gbnr 45/170' / 'gnr 45 bnr 170' direkte fra teksten - brukes
    som fallback når det ikke finnes noen geokodbar gateadresse i tittelen
    (f.eks. 'Hurum kommune - tilsyn ... på gbnr 45/170 - Tangen')."""
    if not sakstittel:
        return None
    m = _GBNR_MØNSTER.search(sakstittel)
    if not m:
        return None
    groups = [g for g in m.groups() if g is not None]
    if len(groups) != 2:
        return None
    return f"{groups[0]}/{groups[1]}"


def _geocode_kandidater(adresse):
    """Søkevarianter å prøve i rekkefølge: adressen som den er, deretter (hvis
    gatenavnet er satt sammen med et suffiks som normalt skal ha mellomrom
    foran, f.eks. '...vei'/'...plass' - Acos skriver iblant "Hans Øverlandsvei"
    i ett ord der Geonorge har "Hans Øverlands vei") en variant med mellomrom
    satt inn, og for tallspenn ('13-25') en variant med bare første tall."""
    yield adresse
    m = _STREET_AND_NUMBER.match(adresse)
    if not m:
        return
    gate, nr = m.group(1), m.group(2)
    sm = _KOMPONERT_SUFFIKS.match(gate)
    if sm:
        yield f"{sm.group(1)} {sm.group(2)} {nr}"
    rm = _RANGE_TOKEN.match(nr)
    if rm:
        yield f"{gate} {rm.group(1)}"
        if sm:
            yield f"{sm.group(1)} {sm.group(2)} {rm.group(1)}"
    lrm = _LETTER_RANGE_TOKEN.match(nr)
    if lrm:
        yield f"{gate} {lrm.group(1)}"
        if sm:
            yield f"{sm.group(1)} {sm.group(2)} {lrm.group(1)}"


def geocode_adresse(adresse):
    """Slår opp gnr/bnr for en adresse via Geonorge (avgrenset til Bærum)."""
    if not adresse:
        return None, None
    for query in _geocode_kandidater(adresse):
        for fuzzy in ("false", "true"):   # eksakt først, så fuzzy som fallback
            try:
                r = requests.get(GEONORGE_URL, params={
                    "sok": query,
                    "kommunenummer": str(KOMMUNE_NR),
                    "fuzzy": fuzzy,
                    "treffPerSide": 1,
                }, timeout=TIMEOUT)
                ad = r.json().get("adresser") or []
                if ad:
                    gnr, bnr = ad[0].get("gardsnummer"), ad[0].get("bruksnummer")
                    if gnr is not None and bnr is not None:
                        gnr_bnr = f"{gnr}/{bnr}"
                        return gnr_bnr, f"{KOMMUNE_NR}-{gnr_bnr}"
            except Exception:  # noqa: BLE001
                continue
    return None, None


def build_journalpost(doc_item):
    props = doc_item.get("properties", {})
    av = props.get("avsender")
    return {
        "identifier": doc_item.get("identifier"),
        "dokument_id": props.get("dokumentID"),
        "tittel": doc_item.get("title"),
        "type": doc_item.get("type"),          # Inngående/Utgående dokument osv.
        "dato": props.get("dato"),
        "avsender": [av] if av else [],        # fylles komplett fra details (fra)
        "mottaker": [],                        # fylles fra details (til)
        "vedlegg": [],
    }


def enrich_korrespondanse(session, saker):
    """Fyll avsender (fra) + mottaker (til) per journalpost via details-endepunktet."""
    med_jp = [s for s in saker if s["journalposter"]]
    print(f"Henter avsender/mottaker (details) for {len(med_jp)} saker…")

    def process(sak):
        det = fetch_case_details(session, sak["identifier"])
        if not det:
            return
        korr = {d.get("identifikator"): (d.get("fra") or [], d.get("til") or [])
                for d in det.get("dokumenter", [])}
        for jp in sak["journalposter"]:
            if jp["identifier"] in korr:
                jp["avsender"], jp["mottaker"] = korr[jp["identifier"]]

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(process, s) for s in med_jp]
        for _ in as_completed(futs):
            done += 1
            if done % 1000 == 0:
                print(f"    {done}/{len(med_jp)}")


def build_sak(case_item, journalposter):
    props = case_item.get("properties", {})
    ident = case_item.get("identifier")
    return {
        "identifier": ident,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": props.get("saksnummer"),
        "sakstype": case_item.get("type"),     # "Byggesak"
        "sakstittel": case_item.get("title"),
        "adresse": extract_adresse(case_item.get("title")),
        "gnr_bnr": None,                       # fylles av geokoding
        "matrikkelnr": None,                   # fylles av geokoding
        "status": case_item.get("status"),
        "dato": props.get("dato"),
        "url": PORTAL_URL + "#/details/" + ident if ident else None,
        "journalposter": journalposter,
    }


def save(saker):
    tmp = OUTPUT_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(OUTPUT_FILE)


def enrich_matrikkel(saker):
    """Geokod adressene (dedup - en sak kan ha flere ved 'og'/komma-oppramsing
    i tittelen) og sett gnr_bnr/matrikkelnr. Faller tilbake til gnr/bnr nevnt
    direkte i sakstittelen (f.eks. "... gbnr 45/170 ...") når det ikke finnes
    noen geokodbar gateadresse i det hele tatt."""
    unike = set()
    for s in saker:
        for kandidat in (s.get("adresse") or "").split("; "):
            kandidat = kandidat.strip()
            if kandidat:
                unike.add(kandidat)
    print(f"Geokoder {len(unike)} unike adresser (Geonorge)…")
    oppslag = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(geocode_adresse, a): a for a in unike}
        done = 0
        for fut in as_completed(futs):
            oppslag[futs[fut]] = fut.result()
            done += 1
            if done % 2000 == 0:
                print(f"    geokodet {done}/{len(unike)}")
    for s in saker:
        kandidater = [k.strip() for k in (s.get("adresse") or "").split("; ") if k.strip()]
        treff = [oppslag.get(k) for k in kandidater]
        gnr_bnr_liste = [t[0] for t in treff if t and t[0]]
        matrikkel_liste = [t[1] for t in treff if t and t[1]]
        if gnr_bnr_liste:
            s["gnr_bnr"] = "; ".join(dict.fromkeys(gnr_bnr_liste))
            s["matrikkelnr"] = "; ".join(dict.fromkeys(matrikkel_liste))
        else:
            direkte = extract_gbnr_fra_tittel(s.get("sakstittel"))
            if direkte:
                s["gnr_bnr"] = direkte
                s["matrikkelnr"] = f"{KOMMUNE_NR}-{direkte}"
    traff = sum(1 for s in saker if s.get("gnr_bnr"))
    print(f"    fant gnr/bnr for {traff}/{len(saker)} saker")


def run(max_pages=None, start=None, end=None):
    session = make_session()
    t0 = time.time()

    from_iso = start.isoformat() if start else None
    to_iso = end.isoformat() if end else None
    if from_iso and to_iso:
        print(f"Datovindu: {from_iso} – {to_iso}")

    print("Henter saker (CaseOnly)…")
    case_items = fetch_all_overview(session, "CaseOnly", max_pages=max_pages, from_iso=from_iso, to_iso=to_iso)

    print("Henter dokumenter (DocOnly)…")
    doc_items = fetch_all_overview(session, "DocOnly", max_pages=max_pages, from_iso=from_iso, to_iso=to_iso)
    docs_by_case = defaultdict(list)
    for d in doc_items:
        docs_by_case[d.get("parentIdentifier")].append(d)

    saker = []
    all_docs = []
    for case in case_items:
        jps = [build_journalpost(d) for d in docs_by_case.get(case.get("identifier"), [])]
        saker.append(build_sak(case, jps))
        all_docs.extend(jp for jp in jps if jp["identifier"])
    print(f"Satte sammen {len(saker)} saker, {len(all_docs)} journalposter")

    if WITH_MATRIKKEL:
        enrich_matrikkel(saker)

    if WITH_KORRESPONDANSE:
        enrich_korrespondanse(session, saker)

    if WITH_VEDLEGG and all_docs:
        print(f"Henter vedlegg for {len(all_docs)} dokumenter…")
        done = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(fetch_vedlegg, session, jp["identifier"]): jp for jp in all_docs}
            for fut in as_completed(futs):
                futs[fut]["vedlegg"] = fut.result()
                done += 1
                if done % 500 == 0:
                    save(saker)
                    print(f"    vedlegg {done}/{len(all_docs)} (lagret)")

    save(saker)
    n_ved = sum(len(jp["vedlegg"]) for jp in all_docs)
    print(f"Ferdig: {len(saker)} saker, {len(all_docs)} journalposter, "
          f"{n_ved} vedlegg -> {OUTPUT_FILE} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    import sys
    # Valgfrie argumenter:
    #   python3 main.py                        -> full dump fra 2015
    #   python3 main.py 30                      -> kun siste 30 dager
    #   python3 main.py 2023-01-01 2023-03-31   -> eksplisitt datointervall (start slutt)
    if len(sys.argv) == 3:
        run(start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    elif len(sys.argv) == 2:
        n_days = int(sys.argv[1])
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        start = end - timedelta(days=n_days - 1)
        run(start=start, end=end)
    else:
        run()
