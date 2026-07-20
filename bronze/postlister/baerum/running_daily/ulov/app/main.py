import json
import re
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Daglig endringslogg for Bærum-ulovlighetsoppfølgingssaker (Acos Innsynpluss,
# portal 2015-i dag). Samme oppsett som byggesak sin running_daily, bare med
# Sakstype-filter for Ulovlighetsoppfølging i stedet for Byggesak.
#
# Bærum har ekte datoer, så dette er dato-basert (som Bergen/Stavanger) – ingen
# snapshot trengs. Dato-filteret på DocOnly fanger nye dokumenter uansett hvor
# gammel saken er, så endringsloggen blir komplett:
#   - nye saker            = CaseOnly i datovinduet
#   - nye journalposter    = DocOnly i datovinduet som hører til eldre saker
#
# Kun 2015-i dag overvåkes daglig. Den historiske 2003-2015-portalen er et frossent
# arkiv (nyeste dokument mars 2015) som aldri får ny aktivitet – den har derfor bare
# en full-dump, ingen running_daily.
#
# Produksjonsherding (lagt til for å sikre nøyaktige data mot fremtidig Azure-opplasting):
#   - state.json husker siste vellykkede dag + hvilke identifiers som allerede er
#     rapportert -> auto-backfiller hull (f.eks. maskin var av en dag) i stedet for
#     at data stille forsvinner, og forhindrer duplikater fra det overlappende vinduet.
#   - WINDOW_DAYS=3 gir 2 dagers overlapp, som forsikring mot at Acos indekserer en
#     sak/dokument noen dager forsinket i forhold til "dato"-feltet.
#   - Feil under en dags kjøring stopper backfillen og gir exit-kode 1 + logglinje,
#     i stedet for å stille hoppe over dagen (som ville sett ut som "ingen aktivitet").

BASE_URL = "https://innsynpluss.onacos.no"
API = BASE_URL + "/api/presentation/v2/nye-innsyn"
PORTAL_URL = BASE_URL + "/barum-gard/sok-i-plan-og-byggesaker-fra-2015/"

# Portal 2015-i dag
PORTAL_ID = "100"
MENYPUNKT_ID = "1631"
SAKSTYPE = "at-8de09341__cd12__4975__9460__e28961550ab5-UFO!PHbMQ6"   # Ulovlighetsoppfølging

KOMMUNE = "Bærum"
KOMMUNE_NR = 3201

GEONORGE_URL = "https://ws.geonorge.no/adresser/v1/sok"

HERE = Path(__file__).parent
OUTPUT_DIR = HERE / "output"
STATE_FILE = HERE / "state.json"
LOG_FILE = HERE / "run.log"

PAGE_SIZE = 100
MAX_WORKERS = 10
TIMEOUT = 60
RETRIES = 4
WINDOW_DAYS = 3             # dager bakover fra referansedatoen (overlapper mot forsinket indeksering)
MAX_BACKFILL_DAYS = 30      # sikkerhetsgrense: aldri auto-backfill lenger enn dette i én kjøring
SEEN_RETENTION_DAYS = 14    # hvor lenge vi husker identifiers for dedup (must be >= WINDOW_DAYS)
WITH_VEDLEGG = True
WITH_MATRIKKEL = True
WITH_KORRESPONDANSE = True  # avsender+mottaker per journalpost via details


# --------------------------------------------------------------------------- #
# Logging + tilstand
# --------------------------------------------------------------------------- #
def log(msg):
    line = f"[{datetime.now(ZoneInfo('Europe/Oslo')).isoformat(timespec='seconds')}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_state():
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            state = {}
    else:
        state = {}
    state.setdefault("last_success_date", None)
    state.setdefault("seen_saker", {})
    state.setdefault("seen_journalposter", {})
    return state


def save_state(state):
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def prune_seen(state, today):
    """Fjern gamle identifiers fra dedup-hukommelsen (trengs bare for overlappende vindu)."""
    cutoff = (today - timedelta(days=SEEN_RETENTION_DAYS)).isoformat()
    state["seen_saker"] = {k: v for k, v in state["seen_saker"].items() if v >= cutoff}
    state["seen_journalposter"] = {k: v for k, v in state["seen_journalposter"].items() if v >= cutoff}


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    })
    s.get(PORTAL_URL, timeout=TIMEOUT)   # session-cookies (anti-xsrf)
    s.headers.update({
        "PortalID": PORTAL_ID, "MenypunktID": MENYPUNKT_ID, "SprakID": "1",
        "X-ANTI-CSRF": "1", "Content-Type": "application/json",
        "Referer": PORTAL_URL, "Origin": BASE_URL,
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


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def fetch_overview_window(session, filter_content, from_iso, to_iso):
    """Alle sider fra /overview i datovinduet [from_iso, to_iso] (ISO yyyy-mm-dd)."""
    def page(n):
        body = {"type": 0, "keyValues": [
            {"key": "Dato", "value": "Other"},
            {"key": "FromDate", "value": from_iso},
            {"key": "ToDate", "value": to_iso},
            {"key": "FilterContent", "value": filter_content},
            {"key": "Sakstype", "value": SAKSTYPE},
            {"key": "page", "value": str(n)},
            {"key": "pageSize", "value": str(PAGE_SIZE)},
        ]}
        return _post(session, "/overview", body)["content"]["searchItems"]

    first = page(1)
    items = list(first["items"])
    for n in range(2, first["pageCount"] + 1):
        items.extend(page(n)["items"])
    return items


def fetch_case_details(session, identifier):
    try:
        return _get(session, "/details/" + quote(identifier, safe="")).get("content", {}).get("sak")
    except Exception:  # noqa: BLE001
        return None


def fetch_details_parallel(session, identifiers):
    """Hent /details for flere saker samtidig. Returnerer dict identifier -> sak (eller None)."""
    resultater = {}
    if not identifiers:
        return resultater
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_case_details, session, ident): ident for ident in identifiers}
        for fut in as_completed(futs):
            resultater[futs[fut]] = fut.result()
    return resultater


def fetch_vedlegg(session, dokument_identifier):
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
                "kategori": gruppe.get("title"),
                "type": v.get("type"),
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
    if not adresse:
        return None, None
    for query in _geocode_kandidater(adresse):
        for fuzzy in ("false", "true"):
            try:
                r = requests.get(GEONORGE_URL, params={
                    "sok": query, "kommunenummer": str(KOMMUNE_NR),
                    "fuzzy": fuzzy, "treffPerSide": 1,
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


# --------------------------------------------------------------------------- #
# Bygging
# --------------------------------------------------------------------------- #


def build_journalpost(doc_item):
    props = doc_item.get("properties", {})
    av = props.get("avsender")
    return {
        "identifier": doc_item.get("identifier"),
        "dokument_id": props.get("dokumentID"),
        "tittel": doc_item.get("title"),
        "type": doc_item.get("type"),
        "dato": props.get("dato"),
        "avsender": [av] if av else [],        # fylles komplett fra details (fra)
        "mottaker": [],                        # fylles fra details (til)
        "vedlegg": [],
    }


def fill_korrespondanse(det, journalposter):
    """Fyll avsender (fra) + mottaker (til) på journalpostene fra en details-sak."""
    if not det:
        return
    korr = {d.get("identifikator"): (d.get("fra") or [], d.get("til") or [])
            for d in det.get("dokumenter", [])}
    for jp in journalposter:
        if jp["identifier"] in korr:
            jp["avsender"], jp["mottaker"] = korr[jp["identifier"]]


def build_sak(case_item, journalposter):
    props = case_item.get("properties", {})
    ident = case_item.get("identifier")
    return {
        "identifier": ident,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": props.get("saksnummer"),
        "sakstype": case_item.get("type"),     # "Ulovlighetsoppfølging"
        "sakstittel": case_item.get("title"),
        "adresse": extract_adresse(case_item.get("title")),
        "gnr_bnr": None,
        "matrikkelnr": None,
        "status": case_item.get("status"),
        "dato": props.get("dato"),
        "url": PORTAL_URL + "#/details/" + ident if ident else None,
        "journalposter": journalposter,
    }


def parent_sak_ref(identifier, sak):
    """Metadata om en eldre forelder-sak (som fikk en ny journalpost). sak = ferdig hentet details."""
    tittel = sak.get("tittel") if sak else None
    return {
        "identifier": identifier,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": sak.get("saksnummer") if sak else None,
        "sakstittel": tittel,
        "adresse": extract_adresse(tittel),
        "gnr_bnr": None,
        "matrikkelnr": None,
        "url": PORTAL_URL + "#/details/" + identifier,
    }


# --------------------------------------------------------------------------- #
# Berikelse
# --------------------------------------------------------------------------- #
def enrich_matrikkel(objekter):
    """Geokod adressene (dedup - en sak kan ha flere ved 'og'/komma-oppramsing
    i tittelen) og sett gnr_bnr/matrikkelnr. Faller tilbake til gnr/bnr nevnt
    direkte i sakstittelen (f.eks. "... gbnr 45/170 ...") når det ikke finnes
    noen geokodbar gateadresse i det hele tatt."""
    if not WITH_MATRIKKEL:
        return
    unike = set()
    for o in objekter:
        for kandidat in (o.get("adresse") or "").split("; "):
            kandidat = kandidat.strip()
            if kandidat:
                unike.add(kandidat)
    oppslag = {}
    if unike:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(geocode_adresse, a): a for a in unike}
            for fut in as_completed(futs):
                oppslag[futs[fut]] = fut.result()
    for o in objekter:
        kandidater = [k.strip() for k in (o.get("adresse") or "").split("; ") if k.strip()]
        treff = [oppslag.get(k) for k in kandidater]
        gnr_bnr_liste = [t[0] for t in treff if t and t[0]]
        matrikkel_liste = [t[1] for t in treff if t and t[1]]
        if gnr_bnr_liste:
            o["gnr_bnr"] = "; ".join(dict.fromkeys(gnr_bnr_liste))
            o["matrikkelnr"] = "; ".join(dict.fromkeys(matrikkel_liste))
        else:
            direkte = extract_gbnr_fra_tittel(o.get("sakstittel"))
            if direkte:
                o["gnr_bnr"] = direkte
                o["matrikkelnr"] = f"{KOMMUNE_NR}-{direkte}"


def enrich_vedlegg(session, journalposter):
    if not WITH_VEDLEGG or not journalposter:
        return
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_vedlegg, session, jp["identifier"]): jp
                for jp in journalposter if jp.get("identifier")}
        for fut in as_completed(futs):
            futs[fut]["vedlegg"] = fut.result()


# --------------------------------------------------------------------------- #
# Kjøring
# --------------------------------------------------------------------------- #
def write_output(nye_saker, nye_journalposter, ref_date):
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"saker_{ref_date}.json").write_text(
        json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUTPUT_DIR / f"journalposter_{ref_date}.json").write_text(
        json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Skrev {len(nye_saker)} nye saker og "
        f"{len(nye_journalposter)} saker med nye journalposter -> {OUTPUT_DIR}")
    # TODO: last opp som .jsonl.gz til Azure (bronze/baerum/load_type=incremental/date=<ref>/)


def run_for_date(session, ref_date, state):
    """Kjør datovinduet [ref_date - (WINDOW_DAYS-1), ref_date], dedupliser mot state,
    skriv output, og oppdater state med det som nettopp ble rapportert."""
    from_iso = (ref_date - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    to_iso = ref_date.isoformat()
    log(f"Datovindu: {from_iso} .. {to_iso}")

    # 1) Nye saker + nye dokumenter i (det overlappende) vinduet
    case_items = fetch_overview_window(session, "CaseOnly", from_iso, to_iso)
    doc_items = fetch_overview_window(session, "DocOnly", from_iso, to_iso)
    log(f"  {len(case_items)} saker, {len(doc_items)} dokumenter i rå-vinduet (før dedup)")

    docs_by_case = defaultdict(list)
    for d in doc_items:
        docs_by_case[d.get("parentIdentifier")].append(d)

    seen_saker = state["seen_saker"]
    seen_jp = state["seen_journalposter"]

    # En sak regnes som "ny" kun hvis vi aldri har rapportert den før (ikke bare fordi
    # den fortsatt ligger i det overlappende vinduet).
    alle_case_ids = {c.get("identifier") for c in case_items if c.get("identifier")}
    nye_sak_ids = {cid for cid in alle_case_ids if cid not in seen_saker}

    # 2) Nye saker (med sine journalposter fra vinduet, dedup på journalpost-nivå)
    nye_med_jp_ids = [
        cid for cid in nye_sak_ids
        if any(d.get("identifier") not in seen_jp for d in docs_by_case.get(cid, []))
    ]
    detaljer_nye = (fetch_details_parallel(session, nye_med_jp_ids)
                     if WITH_KORRESPONDANSE and nye_med_jp_ids else {})

    nye_saker, alle_jp = [], []
    for c in case_items:
        cid = c.get("identifier")
        if cid not in nye_sak_ids:
            continue
        docs = [d for d in docs_by_case.get(cid, []) if d.get("identifier") not in seen_jp]
        jps = [build_journalpost(d) for d in docs]
        if WITH_KORRESPONDANSE and jps:
            fill_korrespondanse(detaljer_nye.get(cid), jps)
        nye_saker.append(build_sak(c, jps))
        alle_jp.extend(jps)

    # 3) Nye journalposter på eldre/allerede-rapporterte saker (dedup på journalpost-nivå)
    gamle_parent_ids = [
        pid for pid in docs_by_case
        if pid and pid not in nye_sak_ids
        and any(d.get("identifier") not in seen_jp for d in docs_by_case[pid])
    ]
    detaljer_gamle = fetch_details_parallel(session, gamle_parent_ids)

    nye_journalposter = []
    for parent_id in gamle_parent_ids:
        docs = [d for d in docs_by_case[parent_id] if d.get("identifier") not in seen_jp]
        if not docs:
            continue
        det = detaljer_gamle.get(parent_id)
        jps = [build_journalpost(d) for d in docs]
        if WITH_KORRESPONDANSE:
            fill_korrespondanse(det, jps)
        nye_journalposter.append({
            "parent_sak": parent_sak_ref(parent_id, det),
            "nye_journalposter": jps,
        })
        alle_jp.extend(jps)

    # 4) Berik: gnr/bnr (nye saker + forelder-saker) og vedlegg (alle journalposter)
    enrich_matrikkel(nye_saker + [x["parent_sak"] for x in nye_journalposter])
    enrich_vedlegg(session, alle_jp)

    write_output(nye_saker, nye_journalposter, to_iso)

    # 5) Marker det vi nettopp skrev som "sett" (hindrer duplikater neste gang vinduet overlapper)
    for s in nye_saker:
        seen_saker[s["identifier"]] = to_iso
        for jp in s["journalposter"]:
            if jp.get("identifier"):
                seen_jp[jp["identifier"]] = to_iso
    for entry in nye_journalposter:
        for jp in entry["nye_journalposter"]:
            if jp.get("identifier"):
                seen_jp[jp["identifier"]] = to_iso

    return len(nye_saker), len(nye_journalposter)


def main(day_back=1):
    state = load_state()
    today = datetime.now(ZoneInfo("Europe/Oslo")).date()
    target = today - timedelta(days=day_back)

    if state["last_success_date"]:
        start = datetime.fromisoformat(state["last_success_date"]).date() + timedelta(days=1)
    else:
        start = target  # ingen kjent historie ennå -- ikke gjett, bare kjør den etterspurte dagen

    if start > target:
        log(f"Ingenting å gjøre – siste vellykkede dag ({state['last_success_date']}) "
            f"er allerede >= mål ({target}).")
        return

    dager = []
    d = start
    while d <= target:
        dager.append(d)
        d += timedelta(days=1)

    if len(dager) > MAX_BACKFILL_DAYS:
        log(f"ADVARSEL: {len(dager)} dager mangler siden siste kjøring – begrenser "
            f"auto-backfill til de {MAX_BACKFILL_DAYS} nyeste. Kjør eldre datoer manuelt "
            f"med day_back om nødvendig.")
        dager = dager[-MAX_BACKFILL_DAYS:]

    if len(dager) > 1:
        log(f"Backfiller {len(dager)} manglende dag(er): {dager[0]} .. {dager[-1]}")

    session = make_session()
    for ref_date in dager:
        try:
            n_saker, n_jp = run_for_date(session, ref_date, state)
            state["last_success_date"] = ref_date.isoformat()
            prune_seen(state, today)
            save_state(state)   # lagre fremgang etter HVER dag, ikke bare til slutt
            log(f"OK {ref_date}: {n_saker} nye saker, {n_jp} saker med nye journalposter")
        except Exception:  # noqa: BLE001
            log(f"FEIL under kjøring for {ref_date}:\n{traceback.format_exc()}")
            log(f"Stopper her. Siste vellykkede dag forblir {state['last_success_date']}.")
            sys.exit(1)


if __name__ == "__main__":
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    main(day_back=db)
