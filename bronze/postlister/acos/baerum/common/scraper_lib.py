"""Bærum - ACOS "Innsynpluss" (samme ACOS "nye-innsyn"-API som Gjøvik/Moss/
Sandefjord/Tønsberg, men på en delt ACOS-tenant, innsynpluss.onacos.no, i
stedet for en selvhostet instans). Samme seksjonsoppdeling som de andre
ACOS-kommunene: konstanter -> adresseparsing -> KILDER -> API-kall ->
engangsdump -> daglig endringslogg - men med to reelle arkitekturforskjeller:

  1) /details/{id} brukes som ENESTE og autoritative dokumentkilde per sak,
     ikke /sak/{id}. Et tidligere DocOnly-søk paginert på tvers av hele
     arkivet (samme idé som /overview) viste seg å systematisk mangle
     29-94% av dokumentene per sak (verifisert: 8/8 stikkprøvde saker) og
     er derfor fjernet som datakilde (brukes fortsatt i running_daily for å
     OPPDAGE hvilke saker som har fått aktivitet, men dokumentlista hentes
     alltid fra /details). Eneste tapte felt er dokument-"type"
     (Inngående/Utgående), som /details ikke har.
  2) Acos-API-et har ikke noe eiendom/matrikkel-felt i det hele tatt - gnr/
     bnr hentes i stedet ved å GEOKODE adressen via Geonorge
     (ws.geonorge.no/adresser/v1/sok), med et fallback til gnr/bnr nevnt
     eksplisitt i sakstittelen (extract_gbnr_fra_tittel) når adressen ikke
     er geokodbar. Dette er omvendt rekkefølge av de andre ACOS-kommunene,
     som utleder BÅDE adresse og gnr/bnr fra sakstittelen alene.

FIRE KILDER (KILDER-nøkler "bygg"/"ulov"/"bygg_hist"/"ulov_hist"): to
sakstyper (Byggesak, Ulovlighetsoppfølging) på hver av to portaler (2015-nå,
og et frosset arkiv 2003-2015 fra FØR portalen ble byttet - ingen daglig
overvåking av de to _hist-kildene, de får aldri ny aktivitet).

ADRESSE/GNR-BNR-PARSING: adressen står først i tittelen, skilt med " - "
ELLER "–" (Acos bruker begge om hverandre), med kjente Bærum-spesifikke
mønstre å rense bort (se _fjern_hengende_beskrivelse/_er_soppel): "Bygg
D"-etterheng, "(parentes)"-innhold, "m.fl."/"snr N"-suffiks, gnr/bnr-titler
og rene kommunenavn ("Hurum kommune") gjenkjennes som IKKE en gateadresse.
"Gate 3 og 7"/"Gate 2, 4, 6 og 8" splittes til flere enkelt-adresser siden
Geonorge aldri finner en slik oppramsing som helhet.
"""

import json
import re
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://innsynpluss.onacos.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"
GEONORGE_URL = "https://ws.geonorge.no/adresser/v1/sok"

KOMMUNE_NR = 3201   # Bærum (Akershus fra 2024; var 3024 i Viken 2020-2023)
KOMMUNE = "Bærum"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100
MAX_WORKERS = 10
TIMEOUT = 60
RETRIES = 4


# --------------------------------------------------------------------------- #
# Adresse/gnr_bnr/matrikkelnr fra sakstittel (+ Geonorge-geokoding)
# --------------------------------------------------------------------------- #
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

# Ett husnummer-token: tall + evt. bokstav-suffiks, evt. bindestrek-spenn der
# andre enden enten er et nytt tall+bokstav ("13-25") eller bare en bokstav
# ("118B-N", flere oppganger på samme husnummer).
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
# "Gate 36 - 38" (mellomrom rundt streken) er to distinkte eiendommer, i
# motsetning til et tallspenn uten mellomrom ("13-25", kan dekke mange
# enheter) som IKKE splittes opp (se _RANGE_TOKEN-fallback i geocode_adresse).
_SPACED_PAIR = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+-\s+(\d+[A-Za-zæøåÆØÅ]?)$")


def _normaliser_nr_bokstav(s):
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _finn_hode(tittel):
    """Del tittelen ved FØRSTE '-'/'–' som ikke er del av et tallspenn
    ("13-25") eller en lengre tall-liste ("51 - 53 - 55 - 61"). En
    bindestrek MIDT INNI ett ord uten mellomrom på noen av sidene
    ("Munthe-Kaas vei") er aldri en reell separator og hoppes over. Ved en
    lengre tall-liste (3+ tall) beholdes bare gate+FØRSTE tall; et enkelt
    tallspenn (to tall) beholdes intakt siden geocode_adresse har egen
    fallback for det."""
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
        cand = min(kandidater)
        space_before = cand == 0 or tittel[cand - 1].isspace()
        space_after = cand + 1 >= len(tittel) or tittel[cand + 1].isspace()
        if not space_before and not space_after:
            i = cand + 1
            continue
        pos = cand
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
    """Del opp "Gate 3 og 7"/"Gate 2, 4, 6 og 8" i enkelt-adresser. Et
    bindestrek-spenn ("13-25") beholdes som ett element. Bare tall/bokstav-
    elementer arver gatenavn (og evt. siste tall) fra forrige fulle element."""
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


def _adresser_fra_head(head):
    """Prøv å tolke ett hode-segment som adresse(r). Returnerer [] hvis
    segmentet ikke ser ut som en gyldig adresse (tomt, søppel, eller
    mangler tall helt)."""
    head = _fjern_hengende_beskrivelse(head)
    if not head or _er_soppel(head) or not _HAR_TALL.search(head):
        return []
    candidate = _normaliser_nr_bokstav(head)
    par_m = _SPACED_PAIR.match(candidate)
    if par_m and _STARTS_UPPER.match(par_m.group(1).strip()):
        gate = par_m.group(1).strip()
        return [f"{gate} {par_m.group(2)}", f"{gate} {par_m.group(3)}"]
    return _split_flere_adresser(candidate)


_MAKS_SEGMENTER = 5  # sikkerhetsgrense - ingen observert tittel trenger mer enn 3-4 hopp


def extract_adresser(sakstittel):
    """Liste med adresse-kandidater (flere ved "og"/komma-oppramsing, eller
    et mellomrom-adskilt tallpar som "36 - 38"), eller [] hvis tittelen ikke
    inneholder noen ekte gateadresse. Hvis et segment ikke gir treff
    (typisk et stedsnavn/beskrivelse helt uten tall), fortsettes det til
    neste segment - det er der adressen ofte står i disse tilfellene."""
    if not sakstittel:
        return []
    gjenstaende = sakstittel
    for _ in range(_MAKS_SEGMENTER):
        head, rest = _finn_hode(gjenstaende.strip())
        treff = _adresser_fra_head(head)
        if treff:
            return treff
        if not rest.strip():
            break
        gjenstaende = rest
    return []


def extract_adresse(sakstittel):
    """Enkelt-felt: kandidatene slått sammen med "; "."""
    kandidater = extract_adresser(sakstittel)
    return "; ".join(kandidater) if kandidater else None


def extract_gbnr_fra_tittel(sakstittel):
    """Fanger opp "gbnr 45/170"/"gnr 45 bnr 170" direkte fra teksten - brukt
    som fallback når det ikke finnes noen geokodbar gateadresse i tittelen."""
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
    """Søkevarianter i rekkefølge: adressen som den er, deretter (hvis
    gatenavnet er satt sammen med et suffiks som normalt skal ha mellomrom
    foran, f.eks. Acos' "Hans Øverlandsvei" der Geonorge har "Hans
    Øverlands vei") en variant med mellomrom satt inn, og for tallspenn
    ("13-25") en variant med bare første tall."""
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
    """Slår opp gnr/bnr for én adresse via Geonorge (avgrenset til Bærum)."""
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


def enrich_matrikkel(saker):
    """Geokod adressene (dedup - en sak kan ha flere ved "og"/komma-
    oppramsing) og sett gnr_bnr/matrikkelnr. Faller tilbake til gnr/bnr
    nevnt direkte i sakstittelen når ingen adresse er geokodbar."""
    unike = set()
    for s in saker:
        for kandidat in (s.get("adresse") or "").split("; "):
            kandidat = kandidat.strip()
            if kandidat:
                unike.add(kandidat)
    oppslag = {}
    if unike:
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


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        portal_id="100", menypunkt_id="1631",
        sakstype="at-8de09341__cd12__4975__9460__e28961550ab5-BS!r1IrDy",
        portal_url=f"{BASE_URL}/barum-gard/sok-i-plan-og-byggesaker-fra-2015/",
        periode="2015-nå", frosset=False,
    ),
    "ulov": dict(
        sakstype_navn="Ulovlighetsoppfølging",
        portal_id="100", menypunkt_id="1631",
        sakstype="at-8de09341__cd12__4975__9460__e28961550ab5-UFO!PHbMQ6",
        portal_url=f"{BASE_URL}/barum-gard/sok-i-plan-og-byggesaker-fra-2015/",
        periode="2015-nå", frosset=False,
    ),
    "bygg_hist": dict(
        sakstype_navn="Byggesak",
        portal_id="102", menypunkt_id="1661",
        sakstype="at-ae6e937e__a279__4b7b__99c7__e38f9a93cfd6-BS!BeVgFX",
        portal_url=f"{BASE_URL}/barum-gard-hist-03-15/sok-i-plan-og-byggesaker-fra-2003-2015/",
        periode="2003-2015", frosset=True,
    ),
    "ulov_hist": dict(
        sakstype_navn="Ulovlighetsoppfølging",
        portal_id="102", menypunkt_id="1661",
        sakstype="at-ae6e937e__a279__4b7b__99c7__e38f9a93cfd6-UFO!kOJWCE",
        portal_url=f"{BASE_URL}/barum-gard-hist-03-15/sok-i-plan-og-byggesaker-fra-2003-2015/",
        periode="2003-2015", frosset=True,
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def make_session(kilde_key):
    """Session med browser-UA + cookies + Acos-headere for gitt kilde sin portal."""
    kilde = KILDER[kilde_key]
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    })
    s.get(kilde["portal_url"], timeout=TIMEOUT)   # session-cookies (anti-xsrf)
    s.headers.update({
        "PortalID": kilde["portal_id"],
        "MenypunktID": kilde["menypunkt_id"],
        "SprakID": "1",
        "X-ANTI-CSRF": "1",
        "Content-Type": "application/json",
        "Referer": kilde["portal_url"],
        "Origin": BASE_URL,
    })
    return s


def _request_with_retry(send, retries=RETRIES):
    """Delt retry/backoff-logikk for _post/_get. `send` er en no-arg
    funksjon som utfører selve HTTP-kallet og returnerer et Response."""
    last_err = None
    for attempt in range(retries):
        try:
            r = send()
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def _post(session, path, body, retries=RETRIES):
    return _request_with_retry(
        lambda: session.post(API_BASE + path, data=json.dumps(body), timeout=TIMEOUT), retries)


def _get(session, path, retries=RETRIES):
    return _request_with_retry(lambda: session.get(API_BASE + path, timeout=TIMEOUT), retries)


def fetch_overview(session, kilde_key, filter_content, page, page_size=PAGE_SIZE,
                    from_iso=None, to_iso=None):
    """Én side fra /overview for gitt FilterContent (CaseOnly/DocOnly)."""
    kilde = KILDER[kilde_key]
    dato_kv = ([{"key": "Dato", "value": "Other"},
                {"key": "FromDate", "value": from_iso},
                {"key": "ToDate", "value": to_iso}]
               if from_iso and to_iso else [{"key": "Dato", "value": "AllCases"}])
    body = {"type": OVERVIEW_TYPE_SAK, "keyValues": dato_kv + [
        {"key": "FilterContent", "value": filter_content},
        {"key": "Sakstype", "value": kilde["sakstype"]},
        {"key": "page", "value": str(page)},
        {"key": "pageSize", "value": str(page_size)},
    ]}
    return _post(session, "/overview", body)["content"]["searchItems"]


def fetch_all_overview(session, kilde_key, filter_content, max_pages=None,
                        from_iso=None, to_iso=None):
    """Paginer gjennom alle sider av /overview."""
    first = fetch_overview(session, kilde_key, filter_content, page=1,
                            from_iso=from_iso, to_iso=to_iso)
    items = list(first["items"])
    page_count = first["pageCount"]
    if max_pages:
        page_count = min(page_count, max_pages)
    print(f"  {filter_content}: {first['totalItemCount']} totalt, {page_count} sider")
    for page in range(2, page_count + 1):
        items.extend(fetch_overview(session, kilde_key, filter_content, page=page,
                                     from_iso=from_iso, to_iso=to_iso)["items"])
        if page % 20 == 0:
            print(f"    {filter_content} side {page}/{page_count}")
    return items


def fetch_case_details(session, identifier):
    """Full sak fra /details/{id} - den ENESTE og autoritative kilden til
    dokumentliste + saksbehandler (se modul-docstring)."""
    try:
        return _get(session, "/details/" + quote(identifier, safe="")).get("content", {}).get("sak")
    except Exception:  # noqa: BLE001
        return None


def fetch_details_parallel(session, identifiers):
    """Hent /details for mange saker samtidig. dict identifier -> sak (eller None)."""
    resultater = {}
    if not identifiers:
        return resultater
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_case_details, session, ident): ident for ident in identifiers}
        for fut in as_completed(futs):
            resultater[futs[fut]] = fut.result()
    return resultater


def fetch_vedlegg(session, dokument_identifier):
    """Henter vedlegg (filer) for ett dokument via /dokument/{id}."""
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


def build_journalpost(d):
    """Bygg journalpost direkte fra en /details-dokument-node. Ingen "type"
    (Inngående/Utgående) tilgjengelig her (se modul-docstring)."""
    return {
        "identifier": d.get("identifikator"),
        "dokument_id": d.get("friendlyId"),
        "tittel": d.get("tittel"),
        "type": None,
        "dato": d.get("dato"),
        "avsender": d.get("fra") or [],
        "mottaker": d.get("til") or [],
        "vedlegg": [],
    }


def build_sak(kilde_key, case_item, journalposter, det=None):
    kilde = KILDER[kilde_key]
    props = case_item.get("properties", {})
    ident = case_item.get("identifier")
    return {
        "identifier": ident,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": props.get("saksnummer"),
        "sakstype": kilde["sakstype_navn"],
        "sakstittel": case_item.get("title"),
        "adresse": extract_adresse(case_item.get("title")),
        "gnr_bnr": None,        # fylles av enrich_matrikkel
        "matrikkelnr": None,    # fylles av enrich_matrikkel
        "status": case_item.get("status"),
        "dato": props.get("dato"),
        "saksbehandler": (det or {}).get("saksbehandler"),
        "url": kilde["portal_url"] + "#/details/" + ident if ident else None,
        "journalposter": journalposter,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump
# --------------------------------------------------------------------------- #
def save_resultater(saker, output_file):
    tmp = output_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(output_file)


def run_full_dump(kilde_key, output_file, max_pages=None, from_date=None, to_date=None,
                   with_vedlegg=True, with_matrikkel=True):
    """Full historisk dump for én kilde. from_date/to_date (date-objekter)
    filtrerer via /overview sitt eget FromDate/ToDate-vindu (server-side,
    til forskjell fra de andre ACOS-kommunene som filtrerer lokalt - Bærums
    /overview støtter dette direkte)."""
    kilde = KILDER[kilde_key]
    session = make_session(kilde_key)
    t0 = time.time()

    from_iso = from_date.isoformat() if from_date else None
    to_iso = to_date.isoformat() if to_date else None
    if from_iso and to_iso:
        print(f"Datovindu: {from_iso} - {to_iso}")

    print("Henter saker (CaseOnly)...")
    case_items = fetch_all_overview(session, kilde_key, "CaseOnly", max_pages=max_pages,
                                     from_iso=from_iso, to_iso=to_iso)

    print(f"Henter /details for {len(case_items)} saker (dokumentliste + saksbehandler)...")
    detaljer = fetch_details_parallel(session, [c.get("identifier") for c in case_items if c.get("identifier")])

    saker, all_docs = [], []
    for case in case_items:
        cid = case.get("identifier")
        det = detaljer.get(cid)
        jps = [build_journalpost(d) for d in (det or {}).get("dokumenter", [])]
        saker.append(build_sak(kilde_key, case, jps, det))
        all_docs.extend(jp for jp in jps if jp["identifier"])
    print(f"Satte sammen {len(saker)} saker, {len(all_docs)} journalposter")

    if with_matrikkel:
        print("Geokoder adresser (Geonorge)...")
        enrich_matrikkel(saker)
        traff = sum(1 for s in saker if s.get("gnr_bnr"))
        print(f"    fant gnr/bnr for {traff}/{len(saker)} saker")

    if with_vedlegg and all_docs:
        print(f"Henter vedlegg for {len(all_docs)} dokumenter...")
        done = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(fetch_vedlegg, session, jp["identifier"]): jp for jp in all_docs}
            for fut in as_completed(futs):
                futs[fut]["vedlegg"] = fut.result()
                done += 1
                if done % 500 == 0:
                    save_resultater(saker, output_file)
                    print(f"    vedlegg {done}/{len(all_docs)} (lagret)")

    save_resultater(saker, output_file)
    n_ved = sum(len(jp["vedlegg"]) for jp in all_docs)
    print(f"Ferdig: {len(saker)} saker, {len(all_docs)} journalposter, "
          f"{n_ved} vedlegg -> {output_file} ({time.time() - t0:.0f}s)")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - KUN for bygg/ulov (2015-nå), de to
# _hist-kildene er frosset og får aldri ny aktivitet.
#
# Bærum har ekte datoer, så dette er dato-vindu-basert (som Bergen/
# Stavanger), ikke en full sveip: nye saker = CaseOnly i vinduet, nye
# journalposter = DocOnly i vinduet som hører til eldre saker. DocOnly er
# derimot IKKE bevist komplett alene (se modul-docstring) - for hver sak vi
# finner slås den derfor sammen med den autoritative /details-dokument-
# lista, filtrert mot allerede-sette journalpost-ID-er.
#
# state.json husker siste vellykkede dag + rapporterte identifikatorer ->
# auto-backfiller hull i stedet for at data stille forsvinner.
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 3             # dager bakover fra referansedatoen (overlapper mot forsinket indeksering)
MAX_BACKFILL_DAYS = 30      # sikkerhetsgrense: aldri auto-backfill lenger enn dette i én kjøring
SEEN_RETENTION_DAYS = 14    # hvor lenge identifiers huskes for dedup (må være >= WINDOW_DAYS)


def load_state(state_file):
    if state_file.exists():
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            state = {}
    else:
        state = {}
    state.setdefault("last_success_date", None)
    state.setdefault("seen_saker", {})
    state.setdefault("seen_journalposter", {})
    return state


def save_state(state, state_file):
    tmp = state_file.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(state_file)


def prune_seen(state, today, retention_days=SEEN_RETENTION_DAYS):
    cutoff = (today - timedelta(days=retention_days)).isoformat()
    state["seen_saker"] = {k: v for k, v in state["seen_saker"].items() if v >= cutoff}
    state["seen_journalposter"] = {k: v for k, v in state["seen_journalposter"].items() if v >= cutoff}


def fill_korrespondanse(det, journalposter):
    """Fyll avsender (fra) + mottaker (til) på journalpostene fra en details-sak."""
    if not det:
        return
    korr = {d.get("identifikator"): (d.get("fra") or [], d.get("til") or [])
            for d in det.get("dokumenter", [])}
    for jp in journalposter:
        if jp["identifier"] in korr:
            jp["avsender"], jp["mottaker"] = korr[jp["identifier"]]


def build_journalpost_fra_docitem(doc_item):
    """DocOnly-oppslaget sin egen (mindre komplette) journalpost-form - kun
    brukt for å OPPDAGE aktivitet i running_daily, fylles ut videre av
    fill_korrespondanse fra den autoritative /details-lista."""
    props = doc_item.get("properties", {})
    av = props.get("avsender")
    return {
        "identifier": doc_item.get("identifier"),
        "dokument_id": props.get("dokumentID"),
        "tittel": doc_item.get("title"),
        "type": doc_item.get("type"),
        "dato": props.get("dato"),
        "avsender": [av] if av else [],
        "mottaker": [],
        "vedlegg": [],
    }


def parent_sak_ref(kilde_key, identifier, sak):
    """Metadata om en eldre forelder-sak som fikk en ny journalpost. sak =
    ferdig hentet /details-respons."""
    kilde = KILDER[kilde_key]
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
        "saksbehandler": sak.get("saksbehandler") if sak else None,
        "url": kilde["portal_url"] + "#/details/" + identifier,
    }


def write_output(nye_saker, nye_journalposter, ref_date, output_dir, log=print):
    output_dir.mkdir(exist_ok=True)
    (output_dir / f"saker_{ref_date}.json").write_text(
        json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"journalposter_{ref_date}.json").write_text(
        json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Skrev {len(nye_saker)} nye saker og "
        f"{len(nye_journalposter)} saker med nye journalposter -> {output_dir}")


def run_for_date(kilde_key, session, ref_date, state, output_dir, window_days=WINDOW_DAYS, log=print):
    """Kjør datovinduet [ref_date - (window_days-1), ref_date], dedupliser
    mot state, skriv output, og oppdater state med det som ble rapportert."""
    from_iso = (ref_date - timedelta(days=window_days - 1)).isoformat()
    to_iso = ref_date.isoformat()
    log(f"Datovindu: {from_iso} .. {to_iso}")

    case_items = fetch_all_overview(session, kilde_key, "CaseOnly", from_iso=from_iso, to_iso=to_iso)
    doc_items = fetch_all_overview(session, kilde_key, "DocOnly", from_iso=from_iso, to_iso=to_iso)
    log(f"  {len(case_items)} saker, {len(doc_items)} dokumenter i rå-vinduet (før dedup)")

    docs_by_case = defaultdict(list)
    for d in doc_items:
        docs_by_case[d.get("parentIdentifier")].append(d)

    seen_saker = state["seen_saker"]
    seen_jp = state["seen_journalposter"]

    alle_case_ids = {c.get("identifier") for c in case_items if c.get("identifier")}
    nye_sak_ids = {cid for cid in alle_case_ids if cid not in seen_saker}

    nye_med_jp_ids = [
        cid for cid in nye_sak_ids
        if any(d.get("identifier") not in seen_jp for d in docs_by_case.get(cid, []))
    ]
    detaljer_nye = fetch_details_parallel(session, nye_med_jp_ids) if nye_med_jp_ids else {}

    nye_saker, alle_jp = [], []
    for c in case_items:
        cid = c.get("identifier")
        if cid not in nye_sak_ids:
            continue
        docs = [d for d in docs_by_case.get(cid, []) if d.get("identifier") not in seen_jp]
        jps = [build_journalpost_fra_docitem(d) for d in docs]
        det = detaljer_nye.get(cid)
        if jps:
            fill_korrespondanse(det, jps)
        sak = build_sak(kilde_key, c, jps)
        if det:
            sak["saksbehandler"] = det.get("saksbehandler")
        nye_saker.append(sak)
        alle_jp.extend(jps)

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
        jps = [build_journalpost_fra_docitem(d) for d in docs]
        fill_korrespondanse(det, jps)
        nye_journalposter.append({
            "parent_sak": parent_sak_ref(kilde_key, parent_id, det),
            "nye_journalposter": jps,
        })
        alle_jp.extend(jps)

    enrich_matrikkel(nye_saker + [x["parent_sak"] for x in nye_journalposter])
    if alle_jp:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(fetch_vedlegg, session, jp["identifier"]): jp
                    for jp in alle_jp if jp.get("identifier")}
            for fut in as_completed(futs):
                futs[fut]["vedlegg"] = fut.result()

    write_output(nye_saker, nye_journalposter, to_iso, output_dir, log=log)

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


def run_daily(kilde_key, state_file, output_dir, day_back=1, log_fn=print,
              window_days=WINDOW_DAYS, max_backfill_days=MAX_BACKFILL_DAYS):
    """Daglig endringslogg med auto-backfill av manglende dager siden siste
    vellykkede kjøring (se modul-docstring). `day_back=1` betyr "til og med
    i går" hvis ingen state finnes ennå."""
    state = load_state(state_file)
    today = datetime.now(ZoneInfo("Europe/Oslo")).date()
    target = today - timedelta(days=day_back)

    if state["last_success_date"]:
        start = datetime.fromisoformat(state["last_success_date"]).date() + timedelta(days=1)
    else:
        start = target

    if start > target:
        log_fn(f"Ingenting å gjøre - siste vellykkede dag ({state['last_success_date']}) "
               f"er allerede >= mål ({target}).")
        return

    dager = []
    d = start
    while d <= target:
        dager.append(d)
        d += timedelta(days=1)

    if len(dager) > max_backfill_days:
        log_fn(f"ADVARSEL: {len(dager)} dager mangler siden siste kjøring - begrenser "
               f"auto-backfill til de {max_backfill_days} nyeste.")
        dager = dager[-max_backfill_days:]

    if len(dager) > 1:
        log_fn(f"Backfiller {len(dager)} manglende dag(er): {dager[0]} .. {dager[-1]}")

    session = make_session(kilde_key)
    for ref_date in dager:
        try:
            n_saker, n_jp = run_for_date(kilde_key, session, ref_date, state, output_dir,
                                          window_days=window_days, log=log_fn)
            state["last_success_date"] = ref_date.isoformat()
            prune_seen(state, today)
            save_state(state, state_file)
            log_fn(f"OK {ref_date}: {n_saker} nye saker, {n_jp} saker med nye journalposter")
        except Exception:  # noqa: BLE001
            log_fn(f"FEIL under kjøring for {ref_date}:\n{traceback.format_exc()}")
            log_fn(f"Stopper her. Siste vellykkede dag forblir {state['last_success_date']}.")
            raise
