"""Delt bibliotek for Fredrikstad byggesak-scraperen.

Som Oslo har Fredrikstad ingen egen postliste-portal - byggesaker hentes fra
den nasjonale eInnsyn-plattformen (https://einnsyn.no): /api/result (søk),
/api/v2/saksmappe (journalpostliste per sak) og /api/v2/fil (selve
vedleggsfilene). Det finnes ingen egen sakstype-kode, så byggesaker skilles
ut fra resten av arkivet via EXCLUDE_TERM.

Byggesaksarkivet er spredt over TO separate virksomheter i eInnsyns
organisasjonstre (VIRKSOMHET_IDER) - "Virksomhet Regulering og byggesak"
(hovedenheten, ca. 5300 saker) og "Byggesak og geomatikk" (en mindre,
overlappende gren, ca. 330 saker) - begge må med for å matche kommunens
egen lagrede søkefilter på einnsyn.no (bekreftet mot brukerens filter:
5608 treff totalt med samme EXCLUDE_TERM). "JournalpostForMøte" ekskluderes
i tillegg (samme som Oslo). EXCLUDE_TERM-listen er hentet direkte fra denne
lagrede filteren, IKKE satt sammen selv - i motsetning til andre kommuner i
dette repoet ekskluderes "tilsyn" her, fordi kommunens egen filter gjør det.

Stor forskjell fra Oslo: Fredrikstads titler merker gnr/bnr eksplisitt med
ordet "Eiendom" (evt. "Gnr X, bnr Y") i stedet for å skrive nakne tall - det
gjør gnr/bnr-uthenting mer presis enn Oslos frittstående tallpar-søk, og
adresseuthenting skjer ved å maskere bort denne blokken og lete etter et
gate+husnummer-mønster i det som blir igjen. Fredrikstad har i tillegg reell
vedleggstilgang (dokumentbeskrivelser -> dokumentobjekter -> /api/v2/fil),
som Oslo ikke bruker.

API-et returnerer maks 50 treff per side ("size" ignoreres).

To kjøremåter, forskjellig søkestrategi (se Oslos modul for full begrunnelse
- identisk mekanisme her):
  run_full_dump - kun "Saksmappe"-treff, hele historikken kronologisk.
  run_daily     - BEGGE treff-typer (Saksmappe OG Journalpost) i et lite
                  datovindu, for å fange både nye saker og nye journalposter
                  på eldre, allerede kjente saker.

Brukes av:
  - publisert-2024-today_dump/bygg/main.py   (engangs historisk dump)
  - running_daily/bygg/app/main.py (daglig endringslogg)
"""

import json
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://www.einnsyn.no"
RESULT_URL = f"{BASE_URL}/api/result"
DETAIL_URL = f"{BASE_URL}/api/v2/saksmappe"
FIL_URL = f"{BASE_URL}/api/v2/fil"
PORTAL_SAK_URL = f"{BASE_URL}/saksmappe"

VIRKSOMHET_IDER = [
    "http://data.einnsyn.no/virksomhet/28743c4c-d7b4-44b8-a387-86b990b5eccf",  # Virksomhet Regulering og byggesak
    "http://data.einnsyn.no/virksomhet/d59df1ab-a889-4030-9bc7-aab357f1901d",  # Byggesak og geomatikk
]

KOMMUNE = "Fredrikstad"
KOMMUNE_NR = "3107"

START_DATE = date(2024, 1, 12)  # eldste publisertDato funnet på tvers av begge virksomhetene

PAGE_SIZE = 50  # API-et gir aldri flere treff enn dette per side
MAX_WORKERS = 10
TIMEOUT = 30
RETRIES = 4

# Fra kommunens egen lagrede søkefilter på einnsyn.no (verifisert: gir 5608
# treff sammen med VIRKSOMHET_IDER og JournalpostForMøte-ekskluderingen under
# - identisk med tallet i den lagrede filteren). Fjerner rene matrikkel-/
# geomatikk-saker og møtejournalposter fra byggesaksarkivet.
EXCLUDE_TERM = (
    '"seksjonering", "matrikkelen", "delesøknad", "adresseendring", '
    '"oppmålingsforretning", "tilsyn", "målebrev", "innsyn"'
)

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://einnsyn.no",
    "Referer": "https://einnsyn.no/",
}


# --------------------------------------------------------------------------- #
# Adresseparsing
# --------------------------------------------------------------------------- #
# Gnr/bnr er nesten alltid eksplisitt merket i tittelen ("Eiendom 439/435",
# evt. flere par i en liste: "Eiendom 300/27, 300/865, 300/5", eller det
# eldre "Gnr X, bnr Y"-formatet). Dette gjør gnr/bnr-uthenting vesentlig mer
# presis enn en frittstående tallpar-scan (se Oslos extract_gnr_bnr).
_EIENDOM_RE = re.compile(r"[Ee]iendom(?:mer)?\s+")
# Gruppe 2 inkluderer en evt. festenr/seksjonsnr-hale ("60/33/71" ->
# gnr="60", resten="33/71") - denne halen må bli med i det returnerte
# gnr/bnr-paret (og dermed matrikkelnr), ikke bare kappes bort, ellers mister
# f.eks. "Eiendom 60/33/71" seksjonsnummeret og blir feilaktig til "60/33".
_GNRBNR_TOKEN = re.compile(r"(\d{1,4})\s*/\s*(\d{1,5}(?:/\d{1,5}){0,2})")
_GNR_BNR_LABELED = re.compile(r"[Gg]nr\.?\s*(\d{1,4})\s*[,/]?\s*[Bb]nr\.?\s*(\d{1,5})")
# Rene tall (uten "/") etter et komma/"og" i en "Eiendom ..."-liste tolkes
# som en fortsettelse av SISTE gnr, f.eks. "Eiendom 601/508, 509, 510, 511"
# -> 601/508, 601/509, 601/510, 601/511 (samme gnr, ny bnr per tall). Gjelder
# KUN komma/"og" som skille, IKKE "-", ellers ville f.eks. "- 4
# tomannsboliger" (antall, ikke en fortsettelse av lista) blitt feiltolket
# som "601/4".
_BARE_BNR = re.compile(r"\d{1,5}")


def _gnrbnr_liste(tittel, start):
    """Går gjennom en gnr/bnr-liste i "tittel" fra og med absolutt posisjon
    "start" (rett etter "Eiendom(mer) ") og returnerer (par_liste,
    sluttposisjon). Delt av extract_gnr_bnr og _mask_eiendom_chunk, som
    ellers hadde denne listegjennomgangen duplisert med hverandre."""
    par_liste = []
    last_gnr = None
    pos = start
    while True:
        tok = _GNRBNR_TOKEN.match(tittel, pos)
        if tok:
            last_gnr, bnr_hale = tok.group(1), tok.group(2)
            pos = tok.end()
        else:
            bare = _BARE_BNR.match(tittel, pos) if last_gnr else None
            if not bare:
                break
            bnr_hale = bare.group(0)
            pos = bare.end()
        par = f"{last_gnr}/{bnr_hale}"
        if par not in par_liste:
            par_liste.append(par)
        sep = re.match(r"\s*(,|-|og|m\.fl\.)\s*", tittel[pos:], re.IGNORECASE)
        if not sep:
            break
        neste = pos + sep.end()
        if _GNRBNR_TOKEN.match(tittel, neste):
            pos = neste
            continue
        if sep.group(1).lower() in (",", "og") and _BARE_BNR.match(tittel, neste):
            pos = neste
            continue
        break
    return par_liste, pos


def extract_gnr_bnr(sakstittel):
    """Alle gnr/bnr-par funnet i tittelen (via "Eiendom ..."-lista og/eller
    "Gnr X, bnr Y"), inkludert en evt. festenr/seksjonsnr-hale, i rekkefølge,
    uten duplikater."""
    if not sakstittel:
        return None
    par_liste = []
    m = _EIENDOM_RE.search(sakstittel)
    if m:
        for par in _gnrbnr_liste(sakstittel, m.end())[0]:
            if par not in par_liste:
                par_liste.append(par)
    for m2 in _GNR_BNR_LABELED.finditer(sakstittel):
        par = f"{m2.group(1)}/{m2.group(2)}"
        if par not in par_liste:
            par_liste.append(par)
    return par_liste or None


def build_matrikkelnr(gnr_bnr_liste):
    if not gnr_bnr_liste:
        return None
    return [f"{KOMMUNE_NR}-{par}" for par in gnr_bnr_liste]


def _mask_eiendom_chunk(tittel):
    """Fjerner "Eiendom <gnr/bnr[, gnr/bnr...]>"-blokken (uansett hvor i
    tittelen den står - før, midt i eller etter en evt. gateadresse) slik at
    resten kan adresse-tolkes uavhengig av blokkens plassering."""
    m = _EIENDOM_RE.search(tittel)
    if not m:
        return _GNR_BNR_LABELED.sub("", tittel)
    start = m.start()
    end = _gnrbnr_liste(tittel, m.end())[1]
    venstre, hoyre = tittel[:start].rstrip(), tittel[end:].lstrip()
    if venstre.endswith("-") or venstre.endswith("–"):
        venstre = venstre[:-1].rstrip()
    # "-" og "," forekommer begge som skilletegn foran adressen når
    # "Eiendom ..."-blokken kommer FØR den ("Eiendom 50/159, Trondalsveien 3"
    # / "Eiendom 50/159 - Trondalsveien 3") - begge må strippes, ellers feiler
    # _HUSNR_SLUTT-valideringen fordi segmentet begynner med tegnet i stedet
    # for gatenavnet.
    if hoyre.startswith("-") or hoyre.startswith("–"):
        hoyre = hoyre[1:].lstrip()
    if hoyre.startswith(","):
        hoyre = hoyre[1:].lstrip()
    if venstre and hoyre:
        return f"{venstre} - {hoyre}"
    return venstre or hoyre


# Positivt anker: ekte gateadresser i Fredrikstad slutter (nesten) alltid på
# et husnummer - evt. med bokstavsuffiks ("21B"), et tall-/bokstavspenn
# ("10-14", "16 A-C"), en parentes ("(tomt 36)") eller et etterfølgende
# stedsnavn (", Onsøy"). Kommunen har for mange kystrelaterte gatenavn-
# endelser (-holme, -kilen, -stranda, -bølgen, -bruket, -dalen ...) til at en
# endelsesliste (slik Oslo bruker) ville dekket dem alle - husnummeret er et
# mer robust kjennetegn her.
_HUSNR_SLUTT = re.compile(
    r"^[A-ZÆØÅ].{1,40}?\s(\d{1,4})(?:\s?[A-Za-z])?"
    r"(?:\s*-\s*(\d{1,4})(?:\s?[A-Za-z])?)?"
    r"(?:\s*\(.*\))?"
    r"(?:,\s*[A-ZÆØÅa-zæøå]+)?$"
)
_NO_ADDRESS_PATTERNS = [
    "innsyn", "hms-referater", "bruk av teknologi", "jordskiftesak",
    "setningsnivellement", "adresseendring", "adressering", "omadressering",
]

# Et 4-sifret "husnummer" i årstall-området er så godt som alltid en
# årsreferanse i en saksarkiv-tittel ("Planutvalget 2020", "Planutvalget
# 2019-2023"), ikke et reelt husnummer - Fredrikstads egne adresser med
# 4-sifret husnummer (f.eks. "Sarpsborgveien 1126") ligger uansett langt
# utenfor dette området, så det er trygt å avvise alt her.
_ARSTALL_MIN, _ARSTALL_MAKS = 1900, 2099


def _er_arstall(match):
    tall = [g for g in match.groups() if g]
    return bool(tall) and all(len(t) == 4 and _ARSTALL_MIN <= int(t) <= _ARSTALL_MAKS for t in tall)

# Offisiell adresseform har ikke mellomrom mellom husnummer og bokstavsuffiks
# ("10 A" -> "10A", "9 B-C" -> "9B-C") - samme normalisering som Oslo bruker
# (_NR_BOKSTAV_MELLOMROM der).
_NR_BOKSTAV_MELLOMROM = re.compile(r"(\d)\s+([A-Za-zÆØÅæøå])\b")
# Det etterfølgende stedsnavnet (", Onsøy", ", Kråkerøy" ...) i _HUSNR_SLUTT
# er kun der for å GODKJENNE segmentet som gyldig adresse - selve stedsnavnet
# hører til bydel/område, ikke gateadressen, og skal ikke være med i den
# returnerte "adresse"-verdien.
_TRAILING_STED = re.compile(r",\s*[A-ZÆØÅa-zæøå]+$")


def _rens_adresse(segment):
    """Etterrensk av et allerede validert adressesegment: fjerner et
    etterfølgende stedsnavn, normaliserer mellomrom foran bokstavsuffiks, og
    kollapser doble mellomrom som forekommer i enkelte kildetitler
    ("Paul  Holmsens vei")."""
    segment = _TRAILING_STED.sub("", segment).strip()
    segment = _NR_BOKSTAV_MELLOMROM.sub(r"\1\2", segment)
    return re.sub(r"\s+", " ", segment).strip()


# To varianter av flere adresser i samme kandidat, begge skrevet om til
# "Adresse1; Adresse2..." (samme "; "-konvensjon som ellers i prosjektet):
#   1. Samme gate, kun tallet varierer: "Ferjestedsveien 20A, 20B og 20C",
#      "Rødsmyraskogen 32, 34, 36, 38" (komma- og/eller "og"-adskilt).
#   2. Egne gate+nr-par: "Måkeveien 6, Måkeveien 8" - matcher IKKE mønster 1
#      (tallet der må stå RETT etter komma/"og", ikke etter et nytt
#      gatenavn), så de to er trygt uavhengige av hverandre.
_SAMME_GATE_LISTE_RE = re.compile(
    r"^([A-ZÆØÅ][\wæøåÆØÅ.\- ]*?)\s+(\d+[A-Za-zæøåÆØÅ]?)"
    r"((?:\s*,\s*\d+[A-Za-zæøåÆØÅ]?)*)"
    r"(?:\s+og\s+(\d+[A-Za-zæøåÆØÅ]?))?$"
)
_DEL_RE = re.compile(r"\s*,\s*|\s+og\s+")
_GATE_NR_SEGMENT_RE = re.compile(r"^[A-ZÆØÅ][\wæøåÆØÅ.\-]*(?:\s[\wæøåÆØÅ.\-]+)*\s\d+[A-Za-zæøåÆØÅ]?$")


def _utvid_flere_adresser(adresse):
    m = _SAMME_GATE_LISTE_RE.match(adresse)
    if m:
        gate = m.group(1).strip()
        tall = [m.group(2)] + [t.strip() for t in m.group(3).split(",") if t.strip()]
        if m.group(4):
            tall.append(m.group(4))
        return "; ".join(f"{gate} {n}" for n in tall)
    deler = [d.strip() for d in _DEL_RE.split(adresse) if d.strip()]
    if len(deler) >= 2 and all(_GATE_NR_SEGMENT_RE.match(d) for d in deler):
        return "; ".join(deler)
    return adresse


def extract_adresse(sakstittel):
    """Maskerer bort "Eiendom ..."-blokken, tar første "-"-adskilte segment
    av resten, og godkjenner det som adresse hvis det slutter på et
    husnummer-mønster (se _HUSNR_SLUTT). Den returnerte verdien renses for
    stedsnavn og husnummer/bokstav-mellomrom (se _rens_adresse), og utvides
    til flere ";"-skilte adresser hvis segmentet faktisk lister opp mer enn
    én (se _utvid_flere_adresser) - disse trinnene brukes kun til å
    validere/tolke segmentet over, ikke til å endre hva som ble validert."""
    if not sakstittel:
        return None
    masked = _mask_eiendom_chunk(sakstittel)
    segment = masked.split(" - ", 1)[0].split(" – ", 1)[0].strip()
    if not segment:
        return None
    lav = segment.lower()
    if any(p in lav for p in _NO_ADDRESS_PATTERNS):
        return None
    m = _HUSNR_SLUTT.match(segment)
    if m and not _er_arstall(m):
        return _utvid_flere_adresser(_rens_adresse(segment))
    return None


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _post(session, body):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.post(RESULT_URL, data=json.dumps(body), timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def _get(session, url, params=None):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def fetch_detail(session, external_id):
    """Full journalpostliste for én sak via /api/v2/saksmappe."""
    url = f"{DETAIL_URL}?iri={quote(external_id, safe='')}"
    try:
        return _get(session, url)
    except Exception:  # noqa: BLE001
        return None


def _paginate(session, body_base, should_stop=None):
    """Henter alle sider for et gitt søk (body_base uten "size"/"offset").
    should_stop(items), om satt, sjekkes før hver ekstra side og lar
    fetch_range avslutte paginering tidlig - se der for begrunnelse. Brukt av
    både fetch_range og fetch_window, som ellers er identiske bortsett fra
    filtrene som sendes inn."""
    first = _post(session, {**body_base, "size": PAGE_SIZE, "offset": 0})
    total = first.get("hitCount", 0)
    items = list(first.get("searchHits", []))
    offset = PAGE_SIZE
    while offset < total:
        if should_stop is not None and should_stop(items):
            break
        page = _post(session, {**body_base, "size": PAGE_SIZE, "offset": offset})
        hits = page.get("searchHits", [])
        if not hits:
            break
        items.extend(hits)
        offset += PAGE_SIZE
    return [h["source"] for h in items if h.get("source")]


def fetch_range(session, from_iso, to_iso, done_ids=None, need=None):
    """Alle Saksmappe-treff (byggesaker) publisert i [from_iso, to_iso] -
    brukes av run_full_dump. done_ids/need: stopper paginering så snart
    `need` treff som IKKE allerede finnes i done_ids er samlet opp."""
    filters = [
        {"fieldName": "type", "fieldValue": ["Saksmappe"], "type": "termQueryFilter"},
        {"fieldName": "type", "fieldValue": ["JournalpostForMøte"], "type": "notQueryFilter"},
        {"fieldName": "arkivskaperTransitive", "fieldValue": VIRKSOMHET_IDER, "type": "postQueryFilter"},
        {"to": f"{to_iso}||/d", "from": f"{from_iso}||/d", "fieldName": "publisertDato", "type": "rangeQueryFilter"},
    ]
    body_base = {
        "appliedFilters": filters,
        "searchTerms": [{"field": "search_tittel", "operator": "NOT_ANY", "searchTerm": EXCLUDE_TERM}],
        "sort": {"fieldName": "publisertDato", "order": "DESC", "id": "published"},
    }

    def _n_new(items):
        if done_ids is None:
            return None
        return sum(1 for h in items if (h.get("source") or {}).get("externalId") not in done_ids)

    should_stop = None
    if need is not None:
        should_stop = lambda items: _n_new(items) is not None and _n_new(items) >= need  # noqa: E731
    return _paginate(session, body_base, should_stop=should_stop)


def fetch_window(session, from_iso, to_iso):
    """Alle Saksmappe- OG Journalpost-treff publisert i [from_iso, to_iso] -
    brukes av run_daily (se modul-docstring)."""
    filters = [
        {"fieldName": "type", "fieldValue": ["JournalpostForMøte"], "type": "notQueryFilter"},
        {"fieldName": "arkivskaperTransitive", "fieldValue": VIRKSOMHET_IDER, "type": "postQueryFilter"},
        {"to": f"{to_iso}||/d", "from": f"{from_iso}||/d", "fieldName": "publisertDato", "type": "rangeQueryFilter"},
    ]
    body_base = {
        "appliedFilters": filters,
        "searchTerms": [{"field": "search_tittel", "operator": "NOT_ANY", "searchTerm": EXCLUDE_TERM}],
        "sort": {"fieldName": "publisertDato", "order": "DESC", "id": "published"},
    }
    return _paginate(session, body_base)


# --------------------------------------------------------------------------- #
# Bygg opp poster
# --------------------------------------------------------------------------- #
def build_vedlegg(dokumentbeskrivelser):
    """Bygger vedleggsliste fra en journalposts dokumentbeskrivelser. Feltnavn
    varierer mellom de to eInnsyn-endepunktene: /api/v2/saksmappe (brukt av
    run_full_dump) gir "dokumentbeskrivelser"/"dokumentobjekter"/"id"/
    "tilknytning" (bare ordet, f.eks. "vedlegg"), mens søketreff for
    Journalpost (brukt av run_daily) gir de samme dataene entall-navngitt:
    "dokumentbeskrivelse"/"dokumentobjekt"/"externalId"/
    "tilknyttetRegistreringSom" (full Noark-URI der siste stiledd er selve
    ordet). Denne funksjonen leser begge variantene. Filen kan lastes ned
    direkte fra "url" (GET, ingen autentisering kreves) - i motsetning til
    Oslo har Fredrikstad reell vedleggstilgang gjennom eInnsyn."""
    vedlegg = []
    for dok in dokumentbeskrivelser or []:
        objekter = dok.get("dokumentobjekter") or dok.get("dokumentobjekt") or []
        tilknytning = dok.get("tilknytning")
        if not tilknytning:
            uri = dok.get("tilknyttetRegistreringSom") or ""
            tilknytning = uri.rstrip("/").rsplit("/", 1)[-1] or None
        for objekt in objekter:
            fil_id = objekt.get("id") or objekt.get("externalId")
            if not fil_id:
                continue
            vedlegg.append({
                "tittel": dok.get("tittel"),
                "tilknytning": tilknytning,  # "hoveddokument" eller "vedlegg"
                "format": objekt.get("format"),
                "url": f"{FIL_URL}?iri={quote(fil_id, safe='')}",
            })
    return vedlegg


def build_journalpost(jp, saks_url):
    avsender = jp.get("korrespondansepartAvsender", [])
    mottaker = jp.get("korrespondansepartMottaker", [])
    return {
        "identifier": jp.get("id"),
        "jp_nr": jp.get("journalpostnummer"),
        "tittel": jp.get("tittel"),
        "type": jp.get("journalposttype"),
        "dato": jp.get("dokumentdato"),
        "journalpostdato": jp.get("journalpostdato"),
        "avsender": [p.get("navn") for p in avsender if p.get("navn")],
        "mottaker": [p.get("navn") for p in mottaker if p.get("navn")],
        "vedlegg": build_vedlegg(jp.get("dokumentbeskrivelser")),
        "url": f"{saks_url}&jid={quote(jp.get('id') or '', safe='')}",
    }


def _adresse_felter(sakstittel):
    """adresse/gnr_bnr/matrikkelnr utledet fra én sakstittel - delt av
    build_sak og parent_sak_ref, som begge trenger nøyaktig denne triaden."""
    gnr_bnr = extract_gnr_bnr(sakstittel)
    return {
        "adresse": extract_adresse(sakstittel),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": build_matrikkelnr(gnr_bnr),
    }


def build_sak(source, detail):
    external_id = source.get("externalId")
    saksnr = source.get("saksnummer")
    saks_url = f"{PORTAL_SAK_URL}?id={quote(external_id, safe='')}" if external_id else None

    journalposter = []
    if detail:
        for jp in detail.get("journalposter", []) or []:
            journalposter.append(build_journalpost(jp, saks_url))

    sakstittel = source.get("offentligTittel")

    return {
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "identifier": external_id,
        "saksnummer": saksnr,
        "sakstittel": sakstittel,
        **_adresse_felter(sakstittel),
        "publisert_dato": source.get("publisertDato"),
        "url_einnsyn": saks_url,
        "n_jp": len(journalposter),
        "journalposter": journalposter,
    }


def parent_sak_ref(parent):
    """Metadata om en eldre forelder-sak (som fikk en ny journalpost) - hele
    Saksmappe-objektet ligger ferdig i søketreffets "parent"-felt."""
    if not parent:
        return None
    external_id = parent.get("externalId")
    saksnr = parent.get("saksnummer")
    saks_url = f"{PORTAL_SAK_URL}?id={quote(external_id, safe='')}" if external_id else None
    tittel = parent.get("offentligTittel")
    return {
        "identifier": external_id,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnr,
        "sakstittel": tittel,
        **_adresse_felter(tittel),
        "url": saks_url,
    }


def jp_hit_to_info(source, saks_url):
    korr = source.get("korrespondansepart", []) or []
    avsender, mottaker = [], []
    for k in korr:
        navn = k.get("korrespondansepartNavn")
        if not navn:
            continue
        if (k.get("korrespondanseparttype") or "").endswith("mottaker"):
            mottaker.append(navn)
        else:
            avsender.append(navn)
    return {
        "identifier": source.get("id"),
        "jp_nr": source.get("journalpostnummer"),
        "tittel": source.get("offentligTittel"),
        "type": source.get("journalposttype"),
        "dato": source.get("dokumentetsDato") or source.get("journaldato"),
        "journalpostdato": source.get("journaldato"),
        "avsender": avsender,
        "mottaker": mottaker,
        "vedlegg": build_vedlegg(source.get("dokumentbeskrivelse")),
        "url": f"{saks_url}&jid={quote(source.get('id') or '', safe='')}" if saks_url else None,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump (publisert-2024-today_dump) - kronologisk chunking +
# JSONL-sjekkpunktfil (trygt å avbryte/gjenoppta).
# --------------------------------------------------------------------------- #
def _chunk_range(start, end, max_days=180):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _load_checkpoint_ids(checkpoint_file):
    if not checkpoint_file.exists():
        return set()
    ids = set()
    with open(checkpoint_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ids.add(json.loads(line).get("identifier"))
            except Exception:  # noqa: BLE001
                continue
    return ids


def _consolidate_checkpoint(checkpoint_file, output_file):
    """Skriver sjekkpunktfila (JSONL) ut som én JSON-liste. Fila er append-only,
    så en sak som er hentet på nytt står der flere ganger - siste linje vinner,
    mens saken beholder plassen si fra første gang den ble skrevet."""
    if not checkpoint_file.exists():
        return 0
    saker = {}
    with open(checkpoint_file, encoding="utf-8") as f:
        for linjenr, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            sak = json.loads(line)
            # Saker uten identifier kan ikke dedupliseres - gi dem hver sin nøkkel
            saker[sak.get("identifier") or f"__linje_{linjenr}"] = sak
    output_file.write_text(json.dumps(list(saker.values()), ensure_ascii=False, indent=2),
                           encoding="utf-8")
    return len(saker)


def run_full_dump(output_file, checkpoint_file, start=None, end=None, max_saker=None,
                   time_budget=None, chunk_days=180, resume=False):
    """Full historisk dump, kronologisk i biter av chunk_days dager.

    Hele det forespurte intervallet hentes på nytt hver kjøring, slik at saker
    som har fått nye journalposter siden sist blir oppdatert (samme konvensjon
    som innsynsportal-kommunene). Sjekkpunktfila er append-only, og
    _consolidate_checkpoint lar siste versjon av en sak vinne.

    resume=True hopper i stedet over saker som allerede ligger i sjekkpunkt-
    filen. Bruk det når en flerårs-backfill skal fortsette der forrige kjøring
    slapp - ikke når ferske saker skal oppdateres, siden journalpostlista da
    fryses på verdien saken hadde ved første henting.

    max_saker/time_budget: valgfrie grenser for denne kjøringen."""
    session = make_session()
    if start is None:
        start = START_DATE
    if end is None:
        end = date.today()

    kjente_ids = _load_checkpoint_ids(checkpoint_file)
    # done_ids styrer både hva som hoppes over og hva max_saker teller som nytt.
    # Uten resume starter det tomt, slik at alt i intervallet hentes på nytt.
    done_ids = set(kjente_ids) if resume else set()
    print(f"{len(kjente_ids)} saker i sjekkpunktfilen fra før"
          + (" - hopper over dem (resume)." if resume else " - hentes på nytt."))
    n_new_this_run = 0
    t0 = time.time()

    with open(checkpoint_file, "a", encoding="utf-8") as ckpt:
        for frm, til in _chunk_range(start, end, max_days=chunk_days):
            if max_saker is not None and n_new_this_run >= max_saker:
                print(f"Nådde grensen på {max_saker} nye saker for denne kjøringen - stopper her.")
                break
            if time_budget is not None and (time.time() - t0) >= time_budget:
                print(f"Nådde tidsbudsjettet på {time_budget}s for denne kjøringen - stopper her.")
                break
            print(f"Henter {frm.isoformat()}..{til.isoformat()}…")
            need = (max_saker - n_new_this_run) if max_saker is not None else None
            try:
                sources = fetch_range(session, frm.isoformat(), til.isoformat(), done_ids=done_ids, need=need)
            except Exception as e:  # noqa: BLE001
                print(f"  FEIL ved henting av {frm}..{til}: {e}")
                continue

            sources = [s for s in sources if s.get("externalId") not in done_ids]
            if max_saker is not None:
                sources = sources[: max(0, max_saker - n_new_this_run)]
            if not sources:
                print("  (alle saker i dette vinduet er allerede hentet fra før)")
                continue
            print(f"  {len(sources)} nye saker funnet, henter journalpostlister…")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                details = list(ex.map(lambda s: fetch_detail(session, s.get("externalId")), sources))

            for source, detail in zip(sources, details):
                sak = build_sak(source, detail)
                ckpt.write(json.dumps(sak, ensure_ascii=False) + "\n")
                ckpt.flush()
                done_ids.add(sak["identifier"])
                n_new_this_run += 1

    total = _consolidate_checkpoint(checkpoint_file, output_file)
    print(f"\nDenne kjøringen hentet {n_new_this_run} nye saker på {time.time() - t0:.0f}s. "
          f"Sjekkpunktfilen har nå {total} saker totalt, konsolidert til {output_file}")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - datovindu + seen-dedup i state.json +
# auto-backfill.
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 5
MAX_BACKFILL_DAYS = 30
SEEN_RETENTION_DAYS = 14


def _build_saks_url(external_id):
    return f"{PORTAL_SAK_URL}?id={quote(external_id, safe='')}" if external_id else None


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


def prune_seen(state, today, seen_retention_days=SEEN_RETENTION_DAYS):
    cutoff = (today - timedelta(days=seen_retention_days)).isoformat()
    state["seen_saker"] = {k: v for k, v in state["seen_saker"].items() if v >= cutoff}
    state["seen_journalposter"] = {k: v for k, v in state["seen_journalposter"].items() if v >= cutoff}


def write_output(nye_saker, nye_journalposter, ref_date, output_dir, log_fn=print):
    output_dir.mkdir(exist_ok=True)
    (output_dir / f"saker_{ref_date}.json").write_text(
        json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"journalposter_{ref_date}.json").write_text(
        json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")
    log_fn(f"Skrev {len(nye_saker)} nye saker og "
           f"{len(nye_journalposter)} saker med nye journalposter -> {output_dir}")


def run_for_date(session, ref_date, state, output_dir, log_fn=print):
    """Én dags datovindu: hent, skill Saksmappe/Journalpost-treff, dedup mot
    state, bygg poster og skriv output."""
    from_iso = (ref_date - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    to_iso = ref_date.isoformat()
    log_fn(f"Datovindu: {from_iso} .. {to_iso}")

    sources = fetch_window(session, from_iso, to_iso)
    log_fn(f"  {len(sources)} treff i rå-vinduet (før dedup)")

    seen_saker = state["seen_saker"]
    seen_jp = state["seen_journalposter"]

    sak_hits, jp_hits = [], []
    for s in sources:
        typ = (s.get("type") or [None])[0]
        if typ == "Saksmappe":
            sak_hits.append(s)
        elif typ == "Journalpost":
            jp_hits.append(s)

    nye_sak_sources = [s for s in sak_hits if s.get("externalId") not in seen_saker]
    nye_sak_ids = {s.get("externalId") for s in nye_sak_sources}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        details = list(ex.map(lambda s: fetch_detail(session, s.get("externalId")), nye_sak_sources))
    nye_saker = [build_sak(s, d) for s, d in zip(nye_sak_sources, details)]

    by_parent = {}
    for jp in jp_hits:
        if jp.get("id") in seen_jp:
            continue
        parent = jp.get("parent") or {}
        parent_id = parent.get("externalId")
        if not parent_id or parent_id in nye_sak_ids:
            continue
        by_parent.setdefault(parent_id, {"parent": parent, "jp": []})
        by_parent[parent_id]["jp"].append(jp)

    nye_journalposter = []
    for entry in by_parent.values():
        parent = entry["parent"]
        saks_url = _build_saks_url(parent.get("externalId"))
        nye_jp_liste = [jp_hit_to_info(jp, saks_url) for jp in entry["jp"]]
        nye_journalposter.append({"parent_sak": parent_sak_ref(parent), "nye_journalposter": nye_jp_liste})

    write_output(nye_saker, nye_journalposter, to_iso, output_dir, log_fn=log_fn)

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


def run_daily(output_dir, state_file, day_back=1, log_fn=print):
    """Daglig endringslogg med auto-backfill: kjører én dag om gangen fra
    dagen etter siste vellykkede kjøring og frem til target (i dag - day_back),
    begrenset til MAX_BACKFILL_DAYS. Stopper (og lar last_success_date stå
    uendret) hvis en dag feiler, slik at neste kjøring prøver den samme
    dagen på nytt."""
    state = load_state(state_file)
    today = datetime.now(ZoneInfo("Europe/Oslo")).date()
    target = today - timedelta(days=day_back)

    if state["last_success_date"]:
        start = datetime.fromisoformat(state["last_success_date"]).date() + timedelta(days=1)
    else:
        start = target

    if start > target:
        log_fn(f"Ingenting å gjøre – siste vellykkede dag ({state['last_success_date']}) "
               f"er allerede >= mål ({target}).")
        return

    dager = []
    d = start
    while d <= target:
        dager.append(d)
        d += timedelta(days=1)

    if len(dager) > MAX_BACKFILL_DAYS:
        log_fn(f"ADVARSEL: {len(dager)} dager mangler siden siste kjøring – begrenser "
               f"auto-backfill til de {MAX_BACKFILL_DAYS} nyeste.")
        dager = dager[-MAX_BACKFILL_DAYS:]

    if len(dager) > 1:
        log_fn(f"Backfiller {len(dager)} manglende dag(er): {dager[0]} .. {dager[-1]}")

    session = make_session()
    for ref_date in dager:
        try:
            n_saker, n_jp = run_for_date(session, ref_date, state, output_dir, log_fn=log_fn)
            state["last_success_date"] = ref_date.isoformat()
            prune_seen(state, today, SEEN_RETENTION_DAYS)
            save_state(state, state_file)
            log_fn(f"OK {ref_date}: {n_saker} nye saker, {n_jp} saker med nye journalposter")
        except Exception:  # noqa: BLE001
            log_fn(f"FEIL under kjøring for {ref_date}:\n{traceback.format_exc()}")
            log_fn(f"Stopper her. Siste vellykkede dag forblir {state['last_success_date']}.")
            raise
