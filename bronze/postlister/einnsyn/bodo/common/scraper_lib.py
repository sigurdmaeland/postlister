"""Delt bibliotek for Bodø byggesak-scraperen.

Bodø har ingen egen postliste-portal - byggesaker hentes fra den nasjonale
eInnsyn-plattformen (https://einnsyn.no), samme API som Oslo bruker:
/api/result (søk) og /api/v2/saksmappe (journalpostliste per sak). Til
forskjell fra Oslo (der "Plan- og bygningsetaten"-virksomheten inneholder
mye annet enn byggesaker og krever en EXCLUDE_TERM-liste for å luke bort
reguleringsplaner/kommuneplaner/tilsyn osv.) er Bodøs virksomhet-ID
("Byggesaksavdelingen", ikke hele PBE-ekvivalenten) tydeligvis smalere -
et rent "type=Saksmappe + arkivskaperTransitive=<virksomhet>"-søk (uten
noen searchTerms-ekskludering) ga 9490 treff som stikkprøve (300 tilfeldige
titler på tvers av hele arkivet) bekreftet er byggesaker, ingen støy av
typen Oslo måtte filtrere bort. Se derfor INGEN "searchTerms"-nøkkel i
body_base her, i motsetning til Oslo sin EXCLUDE_TERM.

VIKTIG OM DATOER: publisertDato for ALLE 9490 treff starter 2024-06-17
(bekreftet ved ASC-sortert søk - de eldste treffene har publisertDato
innenfor samme få minutter den dagen). Dette er tydeligvis datoen Bodø sitt
arkiv ble migrert/publisert til eInnsyn, IKKE når sakene faktisk ble
opprettet - saksnummer i selve tittelen går mye lenger tilbake (sett
eksempler helt til "2001/16", "07/3905", "09/4906" i stikkprøven). Kronologisk
chunking (som i Oslo sin run_full_dump) bruker derfor START_DATE=2024-06-01
(rund margin før den faktiske migreringsdatoen) - selve sak-historikken
ligger likevel tilgjengelig via publisertDato-vinduet, bare klumpet sammen
rundt migreringstidspunktet i stedet for jevnt fordelt over årene sakene
faktisk stammer fra.

ADRESSEPARSING - samme segment-for-segment-strategi som Oslo bruker
(_finn_skille finner reelle "-"/"–"-skiller, til forskjell fra tallspenn),
MEN valideringen (_er_gyldig) er IKKE en kuratert liste med kjente
gate-endelser slik Oslo/Fredrikstad gjør det. Bodø-arkivet har for stor
variasjon i egennavn ("Sprinten", "Kvilut", "Ørntuva", "Bukken Bruse" osv. -
stedsnavn uten noen felles endelse) til at en endelse-liste holder (bekreftet
av konkret feilrapport med 11 eksempler: både reelle adresser som ikke ble
funnet, OG feilaktig godkjente ikke-adresser). Validering er derfor lagt om
til samme STRUKTURELLE mønster som innsynsportal-familien (Trondheim/Tromsø/
Drammen) bruker: krev stor forbokstav + at kandidaten SLUTTER i et
husnummer(-aktig) mønster, se _er_gyldig/_ENDS_WITH_HUSNR. _rens_kandidat
strips i tillegg: trailing saksreferanse i parentes ("(2019/3672)"), og en
ledende navnedel før komma uten tall ("Ole Chr. Simonsen, Kongsdatterveien 8"
-> "Kongsdatterveien 8") - se _LEADING_NAVN_KOMMA.

GNR/BNR - Bodø-titler bruker (i tillegg til Oslos rene "gnr/bnr"-skråstrek-
form og fulle matrikkelnummer "1804-gnr/bnr/feste/seksjon") også en merket
"Gnr. N bnr. N"-form ("Gnr. 124 bnr. 92 og Gnr. 124 bnr. 80") - dekkes av
_GNR_BNR_LABELED_RE, maskert bort før den rene skråstrek-formen prøves for
å unngå dobbelttelling.

VEDLEGG: i motsetning til Oslo (som IKKE har reell vedleggstilgang gjennom
eInnsyn) har Bodø det - bekreftet direkte ved å laste ned en ekte fil via
/api/v2/fil (200 OK, application/pdf). Samme mønster som Fredrikstad:
build_vedlegg() leser en journalposts dokumentbeskrivelser -> dokumentobjekter
og bygger en nedlastbar URL per fil (ingen autentisering kreves). Feltnavn
varierer mellom de to eInnsyn-endepunktene (se build_vedlegg-docstring).

Brukes av:
  - 2024-today_dump/bygg/main.py   (engangs historisk dump)
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

VIRKSOMHET_ID = "http://data.einnsyn.no/virksomhet/79325c4b-83be-4937-b459-c7be1680626e"  # Bodø - Byggesaksavdelingen

KOMMUNE = "Bodø"
KOMMUNE_NR = "1804"

START_DATE = date(2024, 6, 1)  # rund margin før faktisk migreringsdato 2024-06-17 (se modul-docstring)

PAGE_SIZE = 50  # API-et gir aldri flere treff enn dette per side
MAX_WORKERS = 10
TIMEOUT = 30
RETRIES = 4

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://einnsyn.no",
    "Referer": "https://einnsyn.no/",
}


# --------------------------------------------------------------------------- #
# Adresseparsing
# --------------------------------------------------------------------------- #
# Adressen står vanligvis først i tittelen, adskilt med " - " fra resten
# (f.eks. "Sprinten 3B - Endring av bruk - ..."), men et vanlig mønster i
# Bodø-data er også "Beskrivelse - Navn - Gateadresse[ - saksnr]" (adressen i
# et SENERE segment) - samme flersegment-strategi som Oslo håndterer dette
# med (prøv opptil _MAKS_SEGMENTER segmenter før vi gir opp).
#
# " - " kan IKKE brukes som naiv separator alene: adressen selv inneholder
# ofte et tallspenn med mellomrom rundt bindestreken ("Åmotveien 9 - 11") som
# må holdes samlet, ikke kuttes midt i. Se _finn_skille for hvordan reelle
# skiller kjennes fra tallspenn.
_NO_ADDRESS_PATTERNS = [
    "forhåndskonferanse", "spørsmål om", "høring", "kartlegging", "innsynsbegjæring",
    "kundeveiledning", "veiledning", "ingen adresse", "ukjent adresse", "bestilling",
    "tilsyn med", "anmodning om",
]
# Ord som ALDRI innleder et reelt gate-/stedsnavn i Bodø-data, men som (i
# motsetning til _NO_ADDRESS_PATTERNS over) selv kan ende i et tall og dermed
# ellers ville sluppet gjennom _ENDS_WITH_HUSNR - typisk en byggearbeid-
# beskrivelse som tilfeldigvis ender i en tallreferanse ("Støpt båtopptrekk
# foran naust 2", "Riving av garasje 3"), ikke en adresse.
_GENERISKE_STARTORD = {
    "støpt", "støp", "riving", "nytt", "graving", "sprengning", "montering",
    "oppføring", "etablering", "utbedring", "oppgradering", "tilbygg", "påbygg",
    "bygging", "oppussing", "rehabilitering",
}

_TRAILING_GBNR = re.compile(r"\s*-\s*\d+/\s*\d+\s*$")
# Saksnummer ("YYYY/NNNNN") limt rett inntil adressen uten eget skille, f.eks.
# "Innsynsbegjæring - 2026/22197 Mjønesveien 110" - _finn_skille kutter riktig
# ved "-" etter "Innsynsbegjæring", men igjen står "2026/22197 Mjønesveien
# 110" som ETT segment (siden det ikke er noe "-" mellom saksnr og adresse).
_LEADING_SAKSNR = re.compile(r"^\d{4}/\d{1,6}\s+")
_MAKS_SEGMENTER = 5


def _finn_skille(tittel):
    """Finn FØRSTE '-' som er en reell separator mellom adresse og resten -
    IKKE del av et tallspenn ('9 - 11'), et bokstav-spenn med tall-anker
    ('15A - B'), et sammensatt ord uten mellomrom på noen av sidene, en
    initial ('P. T. Mallings') eller en forkortelse med skråstrek. Ved en
    lengre tall-liste (3+ ledd) holdes kun gate+FØRSTE tall igjen."""
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
        left_m = re.search(r"(\d+\s?[A-Za-zæøåÆØÅ]?)\s*$", left)
        right_m = re.match(r"\s*(\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ])\b(?![./])", right)
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


_NR_BOKSTAV_MELLOMROM = re.compile(r"(\d)\s+([A-Za-zÆØÅæøå])\b")
_TRAILING_ORD_ETTER_KOMMA = re.compile(r",\s*[A-ZÆØÅ][a-zæøå]{2,}$")
# Saksreferanse i parentes helt til slutt, f.eks. "Tårnvikveien 36 (2019/3672)"
# -> "Tårnvikveien 36" - dette er en henvisning til en tidligere sak, ikke en
# del av selve adressen.
_TRAILING_PAREN = re.compile(r"\s*\([^)]*\)\s*$")
# Ledende navnedel før komma UTEN tall, f.eks. "Ole Chr. Simonsen,
# Kongsdatterveien 8" eller "Meby Frode og Holmen Svenn Arne, General
# Fleischers gate 16a og 16b" -> navnedelen (aldri en adresse i seg selv,
# siden den ikke inneholder noe husnummer) fjernes og bare resten etter
# kommaet valideres videre. Adresselister som "Sundveien 18A, B, C" har
# derimot ALLTID et tall i den første kommadelen og rammes ikke av dette.
_LEADING_NAVN_KOMMA = re.compile(r"^([^,\d]+),\s*(.+)$")


def _rens_kandidat(kandidat):
    kandidat = kandidat.strip()
    if "kommune - " in kandidat:
        kandidat = kandidat.split("kommune - ", 1)[1].strip()
    if kandidat[:4].lower() == "ved ":
        kandidat = kandidat[4:].strip()
    kandidat = _LEADING_SAKSNR.sub("", kandidat).strip()
    # Kjent "ingen adresse her"-frase innledningsvis, men med en reell adresse
    # RETT ETTER innenfor samme segment ("Forhåndskonferanse Grensen 3A") -
    # strip frasen og valider resten, i stedet for å bare avvise hele greia.
    lav_start = kandidat.lower()
    for p in _NO_ADDRESS_PATTERNS:
        if lav_start.startswith(p):
            kandidat = kandidat[len(p):].strip(" :-")
            break
    navn_m = _LEADING_NAVN_KOMMA.match(kandidat)
    if navn_m:
        kandidat = navn_m.group(2).strip()
    kandidat = _TRAILING_GBNR.sub("", kandidat).strip()
    kandidat = _TRAILING_PAREN.sub("", kandidat).strip()
    kandidat = _TRAILING_ORD_ETTER_KOMMA.sub("", kandidat).strip()
    kandidat = _NR_BOKSTAV_MELLOMROM.sub(r"\1\2", kandidat)
    return re.sub(r"\s+", " ", kandidat).strip()


_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")
# Kandidaten må SLUTTE i et husnummer(-aktig) mønster: tall (+ evt. én
# bokstav), eller en "og <bokstav>"/", <bokstav>"-fortsettelse av et tall
# nevnt tidligere i kandidaten ("...vei 1A og B"). Dette erstatter den
# gate-endelse-baserte sjekken (se modul-docstring) - langt mer robust mot
# Bodøs store variasjon i egennavn, og luker samtidig ut bade rene
# beskrivelser ("...under 70 m²" slutter ikke i et husnummer) og bare
# stedsnavn uten husnummer ("Hålogalandsgata" alene - se feilrapport).
_ENDS_WITH_HUSNR = re.compile(r"(\d+\s?[A-Za-zæøåÆØÅ]?|(?:og|,)\s+[A-Za-zæøåÆØÅ])\s*$")
# "felt B2-1"/"felt B21" er en regulerings-/tomtebetegnelse (delområde i en
# utbygging), ikke en postadresse - selv om den strukturelt sett kan ende i
# et tall og dermed ellers ville sluppet gjennom _ENDS_WITH_HUSNR ("Oksebakken
# felt B2-1", se feilrapport). \b...\b treffer IKKE sammensatte ord som
# "boligfelt", bare det bare, frittstående ordet "felt".
_INNEHOLDER_FELT = re.compile(r"\bfelt\b", re.IGNORECASE)


def _er_gyldig(kandidat):
    if not kandidat:
        return False
    lav = kandidat.lower()
    if any(lav.startswith(p) for p in _NO_ADDRESS_PATTERNS):
        return False
    ord_liste = lav.split()
    if ord_liste and ord_liste[0].strip(".,:;") in _GENERISKE_STARTORD:
        return False
    if _INNEHOLDER_FELT.search(lav):
        return False
    if not _STARTS_UPPER.match(kandidat):
        return False
    if not _ENDS_WITH_HUSNR.search(kandidat):
        return False
    return True


# --------------------------------------------------------------------------- #
# Flere adresser i samme kandidat ("Sneveien 71 og 73", "vei 1A og B",
# "Forskerveien 1, 5, 7, 9 ...") - samme grunnmotor som innsynsportal-familien
# (Trondheim/Tromsø/Drammen) bruker for adresselister, tilpasset til å
# splitte på BÅDE komma og " og " (Bodø-titler bruker begge om hverandre).
# Resultatet er "; "-adskilt, samme konvensjon som der - IKKE en Python-liste,
# for å holde "adresse"-feltets type (streng|null) konsistent på tvers av
# alle kommuner i prosjektet.
# --------------------------------------------------------------------------- #
_HUSNR_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN_FULL = re.compile(rf"^{_HUSNR_TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_GATE_FORSTE_TOKEN = re.compile(rf"^(.+?)\s+({_HUSNR_TOKEN})\b")
_MULTI_SPLIT = re.compile(r"\s*(,|\bog\b)\s*", re.IGNORECASE)
# Sekundært fallback - se _split_ulike_gater.
_DEL_RE = re.compile(r"\s*,\s*|\s+og\s+", re.IGNORECASE)
_GATE_NR_SEGMENT_RE = re.compile(rf"^[A-ZÆØÅ][\w.\-]*(?:\s[\w.\-]+)*\s{_HUSNR_TOKEN}$")


def _split_ulike_gater(kandidat):
    """Sekundært fallback til _split_multi_adresse (kalles fra dennes
    bail-out-punkter i stedet for å gi opp direkte): dekker HELT ULIKE
    gate+nr-par adskilt med komma/"og" ("Måkeveien 6, Måkeveien 8"), til
    forskjell fra hovedmønsteret der (samme gate, kun tallet/bokstaven
    varierer). Krever mellomrom rett før husnummeret i HVERT element, så en
    duplikat-adresse med skrivefeil ("Smed Qvales vei 2, Smed Qvales vei2" -
    reell forekomst i arkivet, mangler mellomrom i det andre) ikke
    feiltolkes som to forskjellige adresser."""
    deler = [d.strip() for d in _DEL_RE.split(kandidat) if d.strip()]
    if len(deler) >= 2 and all(_GATE_NR_SEGMENT_RE.match(d) for d in deler):
        return "; ".join(deler)
    return kandidat


def _split_multi_adresse(kandidat):
    """Utvider en allerede godkjent kandidat til flere fulle adresser hvis
    den inneholder en liste med husnummer/bokstaver - ellers prøves
    _split_ulike_gater (helt andre gate+nr-par) før den gir opp og returnerer
    uendret. Et bart tall/spenn ("73") eller en bar bokstav ("B") arver
    gatenavnet (og for en bar bokstav: forrige tall) fra det første
    elementet."""
    m = _GATE_FORSTE_TOKEN.match(kandidat)
    if not m:
        return _split_ulike_gater(kandidat)
    gate = m.group(1).strip()
    hale = kandidat[len(gate):].strip()
    deler = _MULTI_SPLIT.split(hale)
    parts = [deler[0]] + deler[2::2]
    if len(parts) < 2:
        return _split_ulike_gater(kandidat)

    entries = []
    last_number = None
    for i, part in enumerate(parts):
        part = part.strip()
        if not part:
            return _split_ulike_gater(kandidat)
        if i == 0:
            if not _BARE_TOKEN_FULL.match(part):
                return _split_ulike_gater(kandidat)
            entries.append(f"{gate} {part}")
            mnum = re.match(r"^(\d+)", part)
            last_number = mnum.group(1) if mnum else None
            continue
        if _BARE_LETTER_TOKEN.match(part):
            if last_number is None:
                return _split_ulike_gater(kandidat)
            entries.append(f"{gate} {last_number}{part}")
            continue
        if _BARE_TOKEN_FULL.match(part):
            entries.append(f"{gate} {part}")
            mnum = re.match(r"^(\d+)", part)
            if mnum:
                last_number = mnum.group(1)
            continue
        return _split_ulike_gater(kandidat)

    seen, uniq = set(), []
    for e in entries:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return "; ".join(uniq)


def extract_adresse(sakstittel):
    """Prøver segment for segment (skilt ved _finn_skille) til ett gir en
    gyldig adresse, eller til tittelen er tom for flere segmenter. En
    kandidat med flere husnummer ("Sneveien 71 og 73") utvides til flere
    "; "-adskilte adresser via _split_multi_adresse før den returneres."""
    if not sakstittel:
        return None
    gjenstaende = sakstittel
    for _ in range(_MAKS_SEGMENTER):
        head, rest = _finn_skille(gjenstaende.strip())
        kandidat = _rens_kandidat(head)
        if _er_gyldig(kandidat):
            return _split_multi_adresse(kandidat)
        if not rest.strip():
            break
        gjenstaende = rest
    return None


# Gårds-/bruksnummer ("gnr/bnr") skrevet direkte i sakstittelen - HELT
# uavhengig av extract_adresse(). To former forekommer: ren skråstrek
# ("124/92") og en merket "Gnr. N bnr. N"-form ("Gnr. 124 bnr. 92 og Gnr.
# 124 bnr. 80") - sistnevnte maskeres bort før skråstrek-formen prøves, for
# å unngå dobbelttelling og feiltolkning av "N bnr. N" som et vanlig tallpar.
_GNR_BNR_RE = re.compile(r"\b(\d{1,4})/(\d{1,5})\b")
_GNR_BNR_LABELED_RE = re.compile(r"[Gg]nr\.?\s*(\d{1,4})\s*[Bb]nr\.?\s*(\d{1,5})")
_GNR_BNR_AARSTALL_MIN, _GNR_BNR_AARSTALL_MAKS = 2010, 2099
_GNR_BNR_BLOKKORD = {
    "sak", "saken", "saka", "byggesak", "byggesaken", "arkivsak",
    "arkivsaken", "spørsmål", "referanse", "ref", "anlnr", "journalpost",
}
_ORD_FOR_TALLPAR_RE = re.compile(r"([a-zæøåA-ZÆØÅ]+)[:\s]*$")
# Fullt matrikkelnummer ("knr-gnr/bnr/festenr/seksjonsnr", f.eks.
# "1804-138/4792/0/0") - festenr/seksjonsnr-halene må ikke tolkes som egne,
# ekstra gnr/bnr-par. Maskes bort først.
_MATRIKKELNR_FULL_RE = re.compile(r"\b\d{4}-(\d{1,4})/(\d{1,5})(?:/\d{1,5})*\b")


def extract_gnr_bnr(sakstittel):
    """Alle gnr/bnr-par funnet i tittelen, i rekkefølge, uten duplikater.
    Returnerer None hvis ingen funnet (mest titler har bare en gateadresse
    og ikke gnr/bnr skrevet ut)."""
    if not sakstittel:
        return None
    par_liste = []

    def _legg_til(gnr, bnr):
        par = f"{gnr}/{bnr}"
        if par not in par_liste:
            par_liste.append(par)

    maskert = sakstittel
    for m in _MATRIKKELNR_FULL_RE.finditer(sakstittel):
        _legg_til(m.group(1), m.group(2))
        maskert = maskert[: m.start()] + " " * (m.end() - m.start()) + maskert[m.end():]

    for m in _GNR_BNR_LABELED_RE.finditer(maskert):
        _legg_til(m.group(1), m.group(2))
        maskert = maskert[: m.start()] + " " * (m.end() - m.start()) + maskert[m.end():]

    for m in _GNR_BNR_RE.finditer(maskert):
        gnr, bnr = m.group(1), m.group(2)
        if any(_GNR_BNR_AARSTALL_MIN <= int(tall) <= _GNR_BNR_AARSTALL_MAKS for tall in (gnr, bnr)):
            continue
        foran = maskert[: m.start()]
        ord_m = _ORD_FOR_TALLPAR_RE.search(foran)
        if ord_m and ord_m.group(1).lower() in _GNR_BNR_BLOKKORD:
            continue
        _legg_til(gnr, bnr)

    return par_liste or None


def build_matrikkelnr(gnr_bnr_liste):
    """Fullt matrikkelnummer ("knr-gnr/bnr") for hvert gnr/bnr-par - festenr/
    seksjonsnr er ikke tilgjengelig fra sakstittelen alene (utenom når
    tittelen allerede skriver det fullt ut, se _MATRIKKELNR_FULL_RE) og
    utelates ellers."""
    if not gnr_bnr_liste:
        return None
    return [f"{KOMMUNE_NR}-{par}" for par in gnr_bnr_liste]


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _with_retries(do_request):
    """Kjører do_request() med opptil RETRIES forsøk (økende ventetid mellom
    hver) - delt retry-logikk for _post og _get."""
    last_err = None
    for attempt in range(RETRIES):
        try:
            return do_request()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def _post(session, body):
    def do_request():
        r = session.post(RESULT_URL, data=json.dumps(body), timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    return _with_retries(do_request)


def _get(session, url):
    def do_request():
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    return _with_retries(do_request)


def fetch_detail(session, external_id):
    """Full journalpostliste for én sak via /api/v2/saksmappe."""
    url = f"{DETAIL_URL}?iri={quote(external_id, safe='')}"
    try:
        return _get(session, url)
    except Exception:  # noqa: BLE001
        return None


def _fetch_details_parallel(session, sources):
    """fetch_detail() for flere saker samtidig (MAX_WORKERS tråder) - brukes
    av både run_full_dump og run_for_date. Rekkefølgen i resultatet matcher
    sources."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        return list(ex.map(lambda s: fetch_detail(session, s.get("externalId")), sources))


def _paginate(session, body_base, stop_check=None):
    """Henter alle sider av et søk (body_base + size/offset) til alle treff
    er samlet opp, eller til stop_check(items) (kalt før hver ny side
    hentes) returnerer True. Delt paginering for fetch_range/fetch_window."""
    first = _post(session, {**body_base, "size": PAGE_SIZE, "offset": 0})
    total = first.get("hitCount", 0)
    items = list(first.get("searchHits", []))
    offset = PAGE_SIZE
    while offset < total:
        if stop_check is not None and stop_check(items):
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
    brukes av run_full_dump. INGEN searchTerms-ekskludering (se modul-
    docstring - Bodøs virksomhet-ID er smal nok uten). done_ids/need:
    stopper paginering så snart `need` treff som IKKE allerede finnes i
    done_ids er samlet opp."""
    filters = [
        {"fieldName": "type", "fieldValue": ["Saksmappe"], "type": "termQueryFilter"},
        {"fieldName": "type", "fieldValue": ["JournalpostForMøte"], "type": "notQueryFilter"},
        {"fieldName": "arkivskaperTransitive", "fieldValue": [VIRKSOMHET_ID], "type": "postQueryFilter"},
        {"to": f"{to_iso}||/d", "from": f"{from_iso}||/d", "fieldName": "publisertDato", "type": "rangeQueryFilter"},
    ]
    body_base = {
        "appliedFilters": filters,
        "sort": {"fieldName": "publisertDato", "order": "DESC", "id": "published"},
    }

    stop_check = None
    if need is not None and done_ids is not None:
        def stop_check(items):  # noqa: E731
            n_new = sum(1 for h in items if (h.get("source") or {}).get("externalId") not in done_ids)
            return n_new >= need

    return _paginate(session, body_base, stop_check=stop_check)


def fetch_window(session, from_iso, to_iso):
    """Alle Saksmappe- OG Journalpost-treff (ekskl. JournalpostForMøte)
    publisert i [from_iso, to_iso] - brukes av run_daily (se Oslo-modulens
    docstring for hvorfor begge treff-typer trengs her, men ikke i
    run_full_dump)."""
    filters = [
        {"fieldName": "type", "fieldValue": ["JournalpostForMøte"], "type": "notQueryFilter"},
        {"fieldName": "arkivskaperTransitive", "fieldValue": [VIRKSOMHET_ID], "type": "postQueryFilter"},
        {"to": f"{to_iso}||/d", "from": f"{from_iso}||/d", "fieldName": "publisertDato", "type": "rangeQueryFilter"},
    ]
    body_base = {
        "appliedFilters": filters,
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
    direkte fra "url" (GET, ingen autentisering kreves) - bekreftet live mot
    en ekte Bodø-fil (200 OK, application/pdf), samme som Fredrikstad."""
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


def _build_saks_url(external_id):
    return f"{PORTAL_SAK_URL}?id={quote(external_id, safe='')}" if external_id else None


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


def build_sak(source, detail):
    external_id = source.get("externalId")
    saksnr = source.get("saksnummer")
    saks_url = _build_saks_url(external_id)

    journalposter = []
    if detail:
        for jp in detail.get("journalposter", []) or []:
            journalposter.append(build_journalpost(jp, saks_url))

    sakstittel = source.get("offentligTittel")
    gnr_bnr = extract_gnr_bnr(sakstittel)

    return {
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "identifier": external_id,
        "saksnummer": saksnr,
        "sakstittel": sakstittel,
        "adresse": extract_adresse(sakstittel),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": build_matrikkelnr(gnr_bnr),
        "publisert_dato": source.get("publisertDato"),
        "url_einnsyn": saks_url,
        "n_jp": len(journalposter),
        "journalposter": journalposter,
    }


def parent_sak_ref(parent):
    """Metadata om en eldre forelder-sak (som fikk en ny journalpost) - hele
    Saksmappe-objektet ligger ferdig i søketreffets "parent"-felt, ingen
    ekstra API-kall trengs mot eInnsyn."""
    if not parent:
        return None
    external_id = parent.get("externalId")
    saksnr = parent.get("saksnummer")
    saks_url = _build_saks_url(external_id)
    tittel = parent.get("offentligTittel")
    gnr_bnr = extract_gnr_bnr(tittel)
    return {
        "identifier": external_id,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnr,
        "sakstittel": tittel,
        "adresse": extract_adresse(tittel),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": build_matrikkelnr(gnr_bnr),
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
# Engangs historisk dump (2024-today_dump) - kronologisk chunking + JSONL-
# sjekkpunktfil (trygt å avbryte/gjenoppta).
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

            details = _fetch_details_parallel(session, sources)

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

    details = _fetch_details_parallel(session, nye_sak_sources)
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
