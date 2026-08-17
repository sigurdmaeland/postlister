"""Delt bibliotek for Oslo byggesak-scraperen.

Oslo har ingen egen postliste-portal - byggesaker hentes fra den nasjonale
eInnsyn-plattformen (https://einnsyn.no): /api/result (søk) og
/api/v2/saksmappe (journalpostliste per sak). Det finnes ingen egen
sakstype-kode, så byggesaker skilles ut fra resten av "Plan- og
bygningsetaten"-arkivet via EXCLUDE_TERM (tittelbasert ekskludering av
kjente ikke-byggesak-mønstre, hentet fra kommunens egen lagrede
"byggesaker"-filter på einnsyn.no). Ingen saker finnes før 2018.

API-et returnerer maks 50 treff per side ("size" ignoreres), og store
datovinduer gir upålitelige/trege søk - engangsdumpen henter derfor i
kronologiske biter (chunk_days) med en gjenopptakbar JSONL-sjekkpunktfil.

Vi henter KUN data fra eInnsyn. url_oslo_kommune er en direktelenke til Oslo
kommunes PBE-saksinnsyn (samme saksnummer som eInnsyn, se
build_oslo_kommune_url) - vi skraper ikke selve PBE-siden, kundene klikker
seg inn der selv.

To kjøremåter, forskjellig søkestrategi:
  run_full_dump - kun "Saksmappe"-treff, hele historikken kronologisk.
  run_daily     - BEGGE treff-typer (Saksmappe OG Journalpost, kun
                  "JournalpostForMøte" ekskluderes) i et lite datovindu, for
                  å fange både nye saker og nye journalposter på eldre,
                  allerede kjente saker. Et Journalpost-treff har hele
                  forelder-saken ferdig utfylt i "parent"-feltet - ingen
                  ekstra API-kall trengs for å vite hvilken sak den hører
                  til. publisertDato oppdateres pålitelig og nær sanntid,
                  men et lite overlappende vindu (WINDOW_DAYS) og en
                  seen_saker/seen_journalposter-dedup i state.json brukes
                  som forsikring likevel.

Brukes av:
  - 2018-today_dump/bygg/main.py   (engangs historisk dump)
  - running_daily/bygg/app/main.py (daglig endringslogg)
"""

import json
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://www.einnsyn.no"
RESULT_URL = f"{BASE_URL}/api/result"
DETAIL_URL = f"{BASE_URL}/api/v2/saksmappe"
PORTAL_SAK_URL = f"{BASE_URL}/saksmappe"

VIRKSOMHET_ID = "http://data.einnsyn.no/virksomhet/3419aea8-8e14-41ea-b395-1c8742237c5d"  # Plan- og bygningsetaten

KOMMUNE = "Oslo"
KOMMUNE_NR = "0301"

START_YEAR = 2018  # eldste sak for denne virksomheten

PAGE_SIZE = 50  # API-et gir aldri flere treff enn dette per side
MAX_WORKERS = 10
TIMEOUT = 30
RETRIES = 4

EXCLUDE_TERM = (
    '"opprettelse av grunneiendom", "opprettelse av anleggseiendom", "arealoverføring", '
    '"grensejustering", "arealdel", "kommuneplan", "konsesjonsfrihet", "klagesak", '
    '"forespørsel", "matrikkelen", "retting", "anleggsnummer", "adressering", '
    '"omadressering", "planforslag", "sikkerhetskontroll", "detaljregulering", '
    '"områderegulering", "regulering", "test", "testsak", "delesøknad", "adresseendring", '
    '"oppmålingsforretning", "tilsyn", "målebrev", "innsyn"'
)

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://einnsyn.no",
    "Referer": "https://einnsyn.no/",
}


# --------------------------------------------------------------------------- #
# PBE-lenke
# --------------------------------------------------------------------------- #
_SAKSNR_RE = re.compile(r"^(\d{4})/(\d+)$")
OSLO_KOMMUNE_SAK_URL = "https://innsyn.pbe.oslo.kommune.no/saksinnsyn/casedet.asp?caseno={}"


def build_oslo_kommune_url(saksnr):
    """Direktelenke til PBEs saksinnsyn ut fra eInnsyns saksnummer
    ("ÅÅÅÅ/LØPENR") - PBE bruker samme saksnummer (delt Noark-arkiv). caseno
    = år + løpenummer zero-paddet til 5 siffer, uten skråstrek (f.eks.
    "2021/3336" -> "202103336"). Ren strengoperasjon, ingen HTTP-kall."""
    if not saksnr:
        return None
    m = _SAKSNR_RE.match(saksnr.strip())
    if not m:
        return None
    year, lopenr = m.groups()
    return OSLO_KOMMUNE_SAK_URL.format(year + lopenr.zfill(5))


# --------------------------------------------------------------------------- #
# Adresseparsing
# --------------------------------------------------------------------------- #
# Adressen står vanligvis først i tittelen, adskilt med " - " fra resten
# (f.eks. "Munkerudveien 57 - Bruksendring av kjeller"). Noen titler har ingen
# adresse i det hele tatt (rene fagsystem-/planoppgaver), og noen har adressen
# i et SENERE segment (typisk øy-/områdenavn og gnr/bnr FØRST, gateadresse
# etterpå - f.eks. "Ormøya - 198/145 - Singasteinveien 19 C - ..."), derfor
# prøves flere segmenter (se _MAKS_SEGMENTER) før vi gir opp.
#
# " - " kan IKKE brukes som naiv separator alene: adressen selv inneholder
# ofte et tallspenn med mellomrom rundt bindestreken ("Åmotveien 9 - 11",
# "Sinsenterrassen 18 - 20") som må holdes samlet, ikke kuttes midt i. Se
# _finn_skille for hvordan reelle skiller kjennes fra tallspenn.
_NO_ADDRESS_PATTERNS = [
    "barnas representanter", "handlingsplan", "nytt kgv", "mulig ulovlig",
    "ulovlig ", "deltakelse i", "tverretatlig", "demografi", "konseptvalgutredning",
    "forslag om midlertidig forbud", "futurebuilt", "henvendelser til", "spørsmål til",
    "ingen adresse", "ukjent adresse", "forhåndskonferanse", "bestilling",
]
# "li" er utelatt (for kort/generisk - matcher vanlige ord som "juli", ikke
# bare gatenavn). "veita"/"lia"/"gangen"/"hellinga"/"sletta" er lagt til -
# reelle gatenavn-endelser i ekte Oslo-data som "vei(en)"/"gang"/"bakken"
# alene ikke fanger opp.
_STREET_TYPES = [
    "vei", "veg", "veien", "vegen", "veita", "lia", "gate", "gata", "gaten",
    "allé", "alléen", "allée", "plass", "plassen", "terrasse", "terrassen",
    "sving", "svingen", "vingen", "faret", "kollen", "bakken", "grensa",
    "grensen", "gård", "stien", "åsen", "gang", "gangen", "leir", "leiret",
    "hagan", "stubb", "stubben", "haugen", "naret", "kloa", "bølgen", "enga",
    "åsveien", "hellinga", "sletta",
    # Lagt til etter funn i ekte Oslo-data (se _er_gyldig sin fallback for
    # gatenavn uten NOEN kjent endelse, som dekker resten av dette problemet
    # mer generelt - disse er likevel lagt til fordi de er ekte, gjenbrukbare
    # gatetype-ord, ikke bare enkeltstående titler):
    "aveny", "kroken", "stredet", "hagen", "skrenten", "høgda", "brygge",
    "bryggen", "promenaden", "tråkket", "smutt", "smutten",
]

_ORD_RE = re.compile(r"[a-zæøåéA-ZÆØÅÉ]+")
_TRAILING_GBNR = re.compile(r"\s*-\s*\d+/\s*\d+\s*$")
_MAKS_SEGMENTER = 5


def _finn_skille(tittel):
    """Finn FØRSTE '-' som er en reell separator mellom adresse og resten -
    IKKE del av et tallspenn ('9 - 11'), et bokstav-spenn med tall-anker
    ('15A - B'), et sammensatt ord uten mellomrom på noen av sidene
    ('Riiser-Larsens vei'), en initial ('P. T. Mallings') eller en
    forkortelse med skråstrek ('r/e-bekreftelse'). Ved en lengre tall-liste
    (3+ ledd) holdes kun gate+FØRSTE tall igjen."""
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
        # Venstre side MÅ ha et tall rett før (evt. med én bokstav rett
        # etter) - en bar bokstav ALENE (uten tall) teller ikke, ellers ville
        # ord som "...ering" (Reseksjonering) blitt lest som en spenn-bokstav.
        left_m = re.search(r"(\d+\s?[A-Za-zæøåÆØÅ]?)\s*$", left)
        # Høyre side: tall, ELLER én bokstav som IKKE er en initial (etterfulgt
        # av punktum) eller en forkortelse (etterfulgt av skråstrek).
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
# Komma (i motsetning til "-"/"–") behandles ikke som et skille i
# _finn_skille, så et ord rett etter et komma innenfor kandidat-segmentet blir
# stående med mindre det fjernes eksplisitt - f.eks. "Trondheimsveien 2,
# Bruksendring - innredning..." eller "Diakonveien 9, Oslo - Gnr/Bnr-37/152"
# gir ellers "Trondheimsveien 2, Bruksendring"/"Diakonveien 9, Oslo" som
# adresse. Krever minst 2 bokstaver etter forbokstaven slik at en
# enhetsliste med enkeltbokstav-suffikser ("18 A, B, C") IKKE trimmes bort.
_TRAILING_ORD_ETTER_KOMMA = re.compile(r",\s*[A-ZÆØÅ][a-zæøå]{2,}$")


def _rens_kandidat(kandidat):
    kandidat = kandidat.strip()
    if "kommune - " in kandidat:
        kandidat = kandidat.split("kommune - ", 1)[1].strip()
    if kandidat[:4].lower() == "ved ":
        kandidat = kandidat[4:].strip()
    # Fjern en evt. hengende gnr/bnr-rest ("...16 - 196/69") - oppstår når et
    # gnr/bnr-tall rett etter adressen feiltolkes som en fortsettelse av et
    # tallspenn i _finn_skille (begge er rene tallpar).
    kandidat = _TRAILING_GBNR.sub("", kandidat).strip()
    # Fjern et evt. hengende ord etter komma (se begrunnelse over).
    kandidat = _TRAILING_ORD_ETTER_KOMMA.sub("", kandidat).strip()
    # Fjern mellomrom mellom husnummer og bokstavsuffiks ("26 B" -> "26B",
    # "9 B-C" -> "9B-C") - offisiell adresseform har ingen mellomrom her.
    kandidat = _NR_BOKSTAV_MELLOMROM.sub(r"\1\2", kandidat)
    return re.sub(r"\s+", " ", kandidat).strip()


def _er_gyldig(kandidat):
    if not kandidat:
        return False
    lav = kandidat.lower()
    if any(lav.startswith(p) for p in _NO_ADDRESS_PATTERNS):
        return False
    ord_liste = _ORD_RE.findall(lav)
    har_tall = bool(re.search(r"\d", kandidat))
    har_gatetype = any(any(w.endswith(st) for st in _STREET_TYPES) for w in ord_liste)
    if not har_gatetype:
        # Ingen kjent gatetype-endelse (f.eks. "Stranden 15", "Torget 2") -
        # _STREET_TYPES kan aldri bli komplett (nye/uvanlige gatenavn dukker
        # stadig opp i ekte data), så godta likevel når kandidaten er kort
        # (maks 2 "ordentlige" ord - ett egennavn, evt. et sammensatt), har et
        # husnummer, OG starter med stor bokstav (ikke f.eks. et rent
        # gnr/bnr-tall som "180/81, 180/113 og 180/126", som starter med et
        # siffer). Enkeltbokstaver (fra "22A-B" o.l., der _ORD_RE splitter ut
        # "a"/"b" som egne "ord" siden tallet mellom bryter dem) telles ikke
        # som egne ord her.
        ord_liste_reelle = [w for w in ord_liste if len(w) > 1]
        # Ekskluder kandidater som egentlig er en SAKS-/SPØRSMÅLSREFERANSE, ikke
        # et husnummer - f.eks. "Spørsmål 359/2026" (byrådsspørsmål) eller
        # "Arendalsuka 2026" (arrangementsår). Kjennetegn: et "/"-tall (samme
        # form som gnr/bnr, aldri reelt husnummer i denne tittelformen), ELLER
        # et ENKELT tall som ligner et årstall (samme vindu som brukes i
        # extract_gnr_bnr for å luke ut årstall der). Uten denne sjekken
        # kapres extract_adresse av referansetallet FØR den når den faktiske
        # adressen som ofte står i et senere segment av samme tittel.
        tall_i_kandidat = re.findall(r"\d+", kandidat)
        ser_ut_som_referanse = "/" in kandidat or (
            len(tall_i_kandidat) == 1
            and _GNR_BNR_AARSTALL_MIN <= int(tall_i_kandidat[0]) <= _GNR_BNR_AARSTALL_MAKS
        )
        if ser_ut_som_referanse or not (
            har_tall and len(ord_liste_reelle) <= 2 and kandidat[:1].isalpha() and kandidat[:1].isupper()
        ):
            return False
    # Uten noe tall er kandidaten trolig et rent (kort) gatenavn - eller en
    # hel beskrivelse-setning som tilfeldigvis inneholder et gatenavn-liknende
    # ord et sted. Skiller på lengde.
    if not har_tall and len(ord_liste) > 3:
        return False
    return True


# _finn_skille skiller kun på "-"/"–", ikke komma - en kandidat kan derfor
# fortsatt liste opp FLERE adresser adskilt med komma/"og" ("Trostefaret
# 29A-29B, Svarttrostveien 5", "Vallegata 13, 15 og 17"). To varianter,
# begge skrevet om til "Adresse1; Adresse2..." (samme "; "-konvensjon som
# ellers i prosjektet):
#   1. Samme gate, kun tallet (evt. et tall-/bokstavspenn) varierer.
#   2. Egne gate+nr-par ("Løkkeveien 10-12, Huitfeldts gate 12A-C") - matcher
#      IKKE mønster 1 (tallet der må stå RETT etter komma/"og", ikke etter et
#      nytt gatenavn), så de to er trygt uavhengige av hverandre. Et tall-/
#      bokstavspenn ("29A-29B", "76-86", "12A-C") holdes samlet som ETT
#      element i begge varianter - se _HUSNR_TOKEN.
_HUSNR_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:-(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
# Gatenavn-gruppen (\1) holdes til BOKSTAVER/punktum/bindestrek/mellomrom -
# IKKE \w (som også matcher tall). Uten dette kan gruppen "sluke" et helt
# tall+"og"+neste gatenavn som om det var ett langt gatenavn, f.eks.
# "Bjørnefaret 4 og Grevlinglia 3" ville ellers matchet med gate="Bjørnefaret
# 4 og Grevlinglia" og tall=["3"] - altså INGEN splitt, siden det (feilaktig)
# tolkes som samme gate med ett tall. Med tall utelatt fra gate-gruppen
# stopper den ved "4", " og Grevlinglia 3" kan ikke fullføre matchen (feil
# gatenavn, ikke et rent tall/spenn), og regexen gir opp - _utvid_flere_adresser
# faller da videre til variant 2 (egne gate+nr-par), som splitter korrekt til
# "Bjørnefaret 4; Grevlinglia 3".
_SAMME_GATE_LISTE_RE = re.compile(
    rf"^([A-ZÆØÅ][A-Za-zæøåÆØÅ.\- ]*?)\s+({_HUSNR_TOKEN})"
    rf"((?:\s*,\s*{_HUSNR_TOKEN})*)"
    rf"(?:\s+og\s+({_HUSNR_TOKEN}))?$"
)
_DEL_RE = re.compile(r"\s*,\s*|\s+og\s+")
_GATE_NR_SEGMENT_RE = re.compile(rf"^[A-ZÆØÅ][\w.\-]*(?:\s[\w.\-]+)*\s{_HUSNR_TOKEN}$")


def _utvid_flere_adresser(kandidat):
    m = _SAMME_GATE_LISTE_RE.match(kandidat)
    if m:
        gate = m.group(1).strip()
        tall = [m.group(2)] + [t.strip() for t in m.group(3).split(",") if t.strip()]
        if m.group(4):
            tall.append(m.group(4))
        return "; ".join(f"{gate} {n}" for n in tall)
    deler = [d.strip() for d in _DEL_RE.split(kandidat) if d.strip()]
    if len(deler) >= 2 and all(_GATE_NR_SEGMENT_RE.match(d) for d in deler):
        return "; ".join(deler)
    return kandidat


def extract_adresse(sakstittel):
    """Prøver segment for segment (skilt ved _finn_skille) til ett gir en
    gyldig adresse, eller til tittelen er tom for flere segmenter. Utvider
    til flere ";"-skilte adresser hvis segmentet faktisk lister opp mer enn
    én (se _utvid_flere_adresser)."""
    if not sakstittel:
        return None
    gjenstaende = sakstittel
    for _ in range(_MAKS_SEGMENTER):
        head, rest = _finn_skille(gjenstaende.strip())
        kandidat = _rens_kandidat(head)
        if _er_gyldig(kandidat):
            return _utvid_flere_adresser(kandidat)
        if not rest.strip():
            break
        gjenstaende = rest
    return None


# Gårds-/bruksnummer ("gnr/bnr", f.eks. "49/42") skrevet direkte i
# sakstittelen - HELT uavhengig av extract_adresse()/gateadresse. Enkelte
# saker (seksjonering, sammenslåing, bruksnavn-saker o.l.) har KUN gnr/bnr i
# tittelen og ingen gateadresse i det hele tatt, f.eks. "49/42 -
# reklamefinansiert lehus, holdeplass Blindern VGS, FORELØPIG SAK" eller
# "225/433 - om bruksnavn". Siden PBE-siden ikke skrapes er sakstittelen
# eneste kilde til stedfesting for slike saker. Ingen kollisjonsfare med
# saksnummeret ("2026/7714") - det ligger i et helt separat felt
# (source["saksnummer"]), aldri i selve tittelen.
_GNR_BNR_RE = re.compile(r"\b(\d{1,4})/(\d{1,5})\b")
# Titler refererer ofte til ANDRE saker eller årstall i samme "tall/tall"-form
# som gnr/bnr ("Byggesak: 2025/20450", "spørsmål 359/2026") - disse luker vi
# bort. Oslos gnr ligger uansett aldri i et årstall-lignende leie, så par der
# ETT av tallene ligner et årstall i vårt driftsvindu forkastes.
_GNR_BNR_AARSTALL_MIN, _GNR_BNR_AARSTALL_MAKS = 2010, 2099
_GNR_BNR_BLOKKORD = {
    "sak", "saken", "saka", "byggesak", "byggesaken", "arkivsak",
    "arkivsaken", "spørsmål", "referanse", "ref", "anlnr", "journalpost",
}
_ORD_FOR_TALLPAR_RE = re.compile(r"([a-zæøåA-ZÆØÅ]+)[:\s]*$")
# Noen titler skriver ALLEREDE et fullt matrikkelnummer ("knr-gnr/bnr/festenr/
# seksjonsnr", f.eks. "0301-250/12/0/0") - festenr/seksjonsnr-halene ("/0/0")
# må da IKKE tolkes som et eget, ekstra gnr/bnr-par. Disse maskes bort først.
_MATRIKKELNR_FULL_RE = re.compile(r"\b\d{4}-(\d{1,4})/(\d{1,5})(?:/\d{1,5})*\b")


def extract_gnr_bnr(sakstittel):
    """Alle gnr/bnr-par funnet i tittelen, i rekkefølge, uten duplikater.
    Returnerer None hvis ingen funnet (mest titler - de aller fleste har
    bare en gateadresse og ikke gnr/bnr skrevet ut)."""
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
    seksjonsnr er ikke tilgjengelig fra sakstittelen alene og utelates."""
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


def _get(session, url):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.get(url, timeout=TIMEOUT)
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


def fetch_range(session, from_iso, to_iso, done_ids=None, need=None):
    """Alle Saksmappe-treff (byggesaker) publisert i [from_iso, to_iso] -
    brukes av run_full_dump. done_ids/need: stopper paginering så snart
    `need` treff som IKKE allerede finnes i done_ids er samlet opp - unngår
    å måtte paginere gjennom et helt (evt. stort) datovindu for noen få
    nye saker."""
    filters = [
        {"fieldName": "type", "fieldValue": ["Saksmappe"], "type": "termQueryFilter"},
        {"fieldName": "type", "fieldValue": ["JournalpostForMøte"], "type": "notQueryFilter"},
        {"fieldName": "arkivskaperTransitive", "fieldValue": [VIRKSOMHET_ID], "type": "postQueryFilter"},
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

    first = _post(session, {**body_base, "size": PAGE_SIZE, "offset": 0})
    total = first.get("hitCount", 0)
    items = list(first.get("searchHits", []))
    offset = PAGE_SIZE
    while offset < total:
        if need is not None and _n_new(items) is not None and _n_new(items) >= need:
            break
        page = _post(session, {**body_base, "size": PAGE_SIZE, "offset": offset})
        hits = page.get("searchHits", [])
        if not hits:
            break
        items.extend(hits)
        offset += PAGE_SIZE
    return [h["source"] for h in items if h.get("source")]


def fetch_window(session, from_iso, to_iso):
    """Alle Saksmappe- OG Journalpost-treff (ekskl. JournalpostForMøte og
    kjente ikke-byggesak-titler) publisert i [from_iso, to_iso] - brukes av
    run_daily (se modul-docstring for hvorfor begge treff-typer trengs her,
    men ikke i run_full_dump)."""
    filters = [
        {"fieldName": "type", "fieldValue": ["JournalpostForMøte"], "type": "notQueryFilter"},
        {"fieldName": "arkivskaperTransitive", "fieldValue": [VIRKSOMHET_ID], "type": "postQueryFilter"},
        {"to": f"{to_iso}||/d", "from": f"{from_iso}||/d", "fieldName": "publisertDato", "type": "rangeQueryFilter"},
    ]
    body_base = {
        "appliedFilters": filters,
        "searchTerms": [{"field": "search_tittel", "operator": "NOT_ANY", "searchTerm": EXCLUDE_TERM}],
        "sort": {"fieldName": "publisertDato", "order": "DESC", "id": "published"},
    }
    first = _post(session, {**body_base, "size": PAGE_SIZE, "offset": 0})
    total = first.get("hitCount", 0)
    items = list(first.get("searchHits", []))
    offset = PAGE_SIZE
    while offset < total:
        page = _post(session, {**body_base, "size": PAGE_SIZE, "offset": offset})
        hits = page.get("searchHits", [])
        if not hits:
            break
        items.extend(hits)
        offset += PAGE_SIZE
    return [h["source"] for h in items if h.get("source")]


# --------------------------------------------------------------------------- #
# Bygg opp poster
# --------------------------------------------------------------------------- #
def _build_saks_url(external_id):
    return f"{PORTAL_SAK_URL}?id={quote(external_id, safe='')}" if external_id else None


def _stedfesting(tittel):
    """Adresse + gnr/bnr + matrikkelnr utledet fra en sakstittel - felles
    utledning brukt av både build_sak og parent_sak_ref."""
    gnr_bnr = extract_gnr_bnr(tittel)
    return extract_adresse(tittel), gnr_bnr, build_matrikkelnr(gnr_bnr)


def build_journalpost(jp, saks_url):
    avsender = jp.get("korrespondansepartAvsender", [])
    mottaker = jp.get("korrespondansepartMottaker", [])
    return {
        "identifier": jp.get("id"),
        "jp_nr": jp.get("journalpostnummer"),
        "tittel": jp.get("tittel"),
        "type": jp.get("journalposttype"),
        "dato": jp.get("dokumentdato"),
        "journalpostdato": jp.get("journalpostdato"),  # når journalposten faktisk ble registrert
        "avsender": [p.get("navn") for p in avsender if p.get("navn")],
        "mottaker": [p.get("navn") for p in mottaker if p.get("navn")],
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
    adresse, gnr_bnr, matrikkelnr = _stedfesting(sakstittel)

    return {
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "identifier": external_id,
        "saksnummer": saksnr,
        "sakstittel": sakstittel,
        "adresse": adresse,
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "publisert_dato": source.get("publisertDato"),
        "url_einnsyn": saks_url,
        "url_oslo_kommune": build_oslo_kommune_url(saksnr),
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
    adresse, gnr_bnr, matrikkelnr = _stedfesting(tittel)
    return {
        "identifier": external_id,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnr,
        "sakstittel": tittel,
        "adresse": adresse,
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "url": saks_url,
        "url_oslo_kommune": build_oslo_kommune_url(saksnr),
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
        "journalpostdato": source.get("journaldato"),  # søke-endepunktets feltnavn (detalj-endepunktet bruker "journalpostdato", se build_journalpost)
        "avsender": avsender,
        "mottaker": mottaker,
        "url": f"{saks_url}&jid={quote(source.get('id') or '', safe='')}" if saks_url else None,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump (2018-today_dump) - kronologisk chunking + JSONL-
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

    max_saker/time_budget: valgfrie grenser for denne kjøringen (deler en
    flerårs-backfill i mindre biter)."""
    session = make_session()
    if start is None:
        start = date(START_YEAR, 1, 1)
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
WINDOW_DAYS = 5             # dager bakover fra referansedatoen (overlapp - se modul-docstring)
MAX_BACKFILL_DAYS = 30
SEEN_RETENTION_DAYS = 14    # må være >= WINDOW_DAYS


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


def build_state_from_dump(dump_files, today_fn=None):
    """Seeder seen_saker/seen_journalposter fra én eller flere ferdige
    historiske dump-filer, slik at run_daily() ikke feilaktig flagger
    allerede-dumpede saker som "nye" ved (eller etter) første kjøring. Uten
    denne seedingen starter run_for_date() med tomme seen_saker/seen_jp, og
    ALT innenfor det første datovinduet blir feilklassifisert som ny sak -
    selv om saken allerede finnes i dumpen (bekreftet skjedd i praksis, se
    run_daily-docstring).

    dump_files: én filsti eller en liste. Støtter både vanlig JSON-liste
    (.json, fra en fullført run_full_dump) og linje-separert JSON (.jsonl,
    en gjenopptakbar sjekkpunktfil fra en PÅGÅENDE dump - se modul-
    docstring). Bruk en sjekkpunktfil hvis den fulle dumpen ikke er ferdig
    ennå; kjør denne funksjonen på nytt (den kan trygt kjøres flere ganger)
    når den fulle dumpen er konsolidert.

    Returnerer {"seen_saker": {...}, "seen_journalposter": {...}} - bruk
    merge_state_from_dump() for å slå dette sammen med et eksisterende
    state.json uten å tape allerede oppdatert last_success_date/state."""
    today_fn = today_fn or (lambda: datetime.now(ZoneInfo("Europe/Oslo")).date())
    i_dag = today_fn().isoformat()
    seen_saker, seen_jp = {}, {}
    paths = dump_files if isinstance(dump_files, (list, tuple)) else [dump_files]
    for p in paths:
        p = Path(p)
        if p.suffix == ".jsonl":
            records = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        else:
            records = json.loads(p.read_text(encoding="utf-8"))
        for r in records:
            if not r.get("identifier"):
                continue
            seen_saker[r["identifier"]] = i_dag
            for jp in r.get("journalposter") or []:
                if jp.get("identifier"):
                    seen_jp[jp["identifier"]] = i_dag
    return {"seen_saker": seen_saker, "seen_journalposter": seen_jp}


def merge_state_from_dump(dump_files, state_file, today_fn=None):
    """Slår build_state_from_dump() inn i et eksisterende (eller tomt)
    state.json - dumpens funn fylles inn UNDER det som allerede er
    sett/kjørt (last_success_date og nyere seen-oppføringer fra faktiske
    run_daily-kjøringer overstyrer, ikke motsatt). Trygt å kjøre flere
    ganger, f.eks. på nytt hver gang dumpen får mer historikk."""
    state = load_state(state_file)
    seed = build_state_from_dump(dump_files, today_fn=today_fn)
    state["seen_saker"] = {**seed["seen_saker"], **state["seen_saker"]}
    state["seen_journalposter"] = {**seed["seen_journalposter"], **state["seen_journalposter"]}
    save_state(state, state_file)
    return state


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
