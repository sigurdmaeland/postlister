"""Tønsberg - ACOS "nye-innsyn" (samme plattform som Gjøvik/Moss/Sandefjord
i dette prosjektet, egen selvhostet instans). Samme seksjonsoppdeling som
de andre ACOS-kommunene: konstanter -> adresseparsing -> KILDER -> API-kall
-> engangsdump -> daglig endringslogg.

FIRE DATAKILDER (samme "Datasource"-nøkkel-mekanisme som Sandefjord):
  - bygg_ny/tilsyn_ny : Tønsberg fra 2020-nå (dagens system, MED vedlegg)
  - tonsberg_hist       : gamle Tønsberg 2006-2019, FØR sammenslåing med Re (MED vedlegg)
  - re_hist              : gamle Re 2006-2019, eget separat arkiv/system (MED vedlegg)
Til forskjell fra Sandefjord har BEGGE historiske arkiv her bekreftet
offentlig nedlastbare vedlegg - hent_filer=True for alle fire kilder.
Uten "Datasource" i body ignoreres Sakstype-filteret stille (samme kjente
edge case som resten av ACOS-kommunene).

ADRESSE/GNR-BNR-PARSING - tittelformatet er mer inkonsekvent enn
Sandefjord/Moss, bekreftet ved stikkprøve av 100-220 titler per kilde:

  a) bygg_ny/tilsyn_ny: "Adresse - GNR/BNR[, GNR/BNR][ og BNR] - beskrivelse"
     (Moss-mønster). Støtter flerledds matrikkel (gnr/bnr/feste/seksjon) og
     to adresser i samme segment ("Heimskringla 16 og 18 - ...") ->
     "Gate N1; Gate N2". Et fåtall titler mangler gnr/bnr helt -> None/None.

  b) tonsberg_hist: samme mønster som (a) i ~96% av titlene, men gnr/bnr er
     ZERO-PADDET til 4 sifre ("0082/0200") - strippes med _avpad. Et lite
     mindretall har gnr/bnr FØRST i stedet - håndteres av samme
     posisjons-uavhengige gjenkjenner som (c) (_finn_gnrbnr_blokk).

  c) re_hist: tre formater om hverandre i samme arkiv (stikkprøve av 180
     titler): ~55% "Adresse, GNR/BNR - beskrivelse" (KOMMA, unikt for Re),
     ~27% "[RE -] Adresse - GNR/BNR - beskrivelse" (som a/b, med valgfritt
     "RE -"-prefiks som strippes først), ~17% "Gbnr GNR/BNR - Adresse -
     beskrivelse" (gnr/bnr FØRST, som Sandefjords bygg_ny). Håndterer i
     tillegg flerledds matrikkel og bnr-tallspenn uten mellomrom
     ("84/248-249" - bevares intakt, splittes ikke).

Alle adresseparsere har signaturen (tittel, subtitle=None) -> (adresse,
gnr_bnr_liste, matrikkelnr) - subtitle brukes ikke her (Re sitt subtitle er
et internt kryssreferansenummer, aldri en adresse).

GNR-OFFSET VED SAMMENSLÅING: re_hist sine titler bruker den GAMLE, egne
Re-kommunens gnr-numrering (fra før 01.01.2020). Kartverket sin offisielle
oversikt over gnr-endringer ved kommunereformen ("Endringer i gårdsnummer
i nye og sammenslåtte kommuner") viser at Re fikk +300 lagt til sitt gnr
ved sammenslåingen inn i Tønsberg (Tønsberg sitt eget gnr ble uendret,
+0) - bnr endres aldri. Uten dette tillegget ville matrikkelnr for
re_hist pekt på FEIL eiendom under dagens KOMMUNE_NR. Se _med_gnr_offset
og parse_adresse_re_hist.
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://www.tonsberg.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"

PORTAL_ID = "1"
MENYPUNKT_ID = "1594"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 3905   # Tønsberg (gjeldende siden fylkessammenslåing-oppdatering
                    # 01.01.2024; var 3803 (2020-2023) og 0704 (gamle Tønsberg,
                    # før Re-sammenslåingen) - Re sitt eget historiske arkiv
                    # bruker OGSÅ dette (nåværende) nummeret, samme konvensjon
                    # som resten av prosjektet (jf. Sandefjord/Andebu/Stokke).
KOMMUNE = "Tønsberg"

MAX_WORKERS = 8
TIMEOUT = 30
RETRIES = 3

HEADERS = {
    "Content-Type": "application/json",
    "PortalID": PORTAL_ID,
    "MenypunktID": MENYPUNKT_ID,
    "SprakID": "1",
    "X-ANTI-CSRF": "1",
}


# --------------------------------------------------------------------------- #
# Delt adresse/gnr-bnr-parsing-grunnmur
# --------------------------------------------------------------------------- #
# Ekte bindestrek-skilletegn (mellomrom på minst én side) - IKKE en bar
# bindestrek uten mellomrom (husnummer-spenn som "40-44") - samme som de
# andre ACOS-kommunene.
_SPLIT_RE = re.compile(r"(?:(?<=\s)-\s*|-(?=\s))")


def _split_tittel(tittel):
    segs = [s.strip() for s in _SPLIT_RE.split((tittel or "").strip())]
    return [s for s in segs if s]


_GBNR_PREFIX_RE = re.compile(r"^\s*g\s*bnr\.?\s*", re.IGNORECASE)
_MFL_SUFFIX_RE = re.compile(r"\s*m\.?\s*/?\s*fl\.?\s*$", re.IGNORECASE)
_RE_PREFIX_RE = re.compile(r"^\s*re\s*-\s*", re.IGNORECASE)
# Et sjeldent observert alternativt/tidligere gnr/bnr i parentes
# ("525/33 (tidligere 225/33)", "0088/0053 (andre 0088/0055)") - ignoreres
# helt (samme prinsipp som Moss sin "(tidl. X/Y)"-håndtering).
_PAREN_RE = re.compile(r"\(.*?\)")
# Utskrevet festenummer-suffiks ("0159/0001 festenr 25") - gjør at
# segmentet fortsatt teller som en ren gnr/bnr-blokk selv om feste-tallet
# ikke er på slash-form.
_FESTENR_SUFFIX_RE = re.compile(r"\s*(?:festenr|fnr)\.?\s*\d+\s*$", re.IGNORECASE)

# Ett gnr/bnr-tall-uttrykk: gnr/bnr[-bnr2](/feste(/seksjon)?)? - "-bnr2" er
# et sjeldent observert, tett bnr-tallspenn ("33/5-7") som bevares intakt
# (se modul-docstring punkt c). Mellomrom rundt skråstreken tolereres
# ("612 /1" - sett i sjeldne bygg_ny-titler).
_TOKEN = r"\d+\s*/\s*\d+(?:-\d+)?(?:/\s*\d+(?:/\s*\d+)?)?"
_BARE_NUM = r"\d+(?:-\d+)?"

_GNRBNR_LIST_FULL_RE = re.compile(
    rf"^{_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*"
    rf"(?:\s*(?:festenr|fnr)\.?\s*\d+)?"
    rf"\s*(?:m\.?\s*/?\s*fl\.?)?\s*$",
    re.IGNORECASE,
)
# Løs variant: matcher et gnr/bnr-uttrykk i STARTEN av et segment, uten å
# kreve at resten av segmentet også er rent gnr/bnr - Re sitt arkiv mangler
# noen ganger skilletegnet mellom gnr/bnr-blokken og beskrivelsen som
# følger i SAMME segment ("Åsveien 69 - 272/1 tillatelse uten tiltak -
# Driftsbygning", "Skjeggestadåsen 226/27 - Mottatt nabovarsel" - se
# modul-docstring punkt c). Brukt KUN som siste fallback.
_LEADING_TOKEN_RE = re.compile(
    rf"^(?:g\s*bnr\.?\s*)?({_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)(?:\s|$)",
    re.IGNORECASE,
)

_FULL_TOKEN_RE = re.compile(r"^(\d+)\s*/\s*(\d+)(?:-(\d+))?(?:/\s*(\d+)(?:/\s*(\d+))?)?$")
_BARE_NUM_RE = re.compile(r"^(\d+)(?:-(\d+))?$")
_SUBSPLIT_RE = re.compile(r"\s*(?:,|og)\s*", re.IGNORECASE)


def _avpad(s):
    """Fjerner ledende nuller ('0082' -> '82') - kun Tønsbergs to historiske
    arkiv zero-padder tallene sine, i motsetning til alle andre kommuner i
    prosjektet (se modul-docstring punkt b/c)."""
    return str(int(s))


def _er_gnrbnr_segment(seg):
    """Sjekker om et helt tittel-segment (etter evt. 'Gbnr'-prefiks og evt.
    parentetisk alternativ-nummer) UTELUKKENDE består av ett eller flere
    gnr/bnr-uttrykk."""
    kandidat = _GBNR_PREFIX_RE.sub("", seg).strip()
    if _GNRBNR_LIST_FULL_RE.match(kandidat):
        return True
    stripped = _PAREN_RE.sub("", kandidat).strip()
    return bool(stripped) and bool(_GNRBNR_LIST_FULL_RE.match(stripped))


def _parse_gnrbnr_expr(seg):
    """Parser en ren gnr/bnr-uttrykk-streng til (gnr_bnr_liste,
    matrikkel_liste), med ledende nuller stripped fra hver tall-del."""
    seg = _GBNR_PREFIX_RE.sub("", seg).strip()
    seg = _PAREN_RE.sub("", seg).strip()
    seg = _FESTENR_SUFFIX_RE.sub("", seg).strip()
    seg = _MFL_SUFFIX_RE.sub("", seg).strip()
    parts = [p.strip() for p in _SUBSPLIT_RE.split(seg) if p.strip()]
    gnr_bnr, matrikkel = [], []
    last_gnr = None
    for p in parts:
        m = _FULL_TOKEN_RE.match(p)
        if m:
            gnr_raw, bnr_raw, bnr2_raw, feste_raw, seksjon_raw = m.groups()
            gnr, bnr = _avpad(gnr_raw), _avpad(bnr_raw)
            if bnr2_raw:
                bnr = f"{bnr}-{_avpad(bnr2_raw)}"
            feste = _avpad(feste_raw) if feste_raw else None
            seksjon = _avpad(seksjon_raw) if seksjon_raw else None
            last_gnr = gnr
        else:
            m2 = _BARE_NUM_RE.match(p)
            if m2 and last_gnr:
                bnr_raw, bnr2_raw = m2.groups()
                bnr = _avpad(bnr_raw)
                if bnr2_raw:
                    bnr = f"{bnr}-{_avpad(bnr2_raw)}"
                gnr, feste, seksjon = last_gnr, None, None
            else:
                continue
        par = f"{gnr}/{bnr}"
        if par not in gnr_bnr:
            gnr_bnr.append(par)
        d = (f"{gnr}/{bnr}/{feste or '0'}/{seksjon or '0'}"
             if (feste and feste != "0") or (seksjon and seksjon != "0") else par)
        if d not in matrikkel:
            matrikkel.append(d)
    return gnr_bnr, matrikkel


def _finn_gnrbnr_blokk(segs):
    """Finner den (evt. flerledd) blokken av segmenter som UTELUKKENDE er
    gnr/bnr-uttrykk - blokken kan komme enten FØR eller ETTER adressen (til
    forskjell fra Moss, der den alltid kommer etter - se modul-docstring
    punkt b/c). Returnerer (start, end, gnr_bnr_liste, matrikkel_liste) -
    start=None hvis ingen ren gnr/bnr-blokk funnet noe sted."""
    start = None
    for i, seg in enumerate(segs):
        if _er_gnrbnr_segment(seg):
            start = i
            break
    if start is None:
        return None, None, None, None

    end = start
    gnr_bnr, matrikkel = [], []
    for j in range(start, len(segs)):
        if j > start and not _er_gnrbnr_segment(segs[j]):
            break
        end = j
        g, m = _parse_gnrbnr_expr(segs[j])
        for p in g:
            if p not in gnr_bnr:
                gnr_bnr.append(p)
        for d in m:
            if d not in matrikkel:
                matrikkel.append(d)
    return start, end, gnr_bnr, matrikkel


def _finn_gnrbnr_blokk_lopende(segs):
    """Løs fallback brukt KUN når _finn_gnrbnr_blokk ikke finner noe: leter
    etter et segment som STARTER MED et gnr/bnr-uttrykk etterfulgt av mer
    fri tekst i SAMME segment (se _LEADING_TOKEN_RE). Returnerer samme
    (start, end, gnr_bnr_liste, matrikkel_liste)-form, med start==end siden
    dette kun håndterer ett enkelt segment om gangen."""
    for i, seg in enumerate(segs):
        kandidat = _PAREN_RE.sub("", seg.strip())
        m = _LEADING_TOKEN_RE.match(kandidat)
        if m:
            gnr_bnr, matrikkel = _parse_gnrbnr_expr(m.group(1))
            if gnr_bnr:
                return i, i, gnr_bnr, matrikkel
    return None, None, None, None


_HAS_DIGIT_RE = re.compile(r"\d")
_NUM_LETTER_SPACE_RE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_OG_PAIR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+og\s+(\d+[A-Za-zæøåÆØÅ]?)$",
                         re.IGNORECASE)
_SPACED_PAIR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+-\s+(\d+[A-Za-zæøåÆØÅ]?)$")


def _utvid_flere_adresser(adresse):
    """To distinkte adresser på samme gate ('Heimskringla 16 og 18',
    'Gate 19 - 21') skrives om til 'Gate N1; Gate N2' - samme "; "-
    konvensjon som ellers i prosjektet (se modul-docstring punkt a)."""
    if not adresse:
        return adresse
    for rx in (_OG_PAIR_RE, _SPACED_PAIR_RE):
        m = rx.match(adresse)
        if m:
            gate = m.group(1).strip()
            return f"{gate} {m.group(2)}; {gate} {m.group(3)}"
    return adresse


def _ferdigstill_adresse(adresse):
    """Siste steg for enhver kandidat-adresse: trim tegnsetting, normaliser
    nummer+bokstav-mellomrom, ekspander evt. "N1 og/- N2"-adressepar, og
    forkast kandidater som ikke inneholder et husnummer i det hele tatt
    (stedsnavn/bygningsnavn/saksbeskrivelser - samme prinsipp som
    Sandefjord)."""
    if not adresse:
        return None
    adresse = adresse.strip().strip(",.;:").strip()
    if not adresse or not _HAS_DIGIT_RE.search(adresse):
        return None
    adresse = _NUM_LETTER_SPACE_RE.sub(r"\1\2", adresse)
    return _utvid_flere_adresser(adresse)


def _adresse_fra_blokk(segs, start, end):
    """Adressen er alt FØR blokken hvis blokken ikke står først (Moss-stil,
    flere adresse-segmenter slås sammen med ' - '), ellers det FØRSTE
    segmentet etter blokken som ikke selv bare er en gnr/bnr-fortsettelse
    (Sandefjord-ny-stil, se _er_gnrbnr_segment)."""
    if start > 0:
        return " - ".join(segs[:start])
    for kandidat in segs[end + 1:]:
        if _er_gnrbnr_segment(kandidat):
            continue
        return kandidat
    return None


def _matrikkelnr(matrikkel):
    return ("; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel)
            if matrikkel else None)


def _med_gnr_offset(strenger, offset):
    """Legger et fast gnr-offset på gnr-delen (FØRSTE ledd) av hver
    "gnr/bnr[/feste/seksjon]"-streng, uten å røre bnr/feste/seksjon. Brukes
    for re_hist, der saks-titlene bruker den GAMLE, egne Re-kommunens
    gnr-numrering fra før 01.01.2020-sammenslåingen med Tønsberg. Dagens
    offisielle matrikkelnr for disse eiendommene bruker Kartverket sitt
    faste, dokumenterte +300-tillegg (se "Endringer i gårdsnummer i nye og
    sammenslåtte kommuner", kartverket.no; Tønsberg sitt eget gnr ble
    uendret, +0) - bnr endres aldri ved en sammenslåing."""
    ut = []
    for s in strenger:
        gnr, rest = s.split("/", 1)
        ny = f"{int(gnr) + offset}/{rest}"
        if ny not in ut:
            ut.append(ny)
    return ut


_EMBEDDED_TOKEN_RE = re.compile(
    rf"({_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)",
    re.IGNORECASE,
)
_GBNR_KEYWORD_TRAILING_RE = re.compile(
    r"(?:g\s*b\s*nr|b\s*g\s*nr|gnr|gbn)\.?\s*$", re.IGNORECASE
)


def _finn_gnrbnr_blokk_innbakt(segs):
    """Løsest mulige fallback, brukt sist: leter etter et gnr/bnr-uttrykk
    et sted MIDT I et segment, med adressen FORAN det i SAMME segment uten
    noe skilletegn i det hele tatt ('Myreveien 49 369/6 - ...',
    'Skjeggestadåsen 226/27 - ...', 'Fossanveien 143 gbn 237/5 - ...' -
    Re sitt arkiv mangler av og til ethvert skilletegn mellom adresse og
    gnr/bnr, se modul-docstring punkt c). Returnerer (adresse_eller_None,
    gnr_bnr_liste, matrikkel_liste) direkte (ikke start/end-indekser, siden
    adressen her er en DEL av et segment, ikke hele segmenter)."""
    for i, seg in enumerate(segs):
        kandidat = _PAREN_RE.sub("", seg)
        m = _EMBEDDED_TOKEN_RE.search(kandidat)
        if not m or m.start() == 0:
            continue  # start==0 dekkes allerede av _lopende-fallbacken
        adresse_del = kandidat[:m.start()].strip()
        adresse_del = _GBNR_KEYWORD_TRAILING_RE.sub("", adresse_del).strip()
        adresse_del = adresse_del.rstrip(",").strip()
        if not _HAS_DIGIT_RE.search(adresse_del):
            continue
        gnr_bnr, matrikkel = _parse_gnrbnr_expr(m.group(1))
        if not gnr_bnr:
            continue
        adresse = " - ".join(segs[:i] + [adresse_del]) if i > 0 else adresse_del
        return adresse, gnr_bnr, matrikkel
    return None, None, None


def _parse_generisk(tittel, gnr_offset=0):
    """Posisjons-uavhengig fallback-parser: finner gnr/bnr-blokken (foran
    ELLER bak adressen) via segment-skanning. Brukt direkte av (a)/(b), og
    som siste fallback av (c) etter at komma- og RE-prefiks-varianten er
    forsøkt (se modul-docstring).
    gnr_offset: fast tillegg på gnr-delen, brukt av re_hist (se
    _med_gnr_offset)."""
    segs = _split_tittel(tittel)
    if not segs:
        return None, None, None
    start, end, gnr_bnr, matrikkel = _finn_gnrbnr_blokk(segs)
    if start is None:
        start, end, gnr_bnr, matrikkel = _finn_gnrbnr_blokk_lopende(segs)
    if start is not None:
        adresse = _ferdigstill_adresse(_adresse_fra_blokk(segs, start, end))
        if gnr_offset and gnr_bnr:
            gnr_bnr, matrikkel = _med_gnr_offset(gnr_bnr, gnr_offset), _med_gnr_offset(matrikkel, gnr_offset)
        return adresse, (gnr_bnr or None), _matrikkelnr(matrikkel)

    adresse_raw, gnr_bnr, matrikkel = _finn_gnrbnr_blokk_innbakt(segs)
    if gnr_bnr is None:
        return None, None, None
    adresse = _ferdigstill_adresse(adresse_raw)
    if gnr_offset:
        gnr_bnr, matrikkel = _med_gnr_offset(gnr_bnr, gnr_offset), _med_gnr_offset(matrikkel, gnr_offset)
    return adresse, (gnr_bnr or None), _matrikkelnr(matrikkel)


def parse_adresse_ny(tittel, subtitle=None):
    """Tønsberg fra 2020-nå (Byggesak og Byggetilsynssak) - se
    modul-docstring punkt (a)."""
    return _parse_generisk(tittel)


def parse_adresse_tonsberg_hist(tittel, subtitle=None):
    """Tønsberg historisk 2006-2019 - se modul-docstring punkt (b). Samme
    generelle parser som (a); zero-padding håndteres av _avpad inni
    _parse_gnrbnr_expr uansett kilde."""
    return _parse_generisk(tittel)


# --------------------------------------------------------------------------- #
# Re historisk - egen parser pga tre reelt forekommende formater om hverandre
# (se modul-docstring punkt c).
# --------------------------------------------------------------------------- #
_KOMMA_ADRESSE_RE = re.compile(
    rf"^(?P<adresse>.+?\d[A-Za-zæøåÆØÅ]?)\s*,\s*"
    rf"(?P<gnrbnr>{_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)\s*$",
    re.IGNORECASE,
)


def parse_adresse_re_hist(tittel, subtitle=None):
    """Re historisk 2006-2019 - se modul-docstring punkt (c). Prøver i tur
    og orden: (1) valgfritt "RE -"/"Re -"-prefiks strippes alltid først,
    (2) komma-mønsteret "Adresse, GNR/BNR" (dominerende, ~55 % av
    stikkprøven) sjekket KUN på første segment, (3) den samme
    posisjons-uavhengige fallback-parseren som (a)/(b) (dekker både
    "Adresse - GNR/BNR - ..." og "Gbnr GNR/BNR - Adresse - ..."). gnr-delen
    får Kartverket sitt faste +300-offset for den gamle Re-kommunens egen
    gnr-numrering i alle tre grener (se _med_gnr_offset)."""
    uten_prefiks = _RE_PREFIX_RE.sub("", tittel or "")
    segs = _split_tittel(uten_prefiks)
    if segs:
        m = _KOMMA_ADRESSE_RE.match(segs[0])
        if m:
            adresse = _ferdigstill_adresse(m.group("adresse"))
            gnr_bnr, matrikkel = _parse_gnrbnr_expr(m.group("gnrbnr"))
            if gnr_bnr:
                gnr_bnr, matrikkel = _med_gnr_offset(gnr_bnr, 300), _med_gnr_offset(matrikkel, 300)
            if adresse or gnr_bnr:
                return adresse, (gnr_bnr or None), _matrikkelnr(matrikkel)
    return _parse_generisk(uten_prefiks, gnr_offset=300)


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg_ny": dict(
        sakstype_navn="Byggesak",
        datasource="e8411f20-3835-4913-bf04-3c27a9f93c86",
        sakstype="at-e8411f20__3835__4913__bf04__3c27a9f93c86-BS!RnA5d9",
        adresse_parser=parse_adresse_ny,
        hent_filer=True,
        periode="2020-nå",
    ),
    "tilsyn_ny": dict(
        sakstype_navn="Byggetilsynssak",
        datasource="e8411f20-3835-4913-bf04-3c27a9f93c86",
        sakstype="at-e8411f20__3835__4913__bf04__3c27a9f93c86-TSBS!ovzivr",
        adresse_parser=parse_adresse_ny,
        hent_filer=True,
        periode="2020-nå",
    ),
    "tonsberg_hist": dict(
        sakstype_navn="Byggesak",
        datasource="a6fc2cc7-e78f-4483-a897-6bb8c5963bd6",
        sakstype="at-a6fc2cc7__e78f__4483__a897__6bb8c5963bd6-BS!vk8H06",
        adresse_parser=parse_adresse_tonsberg_hist,
        hent_filer=True,
        periode="2006-2019 (Tønsberg)",
    ),
    "re_hist": dict(
        sakstype_navn="Byggesak",
        datasource="1e115272-ea90-40a3-9f0a-b145817c172d",
        sakstype="at-1e115272__ea90__40a3__9f0a__b145817c172d-BS!QdRJUv",
        adresse_parser=parse_adresse_re_hist,
        hent_filer=True,
        periode="2006-2019 (Re)",
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(method, session, path, retries=RETRIES, **kwargs):
    """Delt retry-logikk for _post/_get - samme backoff (1.5s * forsøksnr)
    og samme feilmelding-format for begge HTTP-metodene."""
    url = f"{API_BASE}/{path}"
    last_err = None
    for attempt in range(retries):
        try:
            r = getattr(session, method)(url, headers=HEADERS, timeout=TIMEOUT, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{method.upper()} {path} feilet etter {retries} forsøk") from last_err


def _post(session, path, body, retries=RETRIES):
    return _request("post", session, path, retries=retries, json=body)


def _get(session, path, retries=RETRIES):
    return _request("get", session, path, retries=retries)


def get_case_list(kilde_key, session=None, max_items=None):
    """Henter identifier+saksnummer+dato for saker i én datakilde, nyeste
    først. "Datasource" MÅ være med i body (se modul-docstring). max_items=N
    stopper paginering tidlig - brukes kun når man ikke også dato-filtrerer."""
    kilde = KILDER[kilde_key]
    session = session or requests.Session()

    def body_for(page):
        return {
            "type": OVERVIEW_TYPE_SAK,
            "keyValues": [
                {"key": "Datasource", "value": kilde["datasource"]},
                {"key": "Dato", "value": "AllCases"},
                {"key": "Sakstype", "value": kilde["sakstype"]},
                {"key": "FilterContent", "value": "CaseOnly"},
                {"key": "pageSize", "value": str(PAGE_SIZE)},
                {"key": "page", "value": str(page)},
            ],
        }

    first = _post(session, "overview", body_for(1))
    si = first["content"]["searchItems"]
    items = list(si["items"])
    page_count = si["pageCount"]

    if max_items and len(items) >= max_items:
        return items[:max_items]

    for page in range(2, page_count + 1):
        d = _post(session, "overview", body_for(page))
        items.extend(d["content"]["searchItems"]["items"])
        if max_items and len(items) >= max_items:
            return items[:max_items]

    return items


def fetch_dokument_filer(session, dokument_identifier):
    """Henter fillommer for ETT dokument. Alle fire kilder har hent_filer=True
    (til forskjell fra Sandefjord er begge historiske arkiv her bekreftet
    offentlig nedlastbare)."""
    d = _get(session, f"dokument/{dokument_identifier}?fromMeeting=false")
    model = d.get("content", {}).get("model") or {}
    filer = []
    for gruppe in model.get("vedleggGruppe") or []:
        kategori = gruppe.get("title")
        for f in gruppe.get("vedlegg") or []:
            filer.append({
                "navn": f.get("title"),
                "kategori": kategori,
                "filtype": f.get("filtype"),
                "storrelse": f.get("filstorrelseFormatted"),
                "url": BASE_URL + f["fileUrl"] if f.get("fileUrl") else None,
            })
    return filer


def _finn_metadata(blocks, key):
    for b in blocks or []:
        if b.get("type") == "Metadata" and b.get("key") == key:
            return b.get("text") or b.get("title")
    return None


def _finn_property(blocks, tittel):
    for b in blocks or []:
        if b.get("type") == "PropertyList":
            for p in b.get("b") or []:
                if p.get("type") == "Property" and p.get("title") == tittel:
                    return p.get("text")
    return None


def fetch_case(session, kilde_key, identifier, hent_filer=None):
    """Henter og parser én sak. `hent_filer=None` bruker kildens egen
    standardverdi (se KILDER over)."""
    kilde = KILDER[kilde_key]
    if hent_filer is None:
        hent_filer = kilde["hent_filer"]

    try:
        d = _get(session, f"sak/{identifier}")
        body = d["content"]["body"]
    except Exception as e:  # noqa: BLE001
        return {"document_id": identifier, "error": str(e)}

    sakstittel = body.get("title")
    subtitle = body.get("subtitle")
    saksnummer = _finn_metadata(body.get("b"), "FRIENDLY_ID")
    props = body.get("props") or {}
    adresse, gnr_bnr_liste, matrikkelnr = kilde["adresse_parser"](sakstittel, subtitle)

    dokumenter = []
    for blokk in body.get("b") or []:
        if blokk.get("type") != "DocumentList":
            continue
        for doc in blokk.get("b") or []:
            if doc.get("type") != "AsyncDocument":
                continue
            doc_id = doc.get("identifier")
            doc_blocks = doc.get("b")

            filer = []
            if hent_filer:
                try:
                    filer = fetch_dokument_filer(session, doc_id)
                except Exception:  # noqa: BLE001
                    filer = []

            dokumenter.append({
                "journalpost_id": doc_id,
                "journalnummer": _finn_property(doc_blocks, "DokumentID"),
                "tittel": doc.get("title"),
                "dato": _finn_property(doc_blocks, "Dato"),
                "dokumenttype": _finn_property(doc_blocks, "Dokumenttype"),
                "dokumenttype_kode": _finn_metadata(doc_blocks, "DOCUMENT_TYPE_CODE"),
                "avsender": _finn_property(doc_blocks, "Avsender"),
                "mottaker": _finn_property(doc_blocks, "Mottaker"),
                "filer": filer,
            })

    return {
        "document_id": identifier,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": kilde["sakstype_navn"],
        "sakstittel": sakstittel,
        "adresse": adresse,
        "gnr_bnr": "; ".join(gnr_bnr_liste) if gnr_bnr_liste else None,
        "matrikkelnr": matrikkelnr,
        "saksbehandler": props.get("saksbehandler"),
        "status": props.get("status"),
        "dato": props.get("dato"),
        "url": f"{BASE_URL}/tjenester/innsyn/sok-i-postlister-saker-og-dokumenter/#/details/{identifier}",
        "dokumenter": dokumenter,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump - gjenopptakbar kjøring
# --------------------------------------------------------------------------- #
def load_done(output_file):
    if not output_file.exists():
        return {}
    try:
        data = json.loads(output_file.read_text(encoding="utf-8"))
        return {d["document_id"]: d for d in data
                if "dokumenter" in d and "error" not in d}
    except Exception:  # noqa: BLE001
        return {}


def save_resultater(results, output_file):
    tmp = output_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
    tmp.replace(output_file)


def _parse_iso_date(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


_DATO_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")


def _parse_dato(dato_str):
    if not dato_str:
        return None
    m = _DATO_RE.match(dato_str.strip())
    if not m:
        return None
    dag, maaned, aar = m.groups()
    try:
        return datetime(int(aar), int(maaned), int(dag)).date()
    except ValueError:
        return None


def run_full_dump(kilde_key, output_file, limit=None, from_date=None, to_date=None,
                   save_every=200, hent_filer=None):
    """Full historisk dump for én datakilde. limit=N henter kun de N nyeste
    (stopper paginering tidlig, se get_case_list). from_date/to_date
    (YYYY-MM-DD) filtrerer LOKALT på sakslistas "dato"-felt - krever at hele
    lista hentes først."""
    if from_date or to_date:
        items = get_case_list(kilde_key)
    else:
        items = get_case_list(kilde_key, max_items=limit)
    if from_date or to_date:
        fra = _parse_iso_date(from_date) if isinstance(from_date, str) else from_date
        til = _parse_iso_date(to_date) if isinstance(to_date, str) else to_date
        filtrert = []
        for it in items:
            dato = _parse_dato((it.get("properties") or {}).get("dato"))
            if dato is None:
                continue
            if fra and dato < fra:
                continue
            if til and dato > til:
                continue
            filtrert.append(it)
        items = filtrert
    if limit:
        items = items[:limit]
    ids = [it["identifier"] for it in items]

    results = load_done(output_file)
    todo = [i for i in ids if i not in results]
    periode = f" (fra_dato={from_date}, til_dato={to_date})" if (from_date or to_date) else ""
    print(f"{len(ids)} saker totalt" + (f" (limit={limit})" if limit else "") + periode +
          f", {len(results)} allerede hentet, {len(todo)} gjenstår")

    session = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_case, session, kilde_key, i, hent_filer): i for i in todo}
        for fut in as_completed(futures):
            res = fut.result()
            results[res["document_id"]] = res
            done += 1
            if done % save_every == 0:
                save_resultater(results, output_file)
                print(f"  {done}/{len(todo)} hentet (lagret)")

    save_resultater(results, output_file)
    errors = sum(1 for r in results.values() if "error" in r)
    print(f"Ferdig: {len(results)} saker skrevet til {output_file} ({errors} feilet)")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - liste-så-filtrer + "seen"-dedup
# (KUN for bygg_ny/tilsyn_ny, de to historiske arkivene er lukket og får
# aldri nye saker).
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 30
SEEN_RETENTION_DAYS = None


def get_recent_case_ids(kilde_key, cutoff_date):
    items = get_case_list(kilde_key)
    recent_ids = []
    for it in items:
        dato = _parse_dato((it.get("properties") or {}).get("dato"))
        if dato is None or dato >= cutoff_date:
            recent_ids.append(it["identifier"])
    return recent_ids, len(items)


def load_state(state_file):
    if not state_file.exists():
        return {"seen_saker": {}, "seen_journalposter": {}}
    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        state = {}
    state.setdefault("seen_saker", {})
    state.setdefault("seen_journalposter", {})
    return state


def save_state(state, state_file):
    tmp = state_file.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(state_file)


def _merk_sett(r, ref_iso, seen_saker, seen_jp):
    """Registrerer én sak + dens journalposter som sett pr. ref_iso - delt av
    build_state_from_dump (i_dag) og run_daily (ref_iso)."""
    seen_saker[r["document_id"]] = ref_iso
    for jp in r["dokumenter"]:
        if jp.get("journalpost_id"):
            seen_jp[jp["journalpost_id"]] = ref_iso


def build_state_from_dump(dump_file, today_fn=None):
    today_fn = today_fn or _i_dag_oslo
    data = json.loads(Path(dump_file).read_text(encoding="utf-8"))
    i_dag = today_fn().isoformat()
    seen_saker, seen_jp = {}, {}
    for r in data:
        if "dokumenter" not in r:
            continue
        _merk_sett(r, i_dag, seen_saker, seen_jp)
    return {"seen_saker": seen_saker, "seen_journalposter": seen_jp}


def prune_seen(state, today):
    if SEEN_RETENTION_DAYS is not None:
        cutoff = (today - timedelta(days=SEEN_RETENTION_DAYS)).isoformat()
        state["seen_saker"] = {k: v for k, v in state["seen_saker"].items() if v >= cutoff}
        state["seen_journalposter"] = {k: v for k, v in state["seen_journalposter"].items() if v >= cutoff}


def upload_til_azure(saker_file, jp_file, ref_date, sakstype):
    print(f"  [Azure] opplasting ikke satt opp ennå - {saker_file.name} og "
          f"{jp_file.name} ({sakstype}, {ref_date}) ligger foreløpig kun lokalt.")


def write_changelog(nye_saker, nye_journalposter, ref_date, output_dir, sakstype):
    output_dir.mkdir(exist_ok=True)
    saker_file = output_dir / f"saker_{ref_date}.json"
    jp_file = output_dir / f"journalposter_{ref_date}.json"

    saker_file.write_text(json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    jp_file.write_text(json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Skrev {len(nye_saker)} nye saker  -> {saker_file}")
    print(f"Skrev {len(nye_journalposter)} saker med nye journalposter -> {jp_file}")
    upload_til_azure(saker_file, jp_file, ref_date, sakstype)


def _i_dag_oslo():
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def run_daily(kilde_key, state_file, output_dir, window_days=WINDOW_DAYS, today_fn=_i_dag_oslo):
    """Daglig endringslogg. `state_file` må allerede finnes (bygget via
    build_state_from_dump() fra en fullført historisk dump) før første
    kjøring."""
    kilde = KILDER[kilde_key]
    ref_date = today_fn()
    ref_iso = ref_date.isoformat()
    cutoff = ref_date - timedelta(days=window_days)
    state = load_state(state_file)
    seen_saker = state["seen_saker"]
    seen_jp = state["seen_journalposter"]

    kandidat_ids, n_totalt = get_recent_case_ids(kilde_key, cutoff)
    print(f"{n_totalt} saker totalt, {len(kandidat_ids)} innenfor "
          f"{window_days}-dagersvinduet (cutoff={cutoff.isoformat()}) hentes i detalj")

    session = requests.Session()
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_case, session, kilde_key, i) for i in kandidat_ids]
        for fut in as_completed(futures):
            results.append(fut.result())

    nye_saker = []
    nye_journalposter = []
    for r in results:
        if "dokumenter" not in r:
            continue
        cid = r["document_id"]
        nye_jp = [jp for jp in r["dokumenter"]
                  if jp.get("journalpost_id") and jp["journalpost_id"] not in seen_jp]

        if cid not in seen_saker:
            nye_saker.append(r)
        elif nye_jp:
            nye_journalposter.append({
                "parent_sak": {
                    "document_id": cid,
                    "saksnummer": r.get("saksnummer"),
                    "sakstype": r.get("sakstype"),
                    "sakstittel": r.get("sakstittel"),
                    "kommune": KOMMUNE,
                    "kommune_nr": KOMMUNE_NR,
                    "adresse": r.get("adresse"),
                    "gnr_bnr": r.get("gnr_bnr"),
                    "matrikkelnr": r.get("matrikkelnr"),
                    "url": r.get("url"),
                },
                "nye_journalposter": nye_jp,
            })

        _merk_sett(r, ref_iso, seen_saker, seen_jp)

    feilet = sum(1 for r in results if "error" in r)
    write_changelog(nye_saker, nye_journalposter, ref_iso, output_dir, kilde["sakstype_navn"])
    prune_seen(state, ref_date)
    save_state(state, state_file)
    print(f"Ferdig ({feilet}/{len(kandidat_ids)} feilet under henting).")
