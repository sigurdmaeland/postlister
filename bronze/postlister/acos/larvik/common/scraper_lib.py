"""Larvik - ACOS "nye-innsyn", men på "innsynpluss.onacos.no" - en DELT
fleirtenant-vert (til forskjell fra Gjøvik/Moss/Sandefjord/Tønsberg/Ålesund,
som alle er selvhostede egne instanser på sin egen "www.X.kommune.no"). Samme
seksjonsoppdeling som de andre ACOS-kommunene: konstanter -> adresseparsing
-> KILDER -> API-kall -> engangsdump -> daglig endringslogg.

VIKTIG DRIFTSDETALJ - API-et er IKKE identisk med de selvhostede instansene:
  - Kommunenavnet er IKKE en del av API-stien ("api/presentation/v2/
    nye-innsyn" ligger rett under ROOT_URL, ALDRI under "/larvik/") - hvilken
    kommune som menes styres i stedet av "PortalID"-headeren (72 for Larvik).
  - Endepunktet for søk heter "overviewInit", IKKE "overview" som på de
    selvhostede instansene - et rent POST mot "overview" her svarer med en
    302-omdirigering til /login/ (så lenge alt annet er identisk, inkludert
    anti-forgery-cookien) - "overviewInit" er derimot fullt offentlig og gir
    ekte JSON uten noen innlogging. Bekreftet ved å fange opp et ekte
    nettleser-kall (Brave/Chromium) i DevTools, siden statisk analyse av de
    minifiserte JS-bundlene på siden IKKE avslørte dette (widgeten som
    initialiserer søket lastes av kode utenfor de bundlene som faktisk
    hentes ved førstelasting av /sok/-siden).
  - "Sakstype" i søkekroppen kan gjentas flere ganger i SAMME kall (OR-logikk)
    for å hente flere sakstyper samtidig i én overviewInit-respons - brukes
    likevel IKKE her, siden hver kilde (bygg/tilsyn) skrapes for seg.

TO DATAKILDER (samme Datasource-ID, ulik Sakstype-verdi):
  - bygg   : Byggesak, Sakstype "...-BS!qstXWj" - 14172 saker (02.01.2018 - i dag)
  - tilsyn : Tilsynssak, Sakstype "...-TIL!LHop6j" - KUN 3 saker totalt
             (04.01.2018 - 18.12.2019). De aller fleste tilsyn/ulovlighets-
             saker i Larvik ligger tydeligvis som VANLIGE byggesaker (med
             "Tilsyn eller ulovlighetsoppfølging - ..." som tittelprefiks),
             ikke som egen "Tilsynssak"-arkivsakstype - denne kilden er derfor
             reelt sett nesten tom, men tas med for fullstendighet siden
             brukeren eksplisitt ba om filtrering på tilsynssaker.
Begge kilder startet arkivet sitt 2018 - derfor felles dump-mappenavn
"2018-today_dump" med underkatalog per sakstype (bygg/tilsyn), samme mønster
som Gjøvik (som også bare har én sakstype men samme "år-today"-navngiving).

ADRESSE/GNR-BNR-PARSING - annen RESKKEFØLGE enn både Moss ("Adresse - GNR/BNR
- beskrivelse") og Ålesund ("Gbnr GNR/BNR - adresse - beskrivelse"): Larvik
skriver "Beskrivelse[ - flere beskrivelses-ledd] - Adresse - Gbnr GNR/BNR"
(gbnr-blokken kommer SIST, adressen er alltid segmentet RETT FØR den - alt
FORAN det igjen er ren beskrivelse og skal IKKE bli med i adressefeltet).
Gjenbruker derfor samme lavnivå-motor som Ålesund (_finn_gnrbnr_blokk,
_parse_gnrbnr_expr, parentes-/mfl-/festenr-/seksjonsnr-håndtering - se
Ålesund-modulens docstring for alle detaljer om disse), men med EGEN
_adresse_fra_blokk (se der) som henter KUN det ene segmentet rett før
gbnr-blokken i stedet for å slå sammen alt foran (som ville dratt med seg
beskrivelsen for Larvik). Et fåtall titler skriver gbnr FØRST i stedet
("Gbnr 3020/1983 - Storgata 59 - Forhåndskonferanse...") - da brukes samme
"se etter blokken"-fallback som Ålesund. "gbfnr" (åpenbar skrivefeil for
"gbnr") forekommer også og dekkes av samme prefiks-regex.

Adressen får ofte et postnummer+poststed hengt på etter husnummeret
("Vinjes vei 22, 3269 Larvik") - dette fjernes før validering (se
_POSTNR_STED_RE), ellers ville adressen feilaktig blitt forkastet (ser ikke
lenger ut til å ende i et husnummer).

Verifisert ved stikkprøve av 772 ekte titler på tvers av hele arkivet: 681/772
(88.2%) gir en adresse (745/772, 96.5%, matcher et gjenkjennelig
gbnr-mønster). Titler helt uten gbnr forsøkes likevel via
_gjett_adresse_uten_gbnr, med egne vakter mot falske positiver (se dens
docstring) - bl.a. avvises rene tall/skråstrek-fragmenter ("71/6/34-50") og
bare årstall ("Evaluering byggetilsyn 2017"). Resterende restandel er enten
reelt adresseløse (f.eks. "Saksbehandling byggesak - Larvik kommune") eller
uvanlige enkelttilfeller (dobbel skråstrek "3020/1440//01", en 3-veis
adresseliste helt uten gbnr "Elveveien 135, Løkka 6 og Løkka 8", der kun ETT
segment finnes og multi-adresse-splitteren ikke dekker komma+og-lister på 3+
adresser) som ikke er forsøkt dekket eksplisitt - samme akseptable "liten
restandel" som øvrige ACOS-kommuner i prosjektet.

Brukes av:
  - 2018-today_dump/bygg/main.py
  - 2018-today_dump/tilsyn/main.py
  - running_daily/bygg/app/main.py
  - running_daily/tilsyn/app/main.py
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
ROOT_URL = "https://innsynpluss.onacos.no"
BASE_URL = f"{ROOT_URL}/larvik"          # brukes kun for lenker/session-priming
API_BASE = f"{ROOT_URL}/api/presentation/v2/nye-innsyn"   # INGEN "/larvik/"-prefiks her!
INNSYN_SIDE_URL = f"{BASE_URL}/sok/?response=arkivsak_sok_tomdefault&"

PORTAL_ID = "72"
MENYPUNKT_ID = "1180"

DATASOURCE = "a33aa9ba-e239-4da4-bb8a-c08ce91d309f"

PAGE_SIZE = 100

KOMMUNE_NR = 3909   # Larvik (bekreftet via ws.geonorge.no/kommuneinfo/v1/sok)
KOMMUNE = "Larvik"

MAX_WORKERS = 8
TIMEOUT = 30
RETRIES = 3

HEADERS = {
    "Content-Type": "application/json",
    "PortalID": PORTAL_ID,
    "MenypunktID": MENYPUNKT_ID,
    "SprakID": "1",
    "X-ANTI-CSRF": "1",
    "Referer": INNSYN_SIDE_URL,
    "Origin": ROOT_URL,
}


def make_session():
    """Se modul-docstring - "overviewInit" krever samme økt-cookies (anti-
    forgery m.m.) som må settes av et forutgående GET mot selve søkesiden."""
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    s.get(INNSYN_SIDE_URL, timeout=TIMEOUT)
    return s


# --------------------------------------------------------------------------- #
# Delt adresse/gnr-bnr-parsing-grunnmur (samme lavnivå-motor som Ålesund, se
# der for full forklaring av hvert delmønster - kun _adresse_fra_blokk er
# reelt annerledes for Larvik, se modul-docstring)
# --------------------------------------------------------------------------- #
# Både vanlig bindestrek OG en-dash ("–") brukes om hverandre som skille i
# ekte Larvik-titler ("Tiltak unntatt søknadsplikt – Håkons gate 62 - gbnr
# 3020/909") - begge må splittes på, ellers blir en-dash-siden stående igjen
# klistret til beskrivelsen foran.
_SPLIT_RE = re.compile(r"(?:(?<=\s)[-–]\s*|[-–](?=\s))")


def _split_tittel(tittel):
    segs = [s.strip() for s in _SPLIT_RE.split((tittel or "").strip())]
    return [s for s in segs if s]


# "Gbnr"/"Gbnr."/"Gbnr:"/"Gnr."/"Gbfnr." (kjent skrivefeil for "Gbnr") - "b"
# og "f" er begge valgfrie, kolon eller punktum kan følge "nr".
_GBNR_PREFIX_RE = re.compile(r"^(?:\s*g\s*b?\s*[sf]?\s*nr\.?:?\s*)+", re.IGNORECASE)
_MFL_SUFFIX_RE = re.compile(r"\s*m\.?\s*/?\s*fl\.?\s*$", re.IGNORECASE)
_PAREN_RE = re.compile(r"\(.*?\)")
_FESTENR_SUFFIX_RE = re.compile(r"\s*(?:festenr|fnr)\.?\s*\d+\s*$", re.IGNORECASE)
_SEKSJON_SUFFIX_RE = re.compile(r"\s*seksjon(?:snr)?\.?\s*\d+\s*$", re.IGNORECASE)

_TOKEN = r"\d+\s*/\s*\d+(?:-\d+)?(?:/\s*\d+(?:/\s*\d+)?)?"
_BARE_NUM = r"\d+(?:-\d+)?"

_GNRBNR_LIST_FULL_RE = re.compile(
    rf"^{_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*"
    rf"(?:\s*(?:festenr|fnr)\.?\s*\d+)?"
    rf"(?:\s*seksjon(?:snr)?\.?\s*\d+)?"
    rf"\s*(?:m\.?\s*/?\s*fl\.?)?\s*[,.]?\s*$",
    re.IGNORECASE,
)
_LEADING_TOKEN_RE = re.compile(
    rf"^(?:g\s*b?\s*f?\s*nr\.?:?\s*)?({_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)(?:\s|,|$)",
    re.IGNORECASE,
)

_FULL_TOKEN_RE = re.compile(r"^(\d+)\s*/\s*(\d+)(?:-(\d+))?(?:/\s*(\d+)(?:/\s*(\d+))?)?$")
_BARE_NUM_RE = re.compile(r"^(\d+)(?:-(\d+))?$")
_SUBSPLIT_RE = re.compile(r"\s*(?:,|og)\s*", re.IGNORECASE)
# Skrivefeil/kommentar-ord limt rett inntil siste tall uten mellomrom ("gbnr
# 4039/21mant") - luket bort FØR resten av gbnr-gjenkjenningen, ellers
# forkastes hele segmentet som "ikke gbnr" og adressen i segmentet FORAN
# (som denne funksjonen aldri ser) går tapt. Trygt: ekte gnr/bnr-tall har
# aldri bokstaver rett inntil siste siffer uten mellomrom.
_TRAILING_ALPHA_JUNK_RE = re.compile(r"(?<=\d)[a-zæøå]+\s*$")


def _avpad(s):
    """Fjerner ledende nuller ('03' -> '3', '052' -> '52')."""
    return str(int(s))


def _er_gnrbnr_segment(seg):
    kandidat = _GBNR_PREFIX_RE.sub("", seg).strip()
    kandidat = _TRAILING_ALPHA_JUNK_RE.sub("", kandidat).strip()
    if _GNRBNR_LIST_FULL_RE.match(kandidat):
        return True
    stripped = _PAREN_RE.sub("", kandidat).strip()
    return bool(stripped) and bool(_GNRBNR_LIST_FULL_RE.match(stripped))


def _parse_gnrbnr_expr(seg):
    seg = _GBNR_PREFIX_RE.sub("", seg).strip()
    seg = _TRAILING_ALPHA_JUNK_RE.sub("", seg).strip()
    seg = _PAREN_RE.sub("", seg).strip()
    seg = _FESTENR_SUFFIX_RE.sub("", seg).strip()
    seg = _SEKSJON_SUFFIX_RE.sub("", seg).strip()
    seg = _MFL_SUFFIX_RE.sub("", seg).strip()
    seg = seg.rstrip(",.").strip()
    parts = [p.strip().rstrip(",.").strip() for p in _SUBSPLIT_RE.split(seg) if p.strip()]
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
    gnr/bnr-uttrykk, foran ELLER etter adressen."""
    start = None
    for i, seg in enumerate(segs):
        if _er_gnrbnr_segment(seg):
            start = i
            break
    if start is None:
        return None, None, None, None
    end = start
    gnr_bnr, matrikkel = [], []
    last_gnr = None
    for j in range(start, len(segs)):
        seg = segs[j]
        er_full = j == start or _er_gnrbnr_segment(seg)
        bare_m = None if er_full else (_BARE_NUM_RE.match(seg.strip()) if last_gnr else None)
        if j > start and not (er_full or bare_m):
            break
        end = j
        if bare_m:
            bnr_raw, bnr2_raw = bare_m.groups()
            bnr = _avpad(bnr_raw)
            if bnr2_raw:
                bnr = f"{bnr}-{_avpad(bnr2_raw)}"
            par = f"{last_gnr}/{bnr}"
            if par not in gnr_bnr:
                gnr_bnr.append(par)
            if par not in matrikkel:
                matrikkel.append(par)
            continue
        g, m = _parse_gnrbnr_expr(seg)
        for p in g:
            if p not in gnr_bnr:
                gnr_bnr.append(p)
            last_gnr = p.split("/")[0]
        for d in m:
            if d not in matrikkel:
                matrikkel.append(d)
    return start, end, gnr_bnr, matrikkel


def _finn_gnrbnr_blokk_lopende(segs):
    """Løs fallback: token FØRST i et segment, med mer fri tekst i SAMME
    segment uten skilletegn ("Heiskontroll Gbnr 3020/1753")."""
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
_OG_PAIR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+og\s+(\d+[A-Za-zæøåÆØÅ]?)$", re.IGNORECASE)
_SPACED_PAIR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+-\s+(\d+[A-Za-zæøåÆØÅ]?)$")
# To HELT ULIKE gater i samme kandidat ("Kristian Fredriks vei 23 og
# Damveien 1", "Rekkeviks gate 11 /  Iver Hesselbergs vei 8") - til forskjell
# fra _OG_PAIR_RE/_SPACED_PAIR_RE (samme gate, kun andre husnummeret er bart)
# har BEGGE sider her sitt eget gatenavn+husnummer. "og" ELLER "/" brukes om
# hverandre som skille i ekte Larvik-data.
_TO_ULIKE_ADRESSER_RE = re.compile(
    r"^([A-ZÆØÅ][\wæøåÆØÅ.\- ]*?\s\d+[A-Za-zæøåÆØÅ]?)\s*(?:og|/)\s*"
    r"([A-ZÆØÅ][\wæøåÆØÅ.\- ]*?\s\d+[A-Za-zæøåÆØÅ]?)$"
)
# Ekte gateadresser slutter (nesten) alltid på et husnummer - falske
# positiver forkastes eksplisitt (se Ålesund-modulens docstring for samme
# resonnement/eksempler).
_ENDS_WITH_HUSNR_RE = re.compile(r"\d+\s?[A-Za-zæøåÆØÅ]?\s*$")
_ENDS_WITH_SAKSREF_RE = re.compile(r"\d+\s*/\s*\d+\s*$")
# Postnummer+poststed hengt på etter husnummeret ("Vinjes vei 22, 3269
# Larvik") - fjernes FØR husnummer-sjekken, ellers ser adressen feilaktig ut
# til IKKE å ende i et husnummer og forkastes uriktig.
_POSTNR_STED_RE = re.compile(r",\s*\d{4}\s+[A-ZÆØÅ][\wæøåÆØÅ.\-]*\s*$")


def _utvid_flere_adresser(adresse):
    """To distinkte adresser skrives om til 'Adresse1; Adresse2' - samme
    "; "-konvensjon som ellers i prosjektet. Prøver først samme-gate-mønstrene
    (kun ANDRE husnummeret er bart, "Gate N1 og N2"), deretter to HELT ULIKE
    gater ("Gate1 N1 og/ / Gate2 N2", se _TO_ULIKE_ADRESSER_RE)."""
    if not adresse:
        return adresse
    for rx in (_OG_PAIR_RE, _SPACED_PAIR_RE):
        m = rx.match(adresse)
        if m:
            gate = m.group(1).strip()
            return f"{gate} {m.group(2)}; {gate} {m.group(3)}"
    m = _TO_ULIKE_ADRESSER_RE.match(adresse)
    if m:
        return f"{m.group(1).strip()}; {m.group(2).strip()}"
    return adresse


def _ferdigstill_adresse(adresse):
    if not adresse:
        return None
    adresse = adresse.strip().strip(",.;:").strip()
    if not adresse:
        return None
    adresse = _POSTNR_STED_RE.sub("", adresse).strip().strip(",.;:").strip()
    if not adresse:
        return None
    if _ENDS_WITH_SAKSREF_RE.search(adresse):
        return None
    if not _ENDS_WITH_HUSNR_RE.search(adresse):
        return None
    adresse = _NUM_LETTER_SPACE_RE.sub(r"\1\2", adresse)
    return _utvid_flere_adresser(adresse)


def _adresse_fra_blokk(segs, start, end):
    """Larvik-spesifikk (se modul-docstring): når gbnr-blokken IKKE er første
    segment, er adressen KUN segmentet RETT FØR blokken - ikke alt foran (som
    da ville dratt med seg beskrivelsen). Når blokken ER første segment (det
    sjeldnere "Gbnr X/Y - Adresse - beskrivelse"-mønsteret), se etter første
    ikke-gbnr-segment ETTER blokken i stedet."""
    if start > 0:
        return segs[start - 1]
    for kandidat in segs[end + 1:]:
        if _er_gnrbnr_segment(kandidat):
            continue
        return kandidat
    return None


def _matrikkelnr(matrikkel):
    return ("; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel) if matrikkel else None)


_EMBEDDED_TOKEN_RE = re.compile(
    rf"({_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)", re.IGNORECASE,
)
_GBNR_KEYWORD_TRAILING_RE = re.compile(r"(?:g\s*b\s*[sf]?\s*nr|b\s*g\s*nr|gnr|gbn)\.?:?\s*$", re.IGNORECASE)


def _finn_gnrbnr_blokk_innbakt(segs):
    """Løsest mulige fallback: gnr/bnr-uttrykk et sted MIDT I et segment,
    uten noe skilletegn mot adressen/beskrivelsen foran i det hele tatt."""
    for i, seg in enumerate(segs):
        kandidat = _PAREN_RE.sub("", seg)
        m = _EMBEDDED_TOKEN_RE.search(kandidat)
        if not m or m.start() == 0:
            continue
        adresse_del = kandidat[:m.start()].strip()
        adresse_del = _GBNR_KEYWORD_TRAILING_RE.sub("", adresse_del).strip()
        adresse_del = adresse_del.rstrip(",").strip()
        gnr_bnr, matrikkel = _parse_gnrbnr_expr(m.group(1))
        if not gnr_bnr:
            continue
        if not _HAS_DIGIT_RE.search(adresse_del):
            return None, gnr_bnr, matrikkel
        adresse = adresse_del
        return adresse, gnr_bnr, matrikkel
    return None, None, None


# Denne fallback-veien har INGEN gbnr-anker å støtte seg på (til forskjell
# fra de andre parse-stiene), så den trenger strengere vakter mot falske
# positiver enn resten av modulen - funnet via egen etterkontroll (ikke
# brukerrapportert): rene tall/skråstrek-fragmenter som "71/6/34-50" (rest av
# en matrikkelhenvisning som ikke ble gjenkjent som gbnr-segment) og
# generiske rapport-/strategititler der et bart årstall ("Evaluering
# byggetilsyn 2017") blir feiltolket som et husnummer av _ENDS_WITH_HUSNR_RE.
_STARTS_UPPER_RE = re.compile(r"^[A-ZÆØÅ]")
_ALLE_TALL_RE = re.compile(r"\d+[A-Za-zæøåÆØÅ]?")
_BART_AARSTALL_RE = re.compile(r"^(?:19|20)\d{2}$")


def _ser_ut_som_bare_aarstall(seg):
    """True hvis DET ENESTE tallet i segmentet er et bart 4-sifret årstall
    (1900-2099) uten bokstav-suffiks - typisk "Evaluering byggetilsyn 2017",
    ikke en reell adresse. Ekte adresser har enten et annet tall i tillegg,
    eller et husnummer som ikke ligner et årstall."""
    tall = _ALLE_TALL_RE.findall(seg)
    return len(tall) == 1 and bool(_BART_AARSTALL_RE.match(tall[0]))


def _gjett_adresse_uten_gbnr(segs):
    """Ingen gbnr funnet noe sted i tittelen (skjer for et lite mindretall
    titler, typisk "Tilsyn ... - Beskrivelse - Adresse[og/  /Adresse2]" helt
    uten matrikkelhenvisning) - prøv likevel segmentene, siste (der adressen
    som oftest står når det ikke er noe gbnr å forholde seg til) først.
    _ferdigstill_adresse (og dermed _utvid_flere_adresser) håndterer også her
    en evt. "Adresse1 og/ / Adresse2"-liste i samme segment."""
    for seg in reversed(segs):
        kandidat = seg.strip()
        if not _STARTS_UPPER_RE.match(kandidat):
            continue  # f.eks. "71/6/34-50" - rent tall/matrikkel-fragment
        if _ser_ut_som_bare_aarstall(kandidat):
            continue  # f.eks. "Evaluering byggetilsyn 2017"
        ferdig = _ferdigstill_adresse(seg)
        if ferdig:
            return ferdig
    return None


def parse_adresse_larvik(tittel, subtitle=None):
    """Standardparser for både bygg- og tilsynskilden, se modul-docstring."""
    segs = _split_tittel(tittel)
    if not segs:
        return None, None, None
    start, end, gnr_bnr, matrikkel = _finn_gnrbnr_blokk(segs)
    if start is None:
        start, end, gnr_bnr, matrikkel = _finn_gnrbnr_blokk_lopende(segs)
    if start is not None:
        adresse = _ferdigstill_adresse(_adresse_fra_blokk(segs, start, end))
        return adresse, (gnr_bnr or None), _matrikkelnr(matrikkel)
    adresse_raw, gnr_bnr, matrikkel = _finn_gnrbnr_blokk_innbakt(segs)
    if gnr_bnr is not None:
        adresse = _ferdigstill_adresse(adresse_raw)
        return adresse, (gnr_bnr or None), _matrikkelnr(matrikkel)
    # Ingen gbnr i det hele tatt noe sted i tittelen - se modul-docstring.
    return _gjett_adresse_uten_gbnr(segs), None, None


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        sakstype="at-a33aa9ba__e239__4da4__bb8a__c08ce91d309f-BS!qstXWj",
        adresse_parser=parse_adresse_larvik,
        hent_filer=True,
        periode="2018-nå",
    ),
    "tilsyn": dict(
        sakstype_navn="Tilsynssak",
        sakstype="at-a33aa9ba__e239__4da4__bb8a__c08ce91d309f-TIL!LHop6j",
        adresse_parser=parse_adresse_larvik,
        hent_filer=True,
        periode="2018-2019 (kun 3 saker totalt - se modul-docstring)",
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(method, session, path, body=None, retries=RETRIES):
    """Delt retry-løkke for _post/_get - samme oppsett (session, headers,
    timeout, backoff), kun selve HTTP-verbet skiller dem."""
    url = f"{API_BASE}/{path}"
    last_err = None
    for attempt in range(retries):
        try:
            if method == "POST":
                r = session.post(url, json=body, headers=HEADERS, timeout=TIMEOUT)
            else:
                r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{method} {path} feilet etter {retries} forsøk") from last_err


def _post(session, path, body, retries=RETRIES):
    return _request("POST", session, path, body=body, retries=retries)


def _get(session, path, retries=RETRIES):
    return _request("GET", session, path, retries=retries)


def get_case_list(kilde_key, session=None, max_items=None):
    """Henter identifier+saksnummer+dato for saker i én kilde, nyeste først.
    MERK: bruker "overviewInit", IKKE "overview" (se modul-docstring).
    max_items=N stopper paginering tidlig - brukes kun når man ikke også
    dato-filtrerer (se run_full_dump)."""
    kilde = KILDER[kilde_key]
    session = session or make_session()

    def body_for(page):
        return {
            "keyValues": [
                {"key": "Datasource", "value": DATASOURCE},
                {"key": "Dato", "value": "AllCases"},
                {"key": "FilterContent", "value": "CaseOnly"},
                {"key": "Sakstype", "value": kilde["sakstype"]},
                {"key": "pageSize", "value": str(PAGE_SIZE)},
                {"key": "page", "value": str(page)},
            ],
            "type": 0,
        }

    first = _post(session, "overviewInit", body_for(1))
    si = first["content"]["searchItems"]
    items = list(si["items"])
    page_count = si["pageCount"]

    if max_items and len(items) >= max_items:
        return items[:max_items]

    for page in range(2, page_count + 1):
        d = _post(session, "overviewInit", body_for(page))
        items.extend(d["content"]["searchItems"]["items"])
        if max_items and len(items) >= max_items:
            return items[:max_items]

    return items


def fetch_dokument_filer(session, dokument_identifier):
    """Henter fillommer for ETT dokument (samme responsform som Moss:
    "content.model.vedleggGruppe[]"). "fileUrl" er relativ til ROOT_URL
    (domenerota), IKKE til BASE_URL/"larvik" (inkluderer allerede "?pid=72")."""
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
                "url": ROOT_URL + f["fileUrl"] if f.get("fileUrl") else None,
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
        "url": f"{INNSYN_SIDE_URL}#/details/{identifier}",
        "dokumenter": dokumenter,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump - gjenopptakbar kjøring. Hvert fetch_case-kall får
# sin egen ferske make_session() (session_per_task) i stedet for å dele én
# sesjon på tvers av alle tråder.
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


def _atomic_write_json(data, path):
    """Skriver til en .tmp-fil og bytter den inn atomisk - brukes av både
    save_resultater og save_state for å unngå halvskrevne filer ved krasj."""
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def save_resultater(results, output_file):
    _atomic_write_json(list(results.values()), output_file)


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
    """Full historisk dump for én kilde. limit=N henter kun de N nyeste
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

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_case, make_session(), kilde_key, i, hent_filer): i for i in todo}
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
# Daglig endringslogg (running_daily) - liste-så-filtrer + "seen"-dedup.
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
    _atomic_write_json(state, state_file)


def _merk_sett(r, seen_saker, seen_jp, dato_iso):
    """Markerer en sak (og alle journalpostene dens) som sett per dags dato -
    delt av build_state_from_dump og run_daily."""
    seen_saker[r["document_id"]] = dato_iso
    for jp in r["dokumenter"]:
        if jp.get("journalpost_id"):
            seen_jp[jp["journalpost_id"]] = dato_iso


def build_state_from_dump(dump_file, today_fn=None):
    today_fn = today_fn or _i_dag_oslo
    data = json.loads(Path(dump_file).read_text(encoding="utf-8"))
    i_dag = today_fn().isoformat()
    seen_saker, seen_jp = {}, {}
    for r in data:
        if "dokumenter" not in r:
            continue
        _merk_sett(r, seen_saker, seen_jp, i_dag)
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

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_case, make_session(), kilde_key, i) for i in kandidat_ids]
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

        _merk_sett(r, seen_saker, seen_jp, ref_iso)

    feilet = sum(1 for r in results if "error" in r)
    write_changelog(nye_saker, nye_journalposter, ref_iso, output_dir, kilde["sakstype_navn"])
    prune_seen(state, ref_date)
    save_state(state, state_file)
    print(f"Ferdig ({feilet}/{len(kandidat_ids)} feilet under henting).")
