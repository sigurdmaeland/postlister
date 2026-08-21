"""Ålesund - ACOS "nye-innsyn" (samme plattform som Gjøvik/Moss/Sandefjord/
Tønsberg i dette prosjektet, egen selvhostet instans). Samme seksjonsoppdeling
som de andre ACOS-kommunene: konstanter -> adresseparsing -> KILDER -> API-kall
-> engangsdump -> daglig endringslogg.

SEKS DATAKILDER ("Datasource"-nøkkel-mekanisme, som Sandefjord/Tønsberg):
  - alesund_2024   : Ålesund fra 2024-nå (dagens system, MED vedlegg)
  - alesund_2020_2023 : Ålesund 2020-2023 + fellesnemnd 2017-2019 (samme
                        Datasource - fellesnemnda var overgangsorganet før
                        selve sammenslåingen trådte i kraft 01.01.2020)
  - alesund_2010_2019 : gamle Ålesund 2010-2019, FØR sammenslåingen
  - skodje_hist    : gamle Skodje kommune 2015-2019 (slått sammen inn i nye
                      Ålesund 01.01.2020)
  - orskog_hist    : gamle Ørskog kommune 2009-2019 (samme sammenslåing)
  - sandoy_hist    : gamle Sandøy kommune 2015-2019 (samme sammenslåing)
Alle seks er egne separate arkiv i samme ACOS-installasjon - INGEN av dem
viser flere treff enn 100/side ("pageSize" over dette ignoreres stille,
samme kjente edge case som resten av ACOS-kommunene).

VIKTIG DRIFTSDETALJ - autentisering: til forskjell fra Tønsberg/Gjøvik/
Sandefjord krever alesund.kommune.no sitt "api/presentation/v2/nye-innsyn"-
endepunkt at kallet ser ut som det kommer fra selve nettsiden - uten en
forutgående GET mot innsynssiden (for anti-forgery-cookies) OG en Referer/
Origin-header som peker til samme side, svarer serveren med en 302-omdirigering
til Azure AD-innlogging i stedet for søkeresultatet. make_session() gjør
begge deler; alle API-kall MÅ bruke denne (eller en session bygget på samme
måte), ellers feiler alt stille med en HTML-innloggingsside i stedet for JSON.

VEDLEGGSTILGANG varierer sak for sak (IKKE en fast per-kilde-regel) - et
dokument har enten en reell "fileUrl" (fritt nedlastbart) eller står oppført
som "X filer som ikkje er publisert" med tomt "fileUrl" og
"buttonText": "Bestill dokument" (må bestilles fra kommunen, ikke automatisk
tilgjengelig). Stikkprøve (10-40 saker per kilde) viste at Ålesund
2024/2020-2023/2010-2019 og Sandøy stort sett har frie vedlegg (~85-99 %),
mens Skodje og Ørskog har en vesentlig større "må bestilles"-andel (~30-36 %
frie i stikkprøven) - trolig fordi disse to minste arkivene har lavere andel
publiserte vedlegg fra da de ble skrapet/lastet opp originalt. Alle seks
kilder har hent_filer=True likevel (henting er alltid forsøkt), og hvert
enkelt vedlegg merkes med "offentlig_tilgjengelig" slik at forskjellen synes
i output-dataen i stedet for å anta noe per kilde.

ADRESSE/GNR-BNR-PARSING - fem av seks kilder deler samme "Gbnr GNR/BNR[,
GNR/BNR][ og GNR/BNR] [- adresse] - beskrivelse"-grunnform (identisk motor
som Tønsberg/Re sin generiske fallback-parser, gjenbrukt her via
parse_adresse_generisk), men med lokale stavevarianter av selve
"Gbnr"-nøkkelordet ("Gbnr", "Gbnr.", "Gnr.", doble prefiks som "Gbnr. Gbnr."
- alle håndteres av _GBNR_PREFIX_RE) og noen kilde-spesifikke kvirker:

  - alesund_2024/2020_2023/2010_2019: rendyrket "Gbnr. GNR/BNR - adresse -
    beskrivelse", verifisert mot 300 ekte titler per kilde (medgir ~80-85 %
    med adresse funnet).

  - orskog_hist: samme grunnform, men blander i tillegg inn et rent
    "Gnr NN bnr NN" (mellomromsseparert, uten skråstrek)-format om hverandre
    - parse_adresse_orskog prøver (a) den vanlige skråstrek-formen først, og
    (b) normaliserer "Gnr NN bnr NN" til skråstrek-form og prøver på nytt
    hvis (a) ikke gir noe.

  - sandoy_hist: samme grunnform, men mangler ofte skilletegn helt mellom
    gnr/bnr og en kort beskrivelse i SAMME segment ("Gbnr 12/045 Nytt bygg")
    - dekkes av den generiske motorens løse fallback
    (_finn_gnrbnr_blokk_lopende), som gir kun gnr/bnr uten adresse i disse
    tilfellene (ingen adresse er reelt til stede uansett).

  - skodje_hist: HELT ANNERLEDES format - "Gnr.NN Bnr.NNN[ og Bnr.NNN] -
    Søkjar-/eigarnamn - beskrivelse". Ingen gateadresse forekommer NOEN
    gang i dette arkivet (kun personnavn/organisasjonsnavn i midtsegmentet),
    bekreftet mot 300 ekte titler - parse_adresse_skodje henter derfor KUN
    gnr/bnr og returnerer alltid adresse=None.

GNR-OFFSET VED SAMMENSLÅING: skodje_hist/orskog_hist/sandoy_hist sine
titler bruker den GAMLE, egne kommunens gnr-numrering (fra før 01.01.2020).
Kartverket sin offisielle oversikt over gnr-endringer ved kommunereformen
("Endringer i gårdsnummer i nye og sammenslåtte kommuner") viser at Ørskog
fikk +600, Skodje +500 og Sandøy +800 lagt til sitt gnr ved sammenslåingen
inn i nye Ålesund (Ålesund sitt eget gnr ble uendret, +0) - bnr endres
aldri. Uten dette tillegget ville matrikkelnr for disse tre arkivene pekt
på FEIL eiendom under dagens KOMMUNE_NR. Se _med_gnr_offset og de tre
respektive adresseparserne.

Falske positiver luket ut under bygging (verifisert mot ekte data, se også
tilsvarende arbeid gjort for Fredrikstad/Oslo i einnsyn/-modulene): en
adressekandidat må SLUTTE på et husnummer-mønster (ikke bare inneholde et
tall et sted i teksten - Sandøy hadde ellers falske treff som "Søknad om
løyve til tiltak for 3 tomter" og saksreferanser på "ÅÅÅÅ/NN"-form i enden
av en kandidat forkastes eksplisitt (ekte husnummer skrives aldri slik).

Alle adresseparsere har signaturen (tittel, subtitle=None) -> (adresse,
gnr_bnr_liste, matrikkelnr) - subtitle brukes ikke her.

Brukes av:
  - alesund-2024-today_dump/bygg/main.py
  - alesund-2020-2023_dump/bygg/main.py
  - alesund-2010-2019_dump/bygg/main.py
  - skodje-2015-2019_dump/bygg/main.py
  - orskog-2009-2019_dump/bygg/main.py
  - sandoy-2015-2019_dump/bygg/main.py
  - running_daily/bygg/app/main.py   (KUN alesund_2024 - de fem andre er
                                       lukkede historiske arkiv uten nye saker)
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
BASE_URL = "https://alesund.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"
INNSYN_SIDE_URL = f"{BASE_URL}/innsyn/sok-og-postliste/"

PORTAL_ID = "1"
MENYPUNKT_ID = "1588"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 1508  # Ålesund (gjeldende siden sammenslåingen 01.01.2020 med
                   # Skodje/Ørskog/Sandøy - bekreftet via ws.geonorge.no)
KOMMUNE = "Ålesund"

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
    "Origin": BASE_URL,
}


def make_session():
    """Se modul-docstring: et rent POST mot API-et uten en forutgående GET
    mot selve innsynssiden (for anti-forgery-cookiene) svarer med en
    302-omdirigering til Azure AD-innlogging i stedet for JSON."""
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    s.get(INNSYN_SIDE_URL, timeout=TIMEOUT)
    return s


# --------------------------------------------------------------------------- #
# Delt adresse/gnr-bnr-parsing-grunnmur (generisk motor, se modul-docstring)
# --------------------------------------------------------------------------- #
_SPLIT_RE = re.compile(r"(?:(?<=\s)-\s*|-(?=\s))")


def _split_tittel(tittel):
    segs = [s.strip() for s in _SPLIT_RE.split((tittel or "").strip())]
    return [s for s in segs if s]


# "Gbnr"/"Gbnr."/"Gnr."/doble prefiks ("Gbnr. Gbnr.") - "b" er valgfri og
# hele prefikset kan gjenta seg (Ørskog/Ålesund har begge stavemåter).
_GBNR_PREFIX_RE = re.compile(r"^(?:\s*g\s*b?\s*nr\.?\s*)+", re.IGNORECASE)
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
    rf"^(?:g\s*b?\s*nr\.?\s*)?({_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)(?:\s|,|$)",
    re.IGNORECASE,
)

_FULL_TOKEN_RE = re.compile(r"^(\d+)\s*/\s*(\d+)(?:-(\d+))?(?:/\s*(\d+)(?:/\s*(\d+))?)?$")
_BARE_NUM_RE = re.compile(r"^(\d+)(?:-(\d+))?$")
_SUBSPLIT_RE = re.compile(r"\s*(?:,|og)\s*", re.IGNORECASE)


def _avpad(s):
    """Fjerner ledende nuller ('03' -> '3', '052' -> '52')."""
    return str(int(s))


def _append_unique(lst, val):
    if val not in lst:
        lst.append(val)


def _er_gnrbnr_segment(seg):
    kandidat = _GBNR_PREFIX_RE.sub("", seg).strip()
    if _GNRBNR_LIST_FULL_RE.match(kandidat):
        return True
    stripped = _PAREN_RE.sub("", kandidat).strip()
    return bool(stripped) and bool(_GNRBNR_LIST_FULL_RE.match(stripped))


def _parse_gnrbnr_expr(seg):
    seg = _GBNR_PREFIX_RE.sub("", seg).strip()
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
        _append_unique(gnr_bnr, par)
        d = (f"{gnr}/{bnr}/{feste or '0'}/{seksjon or '0'}"
             if (feste and feste != "0") or (seksjon and seksjon != "0") else par)
        _append_unique(matrikkel, d)
    return gnr_bnr, matrikkel


def _finn_gnrbnr_blokk(segs):
    """Finner den (evt. flerledd) blokken av segmenter som UTELUKKENDE er
    gnr/bnr-uttrykk, foran ELLER etter adressen. En sjelden variant (sett i
    Sandøy) skriver en bnr-liste med BINDESTREK i stedet for komma/"og"
    ("Gbnr 07/240 - 261 - 262 ...") - hvert dash-segment som er UTELUKKENDE
    et bart tall videreføres da som nok en bnr under samme gnr (last_gnr),
    slik at det ikke feiltolkes som en adresse-kandidat."""
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
            _append_unique(gnr_bnr, par)
            _append_unique(matrikkel, par)
            continue
        g, m = _parse_gnrbnr_expr(seg)
        for p in g:
            _append_unique(gnr_bnr, p)
            last_gnr = p.split("/")[0]
        for d in m:
            _append_unique(matrikkel, d)
    return start, end, gnr_bnr, matrikkel


# Et navn på sameiet/borettslaget/eierselskapet rett foran selve gateadressen
# i SAMME segment ("201/361 Sameiet Kipervikgata 23") er ikke en del av
# adressen. Rammer kun navnet på selve eierformen, ikke et vanlig gatenavn.
_LEDENDE_EIERFORM_RE = re.compile(
    r"^(?:sameiet|sameie|borettslaget|burettslaget)\s+", re.IGNORECASE
)


def _finn_gnrbnr_blokk_lopende(segs):
    """Løs fallback: token FØRST i et segment, med mer fri tekst i SAMME
    segment ("Gbnr 12/045 Nytt bygg", uten skilletegn - typisk Sandøy).
    Returnerer i tillegg resten av segmentet etter selve tallet/tallene
    ("Sameiet Kipervikgata 23" fra "201/361 Sameiet Kipervikgata 23") - uten
    denne gikk adressen i SAMME segment som gnr/bnr-tallet helt tapt, siden
    _adresse_fra_blokk kun ser på segmenter ETTER denne blokken, aldri
    resten av selve blokk-segmentet."""
    for i, seg in enumerate(segs):
        kandidat = _PAREN_RE.sub("", seg.strip())
        m = _LEADING_TOKEN_RE.match(kandidat)
        if m:
            gnr_bnr, matrikkel = _parse_gnrbnr_expr(m.group(1))
            if gnr_bnr:
                rest = _LEDENDE_EIERFORM_RE.sub("", kandidat[m.end():].strip()).strip()
                return i, i, gnr_bnr, matrikkel, rest
    return None, None, None, None, None


_HAS_DIGIT_RE = re.compile(r"\d")
_NUM_LETTER_SPACE_RE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_OG_PAIR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+og\s+(\d+[A-Za-zæøåÆØÅ]?)$", re.IGNORECASE)
_SPACED_PAIR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)\s+-\s+(\d+[A-Za-zæøåÆØÅ]?)$")
# Ekte gateadresser i disse arkivene slutter (nesten) alltid på et
# husnummer - falske positiver (setninger som tilfeldigvis inneholder et
# tall et sted, f.eks. "... for 3 tomter", "Vidareføring av EPH-sak
# 2012/95") er vanlige nok i Sandøy/Ørskog til at en enkel "inneholder et
# tall"-sjekk ikke er nok. Krever i stedet at kandidaten SLUTTER på et
# husnummer-mønster, og forkaster eksplisitt saksreferanser på "N/N"-form i
# enden (ekte husnummer skrives aldri slik).
#
# Tallet MÅ stå som et eget "ord" - dvs. være foran av tekststart eller
# mellomrom, ikke klistret rett på foregående bokstaver uten mellomrom. Uten
# dette kravet godtas felt-/reguleringskoder som "BK2"/"KS1" (bokstaver
# FØRST, så tall - motsatt av et ekte husnummer, som alltid er tall FØRST,
# evt. med ÉN bokstav etter, "20J"/"18A") som om siste tallet i dem var et
# reelt husnummer - bekreftet falskt positiv: "Ratvika BK2" ble godtatt som
# adresse.
_ENDS_WITH_HUSNR_RE = re.compile(r"(?:^|\s)\d+\s?[A-Za-zæøåÆØÅ]?\s*$")
_ENDS_WITH_SAKSREF_RE = re.compile(r"\d+\s*/\s*\d+\s*$")
# En ledende klage-/beskrivelsesfrase foran selve gateadressen ("Ulempe for
# Kaptein Lingesveg 90") er ikke en del av adressen.
_LEDENDE_BESKRIVELSE_RE = re.compile(r"^ulempe\s+for\s+", re.IGNORECASE)


def _utvid_flere_adresser(adresse):
    """To distinkte adresser på samme gate skrives om til 'Gate N1; Gate
    N2' - samme "; "-konvensjon som ellers i prosjektet."""
    if not adresse:
        return adresse
    for rx in (_OG_PAIR_RE, _SPACED_PAIR_RE):
        m = rx.match(adresse)
        if m:
            gate = m.group(1).strip()
            return f"{gate} {m.group(2)}; {gate} {m.group(3)}"
    return adresse


def _ferdigstill_adresse(adresse):
    if not adresse:
        return None
    adresse = adresse.strip().strip(",.;:").strip()
    adresse = _LEDENDE_BESKRIVELSE_RE.sub("", adresse).strip()
    if not adresse:
        return None
    if _ENDS_WITH_SAKSREF_RE.search(adresse):
        return None
    if not _ENDS_WITH_HUSNR_RE.search(adresse):
        return None
    adresse = _NUM_LETTER_SPACE_RE.sub(r"\1\2", adresse)
    return _utvid_flere_adresser(adresse)


def _adresse_fra_blokk(segs, start, end):
    """Ørskog skriver noen ganger "Gbnr X/Y - beskrivelse - adresse" i
    stedet for den vanlige "Gbnr X/Y - adresse - beskrivelse"-rekkefølgen
    (f.eks. "Gbnr 89/5 - Tilbygg og bruksendring - Giskemovegen 76",
    verifisert mot ekte data) - prøver derfor ALLE segmenter etter
    gnr/bnr-blokken (ikke bare det første) og returnerer det første som
    faktisk består adressevalideringen (_ferdigstill_adresse), i stedet for
    å gi opp på et første, ugyldig kandidatsegment."""
    if start > 0:
        return " - ".join(segs[:start])
    for kandidat in segs[end + 1:]:
        if _er_gnrbnr_segment(kandidat):
            continue
        if _ferdigstill_adresse(kandidat):
            return kandidat
    return None


def _matrikkelnr(matrikkel):
    return ("; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel) if matrikkel else None)


def _med_gnr_offset(strenger, offset):
    """Legger et fast gnr-offset på gnr-delen (FØRSTE ledd) av hver
    "gnr/bnr[/feste/seksjon]"-streng, uten å røre bnr/feste/seksjon.
    Brukes for skodje_hist/orskog_hist/sandoy_hist, der saks-titlene bruker
    den GAMLE, egne kommunens gnr-numrering fra før 01.01.2020-
    sammenslåingen. Dagens offisielle matrikkelnr for disse eiendommene
    bruker Kartverket sitt faste, dokumenterte tillegg (Ørskog +600, Skodje
    +500, Sandøy +800 - se "Endringer i gårdsnummer i nye og sammenslåtte
    kommuner", kartverket.no) lagt til den gamle kommunens gnr; bnr endres
    aldri ved en sammenslåing."""
    ut = []
    for s in strenger:
        gnr, rest = s.split("/", 1)
        ny = f"{int(gnr) + offset}/{rest}"
        if ny not in ut:
            ut.append(ny)
    return ut


_EMBEDDED_TOKEN_RE = re.compile(
    rf"({_TOKEN}(?:\s*(?:,|og)\s*(?:{_TOKEN}|{_BARE_NUM}))*)", re.IGNORECASE,
)
_GBNR_KEYWORD_TRAILING_RE = re.compile(r"(?:g\s*b\s*nr|b\s*g\s*nr|gnr|gbn)\.?\s*$", re.IGNORECASE)


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
            # Teksten foran gnr/bnr-uttrykket har ikke noe tall, så den er
            # neppe en reell gateadresse ("Byggeplanar Lånavegen gnr.
            # 97/359, ...") - behold likevel selve gnr/bnr-paret/-lista.
            return None, gnr_bnr, matrikkel
        adresse = " - ".join(segs[:i] + [adresse_del]) if i > 0 else adresse_del
        return adresse, gnr_bnr, matrikkel
    return None, None, None


def _parse_adresse_generisk_indre(tittel, gnr_offset=0):
    segs = _split_tittel(tittel)
    if not segs:
        return None, None, None
    rest_i_segment = None
    start, end, gnr_bnr, matrikkel = _finn_gnrbnr_blokk(segs)
    if start is None:
        start, end, gnr_bnr, matrikkel, rest_i_segment = _finn_gnrbnr_blokk_lopende(segs)
    if start is not None:
        adresse_kandidat = _adresse_fra_blokk(segs, start, end)
        if not adresse_kandidat and rest_i_segment:
            adresse_kandidat = rest_i_segment
        adresse = _ferdigstill_adresse(adresse_kandidat)
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


def parse_adresse_generisk(tittel, subtitle=None, gnr_offset=0):
    """Standardparser for alesund_2024/alesund_2020_2023/alesund_2010_2019
    (og førsteforsøk for orskog_hist), se modul-docstring.
    gnr_offset: fast tillegg på gnr-delen, brukt av pre-2020-arkivene som
    har sin egen gamle gnr-numrering (se _med_gnr_offset).

    Siste fallback (etter _parse_adresse_generisk_indre): et rent
    "Gnr NN bnr NN"-format UTEN skråstrek ("Gnr.118 bnr. 976") - Ørskogs
    hovedformat (se modul-docstring), men forekommer sjeldnere også i de
    andre kildene og fanges derfor her, ikke bare i parse_adresse_orskog."""
    adresse, gnr_bnr, matrikkel = _parse_adresse_generisk_indre(tittel, gnr_offset=gnr_offset)
    if gnr_bnr:
        return adresse, gnr_bnr, matrikkel
    m = _GNR_BNR_LABELED_RE.search(tittel or "")
    if not m:
        return None, None, None
    normalisert = tittel[:m.start()] + f"Gbnr {m.group(1)}/{m.group(2)}" + tittel[m.end():]
    adresse, gnr_bnr, matrikkel = _parse_adresse_generisk_indre(normalisert, gnr_offset=gnr_offset)
    if gnr_bnr:
        return adresse, gnr_bnr, matrikkel
    par = f"{int(m.group(1)) + gnr_offset}/{_avpad(m.group(2))}"
    return None, [par], _matrikkelnr([par])


# --------------------------------------------------------------------------- #
# Ørskog - blander inn et eget "Gnr NN bnr NN"-format (se modul-docstring)
# --------------------------------------------------------------------------- #
_GNR_BNR_LABELED_RE = re.compile(r"[Gg]nr\.?\s*(\d+)\s*[Bb]nr\.?\s*(\d+)")


def parse_adresse_orskog(tittel, subtitle=None):
    """Ørskog 2009-2019 - se modul-docstring. Legger Kartverket sitt faste
    +600-gnr-offset på alt som finnes (se _med_gnr_offset), siden titlene
    bruker den gamle Ørskog-kommunens egen gnr-numrering. Det "Gnr NN bnr
    NN"-formatet uten skråstrek som er hovedformen her, håndteres nå av
    parse_adresse_generisk sin egen fallback (samme mekanisme, delt med de
    andre kildene) - denne funksjonen er bare en tynn wrapper for offset."""
    return parse_adresse_generisk(tittel, gnr_offset=600)


def parse_adresse_sandoy(tittel, subtitle=None):
    """Sandøy 2015-2019 - se modul-docstring. Samme generiske motor som
    dagens Ålesund, men med Kartverket sitt faste +800-gnr-offset for den
    gamle Sandøy-kommunens egen gnr-numrering (se _med_gnr_offset)."""
    return parse_adresse_generisk(tittel, gnr_offset=800)


# --------------------------------------------------------------------------- #
# Skodje - eget, enklere format: ALDRI gateadresse (se modul-docstring)
# --------------------------------------------------------------------------- #
_BNR_OG_RE = re.compile(r"(?:,|og)\s*[Bb]nr\.?\s*(\d+)", re.IGNORECASE)


def parse_adresse_skodje(tittel, subtitle=None):
    """Skodje 2015-2019 - "Gnr.NN Bnr.NNN[ og Bnr.NNN] - Navn - beskrivelse".
    Ingen gateadresse forekommer i dette arkivet (kun søkjar-/eigarnamn i
    midtsegmentet) - returnerer derfor alltid adresse=None. gnr-delen får
    Kartverket sitt faste +500-offset for den gamle Skodje-kommunens egen
    gnr-numrering (se _med_gnr_offset)."""
    if not tittel:
        return None, None, None
    m = _GNR_BNR_LABELED_RE.search(tittel)
    if not m:
        return None, None, None
    gnr = str(int(m.group(1)) + 500)
    bnr_liste = [_avpad(m.group(2))]
    rest = tittel[m.end():].split(" - ", 1)[0]
    for m2 in _BNR_OG_RE.finditer(rest):
        b = _avpad(m2.group(1))
        if b not in bnr_liste:
            bnr_liste.append(b)
    gnr_bnr = [f"{gnr}/{b}" for b in bnr_liste]
    return None, gnr_bnr, _matrikkelnr(gnr_bnr)


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "alesund_2024": dict(
        sakstype_navn="Byggesak",
        datasource="1acb36f4-d80e-43d7-8ae1-0ecc2069589e",
        sakstype="at-1acb36f4__d80e__43d7__8ae1__0ecc2069589e-BS!EmJ5Xf",
        adresse_parser=parse_adresse_generisk,
        hent_filer=True,
        periode="2024-nå",
    ),
    "alesund_2020_2023": dict(
        sakstype_navn="Byggesak",
        datasource="b6ee633d-2b3b-436e-9663-c1cb4cea660f",
        sakstype="at-b6ee633d__2b3b__436e__9663__c1cb4cea660f-BS!YsxAwa",
        adresse_parser=parse_adresse_generisk,
        hent_filer=True,
        periode="2020-2023 (Ålesund) + fellesnemnd 2017-2019",
    ),
    "alesund_2010_2019": dict(
        sakstype_navn="Byggesak",
        datasource="f654b888-9e81-49a7-9a1a-b4a251d1a672",
        sakstype="at-f654b888__9e81__49a7__9a1a__b4a251d1a672-BS!asAJ9Q",
        adresse_parser=parse_adresse_generisk,
        hent_filer=True,
        periode="2010-2019 (gamle Ålesund)",
    ),
    "skodje_hist": dict(
        sakstype_navn="Byggesak",
        datasource="b9632c01-cd90-4e34-808e-48d464564078",
        sakstype="at-b9632c01__cd90__4e34__808e__48d464564078-BYG!U1B07J",
        adresse_parser=parse_adresse_skodje,
        hent_filer=True,
        periode="2015-2019 (Skodje)",
    ),
    "orskog_hist": dict(
        sakstype_navn="Byggesak",
        datasource="0485f790-adbc-410a-a46d-aadc0851aafb",
        sakstype="at-0485f790__adbc__410a__a46d__aadc0851aafb-BS!rmetp6",
        adresse_parser=parse_adresse_orskog,
        hent_filer=True,
        periode="2009-2019 (Ørskog)",
    ),
    "sandoy_hist": dict(
        sakstype_navn="Byggesak",
        datasource="301e1832-5d77-4b90-87d2-07f8e2775b77",
        sakstype="at-301e1832__5d77__4b90__87d2__07f8e2775b77-BYGG!Nu2bRZ",
        adresse_parser=parse_adresse_sandoy,
        hent_filer=True,
        periode="2015-2019 (Sandøy)",
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(method, session, path, body=None, retries=RETRIES):
    """Delt retry/feilhåndtering for _post/_get under - samme mønster
    (raise_for_status + eksponentiell-ish backoff) uansett HTTP-verb."""
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
    """Henter identifier+saksnummer+dato for saker i én datakilde, nyeste
    først. "Datasource" MÅ være med i body. max_items=N stopper paginering
    tidlig - brukes kun når man ikke også dato-filtrerer."""
    kilde = KILDER[kilde_key]
    session = session or make_session()

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
    """Henter fillommer for ETT dokument. Hvert enkelt vedlegg merkes med
    "offentlig_tilgjengelig" - se modul-docstring om hvorfor dette varierer
    sak for sak i stedet for å være en fast per-kilde-regel."""
    d = _get(session, f"dokument/{dokument_identifier}?fromMeeting=false")
    model = d.get("content", {}).get("model") or {}
    bestilling_id = (model.get("bestilling") or {}).get("identifikator")
    filer = []
    for gruppe in model.get("vedleggGruppe") or []:
        kategori = gruppe.get("title")
        for f in gruppe.get("vedlegg") or []:
            har_fil = bool(f.get("fileUrl"))
            filer.append({
                "navn": f.get("title"),
                "kategori": kategori,
                "filtype": f.get("filtype"),
                "storrelse": f.get("filstorrelseFormatted"),
                "url": BASE_URL + f["fileUrl"] if har_fil else None,
                "offentlig_tilgjengelig": har_fil,
                "bestilling_id": None if har_fil else bestilling_id,
            })
    return filer


def _finn_metadata(blocks, key):
    for b in blocks or []:
        if b.get("type") == "Metadata" and b.get("key") == key:
            return b.get("text") or b.get("title")
    return None


def _finn_property(blocks, *titler):
    """Ålesund blander bokmål/nynorsk i PropertyList-titlene ("Avsendar" i
    stedet for "Avsender" er observert i praksis) - godtar derfor flere
    alternative titler for samme egenskap."""
    for b in blocks or []:
        if b.get("type") == "PropertyList":
            for p in b.get("b") or []:
                if p.get("type") == "Property" and p.get("title") in titler:
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
                "avsender": _finn_property(doc_blocks, "Avsender", "Avsendar"),
                "mottaker": _finn_property(doc_blocks, "Mottaker", "Mottakar"),
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
        "saksbehandler": props.get("saksbehandler") or props.get("sakshandsamar"),
        "status": props.get("status"),
        "dato": props.get("dato"),
        "url": f"{INNSYN_SIDE_URL}#/details/{identifier}",
        "dokumenter": dokumenter,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump - gjenopptakbar kjøring (KUN alesund_2024, de fem
# andre er lukkede historiske arkiv som aldri får nye saker). Hvert
# fetch_case-kall får sin egen ferske make_session() (session_per_task) i
# stedet for å dele én sesjon på tvers av alle tråder.
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
# Daglig endringslogg (running_daily, KUN alesund_2024) - liste-så-filtrer +
# "seen"-dedup.
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


def build_state_from_dump(dump_file, today_fn=None):
    today_fn = today_fn or _i_dag_oslo
    data = json.loads(Path(dump_file).read_text(encoding="utf-8"))
    i_dag = today_fn().isoformat()
    seen_saker, seen_jp = {}, {}
    for r in data:
        if "dokumenter" not in r:
            continue
        seen_saker[r["document_id"]] = i_dag
        for jp in r["dokumenter"]:
            if jp.get("journalpost_id"):
                seen_jp[jp["journalpost_id"]] = i_dag
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

        seen_saker[cid] = ref_iso
        for jp in r["dokumenter"]:
            if jp.get("journalpost_id"):
                seen_jp[jp["journalpost_id"]] = ref_iso

    feilet = sum(1 for r in results if "error" in r)
    write_changelog(nye_saker, nye_journalposter, ref_iso, output_dir, kilde["sakstype_navn"])
    prune_seen(state, ref_date)
    save_state(state, state_file)
    print(f"Ferdig ({feilet}/{len(kandidat_ids)} feilet under henting).")
