"""Sandefjord - ACOS "nye-innsyn" (samme plattform som Gjøvik/Moss/Tønsberg
i dette prosjektet, egen selvhostet instans). Filen følger samme
seksjonsoppdeling som de andre ACOS-kommunene: konstanter -> adresseparsing
-> KILDER -> API-kall -> engangsdump -> daglig endringslogg.

Sandefjord har to datakilder (egen "Datasource"-nøkkel i API-body):
  - bygg_ny   : Byggesak, 2017-nå (dagens system, MED vedlegg)
  - tilsyn_ny : Byggetilsynssak, 2017-nå (dagens system, MED vedlegg)
Uten "Datasource" i body ignoreres Sakstype-filteret stille - MÅ alltid med.

ADRESSE/GNR-BNR-PARSING: "Gbnr GNR/BNR[...] - Adresse - beskrivelse" - gnr/bnr
alltid først. Støtter "og"-forkortet flerpunkts-notasjon ("114/82 og 99") og
placeholder "GBNR. /" (ingen eiendom -> adresse=None). Se parse_adresse_ny.
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
BASE_URL = "https://www.sandefjord.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"

PORTAL_ID = "1"
MENYPUNKT_ID = "6093"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 3907   # gjeldende nummer (var 3804 2020-2023, 0710 før - se
                    # prosjektkonvensjon: historiske arkiv bruker også dette)
KOMMUNE = "Sandefjord"

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
# a) bygg_ny/tilsyn_ny: "Gbnr GNR/BNR[...] - Adresse - beskrivelse"
# --------------------------------------------------------------------------- #
_FULL_TOKEN_RE = re.compile(r"^(\d+)/(\d+)(?:/(\d+)(?:/(\d+))?)?$")
_BARE_NUM_RE = re.compile(r"^\d+$")
_MFL_SUFFIX_RE = re.compile(r"\s*m\.?\s*/?\s*fl\.?\s*$", re.IGNORECASE)
_SUBSPLIT_RE = re.compile(r"\s*(?:,|og)\s*", re.IGNORECASE)
_GNRBNR_EXPR_RE = re.compile(
    r"^\d+/\d+(?:/\d+(?:/\d+)?)?"
    r"(?:\s*(?:,|og)\s*(?:\d+/\d+(?:/\d+(?:/\d+)?)?|\d+))*"
    r"\s*(?:m\.?\s*/?\s*fl\.?)?\s*$",
    re.IGNORECASE,
)
# Ekte bindestrek-skilletegn (mellomrom på minst én side), ikke et bart
# husnummer-spenn uten mellomrom ("40-44").
_SPLIT_RE = re.compile(r"(?:(?<=\s)-\s*|-(?=\s))")


def _split_tittel(tittel):
    segs = [s.strip() for s in _SPLIT_RE.split((tittel or "").strip())]
    return [s for s in segs if s]


def _dedup_append(liste, verdi):
    """Legger til `verdi` i `liste` hvis den ikke allerede er der - bevarer
    rekkefølge (brukes for gnr_bnr/matrikkel-samling, som ellers lett kunne
    fått duplikater ved flerpunkts-notasjon)."""
    if verdi not in liste:
        liste.append(verdi)


def _parse_gnrbnr_expr(seg):
    """'249/2 og 249/7', '114/82 og 99', '173/144/0/5' -> (gnr_bnr_liste, matrikkel_deler)."""
    seg = _MFL_SUFFIX_RE.sub("", seg).strip()
    parts = [p.strip() for p in _SUBSPLIT_RE.split(seg) if p.strip()]
    gnr_bnr, matrikkel = [], []
    last_gnr = None
    for p in parts:
        m = _FULL_TOKEN_RE.match(p)
        if m:
            gnr, bnr, feste, seksjon = m.groups()
            last_gnr = gnr
        elif _BARE_NUM_RE.match(p) and last_gnr:
            gnr, bnr, feste, seksjon = last_gnr, p, None, None
        else:
            continue
        par = f"{gnr}/{bnr}"
        _dedup_append(gnr_bnr, par)
        d = f"{gnr}/{bnr}/{feste or '0'}/{seksjon or '0'}" if (feste and feste != "0") or (seksjon and seksjon != "0") else par
        _dedup_append(matrikkel, d)
    return gnr_bnr, matrikkel


# --------------------------------------------------------------------------- #
# Adresse-validering/-normalisering, delt av parserne under.
# --------------------------------------------------------------------------- #
_ADDR_LIKE_RE = re.compile(r"[A-Za-zÆØÅæøå].*\d+\s*[A-Za-z]?\s*$")
_NUM_LETTER_SPACE_RE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_HAS_DIGIT_RE = re.compile(r"\d")


def _normaliser_nummer_bokstav(adresse):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks ("4 C" -> "4C") -
    ellers er adressen ikke søkbar i eiendomsregisteret."""
    if not adresse:
        return adresse
    return _NUM_LETTER_SPACE_RE.sub(r"\1\2", adresse)


def _gyldig_adresse(adresse):
    """En kandidat telles som reell adresse hvis den inneholder minst ett
    tall - filtrerer bort rene sted-/bygningsnavn uten husnummer."""
    return bool(adresse and _HAS_DIGIT_RE.search(adresse.strip()))


def _ferdigstill_adresse(adresse):
    if not adresse:
        return None
    adresse = adresse.strip().strip(",.;:").strip()
    adresse = _normaliser_nummer_bokstav(adresse)
    return adresse if _gyldig_adresse(adresse) else None


def parse_adresse_ny(tittel, subtitle=None):
    """Sandefjord fra 2017-nå (Byggesak og Byggetilsynssak).

    RUNDE 1 (2026-09-07): "GBNR. 3907/484-1 - Stokke Ravei 716 - ..." ga
    tidligere BÅDE adresse=None og gnr_bnr=None, fordi et ugyldig/korrupt
    Gbnr-uttrykk (her med en feilaktig bindestrek i stedet for skråstrek
    foran bnr-delen) gjorde at funksjonen avbrøt HELT før den i det hele
    tatt lette etter en adresse i resten av tittelen.

    RUNDE 2 (2026-09-08, 13 brukerflaggede eksempler): den opprinnelige
    logikken antok at gnr/bnr-blokken ALLTID var det aller FØRSTE
    dash-segmentet i tittelen, og at adressen ALLTID lå i et SENERE
    segment. Det stemmer for det vanligste tilfellet ("Gbnr 152/36 - Gate 1
    - ..."), men ikke for:
      - Beskrivelse FØR "Gbnr" i samme segment uten eget skilletegn
        ("Behandlet i sak 20/12949 Gbnr 152/36 - Gneisveien 10 - ...").
      - Ingen "Gbnr"-nøkkelord i det hele tatt, med adressen selv som
        FØRSTE segment ("Haneholmveien 90B - Leilighet - Tilsynssak") -
        ble tidligere alltid hoppet over siden segs[0] antas å alltid være
        gnr/bnr-blokken, uansett innhold.
      - "Gbnr"-uttrykket og adressen limt sammen UTEN skilletegn i samme
        segment ("Gbnr 140/1  Furustadveien 92-94 - ...").
      - En feilplassert bindestrek mellom selve nøkkelordet og tallet
        ("Gbnr - 42/358 - ...") som splittet nøkkelordet fra tallet i to
        separate segmenter og fikk HELE funksjonen til å returnere
        (None, None, None) - segs[0] ble bare "Gbnr" med ingenting igjen
        etter at nøkkelordet ble fjernet.
      - Flere ekte adresser i samme tittel ("Landstadsgate 2 - Florsgate 1
        - Brannstiger") - kun den FØRSTE ble tidligere brukt.
      - Utskrevet "seksjon N"-hale uten egen skråstrek-notasjon for
        seksjonen ("Gbnr 52/65 seksjon 5 - ...").
      - Rene beskrivelses-/årstallskandidater ("Tilsynsstrategi for
        byggesak - 2020", "Voll langs fv. 560", "Oppføring av enebolig på
        tomt 5-2") ble godtatt som adresse siden den gamle valideringen
        kun krevde AT MINST ETT tall i kandidaten, uten krav om
        bokstav-start eller gjenkjennelig gatenavn-form.
    Løst ved å bruke en posisjonsuavhengig _finn_gnrbnr_nokkelord_NEW til å
    finne gnr/bnr HVOR SOM HELST i tittelen (uavhengig av segment-
    rekkefølge), og _beste_adressekandidat (med filtre for årstall,
    paragraf, felt-koder, forkortelser og beskrivende setninger) til å
    validere adressekandidater i ALLE dash-segmenter som ikke er identisk
    med selve gnr/bnr-treffet - inkludert segs[0] når den ikke er
    gnr/bnr-blokken. Flere distinkte, gyldige adresser i samme tittel slås
    sammen med "; ".

    RUNDE 5 (2026-09-12, full-korpus-gjennomgang av bygg_ny etter at
    2001-2016/andebu/stokke-dumpene ble fjernet) fant og fikset:
      - Bokstav-transponerte skrivefeil for "gbnr" ("Gbrn 111/200", "Gnbr
        170/74", "Gbn 80/425") - se _GNR_KEYWORD_ANCHOR_RE/_GNR_BARE_FIRST_RE.
      - Bar ledende "GNR/BNR[/FESTE][,]" helt UTEN nøkkelord ("87/18/39,
        Røysebekkveien 20") - adressen ble allerede funnet riktig via
        komma-splitten i _beste_adressekandidat, men selve gnr/bnr-tallene
        ble aldri hentet ut - se _LEAD_BARE_MATRIKKEL_RE under.
      - "m.fl."/"med flere" limt rett på FORAN adressen i samme
        dash-segment som selve gnr/bnr-uttrykket ("Gbnr 34/122 m.fl.
        Kullerødsvingen 13" ga tidligere "m.fl. Kullerødsvingen 13" som
        adresse) - nøkkelord-strippingen under fjerner nå denne halen også.
      - "byggetrinn N" (byggefase-referanse, som i "Verkstedveien 2, 4 og 6
        - Ny boligbebyggelse - Byggetrinn 2") ble feilaktig lest som en egen
        andre adresse og slått sammen med "; " - se _SETNINGS_ORD.
      - "seks.nr N-N" (seksjons-spenn-forkortelse uten gjenkjent
        "seksjon"/"snr"-form) ble feilaktig godtatt som adresse - se
        _ferdigstill_adresse_ny."""
    tittel = tittel or ""
    if not tittel.strip():
        return None, None, None

    m, gnr_bnr, matrikkelnr = _finn_gnrbnr_nokkelord_NEW(tittel)
    if not m:
        lm = _LEAD_BARE_MATRIKKEL_RE.match(tittel)
        if lm:
            gnr, bnr, feste, seksjon = lm.groups()
            par = f"{gnr}/{bnr}"
            d = (f"{gnr}/{bnr}/{feste or '0'}/{seksjon or '0'}"
                 if (feste and feste != "0") or (seksjon and seksjon != "0") else par)
            gnr_bnr = [par]
            matrikkelnr = f"{KOMMUNE_NR}-{d}"

    kandidater = []
    for seg in _split_tittel(tittel):
        if m and seg == tittel[m.start():m.end()].strip():
            continue
        candidate = re.sub(
            r"(?:g\s*b\s*nr|gnr|grn|bnr|gbrn|gnbr|gbn(?!r))\.?\s*[\d/,\s]+"
            r"(?:m\.?\s*/?\s*fl\.?\s*|med\s+flere\s*)?",
            "", seg, flags=re.IGNORECASE).strip()
        cand_adresse = _beste_adressekandidat(candidate) if candidate else None
        if cand_adresse:
            _dedup_append(kandidater, cand_adresse)

    adresse = "; ".join(kandidater) if kandidater else None
    if not adresse and subtitle and _ADDR_LIKE_RE.match(subtitle.strip()):
        adresse = subtitle.strip()
    return _normaliser_nummer_bokstav(adresse), gnr_bnr, matrikkelnr


# Runde 5: bar ledende "GNR/BNR[/FESTE[/SEKSJON]]" helt UTEN gbnr/gnr-
# nøkkelord ("87/18/39, Røysebekkveien 20 - ...", "80/468 - Solløkkasvingen
# 20 - ..."). Krever komma ELLER " - " (bindestrek med mellomrom FØR OG
# ETTER) RETT ETTER tallene for å skille fra en tilfeldig tallstart et
# annet sted - adresser starter alltid på bokstav, så denne kan ikke
# forveksles med en ekte adressekandidat. Bindestreken må ha mellomrom
# etter seg (ikke bare før) for å unngå å tolke en dato som "12/5-2020"
# (tall-skråstrek-DIREKTE-bindestrek-tall, uten mellomrom) som matrikkel -
# funnet i full-korpus-regresjonen (46 saker i bygg_ny mistet gnr/bnr fordi
# den opprinnelige regexen kun håndterte komma-varianten).
_LEAD_BARE_MATRIKKEL_RE = re.compile(
    r"^\s*(\d+)\s*/\s*(\d+)(?:\s*/\s*(\d+))?(?:\s*/\s*(\d+))?\s*(?:,\s*|-\s+)"
)


# --------------------------------------------------------------------------- #
# gnr/bnr-nøkkelord funnet hvor som helst i tittelen
# --------------------------------------------------------------------------- #
_BNR_GNR_REVERSED_RE = re.compile(r"bnr\.?\s*(\d+)\s*gnr\.?\s*-?\s*(\d+)", re.IGNORECASE)


_AARSTALL_RE = re.compile(r"(?<!\d)(1[89]\d{2}|20\d{2})\s*[A-Za-z]?\s*$")


def _ser_ut_som_aarstall(kandidat):
    """Et 4-sifret tall i 1800/1900/2000-tallet helt til slutt i en
    adressekandidat er nesten alltid et årstall (skjema-/journalår,
    reguleringsplan-år, "samlemappe <år>" osv.), ikke et husnummer - ekte
    Sandefjord-husnummer i dette datasettet går ikke i nærheten av 4 siffer.
    Verifisert 2026-09-07 mot 8 reelle feiltreff av typen "REGISTRERINGS-
    SKJEMA OG KART FOR IKKE SØKNADSPLIKTIGE TILTAK 2015" og "TESTSAK 2015",
    som ellers ble tolket som adresse siden de tilfeldigvis ender på tall."""
    return bool(kandidat and _AARSTALL_RE.search(kandidat.strip()))


_TRAIL_GNR_WORD_RE = re.compile(r"\s*(?:gbnr|bgnr|gnr)\.?\s*$", re.IGNORECASE)
# Løsere enn den delte _ADDR_LIKE_RE: krever fortsatt bokstav-start og et tall
# et sted, men aksepterer DOTALL slik at ekte adresser med linjeskift/rare
# mellomromstegn ikke faller ut.
_ADDR_LOOSE_RE = re.compile(r"^[A-Za-zÆØÅæøå].*\d+\s*[A-Za-zÆØÅæøå]?\s*$", re.DOTALL)
# Egen "OG"-fortsettelse ("GÅRDSVEIEN 3C OG D", "HVALÅSVEIEN 21 OG 11") -
# _ADDR_LOOSE_RE alene godtar ikke disse siden de ikke slutter RETT ETTER
# tallet/bokstaven, men vi vil heller ikke godta VILKÅRLIG tekst etter
# tallet (det ville sluppet gjennom firmanavn som "SPAREBANK1 VESTFOLD" og
# "KVARTAL 19 ARKITEKTKONTOR" som falske adresser).
# Runde 4: tillater også en komma-separert bokstav-liste FØR og-halen
# ("Gokstadveien 4 b,c,d,f og g") - uten dette ble slike lister trunkert
# til bare "Gokstadveien 4b" av komma-splitten i _beste_adressekandidat,
# siden den opprinnelige _ADDR_OG_TAIL_RE ikke matchet HELE strengen og
# dermed ikke hindret komma-splitting. Hvert listeelement etter kommaet
# må fortsatt være ETT ord (ingen fri tekst), så firmanavn med kommaer
# ("X, Y OG Z AS") slipper fortsatt ikke gjennom siden de mangler et tall
# rett før den komma-separerte halen.
_ADDR_OG_TAIL_RE = re.compile(
    r"^[A-Za-zÆØÅæøå].*\d+\s*[A-Za-zÆØÅæøå]?"
    r"(?:\s*,\s*[A-Za-zÆØÅæøå0-9]+)*"
    r"\s+og\s+[A-Za-zÆØÅæøå0-9]+\s*$",
    re.IGNORECASE | re.DOTALL,
)
# Runde 4: bokstav-spenn på husnummeret ("Peer Gynts vei 40A-D", "Schanches
# gate 5A-E") - _ADDR_LOOSE_RE alene godtar ikke disse siden strengen ikke
# slutter RETT ETTER tallet+én bokstav, men vi vil heller ikke godta
# VILKÅRLIG bokstav-bokstav-hale.
_ADDR_LETTER_RANGE_TAIL_RE = re.compile(
    r"^[A-Za-zÆØÅæøå].*\d+[A-Za-zÆØÅæøå]\s*-\s*[A-Za-zÆØÅæøå]\s*$",
    re.DOTALL,
)


# --------------------------------------------------------------------------- #
# Runde 3 (2026-09-07): utvidet _ferdigstill_adresse_ny/_beste_adressekandidat
# etter gjennomgang av 13 brukerflaggede eksempler.
# Nye avvisningsfiltre (alle brukt av _ferdigstill_adresse_ny under):
#   - "§"-tegn (lov-/paragrafhenvisninger, f.eks. "PLAN - OG BYGNINGS LOV
#     § 33 - 1") skal aldri godtas som adresse.
#   - Felt-/byggetrinnkoder ("B4", "B5", "B6", "C6" - bokstav FØR tall) er
#     IKKE husnummer (ekte husnummer er alltid tall FØR evt. bokstav, f.eks.
#     "26A") - se _FELT_KODE_HALE_RE.
#   - Bar "stedsnavn NN/MM"-hale uten noe gjenkjent gate-/stedsnavnsuffiks
#     foran tallparet (f.eks. "HUVIK 113/179") er en matrikkel-referanse i
#     forkledning, ikke en flerpunkts-gateadresse (som DERIMOT har et
#     suffiks, f.eks. "KONGENSGATE 26/28") - se _ser_ut_som_matrikkel_ikke_
#     adresse.
#   - Korte (1-3 bokstaver) sammenklistrede bokstav+tall-"ord" uten
#     mellomrom ("VERA MILJØ A7S") ligner et selskaps-/forkortelsesnavn, til
#     forskjell fra ekte sammenklistrede gatenavn som alltid har en lang,
#     gjenkjennelig stamme (>= 4 bokstaver, f.eks. "ASNESVEIEN11") - se
#     _ser_ut_som_forkortelse.
# --------------------------------------------------------------------------- #
_PARAGRAF_RE = re.compile(r"§")
_FELT_KODE_HALE_RE = re.compile(r"(?:^|[\s/])[A-Za-z]\d+\s*(?:/\s*[A-Za-z]?\d+\s*)*$")
_SISTE_ORD_MED_TALL_RE = re.compile(r"([A-Za-zÆØÅæøå]*)(\d[\wÆØÅæøå]*)\s*$")

# Runde 5: "Seks.nr 1-4" / "Seksjonsnr 2" o.l. er en seksjonsnummer-
# henvisning (til et enkelt bruksenhetsnummer i et seksjonert bygg), ikke
# en adresse - forekommer typisk RETT FØR selve gatenavnet i tittelen
# ("Gbnr 218/126 seks.nr 1-4 - Seljeveien Hus B..."), og blir da plukket
# opp av segment-splittingen som et eget, feilaktig kandidatsegment.
_SEKS_NR_RE = re.compile(r"^seks(?:jon)?\.?\s*nr\.?\s*\d", re.IGNORECASE)


def _ser_ut_som_forkortelse(a):
    """Avviser korte, sammenklistrede bokstav+tall-"ord" uten mellomrom
    ("A7S", "VERA MILJØ A7S") som ligner et selskaps-/forkortelsesnavn -
    ekte sammenklistrede gatenavn ("ASNESVEIEN11") er alltid et langt,
    gjenkjennelig ord (>= 4 bokstaver) foran tallet."""
    siste_ord = a.split()[-1] if a.split() else ""
    m = _SISTE_ORD_MED_TALL_RE.match(siste_ord)
    if not m:
        return False
    bokstaver_foran = m.group(1)
    return 1 <= len(bokstaver_foran) <= 3


_BAR_SKRASTREK_HALE_RE = re.compile(r"([A-Za-zÆØÅæøå]+)\.?\s+\d+\s*/\s*\d+\s*$")


def _ser_ut_som_matrikkel_ikke_adresse(a):
    """Skiller en bar 'stedsnavn NN/MM'-hale (matrikkel-referanse uten
    nøkkelord, f.eks. 'HUVIK 113/179') fra en ekte skråstrek-adresse
    ('KONGENSGATE 26/28') - sistnevnte har et gjenkjent gate-/steds-
    navnsuffiks rett før tallparet, førstnevnte har det ikke."""
    m = _BAR_SKRASTREK_HALE_RE.search(a)
    if not m:
        return False
    ordet = m.group(1)
    return not re.search(rf"(?:{_GATENAVN_SUFFIKS})$", ordet, re.IGNORECASE)


def _ferdigstill_adresse_ny(adresse):
    """Som _ferdigstill_adresse, men fjerner i tillegg et løst hengende
    "gnr"/"gbnr"/"bgnr"-ord på slutten (rester fra segment-splitting) og
    avviser årstall-kandidater (se _ser_ut_som_aarstall), paragraf-
    henvisninger, felt-/byggetrinnkoder, matrikkel-i-forkledning og korte
    selskaps-forkortelser (se runde 3-docstringen over) - brukt av
    parse_adresse_ny (via _beste_adressekandidat/_finn_innbakt_adresse)."""
    if not adresse:
        return None
    a = adresse.strip(" .,-:;").strip()
    a = _TRAIL_GNR_WORD_RE.sub("", a).strip()
    a = _ferdigstill_adresse(a)
    if a and _ser_ut_som_aarstall(a):
        return None
    if a and _PARAGRAF_RE.search(a):
        return None
    if a and _FELT_KODE_HALE_RE.search(a):
        return None
    if a and _ser_ut_som_matrikkel_ikke_adresse(a):
        return None
    if a and _ser_ut_som_forkortelse(a):
        return None
    if a and _SEKS_NR_RE.match(a):
        return None
    return a


# --------------------------------------------------------------------------- #
# Runde 3: gjenkjenner et gyldig norsk gate-/stedsnavn INNI en lengre,
# beskrivende setning ("GARASJE I LINGELEMVEIEN 50", "SØKNAD OM RIVING AV
# NAUST PÅ NEDRE GOGSTADVEI 48") - ikke bare hele/komma-delte segmentet
# (_beste_adressekandidat over). Suffikslisten dekker vei-/gate-typer og
# stedsnavn-endelser observert i datasettet; _ADRESSE_PREFIKS dekker
# valgfrie retnings-/beskrivelsesord som kan stå foran suffiks-ordet
# ("NEDRE GOGSTADVEI", "GAMLE RAVEIEN").
# --------------------------------------------------------------------------- #
_GATENAVN_SUFFIKS = (
    "veien|vegen|veg|vei|gaten|gata|gate|alleen|alle|alleé|bakken|bakke|"
    "svingen|kroken|stubben|ringen|liene|lia|plassen|plass|torget|torg|"
    "bryggen|brygga|hagen|hage|tunet|tun|øya|holmen|jordet|marka|åsen|"
    "høyden|kollen|dalen|skogen|moen|myra|stranda|strand|odden|eiket|eika|"
    "hagan|feltet|grenda|stien|tjernet|vollen|lunden|berget|kleiva|løkka|"
    "letta|stigen|stiger|kauen|dåsen"
)
_ADRESSE_PREFIKS = "nedre|øvre|vestre|østre|søndre|nordre|gamle|lille|store|indre|ytre"
_INNBAKT_ADRESSE_RE = re.compile(
    rf"("
    # Runde 4: retningsord + BART suffiks-ord ("SØNDRE STIGER 71") - gate-
    # navnet er her selve suffikset alene, uten noe stammeord foran. Kan
    # ikke fanges av alternativet under, som krever minst én stavelse FØR
    # selve suffikset (det bare suffiks-ordet er nøyaktig like langt som
    # mønsteret det skal matche mot, så det er ikke plass til en egen
    # ledende bokstav i tillegg).
    rf"(?:{_ADRESSE_PREFIKS})\s+(?:{_GATENAVN_SUFFIKS})"
    rf"|"
    # Vanlig: valgfritt retningsord + sammensatt gatenavn som ENDER på
    # suffikset ("NEDRE GOGSTADVEI", "BOGENSGATE").
    rf"(?:(?:{_ADRESSE_PREFIKS})\s+)?"
    rf"[A-Za-zÆØÅæøå][\wæøåÆØÅ.]*"
    rf"(?:{_GATENAVN_SUFFIKS})"
    rf")\.?\s+"
    rf"(\d+\s*(?:[A-Za-zÆØÅæøå](?![A-Za-zÆØÅæøå]))?)",
    re.IGNORECASE,
)


def _innbakt_treff(tekst):
    """finditer over _INNBAKT_ADRESSE_RE, men forkaster (ikke bare
    trunkerer!) et treff som i virkeligheten er begynnelsen på et
    skråstrek-tallpar ("HUVIK 113/179") - matrikkel, ikke adresse. Gjøres
    som et etterhånds-python-sjekk i stedet for en negativ lookahead inni
    selve tall-gruppen, for å unngå at regex-motoren "spiser seg bakover"
    i sifrene for å tilfredsstille lookahead-et (samme fallgruve som i
    _prov_bar_kontinuasjon)."""
    for m in _INNBAKT_ADRESSE_RE.finditer(tekst):
        rest = tekst[m.end():]
        if re.match(r"\s*/\s*\d", rest):
            continue
        yield m


def _finn_innbakt_adresse(seg):
    """Siste fallback: finner et gatenavn+husnummer INNI en lengre,
    beskrivende setning (ikke bare hele/komma-delte segmentet, se
    _beste_adressekandidat under) - gatenavnet må ende på et kjent norsk
    gate-/stedsnavn-suffiks for å unngå å plukke opp firmanavn eller
    saksbeskrivelser med tilfeldige tall. Tar SISTE treff i segmentet
    (adressen står oftest til slutt, etter en preposisjon som "i"/"på")."""
    siste = None
    for m in _innbakt_treff(seg):
        siste = m
    if not siste:
        return None
    kandidat = f"{siste.group(1)} {siste.group(2)}"
    return _ferdigstill_adresse_ny(kandidat)


_TRAIL_ANNOTASJON_RE = re.compile(r'\s*(?:"[^"]*"|«[^»]*»|\([^)]*\))\s*$')


def _fjern_hale_annotasjon(sp_clean):
    """Fjerner et evt. anførselstegn-/parentes-tillegg ("kallenavn" e.l.) helt
    til slutt, f.eks. 'KNATTHOLMEN 26 "TOLLERHYTTA"' -> 'KNATTHOLMEN 26'.

    RUNDE 4: fjerner i tillegg en hengende "m.fl."/"m/fl"-hale på selve
    ADRESSEN ("Engeveien 40 m.fl.", "Langes gate 6 m.fl." - "og flere andre
    berørte eiendommer", samme kvalifiserende hale som allerede ble strippet
    fra gnr/bnr-uttrykk av _MFL_SUFFIX_RE) - ellers blir hele kandidaten
    forkastet siden "m.fl." ikke ser adresse-aktig ut. Hopper over dette
    steget når strengen inneholder en skråstrek - en ekte adresse har
    aldri en bar skråstrek, mens en garblet gnr/bnr-rest kan gjøre det
    (f.eks. "GNR 155 M.FL/12 M.FL" -> uten denne sperren ville "M.FL/12"
    blitt stående igjen og feilaktig sett adresse-aktig ut)."""
    while True:
        ny = _TRAIL_ANNOTASJON_RE.sub("", sp_clean)
        if "/" not in ny:
            ny = _MFL_SUFFIX_RE.sub("", ny).strip()
        if ny == sp_clean:
            return sp_clean
        sp_clean = ny


def _prov_innbakt_med_og_hale(sp_clean):
    """Kjører _finn_innbakt_adresse-mønsteret på sp_clean og prøver å
    utvide treffet med en påfølgende " OG <token>"-hale helt til slutt i
    strengen (samme idé som _ADDR_OG_TAIL_RE, men for et treff inni en
    lengre setning, f.eks. "... GÅRDSVEIEN 3C OG D")."""
    siste = None
    for cand in _innbakt_treff(sp_clean):
        siste = cand
    if not siste:
        return None
    hale = sp_clean[siste.end():]
    if re.match(r"\s*og\s+[A-Za-zÆØÅæøå0-9]+\s*$", hale, re.IGNORECASE):
        # behold opprinnelig kasus/mellomrom på "og"-halen ved å hente den
        # rå teksten fra kilden i stedet for å bygge den opp selv
        kandidat = f"{siste.group(1)} {sp_clean[siste.start(2):].strip()}"
    else:
        kandidat = f"{siste.group(1)} {siste.group(2).strip()}"
    return _ferdigstill_adresse_ny(kandidat)


_SETNINGS_ORD = {
    "i", "på", "for", "om", "til", "ved", "fra", "med", "av", "hos", "under", "over",
    "garasje", "tilbygg", "oppføring", "søknad", "søknadsplikt", "bruksendring",
    "riving", "enebolig", "gjerde", "utbedring", "skilt",
    "skilting", "uteareal", "vindu", "plassering", "forespørsel",
    "endringer", "hus", "uthus", "utslippstillatelse", "bruksstillatelse",
    "brukstillatelse", "svømmebasseng", "tomannsbolig", "fritidsbolig", "naust",
    "støttemur", "takforlengelse", "inngangsparti", "veranda", "carport", "bod",
    "anlegg", "parkeringsplass", "kunstgressbaner", "tømmerlunde", "vann",
    "bevaringsverdig", "farlig", "utleieleilighet", "tiltakshavere",
    "bekreftelse", "opprydding", "istandsetting", "sikring", "eiendom",
    "eiendommen", "ferdigattest", "nybygg", "påbygg",
    "ombygging", "fasadeendring", "dispensasjon", "avkjørsel", "usikret",
    # Runde 4 (parse_adresse_ny): vei-/rute-referanser ("Voll langs fv. 560")
    # er beskrivelser av et anlegg LANGS en offentlig vei, ikke en gateadresse.
    "langs", "fv", "rv", "ev", "kv",
    # Runde 5 (parse_adresse_ny): "byggetrinn N" er en byggefase-referanse
    # ("Verkstedveien 2, 4 og 6 - Ny boligbebyggelse - Byggetrinn 2"), ikke
    # en egen andre adresse - samme mønster som allerede fikset for
    # Trondheim (se HANDOVER.md).
    "byggetrinn",
    # Runde 5 (full-korpus-regresjon, bygg_ny): en rekke bygnings-/enhets-
    # /saks-referanse-ord etterfulgt av MELLOMROM + tall ("Bygg 3", "Tomt
    # 2", "Felt B9", "BFS1 09-10", "Seksjon 2", "Seksjonsnr 24", "Snr 1-5",
    # "JNR 2019.359", "Bolig 4", "Leilighet 313", "Rekke 1",
    # "Firemannsbolig 4") ble tidligere godtatt som en EGEN, andre adresse
    # og slått sammen med "; " til den ekte adressen - disse fanges IKKE av
    # _ser_ut_som_forkortelse (som kun ser på siste ord via .split(), og
    # dermed aldri ser selve ordet foran når det står med mellomrom foran
    # tallet, i motsetning til sammenklistrede felt-koder som "B9"/"BA2").
    "bygg", "tomt", "felt", "delfelt", "bfs", "seksjon", "seksjonsnr", "snr",
    "jnr", "bolig", "leilighet", "rekke", "firemannsbolig",
}


def _har_setningsord(tekst):
    """True hvis teksten FØR et gjenkjent adressetreff inneholder et ord som
    tyder på at dette er en beskrivende SETNING (søknadstype, preposisjon)
    og ikke bare et sammensatt/personnavn-basert gatenavn ("Peder Bogens
    gate", "Thor Dahls gate") - i så fall skal vi IKKE godta hele
    kandidaten, men bruke den målrettede uttrekkeren i stedet.

    RUNDE 4: ignorerer ett-bokstavs "ord" - en bokstav som "I" er nesten
    alltid en bygnings-/enhets-bokstav i en adresse ("Nygaards alle 4 I"),
    ikke preposisjonen "i" (den eneste ett-bokstavs oppføringen i
    _SETNINGS_ORD)."""
    ord_liste = re.findall(r"[A-Za-zÆØÅæøå]+", tekst.lower())
    return any(len(o) > 1 and o in _SETNINGS_ORD for o in ord_liste)


def _beste_adressekandidat(seg):
    """Prøver hvert komma-underledd i `seg` (eller `seg` selv om det ikke
    har komma) mot _ADDR_LOOSE_RE/_ADDR_OG_TAIL_RE og returnerer den første
    som ser adresse-aktig ut, ferdigstilt. Brukes overalt i parse_adresse_
    gammel i stedet for et rått _ADDR_LIKE_RE-sjekk på hele segmentet,
    siden segmenter ofte inneholder "BESKRIVELSE, ADRESSE".

    RUNDE 3: fjerner først en evt. hale-annotasjon ("kallenavn"/parentes)
    før adresse-sjekken, godtar HELE kandidaten (bevarer spenn/lister som
    "20-28", "5,7,9 OG 11", og sammensatte personnavn-gatenavn som "PEDER
    BOGENSGATE 4") MED MINDRE teksten foran et gjenkjent gatenavn-treff
    inneholder et ord som avslører at dette egentlig er en beskrivende
    setning (se _har_setningsord) - da brukes i stedet den målrettede
    uttrekkeren _prov_innbakt_med_og_hale/_finn_innbakt_adresse.

    RUNDE 4 (parse_adresse_ny, 2026-09-08): fant et hull i runde 3-logikken
    - når INGEN gjenkjent gatenavn-suffiks finnes noe sted i kandidaten
    (ingen "siste_treff"), ble hele kandidaten likevel godtatt blindt så
    lenge den bare startet på bokstav og sluttet på tall - dette slapp
    beskrivende søknadssetninger som tilfeldigvis ender på et tall gjennom
    ("Oppføring av enebolig på tomt 5-2", "Voll langs fv. 560"). Nå kreves
    i tillegg at HELE kandidaten (ikke bare en "foran"-del, siden det ikke
    finnes noe treff å måle fra) er fri for setningsord i dette tilfellet -
    korte stedsnavn uten kjent suffiks ("BUGÅRDSTOPPEN 6") har ingen
    setningsord og godtas fortsatt.

    RUNDE 4: komma-splitting under er laget for "BESKRIVELSE, ADRESSE"
    (komma som skille mellom to ULIKE ting). Men et komma etterfulgt av
    mellomrom kan OGSÅ være en flertallsliste for SAMME gate ("Skaustranda
    58, 60 og 62") - splitter vi denne på komma først, blir bare "58"
    (første husnummer) stående, og resten av listen mistes. Skiller de to
    fra hverandre ved å sjekke HELE (annotasjon-strippede) segmentet mot
    _ADDR_OG_TAIL_RE FØR komma-splitting: strengen ender i så fall i en
    "OG <siste-nummer>"-hale, noe en ren "beskrivelse, adresse"-tittel
    aldri gjør."""
    sp_helhet = _fjern_hale_annotasjon(seg.strip(" .,-:;")).strip(" .,-:;")
    if _ADDR_OG_TAIL_RE.match(sp_helhet):
        subparts = [seg]
    else:
        subparts = [p.strip() for p in re.split(r",\s+", seg) if p.strip()]
        if len(subparts) <= 1:
            subparts = [seg]
    for sp in subparts:
        sp_clean = sp.strip(" .,-:;")
        if not (_ADDR_LOOSE_RE.match(sp_clean) or _ADDR_OG_TAIL_RE.match(sp_clean)
                or _ADDR_LETTER_RANGE_TAIL_RE.match(sp_clean)):
            sp_uten_annotasjon = _fjern_hale_annotasjon(sp_clean).strip(" .,-:;")
            if sp_uten_annotasjon != sp_clean:
                sp_clean = sp_uten_annotasjon
        if (_ADDR_LOOSE_RE.match(sp_clean) or _ADDR_OG_TAIL_RE.match(sp_clean)
                or _ADDR_LETTER_RANGE_TAIL_RE.match(sp_clean)):
            siste_treff = None
            for siste_treff in _innbakt_treff(sp_clean):
                pass
            if siste_treff is not None:
                foran = sp_clean[:siste_treff.start()]
                godta = not _har_setningsord(foran)
            else:
                godta = not _har_setningsord(sp_clean)
            if godta:
                a = _ferdigstill_adresse_ny(sp_clean)
                if a:
                    return a
        a = _prov_innbakt_med_og_hale(sp_clean)
        if a:
            return a
    return _finn_innbakt_adresse(seg)


# --------------------------------------------------------------------------- #
# _finn_gnrbnr_nokkelord_NEW: posisjonsuavhengig gnr/bnr-søk brukt av
# parse_adresse_ny. Finner "gnr"-ankeret hvor som helst i tittelen (i stedet
# for å anta at "Gbnr"-uttrykket alltid er aller først - noe som ellers
# bryter på beskrivelse FØR "Gbnr", "Gbnr" limt sammen med adressen i samme
# segment, og en feilplassert bindestrek mellom nøkkelordet og tallet).
# Gjenbruker den allerede korrekte _parse_gnrbnr_expr på uttrykket som følger
# et "gnr"-anker, og løkker deretter videre i resten av tittelen for å finne
# og slå sammen EVENTUELLE flere, uavhengige "gnr"-nevnelser (dash-kjedet
# "GNR 115/104 - 115/106", dobbelt nøkkelord "GNR GNR 132/143", eller
# sekundære referanser markert "tidligere"/"nytt"/"utgått" - vi tolker IKKE
# disse kvalifiseringsordene semantisk, men beholder alle funn). Anker
# støtter også "grn" (bokstav-bytte-skrivefeil for "gnr") og det
# sammenslåtte "gnr/bnr"-nøkkelordparet (norsk konvensjon der skråstreken
# står MELLOM selve nøkkelord-forkortelsene, ikke mellom tallene - "GNR/BNR
# 137/1").
# --------------------------------------------------------------------------- #
# Runde 5 (2026-09-12): anker utvidet med tre nye bokstav-transponerte
# skrivefeil for "gbnr" observert i bygg_ny-korpuset - "gbrn" (b/r byttet),
# "gnbr" (n/b byttet) og "gbn" (manglende "r") - i tillegg til de tidligere
# håndterte "bgnr"/"grn". "gbn" trenger en negativ lookahead (?!r) siden det
# ellers ville matchet som et FEILAKTIG prefiks av ekte "gbnr" og kuttet av
# siste bokstaven for enhver vanlig sak.
_GNR_KEYWORD_ANCHOR_RE = re.compile(
    r"(?:(?:gbnr|bgnr|gbrn|gnbr|grn|gnr)\s*/\s*bnr\.?\s*-?\s*"
    r"|(?:gbnr|bgnr|gbrn|gnbr|grn|gnr|gbn(?!r))\.?\s*-?\s*)+",
    re.IGNORECASE,
)
_SLASH_FIRST_RE = re.compile(r"\d+\s*/\s*\d+")
_GNRBNR_EXPR_PREFIX_RE = re.compile(
    r"\d+\s*/\s*\d+(?:\s*/\s*\d+(?:\s*/\s*\d+)?)?"
    r"(?:\s*(?:,|og)\s*(?:\d+\s*/\s*\d+(?:\s*/\s*\d+(?:\s*/\s*\d+)?)?|\d+))*"
    r"(?:\s*m\.?\s*/?\s*fl\.?)?",
    re.IGNORECASE,
)
_DASH_CHAIN_RE = re.compile(r"^\s*-\s*(?=\d+\s*/\s*\d+)")
_GNR_BARE_FIRST_RE = re.compile(
    r"(?:gbnr|bgnr|gbrn|gnbr|grn|gnr|gbn(?!r))\.?\s*(\d+)", re.IGNORECASE)
_CONT_RE = re.compile(
    r"\s*,?\s*(?:bnr\.?\s*)*(\d+(?:\s*(?:,|og)\s*(?:bnr\.?\s*)*\d+)*)",
    re.IGNORECASE,
)
# Runde 4: utskrevet "seksjon N"/"snr N"-hale rett etter et gnr/bnr-par uten
# egen skråstrek-notasjon for seksjonen ("Gbnr 52/65 seksjon 5" - i stedet
# for "52/65/0/5").
_SEKSJON_HALE_RE = re.compile(r"\s*(?:seksjon|snr)\.?\s*(\d+)", re.IGNORECASE)


def _uten_kommune_forveksling(gnr_bnr, matrikkel):
    """Filtrerer bort par der gnr-delen er lik KOMMUNE_NR - et gnr i denne
    størrelsesordenen finnes ikke i Sandefjord, og er nesten alltid en
    forveksling med en annen notasjon (f.eks. "GBNR. 3907/484-1", der 3907
    er kommunenummeret og "484-1" en gnr-bnr-lignende streng med bindestrek
    i stedet for skråstrek - se parse_adresse_ny-docstringen). Returnerer
    (gnr_bnr, matrikkel) med slike par fjernet."""
    kommune_str = str(KOMMUNE_NR)
    filtrert_gnr = [p for p in gnr_bnr if p.split("/", 1)[0] != kommune_str]
    filtrert_mtr = [d for d in matrikkel if d.split("/", 1)[0] != kommune_str]
    return filtrert_gnr, filtrert_mtr


def _prov_slash_expr(tittel, anchor_start, anchor_end):
    """Prøver 'gnr'-anker + skråstrek-par (evt. dash-kjedet videre, f.eks.
    "GNR 115/104 - 115/106"). Returnerer (slutt-posisjon, gnr_bnr, matrikkel)
    eller None.

    RUNDE 4: prøver i tillegg en utskrevet "seksjon N"/"snr N"-hale rett
    etter paret (kun når akkurat ETT par ble funnet - for tvetydig ellers),
    og forkaster par der gnr-delen er lik KOMMUNE_NR (se
    _uten_kommune_forveksling)."""
    rest = tittel[anchor_end:]
    if not _SLASH_FIRST_RE.match(rest):
        return None
    expr_m = _GNRBNR_EXPR_PREFIX_RE.match(rest)
    if not expr_m:
        return None
    expr_text = re.sub(r"\s*/\s*", "/", expr_m.group(0))
    gnr_bnr, matrikkel = _parse_gnrbnr_expr(expr_text)
    gnr_bnr, matrikkel = _uten_kommune_forveksling(gnr_bnr, matrikkel)
    if not gnr_bnr:
        return None
    end = anchor_end + expr_m.end()
    if len(gnr_bnr) == 1:
        sm = _SEKSJON_HALE_RE.match(tittel[end:])
        if sm:
            gnr, bnr = gnr_bnr[0].split("/")
            matrikkel = [f"{gnr}/{bnr}/0/{sm.group(1)}"]
            end = end + sm.end()
    while True:
        dm = _DASH_CHAIN_RE.match(tittel[end:])
        if not dm:
            break
        rest2 = tittel[end + dm.end():]
        em2 = _GNRBNR_EXPR_PREFIX_RE.match(rest2)
        if not em2:
            break
        g2, m2 = _parse_gnrbnr_expr(re.sub(r"\s*/\s*", "/", em2.group(0)))
        g2, m2 = _uten_kommune_forveksling(g2, m2)
        if not g2:
            break
        for p in g2:
            _dedup_append(gnr_bnr, p)
        for d in m2:
            _dedup_append(matrikkel, d)
        end = end + dm.end() + em2.end()
    return end, gnr_bnr, matrikkel


def _prov_bar_kontinuasjon(tittel, search_from=0):
    """Prøver 'gnr <N>[, [bnr] <M>[, ...]]' UTEN skråstrek på selve
    gnr-tallet (f.eks. "gnr 114, 146 og 147", "gnr 166, bnr 232").
    Returnerer (bm_match, slutt-posisjon, gnr_bnr, matrikkel) eller None.
    Egen to-stegs oppslag (i stedet for én kombinert regex) for å unngå at
    regex-motoren "spiser seg bakover" i selve gnr-tallet når resten ikke
    matcher - observert med "GNR 137/ BNR 1,3 OG 7" der en monolittisk regex
    lot (\\d+) backtracke fra "137" til "13" og fabrikkerte en falsk "13/7"
    fra restsifferet i "137".

    RUNDE 3: samme "GNR 137/ BNR 1,3 OG 7"-tittel avdekket en NY variant av
    samme problem - skråstreken der har INGEN tall rett etter seg (bare et
    mellomrom før "BNR"), så den er ikke et ekte skråstrek-par og skal ikke
    få hele funnet til å bli forkastet. Vi sjekker derfor eksplisitt om
    skråstreken er RETT FULGT av et siffer (ekte par, håndteres av
    _prov_slash_expr) og fjerner ellers den spuriøse skråstreken før vi
    fortsetter til den bare fortsettelsen."""
    bm = _GNR_BARE_FIRST_RE.search(tittel, search_from)
    if not bm:
        return None
    after = tittel[bm.end():]
    if re.match(r"\s*/\s*\d", after):
        return None  # ekte skråstrek-par - håndteres av _prov_slash_expr
    after = re.sub(r"^\s*/\s*", "", after)  # spuriøs skråstrek uten tall rett etter
    cont_m = _CONT_RE.match(after)
    if not cont_m:
        return None
    gnr0 = bm.group(1)
    if gnr0 == str(KOMMUNE_NR):
        return None  # se _uten_kommune_forveksling - nesten aldri et ekte gnr
    cont_clean = re.sub(r"bnr\.?\s*", "", cont_m.group(1), flags=re.IGNORECASE)
    bnrs = [b.strip() for b in _SUBSPLIT_RE.split(cont_clean) if b.strip()]
    gnr_bnr, matrikkel = [], []
    for bnr in bnrs:
        if not _BARE_NUM_RE.match(bnr):
            continue
        par = f"{gnr0}/{bnr}"
        _dedup_append(gnr_bnr, par)
        _dedup_append(matrikkel, par)
    if not gnr_bnr:
        return None
    return bm, bm.end() + cont_m.end(), gnr_bnr, matrikkel


class _Span:
    """Minimal match-lignende objekt (kun .start()/.end()) for å kunne
    returnere et KOMBINERT span (primær + evt. sammenslåtte sekundære
    gnr-referanser) fra _finn_gnrbnr_nokkelord_NEW, brukt av parse_adresse_ny
    til å ekskludere riktig segment fra adresse-søket."""

    def __init__(self, start, end):
        self._start, self._end = start, end

    def start(self):
        return self._start

    def end(self):
        return self._end


def _finn_gnrbnr_nokkelord_NEW(tittel):
    """Finner gnr/bnr posisjonsuavhengig hvor som helst i tittelen (brukt av
    parse_adresse_ny) - se modul-seksjonens docstring over for bakgrunn.
    Finner primær gnr/bnr via skråstrek-uttrykk eller bar fortsettelse, slår
    så videre sammen EVENTUELLE flere uavhengige "gnr"-nevnelser senere i
    tittelen."""
    tittel = tittel or ""
    anchor = _GNR_KEYWORD_ANCHOR_RE.search(tittel)
    if anchor:
        gnr_bnr, matrikkel = [], []
        span_start = anchor.start()
        span_end = anchor.end()

        slash_res = _prov_slash_expr(tittel, anchor.start(), anchor.end())
        if slash_res:
            span_end, g, m_ = slash_res
            gnr_bnr, matrikkel = g, m_
        else:
            bar_res = _prov_bar_kontinuasjon(tittel, anchor.start())
            if bar_res:
                bm, span_end, g, m_ = bar_res
                span_start = min(span_start, bm.start())
                gnr_bnr, matrikkel = g, m_

        if gnr_bnr:
            search_from = span_end
            while True:
                nxt = _GNR_KEYWORD_ANCHOR_RE.search(tittel, search_from)
                if not nxt:
                    break
                extra = _prov_slash_expr(tittel, nxt.start(), nxt.end())
                if extra:
                    e_end, g2, m2 = extra
                    for p in g2:
                        _dedup_append(gnr_bnr, p)
                    for d in m2:
                        _dedup_append(matrikkel, d)
                    span_end = max(span_end, e_end)
                    search_from = e_end
                    continue
                extra2 = _prov_bar_kontinuasjon(tittel, nxt.start())
                if extra2:
                    _, e_end, g2, m2 = extra2
                    for p in g2:
                        _dedup_append(gnr_bnr, p)
                    for d in m2:
                        _dedup_append(matrikkel, d)
                    span_end = max(span_end, e_end)
                    search_from = e_end
                    continue
                break

            matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel)
            return _Span(span_start, span_end), gnr_bnr, matrikkelnr

    m2 = _BNR_GNR_REVERSED_RE.search(tittel)
    if m2:
        bnr, gnr = m2.groups()
        par = f"{gnr}/{bnr}"
        return m2, [par], f"{KOMMUNE_NR}-{par}"
    return None, None, None


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg_ny": dict(
        sakstype_navn="Byggesak",
        datasource="9bb75e38-95d6-447c-9079-2b842a40a46a",
        sakstype="at-9bb75e38__95d6__447c__9079__2b842a40a46a-BS!uwUEuz",
        adresse_parser=parse_adresse_ny,
        hent_filer=True,
        periode="2017-nå",
    ),
    "tilsyn_ny": dict(
        sakstype_navn="Byggetilsynssak",
        datasource="9bb75e38-95d6-447c-9079-2b842a40a46a",
        sakstype="at-9bb75e38__95d6__447c__9079__2b842a40a46a-BST!OvQ1mA",
        adresse_parser=parse_adresse_ny,
        hent_filer=True,
        periode="2017-nå",
    ),
}

# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(method_fn, path, retries=RETRIES, **kwargs):
    """Delt retry/backoff-logikk for _post/_get."""
    url = f"{API_BASE}/{path}"
    last_err = None
    for attempt in range(retries):
        try:
            r = method_fn(url, headers=HEADERS, timeout=TIMEOUT, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{method_fn.__name__.upper()} {path} feilet etter {retries} forsøk") from last_err


def _post(session, path, body, retries=RETRIES):
    return _request(session.post, path, retries, json=body)


def _get(session, path, retries=RETRIES):
    return _request(session.get, path, retries)


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
    """Henter fillommer for ETT dokument. Kun relevant for kilder med
    hent_filer=True (kun bygg_ny/tilsyn_ny - se modul-docstring)."""
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
        "url": f"{BASE_URL}/engasjer-deg/innsyn-og-apenhet/sok-etter-saker-og-dokumenter/#/details/{identifier}",
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


def _atomic_write_json(data, path):
    """Skriver JSON til en tmp-fil og bytter den inn atomisk - unngår
    korrupt fil ved avbrutt kjøring midt i skriving."""
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
# (KUN for bygg_ny/tilsyn_ny, de tre historiske arkivene er lukket og får
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
    _atomic_write_json(state, state_file)


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

        seen_saker[cid] = ref_iso
        for jp in r["dokumenter"]:
            if jp.get("journalpost_id"):
                seen_jp[jp["journalpost_id"]] = ref_iso

    feilet = sum(1 for r in results if "error" in r)
    write_changelog(nye_saker, nye_journalposter, ref_iso, output_dir, kilde["sakstype_navn"])
    prune_seen(state, ref_date)
    save_state(state, state_file)
    print(f"Ferdig ({feilet}/{len(kandidat_ids)} feilet under henting).")
