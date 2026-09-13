"""Kristiansand - OpenGov/360online (samme plattform som Lillestrøm/Sandnes/
Sarpsborg). Alle fire sakstyper (Byggesak/Henvendelse/Ulovlighetssak/
Tilsynssak) skrapes fra samme portal (KRSANDEBYGG) med identisk logikk -
eneste reelle forskjell er saksnummer-prefikset ("BYGG-"/"HENV-"/"ULOV-"/
"TILSYN-") brukt i søket, gitt som parameter til hver funksjon.
"""

import gzip
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Konstanter felles for alle sakstyper i Kristiansand
# --------------------------------------------------------------------------- #
BASE_URL = "https://opengov.360online.com"
SEARCH_URL = f"{BASE_URL}/Cases/KRSANDEBYGG"
CASE_BASE_URL = f"{BASE_URL}/Cases/KRSANDEBYGG/Case/Details/"

KOMMUNE_NR = 4204
KOMMUNE = "Kristiansand"

# Azure Blob - samme lagringskonto/container og samme mønster (Service
# Principal via env/.env, JSONL+gzip) som easy-pipelines-repoet sin
# Trondheim-pipeline, se _azure_credential()/upload_til_azure() nedenfor.
AZURE_ACCOUNT_URL = "https://storaggen2eaccountprod.blob.core.windows.net"
AZURE_CONTAINER_NAME = "postlister"
AZURE_BASE_PATH = "bronze/kristiansand"

MAX_WORKERS = 8       # antall parallelle forespørsler
TIMEOUT = 30
RETRIES = 3

GNR_BNR_RE = re.compile(r"\b(\d+/\d+)")   # gårds- og bruksnummer i en adresse
ADDR_INDEX_RE = re.compile(r"^\d+\.\s*")  # løpenummer foran en adresselinje


# --------------------------------------------------------------------------- #
# Opprydding av adresse/gnr_bnr + matrikkelnr
# --------------------------------------------------------------------------- #
# Kristiansands rå adressefelt er en liste med linjer som "Gatenavn 12, 4610
# KRISTIANSAND S, Norge" - ofte med duplikater (samme adresse gjentatt med
# og uten mellomrom i bokstav-suffikset, "16 B" vs "16B"), og noen ganger
# med FLERE husnumre kommaseparert på én linje. Funksjonene under rydder
# dette opp til en flat, deduplisert adresseliste, slik at "adresse" og
# "gnr_bnr" kan lagres som semikolon-separerte strenger og et
# "matrikkelnr"-felt kan avledes.
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
_POSTNR_STED = re.compile(r"^\d{4}(\s|$)")

# Ett "husnummer-token": et tall med evt. bokstav-suffiks, evt. som et
# bindestrek-spenn til et nytt tall/bokstav ("18", "9A", "30A-E", "24-34").
_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN_FULL = re.compile(rf"^{_TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_STREET_AND_NUMBER_RANGE = re.compile(rf"^(.+?)\s+({_TOKEN})$")
_PART_SPLIT = re.compile(r"\s*(,|\bog\b|&)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")


def _normalize_nummer_bokstav(s):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks (f.eks. "16 B" -> "16B")."""
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _strip_sted_suffix(raw):
    """Fjerner ', <postnr> <sted>, Norge' / ', Norge' fra enden av en rå
    adresselinje, og beholder resten (gatenavn + evt. kommaliste av flere
    husnumre) urørt."""
    parts = [p.strip() for p in raw.split(",")]
    if parts and parts[-1].lower() == "norge":
        parts = parts[:-1]
    if parts and (_POSTNR_STED.match(parts[-1]) or parts[-1] in ("", "0")):
        parts = parts[:-1]
    return ", ".join(p for p in parts if p)


def _parse_address_parts(s):
    """Deler en adressekandidat opp i enkeltadresser atskilt med komma
    og/eller ' og ' (samme logikk som brukes for å tolke Trondheims
    sakstitler). Et bart tall/spenn arver gatenavnet fra forrige element
    ('3' etter 'Lagmannsholmen 1' -> 'Lagmannsholmen 3'), en bar bokstav
    arver gatenavn+tall, og et nytt 'Gatenavn nummer' starter en ny gate -
    for FØRSTE element stoler vi alltid på gatenavnet (det kommer fra
    kildens egen adresseliste), for senere elementer (etter komma/og)
    kreves stor forbokstav for å skille en ny gate fra beskrivelsestekst
    med et tall på slutten. Bindestrek-spenn ('24-34') beholdes samlet som
    ett element - vi gjetter ikke husnumrene mellom endepunktene."""
    entries = []
    current_street = None
    last_number = None
    tokens = _PART_SPLIT.split(s)
    part0, seps_and_parts = tokens[0], tokens[1:]
    parts = [(None, part0)] + list(zip(seps_and_parts[0::2], seps_and_parts[1::2]))
    for sep, part in parts:
        part = part.strip()
        if not part:
            continue
        if _BARE_LETTER_TOKEN.match(part):
            if current_street is None or last_number is None:
                break
            entries.append(f"{current_street} {last_number}{part}")
            continue
        if _BARE_TOKEN_FULL.match(part):
            if current_street is None:
                break
            norm = re.sub(r"\s*-\s*", "-", part)
            entries.append(f"{current_street} {norm}")
            mnum = re.match(r"^(\d+)", norm)
            if mnum:
                last_number = mnum.group(1)
            continue
        sm = _STREET_AND_NUMBER_RANGE.match(part)
        if sm and (sep is None or _STARTS_UPPER.match(sm.group(1).strip())):
            gatenavn = sm.group(1).strip()
            if _FELTKODE_I_GATENAVN.search(gatenavn):
                break
            current_street = gatenavn
            norm = re.sub(r"\s*-\s*", "-", sm.group(2))
            entries.append(f"{current_street} {norm}")
            mnum = re.match(r"^(\d+)", norm)
            if mnum:
                last_number = mnum.group(1)
            continue
        break
    seen = set()
    uniq = []
    for e in entries:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq


def _gatenavn(adresse):
    """Gatenavnet (uten husnummer) fra en ferdig adressestreng, f.eks.
    "Vardåsveien 96" -> "Vardåsveien" - brukt til å avgjøre om en tittel-
    gjettet adresse gjelder SAMME gate som panelet allerede har bekreftet
    (se parse_case)."""
    m = _STREET_AND_NUMBER_RANGE.match(adresse)
    return m.group(1).strip() if m else None


def clean_adresser(raw_list):
    """Rydder den rå adresselista - fjerner postnr/sted/Norge-støy, sprenger
    kommalister av flere husnumre per gate, og dedupliserer mellomroms-
    varianter ("16 B" vs "16B") - til én flat, deduplisert liste."""
    entries = []
    for raw in raw_list or []:
        gate_del = _strip_sted_suffix(raw)
        if not gate_del or _INGEN_ADRESSE.match(gate_del.strip()):
            continue
        normalized = _normalize_nummer_bokstav(gate_del)
        for addr in _parse_address_parts(normalized):
            if addr not in entries:
                entries.append(addr)
    return entries


def clean_gnr_bnr(raw_list):
    """Dedupliserer gnr/bnr-lista (de dubleres ofte i kilden), behold rekkefølge."""
    seen = set()
    uniq = []
    for g in raw_list or []:
        if g and g not in seen:
            seen.add(g)
            uniq.append(g)
    return uniq


_MATRIKKEL_TITLE_RE = re.compile(r"(\d+)/(\d+)(?:/(\d+)(?:/(\d+))?)?")


def parse_title_matrikkel(tittel):
    """Finn gnr/bnr -> (festenr, seksjonsnr) i sakstittelen. Adresse-panelet
    oppgir aldri feste/seksjon (kun gnr/bnr) - tittelen ("Gatenavn Nummer
    GNR/BNR/FESTENR/SEKSJONSNR, beskrivelse") er eneste kilde til den
    presisjonen. "0/0" er kildesystemets placeholder for "ingen
    feste/seksjon" og hoppes over - kun reelle verdier (~4% av saker) tas
    med."""
    result = {}
    if not tittel:
        return result
    for gnr, bnr, feste, seksjon in _MATRIKKEL_TITLE_RE.findall(tittel):
        if not feste and not seksjon:
            continue
        feste = feste or "0"
        seksjon = seksjon or "0"
        if feste == "0" and seksjon == "0":
            continue
        result[(gnr, bnr)] = (feste, seksjon)
    return result


def build_matrikkelnr(gnr_bnr_list, kommune_nr, tittel=None):
    """Bygger matrikkelnr fra deduplisert gnr_bnr-liste, f.eks. "4204-150/732" -
    berikes med feste/seksjon parset fra sakstittelen (se
    parse_title_matrikkel) når det finnes og faktisk er != 0/0. gnr_bnr-
    feltet forblir uendret (kun det enkle gnr/bnr-paret) - kun matrikkelnr
    skrives fullt ut."""
    if not gnr_bnr_list:
        return None
    utvidelser = parse_title_matrikkel(tittel) if tittel else {}
    parts = []
    for g in gnr_bnr_list:
        gnr, _, bnr = g.partition("/")
        ext = utvidelser.get((gnr, bnr))
        if ext:
            feste, seksjon = ext
            parts.append(f"{kommune_nr}-{gnr}/{bnr}/{feste}/{seksjon}")
        else:
            parts.append(f"{kommune_nr}-{g}")
    return "; ".join(parts)


# Et "felt-/sonekode"-ord (bokstav+1-2 tall, f.eks. "B3", "K2", "F1") midt i
# en adressekandidat er et pålitelig tegn på at HELE kandidaten er en
# område-/planreferanse ("Eidet felt B3, Eidsdalen 11-41") og ikke en ekte
# gateadresse - ekte gatenavn i kilden inneholder ALDRI et slikt fritt-
# stående tegn (bekreftet: kun 1 forekomst i hele stikkprøven på ~2000
# saker, og den var nettopp en slik feilaktig lest sone-/planreferanse -
# se rapport). Kun ordet FORAN selve husnummeret sjekkes (bokstav-suffiks
# på husnummeret selv, f.eks. "16B", er noe helt annet og skal IKKE avvises).
_FELTKODE_I_GATENAVN = re.compile(r"\b[A-Za-zæøåÆØÅ]\d{1,2}\b")

_HAR_HUSNUMMER = re.compile(r"\s\d+[A-Za-zæøåÆØÅ]?(-\w+)?$")
# " - GNR/BNR - melding om mulig ulovlig tiltak - ..." - eldre/uformelt
# rapporterte ulovlighetssaker; gnr/bnr kan mangle. Vises da enten som en
# bar "/", ELLER (bekreftet i ekte data) som adressen selv gjentatt med en
# etterhengt "/" - f.eks. "Granlia 33 - Granlia 33/ - melding om ...", der
# kildesystemet tydeligvis bruker adressefeltet som plassholder når det
# ikke har noe reelt gnr/bnr å vise. Uansett hvilken variant - gnr_bnr
# forblir None (ingen reell verdi å hente ut), men selve adressen (delen
# FØR den første bindestreken) hentes fortsatt ut.
_DASH_GNRBNR = re.compile(r"\s-\s(\d+/\d+(?:/\d+)?|/|[^-/]*/)\s-\s")
# "Gatenavn [Nummer] GNR/BNR[/FESTENR/SEKSJONSNR][,] beskrivelse" - vanligste
# formatet for bygg/henv/tilsyn (og en del ulov).
_GNR_BNR_TITLE = re.compile(r"(\d+)/(\d+)(?:/\d+){0,2}")


def _har_husnummer(addr):
    """Sjekker om adressekandidaten faktisk har et ekte husnummer til slutt -
    uten dette er kandidaten som regel et sone-/stedsnavn (f.eks. "Kjerrheia,
    Vågsbygd", "Hellemyr felt E1, delfelt B4 ... B7") uten en selvstendig
    identifiserbar adresse."""
    return bool(_HAR_HUSNUMMER.search(" " + addr))


def address_from_tittel(tittel, kommune_nr):
    """Utleder (adresse_liste, gnr_bnr) direkte fra sakstittelen - brukes som
    fallback når adresse-panelet på siden er tomt (skjer ofte for useriøst
    rapporterte ulovlighetssaker). Tre kjente formater: (1) "Gatenavn
    [Nummer] GNR/BNR[/FESTENR/SEKSJONSNR][,] beskrivelse" (vanligst), (2)
    "Gatenavn Nummer - GNR/BNR - melding om mulig ulovlig tiltak - ..."
    (eldre ulovlighetssaker; gnr/bnr kan mangle, vist som bar "/", eller
    vises som kommunenr/gnr/bnr), (3) "GNR/BNR/FESTENR/SEKSJONSNR Gatenavn
    Nummer, beskrivelse" - matrikkelnummeret FØRST i tittelen i stedet for
    etter gateadressen. Hvis tittelen ikke har noe gnr/bnr i det hele tatt,
    sjekkes om starten av tittelen likevel ser ut som en ren adresse
    ("Gatenavn Nummer, beskrivelse", typisk for henvendelser/bevillinger uten
    matrikkeltilknytning). Kandidaten kjøres gjennom samme flerdresse-
    splitting som adresse-panelet (_parse_address_parts), slik at "Gate 4 og
    12" blir to adresser i stedet for én sammenslått streng. Returnerer None
    for adresse hvis kandidaten ikke har et ekte husnummer til slutt."""
    if not tittel:
        return None, None

    ingen_adresse = bool(_INGEN_ADRESSE.match(tittel.strip()))
    addr_candidate = None
    gnr_bnr = None

    m = _DASH_GNRBNR.search(tittel)
    if m:
        addr_candidate = tittel[:m.start()].strip()
        gm = re.match(r"(\d+)/(\d+)(?:/(\d+))?$", m.group(1))
        if gm:
            a, b, c = gm.groups()
            if c is not None and a == str(kommune_nr):
                gnr_bnr = f"{b}/{c}"        # var kommunenr/gnr/bnr
            else:
                gnr_bnr = f"{a}/{b}"
    else:
        gm = _GNR_BNR_TITLE.search(tittel)
        if gm:
            gnr_bnr = f"{gm.group(1)}/{gm.group(2)}"
            if gm.start() == 0:
                # Matrikkelnummeret står FØRST - adressen kommer etter det,
                # ikke før (format 3, se docstring).
                addr_candidate = tittel[gm.end():].lstrip().split(",", 1)[0].strip()
            else:
                addr_candidate = re.sub(r"[\s,\-]+$", "", tittel[:gm.start()].strip())
        else:
            # Ingen gnr/bnr i tittelen i det hele tatt - se om starten
            # likevel ser ut som en ren adresse uten matrikkeltilknytning.
            addr_candidate = tittel.split(",", 1)[0].strip()

    # Kandidaten kan selv starte med "Ingen adresse" selv om den ikke gjør
    # det for HELE tittelen - i så fall forkastes den helt i stedet for å
    # prøve å plukke ut en "ekte" adresse etter frasen, i tråd med samme
    # føre-var-holdning som når hele tittelen starter med den frasen.
    if addr_candidate and _INGEN_ADRESSE.match(addr_candidate.strip()):
        addr_candidate = None

    # En ekte gateadresse starter alltid med stor bokstav (egennavn) - luker
    # ut støy som "m.fl. - Kvartal 15" eller "og 10/53 - Ingen adresse - ..."
    # (rester av tittelen som havnet i kandidaten pga. et sammensatt/uvanlig
    # matrikkel- eller "m.fl."-format) og bare tall/plassholdere ("0", "0000").
    if addr_candidate and not _STARTS_UPPER.match(addr_candidate):
        addr_candidate = None

    adresse_liste = None
    if addr_candidate and not ingen_adresse:
        normalized = _normalize_nummer_bokstav(addr_candidate)
        entries = _parse_address_parts(normalized)
        if (not entries and _har_husnummer(normalized)
                and not _FELTKODE_I_GATENAVN.search(normalized)):
            entries = [normalized]   # kandidaten splittes ikke, men er selv gyldig
        if entries:
            adresse_liste = entries

    return adresse_liste, gnr_bnr


_GNR_BNR_PAIR = re.compile(r"(\d+)/(\d+)(?:/\d+){0,2}")
_MATRIKKEL_SEP = re.compile(r"\s*(?:,|\bog\b)\s*")


_BARE_BNR = re.compile(r"\d+")


def extra_gnr_bnr_fra_tittel(tittel):
    """Finn EKSTRA gnr/bnr-par oppgitt sammen med det første i en "og"/","-
    atskilt matrikkelliste i sakstittelen, f.eks. "244/30/0/0 og 244/70,
    Ingen adresse, ..." eller "10/52/0/0 og 10/53 Branderudveien 2, ...".
    Listen kan også bestå av BARE bruksnummer (samme gårdsnummer for alle),
    f.eks. "Okse - 426/55, 50, 47, 71, 54 - melding om ..." -> 426/50,
    426/47, 426/71, 426/54 (bekreftet på ekte data - se rapport). Disse
    sakene gjelder flere eiendommer samtidig - det andre (og evt. tredje
    osv.) paret finnes ALLTID i TILLEGG til det adresse-panelet allerede
    måtte ha gitt (som typisk kun viser det første), derfor kalles denne
    alltid fra parse_case, uavhengig av om adresse/gnr_bnr allerede er fylt
    ut fra andre kilder. Stopper ved første ledd som ikke selv er et nytt
    matrikkelnummer (fullt par ELLER bart bruksnummer), for å unngå å
    plukke opp uskyldige tall lenger ute i beskrivelsesteksten."""
    if not tittel:
        return []
    m = _GNR_BNR_TITLE.search(tittel)
    if not m:
        return []
    pairs = []
    gnr = m.group(1)
    pos = m.end()
    while True:
        sep = _MATRIKKEL_SEP.match(tittel, pos)
        if not sep:
            break
        pm = _GNR_BNR_PAIR.match(tittel, sep.end())
        if pm:
            gnr = pm.group(1)
            pairs.append(f"{pm.group(1)}/{pm.group(2)}")
            pos = pm.end()
            continue
        bm = _BARE_BNR.match(tittel, sep.end())
        if bm:
            pairs.append(f"{gnr}/{bm.group(0)}")
            pos = bm.end()
            continue
        break
    return pairs


# Et saksreferansenummer skrevet direkte inntil en sakstype-forkortelse
# ("BYGG-26/02377", "TILSYN-24/01394", "BYGG--25/02209" - dobbel bindestrek
# er en kilde-skrivefeil, ikke uvanlig) ser strukturelt ut som et gnr/bnr,
# men er det ALDRI - forkortelsen er kildens EGET saksnummerformat (se
# SAKSTYPE_FRA_PREFIKS). Kjennetegn: 2+ store bokstaver rett foran (uten
# mellomrom), evt. med bindestrek(er) mellom - ekte gatenavn/beskrivelser
# har aldri dette rett før et tall (bekreftet ved stikkprøve mot ~2000
# saker - eneste treff var nettopp slike saksreferanser, se rapport).
_SAKSREF_PREFIX = re.compile(r"[A-ZÆØÅ]{2,}-+$")
# Samme type saksreferanse, men skrevet med vanlig ord + mellomrom i stedet
# for forkortelse+bindestrek, f.eks. "tidligere sak 2015/1002", "se sak
# 26/16416", "tidligere byggesak 2010/0779" - "år/løpenummer"-formatet er
# identisk med et ekte gnr/bnr, men ordet rett foran ("sak"/"saksnummer"/
# "byggesak") er et pålitelig tegn på at det ikke er det (bekreftet ved
# stikkprøve mot ekte data - se rapport).
_SAKSREF_ORD = re.compile(r"\b(?:sak|saksnummer|byggesak)\s*$", re.IGNORECASE)
# Hele tallkjeden fanges (ikke bare de to første) fordi et fåtall titler
# skriver kommunenummeret FØRST i kjeden ("4204/13/143" = KOMMUNE_NR/gnr/bnr,
# ikke gnr/bnr 4204/13 - bekreftet på ekte data, se
# _gnr_bnr_fra_tallkjede_krs), samme mønster som Trondheim/Tromsø/Drammen.
_TALLKJEDE = re.compile(r"\d+(?:/\d+){1,3}")


def _gnr_bnr_fra_tallkjede_krs(tallkjede):
    """Tolk en tallkjede "A/B[/C[/D]]" som gnr/bnr - hopper over en ledende
    KOMMUNE_NR hvis kjeden starter med den. Returnerer None hvis kjeden ETTER
    et slikt KOMMUNE_NR-hopp ikke har et fullt gnr/bnr-par igjen (f.eks.
    "4204/472" - kun kommunenr+gnr, ingen bnr - en ufullstendig/redundant
    delvis gjentakelse, ikke en ekte matrikkel i seg selv)."""
    nums = tallkjede.split("/")
    if nums[0] == str(KOMMUNE_NR):
        if len(nums) < 3:
            return None
        return nums[1], nums[2]
    return nums[0], nums[1]


def ekstra_matrikkel_annet_sted_i_tittel(tittel):
    """Finn et HELT SEPARAT matrikkelnummer nevnt et annet sted i tittelen -
    ikke rett etter det første (se extra_gnr_bnr_fra_tittel for den listen),
    men et fritt-stående gnr/bnr lenger ute i teksten, f.eks. "Borøya -
    425/2 - Strand innerst i Vesterviga 52/2/0/0, inngrep i strandsonen"
    (425/2 OG 52/2 - to ulike matrikler i samme sak, ikke en liste).
    Ekskluderer saksreferansenumre (se _SAKSREF_PREFIX) og "0/0"-
    plassholderen. Rent tillegg - kalles alltid fra parse_case sammen med
    extra_gnr_bnr_fra_tittel, uavhengig av hva andre kilder ga."""
    if not tittel:
        return []
    pairs = []
    for m in _TALLKJEDE.finditer(tittel):
        foran = tittel[:m.start()]
        if _SAKSREF_PREFIX.search(foran) or _SAKSREF_ORD.search(foran):
            continue
        tolket = _gnr_bnr_fra_tallkjede_krs(m.group(0))
        if not tolket:
            continue
        gnr, bnr = tolket
        if gnr == "0" and bnr == "0":
            continue
        pair = f"{gnr}/{bnr}"
        if pair not in pairs:
            pairs.append(pair)
    return pairs


# Saksnummer-prefikset koder sakstypen, f.eks. "BYGG-26/02299" -> Byggesak.
SAKSTYPE_FRA_PREFIKS = {
    "BYGG": "Byggesak",
    "HENV": "Henvendelse",
    "ULOV": "Ulovlighetssak",
    "TILSYN": "Tilsynssak",
    "KLAGE": "Klagesak",
    "OPPM": "Oppmåling",
    "DELE": "Delesak",
    "PLAN": "Plansak",
    "INGN": "Ingeniørvesenet",
    "PARKV": "Parkvesenet",
    "TEKPL": "Tekniskplan",
    "SEKS": "Seksjoneringssak",
    "UTBY": "Utbyggingssak",
}


def sakstype_fra_saksnummer(saksnummer):
    """Utleder sakstype fra saksnummer-prefikset (faller tilbake til prefikset selv)."""
    m = re.match(r"([A-ZÆØÅ]+)-", saksnummer or "")
    if not m:
        return None
    prefiks = m.group(1)
    return SAKSTYPE_FRA_PREFIKS.get(prefiks, prefiks)


def split_saksnummer(text):
    """Deler teksten i saksnummer og resten av tittelen på første ' - '.

    Returnerer (None, teksten) dersom det ikke finnes noe saksnummer.
    """
    text = (text or "").strip()
    if " - " in text:
        nr, rest = text.split(" - ", 1)
        return nr.strip(), rest.strip()
    return None, text


# --------------------------------------------------------------------------- #
# Scraping / parsing - felles for alle sakstyper og for begge kjøremåtene
# (engangs historisk dump og daglig endringslogg)
# --------------------------------------------------------------------------- #
def get_case_list(saksnummer_prefiks):
    """Hent lista over alle saker (document_id-er) for én sakstype. Bruker
    "?q=<PREFIKS>-" (søk på saksnummer-prefiks), IKKE "?casetypeid=..." -
    sistnevnte viste stille kun "siste to måneder" som standardfilter (ingen
    feilmelding, bare et lavere tall enn faktisk). "q="-søket viser "Alle"
    år/sakstype som standard, ingen paginering."""
    search = f"{SEARCH_URL}?q={saksnummer_prefiks}"
    last_err = None
    response = None
    for attempt in range(RETRIES):
        try:
            response = requests.get(search, timeout=TIMEOUT)
            response.raise_for_status()
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    if response is None:
        raise RuntimeError(f"Klarte ikke å hente sakslista etter {RETRIES} forsøk") from last_err

    soup = BeautifulSoup(response.text, "html.parser")
    ids = []
    for li in soup.select("li._casefilter"):
        a = li.select_one("a")
        if not a:
            continue
        document_id = str(a["href"]).rstrip("/").split("/")[-1]
        if document_id:
            ids.append(document_id)
    return ids


def _get_case_ids(saksnummer_prefiks, limit=None):
    """Henter sakslisten via get_case_list() og begrenser den til de `limit`
    nyeste om satt - felles for run_full_dump og full_sweep."""
    ids = get_case_list(saksnummer_prefiks)
    return ids[:limit] if limit else ids


def _aside_section_fields(aside, header_text):
    """Hent felt-verdier (label -> tekst) fra DEN delen av .caseDetailsAside
    hvis <h3>-overskrift matcher header_text (case-insensitivt) - brukes for
    "Saksdetaljer" (inneholder Saksbehandler). Denne seksjonen bruker
    <div class="detailHeader">/<div class="detailContent"> (uten "document"-
    prefiks, ulikt dokumentenes egne felt lenger nede på siden) - en helt
    annen DOM-struktur enn adresse-seksjonens <li>-liste, så den ble tidligere
    aldri lest av parse_case() selv om den alltid har ligget i samme aside."""
    fields = {}
    if not aside:
        return fields
    for div in aside.select(".caseDetailsAsideDiv"):
        h3 = div.select_one(".caseDetailsAsideHeader h3")
        if not h3 or h3.get_text(strip=True).lower() != header_text.lower():
            continue
        for dl in div.select(".detailsList"):
            h = dl.select_one(".detailHeader span")
            v = dl.select_one(".detailContent")
            if h and v:
                fields[h.get_text(strip=True)] = v.get_text(strip=True)
    return fields


def parse_case(html, document_id):
    """Trekk ut all detaljdata fra en saks-side."""
    soup = BeautifulSoup(html, "html.parser")
    for s in soup(["script", "style"]):
        s.extract()

    h2 = soup.select_one(".pageTitleHeader h2")
    saksnummer, sakstittel = split_saksnummer(h2.get_text(strip=True) if h2 else "")

    adresse = []
    gnr_bnr = []
    aside = soup.select_one(".caseDetailsAside")
    saksbehandler = _aside_section_fields(aside, "Saksdetaljer").get("Saksbehandler")
    if aside:
        for li in aside.select("li"):
            t = li.get_text(strip=True)
            if not t:
                continue
            t = ADDR_INDEX_RE.sub("", t)            # fjern løpenummeret foran
            m = GNR_BNR_RE.search(t)
            if m:
                gnr_bnr.append(m.group(1))
                t = t[m.end():].strip()             # behold kun selve gateadressen
            adresse.append(t)

    dokumenter = []
    for btn in soup.select("[id^=caseDocument_]"):
        jid = btn.get("id", "").replace("caseDocument_", "")   # journalpost-id fra element-id-en
        h4 = btn.select_one(".accordionTitle h4")
        journalnummer, doktittel = split_saksnummer(h4.get_text(strip=True) if h4 else "")

        panel = btn.find_next_sibling()   # innholdet ligger i søsken-elementet
        fields = {}
        filer = []
        if panel:
            for dl in panel.select(".detailsList"):
                h = dl.select_one(".documentDetailHeader span")
                v = dl.select_one(".documentDetailContent")
                if h and v:
                    fields[h.get_text(strip=True)] = v.get_text(strip=True)

            for li in panel.select(".filesList li.fileLink"):
                a = li.select_one("a")
                href = a["href"] if a and a.get("href") else None
                fn = li.select_one(".fileNameDetail")
                cat = li.select_one(".fileDocumentCategory")

                fil_id, storrelse = None, None
                if href:
                    fil_id = Path(urlparse(href).path).stem   # fil-id er filnavnet uten filending
                    qs = parse_qs(urlparse(href).query)        # filstørrelsen ligger i lenkas parametere
                    if "fileSize" in qs:
                        try:
                            storrelse = int(qs["fileSize"][0])
                        except ValueError:
                            storrelse = None

                filer.append({
                    "navn": fn.get_text(strip=True) if fn else None,
                    "kategori": cat.get_text(strip=True) if cat else None,
                    "storrelse_bytes": storrelse,
                    "fil_id": fil_id,
                    "url": BASE_URL + href if href else None,   # gjør relativ lenke absolutt
                })

        dokumenter.append({
            "journalpost_id": jid,
            "journalnummer": journalnummer,
            "tittel": doktittel,
            "tilgangskode": fields.get("Tilgangskode"),
            "dokumenttype": fields.get("Dokumenttype"),
            "avsender": fields.get("Avsender"),
            "mottaker": fields.get("Mottaker"),
            "filer": filer,
        })

    adresse_clean = clean_adresser(adresse)
    gnr_bnr_clean = clean_gnr_bnr(gnr_bnr)

    # Hentes alltid (ikke bare når panelet er tomt) - brukes både som
    # fallback (panelet helt tomt) og som TILLEGG (panelet delvis tomt,
    # se under), aldri som overstyring av det panelet faktisk ga.
    fallback_adresse, fallback_gnr_bnr = address_from_tittel(sakstittel, KOMMUNE_NR)

    if not adresse_clean and fallback_adresse:
        adresse_clean = list(fallback_adresse)
    elif adresse_clean and fallback_adresse:
        # Tittelen kan liste FLERE husnumre for samme gate enn det
        # adresse-panelet faktisk viser ("Vardåsveien 94 og 96 58/124/0/0,
        # ..." -> panelet ga kun "Vardåsveien 94") - berik additivt, men
        # bare for gater panelet allerede har bekreftet, slik at en urelatert
        # tittel-gjetning ikke blandes inn når panelet har full/korrekt data
        # for en annen gate (bekreftet på ekte data - se rapport).
        kjente_gater = {g for g in (_gatenavn(a) for a in adresse_clean) if g}
        for cand in fallback_adresse:
            if cand not in adresse_clean and _gatenavn(cand) in kjente_gater:
                adresse_clean.append(cand)

    if not gnr_bnr_clean and fallback_gnr_bnr:
        gnr_bnr_clean = [fallback_gnr_bnr]

    # Saken kan gjelde FLERE eiendommer samtidig - enten som en liste rett
    # etter det første matrikkelnummeret ("244/30/0/0 og 244/70, ..." eller
    # "426/55, 50, 47, 71, 54 - ...", se extra_gnr_bnr_fra_tittel), eller som
    # et helt separat matrikkelnummer et annet sted i tittelen ("Borøya -
    # 425/2 - Strand innerst i Vesterviga 52/2/0/0, ...", se
    # ekstra_matrikkel_annet_sted_i_tittel). Fanger opp begge, uansett hvor
    # adresse/gnr_bnr for øvrig kom fra (rent tillegg, aldri en overstyring).
    for ekstra in extra_gnr_bnr_fra_tittel(sakstittel) + ekstra_matrikkel_annet_sted_i_tittel(sakstittel):
        if ekstra not in gnr_bnr_clean:
            gnr_bnr_clean.append(ekstra)

    return {
        "document_id": str(document_id),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": sakstype_fra_saksnummer(saksnummer),
        "sakstittel": sakstittel,
        "adresse": "; ".join(adresse_clean) or None,
        "gnr_bnr": "; ".join(gnr_bnr_clean) or None,
        "matrikkelnr": build_matrikkelnr(gnr_bnr_clean, KOMMUNE_NR, sakstittel),
        "saksbehandler": saksbehandler,
        "url": CASE_BASE_URL + str(document_id),
        "dokumenter": dokumenter,
    }


def fetch_case(session, document_id):
    """Henter og parser én sak, med gjentatte forsøk ved feil."""
    url = CASE_BASE_URL + str(document_id)
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return parse_case(r.text, document_id)
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))   # vent lenger for hvert nye forsøk
    # Alle forsøk feilet - returner saken med feilmelding i stedet for å stoppe kjøringen
    return {"document_id": str(document_id), "url": url, "error": str(last_err)}


def i_dag_oslo():
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def forkast_uventet_sakstype(records, saksnummer_prefiks):
    """Defensiv sjekk: "q="-søket (i get_case_list) er et fritekstsøk, ikke
    en garantert sakstype-filtrering - forkast enhver sak som mot
    formodning har et annet saksnummer-prefiks enn forventet. Returnerer
    (ok, forkastet)."""
    ok, forkastet = [], []
    for r in records:
        if "error" not in r and not (r.get("saksnummer") or "").startswith(saksnummer_prefiks):
            forkastet.append(r)
        else:
            ok.append(r)
    if forkastet:
        print(f"OBS: {len(forkastet)} saker hadde uventet saksnummer-prefiks og ble forkastet "
              f"(f.eks. saksnummer={forkastet[0].get('saksnummer')!r})")
    return ok, forkastet


# --------------------------------------------------------------------------- #
# Engangs historisk dump (2020-today_dump) - gjenopptakbar kjøring
# --------------------------------------------------------------------------- #
def _load_json(path):
    """Leser og parser en JSON-fil - returnerer None hvis den ikke finnes eller
    er korrupt/ugyldig (felles for load_done og load_snapshot)."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _atomic_write_json(data, path, **json_kwargs):
    """Skriver JSON til disk via en midlertidig fil for å unngå halvskrevet
    output ved krasj/avbrudd (felles for save_resultater og save_snapshot)."""
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, **json_kwargs)
    tmp.replace(path)   # bytt inn den ferdige fila i ett steg


def load_done(output_file):
    """Leser eksisterende output slik at kjøringen kan gjenopptas."""
    data = _load_json(output_file)
    if data is None:
        return {}
    # Bare saker som ble ferdig og uten feil regnes som gjort; resten hentes på nytt
    return {d["document_id"]: d for d in data if "dokumenter" in d and "error" not in d}


def save_resultater(results, output_file):
    """Skriver resultatene til disk via en midlertidig fil for å unngå halvskrevet output."""
    _atomic_write_json(list(results.values()), output_file, indent=2)


def run_full_dump(output_file, saksnummer_prefiks, limit=None, save_every=200):
    """Full historisk dump av én sakstype (brukes av 2020-today_dump/*/main.py).

    limit=N henter kun de N nyeste sakene (lista fra get_case_list() er
    sortert nyeste først) - nyttig for raske delkjøringer/testing uten å
    måtte gå gjennom hele den tunge full-dumpen (~21000 saker totalt på
    tvers av alle sakstyper) hver gang."""
    ids = _get_case_ids(saksnummer_prefiks, limit)

    results = load_done(output_file)   # hopp over saker som allerede ligger i output
    todo = [i for i in ids if i not in results]
    print(f"{len(ids)} saker totalt" + (f" (limit={limit})" if limit else "") +
          f", {len(results)} allerede hentet, {len(todo)} gjenstår")

    session = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_case, session, i): i for i in todo}
        for fut in as_completed(futures):   # ta imot resultatene etter hvert som de blir ferdige
            res = fut.result()
            results[res["document_id"]] = res
            done += 1
            if done % save_every == 0:   # lagre fremdriften jevnlig underveis
                save_resultater(results, output_file)
                print(f"  {done}/{len(todo)} hentet (lagret)")

    _, forkastet = forkast_uventet_sakstype(list(results.values()), saksnummer_prefiks)
    for r in forkastet:
        del results[r["document_id"]]

    save_resultater(results, output_file)
    errors = sum(1 for r in results.values() if "error" in r)
    print(f"Ferdig: {len(results)} saker skrevet til {output_file} ({errors} feilet)")

    # Azure-opplasting er BEVISST frakoblet her - kjør run_full_dump() rent
    # lokalt (ingen forsøk på tilkobling). Funksjonen upload_full_dump_til_azure()
    # står fortsatt klar og er uendret - kall den manuelt når Azure-tilgang er
    # på plass.
    # if limit is None:   # ikke last opp delkjøringer/lokal test til Azure
    #     upload_full_dump_til_azure(list(results.values()), saksnummer_prefiks)


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - full sweep + snapshot-diff
# --------------------------------------------------------------------------- #
def full_sweep(saksnummer_prefiks, limit=None):
    """Hent alle sakene av én sakstype på nytt (full skanning).

    limit=N henter kun de N nyeste sakene (lista er sortert nyeste først).
    Nyttig for rask testing - i produksjon kjøres den uten limit. NB: siden
    "siste to måneder"-snarveien i get_case_list er bekreftet basert på når
    saken ble OPPRETTET (ikke sist oppdatert), må denne fortsatt sveipe
    absolutt alle sakene hver gang for å oppdage nye dokumenter på eldre,
    fortsatt aktive saker."""
    ids = _get_case_ids(saksnummer_prefiks, limit)
    print(f"Full sweep: {len(ids)} saker" + (f" (limit={limit})" if limit else ""))
    results = []
    session = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_case, session, i) for i in ids]
        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1
            if done % 1000 == 0:
                print(f"  {done}/{len(ids)} hentet")

    ok, _ = forkast_uventet_sakstype(results, saksnummer_prefiks)
    return ok


def load_snapshot(snapshot_file):
    """Leser gårsdagens snapshot: {document_id: [journalpost_id, ...]}."""
    data = _load_json(snapshot_file)
    return data if data is not None else {}


def build_snapshot(results):
    """Lager dagens snapshot fra en full sweep."""
    snapshot = {}
    for r in results:
        if "dokumenter" not in r:
            continue  # hopp over saker som feilet
        snapshot[r["document_id"]] = sorted(
            jp["journalpost_id"] for jp in r["dokumenter"] if jp.get("journalpost_id")
        )
    return snapshot


def save_snapshot(snapshot, snapshot_file):
    """Skriver snapshot atomisk (via midlertidig fil)."""
    _atomic_write_json(snapshot, snapshot_file)


# --------------------------------------------------------------------------- #
# Snapshot-state i Azure Blob - KILDEN TIL SANNHET for running_daily fremover.
#
# Jobben skal etter planen kjøre i Azure (ikke som en manuell script-kjøring
# på en enkelt persons PC), trolig på compute som ikke har en persistent
# lokal disk mellom kjøringer. Da kan ikke snapshot.json på lokal disk være
# det eneste stedet gårsdagens saksliste ligger - den må også (og fremover
# PRIMÆRT) leses fra og skrives til Azure Blob, slik at et helt nytt/annet
# kompute-miljø som plukker opp morgendagens kjøring likevel finner riktig
# baseline. De lokale snapshot.json-filene som ligger i repoet i dag er bare
# en best-effort fallback/cache - ikke noe å stole på i produksjon.
#
# Sti: bronze/kristiansand/_state/<slug>_snapshot.json - EGEN "_state"-mappe,
# ikke under load_type=full|incremental, siden dette er intern
# driftsmetadata (dedup-state), ikke skrapet postliste-data. Ren JSON, ikke
# gzippet JSONL - dette er en enkelt, liten, oppdaterbar fil (overskrives
# hver kjøring), ikke en datopartisjonert dataserie.
# --------------------------------------------------------------------------- #
def _azure_state_blob_name(sakstype):
    slug = _SAKSTYPE_SLUG.get(sakstype, sakstype.lower())
    return f"{AZURE_BASE_PATH}/_state/{slug}_snapshot.json"


def load_snapshot_azure(sakstype):
    """Leser siste snapshot fra Azure Blob. Returnerer None (ikke {}) hvis
    den ikke finnes/ikke kan leses - kallende kode bruker det til å falle
    tilbake til lokal fil, i motsetning til load_snapshot() sin {} (som her
    ville betydd "første kjøring, ingen tidligere data")."""
    try:
        from azure.storage.blob import BlobServiceClient
        credential = _azure_credential()
        client = BlobServiceClient(AZURE_ACCOUNT_URL, credential=credential,
                                    connection_timeout=TIMEOUT, read_timeout=TIMEOUT)
        blob = client.get_container_client(AZURE_CONTAINER_NAME).get_blob_client(
            _azure_state_blob_name(sakstype))
        data = blob.download_blob().readall()
        snapshot = json.loads(data.decode("utf-8"))
        print(f"  [Azure] leste snapshot fra Azure ({len(snapshot)} saker)")
        return snapshot
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] fant ingen brukbar snapshot i Azure ({e}) - "
              f"faller tilbake til lokal fil hvis den finnes")
        return None


def save_snapshot_azure(snapshot, sakstype):
    """Skriver snapshot til Azure Blob (overskriver - dette er alltid siste
    kjente tilstand, ikke en datopartisjonert historikk). Feiler ALDRI hele
    kjøringen - samme mønster som upload_til_azure()."""
    try:
        from azure.storage.blob import BlobServiceClient
        credential = _azure_credential()
        client = BlobServiceClient(AZURE_ACCOUNT_URL, credential=credential,
                                    connection_timeout=TIMEOUT, read_timeout=TIMEOUT)
        blob = client.get_container_client(AZURE_CONTAINER_NAME).get_blob_client(
            _azure_state_blob_name(sakstype))
        payload = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
        blob.upload_blob(payload, overwrite=True)
        print(f"  [Azure] snapshot lagret i Azure ({len(snapshot)} saker)")
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KUNNE IKKE lagre snapshot i Azure ({e}) - lagret kun lokalt")


def diff(results, prev):
    """Sammenlign dagens sweep mot gårsdagens snapshot.

    Returnerer (nye_saker, nye_journalposter):
      - nye_saker:         saker med document_id vi ikke har sett før (hele saken er ny)
      - nye_journalposter: eksisterende saker som har fått journalpost-ID-er vi ikke hadde
    """
    nye_saker = []
    nye_journalposter = []

    for r in results:
        if "dokumenter" not in r:
            continue
        cid = r["document_id"]
        sett_for = set(prev.get(cid, []))
        nye_jp = [jp for jp in r["dokumenter"]
                  if jp.get("journalpost_id") and jp["journalpost_id"] not in sett_for]

        if cid not in prev:
            nye_saker.append(r)                 # helt ny sak
        elif nye_jp:
            nye_journalposter.append({          # ny post på eksisterende sak
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
                    "saksbehandler": r.get("saksbehandler"),
                    "url": r.get("url"),
                },
                "nye_journalposter": nye_jp,
            })

    return nye_saker, nye_journalposter


_SAKSTYPE_SLUG = {
    "Byggesak": "bygg",
    "Henvendelse": "henv",
    "Ulovlighetssak": "ulov",
    "Tilsynssak": "tilsyn",
}


def _azure_credential():
    """Azure Blob-credential, med tre mulige kilder, forsøkt i denne
    rekkefølgen:
      1) Databricks secrets (scope "postlister"), hvis kjørt der.
      2) Service Principal via miljøvariabler - lastes fra bronze/postlister/
         .env (se bronze/postlister/.env.example) hvis python-dotenv er
         installert og fila finnes. Samme hemmeligheter/navnekonvensjon som
         easy-pipelines-repoet sin Trondheim-pipeline
         (SP-PIPELINE-POSTLISTER-CLIENT-ID/-TENANT-ID/-CLIENT-SECRET) - ÉN
         Service Principal for HELE "postlister"-containeren (ikke
         kommune-spesifikk), derfor deles .env-fila på bronze/postlister/-
         nivå i stedet for å duplisere den per kommune.
      3) Egen Azure CLI-innlogging (`az login`) via AzureCliCredential, hvis
         ingen Service Principal-hemmeligheter er satt - nyttig når du ikke
         har fått tildelt en Service Principal, men vil bruke din egen
         brukerkonto. Krever at Azure CLI (`az`) er installert og at
         `az login` er kjørt (én gang, interaktivt) på maskinen som kjører
         scriptet, OG at brukeren din har fått tildelt rollen "Storage Blob
         Data Contributor" (eller lignende) på storage-kontoen/containeren i
         Azure Portal -> denne kontoens "Access Control (IAM)".
    Kaster ValueError/RuntimeError med en tydelig melding hvis ingen av dem
    virker - upload_til_azure()/upload_full_dump_til_azure() fanger denne og
    hopper over opplastingen i stedet for å krasje hele den lokale
    skrape-kjøringen."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    except Exception:  # noqa: BLE001
        dbutils = None

    if dbutils is not None:
        get = lambda k: dbutils.secrets.get(scope="postlister", key=k)  # noqa: E731
    else:
        try:
            from dotenv import load_dotenv
            # Denne fila finnes i to varianter på forskjellig dybde under
            # bronze/postlister/ (den opprinnelige i common/, og en fysisk
            # kopi i hver running_daily/<type>/app/lib/ for wheel-pakking) -
            # let oppover til vi finner mappen som faktisk heter "postlister"
            # i stedet for å anta et fast antall nivåer.
            for parent in Path(__file__).resolve().parents:
                if parent.name == "postlister":
                    load_dotenv(dotenv_path=parent / ".env")
                    break
        except Exception:  # noqa: BLE001
            pass
        get = lambda k: os.environ.get(k)  # noqa: E731

    client_id = get("SP-PIPELINE-POSTLISTER-CLIENT-ID")
    tenant_id = get("SP-PIPELINE-POSTLISTER-TENANT-ID")
    client_secret = get("SP-PIPELINE-POSTLISTER-CLIENT-SECRET")

    if client_id and tenant_id and client_secret:
        from azure.identity import ClientSecretCredential
        return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    # Ingen Service Principal satt opp - fall tilbake til egen az-login.
    try:
        from azure.identity import AzureCliCredential
        credential = AzureCliCredential()
        # Henter et token med det samme her (i stedet for å vente til selve
        # opplastingen) - da feiler vi raskt og tydelig hvis "az login" ikke
        # er gjort/CLI-en ikke er installert, i stedet for å la BlobServiceClient
        # gi en mer kryptisk feil lenger ned.
        credential.get_token("https://storage.azure.com/.default")
        return credential
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "Mangler Azure-credential: ingen Service Principal satt (sett "
            "SP-PIPELINE-POSTLISTER-CLIENT-ID/-TENANT-ID/-CLIENT-SECRET i "
            "bronze/postlister/.env, se .env.example) OG 'az login' via Azure "
            f"CLI er ikke gjort/fungerer ikke ({e})"
        ) from e


def _last_opp_jsonl_gz(poster, blob_name, credential):
    """Serialiser en liste med poster til JSONL (én post per linje),
    gzip-komprimer, og last opp som én blob (overskriver hvis den allerede
    finnes - trygt å kjøre kjøringen på nytt for samme dato)."""
    from azure.storage.blob import BlobServiceClient

    jsonl = "\n".join(json.dumps(p, ensure_ascii=False) for p in poster)
    komprimert = gzip.compress(jsonl.encode("utf-8"))

    # connection_timeout/read_timeout begrenser selve HTTP-kallet - uten
    # disse kan en feilkonfigurert credential eller nettverksfeil la
    # kjøringen henge lenge før azure-sdk sine egne retries gir opp (sett
    # ved konstruksjon, ikke som timeout=-kwarg på upload_blob - den styrer
    # kun server-side operasjonstimeout, ikke selve HTTP-tilkoblingen).
    client = BlobServiceClient(AZURE_ACCOUNT_URL, credential=credential,
                                connection_timeout=TIMEOUT, read_timeout=TIMEOUT)
    blob = client.get_container_client(AZURE_CONTAINER_NAME).get_blob_client(blob_name)
    blob.upload_blob(komprimert, overwrite=True)
    return len(komprimert)


def upload_til_azure(saker_file, jp_file, ref_date, sakstype):
    """Laster dagens endringslogg (nye saker + nye journalposter-på-gamle-
    saker, allerede skrevet lokalt av write_changelog) opp til Azure Blob
    som JSONL+gzip, én blob per liste (hopper over en liste hvis den er
    tom - ingen tomme filer i Azure).

    Stikonvensjon (identisk med easy-pipelines/Trondheim, se BRONZE_README.md
    der): bronze/kristiansand/load_type=incremental/date=<ref_date>/
    <slug>_saker-<ref_date>.jsonl.gz (+ <slug>_journalposter-<ref_date>.jsonl.gz).

    Feiler ALDRI hele kjøringen ved Azure-problemer (manglende credentials,
    nettverksfeil, osv.) - de lokale filene er allerede skrevet før dette
    kalles, så en mislykket opplasting logges og hoppes over i stedet for å
    kaste, slik at neste kjøring (eller en manuell reupload senere) kan ta
    igjen det som ble stående lokalt."""
    slug = _SAKSTYPE_SLUG.get(sakstype, sakstype.lower())
    try:
        credential = _azure_credential()
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] hopper over opplasting - {e}")
        return

    prefix = f"{AZURE_BASE_PATH}/load_type=incremental/date={ref_date}"
    for filnavn, poster in ((f"{slug}_saker", json.loads(saker_file.read_text(encoding="utf-8"))),
                             (f"{slug}_journalposter", json.loads(jp_file.read_text(encoding="utf-8")))):
        if not poster:
            continue
        blob_name = f"{prefix}/{filnavn}-{ref_date}.jsonl.gz"
        try:
            storrelse = _last_opp_jsonl_gz(poster, blob_name, credential)
            print(f"  [Azure] lastet opp {len(poster)} poster -> "
                  f"{AZURE_CONTAINER_NAME}/{blob_name} ({storrelse / 1024:.1f} KB)")
        except Exception as e:  # noqa: BLE001
            print(f"  [Azure] opplasting FEILET for {blob_name}: {e}")


def upload_full_dump_til_azure(poster, saksnummer_prefiks):
    """Laster HELE engangs-historikk-dumpen (all-i-en-fil fra run_full_dump())
    opp til Azure Blob som JSONL+gzip - én blob for hele sakstypen.

    Stikonvensjon: bronze/kristiansand/load_type=full/<slug>/dump_date=<i_dag>/
    <slug>_saker-full-<i_dag>.jsonl.gz. Avviker fra easy-pipelines/Trondheim sin
    load_type=full/<type>/month=<YYYY-MM>/... (partisjonert på SAKENS egen
    dato, siden Trondheims dump-fetcher henter måned for måned) - Kristiansands
    "q="-søk gir ALLE saker i ett kall uten paginering/datofilter, og selve
    case-postene har ikke noe brukbart per-sak datofelt å partisjonere på
    (kun årstall i saksnummeret, f.eks. "BYGG-26/02377") - derfor partisjoneres
    i stedet på KJØREDATO (når dumpen faktisk ble tatt), ikke sakens dato.

    Feiler ALDRI (samme mønster som upload_til_azure) - den lokale filen fra
    run_full_dump() er alltid den autoritative kopien uansett."""
    if not poster:
        print("  [Azure] ingen poster å laste opp (tom dump)")
        return

    slug = saksnummer_prefiks.rstrip("-").lower()
    try:
        credential = _azure_credential()
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] hopper over full-dump-opplasting - {e}")
        return

    i_dag = i_dag_oslo()
    blob_name = f"{AZURE_BASE_PATH}/load_type=full/{slug}/dump_date={i_dag}/{slug}_saker-full-{i_dag}.jsonl.gz"
    try:
        storrelse = _last_opp_jsonl_gz(poster, blob_name, credential)
        print(f"  [Azure] lastet opp {len(poster)} poster (full dump) -> "
              f"{AZURE_CONTAINER_NAME}/{blob_name} ({storrelse / 1024:.1f} KB)")
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] full-dump-opplasting FEILET for {blob_name}: {e}")


def write_changelog(nye_saker, nye_journalposter, ref_date, output_dir, sakstype):
    """Skriver dagens endringslogg lokalt, og kaller Azure-hooken (se over)."""
    output_dir.mkdir(exist_ok=True)
    saker_file = output_dir / f"saker_{ref_date}.json"
    jp_file = output_dir / f"journalposter_{ref_date}.json"

    saker_file.write_text(json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    jp_file.write_text(json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Skrev {len(nye_saker)} nye saker  -> {saker_file}")
    print(f"Skrev {len(nye_journalposter)} saker med nye journalposter -> {jp_file}")
    upload_til_azure(saker_file, jp_file, ref_date, sakstype)


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - inngangspunkt
# --------------------------------------------------------------------------- #
def run_daily(snapshot_file, output_dir, saksnummer_prefiks, sakstype, limit=None, today_fn=i_dag_oslo):
    """Daglig endringslogg for én sakstype (brukes av running_daily/*/app/main.py).

    `today_fn` er injiserbar i stedet for å kalle datetime.now(...) direkte,
    slik at tester kan simulere flere påfølgende "dager" uten å vente på
    ekte klokketid."""
    ref_date = today_fn().isoformat()
    # Azure er kilden til sannhet (se load_snapshot_azure-docstring) - lokal
    # fil er kun en fallback/cache hvis Azure ikke er tilgjengelig/satt opp.
    prev = load_snapshot_azure(sakstype)
    if prev is None:
        prev = load_snapshot(snapshot_file)

    results = full_sweep(saksnummer_prefiks, limit=limit)
    snapshot = build_snapshot(results)
    feilet = sum(1 for r in results if "error" in r)

    if not prev:
        # Første kjøring: ingen snapshot å diffe mot. Etabler baseline uten å
        # dumpe alle sakene som "nye" - neste kjøring gir en ekte endringslogg.
        save_snapshot(snapshot, snapshot_file)
        save_snapshot_azure(snapshot, sakstype)
        print(f"Baseline etablert: {len(snapshot)} saker lagret i snapshot "
              f"({feilet} feilet). Ingen endringslogg skrevet på første kjøring.")
        return

    nye_saker, nye_journalposter = diff(results, prev)
    write_changelog(nye_saker, nye_journalposter, ref_date, output_dir, sakstype)
    save_snapshot(snapshot, snapshot_file)
    save_snapshot_azure(snapshot, sakstype)
    print(f"Ferdig ({feilet} saker feilet under henting).")
