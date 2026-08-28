"""Lillestrøm - OpenGov/360online (samme plattform som Kristiansand/Sandnes/
Sarpsborg). De fire sakstypene (Byggesak/Henvendelse/Ulovlighetssak/
Tilsynssak) hentes fra samme portal med samme logikk - forskjellen er bare
saksnummer-prefikset ("BYGG-"/"HENV-"/"ULOV-"/"TILSYN-") man søker på.
"""

import gzip
import json
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# --- Felles konstanter ---
BASE_URL = "https://opengov.360online.com"
SEARCH_URL = f"{BASE_URL}/Cases/LILLESTROM"
CASE_BASE_URL = f"{BASE_URL}/Cases/LILLESTROM/Case/Details/"

KOMMUNE_NR = 3205   # Lillestrøm (endret fra 3030 ved kommunenummer-endringen i 2024)
KOMMUNE = "Lillestrøm"

# Azure Blob - samme konto/container/opplastingsmønster som de andre
# kommunene, se _azure_credential()/upload_til_azure() lenger ned.
#
# Portalen her har verken dato-filter eller API/feed, så run_daily() må
# gjøre en full sveip av alle sakene hver dag og diffe mot forrige
# snapshot. Snapshotet ligger derfor i Azure, ikke bare lokalt, slik at en
# ny Databricks-kjøring uten lokal disk fra i går fortsatt finner riktig
# utgangspunkt.
#
# Den daglige endringsloggen skriver ikke noen lokal fil i repoet (se
# write_changelog) - jobben kjører evig hver dag på tvers av fire
# sakstyper, og en fil per dag ville bare vokst og vokst.
AZURE_ACCOUNT_URL = "https://storaggen2eaccountprod.blob.core.windows.net"
AZURE_CONTAINER_NAME = "postlister"
AZURE_BASE_PATH = "bronze/lillestrom"

MAX_WORKERS = 8
TIMEOUT = 30
RETRIES = 3

GNR_BNR_RE = re.compile(r"\b(\d+/\d+)")   # gårds- og bruksnummer i en adresse
LOPENUMMER_RE = re.compile(r"^\d+\.\s*")  # løpenummer foran en adresselinje


# --- Opprydding av adresse/gnr_bnr + matrikkelnr ---
#
# Den rå adresselista fra Lillestrøm ser ut som "Gatenavn 12, 4610
# LILLESTRØM, Norge" - ofte med duplikater (samme adresse med og uten
# mellomrom i bokstav-suffikset, "16 B" vs "16B"), og av og til flere
# husnumre kommaseparert på én linje. Alt dette rydder vi til en flat,
# deduplisert liste, slik at "adresse"/"gnr_bnr" lagres som semikolon-
# separerte strenger og "matrikkelnr" kan avledes fra dem.
_TALL_BOKSTAV_MELLOMROM = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
_POSTNR_STED = re.compile(r"^\d{4}(\s|$)")

# Ett husnummer: et tall med valgfritt bokstav-suffiks, evt. som et
# bindestrek-spenn til et nytt tall/bokstav ("18", "9A", "30A-E", "24-34").
_HUSNUMMER = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_KUN_HUSNUMMER = re.compile(rf"^{_HUSNUMMER}$")
_KUN_BOKSTAV = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_GATE_OG_NUMMER = re.compile(rf"^(.+?)\s+({_HUSNUMMER})$")
_SKILLETEGN = re.compile(r"\s*(,|\bog\b|&)\s*")
_STOR_FORBOKSTAV = re.compile(r"^[A-ZÆØÅ]")


def _normalize_nummer_bokstav(s):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks: "16 B" -> "16B"."""
    return _TALL_BOKSTAV_MELLOMROM.sub(r"\1\2", s)


def _strip_sted_suffix(raw):
    """Fjerner ', <postnr> <sted>, Norge' / ', Norge' fra enden av en rå
    adresselinje, og lar resten (gatenavn + evt. flere husnumre) stå urørt."""
    parts = [p.strip() for p in raw.split(",")]
    if parts and parts[-1].lower() == "norge":
        parts = parts[:-1]
    if parts and (_POSTNR_STED.match(parts[-1]) or parts[-1] in ("", "0")):
        parts = parts[:-1]
    return ", ".join(p for p in parts if p)


def _parse_address_parts(s):
    """Splitter en adressekandidat i enkeltadresser atskilt med komma og/
    eller ' og ' (samme triks som for å lese Trondheims sakstitler). Et bart
    tall arver gatenavnet fra forrige element ("3" etter "Lagmannsholmen 1"
    blir "Lagmannsholmen 3"), en bar bokstav arver gatenavn+tall, og et nytt
    "Gatenavn nummer" starter en ny gate. For det aller første elementet
    stoler vi på gatenavnet uansett (det kommer fra kildens egen
    adresseliste); for elementer etter komma/og krever vi stor forbokstav,
    ellers kan beskrivelsestekst med et tall på slutten bli feiltolket som
    en ny gate. Bindestrek-spenn ('24-34') beholdes samlet - vi gjetter ikke
    husnumrene mellom endepunktene."""
    entries = []
    current_street = None
    last_number = None
    tokens = _SKILLETEGN.split(s)
    part0, seps_and_parts = tokens[0], tokens[1:]
    parts = [(None, part0)] + list(zip(seps_and_parts[0::2], seps_and_parts[1::2]))
    for sep, part in parts:
        part = part.strip()
        if not part:
            continue
        if _KUN_BOKSTAV.match(part):
            if current_street is None or last_number is None:
                break
            entries.append(f"{current_street} {last_number}{part}")
            continue
        if _KUN_HUSNUMMER.match(part):
            if current_street is None:
                break
            norm = re.sub(r"\s*-\s*", "-", part)
            entries.append(f"{current_street} {norm}")
            mnum = re.match(r"^(\d+)", norm)
            if mnum:
                last_number = mnum.group(1)
            continue
        sm = _GATE_OG_NUMMER.match(part)
        if sm and (sep is None or _STOR_FORBOKSTAV.match(sm.group(1).strip())):
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
    """Gatenavnet uten husnummer, f.eks. "Vardåsveien 96" -> "Vardåsveien" -
    brukt til å sjekke om en tittel-gjettet adresse gjelder samme gate som
    adresse-panelet allerede har bekreftet (se parse_case)."""
    m = _GATE_OG_NUMMER.match(adresse)
    return m.group(1).strip() if m else None


def clean_adresser(raw_list):
    """Rydder den rå adresselista: fjerner postnr/sted/Norge-støy, sprenger
    kommalister av flere husnumre per gate, og dedupliserer mellomroms-
    varianter ("16 B" vs "16B") til én flat, deduplisert liste."""
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
    """Dedupliserer gnr/bnr-lista (den dubleres ofte i kilden), beholder rekkefølgen."""
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
    oppgir aldri feste/seksjon, kun gnr/bnr - tittelen ("GNR/BNR/FESTENR/
    SEKSJONSNR Gatenavn Nummer, beskrivelse") er eneste sted vi kan hente den
    presisjonen fra. "0/0" betyr "ingen feste/seksjon" og hoppes over."""
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
    """Bygger matrikkelnr fra den dedupliserte gnr_bnr-lista, f.eks.
    "3205-150/732", og fyller på feste/seksjon fra tittelen (se
    parse_title_matrikkel) der det finnes og faktisk ikke er 0/0. gnr_bnr-
    feltet i seg selv holdes uendret - det er bare matrikkelnr som skrives fullt ut."""
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


# Et felt-/sonekode-ord (bokstav + 1-2 tall, f.eks. "B3", "K2", "F1") midt i
# en adressekandidat betyr som regel at hele greia er en område-/
# planreferanse ("Eidet felt B3, Eidsdalen 11-41"), ikke en ekte
# gateadresse. Vi sjekker bare ordet FORAN selve husnummeret - et
# bokstav-suffiks på husnummeret ("16B") er noe helt annet og skal ikke
# forkastes.
_FELTKODE_I_GATENAVN = re.compile(r"\b[A-Za-zæøåÆØÅ]\d{1,2}\b")

_HAR_HUSNUMMER = re.compile(r"\s\d+[A-Za-zæøåÆØÅ]?(-\w+)?$")
# " - GNR/BNR - melding om mulig ulovlig tiltak - ..." - eldre, uformelt
# rapporterte ulovlighetssaker, der gnr/bnr kan mangle. Vises da enten som
# en bar "/", eller som adressen selv gjentatt med en "/" på slutten
# ("Granlia 33 - Granlia 33/ - melding om ..."), som om kildesystemet
# bruker adressefeltet som plassholder når det ikke har noe reelt gnr/bnr.
# Uansett hvilken variant blir gnr_bnr None, men adressen (delen før den
# første bindestreken) henter vi ut likevel.
_DASH_GNRBNR = re.compile(r"\s-\s(\d+/\d+(?:/\d+)?|/|[^-/]*/)\s-\s")
# "Gatenavn [Nummer] GNR/BNR[/FESTENR/SEKSJONSNR][,] beskrivelse" - det
# vanligste formatet for bygg/henv/tilsyn (og en del ulov).
_GNR_BNR_TITLE = re.compile(r"(\d+)/(\d+)(?:/\d+){0,2}")


def _har_husnummer(addr):
    """Sjekker om adressekandidaten faktisk har et ekte husnummer til slutt -
    uten det er kandidaten som regel bare et sone-/stedsnavn."""
    return bool(_HAR_HUSNUMMER.search(" " + addr))


def address_from_tittel(tittel, kommune_nr):
    """Gjetter (adresse_liste, gnr_bnr) ut fra sakstittelen - brukes som
    fallback når adresse-panelet på siden er tomt. Tre formater dukker opp:

    1. "Gatenavn [Nummer] GNR/BNR[/FESTENR/SEKSJONSNR][,] beskrivelse" (vanligst)
    2. "Gatenavn Nummer - GNR/BNR - melding om mulig ulovlig tiltak - ..."
       (eldre ulovlighetssaker, gnr/bnr kan mangle - se _DASH_GNRBNR)
    3. "GNR/BNR/FESTENR/SEKSJONSNR Gatenavn Nummer, beskrivelse" - matrikkel-
       nummeret står FØRST, ikke etter gateadressen. Uten denne grenen ville
       hele biten før komma bli tolket som en tom adresse.

    Har tittelen ikke noe gnr/bnr i det hele tatt, sjekker vi om starten
    likevel ser ut som en ren adresse ("Gatenavn Nummer, beskrivelse"),
    vanlig for henvendelser uten matrikkeltilknytning. Kandidaten kjøres
    gjennom samme adresse-splitting som panelet (_parse_address_parts), slik
    at "Gate 4 og 12" blir to adresser og ikke én sammenslått streng.
    Returnerer None for adresse hvis kandidaten ikke har et ekte husnummer."""
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
                # Matrikkelnummeret står først - adressen kommer etter det (format 3).
                addr_candidate = tittel[gm.end():].lstrip().split(",", 1)[0].strip()
            else:
                addr_candidate = re.sub(r"[\s,\-]+$", "", tittel[:gm.start()].strip())
        else:
            # Ingen gnr/bnr i tittelen - sjekk om starten likevel ligner en ren adresse.
            addr_candidate = tittel.split(",", 1)[0].strip()

    # Kandidaten kan selv starte med "Ingen adresse" selv om ikke hele
    # tittelen gjør det (f.eks. "75/329/0/0 Ingen adresse - Tomt
    # Breidablikkveien 2, ...") - da forkastes den helt, samme
    # føre-var-holdning som når hele tittelen starter med frasen.
    if addr_candidate and _INGEN_ADRESSE.match(addr_candidate.strip()):
        addr_candidate = None

    # En ekte gateadresse starter alltid med stor forbokstav - luker ut støy
    # som "m.fl. - Kvartal 15" eller "og 10/53 - Ingen adresse - ..." (rester
    # fra et sammensatt matrikkel- eller "m.fl."-format) og rene tall/
    # plassholdere ("0", "0000").
    if addr_candidate and not _STOR_FORBOKSTAV.match(addr_candidate):
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
    """Finn EKSTRA gnr/bnr-par listet sammen med det første i en "og"/","-
    atskilt matrikkelliste i tittelen, f.eks. "244/30/0/0 og 244/70, Ingen
    adresse, ..." eller "10/52/0/0 og 10/53 Branderudveien 2, ...". Lista kan
    også bestå av bare bruksnummer med samme gårdsnummer, f.eks.
    "Okse - 426/55, 50, 47, 71, 54 - melding om ..." -> 426/50, 426/47,
    426/71, 426/54. Disse sakene gjelder flere eiendommer samtidig - det
    andre (og evt. tredje) paret kommer alltid i TILLEGG til det adresse-
    panelet allerede ga (som typisk bare viser det første), så denne kalles
    alltid fra parse_case uansett hva andre kilder ga. Stopper ved første
    ledd som ikke selv er et nytt matrikkelnummer, for å ikke plukke opp
    tilfeldige tall lenger ute i beskrivelsen."""
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


# Et saksreferansenummer skrevet rett inntil en sakstype-forkortelse
# ("BYGG-26/02377", "TILSYN-24/01394") ser strukturelt ut som et gnr/bnr,
# men er det aldri - forkortelsen er kildens eget saksnummerformat (se
# SAKSTYPE_FRA_PREFIKS). Kjennetegn: 2+ store bokstaver rett foran uten
# mellomrom, evt. med bindestrek(er) mellom.
_SAKSREF_PREFIX = re.compile(r"[A-ZÆØÅ]{2,}-+$")
# Samme type saksreferanse, men skrevet som vanlig ord + mellomrom i stedet
# for forkortelse+bindestrek, f.eks. "tidligere sak 2015/1002", "se sak
# 26/16416". "år/løpenummer"-formatet er identisk med et ekte gnr/bnr, men
# ordet rett foran ("sak"/"saksnummer"/"byggesak") avslører at det ikke er det.
_SAKSREF_ORD = re.compile(r"\b(?:sak|saksnummer|byggesak)\s*$", re.IGNORECASE)
# Fanger hele tallkjeden, ikke bare de to første, fordi noen titler skriver
# kommunenummeret først i kjeden (f.eks. "3205/13/143" = KOMMUNE_NR/gnr/bnr).
_TALLKJEDE = re.compile(r"\d+(?:/\d+){1,3}")


def _gnr_bnr_fra_tallkjede(tallkjede):
    """Tolker en tallkjede "A/B[/C[/D]]" som gnr/bnr, og hopper over et
    ledende KOMMUNE_NR hvis kjeden starter med det. Returnerer None hvis det
    ikke er noe fullt gnr/bnr-par igjen etter et slikt hopp (f.eks.
    "3205/472" - bare kommunenr+gnr, ingen bnr)."""
    nums = tallkjede.split("/")
    if nums[0] == str(KOMMUNE_NR):
        if len(nums) < 3:
            return None
        return nums[1], nums[2]
    return nums[0], nums[1]


def ekstra_matrikkel_annet_sted_i_tittel(tittel):
    """Finn et helt separat matrikkelnummer et annet sted i tittelen - ikke
    rett etter det første (se extra_gnr_bnr_fra_tittel), men fritt-stående
    lenger ute i teksten, f.eks. "Borøya - 425/2 - Strand innerst i
    Vesterviga 52/2/0/0, inngrep i strandsonen" (425/2 og 52/2 er to ulike
    matrikler i samme sak, ikke en liste). Ekskluderer saksreferansenumre
    (_SAKSREF_PREFIX) og 0/0-plassholderen. Ren tillegging - kalles alltid
    fra parse_case sammen med extra_gnr_bnr_fra_tittel."""
    if not tittel:
        return []
    pairs = []
    for m in _TALLKJEDE.finditer(tittel):
        foran = tittel[:m.start()]
        if _SAKSREF_PREFIX.search(foran) or _SAKSREF_ORD.search(foran):
            continue
        tolket = _gnr_bnr_fra_tallkjede(m.group(0))
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
# Kun de fire sakstypene vi faktisk søker på (se SAKSTYPER i app/main.py) -
# en sak med et annet prefiks blir uansett kastet ut av
# forkast_uventet_sakstype() før denne verdien får noen betydning.
SAKSTYPE_FRA_PREFIKS = {
    "BYGG": "Byggesak",
    "HENV": "Henvendelse",
    "ULOV": "Ulovlighetssak",
    "TILSYN": "Tilsynssak",
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
    Returnerer (None, teksten) hvis det ikke finnes noe saksnummer."""
    text = (text or "").strip()
    if " - " in text:
        nr, rest = text.split(" - ", 1)
        return nr.strip(), rest.strip()
    return None, text


# --- Henting og parsing av saker - felles for begge kjøremåtene (engangs
# historisk dump og daglig endringslogg) ---
def get_case_list(saksnummer_prefiks):
    """Henter lista over alle saker (document_id-er) for én sakstype. Vi
    bruker "?q=<PREFIKS>-" (søk på saksnummer-prefiks) og ikke
    "?casetypeid=..." - sistnevnte viser stille bare "siste to måneder" som
    standardfilter. "q="-søket viser alle år/sakstype som standard, og har
    ingen paginering å bry seg om."""
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


def _aside_section_fields(aside, header_text):
    """Henter felt-verdier (label -> tekst) fra den delen av .caseDetailsAside
    hvis <h3>-overskrift matcher header_text - brukes for "Saksdetaljer"
    (inneholder Saksbehandler). Den seksjonen bruker <div class="detailHeader">
    /<div class="detailContent"> uten "document"-prefiks, en annen DOM-
    struktur enn adresse-listen, så den ble tidligere aldri lest av
    parse_case() selv om den ligger i samme aside."""
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
    """Trekker ut all detaljdata fra en saks-side."""
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
            t = LOPENUMMER_RE.sub("", t)
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

    # Hentes alltid, ikke bare når panelet er tomt - brukes som fallback når
    # panelet er helt tomt, og som tillegg når det er delvis tomt (se under),
    # men aldri som overstyring av det panelet faktisk ga.
    fallback_adresse, fallback_gnr_bnr = address_from_tittel(sakstittel, KOMMUNE_NR)

    if not adresse_clean and fallback_adresse:
        adresse_clean = list(fallback_adresse)
    elif adresse_clean and fallback_adresse:
        # Tittelen kan liste flere husnumre for samme gate enn det adresse-
        # panelet faktisk viser ("Vardåsveien 94 og 96 58/124/0/0, ..." der
        # panelet bare ga "Vardåsveien 94") - vi fyller på, men bare for
        # gater panelet allerede har bekreftet, slik at en urelatert
        # tittel-gjetning ikke blandes inn når panelet har full/korrekt data
        # for en annen gate.
        kjente_gater = {g for g in (_gatenavn(a) for a in adresse_clean) if g}
        for cand in fallback_adresse:
            if cand not in adresse_clean and _gatenavn(cand) in kjente_gater:
                adresse_clean.append(cand)

    if not gnr_bnr_clean and fallback_gnr_bnr:
        gnr_bnr_clean = [fallback_gnr_bnr]

    # Saken kan gjelde flere eiendommer samtidig - enten som en liste rett
    # etter det første matrikkelnummeret ("244/30/0/0 og 244/70, ..." eller
    # "426/55, 50, 47, 71, 54 - ...", se extra_gnr_bnr_fra_tittel), eller som
    # et helt separat matrikkelnummer et annet sted i tittelen ("Borøya -
    # 425/2 - Strand innerst i Vesterviga 52/2/0/0, ...", se
    # ekstra_matrikkel_annet_sted_i_tittel). Vi fanger opp begge, uansett
    # hvor adresse/gnr_bnr for øvrig kom fra.
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
            time.sleep(1.5 * (attempt + 1))
    # Alle forsøk feilet - returner saken med feilmelding i stedet for å stoppe kjøringen
    return {"document_id": str(document_id), "url": url, "error": str(last_err)}


def i_dag_oslo():
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def forkast_uventet_sakstype(records, saksnummer_prefiks):
    """"q="-søket i get_case_list er et fritekstsøk, ikke en garantert
    sakstype-filtrering, så vi forkaster enhver sak som mot formodning har
    et annet saksnummer-prefiks enn forventet. Returnerer (ok, forkastet)."""
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


# --- Små JSON-hjelpere, delt av begge kjøremåtene under ---
def _read_json_or_default(path, default):
    """Leser og parser en JSON-fil, og returnerer `default` hvis fila ikke
    finnes eller ikke lar seg parse (f.eks. avbrutt fra en tidligere kjøring)."""
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return default


def _atomic_write_json(data, path, **dump_kwargs):
    """Skriver `data` som JSON til `path` via en midlertidig fil, så vi
    unngår halvskrevet output hvis noe blir avbrutt underveis."""
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, **dump_kwargs)
    tmp.replace(path)


def _hent_saksliste(saksnummer_prefiks, limit=None):
    """Henter sakslista for én sakstype, og begrenser til de N nyeste hvis
    limit er satt (lista fra get_case_list() er sortert nyeste først)."""
    ids = get_case_list(saksnummer_prefiks)
    if limit:
        ids = ids[:limit]
    return ids


# --- Engangs historisk dump (2020-today_dump) - gjenopptakbar kjøring ---
def load_done(output_file):
    """Leser eksisterende output slik at kjøringen kan gjenopptas."""
    data = _read_json_or_default(output_file, [])
    # Bare saker som ble ferdig og uten feil regnes som gjort; resten hentes på nytt
    return {d["document_id"]: d for d in data if "dokumenter" in d and "error" not in d}


def save_resultater(results, output_file):
    """Skriver resultatene til disk via en midlertidig fil for å unngå halvskrevet output."""
    _atomic_write_json(list(results.values()), output_file, indent=2)


def run_full_dump(output_file, saksnummer_prefiks, limit=None, save_every=200):
    """Full historisk dump av én sakstype (brukes av 2020-today_dump/*/main.py).

    limit=N henter kun de N nyeste sakene (lista fra get_case_list() er
    sortert nyeste først) - kjekt for raske delkjøringer/testing uten å måtte
    kjøre gjennom hele den tunge full-dumpen hver gang."""
    ids = _hent_saksliste(saksnummer_prefiks, limit)

    results = load_done(output_file)   # hopp over saker som allerede ligger i output
    todo = [i for i in ids if i not in results]
    print(f"{len(ids)} saker totalt" + (f" (limit={limit})" if limit else "") +
          f", {len(results)} allerede hentet, {len(todo)} gjenstår")

    session = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_case, session, i): i for i in todo}
        for fut in as_completed(futures):
            res = fut.result()
            results[res["document_id"]] = res
            done += 1
            if done % save_every == 0:
                save_resultater(results, output_file)
                print(f"  {done}/{len(todo)} hentet (lagret)")

    _, forkastet = forkast_uventet_sakstype(list(results.values()), saksnummer_prefiks)
    for r in forkastet:
        del results[r["document_id"]]

    save_resultater(results, output_file)
    errors = sum(1 for r in results.values() if "error" in r)
    print(f"Ferdig: {len(results)} saker skrevet til {output_file} ({errors} feilet)")

    if limit is None:   # ikke last opp delkjøringer/lokal test til Azure
        upload_full_dump_til_azure(list(results.values()), saksnummer_prefiks)


# --- Daglig endringslogg (running_daily) - full sveip + snapshot-diff ---
def full_sweep(saksnummer_prefiks, limit=None):
    """Henter alle sakene av én sakstype på nytt (full skanning).

    limit=N henter kun de N nyeste sakene (lista er sortert nyeste først),
    nyttig for rask testing - i produksjon kjøres den uten limit. "Siste to
    måneder"-snarveien i get_case_list er basert på når saken ble OPPRETTET,
    ikke sist oppdatert, så vi må uansett sveipe alle sakene hver gang for å
    fange opp nye dokumenter på eldre, fortsatt aktive saker."""
    ids = _hent_saksliste(saksnummer_prefiks, limit)
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
    return _read_json_or_default(snapshot_file, {})


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


def diff(results, prev):
    """Sammenligner dagens sweep mot gårsdagens snapshot.

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


# --- Snapshot-state i Azure Blob - kilden til sannhet for running_daily ---
#
# Jobben kjører i Databricks, trolig på compute uten en persistent lokal
# disk mellom kjøringer, så snapshot.json på lokal disk kan ikke være det
# eneste stedet gårsdagens saksliste ligger. Den leses/skrives derfor også
# (og fremover primært) fra/til Azure Blob, slik at et nytt kompute-miljø
# som plukker opp morgendagens kjøring likevel finner riktig utgangspunkt.
# Den lokale snapshot-fila er bare en best-effort fallback/cache.
def _azure_state_blob_name(sakstype):
    slug = _SAKSTYPE_SLUG.get(sakstype, sakstype.lower())
    return f"{AZURE_BASE_PATH}/_state/{slug}_snapshot.json"


def load_snapshot_azure(sakstype):
    """Leser siste snapshot fra Azure Blob. Returnerer None (ikke {}) hvis
    det ikke finnes eller ikke lar seg lese - kallende kode bruker det til å
    falle tilbake til lokal fil, i motsetning til load_snapshot() sin {}
    (som betyr "første kjøring, ingen tidligere data")."""
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
    kjente tilstand, ikke en datopartisjonert historikk). Feiler aldri hele
    kjøringen, samme mønster som upload_til_azure()."""
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


_SAKSTYPE_SLUG = {
    "Byggesak": "bygg",
    "Henvendelse": "henv",
    "Ulovlighetssak": "ulov",
    "Tilsynssak": "tilsyn",
}


def _azure_credential():
    """Finner en Azure Blob-credential, i denne rekkefølgen:
      1) Databricks secrets (scope "postlister"), hvis vi kjører der.
      2) Service Principal via miljøvariabler, lastet fra bronze/postlister/
         .env (delt for hele "postlister"-containeren, ikke kommune-spesifikt).
      3) Egen Azure CLI-innlogging (`az login`) via AzureCliCredential.
    Kaster ValueError/RuntimeError med en tydelig melding hvis ingen virker -
    upload_til_azure()/upload_full_dump_til_azure() fanger den og hopper over
    opplastingen i stedet for å krasje hele den lokale skrape-kjøringen."""
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
            # .env ligger på bronze/postlister/-nivå - let oppover til vi
            # finner mappen som faktisk heter "postlister".
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
    """Serialiserer en liste med poster til JSONL (én post per linje),
    gzip-komprimerer, og laster opp som én blob (overskriver hvis den
    allerede finnes - trygt å kjøre samme dato på nytt)."""
    from azure.storage.blob import BlobServiceClient

    jsonl = "\n".join(json.dumps(p, ensure_ascii=False) for p in poster)
    komprimert = gzip.compress(jsonl.encode("utf-8"))

    client = BlobServiceClient(AZURE_ACCOUNT_URL, credential=credential,
                                connection_timeout=TIMEOUT, read_timeout=TIMEOUT)
    blob = client.get_container_client(AZURE_CONTAINER_NAME).get_blob_client(blob_name)
    blob.upload_blob(komprimert, overwrite=True)
    return len(komprimert)


def _fallback_lagre_midlertidig(poster, filnavn):
    """Skriver en liste med poster til systemets midlertidige mappe (ikke i
    repoet, og ikke et sted som vokser ubegrenset) - brukes bare som
    nødløsning når selve Azure-opplastingen feiler, slik at dagens data ikke
    går helt tapt. Stien logges tydelig slik at filen kan følges opp manuelt."""
    path = Path(tempfile.gettempdir()) / f"{filnavn}.json"
    try:
        path.write_text(json.dumps(poster, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  [Azure] lagret midlertidig lokalt i stedet (IKKE i repoet): {path}")
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KLARTE IKKE Å lagre fallback-kopi heller ({e}) - "
              f"disse {len(poster)} postene er tapt for denne kjøringen")


def upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype):
    """Laster dagens endringslogg (nye saker + nye journalposter på gamle
    saker) direkte til Azure Blob som JSONL+gzip - ingen lokal fil skrives i
    repoet (se write_changelog). Jobben kjører evig hver dag på tvers av
    fire sakstyper, og et filpar per dag/sakstype ville bare vokst.

    Stikonvensjon: bronze/lillestrom/load_type=incremental/date=<ref_date>/
    <slug>_saker-<ref_date>.jsonl.gz (+ <slug>_journalposter-<ref_date>.jsonl.gz).

    Feiler aldri hele kjøringen ved Azure-problemer - i stedet havner en
    midlertidig fallback-kopi utenfor repoet (se _fallback_lagre_midlertidig).

    Returnerer True bare hvis alt som fantes å laste opp faktisk kom seg til
    Azure, ellers False. run_daily() bruker denne verdien til å avgjøre om
    snapshotet er trygt å avansere - se run_daily-docstringen."""
    slug = _SAKSTYPE_SLUG.get(sakstype, sakstype.lower())
    try:
        credential = _azure_credential()
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KUNNE IKKE laste opp - {e}")
        for filnavn, poster in ((f"{slug}_saker-{ref_date}", nye_saker),
                                 (f"{slug}_journalposter-{ref_date}", nye_journalposter)):
            if poster:
                _fallback_lagre_midlertidig(poster, filnavn)
        return False

    prefix = f"{AZURE_BASE_PATH}/load_type=incremental/date={ref_date}"
    alle_ok = True
    for filnavn, poster in ((f"{slug}_saker", nye_saker), (f"{slug}_journalposter", nye_journalposter)):
        if not poster:
            continue
        blob_name = f"{prefix}/{filnavn}-{ref_date}.jsonl.gz"
        try:
            storrelse = _last_opp_jsonl_gz(poster, blob_name, credential)
            print(f"  [Azure] lastet opp {len(poster)} poster -> "
                  f"{AZURE_CONTAINER_NAME}/{blob_name} ({storrelse / 1024:.1f} KB)")
        except Exception as e:  # noqa: BLE001
            print(f"  [Azure] opplasting FEILET for {blob_name}: {e}")
            _fallback_lagre_midlertidig(poster, f"{filnavn}-{ref_date}")
            alle_ok = False
    return alle_ok


def upload_full_dump_til_azure(poster, saksnummer_prefiks):
    """Laster hele engangs-historikk-dumpen (all-i-en-fil fra run_full_dump())
    opp til Azure Blob som JSONL+gzip - én blob for hele sakstypen.

    Stikonvensjon: bronze/lillestrom/load_type=full/<slug>/dump_date=<i_dag>/
    <slug>_saker-full-<i_dag>.jsonl.gz - partisjonert på KJØREDATO (når
    dumpen faktisk ble tatt), ikke sakens egen dato, siden "q="-søket gir
    alle saker i ett kall uten paginering/datofilter, og sakene har ikke noe
    brukbart per-sak datofelt å partisjonere på (bare årstall i saksnummeret).

    Feiler aldri (samme mønster som upload_til_azure) - den lokale filen fra
    run_full_dump() er uansett alltid den autoritative kopien."""
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


def write_changelog(nye_saker, nye_journalposter, ref_date, sakstype):
    """Laster dagens endringslogg til Azure - se upload_til_azure(). Skriver
    bevisst ingen lokal fil i repoet (jobben kjører evig, en varig lokal
    kopi per dag ville bare vokst). Ved en mislykket opplasting havner en
    fallback-kopi i systemets midlertidige mappe i stedet, aldri i repoet.

    Returnerer upload_til_azure() sin suksess-status videre til run_daily(),
    som bruker den til å avgjøre om snapshotet er trygt å avansere."""
    print(f"{len(nye_saker)} nye saker, {len(nye_journalposter)} saker med nye journalposter i dag")
    return upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype)


# --- Daglig endringslogg (running_daily) - inngangspunkt ---
def run_daily(snapshot_file, saksnummer_prefiks, sakstype, limit=None, today_fn=i_dag_oslo):
    """Daglig endringslogg for én sakstype (brukes av running_daily/app/main.py).

    `today_fn` er injiserbar i stedet for å kalle datetime.now(...) direkte,
    slik at tester kan simulere flere påfølgende "dager" uten å vente på ekte
    klokketid.

    To ting å være obs på her, begge lagt inn etter at vi fant dem i en
    sårbarhetsgjennomgang: `limit` er kun til rask lokal testing og skal
    aldri røre Azure eller snapshotet (en delkjøring med f.eks. limit=30
    ville ellers korrumpert produksjonssnapshotet med bare 30 saker) - se
    testkjørings-sjekkene under. Og snapshotet avanseres kun hvis dagens
    endringslogg faktisk kom seg helt til Azure - ellers ville en mislykket
    opplasting stille miste dagens nye saker for godt, siden snapshotet da
    ville "husket" dem som kjente uten at de noensinne kom til Azure."""
    ref_date = today_fn().isoformat()
    # Azure er kilden til sannhet - lokal fil er bare en fallback/cache hvis
    # Azure ikke er tilgjengelig/satt opp.
    prev = load_snapshot_azure(sakstype)
    if prev is None:
        prev = load_snapshot(snapshot_file)

    results = full_sweep(saksnummer_prefiks, limit=limit)
    snapshot = build_snapshot(results)
    feilet = sum(1 for r in results if "error" in r)

    if not prev:
        # Første kjøring: ingen snapshot å diffe mot. Etabler baseline uten å
        # dumpe alle sakene som "nye" - neste kjøring gir en ekte endringslogg.
        if limit is not None:
            print(f"  [Testkjøring] limit={limit} satt - hopper over å lagre "
                  f"baseline-snapshot (ville korrumpert produksjonssnapshotet "
                  f"med kun {len(snapshot)} saker).")
            return
        save_snapshot(snapshot, snapshot_file)
        save_snapshot_azure(snapshot, sakstype)
        print(f"Baseline etablert: {len(snapshot)} saker lagret i snapshot "
              f"({feilet} feilet). Ingen endringslogg skrevet på første kjøring.")
        return

    nye_saker, nye_journalposter = diff(results, prev)

    if limit is not None:
        # Testkjøring - vis hva som ville blitt lastet opp, men rør verken
        # Azure eller snapshotet.
        print(f"  [Testkjøring] limit={limit} satt - fant {len(nye_saker)} nye saker, "
              f"{len(nye_journalposter)} saker med nye journalposter, men hopper over "
              f"Azure-opplasting og snapshot-lagring.")
        return

    changelog_ok = write_changelog(nye_saker, nye_journalposter, ref_date, sakstype)
    if changelog_ok:
        save_snapshot(snapshot, snapshot_file)
        save_snapshot_azure(snapshot, sakstype)
        print(f"Ferdig ({feilet} saker feilet under henting).")
    else:
        print(f"  [Snapshot] IKKE avansert - endringsloggen kom seg ikke helt til Azure. "
              f"Snapshotet forblir uendret slik at dagens nye saker/journalposter blir "
              f"oppdaget og forsøkt lastet opp på nytt ved neste kjøring i stedet for å gå tapt.")
