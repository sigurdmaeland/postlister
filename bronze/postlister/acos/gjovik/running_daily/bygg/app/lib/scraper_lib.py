"""Gjøvik - ACOS "nye-innsyn" (samme plattform som Moss/Sandefjord/Tønsberg
i dette prosjektet, egen selvhostet instans på www.gjovik.kommune.no). Samme
seksjonsoppdeling som de andre ACOS-kommunene: konstanter -> adresseparsing
-> KILDER -> API-kall -> engangsdump -> daglig endringslogg.

Gjøvik har KUN én relevant sakstype (Byggesak, KILDER-nøkkel "bygg"), ingen
"Datasource"-nøkkel (én enkelt arkiv-instans) og ingen MenypunktID-header
(til forskjell fra Moss/Sandefjord/Tønsberg). Fildokument-endepunktet er
også POST i stedet for GET, med en annen responsform (se
fetch_dokument_filer) - dette er en reell API-forskjell, ikke bare stil.
Arkivet går tilbake til minst januar 2017, ingen "siste 2 måneder"-
begrensning som på 360online-plattformene.

ADRESSE/GNR-BNR-PARSING: sakstittelen starter (nesten) alltid med
matrikkelnummeret "GNR/BNR[/FESTENR[/SEKSJONSNR]]", men skilletegnet til
adressen som følger varierer (bindestrek, komma, eller INGEN skilletegn),
f.eks. "61/60 - H. B. Falks gate 1, Gjøvik - Renovering av bolig",
"73/1/1 Myrvegen 10, Hunndalen - Tilbygg til bolig". Adressen har ofte et
tettstedsnavn hengt på etter komma/punktum som fjernes ("Myrvegen 10,
Hunndalen" -> "Myrvegen 10"). "0/0" brukes som placeholder for "intet
spesifikt gnr/bnr" og hoppes over. Et lite mindretall titler mangler
matrikkelnr helt -> adresse=None, gnr_bnr=None. Verifisert ved stikkprøve av
861 titler på tvers av hele arkivet.
"""

import gzip
import json
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://www.gjovik.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"

PORTAL_ID = "2"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 3407   # Gjøvik (uendret siden Vardal-sammenslåingen 01.01.2020)
KOMMUNE = "Gjøvik"

MAX_WORKERS = 8
TIMEOUT = 30
RETRIES = 3

AZURE_ACCOUNT_URL = "https://storaggen2eaccountprod.blob.core.windows.net"
AZURE_CONTAINER_NAME = "postlister"
AZURE_BASE_PATH = "bronze/gjovik"

# Unity Catalog Volume - brukes som landingssone i stedet for Azure Blob når
# koden kjører INNE i Databricks og volumet finnes (se _har_databricks_volume).
# Matcher "Volume auto-detect"-mønsteret i den delte silver-pipelinen.
DATABRICKS_VOLUME_PATH = Path("/Volumes/postlister/bronze/raw/gjovik")

HEADERS = {
    "Content-Type": "application/json",
    "PortalID": PORTAL_ID,
    "SprakID": "1",
    "X-ANTI-CSRF": "1",
}


# --------------------------------------------------------------------------- #
# Adresse/gnr_bnr/matrikkelnr fra sakstittel
# --------------------------------------------------------------------------- #
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
# Tettstedsnavn hengt på etter husnummeret droppes fra adressefeltet -
# kommunen er uansett kjent (kommune-feltet). Halen kan være kommaseparert
# ("Rognstadvegen 14, 2827 Hunndalen", ev. flere ledd:
# "Dalborgvegen 314, Kløverstua, Gjøvik") eller skilt med punktum
# ("Viflatvegen 39. Bybrua") - postnummer foran stedsnavnet forekommer også.
_POSTNR = re.compile(r"^\d{4}\s+")

_MATRIKKEL_TOKEN = r"\d+\s*/\s*\d+(?:\s*/\s*\d+(?:\s*/\s*\d+)?)?"
_MATRIKKEL_TOKEN_RE = re.compile(r"(\d+)\s*/\s*(\d+)(?:\s*/\s*(\d+)(?:\s*/\s*(\d+))?)?")
_MATRIKKEL_SEGMENT_RE = re.compile(
    rf"^{_MATRIKKEL_TOKEN}(?:\s*(?:og|,)\s*{_MATRIKKEL_TOKEN})*"
)
# Skilletegnet mellom matrikkelnr og adresse varierer (se modul-docstring).
_SEP_RE = re.compile(r"^\s*(?:[-,]\s*)?")

# Adressekandidaten kan selv romme flere husnumre på samme gate ("Parkgata 6,
# 8 og 10", "Bassengvegen 14 og 14A") - eller mer sjeldent flere ulike gater
# ("Bjørnsonsgate 1 og Kirkegata 4"). _splitt_multiadresse deler dette opp i
# enkelt-adresser (samme algoritme som brukt for tilsvarende oppramsinger i
# Bærums scraper_lib.py, portert hit siden Gjøvik ikke har noe adressepanel å
# hente en ferdig splittet liste fra - adressen kommer alltid fra tittelen).
_HUSNUMMER_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))*"
_BART_HUSNUMMER = re.compile(rf"^{_HUSNUMMER_TOKEN}$")
_BAR_BOKSTAV = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_GATE_OG_HUSNUMMER = re.compile(rf"^(.+?)\s+({_HUSNUMMER_TOKEN})$")
_MULTIADRESSE_SPLIT = re.compile(r"\s*(,|\bog\b|\beller\b)\s*")
_STARTER_STOR_BOKSTAV = re.compile(r"^[A-ZÆØÅ]")


def _normalize_nummer_bokstav(s):
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


_HAR_HUSNUMMER = re.compile(r"\s\d+[A-Za-zæøåÆØÅ]?(?:-\w+)*$")


def _har_husnummer(addr):
    """Sjekker om adressekandidaten faktisk har et ekte husnummer til slutt -
    uten det er kandidaten som regel bare et gate-/stedsnavn uten egen
    adresse."""
    return bool(_HAR_HUSNUMMER.search(" " + addr))


_GLUED_HUSNUMMER_RE = re.compile(r"(?<=[a-zæøå])(\d)")


def _normalize_glued_husnummer(s):
    """Setter inn mellomrom der et gatenavn og husnummeret er skrevet uten
    mellomrom, f.eks. "Ekornstien13" -> "Ekornstien 13". Trigger kun etter en
    LITEN bokstav - sonekoder som "BKS2"/"BFS3" (som alltid er versaler rett
    før tallet) er ikke ekte husnummer og skal forbli uendret."""
    return _GLUED_HUSNUMMER_RE.sub(r" \1", s)


_PAREN_ETTER_HUSNUMMER = re.compile(
    rf"(\s{_HUSNUMMER_TOKEN})\s*\([^()]*\)"
)


def _strip_paren_etter_husnummer(candidate):
    """Fjerner en parentes rett etter husnummeret - typisk et kallenavn,
    duplikat-nummer eller henvisning til gammel adresse/matrikkel, f.eks.
    "Kollshaugen 26 (Kollsvegen)" -> "Kollshaugen 26", "Strandvegen 4
    (tidl. Østre Totenveg 14)" -> "Strandvegen 4". Selve husnummeret
    beholdes."""
    return _PAREN_ETTER_HUSNUMMER.sub(r"\1", candidate)


_MFL_HALE = re.compile(rf"(\s{_HUSNUMMER_TOKEN})\s+m\.?\s*fl\.?(?=[\s,]|$)", re.IGNORECASE)


def _strip_mfl(candidate):
    """Fjerner en "m.fl."/"m. fl."-hale ("med flere") rett etter
    husnummeret."""
    return _MFL_HALE.sub(r"\1", candidate)


def _strip_periode_sted(candidate):
    """Stripper en enkelt ". Stedsnavn"-hale helt bakerst, men bare når
    punktumet kommer rett etter et tall (husnummeret), f.eks. "Viflatvegen
    39. Bybrua" -> "Viflatvegen 39". IKKE etter en enkeltbokstav, for å
    unngå å knuse forkortelser midt i gatenavn som "H. B. Falks gate 1"."""
    m = re.search(r"(\d)\.\s+[A-ZÆØÅ][\wæøåÆØÅ]*\s*$", candidate)
    if m:
        return candidate[:m.start(1) + 1]
    return candidate


def _strip_sted_hale(candidate):
    """Fjerner stedsnavn-hale(r) bakerst i adressekandidaten. Gjøvik-titler
    har ofte ett eller flere kommaseparerte steds-/områdenavn hengt på etter
    selve gateadressen, av og til med postnummer foran, f.eks.
    "Rognstadvegen 14, 2827 Hunndalen" eller "Dalborgvegen 314, Kløverstua,
    Gjøvik". Stripper kommadelene bakfra så lenge de IKKE selv ser ut som en
    fortsettelse av en adresseliste (bart tall, bar bokstav, eller et nytt
    "Gatenavn Nummer") - stopper med det samme et ekte adresseledd dukker
    opp, slik at "Bondelia hage 7, 9, 11, Gjøvik" beholder alle tre
    husnumrene og bare mister "Gjøvik"."""
    parts = [p.strip() for p in candidate.split(",")]
    while len(parts) > 1:
        uten_postnr = _POSTNR.sub("", parts[-1])
        if (_BART_HUSNUMMER.match(uten_postnr) or _BAR_BOKSTAV.match(uten_postnr)
                or _GATE_OG_HUSNUMMER.match(uten_postnr)):
            break
        parts.pop()
    return ", ".join(parts)


_STED_UTEN_KOMMA_RE = re.compile(
    rf"^(.+\s{_HUSNUMMER_TOKEN})\s+[A-ZÆØÅ][\wæøåÆØÅ]*\s*$"
)
# Vegreferanser ("Fv 2416 Ytterrovegen") har tallet FØR gatenavnet, ikke
# etter - samme overflatemønster som en ekte adresse, men motsatt retning.
_VEGREFERANSE_PREFIX = re.compile(r"^(?:Fv\.?|Rv\.?|E\d+)\s", re.IGNORECASE)
# Hvis "stedsnavnet" vi vurderer å stryke selv ender på et gatenavn-suffiks,
# er det mest sannsynlig den EKTE gata (og tallet foran hører til noe annet,
# f.eks. et butikknavn som "Rema 1000") - ikke strip da.
_GATENAVN_SUFFIKS = re.compile(
    r"(gate|gata|veg|vegen|vei|veien|gutu|stien|sving|svingen)$", re.IGNORECASE
)


def _strip_ukommatert_sted(candidate):
    """Stripper et enkelt stedsnavn helt bakerst når det IKKE er skilt med
    komma eller punktum, bare mellomrom, rett etter et ekte husnummer - f.eks.
    "Ibsens gate 13A Gjøvik" -> "Ibsens gate 13A". Krever at det som står
    foran allerede ser ut som en full gateadresse (gatenavn + husnummer), for
    å unngå å kutte i selve gatenavnet - virker derfor aldri når det er komma
    foran stedsnavnet (det fanges av _strip_sted_hale i stedet). Skipper
    vegreferanser og tilfeller der halen selv ser ut som et gatenavn."""
    if _VEGREFERANSE_PREFIX.match(candidate):
        return candidate
    m = _STED_UTEN_KOMMA_RE.match(candidate)
    if not m:
        return candidate
    hale = candidate[m.end(1):].strip()
    if _GATENAVN_SUFFIKS.search(hale):
        return candidate
    return m.group(1).strip()


_GATE_SLASH_GATE = re.compile(rf"({_HUSNUMMER_TOKEN})\s*/\s*")


def _normaliser_gate_slash(candidate):
    """Konverterer "NUMMER/..." til "NUMMER, ..." når skråstreken kommer
    RETT ETTER et husnummer - typisk to ulike gater ramset opp med
    skråstrek ("Ametystvegen 20/Engehaugen 36") eller flere
    bokstavsuffikser på samme husnummer ("Toppvegen 12A/B"). Skråstrek
    ELLERS i teksten (f.eks. i et sammensatt gatenavn som
    "Røverdalen/Heimdals gate") røres ikke - da står det ikke noe tall
    rett før skråstreken."""
    return _GATE_SLASH_GATE.sub(r"\1, ", candidate)


def _splitt_multiadresse(candidate):
    """Del en renset adressekandidat opp i enkelt-adresser der flere
    husnumre/gater er ramset opp med komma og/eller " og " (se kommentar over
    konstantene). Et bart tall/bokstav-element arver gatenavnet (og evt. siste
    husnummer, for bokstav-suffikser) fra forrige fulle element. Gir tilbake
    kandidaten uendret (som ett element) hvis oppramsingen ikke følger et
    gjenkjennelig mønster - vi gjetter aldri, vi bare unngår å la en gyldig
    enkel adresse gå tapt."""
    entries = []
    current_street, last_number = None, None
    tokens = _MULTIADRESSE_SPLIT.split(candidate)
    parts = [(None, tokens[0])] + list(zip(tokens[1::2], tokens[2::2]))
    for sep, part in parts:
        part = part.strip()
        if not part:
            continue
        if _BAR_BOKSTAV.match(part):
            if current_street is None or last_number is None:
                return [candidate]
            entries.append(f"{current_street} {last_number}{part}")
            continue
        if _BART_HUSNUMMER.match(part):
            if current_street is None:
                return [candidate]
            entries.append(f"{current_street} {part}")
            m = re.match(r"^(\d+)", part)
            if m:
                last_number = m.group(1)
            continue
        sm = _GATE_OG_HUSNUMMER.match(part)
        if sm and (sep is None or _STARTER_STOR_BOKSTAV.match(sm.group(1).strip())):
            current_street = sm.group(1).strip()
            entries.append(f"{current_street} {sm.group(2)}")
            m = re.match(r"^(\d+)", sm.group(2))
            if m:
                last_number = m.group(1)
            continue
        return [candidate]
    seen, uniq = set(), []
    for e in entries:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq or [candidate]


_IKKE_GATENAVN = {
    "etappe", "kvartal", "trinn", "del", "fase", "byggetrinn",
    "gatenummer", "vegnummer", "veinummer",
    "fylkesveg", "fylkesvei", "riksveg", "riksvei",
    "europaveg", "europavei", "kommunalveg", "kommunalvei",
}


def _er_falsk_gatenavn(entry):
    """Sant hvis "gatenavnet" i en splittet adresseoppføring egentlig er en
    kjent ikke-adresse-referanse (byggetrinn, kvartalsnummer, intern
    veinummerering, fylkes-/riksvegreferanse) - f.eks. "Etappe 2",
    "Kvartal 25", "Gatenummer 5140", "Fv 2404" - ikke en reell gateadresse."""
    m = _GATE_OG_HUSNUMMER.match(entry)
    if not m:
        return False
    gatenavn = m.group(1).strip()
    return bool(gatenavn.lower() in _IKKE_GATENAVN
                or _VEGREFERANSE_PREFIX.match(gatenavn + " "))


_SMAABOKSTAV_ORD = re.compile(r"^[a-zæøå]+\.?$")
_BESKRIVELSE_PREPOSISJONER = {
    "av", "ved", "om", "for", "til", "fra", "på", "i", "med",
    "etter", "mot", "gjennom", "over", "under", "mellom",
}
_FORSTE_TALL_RE = re.compile(r"\d")


def _trim_beskrivelse_prefiks(tekst):
    """Fjerner en eventuell beskrivende tekst-prefiks foran selve adressen,
    avgrenset av siste småbokstav-preposisjon ("av", "ved", "om" osv.) FØR
    det første tallet i teksten. Gatenavn og kvalifiserende ord (Østre,
    Niels, Ødegaards osv.) skrives alltid med stor forbokstav på norsk, så
    en preposisjon rett før adressen skiller pålitelig beskrivelse fra
    selve adressen, f.eks. "Gammel meieripipe ved Snertingdalsvegen 2156"
    -> "Snertingdalsvegen 2156". Søket stopper FØR første tall slik at en
    preposisjon inne i en beskrivelse ETTER adressen (kan skje når tittelen
    mangler riktig " - "-skille) ikke feilaktig kutter bort selve adressen.
    "og"/"eller" er bevisst IKKE med i preposisjonslista - de brukes til å
    ramse opp flere adresser og skal ikke trigge kutting."""
    ord = tekst.split(" ")
    grense = len(ord)
    for i, w in enumerate(ord):
        if _FORSTE_TALL_RE.search(w):
            grense = i
            break
    siste = -1
    for i in range(grense):
        w = ord[i]
        if _SMAABOKSTAV_ORD.match(w) and w.rstrip(".").lower() in _BESKRIVELSE_PREPOSISJONER:
            siste = i
    if siste == -1:
        return tekst
    return " ".join(ord[siste + 1:])


def _flere_ord_enn_ett(entry):
    """Sant hvis gatenavn-delen av en splittet adresseoppføring består av
    MER ENN ett ord."""
    m = _GATE_OG_HUSNUMMER.match(entry)
    return bool(m and len(m.group(1).strip().split()) > 1)


_MULTI_HYPHEN_HUSNUMMER = re.compile(
    r"^(.*?)\s+(\d+[A-Za-zæøåÆØÅ]?(?:-\d+[A-Za-zæøåÆØÅ]?){2,})$"
)


def _splitt_hyphen_kjede(entry):
    """Splitter en adresseoppføring med 3+ bindestrek-kjedede husnumre
    ("Torkelia 3-5-7-9") til separate adresser ("Torkelia 3", "Torkelia
    5", ...), i tråd med hvordan komma-/og-lister allerede splittes.
    Enkeltpar ("Odnesvegen 460-462") beholdes uendret som én
    intervall-streng - der er det for uklart om det er en rekkevidde eller
    to spesifikke husnumre."""
    m = _MULTI_HYPHEN_HUSNUMMER.match(entry)
    if not m:
        return [entry]
    gate = m.group(1)
    return [f"{gate} {t}" for t in m.group(2).split("-")]


def _adresse_fra_tekst(tekst):
    """Prøver å trekke ut en adresse fra en ren tekstbit (ett " - "-delt
    segment av tittelen). Returnerer None hvis det ikke finnes noe ekte
    husnummer i teksten. Hvis en beskrivende prefiks faktisk ble kuttet av
    _trim_beskrivelse_prefiks OG det som gjenstår foran husnummeret
    fortsatt er mer enn ett ord, er det for usikkert hvilket av ordene som
    er selve gatenavnet (f.eks. "Drivstoffanlegg Stampevegen") - da gir vi
    heller opp enn å gjette feil."""
    if not tekst or _INGEN_ADRESSE.match(tekst):
        return None
    trimmet = _trim_beskrivelse_prefiks(tekst)
    ble_trimmet = trimmet != tekst
    uten_paren = _strip_mfl(_strip_paren_etter_husnummer(_normalize_glued_husnummer(trimmet)))
    normalisert = _normalize_nummer_bokstav(uten_paren)
    uten_hale = _strip_sted_hale(_strip_periode_sted(normalisert))
    stripped = _strip_ukommatert_sted(uten_hale)
    entries = [e for e in _splitt_multiadresse(_normaliser_gate_slash(stripped))
               if _har_husnummer(e) and not e.strip().isdigit() and not _er_falsk_gatenavn(e)
               and not (ble_trimmet and _flere_ord_enn_ett(e))
               and any(c.isalpha() for c in e)]
    entries = [x for e in entries for x in _splitt_hyphen_kjede(e)]
    return "; ".join(entries) if entries else None


def parse_adresse_gjovik(tittel, subtitle=None):
    """Sakstittelen starter normalt med matrikkelnr, så adresse med
    varierende skilletegn (se modul-docstring). Noen titler mangler et
    gjenkjennelig matrikkelnr helt i starten - da prøves resten av tittelen
    likevel. Selve adressen kan også stå bak et mellomsegment vi ikke klarer
    å tolke (duplikat-matrikkelnr, gårdsnavn, sonekode, "m.fl." e.l.) - vi
    prøver derfor hvert " - "-delte segment i rekkefølge til ett av dem gir
    treff, i stedet for å blokklisteføre hvert enkelt søppel-mønster."""
    if not tittel:
        return None, None, None

    rest = tittel.strip()
    gnr_bnr_liste, matrikkel_deler = [], []
    seg_m = _MATRIKKEL_SEGMENT_RE.match(rest)
    if seg_m:
        segment = seg_m.group(0)
        rest = rest[seg_m.end():]
        sep_m = _SEP_RE.match(rest)
        if sep_m:
            rest = rest[sep_m.end():]
        for gnr, bnr, feste, seksjon in _MATRIKKEL_TOKEN_RE.findall(segment):
            if gnr == "0" and bnr == "0":
                continue   # placeholder for "intet spesifikt gnr/bnr"
            par = f"{gnr}/{bnr}"
            if par not in gnr_bnr_liste:
                gnr_bnr_liste.append(par)
            d = (f"{gnr}/{bnr}/{feste or '0'}/{seksjon or '0'}"
                 if (feste and feste != "0") or (seksjon and seksjon != "0") else par)
            if d not in matrikkel_deler:
                matrikkel_deler.append(d)

    adresse = None
    for seg in rest.split(" - "):
        adresse = _adresse_fra_tekst(seg.strip())
        if adresse:
            break

    matrikkelnr = ("; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel_deler)
                   if matrikkel_deler else None)
    return adresse, (gnr_bnr_liste or None), matrikkelnr


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        datasource=None,
        sakstype="at-ee0f97cc__bfb2__4549__9555__c2dcc5131630-BS!GXwgEc",
        adresse_parser=parse_adresse_gjovik,
        hent_filer=True,
        periode="2017-nå",
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(method, session, path, body=None, retries=RETRIES):
    """Delt retry/feilhåndtering for _post/_get under - samme HTTP-mønster
    (samme HEADERS/TIMEOUT, 1.5s*attempt backoff) uansett metode."""
    url = f"{API_BASE}/{path}"
    last_err = None
    for attempt in range(retries):
        try:
            r = session.request(method, url, json=body, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{method} {path} feilet etter {retries} forsøk") from last_err


def _post(session, path, body, retries=RETRIES):
    return _request("POST", session, path, body, retries)


def _get(session, path, retries=RETRIES):
    return _request("GET", session, path, retries=retries)


def get_case_list(kilde_key, session=None, max_items=None):
    """Henter identifier+saksnummer+dato for saker i én kilde, nyeste først.
    max_items=N stopper paginering tidlig - brukes kun når man ikke også
    dato-filtrerer (se run_full_dump)."""
    kilde = KILDER[kilde_key]
    session = session or requests.Session()

    def body_for(page):
        kv = [{"key": "Sakstype", "value": kilde["sakstype"]},
              {"key": "FilterContent", "value": "CaseOnly"},
              {"key": "pageSize", "value": str(PAGE_SIZE)},
              {"key": "page", "value": str(page)}]
        if kilde.get("datasource"):
            kv.insert(0, {"key": "Datasource", "value": kilde["datasource"]})
        return {"type": OVERVIEW_TYPE_SAK, "keyValues": kv}

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


def fetch_dokument_filer(session, dokument_identifier):
    """Henter fillista for ETT dokument. Til forskjell fra Moss/Sandefjord/
    Tønsberg er dette et POST-kall med en annen responsform
    ("content.b[FileList].b[]" i stedet for "content.model.vedleggGruppe[]")."""
    d = _post(session, f"dokument/{dokument_identifier}", None)
    body = d.get("content") or {}
    filer = []
    for blokk in body.get("b") or []:
        if blokk.get("type") != "FileList":
            continue
        kategori = blokk.get("title")
        for f in blokk.get("b") or []:
            filer.append({
                "navn": f.get("title"),
                "kategori": kategori,
                "fil_id": f.get("identifier"),
                "url": BASE_URL + f["url"] if f.get("url") else None,
            })
    return filer


def fetch_case(session, kilde_key, identifier, hent_filer=None):
    """Henter og parser én sak. `hent_filer=None` bruker kildens egen
    standardverdi (se KILDER over). Til forskjell fra Moss/Sandefjord/
    Tønsberg har hvert dokument også en "konfidensiell"-flagg (filer hentes
    ikke for disse). "saksbehandler" FINNES i props (bekreftet live -
    populeres når saken er tildelt), men ble tidligere feilaktig antatt
    fraværende og derfor aldri kopiert over i retur-dictet under."""
    kilde = KILDER[kilde_key]
    if hent_filer is None:
        hent_filer = kilde["hent_filer"]

    try:
        d = _get(session, f"sak/{identifier}")
        body = d["content"]["body"]
    except Exception as e:  # noqa: BLE001
        return {"document_id": identifier, "error": str(e)}

    sakstittel = body.get("title")
    saksnummer = _finn_metadata(body.get("b"), "FRIENDLY_ID")
    adresse, gnr_bnr_liste, matrikkelnr = kilde["adresse_parser"](sakstittel)

    dokumenter = []
    for blokk in body.get("b") or []:
        if blokk.get("type") != "DocumentList":
            continue
        for doc in blokk.get("b") or []:
            if doc.get("type") != "AsyncDocument":
                continue
            doc_id = doc.get("identifier")
            doc_blocks = doc.get("b")
            konfidensiell = _finn_metadata(doc_blocks, "DOCUMENT_CONFIDENTIAL") == "true"

            filer = []
            if hent_filer and not konfidensiell:
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
                "dokumenttype_kode": _finn_metadata(doc_blocks, "DOCUMENT_TYPE"),
                "konfidensiell": konfidensiell,
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
        "status": (body.get("props") or {}).get("status"),
        "dato": (body.get("props") or {}).get("dato"),
        "saksbehandler": (body.get("props") or {}).get("saksbehandler"),
        "url": f"{BASE_URL}/politikk-planer-og-organisasjon/postliste-dokumenter-og-vedtak/sok-i-postliste/#/details/{identifier}",
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
    """Skriv til .json.tmp og rename over target - unngår korrupt/halvskrevet
    fil hvis prosessen avbrytes midt i (brukes av save_resultater/save_state)."""
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
    dato_filter = bool(from_date or to_date)
    if dato_filter:
        items = get_case_list(kilde_key)
    else:
        items = get_case_list(kilde_key, max_items=limit)
    if dato_filter:
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
    periode = f" (fra_dato={from_date}, til_dato={to_date})" if dato_filter else ""
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


def _azure_credential():
    """Finner en Azure Blob-credential, i denne rekkefølgen:
      1) Databricks secrets (scope "postlister"), hvis vi kjører der.
      2) Service Principal via miljøvariabler, lastet fra bronze/postlister/
         .env (delt for hele "postlister"-containeren, ikke kommune-spesifikt).
      3) Egen Azure CLI-innlogging (`az login`) via AzureCliCredential.
    Kaster ValueError/RuntimeError med en tydelig melding hvis ingen virker -
    upload_til_azure() fanger den og hopper over opplastingen i stedet for å
    krasje hele den lokale skrape-kjøringen."""
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
    saker) til Azure Blob som JSONL+gzip.

    Stikonvensjon: bronze/gjovik/load_type=incremental/date=<ref_date>/
    saker-<ref_date>.jsonl.gz (+ journalposter-<ref_date>.jsonl.gz).

    Feiler aldri hele kjøringen ved Azure-problemer - i stedet havner en
    midlertidig fallback-kopi utenfor repoet (se _fallback_lagre_midlertidig).

    Returnerer True bare hvis alt som fantes å laste opp faktisk kom seg til
    Azure, ellers False. write_changelog()/run_daily() bruker denne verdien
    til å avgjøre om state.json er trygt å avansere."""
    try:
        credential = _azure_credential()
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KUNNE IKKE laste opp - {e}")
        for filnavn, poster in ((f"saker-{ref_date}", nye_saker),
                                 (f"journalposter-{ref_date}", nye_journalposter)):
            if poster:
                _fallback_lagre_midlertidig(poster, filnavn)
        return False

    prefix = f"{AZURE_BASE_PATH}/load_type=incremental/date={ref_date}"
    alle_ok = True
    for filnavn, poster in (("saker", nye_saker), ("journalposter", nye_journalposter)):
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


def _har_databricks_volume():
    """Sant hvis /Volumes finnes - dvs. koden kjører inne i Databricks (Unity
    Catalog-mount), ikke lokalt. Samme deteksjonsprinsipp som
    "Volume auto-detect" i den delte silver-pipelinen (get_bronze_data sjekker
    os.path.isdir('/Volumes/...'))."""
    return Path("/Volumes").is_dir()


def _skriv_til_volume(poster, filnavn, ref_date):
    """Skriver en liste med poster til Unity Catalog Volume-en
    (DATABRICKS_VOLUME_PATH) som JSONL+gzip - samme fil-/mappekonvensjon som
    upload_til_azure() bruker i blob, bare på et vanlig filsystempath (Volumes
    er FUSE-montert, så vanlig Python-filskriving fungerer uendret)."""
    dir_ = DATABRICKS_VOLUME_PATH / f"load_type=incremental/date={ref_date}"
    dir_.mkdir(parents=True, exist_ok=True)
    path = dir_ / f"{filnavn}-{ref_date}.jsonl.gz"
    jsonl = "\n".join(json.dumps(p, ensure_ascii=False) for p in poster)
    path.write_bytes(gzip.compress(jsonl.encode("utf-8")))
    return path


def _skriv_til_volume_changelog(nye_saker, nye_journalposter, ref_date):
    """write_changelog()-varianten for Volume-landingssone - se
    _skriv_til_volume(). Feiler aldri hele kjøringen ved skrivefeil (samme
    prinsipp som upload_til_azure()); returnerer suksess-status."""
    alle_ok = True
    for filnavn, poster in (("saker", nye_saker), ("journalposter", nye_journalposter)):
        if not poster:
            continue
        try:
            path = _skriv_til_volume(poster, filnavn, ref_date)
            print(f"  [Volume] skrev {len(poster)} poster -> {path}")
        except Exception as e:  # noqa: BLE001
            print(f"  [Volume] skriving FEILET for {filnavn}-{ref_date}: {e}")
            alle_ok = False
    return alle_ok


def write_changelog(nye_saker, nye_journalposter, ref_date, sakstype):
    """Laster dagens endringslogg til bronze-laget. Skriver bevisst INGEN
    lokal fil i repoet eller ellers på maskinen (kun midlertidig fallback ved
    feil, se _fallback_lagre_midlertidig). Landingssone velges automatisk:

    - Unity Catalog Volume (DATABRICKS_VOLUME_PATH) hvis koden kjører inne i
      Databricks og volumet finnes - ingen Azure-oppsett nødvendig da.
    - Ellers Azure Blob (se upload_til_azure) - brukes lokalt/uten Databricks.

    Returnerer suksess-status videre til run_daily(), som bruker den til å
    avgjøre om state.json er trygt å avansere."""
    print(f"{len(nye_saker)} nye saker, {len(nye_journalposter)} saker med nye journalposter i dag")
    if _har_databricks_volume():
        return _skriv_til_volume_changelog(nye_saker, nye_journalposter, ref_date)
    return upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype)


def _i_dag_oslo():
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def run_daily(kilde_key, state_file, window_days=WINDOW_DAYS, today_fn=_i_dag_oslo):
    """Daglig endringslogg. `state_file` må allerede finnes (bygget via
    build_state_from_dump() fra en fullført historisk dump) før første
    kjøring. Endringsloggen (nye saker/journalposter) skrives ALDRI lokalt -
    kun til Azure, se write_changelog()."""
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
    changelog_ok = write_changelog(nye_saker, nye_journalposter, ref_iso, kilde["sakstype_navn"])
    if changelog_ok:
        prune_seen(state, ref_date)
        save_state(state, state_file)
        print(f"Ferdig ({feilet}/{len(kandidat_ids)} feilet under henting).")
    else:
        print(f"  [State] IKKE avansert - endringsloggen kom seg ikke helt til Azure. "
              f"state.json forblir uendret slik at dagens nye saker/journalposter blir "
              f"oppdaget og forsøkt lastet opp på nytt ved neste kjøring i stedet for å gå tapt.")
