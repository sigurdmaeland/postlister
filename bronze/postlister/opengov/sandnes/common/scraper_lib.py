"""Sandnes - OpenGov/360online (samme plattform som Kristiansand/
Lillestrøm/Sarpsborg, portal-slug "SANDNESPB"). Data finnes fra november
2022. Fire sakstyper skrapes: BYGG-/HENV-/ULOV-/TILSYN-.

VIKTIG FORSKJELL fra de andre: sakstittelen (h2 på detaljsiden) har en
ekte ANDRE LINJE i kilde-HTML-en med en redundant oppsummering av
adresse+matrikkelnr, f.eks. "BYGG-26/02024 - Frittliggende hagestue\\n
Skogvokterveien 24, 33/603/0/0" - rekkefølgen varierer ("Adresse,
matrikkelnr" eller omvendt). Denne linja fjernes fra det lagrede
"sakstittel"-feltet (se _split_tittel_linje) og brukes kun som fallback
når selve adresse-panelet er tomt (se address_from_andre_linje).
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

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://opengov.360online.com"
SEARCH_URL = f"{BASE_URL}/Cases/SANDNESPB"
CASE_BASE_URL = f"{BASE_URL}/Cases/SANDNESPB/Case/Details/"

KOMMUNE_NR = 1108   # Sandnes (uendret i 2024-omnummereringen - i bruk siden 2020)
KOMMUNE = "Sandnes"

AZURE_ACCOUNT_URL = "https://storaggen2eaccountprod.blob.core.windows.net"
AZURE_CONTAINER_NAME = "postlister"
AZURE_BASE_PATH = "bronze/sandnes"

MAX_WORKERS = 8       # antall parallelle forespørsler
TIMEOUT = 30
RETRIES = 3

GNR_BNR_RE = re.compile(r"\b(\d+/\d+)")   # gårds- og bruksnummer i en adresse
ADDR_INDEX_RE = re.compile(r"^\d+\.\s*")  # løpenummer foran en adresselinje

# Tilsynssaker skriver ofte gnr/bnr i ren tekst FØRST i selve sakstittelen,
# f.eks. "Gnr 65 bnr 81 - Tilsyn - Utvidelse av massefylling...", uten at
# adressepanelet eller andre-linja gir noe (bekreftet på ekte Tilsyn-data der
# gnr_bnr forble null selv med adresse utfylt). Brukes KUN som siste fallback
# i parse_case() - overstyrer aldri panelet eller andre_linje.
_GNR_BNR_TEKST_RE = re.compile(r"\bgnr\.?\s+(\d+)\s*,?\s*bnr\.?\s+(\d+)\b", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Opprydding av adresse/gnr_bnr + matrikkelnr
# --------------------------------------------------------------------------- #
# Sandnes' rå adressefelt er en liste med linjer som
# "Gatenavn 12, 4306 SANDNES, Norge" - noen ganger duplikater (samme
# adresse gjentatt med og uten mellomrom i bokstav-suffikset, "16 B" vs
# "16B"), og av og til flere husnumre kommaseparert på én linje - samme
# mønster som ble fikset for Sarpsborg/Lillestrøm/Kristiansand/Trondheim.
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
_POSTNR_STED = re.compile(r"^\d{4}(\s|$)")

_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN_FULL = re.compile(rf"^{_TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_STREET_AND_NUMBER_RANGE = re.compile(rf"^(.+?)\s+({_TOKEN})$")
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
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
    og/eller ' og '. Et bart tall/spenn arver gatenavnet fra forrige
    element, en bar bokstav arver gatenavn+tall, og et nytt 'Gatenavn
    nummer' starter en ny gate - for FØRSTE element stoler vi alltid på
    gatenavnet, for senere elementer kreves stor forbokstav. Bindestrek-
    spenn ('24-34') beholdes samlet som ett element."""
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
            current_street = sm.group(1).strip()
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


# Andre-linja (se _split_tittel_linje) skriver et fullt matrikkelnr som
# "GNR/BNR/FESTENR/SEKSJONSNR" - brukes til å berike matrikkelnr med
# feste/seksjon når det finnes og faktisk er != 0/0 (samme konvensjon som i
# de andre kommune-scraperne).
_MATRIKKEL_ANDRE_LINJE = re.compile(r"^(\d+)/(\d+)/(\d+)/(\d+)$")


def parse_title_matrikkel(andre_linje):
    """Finn gnr/bnr -> (festenr, seksjonsnr) fra andre-linja, når minst ett
    av dem faktisk er != "0"."""
    result = {}
    if not andre_linje:
        return result
    for del_ in andre_linje.split(","):
        m = _MATRIKKEL_ANDRE_LINJE.match(del_.strip())
        if not m:
            continue
        gnr, bnr, feste, seksjon = m.groups()
        if feste == "0" and seksjon == "0":
            continue
        result[(gnr, bnr)] = (feste, seksjon)
    return result


def build_matrikkelnr(gnr_bnr_list, kommune_nr, andre_linje=None):
    """Bygger matrikkelnr fra deduplisert gnr_bnr-liste, f.eks. "1108-33/603"
    - berikes med feste/seksjon parset fra andre-linja når det finnes og
    faktisk er != 0/0. gnr_bnr-feltet forblir uendret (kun det enkle
    gnr/bnr-paret) - kun matrikkelnr skrives fullt ut."""
    if not gnr_bnr_list:
        return None
    utvidelser = parse_title_matrikkel(andre_linje) if andre_linje else {}
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


_HAR_HUSNUMMER = re.compile(r"\s\d+[A-Za-zæøåÆØÅ]?(-\w+)?$")


def _har_husnummer(addr):
    """Sjekker om adressekandidaten faktisk har et ekte husnummer til slutt -
    uten dette er kandidaten som regel et gate-/stedsnavn uten en
    selvstendig identifiserbar adresse."""
    return bool(_HAR_HUSNUMMER.search(" " + addr))


def _split_tittel_linje(rest):
    """Deler sakstittel-teksten (alt etter saksnummeret) i (tittel,
    andre_linje). Sandnes' h2 har ofte et ekte linjeskift som skiller den
    menneskelige beskrivelsen fra en redundant adresse+matrikkelnr-
    oppsummering (se modul-docstring) - andre_linje er None hvis det ikke
    finnes noe linjeskift."""
    linjer = re.split(r"[\r\n]+", rest, maxsplit=1)
    tittel = linjer[0].strip()
    andre_linje = linjer[1].strip() if len(linjer) > 1 else None
    return tittel, andre_linje


def address_from_andre_linje(andre_linje):
    """Utleder (adresse, gnr_bnr) fra andre-linja - brukes KUN som fallback
    når adresse-panelet på siden ikke ga noe (skjer sjelden - panelet gir
    nesten alltid både adresse og gnr/bnr sammen, se parse_case). Andre-
    linja har format "Adresse, GNR/BNR/FESTE/SEKSJON" ELLER
    "GNR/BNR/FESTE/SEKSJON, Adresse" - rekkefølgen varierer (bekreftet ved
    stikkprøve på tvers av bygg/ulov/henv), så hvert kommaledd klassifiseres
    for seg i stedet for å anta en fast rekkefølge."""
    if not andre_linje:
        return None, None
    adresse, gnr_bnr = None, None
    for del_ in andre_linje.split(","):
        del_ = del_.strip()
        if not del_:
            continue
        m = _MATRIKKEL_ANDRE_LINJE.match(del_) or re.match(r"^(\d+)/(\d+)$", del_)
        if m:
            gnr_bnr = f"{m.group(1)}/{m.group(2)}"
        elif not _INGEN_ADRESSE.match(del_):
            normalized = _normalize_nummer_bokstav(del_)
            if _har_husnummer(normalized):
                adresse = normalized
    return adresse, gnr_bnr


# Saksnummer-prefikset koder sakstypen, f.eks. "BYGG-26/00840" -> Byggesak.
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
    """Hent lista over alle saker (document_id-er) for én sakstype.

    Bruker "?q=<PREFIKS>-" (søk på saksnummer-prefiks), IKKE "?casetypeid=..."
    - sistnevnte viste seg i Kristiansand å stille kun returnere "siste to
    måneder" som standardfilter, ingen feilmelding. "q="-søket viser "Alle"
    som standard (bekreftet for Sandnes: BYGG=5846, ULOV=565, HENV=1949
    treff, ingen paginering)."""
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
    saksnummer, rest = split_saksnummer(h2.get_text(strip=True) if h2 else "")
    sakstittel, andre_linje = _split_tittel_linje(rest)

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

    # Fallback: hent adresse/gnr_bnr fra tittelens andre linje når adresse-
    # panelet på siden ikke ga noe - fyller kun inn hull, overstyrer aldri
    # det panelet faktisk ga.
    if not adresse_clean or not gnr_bnr_clean:
        fallback_adresse, fallback_gnr_bnr = address_from_andre_linje(andre_linje)
        if not adresse_clean and fallback_adresse:
            adresse_clean = [fallback_adresse]
        if not gnr_bnr_clean and fallback_gnr_bnr:
            gnr_bnr_clean = [fallback_gnr_bnr]

    # Siste fallback: gnr/bnr skrevet i ren tekst i selve sakstittelen (typisk
    # Tilsynssaker) - se _GNR_BNR_TEKST_RE.
    if not gnr_bnr_clean:
        m = _GNR_BNR_TEKST_RE.search(sakstittel)
        if m:
            gnr_bnr_clean = [f"{m.group(1)}/{m.group(2)}"]

    return {
        "document_id": str(document_id),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": sakstype_fra_saksnummer(saksnummer),
        "sakstittel": sakstittel,
        "adresse": "; ".join(adresse_clean) or None,
        "gnr_bnr": "; ".join(gnr_bnr_clean) or None,
        "matrikkelnr": build_matrikkelnr(gnr_bnr_clean, KOMMUNE_NR, andre_linje),
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
# Delte IO-hjelpere for begge kjøremåtene (historisk dump og daglig sweep)
# --------------------------------------------------------------------------- #
def _get_case_ids(saksnummer_prefiks, limit=None):
    """Henter sakslista for én sakstype, evt. trunkert til de N nyeste."""
    ids = get_case_list(saksnummer_prefiks)
    if limit:
        ids = ids[:limit]
    return ids


def _read_json_or_none(path):
    """Leser og parser JSON fra path, eller None hvis filen ikke finnes."""
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_json(data, path, **kwargs):
    """Skriver data som JSON til path atomisk (via en midlertidig .json.tmp-fil)."""
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, **kwargs)
    tmp.replace(path)   # bytt inn den ferdige fila i ett steg


# --------------------------------------------------------------------------- #
# Engangs historisk dump (2022-today_dump) - gjenopptakbar kjøring
# --------------------------------------------------------------------------- #
def load_done(output_file):
    """Leser eksisterende output slik at kjøringen kan gjenopptas."""
    try:
        data = _read_json_or_none(output_file)
        if data is None:
            return {}
        # Bare saker som ble ferdig og uten feil regnes som gjort; resten hentes på nytt
        return {d["document_id"]: d for d in data
                if "dokumenter" in d and "error" not in d}
    except Exception:  # noqa: BLE001
        return {}


def save_resultater(results, output_file):
    """Skriver resultatene til disk via en midlertidig fil for å unngå halvskrevet output."""
    _atomic_write_json(list(results.values()), output_file, indent=2)


def run_full_dump(output_file, saksnummer_prefiks, limit=None, save_every=200):
    """Full historisk dump av én sakstype (brukes av 2022-today_dump/*/main.py).

    limit=N henter kun de N nyeste sakene (lista fra get_case_list() er
    sortert nyeste først) - nyttig for raske delkjøringer/testing."""
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
    try:
        data = _read_json_or_none(snapshot_file)
        return {} if data is None else data
    except Exception:  # noqa: BLE001
        return {}


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


def upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype):
    """Laster dagens endringslogg (nye saker + nye journalposter på gamle
    saker) direkte til Azure Blob som JSONL+gzip - ingen lokal fil skrives i
    repoet (se write_changelog).

    Stikonvensjon: bronze/sandnes/load_type=incremental/date=<ref_date>/
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

    Stikonvensjon: bronze/sandnes/load_type=full/<slug>/dump_date=<i_dag>/
    <slug>_saker-full-<i_dag>.jsonl.gz - partisjonert på KJØREDATO (når
    dumpen faktisk ble tatt), ikke sakens egen dato, siden "q="-søket gir
    alle saker i ett kall uten paginering/datofilter, og sakene ikke har noe
    brukbart per-sak datofelt å partisjonere på.

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
    kopi per dag ville bare vokse). Ved en mislykket opplasting havner en
    fallback-kopi i systemets midlertidige mappe i stedet, aldri i repoet.

    Returnerer upload_til_azure() sin suksess-status videre til run_daily(),
    som bruker den til å avgjøre om snapshotet er trygt å avansere."""
    print(f"{len(nye_saker)} nye saker, {len(nye_journalposter)} saker med nye journalposter i dag")
    return upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype)


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - inngangspunkt
# --------------------------------------------------------------------------- #
def run_daily(snapshot_file, saksnummer_prefiks, sakstype, limit=None, today_fn=i_dag_oslo):
    """Daglig endringslogg for én sakstype (brukes av running_daily/*/app/main.py).

    `today_fn` er injiserbar i stedet for å kalle datetime.now(...) direkte,
    slik at tester kan simulere flere påfølgende "dager" uten å vente på ekte
    klokketid.

    To sikkerhetsmekanismer, samme som i Lillestrøm-oppsettet: `limit` er
    kun til rask lokal testing og skal aldri røre Azure eller snapshotet (en
    delkjøring med f.eks. limit=30 ville ellers korrumpert produksjons-
    snapshotet med bare 30 saker). Og snapshotet avanseres kun hvis dagens
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
