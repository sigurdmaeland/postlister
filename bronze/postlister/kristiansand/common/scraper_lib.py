"""Delt bibliotek for Kristiansand-scraperne (bygg/henv/ulov/tilsyn).

Alle fire sakstyper skrapes fra samme OpenGov-portal (KRSANDEBYGG) med
identisk logikk - eneste reelle forskjell mellom dem er hvilket
saksnummer-prefiks ("BYGG-", "HENV-", "ULOV-", "TILSYN-") som brukes i
søket. Dette biblioteket samler all delt logikk ett sted, slik at en
fiks/forbedring gjelder alle fire sakstyper automatisk, i stedet for å
måtte gjøres identisk i 4 (eller 8, med running_daily) nesten-like filer.

Brukes av:
  - 2020-today_dump/{bygg,henv,ulov,tilsyn}/main.py   (engangs historisk dump)
  - running_daily/{bygg,henv,ulov,tilsyn}/app/main.py (daglig endringslogg)
"""

import json
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

MAX_WORKERS = 8       # antall parallelle forespørsler
TIMEOUT = 30
RETRIES = 3

GNR_BNR_RE = re.compile(r"\b(\d+/\d+)")   # gårds- og bruksnummer i en adresse
ADDR_INDEX_RE = re.compile(r"^\d+\.\s*")  # løpenummer foran en adresselinje


# --------------------------------------------------------------------------- #
# Opprydding av adresse/gnr_bnr (nærmere Trondheims format) + matrikkelnr
# --------------------------------------------------------------------------- #
# Kristiansands rå adressefelt er en liste med linjer som
# "Gatenavn 12, 4610 KRISTIANSAND S, Norge" - ofte med duplikater (samme
# adresse gjentatt med og uten mellomrom i bokstav-suffikset, f.eks.
# "16 B" og "16B"), og noen ganger med FLERE husnumre kommaseparert på én
# linje ("Lagmannsholmen 1, 3, 4, 5A, 6, 7, 4610 KRISTIANSAND S, Norge") -
# samme mønster som ble fikset i Trondheims sakstitler. Funksjonene under
# rydder dette opp til en flat, deduplisert adresseliste, slik at "adresse"
# og "gnr_bnr" kan lagres som semikolon-separerte strenger (som i Trondheim)
# i stedet for rå lister, og et nytt "matrikkelnr"-felt kan avledes.
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
_POSTNR_STED = re.compile(r"^\d{4}(\s|$)")

# Ett "husnummer-token": et tall med evt. bokstav-suffiks, evt. som et
# bindestrek-spenn til et nytt tall/bokstav ("18", "9A", "30A-E", "24-34").
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


def build_matrikkelnr(gnr_bnr_list, kommune_nr):
    """Bygger matrikkelnr fra deduplisert gnr_bnr-liste, f.eks. "4204-150/732"
    (samme konvensjon som Trondheims gnr_bnr_matrikkel())."""
    if not gnr_bnr_list:
        return None
    return "; ".join(f"{kommune_nr}-{g}" for g in gnr_bnr_list)


_HAR_HUSNUMMER = re.compile(r"\s\d+[A-Za-zæøåÆØÅ]?(-\w+)?$")
# " - GNR/BNR - melding om mulig ulovlig tiltak - ..." - eldre/uformelt
# rapporterte ulovlighetssaker; gnr/bnr kan mangle (vist som en bar "/").
_DASH_GNRBNR = re.compile(r"\s-\s(\d+/\d+(?:/\d+)?|/)\s-\s")
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
    """Utleder (adresse, gnr_bnr) direkte fra sakstittelen - brukes som
    fallback når adresse-panelet på siden er tomt (skjer ofte for useriøst
    rapporterte ulovlighetssaker, f.eks. "hakleiva 10 - / - melding om mulig
    ulovlig tiltak - ..."). To kjente formater i tittelen:
      1) "Gatenavn [Nummer] GNR/BNR[/FESTENR/SEKSJONSNR][,] beskrivelse"
         (vanligst for bygg/henv/tilsyn, og en del ulov)
      2) "Gatenavn Nummer - GNR/BNR - melding om mulig ulovlig tiltak - ..."
         (eldre/uformelt rapporterte ulovlighetssaker; gnr/bnr kan mangle,
         vist som en bar "/", og vises noen ganger som kommunenr/gnr/bnr i
         stedet for bare gnr/bnr)
    Returnerer None for adresse hvis kandidaten ikke har et ekte,
    gjenkjennelig husnummer til slutt - vi gjetter aldri en adresse som bare
    er et sone-/stedsnavn uten husnummer."""
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
            addr_candidate = tittel[:gm.start()].strip().rstrip(",").strip()

    adresse = None
    if addr_candidate and not ingen_adresse:
        normalized = _normalize_nummer_bokstav(addr_candidate)
        if _har_husnummer(normalized):
            adresse = normalized

    return adresse, gnr_bnr


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
    """Hent lista over alle saker (document_id-er) for én sakstype.

    NB: bruker "?q=<PREFIKS>-" (søk på saksnummer-prefiks), IKKE
    "?casetypeid=...". casetypeid-varianten viste kun "siste to måneder"
    som standardfilter og ga dermed stille bare en brøkdel av alle saker -
    ingen feilmelding, bare et lavere tall enn det som faktisk finnes.
    "q="-søket viser derimot "Alle" år/sakstype som standard (bekreftet
    manuelt: riktig totalt antall for hver sakstype, ingen paginering -
    hele lista lastes i én respons). Egen retry lagt til her, siden dette
    er eneste sted en hel kjøring krasjer før noe arbeid er gjort hvis
    nettverket glipper akkurat på dette kallet."""
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

    # Fallback: hent adresse/gnr_bnr fra sakstittelen når adresse-panelet på
    # siden ikke ga noe (typisk for useriøst rapporterte ulovlighetssaker) -
    # fyller kun inn hull, overstyrer aldri det panelet faktisk ga.
    if not adresse_clean or not gnr_bnr_clean:
        fallback_adresse, fallback_gnr_bnr = address_from_tittel(sakstittel, KOMMUNE_NR)
        if not adresse_clean and fallback_adresse:
            adresse_clean = [fallback_adresse]
        if not gnr_bnr_clean and fallback_gnr_bnr:
            gnr_bnr_clean = [fallback_gnr_bnr]

    return {
        "document_id": str(document_id),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": sakstype_fra_saksnummer(saksnummer),
        "sakstittel": sakstittel,
        "adresse": "; ".join(adresse_clean) or None,
        "gnr_bnr": "; ".join(gnr_bnr_clean) or None,
        "matrikkelnr": build_matrikkelnr(gnr_bnr_clean, KOMMUNE_NR),
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


def _forkast_uventet_sakstype(records, saksnummer_prefiks):
    """Defensiv sjekk: "q="-søket er et fritekstsøk, ikke en garantert
    sakstype-filtrering - forkast (og varsle om) enhver sak som mot
    formodning har et saksnummer som ikke starter med denne mappas
    forventede prefiks, så vi aldri stille blander sakstyper igjen slik det
    skjedde med den gamle casetypeid-bugen. Returnerer (ok, forkastet)."""
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
def load_done(output_file):
    """Leser eksisterende output slik at kjøringen kan gjenopptas."""
    if not output_file.exists():
        return {}
    try:
        data = json.loads(output_file.read_text(encoding="utf-8"))
        # Bare saker som ble ferdig og uten feil regnes som gjort; resten hentes på nytt
        return {d["document_id"]: d for d in data
                if "dokumenter" in d and "error" not in d}
    except Exception:  # noqa: BLE001
        return {}


def save_resultater(results, output_file):
    """Skriver resultatene til disk via en midlertidig fil for å unngå halvskrevet output."""
    tmp = output_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
    tmp.replace(output_file)   # bytt inn den ferdige fila i ett steg


def run_full_dump(output_file, saksnummer_prefiks, limit=None, save_every=200):
    """Full historisk dump av én sakstype (brukes av 2020-today_dump/*/main.py).

    limit=N henter kun de N nyeste sakene (lista fra get_case_list() er
    sortert nyeste først) - nyttig for raske delkjøringer/testing uten å
    måtte gå gjennom hele den tunge full-dumpen (~21000 saker totalt på
    tvers av alle sakstyper) hver gang."""
    ids = get_case_list(saksnummer_prefiks)
    if limit:
        ids = ids[:limit]

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

    _, forkastet = _forkast_uventet_sakstype(list(results.values()), saksnummer_prefiks)
    for r in forkastet:
        del results[r["document_id"]]

    save_resultater(results, output_file)
    errors = sum(1 for r in results.values() if "error" in r)
    print(f"Ferdig: {len(results)} saker skrevet til {output_file} ({errors} feilet)")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - full sweep + snapshot-diff
# --------------------------------------------------------------------------- #
def full_sweep(saksnummer_prefiks, limit=None):
    """Hent alle sakene av én sakstype på nytt (full skanning).

    limit=N henter kun de N nyeste sakene (lista er sortert nyeste først).
    Nyttig for rask testing - i produksjon kjøres den uten limit. NB: siden
    "siste to måneder"-snarveien er bekreftet basert på når saken ble
    OPPRETTET (ikke sist oppdatert), må denne fortsatt sveipe absolutt alle
    sakene hver gang for å oppdage nye dokumenter på eldre, fortsatt aktive
    saker - se samtalen om avveiningen mellom korrekthet og kostnad."""
    ids = get_case_list(saksnummer_prefiks)
    if limit:
        ids = ids[:limit]
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

    ok, _ = _forkast_uventet_sakstype(results, saksnummer_prefiks)
    return ok


def load_snapshot(snapshot_file):
    """Leser gårsdagens snapshot: {document_id: [journalpost_id, ...]}."""
    if not snapshot_file.exists():
        return {}
    try:
        return json.loads(snapshot_file.read_text(encoding="utf-8"))
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
    tmp = snapshot_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    tmp.replace(snapshot_file)


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
                    "url": r.get("url"),
                },
                "nye_journalposter": nye_jp,
            })

    return nye_saker, nye_journalposter


def upload_til_azure(saker_file, jp_file, ref_date, sakstype):
    """Opplastings-hook mot Azure Blob - IKKE IMPLEMENTERT ENNÅ.

    Sentralisert her (i stedet for én TODO-kommentar per sakstype, spredt
    over 4 filer) slik at det bare er ETT sted å koble på ekte opplasting
    når Azure-oppsettet er klart. Filene som faktisk skrives lokalt akkurat
    nå er vanlig UTF-8-JSON med innrykk (json.dumps(..., indent=2)) - IKKE
    .jsonl.gz - så en fremtidig implementasjon må enten konvertere til
    .jsonl.gz før opplasting, eller laste opp .json-filene som de er,
    avhengig av hva resten av Azure-pipelinen forventer. Planlagt
    stikonvensjon (ikke bygget):
    bronze/kristiansand/{sakstype}/load_type=incremental/date=<ref_date>/.
    """
    print(f"  [Azure] opplasting ikke satt opp ennå - {saker_file.name} og "
          f"{jp_file.name} ({sakstype}, {ref_date}) ligger foreløpig kun lokalt.")


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


def _i_dag_oslo():
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def run_daily(snapshot_file, output_dir, saksnummer_prefiks, sakstype, limit=None, today_fn=_i_dag_oslo):
    """Daglig endringslogg for én sakstype (brukes av running_daily/*/app/main.py).

    `today_fn` er injiserbar i stedet for å kalle datetime.now(...) direkte,
    slik at tester kan simulere flere påfølgende "dager" uten å vente på
    ekte klokketid."""
    ref_date = today_fn().isoformat()
    prev = load_snapshot(snapshot_file)

    results = full_sweep(saksnummer_prefiks, limit=limit)
    snapshot = build_snapshot(results)
    feilet = sum(1 for r in results if "error" in r)

    if not prev:
        # Første kjøring: ingen snapshot å diffe mot. Etabler baseline uten å
        # dumpe alle sakene som "nye" - neste kjøring gir en ekte endringslogg.
        save_snapshot(snapshot, snapshot_file)
        print(f"Baseline etablert: {len(snapshot)} saker lagret i snapshot "
              f"({feilet} feilet). Ingen endringslogg skrevet på første kjøring.")
        return

    nye_saker, nye_journalposter = diff(results, prev)
    write_changelog(nye_saker, nye_journalposter, ref_date, output_dir, sakstype)
    save_snapshot(snapshot, snapshot_file)
    print(f"Ferdig ({feilet} saker feilet under henting).")
