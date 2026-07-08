import requests
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

# Dette scriptet henter alle bygg-saker fra Kristiansand sin OpenGov-innsynsløsning:
#   1) Søker opp lista over saker (document_id-er)
#   2) Besøker hver sak og trekker ut journalposter + filer

BASE_URL = "https://opengov.360online.com"
SEARCH_URL = f"{BASE_URL}/Cases/KRSANDEBYGG"
CASE_BASE_URL = f"{BASE_URL}/Cases/KRSANDEBYGG/Case/Details/"

KOMMUNE_NR = 4204
KOMMUNE = "Kristiansand"

CASE_TYPE_ID = "99001"
QUERY = "q=bygg"

HERE = Path(__file__).parent
OUTPUT_FILE = HERE / "kristiansand.json"

MAX_WORKERS = 8       # antall parallelle forespørsler
SAVE_EVERY = 200      # skriv til disk hvert n. sak (gjør kjøringen gjenopptakbar)
TIMEOUT = 30
RETRIES = 3

GNR_BNR_RE = re.compile(r"\b(\d+/\d+)")   # gårds- og bruksnummer i en adresse
ADDR_INDEX_RE = re.compile(r"^\d+\.\s*")  # løpenummer foran en adresselinje


def split_saksnummer(text):
    """Deler teksten i saksnummer og resten av tittelen på første ' - '.

    Returnerer (None, teksten) dersom det ikke finnes noe saksnummer.
    """
    text = (text or "").strip()
    if " - " in text:
        nr, rest = text.split(" - ", 1)
        return nr.strip(), rest.strip()
    return None, text


def get_case_list():
    """Steg 1: hent lista over saker (document_id-er) fra søket."""
    search = f"{SEARCH_URL}?{QUERY}&{CASE_TYPE_ID}"
    response = requests.get(search, timeout=TIMEOUT)
    response.raise_for_status()

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
    """Steg 2: trekk ut all detaljdata fra en saks-side."""
    soup = BeautifulSoup(html, "html.parser")
    for s in soup(["script", "style"]):
        s.extract()

    # Saksnummer og sakstittel fra sidens hovedoverskrift
    h2 = soup.select_one(".pageTitleHeader h2")
    saksnummer, sakstittel = split_saksnummer(h2.get_text(strip=True) if h2 else "")

    # Adresser fra sidepanelet, med gårds-/bruksnummer skilt ut
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

    # Hver journalpost ligger i et eget accordion-panel
    dokumenter = []
    for btn in soup.select("[id^=caseDocument_]"):
        jid = btn.get("id", "").replace("caseDocument_", "")   # journalpost-id fra element-id-en
        h4 = btn.select_one(".accordionTitle h4")
        journalnummer, doktittel = split_saksnummer(h4.get_text(strip=True) if h4 else "")

        panel = btn.find_next_sibling()   # innholdet ligger i søsken-elementet
        fields = {}
        filer = []
        if panel:
            # Metadata-radene: feltnavn i headeren, verdi i innholdet
            for dl in panel.select(".detailsList"):
                h = dl.select_one(".documentDetailHeader span")
                v = dl.select_one(".documentDetailContent")
                if h and v:
                    fields[h.get_text(strip=True)] = v.get_text(strip=True)

            # Vedlagte filer med nedlastingslenke
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

    return {
        "document_id": str(document_id),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstittel": sakstittel,
        "gnr_bnr": gnr_bnr,
        "url": CASE_BASE_URL + str(document_id),
        "adresse": adresse,
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
    # Alle forsøk feilet – returner saken med feilmelding i stedet for å stoppe kjøringen
    return {"document_id": str(document_id), "url": url, "error": str(last_err)}


def load_done():
    """Leser eksisterende output slik at kjøringen kan gjenopptas."""
    if not OUTPUT_FILE.exists():
        return {}
    try:
        data = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        # Bare saker som ble ferdig og uten feil regnes som gjort; resten hentes på nytt
        return {d["document_id"]: d for d in data
                if "dokumenter" in d and "error" not in d}
    except Exception:  # noqa: BLE001
        return {}


def save(results):
    """Skriver resultatene til disk via en midlertidig fil for å unngå halvskrevet output."""
    tmp = OUTPUT_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
    tmp.replace(OUTPUT_FILE)   # bytt inn den ferdige fila i ett steg


def run():
    ids = get_case_list()

    # Hopp over saker som allerede ligger i output, slik at en avbrutt kjøring kan fortsette
    results = load_done()
    todo = [i for i in ids if i not in results]
    print(f"{len(ids)} saker totalt, {len(results)} allerede hentet, {len(todo)} gjenstår")

    session = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_case, session, i): i for i in todo}
        for fut in as_completed(futures):   # ta imot resultatene etter hvert som de blir ferdige
            res = fut.result()
            results[res["document_id"]] = res
            done += 1
            if done % SAVE_EVERY == 0:   # lagre fremdriften jevnlig underveis
                save(results)
                print(f"  {done}/{len(todo)} hentet (lagret)")

    save(results)
    errors = sum(1 for r in results.values() if "error" in r)
    print(f"Ferdig: {len(results)} saker skrevet til {OUTPUT_FILE} ({errors} feilet)")


if __name__ == "__main__":
    run()
