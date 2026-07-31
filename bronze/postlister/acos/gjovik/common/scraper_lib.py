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
# Godtar et ", Stedsnavn"/". Stedsnavn"-hale etter husnummeret ved
# validering (adressen må ha et ekte husnummer), men fjerner halen fra
# selve adressefeltet - kommunen er uansett kjent (kommune-feltet).
_HUSNUMMER_STED_RE = re.compile(
    r"\s\d+[A-Za-zæøåÆØÅ]?(?:-\w+)?(?P<sted>[,.]\s*[A-ZÆØÅ][\wæøåÆØÅ.\- ]*)?$"
)

_MATRIKKEL_TOKEN = r"\d+/\d+(?:/\d+(?:/\d+)?)?"
_MATRIKKEL_TOKEN_RE = re.compile(r"(\d+)/(\d+)(?:/(\d+)(?:/(\d+))?)?")
_MATRIKKEL_SEGMENT_RE = re.compile(
    rf"^{_MATRIKKEL_TOKEN}(?:\s*(?:og|,)\s*{_MATRIKKEL_TOKEN})*"
)
# Skilletegnet mellom matrikkelnr og adresse varierer (se modul-docstring).
_SEP_RE = re.compile(r"^\s*(?:[-,]\s*)?")


def _normalize_nummer_bokstav(s):
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _rens_adresse(addr):
    """Sjekker at adressekandidaten faktisk har et husnummer til slutt, og
    fjerner et evt. tettstedsnavn hengt på etter det (se _HUSNUMMER_STED_RE).
    Returnerer None hvis det ikke er noe ekte husnummer (kandidaten er da
    typisk bare et gate-/stedsnavn uten egen adresse)."""
    padded = " " + addr
    m = _HUSNUMMER_STED_RE.search(padded)
    if not m:
        return None
    if m.group("sted"):
        return padded[:m.start("sted")].strip()
    return addr


def parse_adresse_gjovik(tittel, subtitle=None):
    """Sakstittelen starter med matrikkelnr, adressen følger med varierende
    skilletegn (se modul-docstring). Returnerer (None, None, None) hvis
    tittelen ikke starter med et gjenkjennelig matrikkelnr-mønster."""
    if not tittel:
        return None, None, None

    rest = tittel.strip()
    seg_m = _MATRIKKEL_SEGMENT_RE.match(rest)
    if not seg_m:
        return None, None, None

    segment = seg_m.group(0)
    rest = rest[seg_m.end():]
    sep_m = _SEP_RE.match(rest)
    if sep_m:
        rest = rest[sep_m.end():]

    gnr_bnr_liste, matrikkel_deler = [], []
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

    forste_segment = rest.split(" - ", 1)[0].strip()
    adresse = None
    if forste_segment and not _INGEN_ADRESSE.match(forste_segment):
        adresse = _rens_adresse(_normalize_nummer_bokstav(forste_segment))

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
