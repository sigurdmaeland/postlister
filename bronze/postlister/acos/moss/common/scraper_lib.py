"""Moss - ACOS "nye-innsyn" (samme plattform som Gjøvik/Sandefjord/Tønsberg
i dette prosjektet, egen selvhostet instans på www.moss.kommune.no). Samme
seksjonsoppdeling som de andre ACOS-kommunene: konstanter -> adresseparsing
-> KILDER -> API-kall -> engangsdump -> daglig endringslogg.

Moss har KUN én relevant sakstype (Byggesak, KILDER-nøkkel "bygg") - ingen
egen Tilsynssak/Ulovlighetssak/Henvendelse-kategori finnes her (bekreftet
ved å lese hele searchOptions.searchFilters-lista i en ekte overview-
respons). Arkivet starter reelt ca. 19.12.2019 (START_DATE) - tidligere
saker må bes om via vanlig innsynsforespørsel.

Sak sitt "eiendom"-felt er alltid None i praksis - adresse/gnr/bnr må derfor
utledes fra sakstittelen (samme som Gjøvik). "/sak/{id}"-endepunktet brukes
i stedet for det flatere "/details/{id}" fordi sistnevnte mangler
"Dokumenttype" helt for journalpostene.

ADRESSE/GNR-BNR-PARSING: KONSEKVENT "Adresse - GNR/BNR[, GNR/BNR][ og
GNR/BNR] - beskrivelse", f.eks. "Rørvikveien 72 - 133/75 - svømmebasseng".
Støtter flere gnr/bnr i eget bindestrek-segment ("Batteriveien Verksåsen -
3/3124 - 3/3125 - ..."), "(tidl. X/Y)"-parenteser (ignoreres) og "mfl."-
suffiks (ignoreres). Et fåtall titler mangler gnr/bnr helt -> adresse=None,
gnr_bnr=None. Verifisert ved stikkprøve av 230 ekte titler: 228/230 (99%) OK.
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
BASE_URL = "https://www.moss.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"

PORTAL_ID = "1"
MENYPUNKT_ID = "3790"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 3103   # Moss (Østfold fra 2024; var 3002 i Viken 2020-2023, 0104 før)
KOMMUNE = "Moss"

START_DATE = "2019-12-19"   # reell arkivgrense, se modul-docstring

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
# Adresse/gnr_bnr/matrikkelnr fra sakstittel
# --------------------------------------------------------------------------- #
_GNRBNR_TOKEN = r"\d+/\d+(?:/\d+)?"
_TOKEN_RE = re.compile(r"(\d+)/(\d+)(?:/(\d+))?")

_GNRBNR_LIST_FULL_RE = re.compile(
    rf"^\s*(?:g(?:nr\s*/?\s*)?bnr\s*[:.]?\s*)?"
    rf"{_GNRBNR_TOKEN}(?:\s*(?:,|og)\s*{_GNRBNR_TOKEN})*"
    rf"\s*(?:m\.?\s*/?\s*fl\.?)?"
    rf"\s*(?:\(\s*tidl\.?\s*{_GNRBNR_TOKEN}\s*\))?\s*$",
    re.IGNORECASE,
)
_PAREN_RE = re.compile(r"\(.*?\)")
# Ekte bindestrek-skilletegn (mellomrom på minst én side), ikke et bart
# husnummer-spenn uten mellomrom ("40-44").
_SPLIT_RE = re.compile(r"(?:(?<=\s)-\s*|-(?=\s))")


def _er_gnrbnr_segment(seg):
    """Sjekker om et helt segment utelukkende er en gnr/bnr-liste."""
    if _GNRBNR_LIST_FULL_RE.match(seg):
        return True
    stripped = _PAREN_RE.sub("", seg).strip()
    return bool(stripped) and bool(_GNRBNR_LIST_FULL_RE.match(stripped))


def parse_adresse_moss(tittel, subtitle=None):
    """Splitter tittelen på ekte bindestrek-skilletegn, finner FØRSTE segment
    som utelukkende er gnr/bnr, og slår sammen alt FØR det til adressen.
    Etterfølgende rene gnr/bnr-segmenter (som i Batteriveien-eksempelet i
    modul-docstring) slås sammen i samme gnr/bnr-liste."""
    if not tittel:
        return None, None, None

    segs = [s.strip() for s in _SPLIT_RE.split(tittel.strip())]
    segs = [s for s in segs if s]
    if not segs:
        return None, None, None

    start = None
    for i, seg in enumerate(segs):
        if _er_gnrbnr_segment(seg):
            start = i
            break
    if start is None:
        return None, None, None

    end = start
    for j in range(start + 1, len(segs)):
        if _er_gnrbnr_segment(segs[j]):
            end = j
        else:
            break

    adresse = " - ".join(segs[:start]) if start > 0 else None

    gnr_bnr_liste = []
    for k in range(start, end + 1):
        clean = _PAREN_RE.sub("", segs[k])
        for m in _TOKEN_RE.finditer(clean):
            par = f"{m.group(1)}/{m.group(2)}"
            if par not in gnr_bnr_liste:
                gnr_bnr_liste.append(par)

    matrikkelnr = ("; ".join(f"{KOMMUNE_NR}-{p}" for p in gnr_bnr_liste)
                   if gnr_bnr_liste else None)
    return adresse, (gnr_bnr_liste or None), matrikkelnr


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        datasource=None,
        sakstype="at-8d581357__87af__4e98__a72a__536246692081-BS!z5TQjS",
        adresse_parser=parse_adresse_moss,
        hent_filer=True,
        periode="2019-nå",
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(session, method, path, body=None, retries=RETRIES):
    """Delt retry-/feilhåndteringslogikk for _post og _get (identisk
    backoff og feilmelding - kun HTTP-verbet skiller dem)."""
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
    return _request(session, "POST", path, body, retries)


def _get(session, path, retries=RETRIES):
    return _request(session, "GET", path, retries=retries)


def get_case_list(kilde_key, session=None, max_items=None):
    """Henter identifier+saksnummer+dato for saker i én kilde, nyeste først.
    max_items=N stopper paginering tidlig - brukes kun når man ikke også
    dato-filtrerer (se run_full_dump)."""
    kilde = KILDER[kilde_key]
    session = session or requests.Session()

    def body_for(page):
        kv = [{"key": "Dato", "value": "AllCases"},
              {"key": "Sakstype", "value": kilde["sakstype"]},
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


def fetch_dokument_filer(session, dokument_identifier):
    """Henter fillommer for ETT dokument. sak.dokumenter[].antallVedlegg er
    alltid 0 uansett faktisk antall - dette kallet må derfor alltid gjøres
    per dokument, det kan ikke stoles på for å hoppe over tomme."""
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
        "url": f"{BASE_URL}/alle-tjenester/innsyn/postliste-og-saksinnsyn/#/details/{identifier}",
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


def _write_json_atomic(data, path):
    """Skriver JSON atomisk (til .tmp, så rename) - brukes av både
    save_resultater (engangsdump) og save_state (daglig endringslogg)."""
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def save_resultater(results, output_file):
    _write_json_atomic(list(results.values()), output_file)


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
    _write_json_atomic(state, state_file)


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
