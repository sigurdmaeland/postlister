import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Full-dump av Drammens HISTORISKE byggesaker fra Innsyn (egen liste/arkiv,
# egen subArchiveId – skannede papirsaker, ikke samme datasett som
# 2020-today_dump selv om periodene overlapper i tid). Se
# 2020-today_dump/bygg/main.py for detaljerte notater om API-begrensningene
# (samme GraphQL-plattform, samme query, samme proceedings-datofilter-bug).
#
# journalFromDate/journalToDate virker korrekt, men proceedings sitt
# fromDate/toDate gjør IKKE det (nodes stemmer ikke med tidsrommet, kun
# totalCount er riktig) – derfor henter vi journalposter dato-basert og slår
# opp hver unike sak separat via eksakt saksnummer-match etterpå.
#
# 1. januar hvert år har unormalt mange journalposter (opptil 300-400, mot
# normalt 5-50 andre dager) – ser ut som en plassholderdato brukt ved
# skanning/digitalisering når original dato mangler på papirsaken. Avdeling
# er så godt som alltid "Ingen registrering" på disse (nytteløst som
# etterfyllingsfilter), så vi bruker sakstype (typeIdIn) som fallback i
# stedet. Noen ekstreme dager (f.eks. >150 poster av samme type) er likevel
# umulig å hente fullstendig med tilgjengelige filtre – dette logges som
# ADVARSEL og er en reell begrensning i kildesystemet, ikke en bug hos oss.

BASE_URL = "https://innsyn2020.drammen.kommune.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "fb851964-3185-43eb-81ba-9ac75226dfa8"
BYGG_SUBARCHIVE_ID = "858e1d6d-4226-4699-a2ba-604bb6e08386"   # "Historiske byggesaker"
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Drammen"
KOMMUNE_NR = 3301

START_DATE = date(1800, 1, 1)   # eldste journalpost funnet i dette arkivet

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4
RESOLVE_WORKERS = 16   # parallelle saksoppslag (ingen batch-endpoint finnes for eksakt-match)

HERE = Path(__file__).parent
OUTPUT_FILE = HERE / "drammen_historisk.json"

# NB: .strip() er nødvendig – serveren avviser query-tekst med avvikende
# whitespace (selv ett ekstra linjeskift), uansett om where-innholdet er gyldig.
QUERY = """
query SearchPostJournal($proceedingsLimit: Int!, $proceedingsWhere: ProceedingsWhere!, $proceedingsOrderBy: ProceedingsOrderBy, $journalsLimit: Int!, $journalsWhere: SearchJournalsWhere!, $journalDocumentsWhere: JournalDocumentsWhere!, $journalProceedingWhere: JournalProceedingWhere, $journalsOrderBy: SearchJournalsOrderBy, $includeCount: Boolean) {
  proceedings(
    limit: $proceedingsLimit
    includeCount: $includeCount
    where: $proceedingsWhere
    orderBy: $proceedingsOrderBy
  ) {
    totalCount
    nodes {
      ...ProceedingResult
      __typename
    }
    __typename
  }
  journals: searchJournals(
    limit: $journalsLimit
    includeCount: $includeCount
    where: $journalsWhere
    proceedingWhere: $journalProceedingWhere
    orderBy: $journalsOrderBy
  ) {
    totalCount
    nodes {
      ...JournalResult
      __typename
    }
    __typename
  }
}

fragment ProceedingResult on Proceeding {
  id
  archiveId
  sequenceNumber
  title
  date
  caseworkers
  classified
  archiveSystem {
    id
    name
    __typename
  }
  department {
    id
    name
    __typename
  }
  type {
    id
    name
    __typename
  }
  subArchive {
    id
    name
    __typename
  }
  propertyIdentifications {
    id
    useNr
    propertyNr
    __typename
  }
  __typename
}

fragment JournalResult on Journal {
  id
  archiveId
  journalDate
  classified
  documentDate
  title
  sequenceNumber
  caseworkers
  senders
  unpublished
  recipients
  mainDocumentNotPublishedReason
  archiveSystem {
    id
    name
    __typename
  }
  department {
    id
    name
    __typename
  }
  status {
    id
    description
    name
    __typename
  }
  subArchive {
    id
    name
    __typename
  }
  type {
    id
    name
    description
    __typename
  }
  documents(where: $journalDocumentsWhere) {
    id
    classified
    title
    order
    type {
      id
      name
      __typename
    }
    __typename
  }
  proceeding {
    id
    sequenceNumber
    type {
      id
      name
      __typename
    }
    subArchive {
      id
      name
      __typename
    }
    propertyIdentifications {
      id
      useNr
      propertyNr
      __typename
    }
    __typename
  }
  __typename
}
""".strip()


def make_session():
    s = requests.Session()
    s.headers.update({
        "Content-Type": "application/json",
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    })
    return s


def _post(session, body):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = session.post(API_URL, data=json.dumps(body), timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if data.get("errors"):
                raise RuntimeError(str(data["errors"]))
            return data["data"]
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def search_post_journal(session, *, plimit, jlimit, proceedings_where, journals_where,
                         include_count=True):
    body = {
        "operationName": "SearchPostJournal",
        "variables": {
            "proceedingsLimit": plimit,
            "proceedingsWhere": proceedings_where,
            "proceedingsOrderBy": "date_DESC",
            "journalsLimit": jlimit,
            "journalsWhere": journals_where,
            "journalDocumentsWhere": {"listId": LIST_ID},
            "journalProceedingWhere": {"subArchiveId": BYGG_SUBARCHIVE_ID},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(sequence_number=None, department_id_in=None):
    return {
        "listId": LIST_ID,
        "subArchiveId": BYGG_SUBARCHIVE_ID,
        "sequenceNumber": sequence_number,
        "departmentIdIn": department_id_in,
    }


def base_journals_where(from_iso, to_iso, department_id_in=None, type_id_in=None):
    return {
        "listId": LIST_ID,
        "journalFromDate": from_iso,
        "journalToDate": to_iso,
        "departmentIdIn": department_id_in,
        "typeIdIn": type_id_in,
    }


def _refill_via(items, total, group_key, refetch):
    """Hvis 'items' ble kuttet av MAX_LIMIT-grensen (færre enn 'total'), prøv å
    hente resten ved å filtrere per verdi av 'group_key' (department/type)
    blant dem vi allerede har sett."""
    if len(items) >= total:
        return items
    values = sorted({it[group_key]["id"] for it in items if it.get(group_key)})
    by_id = {it["id"]: it for it in items}
    for val in values:
        for extra in refetch(val):
            by_id[extra["id"]] = extra
    return list(by_id.values())


def fetch_journals_range(session, from_date, to_date, all_journals):
    """Rekursiv dato-halvering på journalposter (den eneste pålitelige
    datofiltreringen på denne lista, se topp-kommentar). Fyller all_journals
    (dict id -> node) i stedet for å returnere. Arkivet er svært ujevnt
    fordelt over tid (1800-2010 er tett, nyere år er glisne), så bisection
    dykker mye dypere i de eldste periodene."""
    from_iso, to_iso = from_date.isoformat(), to_date.isoformat()
    data = search_post_journal(
        session, plimit=1, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(),
        journals_where=base_journals_where(from_iso, to_iso),
    )
    j_total = data["journals"]["totalCount"]

    if j_total > MAX_LIMIT and from_date != to_date:
        mid = from_date + (to_date - from_date) // 2
        fetch_journals_range(session, from_date, mid, all_journals)
        fetch_journals_range(session, mid + timedelta(days=1), to_date, all_journals)
        return

    journals = list(data["journals"]["nodes"])
    # Avdeling først (fungerer best på nyere/digitale saker), sakstype som
    # ekstra fallback (avdeling er nesten alltid "Ingen registrering" på
    # eldre, skannede saker - se topp-kommentar om 1. januar-datoene).
    journals = _refill_via(
        journals, j_total, "department",
        lambda dept: search_post_journal(
            session, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(),
            journals_where=base_journals_where(from_iso, to_iso, department_id_in=[dept]),
            include_count=False,
        )["journals"]["nodes"],
    )
    journals = _refill_via(
        journals, j_total, "type",
        lambda type_id: search_post_journal(
            session, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(),
            journals_where=base_journals_where(from_iso, to_iso, type_id_in=[type_id]),
            include_count=False,
        )["journals"]["nodes"],
    )
    if len(journals) < j_total:
        print(f"    ADVARSEL {from_iso}..{to_iso}: fant kun {len(journals)}/{j_total} journalposter "
              f"(servergrense, trolig plassholderdato med for mange poster i samme type)")
    for j in journals:
        all_journals[j["id"]] = j
    if j_total:
        print(f"  {from_iso}..{to_iso}: {j_total} journalposter")


def resolve_proceedings(session, all_journals):
    """Slå opp full sak-info for hver unike sak journalpostene tilhører.
    proceedings-datofilteret er upålitelig (se topp-kommentar) så vi kan ikke
    bisecte saker direkte – eksakt saksnummer-match er derimot pålitelig, så
    vi slår opp hver sak for seg (parallellisert, ingen batch-endpoint finnes)."""
    seqs = sorted({(j.get("proceeding") or {}).get("sequenceNumber") for j in all_journals.values()} - {None})
    print(f"Slår opp {len(seqs)} unike saker…")

    def lookup(seq):
        data = search_post_journal(
            session, plimit=3, jlimit=1,
            proceedings_where=base_proceedings_where(sequence_number=seq),
            journals_where=base_journals_where(None, None),
            include_count=False,
        )
        for p in data["proceedings"]["nodes"]:
            if p.get("sequenceNumber") == seq:
                return seq, p
        return seq, None

    proceedings = {}
    done = 0
    with ThreadPoolExecutor(max_workers=RESOLVE_WORKERS) as pool:
        for seq, p in pool.map(lookup, seqs):
            if p:
                proceedings[seq] = p
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(seqs)}")
    return proceedings


def gnr_bnr_matrikkel(property_identifications):
    """Dedupe (propertyNr, useNr)-par (de dubleres i lista) -> gnr_bnr + matrikkelnr."""
    seen = []
    for pid in property_identifications or []:
        pair = (pid.get("propertyNr"), pid.get("useNr"))
        if pair not in seen and pair[0] is not None and pair[1] is not None:
            seen.append(pair)
    if not seen:
        return None, None
    gnr_bnr = "; ".join(f"{gnr}/{bnr}" for gnr, bnr in seen)
    matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{gnr}/{bnr}" for gnr, bnr in seen)
    return gnr_bnr, matrikkelnr


def extract_adresse(tittel):
    """I motsetning til Trondheim/nyere Drammen (der tittelen starter med
    adressen) er tittelen her kun en generisk sakstype ("Nybygg", "Tilbygg",
    "Andre avgjørelser" osv.), evt. med "; BID: <id>" til slutt - aldri en
    adresse (verifisert manuelt mot alle unike titler i testdata). Et komma/
    bindestrek-søk slik Trondheim bruker gir falske treff her (f.eks. plukker
    opp "Nybygg; BID: 123" fra "Nybygg; BID: 123, 456"), så vi returnerer
    alltid None - eneste reelle stedsdata for dette arkivet er gnr/bnr."""
    return None


def sak_url(saksnummer):
    params = json.dumps({"subArchiveId": BYGG_SUBARCHIVE_ID, "proceeding": {"sequenceNumber": saksnummer}})
    return f"{PORTAL_URL}?params={quote(params, safe='')}"


def build_vedlegg(doc):
    typ = (doc.get("type") or {}).get("name")
    doc_id = doc.get("id")
    return {
        "tittel": doc.get("title"),
        "kategori": "Hoveddokument" if typ == "H" else "Vedlegg",
        "type": typ,
        "dokument_id": doc_id,
        "url": DOCUMENT_URL.format(doc_id) if doc_id and not doc.get("classified") else None,
    }


def build_journalpost(journal):
    return {
        "identifier": journal.get("id"),
        "dokument_id": journal.get("archiveId"),
        "tittel": journal.get("title"),
        "type": (journal.get("type") or {}).get("name"),
        "dato": journal.get("journalDate"),
        "avsender": journal.get("senders") or [],
        "mottaker": journal.get("recipients") or [],
        "vedlegg": [build_vedlegg(d) for d in journal.get("documents") or []],
    }


def build_sak(proceeding, journalposter):
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(proceeding.get("propertyIdentifications"))
    saksnummer = proceeding.get("sequenceNumber")
    return {
        "identifier": proceeding.get("id"),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": (proceeding.get("subArchive") or {}).get("name"),
        "sakstittel": proceeding.get("title"),
        "adresse": extract_adresse(proceeding.get("title")),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "dato": proceeding.get("date"),
        "url": sak_url(saksnummer) if saksnummer else PORTAL_URL,
        "journalposter": journalposter,
    }


def save(saker):
    tmp = OUTPUT_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(OUTPUT_FILE)


def load_existing():
    """Les inn tidligere lagret resultat (nøklet på saksnummer), slik at
    scriptet kan kjøres i flere omganger på ulike årsintervaller og bygge opp
    hele arkivet gradvis uten å miste tidligere hentede saker."""
    if not OUTPUT_FILE.exists():
        return {}
    try:
        return {s["saksnummer"]: s for s in json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
                if s.get("saksnummer")}
    except Exception:  # noqa: BLE001
        return {}


def group_journalposter(all_journals, proceedings_by_seq):
    """Grupper journalposter på sak (via saksnummer fra journalpostens
    embedded 'proceeding'-felt – dette feltet er alltid korrekt, i motsetning
    til proceedings-spørringens datofilter). Journalposter uten en løst sak
    (oppslag feilet, f.eks. gradert) returneres for seg."""
    by_seq = {}
    ukjent = []
    for j in all_journals.values():
        seq = (j.get("proceeding") or {}).get("sequenceNumber")
        if seq in proceedings_by_seq:
            by_seq.setdefault(seq, []).append(build_journalpost(j))
        else:
            ukjent.append(j)
    return by_seq, ukjent


def run(start=START_DATE, end=None, merge=True):
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    all_journals = {}
    print(f"Henter journalposter {start} .. {end} (rekursiv dato-halvering)…")
    fetch_journals_range(session, start, end, all_journals)

    proceedings_by_seq = resolve_proceedings(session, all_journals)
    journalposter_by_seq, ukjent_sak = group_journalposter(all_journals, proceedings_by_seq)
    nye_saker = {seq: build_sak(p, journalposter_by_seq.get(seq, []))
                 for seq, p in proceedings_by_seq.items()}

    # merge=True lar scriptet kjøres flere ganger på ulike årsintervaller
    # (arkivet er for stort til å hente i én kjøring innen praktiske
    # tidsbegrensninger) og bygge opp hele resultatet gradvis.
    saker_by_seq = load_existing() if merge else {}
    saker_by_seq.update(nye_saker)
    saker = list(saker_by_seq.values())
    save(saker)

    n_jp_nye = sum(len(s["journalposter"]) for s in nye_saker.values())
    print(f"Ferdig: {len(nye_saker)} saker i intervallet ({n_jp_nye} journalposter), "
          f"{len(saker)} saker totalt lagret -> {OUTPUT_FILE} "
          f"({time.time() - start_time:.0f}s)")
    if ukjent_sak:
        print(f"OBS: {len(ukjent_sak)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")


if __name__ == "__main__":
    import sys
    # Arkivet er for stort til å hente i én kjøring - kjør i årsbolker som
    # bygger opp samme fil (merge=True er default, se run()):
    #   python3 main.py             -> full dump fra 1800 til i dag
    #   python3 main.py 2015        -> kun fra 2015 til i dag
    #   python3 main.py 1900 1950   -> kun årene 1900-1950
    if len(sys.argv) > 2:
        run(start=date(int(sys.argv[1]), 1, 1), end=date(int(sys.argv[2]), 12, 31))
    elif len(sys.argv) > 1:
        run(start=date(int(sys.argv[1]), 1, 1))
    else:
        run()
