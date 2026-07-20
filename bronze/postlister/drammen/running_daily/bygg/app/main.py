import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Daglig endringslogg for Drammen-byggesaker (samme Innsyn-plattform som
# Trondheim). Følger kun den aktive lista ("Produksjon", 2020-today_dump) –
# det historiske arkivet er i praksis lukket og trenger ikke daglig oppfølging.
#
# VIKTIG: proceedings-spørringens fromDate/toDate-filter er upålitelig på
# denne lista (totalCount stemmer, men nodes gjør ikke det – bekreftet ved
# testing). Vi henter derfor journalposter dato-basert (journalFromDate/
# journalToDate virker korrekt) og slår opp hver berørte sak separat via
# eksakt saksnummer-match. Se 2020-today_dump/bygg/main.py for detaljerte
# notater om dette og om øvrige API-begrensninger.
#
# PUBLISERINGSFORSINKELSE (bekreftet ved testing): saksbehandlere registrerer
# ikke alt samme dag - en dags aktivitet kan dukke opp i portalen flere dager
# senere. Et rent "gårsdagen"-vindu ville derfor stille mistet forsinkede
# journalposter permanent. Løsning: hver kjøring skanner et bredere
# tilbakeblikk (LOOKBACK_DAYS) og filtrerer mot seen_journalpost_ids.json (id
# -> journalDate) for kun å ta med journalposter vi ikke har skrevet til
# output før. Output-filnavnet er fortsatt datert på referansedagen (ref),
# så Azure-partisjoneringen (bronze/drammen/load_type=incremental/date=<ref>/,
# samme mønster som Trondheim/Bærum/Kristiansand) er upåvirket - det er kun
# INNHOLDET i hver dags fil som nå garantert er komplett.

BASE_URL = "https://innsyn2020.drammen.kommune.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "450d153d-62f7-4564-ab1b-60370477c471"
BYGG_SUBARCHIVE_ID = "1053bcf0-41ec-4598-9e8e-a273447d625a"   # "eByggesak"
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Drammen"
KOMMUNE_NR = 3301

MAX_LIMIT = 100
TIMEOUT = 60
RETRIES = 4
LOOKBACK_DAYS = 14     # hvor mange dager tilbake vi skanner hver kjøring (se topp-kommentar om publiseringsforsinkelse)
SEEN_ID_RETENTION_DAYS = LOOKBACK_DAYS * 3   # hvor lenge en id beholdes i seen-lista før den ryddes bort
RESOLVE_WORKERS = 8    # parallelle saksoppslag (ingen batch-endpoint finnes for eksakt-match)

HERE = Path(__file__).parent
OUTPUT_DIR = HERE / "output"
SEEN_IDS_FILE = HERE / "seen_journalpost_ids.json"

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


def _fetch_journals_leaf(session, from_iso, to_iso, all_journals):
    """Hent journalposter for et vindu som er lite nok til å ligge innenfor
    MAX_LIMIT (eller ikke kan deles videre). Prøver avdeling- og deretter
    type-etterfyll hvis grensen likevel kuttet resultatet."""
    data = search_post_journal(
        session, plimit=1, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(),
        journals_where=base_journals_where(from_iso, to_iso),
    )
    journals = list(data["journals"]["nodes"])
    j_total = data["journals"]["totalCount"]

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
              f"(servergrense, ingen flere avdelinger/typer å prøve)")
    for j in journals:
        all_journals[j["id"]] = j


def fetch_journals_window(session, from_date, to_date):
    """Rekursiv dato-halvering over LOOKBACK_DAYS-vinduet (samme mønster som
    full-dump-scriptene) - et flatt enkeltkall holder kun for et 1-dagersvindu,
    mens LOOKBACK_DAYS=14 typisk gir godt over MAX_LIMIT=100 totalt."""
    all_journals = {}

    def recurse(from_d, to_d):
        from_iso, to_iso = from_d.isoformat(), to_d.isoformat()
        data = search_post_journal(
            session, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(),
            journals_where=base_journals_where(from_iso, to_iso),
        )
        j_total = data["journals"]["totalCount"]
        if j_total > MAX_LIMIT and from_d != to_d:
            mid = from_d + (to_d - from_d) // 2
            recurse(from_d, mid)
            recurse(mid + timedelta(days=1), to_d)
            return
        _fetch_journals_leaf(session, from_iso, to_iso, all_journals)

    recurse(from_date, to_date)
    return list(all_journals.values())


def resolve_proceedings(session, journals):
    """Slå opp full sak-info for hver unike sak journalpostene tilhører
    (proceedings-datofilteret er upålitelig, se topp-kommentar – eksakt
    saksnummer-match er derimot pålitelig)."""
    seqs = sorted({(j.get("proceeding") or {}).get("sequenceNumber") for j in journals} - {None})

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
    with ThreadPoolExecutor(max_workers=RESOLVE_WORKERS) as pool:
        for seq, p in pool.map(lookup, seqs):
            if p:
                proceedings[seq] = p
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
    """Adressen står først i tittelen, skilt med komma (vanligst) eller " - "
    (en del eldre saker). Ingen av delene funnet -> ingen adresse i kilden,
    returner None i stedet for å gjette."""
    if not tittel:
        return None
    komma_idx = tittel.find(",")
    dash_idx = tittel.find(" - ")
    candidates = [i for i in (komma_idx, dash_idx) if i != -1]
    if not candidates:
        return None
    return tittel[:min(candidates)].strip() or None


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


def load_seen_ids():
    """{journalpost-id: journalDate} for poster vi allerede har skrevet til
    output i en tidligere kjøring - brukes til å luke ut duplikater når
    LOOKBACK_DAYS-vinduet overlapper med det forrige kjøringer allerede har
    dekket (se topp-kommentar om publiseringsforsinkelse)."""
    if not SEEN_IDS_FILE.exists():
        return {}
    try:
        return json.loads(SEEN_IDS_FILE.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def save_seen_ids(seen, keep_after_iso):
    """Lagre oppdatert seen-liste, og rydd bort oppføringer godt utenfor
    skann-vinduet (de kan uansett aldri dukke opp igjen i et framtidig
    LOOKBACK_DAYS-vindu, så de trenger ikke tas vare på)."""
    pruned = {jid: d for jid, d in seen.items() if d >= keep_after_iso}
    SEEN_IDS_FILE.write_text(json.dumps(pruned, ensure_ascii=False), encoding="utf-8")


def write_output(saker, ref_date):
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"saker_{ref_date}.json").write_text(
        json.dumps(saker, ensure_ascii=False, indent=2), encoding="utf-8")
    # Ingen egen "eldre sak fikk ny journalpost"-fil her (i motsetning til
    # Trondheim): siden proceedings-datofilteret er upålitelig på denne lista
    # (se topp-kommentar) finnes uansett ingen pålitelig måte å skille "ny
    # sak i dag" fra "eldre sak med ny aktivitet" – alle berørte saker havner
    # derfor i samme fil, med kun deres NYE journalposter (se run()).
    print(f"Skrev {len(saker)} berørte saker (med nye journalposter) -> {OUTPUT_DIR}")
    # TODO: last opp som .jsonl.gz til Azure (bronze/drammen/load_type=incremental/date=<ref>/)
    # - filnavnet er datert på ref (referansedagen), ikke skann-vinduet, så
    #   partisjoneringen er uendret selv om vi nå skanner LOOKBACK_DAYS bakover.


def run(day_back=1):
    session = make_session()
    ref = datetime.now(ZoneInfo("Europe/Oslo")).date() - timedelta(days=day_back)
    from_date = ref - timedelta(days=LOOKBACK_DAYS - 1)
    to_iso = ref.isoformat()
    print(f"Skann-vindu: {from_date.isoformat()} .. {to_iso} (LOOKBACK_DAYS={LOOKBACK_DAYS}, "
          f"referansedag={to_iso})")

    journals = fetch_journals_window(session, from_date, ref)
    seen = load_seen_ids()
    nye_journals = [j for j in journals if j["id"] not in seen]
    print(f"  {len(journals)} journalposter i skann-vinduet, {len(nye_journals)} er nye siden sist kjøring")

    proceedings_by_seq = resolve_proceedings(session, nye_journals)
    journals_by_seq = {}
    ukjent = []
    for j in nye_journals:
        seq = (j.get("proceeding") or {}).get("sequenceNumber")
        if seq in proceedings_by_seq:
            journals_by_seq.setdefault(seq, []).append(j)
        else:
            ukjent.append(j)

    saker = [build_sak(p, [build_journalpost(j) for j in journals_by_seq.get(seq, [])])
             for seq, p in proceedings_by_seq.items()]

    write_output(saker, to_iso)

    for j in nye_journals:
        seen[j["id"]] = j.get("journalDate")
    keep_after = (ref - timedelta(days=SEEN_ID_RETENTION_DAYS)).isoformat()
    save_seen_ids(seen, keep_after)

    if ukjent:
        print(f"OBS: {len(ukjent)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")


if __name__ == "__main__":
    import sys
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run(day_back=db)
