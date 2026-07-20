import json
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Daglig endringslogg for Trondheim-tilsynssaker (GraphQL, samme plattform som
# ../../bygg/app/main.py).
#
# Egen GraphQL-type ("Tilsynssak", type-id under) - ingen heuristikk trengs,
# i motsetning til Asker. Se ../../../2017-03-24-today_dump/tilsyn/main.py for
# detaljerte notater om sakstype-kartleggingen (funnet via fritekstsøk på
# "tilsyn") og den kjente dato-filter-buggen.
#
# Ekte datoer, så dette er dato-basert (som Bærum/bygg) – ingen snapshot
# trengs. Datovindu = i går (WINDOW_DAYS=1). De fleste dager er godt under 100
# saker/journalposter; et fåtall over grensen fylles etter med
# departement-filter.

BASE_URL = "https://trondheim.innsynsportal.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "fc010204-2c07-4f3c-a8ef-879ec218111c"         # byggesak-postliste (delt med bygg/ulov/henv)
TILSYN_TYPE_ID = "7b2a9cb8-1948-4a28-93f1-e6e9d198ad68"  # "Tilsynssak"-type UUID
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Trondheim"
KOMMUNE_NR = 5001

MAX_LIMIT = 100
TIMEOUT = 60
RETRIES = 4
WINDOW_DAYS = 1   # dager bakover fra referansedatoen (1 = kun referansedagen)

HERE = Path(__file__).parent
OUTPUT_DIR = HERE / "output"

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


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
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
            "journalProceedingWhere": {"typeIdIn": [TILSYN_TYPE_ID]},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(from_iso, to_iso, department_id_in=None, sequence_number=None):
    return {
        "listId": LIST_ID,
        "typeIdIn": [TILSYN_TYPE_ID],
        "year": None,
        "fromDate": from_iso,
        "toDate": to_iso,
        "departmentIdIn": department_id_in,
        "sequenceNumber": sequence_number,
    }


def base_journals_where(from_iso, to_iso, department_id_in=None):
    return {
        "listId": LIST_ID,
        "journalFromDate": from_iso,
        "journalToDate": to_iso,
        "departmentIdIn": department_id_in,
    }


def _refill_via_departments(items, total, refetch):
    """Hvis 'items' ble kuttet av MAX_LIMIT-grensen (færre enn 'total'), prøv å
    hente resten ved å filtrere per avdeling blant dem vi allerede har sett."""
    if len(items) >= total:
        return items
    depts = sorted({it["department"]["id"] for it in items if it.get("department")})
    by_id = {it["id"]: it for it in items}
    for dept in depts:
        for extra in refetch(dept):
            by_id[extra["id"]] = extra
    return list(by_id.values())


def fetch_window(session, from_iso, to_iso):
    """Hent alle saker + journalposter i datovinduet. Prøver departement-etterfyll
    hvis en av delene ble kuttet av MAX_LIMIT-grensen (skjer sjelden)."""
    data = search_post_journal(
        session, plimit=MAX_LIMIT, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(from_iso, to_iso),
        journals_where=base_journals_where(from_iso, to_iso),
    )
    proceedings = list(data["proceedings"]["nodes"])
    p_total = data["proceedings"]["totalCount"]
    journals = list(data["journals"]["nodes"])
    j_total = data["journals"]["totalCount"]

    journals = _refill_via_departments(
        journals, j_total,
        lambda dept: search_post_journal(
            session, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(from_iso, to_iso),
            journals_where=base_journals_where(from_iso, to_iso, department_id_in=[dept]),
            include_count=False,
        )["journals"]["nodes"],
    )
    proceedings = _refill_via_departments(
        proceedings, p_total,
        lambda dept: search_post_journal(
            session, plimit=MAX_LIMIT, jlimit=1,
            proceedings_where=base_proceedings_where(from_iso, to_iso, department_id_in=[dept]),
            journals_where=base_journals_where(from_iso, to_iso),
            include_count=False,
        )["proceedings"]["nodes"],
    )

    if len(proceedings) < p_total:
        print(f"    ADVARSEL {from_iso}..{to_iso}: fant kun {len(proceedings)}/{p_total} saker "
              f"(servergrense, ingen flere avdelinger å prøve)")
    if len(journals) < j_total:
        print(f"    ADVARSEL {from_iso}..{to_iso}: fant kun {len(journals)}/{j_total} journalposter "
              f"(servergrense, ingen flere avdelinger å prøve)")

    return proceedings, journals


def fetch_proceeding_by_sequence_number(session, sequence_number):
    """Hent full sak-info for en eldre forelder-sak via eksakt saksnummer-match."""
    try:
        data = search_post_journal(
            session, plimit=1, jlimit=1,
            proceedings_where=base_proceedings_where(None, None, sequence_number=sequence_number),
            journals_where=base_journals_where(None, None),
            include_count=False,
        )
        nodes = data["proceedings"]["nodes"]
        return nodes[0] if nodes else None
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# Bygging
# --------------------------------------------------------------------------- #
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


_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_HAR_HUSNUMMER = re.compile(r"\s\d+[A-Za-zæøåÆØÅ]?(-\w+)?$")
_TRAILING_PAREN = re.compile(r"(?:\s*\([^)]*\))+\s*$")
_TRAILING_MFL = re.compile(r"\s+(?:m\.?\s*fl\.?|med\s+flere)\s*$", re.IGNORECASE)
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
# eiendom / eieindom (kjent skrivefeil i kildedata) + gnr/bnr
_EIENDOM_PREFIX = re.compile(r"^eie(?:i)?ndom(?:men)?\s*\(?\d+/\d+\)?\s*", re.IGNORECASE)
_JUNK_LABELS = {"henvendelse", "tilsyn", "ingen registrering", "eiendom", "ingen adresse", "ufordelte"}

# Ett "husnummer-token": et tall med evt. bokstav-suffiks, evt. som et
# bindestrek-spenn til et nytt tall/bokstav ("18", "9A", "30A - E", "24 - 34").
TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN_FULL = re.compile(rf"^{TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
# gatenavn + husnummer-token på slutten, med obligatorisk mellomrom foran
# tallet (unngår å kutte midt i sammensatte sonekoder som "B2", "BB1")
_STREET_AND_NUMBER_RANGE = re.compile(rf"^(.+?)\s+({TOKEN})$")
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")


def _normalize_nummer_bokstav(s):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks (f.eks.
    "50 A" -> "50A")."""
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _har_husnummer(addr):
    return bool(_HAR_HUSNUMMER.search(" " + addr))


def _top_level_split(s, delim=","):
    """Del s på delim, men ignorer forekomster inni parenteser (så en
    parentetisk kommaliste ikke feilaktig blir tolket som tittel-struktur)."""
    parts = []
    depth = 0
    start = 0
    for i, c in enumerate(s):
        if c == "(":
            depth += 1
        elif c == ")":
            depth = max(0, depth - 1)
        elif c == delim and depth == 0:
            parts.append(s[start:i])
            start = i + 1
    parts.append(s[start:])
    return parts


def _truncate_at_separator_dash(seg):
    """Hvis segmentet inneholder ' - ' som IKKE binder sammen to
    tall/bokstav-tokens (f.eks. 'Østre Rosten 47 - Tiller VGS'), trunker til
    venstre for streken (streken er da en gammel adresse/tittel-separator).
    Hvis streken derimot er et tallspenn ('Ladebekken 2 - 6', '30A - E'), er
    den del av selve adressen og segmentet beholdes uendret."""
    idx = seg.find(" - ")
    if idx == -1:
        return seg
    left, right = seg[:idx], seg[idx + 3:]
    left_m = re.search(r"(\d+[A-Za-zæøåÆØÅ]?)\s*$", left)
    right_m = re.match(r"\s*(\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ])\b", right)
    if left_m and right_m:
        return seg
    return left.strip()


def _parse_address_parts(s):
    """Tolker s som en liste av adresser atskilt med komma og/eller ' og '.
    Hvert element er enten: (a) et bart tall/spenn som arver gatenavnet fra
    forrige fulle element ('20' etter 'Sorgenfriveien 18' -> 'Sorgenfriveien
    20'), (b) en bar bokstav som arver BÅDE gatenavn og tall ('B' etter '18'
    -> '18B'), eller (c) et helt nytt 'Gatenavn nummer' (ulikt gatenavn enn
    forrige element, f.eks. 'Vangslunds gate 10' etter 'Elgeseter gate 16').

    (c) godtas både etter komma og etter " og " - Trondheims titler bruker
    begge til å liste opp helt ulike gateadresser på samme sak (f.eks.
    "Bersvendveita 16, Repslagerveita 6, 8 og 10, og Olav Tryggvasons gate
    48" og "Elgeseter gate 16 og Vangslunds gate 10"). For å unngå å tolke
    beskrivelsestekst som en ny adresse (f.eks. "Nedre Møllenberg gate 60,
    seksjon 13, ..." der "seksjon 13" er et seksjonsnummer, ikke en gate)
    kreves det at kandidaten starter med stor bokstav - norske gatenavn er
    alltid egennavn, mens ord som "seksjon"/"byggetrinn"/"trinn" midt i en
    tittel konsekvent skrives med liten forbokstav i kildedataen.

    Stopper og returnerer det den har klart å tolke så snart et element ikke
    passer noen av mønstrene (det er da resten av saks-tittelen, ikke flere
    adresser) - bindestrek-spenn ('2 - 6') beholdes samlet som ett element."""
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
        # for første element (hodet) stoler vi alltid på gatenavnet, siden det
        # allerede er vasket av head-finnings-løkka i extract_adresse; for
        # senere elementer (etter komma/og) kreves stor forbokstav for å
        # skille en helt ny gate fra beskrivelsestekst med et tall på slutten
        if sm and (sep is None or _STARTS_UPPER.match(sm.group(1).strip())):
            current_street = sm.group(1).strip()
            norm = re.sub(r"\s*-\s*", "-", sm.group(2))
            entries.append(f"{current_street} {norm}")
            mnum = re.match(r"^(\d+)", norm)
            if mnum:
                last_number = mnum.group(1)
            continue
        break  # resten er beskrivelse, ikke flere adresser
    # dedupe, behold rekkefølge
    seen = set()
    uniq = []
    for e in entries:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq


def extract_adresse(tittel):
    """Adressen står først i tittelen (evt. etter en 'Eiendom gnr/bnr'-
    henvisning), skilt med komma - evt. med flere husnumre på samme gate
    (komma- og/eller og-liste, f.eks. '18, 20 og 22') eller et
    bindestrek-spenn ('2 - 6', beholdes samlet siden vi ikke vet nøyaktig
    hvilke husnumre som faktisk finnes mellom endepunktene). En del eldre
    saker bruker ' - ' som separator i stedet for komma når det ikke er noe
    ekte tallspenn involvert.

    Renser bort kjente ikke-adresse-mønstre: "Ingen adresse..." -> None,
    "Eiendom(men)/Eieindom(men) (gnr/bnr) ..." -> prefikset fjernes (evt.
    hoppes helt over til neste komma-segment hvis det ikke er noe annet
    igjen), etterheng i parentes og " m.fl."/" med flere" fjernes, og rene
    stikkord ("Henvendelse", "Tilsyn" osv.) -> None.

    Hvis ingenting av det gjenstående har et ekte husnummer, returneres None
    i stedet for å gjette."""
    if not tittel:
        return None
    segs = _top_level_split(tittel, ",")

    head = None
    head_idx = None
    for idx in range(min(len(segs), 4)):
        seg_stripped = segs[idx].strip()
        if not seg_stripped:
            continue
        if _INGEN_ADRESSE.match(seg_stripped):
            return None
        seg_proc = _truncate_at_separator_dash(seg_stripped)
        had_eiendom_prefix = bool(_EIENDOM_PREFIX.match(seg_proc))
        cleaned = _EIENDOM_PREFIX.sub("", seg_proc).strip()
        cleaned = _TRAILING_PAREN.sub("", cleaned).strip()
        cleaned = _TRAILING_MFL.sub("", cleaned).strip()
        if cleaned and cleaned.lower() not in _JUNK_LABELS:
            head = cleaned
            head_idx = idx
            break
        if had_eiendom_prefix and not cleaned:
            # segmentet var bare en eiendoms-/matrikkel-henvisning uten noe
            # annet innhold - se videre i neste komma-del etter selve
            # adressen (f.eks. "Eiendom 198/7, Kirkeringen 12, ...")
            continue
        # tomt/rent stikkord uten at det var en eiendoms-henvisning å hoppe
        # forbi -> ingen adresse å hente ut, ikke let videre
        return None

    if head is None:
        return None

    rest = ",".join(segs[head_idx + 1:])
    full_candidate = _normalize_nummer_bokstav(head + ("," + rest if rest else ""))
    entries = _parse_address_parts(full_candidate)
    if not entries:
        return None
    return "; ".join(entries)


def sak_url(saksnummer):
    params = json.dumps({"proceeding": {"sequenceNumber": saksnummer, "typeIdIn": [TILSYN_TYPE_ID]}})
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
        "sakstype": (proceeding.get("type") or {}).get("name"),
        "sakstittel": proceeding.get("title"),
        "adresse": extract_adresse(proceeding.get("title")),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "dato": proceeding.get("date"),
        "url": sak_url(saksnummer) if saksnummer else PORTAL_URL,
        "journalposter": journalposter,
    }


def parent_sak_ref(session, journal_proceeding_stub):
    """Metadata om en eldre forelder-sak (som fikk en ny journalpost i dag).
    journal_proceeding_stub er det reduserte 'proceeding'-feltet fra en Journal
    (id/sequenceNumber/type/subArchive/propertyIdentifications, ikke title/date)
    – vi henter full sak via saksnummer-oppslag for å få med tittel/adresse."""
    seq = journal_proceeding_stub.get("sequenceNumber")
    full = fetch_proceeding_by_sequence_number(session, seq) if seq else None
    if full:
        gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(full.get("propertyIdentifications"))
        return {
            "identifier": full.get("id"),
            "kommune": KOMMUNE,
            "kommune_nr": KOMMUNE_NR,
            "saksnummer": full.get("sequenceNumber"),
            "sakstittel": full.get("title"),
            "adresse": extract_adresse(full.get("title")),
            "gnr_bnr": gnr_bnr,
            "matrikkelnr": matrikkelnr,
            "url": sak_url(full.get("sequenceNumber")),
        }
    # Fallback: bruk det vi allerede har fra journalpostens 'proceeding'-felt.
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(journal_proceeding_stub.get("propertyIdentifications"))
    return {
        "identifier": journal_proceeding_stub.get("id"),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": seq,
        "sakstittel": None,   # ikke tilgjengelig uten vellykket saksnummer-oppslag
        "adresse": None,
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "url": sak_url(seq) if seq else PORTAL_URL,
    }


# --------------------------------------------------------------------------- #
# Kjøring
# --------------------------------------------------------------------------- #
def write_output(nye_saker, nye_journalposter, ref_date):
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"saker_{ref_date}.json").write_text(
        json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUTPUT_DIR / f"journalposter_{ref_date}.json").write_text(
        json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Skrev {len(nye_saker)} nye saker og "
          f"{len(nye_journalposter)} saker med nye journalposter -> {OUTPUT_DIR}")
    # TODO: last opp som .jsonl.gz til Azure (bronze/trondheim/load_type=incremental/date=<ref>/tilsyn/)


def run(day_back=1):
    session = make_session()
    ref = datetime.now(ZoneInfo("Europe/Oslo")).date() - timedelta(days=day_back)
    from_iso = (ref - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    to_iso = ref.isoformat()
    print(f"Datovindu: {from_iso} .. {to_iso}")

    proceedings, journals = fetch_window(session, from_iso, to_iso)
    print(f"  {len(proceedings)} nye saker, {len(journals)} nye journalposter i vinduet")

    journals_by_proceeding = defaultdict(list)
    for j in journals:
        pid = (j.get("proceeding") or {}).get("id")
        journals_by_proceeding[pid].append(j)
    nye_sak_ids = {p["id"] for p in proceedings}

    # 1) Nye saker (med sine journalposter fra vinduet)
    nye_saker = [
        build_sak(p, [build_journalpost(j) for j in journals_by_proceeding.get(p["id"], [])])
        for p in proceedings
    ]

    # 2) Nye journalposter på eldre saker (forelder ikke blant dagens nye saker).
    #    "proceeding.date" ser ut til å være siste aktivitetsdato, ikke
    #    opprettelsesdato, så en sak som får en ny journalpost i dag dukker
    #    normalt selv opp blant dagens nye saker. Denne grenen er derfor
    #    sjelden truffet, men beholdes som fallback.
    nye_journalposter = []
    for pid, docs in journals_by_proceeding.items():
        if pid in nye_sak_ids or not pid:
            continue
        stub = docs[0].get("proceeding") or {}
        nye_journalposter.append({
            "parent_sak": parent_sak_ref(session, stub),
            "nye_journalposter": [build_journalpost(j) for j in docs],
        })

    write_output(nye_saker, nye_journalposter, to_iso)


if __name__ == "__main__":
    import sys
    db = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    run(day_back=db)
