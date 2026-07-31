"""Delt bibliotek for Tromsø bygg/tilsyn/ulov/henv-scraperne.

Tromsø kjører NØYAKTIG samme plattform som Trondheim og Asker i dette
prosjektet: "360 Plan & Build" via innsynsportal.no (GraphQL). Denne filen
er en tilpasning av trondheim/2017-03-24-today_dump/bygg/main.py sin
skrape-/parselogikk til et delt bibliotek (samme refaktoreringsmønster som
Sarpsborg/Sandnes/Gjøvik), med Tromsø-spesifikke konstanter og en justert
adresseparser (se under).

Portalside: https://tromso.innsynsportal.no/postjournal-v2/f28a3c5b-afb1-40d4-93cc-f89ada639190
Fire sakstyper er tilgjengelige (samme LIST_ID for alle - kun typeIdIn
skiller dem, bekreftet identisk mønster som Trondheim/Asker):
  Byggesak:        1298e845-ec94-43ee-a11a-5606b40a9f71
  Tilsynssak:       f8e62288-0c16-4379-a942-4e8de53c88bc
  Henvendelse:      c9b50f9e-5697-40fa-8a43-5d934ee7dd0e
  Ulovlighetssak:   5edae728-29a9-4737-88eb-833d9dc5a2cb
Type-UUID-ene ble funnet ved å kalle SearchPostJournal med typeIdIn=None
over flere år og samle opp de ulike (type.id, type.name)-parene som dukket
opp i proceedings-resultatet - IKKE gjettet. OBS: det finnes ALSO separate
"Henvendelse Geo"- og "Henvendelse Plan"-typer i samme portal - disse skal
IKKE tas med, kun den rene "Henvendelse"-typen (id over) som brukeren ba om.

VIKTIG platform-migrasjon: en helt annen (mer finkornet) type-taksonomi
brukes for saker FØR 2019-10-22 (f.eks. "BYG-STD-Standard byggesak",
"BYG-ULOVL", "BYG-TILSYN-Tilsyn i byggesaker", "BYG-HENV-Henvendelse/
Forespørsel" - egne UUID-er per undertype, ikke de fire samlede typene
over). Byggesak-UUID-en over gir 0 treff for hele 2019 frem til nøyaktig
22.10.2019 (bekreftet ved binærsøk dag-for-dag), og deretter jevnt voksende
volum - dette er tydeligvis datoen Tromsø migrerte til dagens portal/
type-system. Datoen stemmer også påfallende godt med brukerens egen note
om at "møtedokumenter fra før oktober 2019" krever manuell innsynsforespørsel
til postmottak - arkivet før den datoen regnes derfor som utenfor scope her
(samme avveining som Trondheims START_DATE=2017-03-24, som også er
plattformens egen tidligste dato med dagens type-system, ikke et forsøk på
å telle med en eldre/inkompatibel arkivperiode).

Adresseformat (bekreftet ved stikkprøve av 60+ ekte titler på tvers av alle
fire sakstyper): "GNR/BNR[/FESTE[/SEKSJON]] Gatenavn Nummer, beskrivelse"
- matrikkelnr FØRST (som Sarpsborg/Gjøvik), etterfulgt av et mellomrom
(IKKE bindestrek/komma - enklere enn både Trondheim og Gjøvik), så selve
adressen, så et komma, så beskrivelsen. gnr/bnr/feste/seksjon hentes
uansett strukturert fra proceeding.propertyIdentifications (samme felt og
samme gnr_bnr_matrikkel()-funksjon som Trondheim/Asker bruker) - den ledende
matrikkelnr-teksten i tittelen brukes KUN til å vite hvor selve adressen
begynner, ikke som datakilde for gnr/bnr. Et lite mindretall titler mangler
matrikkelnr-prefikset helt (rene firmanavn-tilsyn som "Tore Workinn AS,
tilsyn med kvalifikasjoner", eller bare "-") - disse behandles som
"ingen adresse" (samme avveining som andre steder i kodebasen).

Brukes av:
  - 2019-10-22-today_dump/{bygg,tilsyn,ulov,henv}/main.py (engangs historisk dump)
  - running_daily/{bygg,tilsyn,ulov,henv}/app/main.py     (daglig endringslogg)
"""

import json
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://tromso.innsynsportal.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "f28a3c5b-afb1-40d4-93cc-f89ada639190"   # delt for alle fire sakstyper
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Tromsø"
KOMMUNE_NR = 5501   # gjeldende siden 2024 (var 5401 før)

TYPE_IDS = {
    "Byggesak": "1298e845-ec94-43ee-a11a-5606b40a9f71",
    "Tilsynssak": "f8e62288-0c16-4379-a942-4e8de53c88bc",
    "Henvendelse": "c9b50f9e-5697-40fa-8a43-5d934ee7dd0e",
    "Ulovlighetssak": "5edae728-29a9-4737-88eb-833d9dc5a2cb",
}

START_DATE = date(2019, 10, 22)   # se modul-docstring om platform-migrasjonen

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4

# NB: .strip() er nødvendig - serveren avviser query-tekst med avvikende
# whitespace (selv ett ekstra linjeskift), uansett om where-innholdet er gyldig.
# Identisk med Trondheim/Asker sin QUERY (samme plattform/schema).
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
# gnr/bnr/matrikkelnr - strukturert fra API-et (propertyIdentifications)
# --------------------------------------------------------------------------- #
def gnr_bnr_matrikkel(property_identifications):
    """Dedupe (propertyNr, useNr)-par (de dubleres ofte i lista) -> gnr_bnr +
    matrikkelnr. Identisk med Trondheim/Asker sin funksjon - samme API-felt."""
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


# --------------------------------------------------------------------------- #
# Adresse fra sakstittel
# --------------------------------------------------------------------------- #
# Tittelen starter (nesten alltid) med matrikkelnr FØRST, uten skilletegn til
# selve adressen ("182/1/0/0 Sessøya 2, ..." / "115/132 Ringvegen 470, ...") -
# se modul-docstring. Denne strippes av FØR resten tolkes med samme
# komma-baserte adresseparser som Trondheim bruker (_top_level_split,
# _parse_address_parts, husnummer-validering) - se der for detaljerte
# docstrings om hvorfor hvert steg finnes.
_LEADING_MATRIKKEL = re.compile(r"^\d+/\d+(?:/\d+(?:/\d+)?)?\s+")

_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_TRAILING_PAREN = re.compile(r"(?:\s*\([^)]*\))+\s*$")
_TRAILING_MFL = re.compile(r"\s+(?:m\.?\s*fl\.?|med\s+flere)\s*$", re.IGNORECASE)
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
_JUNK_LABELS = {"henvendelse", "tilsyn", "ingen registrering", "eiendom", "ingen adresse",
                "ufordelte", "-"}

TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN_FULL = re.compile(rf"^{TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_STREET_AND_NUMBER_RANGE = re.compile(rf"^(.+?)\s+({TOKEN})$")
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")


def _normalize_nummer_bokstav(s):
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _top_level_split(s, delim=","):
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


def extract_adresse(tittel):
    """Utleder adressen fra sakstittelen: strip ledende matrikkelnr (se
    _LEADING_MATRIKKEL over), deretter samme komma-baserte flerledd-parsing
    som Trondheim (se der for utfyllende docstring om hvorfor hvert steg
    finnes) - gjenbrukt uendret siden selve REST-en av tittel-strukturen
    (etter matrikkelnr) følger samme konvensjoner (komma-/og-lister,
    bindestrek-spenn, "Ingen adresse"/stikkord -> None)."""
    if not tittel:
        return None
    rest = _LEADING_MATRIKKEL.sub("", tittel.strip())
    if rest == tittel.strip():
        # ingen ledende matrikkelnr funnet - noen titler er rene stikkord/
        # firmanavn uten noen adresse i det hele tatt (se modul-docstring)
        pass

    segs = _top_level_split(rest, ",")

    head = None
    head_idx = None
    for idx in range(min(len(segs), 4)):
        seg_stripped = segs[idx].strip()
        if not seg_stripped:
            continue
        if _INGEN_ADRESSE.match(seg_stripped):
            return None
        seg_proc = _truncate_at_separator_dash(seg_stripped)
        cleaned = _TRAILING_PAREN.sub("", seg_proc).strip()
        cleaned = _TRAILING_MFL.sub("", cleaned).strip()
        cleaned = cleaned.rstrip(" -").strip()   # fjern løs bindestrek-hale ("Presis Bolig AS -")
        if cleaned and cleaned.lower() not in _JUNK_LABELS:
            head = cleaned
            head_idx = idx
            break
        return None

    if head is None:
        return None

    tail = ",".join(segs[head_idx + 1:])
    full_candidate = _normalize_nummer_bokstav(head + ("," + tail if tail else ""))
    entries = _parse_address_parts(full_candidate)
    if not entries:
        return None
    return "; ".join(entries)


# --------------------------------------------------------------------------- #
# API-kall
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
            "journalProceedingWhere": {"typeIdIn": proceedings_where.get("typeIdIn")},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(type_id, from_iso, to_iso, department_id_in=None, sequence_number=None):
    return {
        "listId": LIST_ID,
        "typeIdIn": [type_id],
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


def fetch_window(session, type_id, from_iso, to_iso):
    """Hent alle saker + journalposter for én sakstype i et datovindu. Prøver
    departement-etterfyll hvis en av delene ble kuttet av MAX_LIMIT-grensen."""
    data = search_post_journal(
        session, plimit=MAX_LIMIT, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(type_id, from_iso, to_iso),
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
            proceedings_where=base_proceedings_where(type_id, from_iso, to_iso),
            journals_where=base_journals_where(from_iso, to_iso, department_id_in=[dept]),
            include_count=False,
        )["journals"]["nodes"],
    )
    proceedings = _refill_via_departments(
        proceedings, p_total,
        lambda dept: search_post_journal(
            session, plimit=MAX_LIMIT, jlimit=1,
            proceedings_where=base_proceedings_where(type_id, from_iso, to_iso, department_id_in=[dept]),
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


def fetch_proceeding_by_sequence_number(session, type_id, sequence_number):
    """Hent full sak-info for en eldre forelder-sak via eksakt saksnummer-match."""
    try:
        data = search_post_journal(
            session, plimit=1, jlimit=1,
            proceedings_where=base_proceedings_where(type_id, None, None, sequence_number=sequence_number),
            journals_where=base_journals_where(None, None),
            include_count=False,
        )
        nodes = data["proceedings"]["nodes"]
        return nodes[0] if nodes else None
    except Exception:  # noqa: BLE001
        return None


def sak_url(type_id, saksnummer):
    params = json.dumps({"proceeding": {"sequenceNumber": saksnummer, "typeIdIn": [type_id]}})
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
        "saksbehandler": journal.get("caseworkers") or [],
        "vedlegg": [build_vedlegg(d) for d in journal.get("documents") or []],
    }


def build_sak(proceeding, journalposter, type_id, sakstype):
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(proceeding.get("propertyIdentifications"))
    saksnummer = proceeding.get("sequenceNumber")
    return {
        "identifier": proceeding.get("id"),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": (proceeding.get("type") or {}).get("name") or sakstype,
        "sakstittel": proceeding.get("title"),
        "adresse": extract_adresse(proceeding.get("title")),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "saksbehandler": proceeding.get("caseworkers") or [],
        "dato": proceeding.get("date"),
        "url": sak_url(type_id, saksnummer) if saksnummer else PORTAL_URL,
        "journalposter": journalposter,
    }


def parent_sak_ref(session, type_id, journal_proceeding_stub):
    """Metadata om en eldre forelder-sak (som fikk en ny journalpost i dag).
    Bruker full sak-info hvis den lar seg slå opp (fetch_proceeding_by_sequence_number),
    ellers stubben fra journalpostens embedded 'proceeding'-felt - stubben har
    ikke 'title', så sakstittel/adresse blir da None (samme utfall som før)."""
    seq = journal_proceeding_stub.get("sequenceNumber")
    full = fetch_proceeding_by_sequence_number(session, type_id, seq) if seq else None
    data = full or journal_proceeding_stub
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(data.get("propertyIdentifications"))
    saksnummer = data.get("sequenceNumber")
    tittel = data.get("title")
    return {
        "identifier": data.get("id"),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstittel": tittel,
        "adresse": extract_adresse(tittel),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "saksbehandler": data.get("caseworkers") or [],
        "url": sak_url(type_id, saksnummer) if saksnummer else PORTAL_URL,
    }


# --------------------------------------------------------------------------- #
# State-håndtering (lagring/gjenopptak av tidligere kjøringer)
# --------------------------------------------------------------------------- #
def save(saker, output_file):
    tmp = output_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(output_file)


def load_existing_saker(output_file):
    """Leser saker fra en tidligere (evt. delvis) kjøring, keyet på
    identifier - grunnlaget for at run_full_dump kan kalles gjentatte ganger
    med ulike (og gjerne ikke-overlappende) datointervaller, f.eks. pga.
    kjøretidsgrenser i sandkasse-miljøet, uten å miste tidligere hentede
    saker."""
    if not output_file.exists():
        return {}
    try:
        data = json.loads(output_file.read_text(encoding="utf-8"))
        return {s["identifier"]: s for s in data if s.get("identifier")}
    except Exception:  # noqa: BLE001
        return {}


def _merge_journalposter(existing, new):
    """Slår sammen journalpostlister fra to kjøringer av SAMME sak (unngår å
    miste journalposter funnet i et TIDLIGERE kall når saken dukker opp
    igjen i et SENERE kall med et annet datointervall - 'proceeding.date' er
    sakens siste aktivitetsdato, så samme sak kan spres over flere kall)."""
    by_id = {jp["identifier"]: jp for jp in existing if jp.get("identifier")}
    order = [jp["identifier"] for jp in existing if jp.get("identifier")]
    for jp in new:
        jid = jp.get("identifier")
        if not jid:
            continue
        if jid not in by_id:
            order.append(jid)
        by_id[jid] = jp
    return [by_id[i] for i in order]


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def group_journalposter(all_journals, all_proceedings):
    """Grupper journalposter på sak-id. Journalposter uten en kjent sak i
    all_proceedings (forelder utenfor dumpens periode) returneres for seg."""
    by_sak = defaultdict(list)
    ukjent = []
    for j in all_journals:
        pid = (j.get("proceeding") or {}).get("id")
        if pid in all_proceedings:
            by_sak[pid].append(j)
        else:
            ukjent.append(j)
    return by_sak, ukjent


# --------------------------------------------------------------------------- #
# Engangs historisk dump (2019-10-22-today_dump) - dag-for-dag, gjenopptakbar
# --------------------------------------------------------------------------- #
def run_full_dump(output_file, type_id, sakstype, start=START_DATE, end=None, save_every=30):
    """Full historisk dump (dag-for-dag - ingen offset-paginering, se
    modul-docstring for hvorfor). GJENOPPTAKBAR på tvers av kall: laster
    eksisterende saker fra output_file (load_existing_saker) og SLÅR SAMMEN
    med det som hentes i dette kallet (journalposter unionert per sak via
    _merge_journalposter, ikke overskrevet) - så en flerårig dump trygt kan
    deles opp i flere `python3 main.py <start> <end>`-kall med ulike,
    ikke-overlappende datointervaller uten å miste tidligere hentede
    saker/journalposter."""
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    saker_by_id = load_existing_saker(output_file)
    if saker_by_id:
        print(f"{len(saker_by_id)} saker allerede lagret fra tidligere kjøring(er)")

    all_proceedings = {}
    all_journals = []
    days = list(daterange(start, end))
    print(f"Henter {len(days)} dager ({start} .. {end}) for {sakstype}…")

    def merge_and_save():
        journalposter_by_sak, _ = group_journalposter(all_journals, all_proceedings)
        for pid, p in all_proceedings.items():
            journalposter = [build_journalpost(j) for j in journalposter_by_sak.get(pid, [])]
            ny_sak = build_sak(p, journalposter, type_id, sakstype)
            if pid in saker_by_id:
                ny_sak["journalposter"] = _merge_journalposter(
                    saker_by_id[pid]["journalposter"], ny_sak["journalposter"])
            saker_by_id[pid] = ny_sak
        save(list(saker_by_id.values()), output_file)

    for i, d in enumerate(days, 1):
        day_iso = d.isoformat()
        proceedings, journals = fetch_window(session, type_id, day_iso, day_iso)
        for p in proceedings:
            all_proceedings[p["id"]] = p
        all_journals.extend(journals)

        if i % 20 == 0 or i == len(days):
            print(f"  dag {i}/{len(days)} ({day_iso}): "
                  f"{len(all_proceedings)} saker denne kjøringen så langt "
                  f"({len(saker_by_id)} lagret totalt fra før)")

        if i % save_every == 0:
            merge_and_save()
            print(f"    (lagret delresultat: {len(saker_by_id)} saker totalt)")

    _, ukjent_sak = group_journalposter(all_journals, all_proceedings)
    merge_and_save()

    n_jp = sum(len(s["journalposter"]) for s in saker_by_id.values())
    print(f"Ferdig: {len(saker_by_id)} saker totalt, {n_jp} journalposter -> {output_file} "
          f"({time.time() - start_time:.0f}s)")
    if ukjent_sak:
        print(f"OBS: {len(ukjent_sak)} journalposter hørte til saker utenfor "
              f"{start}..{end} og ble ikke tatt med (utenfor dumpens periode).")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - datovindu (ekte datoer, ingen snapshot)
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 1   # dager bakover fra referansedatoen (1 = kun referansedagen)


def write_output(nye_saker, nye_journalposter, ref_date, output_dir):
    output_dir.mkdir(exist_ok=True)
    (output_dir / f"saker_{ref_date}.json").write_text(
        json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"journalposter_{ref_date}.json").write_text(
        json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Skrev {len(nye_saker)} nye saker og "
          f"{len(nye_journalposter)} saker med nye journalposter -> {output_dir}")


def run_daily(output_dir, type_id, sakstype, day_back=1, window_days=WINDOW_DAYS):
    """Daglig endringslogg - datobasert (ekte 'date'/'journalDate'-felt).
    'proceeding.date' ser ut til å være siste aktivitetsdato, ikke
    opprettelsesdato, så en sak som får en ny journalpost i dag dukker
    normalt selv opp blant dagens nye saker - 'nye_journalposter'-grenen er
    derfor sjelden truffet, men beholdes som fallback for eldre forelder-
    saker utenfor vinduet."""
    session = make_session()
    ref = datetime.now(ZoneInfo("Europe/Oslo")).date() - timedelta(days=day_back)
    from_iso = (ref - timedelta(days=window_days - 1)).isoformat()
    to_iso = ref.isoformat()
    print(f"Datovindu: {from_iso} .. {to_iso} ({sakstype})")

    proceedings, journals = fetch_window(session, type_id, from_iso, to_iso)
    print(f"  {len(proceedings)} nye saker, {len(journals)} nye journalposter i vinduet")

    journals_by_proceeding = defaultdict(list)
    for j in journals:
        pid = (j.get("proceeding") or {}).get("id")
        journals_by_proceeding[pid].append(j)
    nye_sak_ids = {p["id"] for p in proceedings}

    nye_saker = [
        build_sak(p, [build_journalpost(j) for j in journals_by_proceeding.get(p["id"], [])],
                  type_id, sakstype)
        for p in proceedings
    ]

    nye_journalposter = []
    for pid, docs in journals_by_proceeding.items():
        if pid in nye_sak_ids or not pid:
            continue
        stub = docs[0].get("proceeding") or {}
        nye_journalposter.append({
            "parent_sak": parent_sak_ref(session, type_id, stub),
            "nye_journalposter": [build_journalpost(j) for j in docs],
        })

    write_output(nye_saker, nye_journalposter, to_iso, output_dir)
