import json
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Full-dump av Trondheim-ulovlighetssaker fra Innsyn (GraphQL, samme plattform
# som ../bygg/main.py).
#
# I motsetning til Asker har Trondheim sin EGEN GraphQL-type for dette
# ("ulovlighetssak", type-id under) - ingen heuristikk trengs. Bekreftet ved
# usortert sampling av nyeste 100 saker uten typefilter: portalen har (minst)
# typene Byggesak, Delingssak, delesak, Plansak, Henvendelse, ulovlighetssak
# og "Ingen registrering". Denne mappa dekker KUN ulovlighetssak-typen, på
# samme måte som ../bygg/main.py kun dekker Byggesak og ../henv/main.py kun
# Henvendelse - egen mappe+fil per sakstype, som i Asker/Bergen.
#
# Sekvensnummer har prefiks "ULOV-" i nyere saker (f.eks. ULOV-26/81993), men
# eldre saker (fra oppstarten i 2017) deler samme "BYGG-"-prefiks som
# byggesaker uansett type - prefikset alene kan altså ikke brukes til å
# skille sakstype, kun type.id/type.name-feltet fra API-et.
#
# VIKTIG BUG i dette API-et (samme som bekreftet for ../bygg/main.py, Asker og
# Drammen): "proceedings"-spørringens dato-filter (fromDate/toDate) beregner
# totalCount riktig for det spurte vinduet, men nodes-lista inneholder likevel
# saker fra andre datoer (bekreftet ved testing: enkeltdags-spørring for
# ulovlighetssak 2026-07-09 ga totalCount=6, men nodes inkluderte saker helt
# tilbake til 2020). De første nodene i lista treffer likevel korrekt for det
# spurte vinduet før den "lekker" utover - siden dag-for-dag-metoden (samme
# som ../bygg/main.py) går gjennom HVER dag i hele perioden, blir hver sak
# uansett fanget opp korrekt på sin egentlige dag, og eventuell "lekkasje" fra
# andre dager gir bare ufarlige duplikater (de dedupliseres på sak-id).
#
# Eldste ulovlighetssak funnet i portalen: 2017-04-27 (BYGG-17/80032).

BASE_URL = "https://trondheim.innsynsportal.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "fc010204-2c07-4f3c-a8ef-879ec218111c"       # byggesak-postliste (delt med bygg/henv)
ULOV_TYPE_ID = "84e9e827-13cc-42d6-a1ea-929c820dfe58"  # "ulovlighetssak"-type UUID
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Trondheim"
KOMMUNE_NR = 5001

START_DATE = date(2017, 4, 27)   # eldste ulovlighetssak funnet i portalen (BYGG-17/80032)

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4

HERE = Path(__file__).parent
OUTPUT_FILE = HERE / "trondheim_ulov.json"

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
            "journalProceedingWhere": {"typeIdIn": [ULOV_TYPE_ID]},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(day_iso, department_id_in=None):
    return {
        "listId": LIST_ID,
        "typeIdIn": [ULOV_TYPE_ID],
        "year": None,
        "fromDate": day_iso,
        "toDate": day_iso,
        "departmentIdIn": department_id_in,
    }


def base_journals_where(day_iso, department_id_in=None):
    return {
        "listId": LIST_ID,
        "journalFromDate": day_iso,
        "journalToDate": day_iso,
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


def fetch_day(session, day_iso):
    """Hent alle saker + journalposter for én dag. Prøver departement-etterfyll
    hvis en av delene ble kuttet av MAX_LIMIT-grensen."""
    data = search_post_journal(
        session, plimit=MAX_LIMIT, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(day_iso),
        journals_where=base_journals_where(day_iso),
    )
    proceedings = list(data["proceedings"]["nodes"])
    p_total = data["proceedings"]["totalCount"]
    journals = list(data["journals"]["nodes"])
    j_total = data["journals"]["totalCount"]

    journals = _refill_via_departments(
        journals, j_total,
        lambda dept: search_post_journal(
            session, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(day_iso),
            journals_where=base_journals_where(day_iso, department_id_in=[dept]),
            include_count=False,
        )["journals"]["nodes"],
    )
    proceedings = _refill_via_departments(
        proceedings, p_total,
        lambda dept: search_post_journal(
            session, plimit=MAX_LIMIT, jlimit=1,
            proceedings_where=base_proceedings_where(day_iso, department_id_in=[dept]),
            journals_where=base_journals_where(day_iso),
            include_count=False,
        )["proceedings"]["nodes"],
    )

    if len(proceedings) < p_total:
        print(f"    ADVARSEL {day_iso}: fant kun {len(proceedings)}/{p_total} saker "
              f"(servergrense, ingen flere avdelinger å prøve)")
    if len(journals) < j_total:
        print(f"    ADVARSEL {day_iso}: fant kun {len(journals)}/{j_total} journalposter "
              f"(servergrense, ingen flere avdelinger å prøve)")

    return proceedings, journals


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
    params = json.dumps({"proceeding": {"sequenceNumber": saksnummer, "typeIdIn": [ULOV_TYPE_ID]}})
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


def save(saker):
    tmp = OUTPUT_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(OUTPUT_FILE)


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
            by_sak[pid].append(build_journalpost(j))
        else:
            ukjent.append(j)
    return by_sak, ukjent


def run(start=START_DATE, end=None, save_every=30):
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    # "proceeding.date" er sakens siste aktivitetsdato, ikke opprettelsesdato,
    # så en sak kan dukke opp flere ganger over ulike dager – vi overskriver
    # bare med nyeste versjon (all_proceedings[id] = p), ingen saker mistes.
    all_proceedings = {}
    all_journals = []
    days = list(daterange(start, end))
    print(f"Henter {len(days)} dager ({start} .. {end})…")

    for i, d in enumerate(days, 1):
        day_iso = d.isoformat()
        proceedings, journals = fetch_day(session, day_iso)
        for p in proceedings:
            all_proceedings[p["id"]] = p
        all_journals.extend(journals)

        if i % 20 == 0 or i == len(days):
            print(f"  dag {i}/{len(days)} ({day_iso}): "
                  f"{len(all_proceedings)} saker, {len(all_journals)} journalposter totalt")

        if i % save_every == 0:
            journalposter_by_sak, _ = group_journalposter(all_journals, all_proceedings)
            saker = [build_sak(p, journalposter_by_sak.get(pid, []))
                     for pid, p in all_proceedings.items()]
            save(saker)
            print(f"    (lagret delresultat: {len(saker)} saker)")

    journalposter_by_sak, ukjent_sak = group_journalposter(all_journals, all_proceedings)
    saker = [build_sak(p, journalposter_by_sak.get(pid, []))
             for pid, p in all_proceedings.items()]
    save(saker)

    n_jp = sum(len(s["journalposter"]) for s in saker)
    print(f"Ferdig: {len(saker)} saker, {n_jp} journalposter -> {OUTPUT_FILE} "
          f"({time.time() - start_time:.0f}s)")
    if ukjent_sak:
        print(f"OBS: {len(ukjent_sak)} journalposter hørte til saker utenfor "
              f"{start}..{end} og ble ikke tatt med (utenfor dumpens periode).")


if __name__ == "__main__":
    import sys
    # Valgfrie argumenter:
    #   python3 main.py                        -> full dump fra 2017-04-27
    #   python3 main.py 30                      -> kun siste 30 dager
    #   python3 main.py 2023-01-01 2023-03-31   -> eksplisitt datointervall (start slutt)
    if len(sys.argv) == 3:
        run(start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    elif len(sys.argv) == 2:
        n_days = int(sys.argv[1])
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        start = end - timedelta(days=n_days - 1)
        run(start=start, end=end)
    else:
        run()
