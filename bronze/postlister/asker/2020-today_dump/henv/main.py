import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# Full-dump av Asker-henvendelser fra Innsyn (GraphQL, samme plattform som
# Trondheim/Drammen - "SearchPostJournal" er eneste queryen som fungerer,
# maks limit=100/felt, query-teksten må sendes byte-eksakt).
#
# Asker har INGEN egen GraphQL-type for "Henvendelsessak" (verifisert
# tidligere: kun 6 typer finnes totalt på denne portalen, ingen av dem heter
# Henvendelse). Bekreftet manuelt (bruker sjekket mot faktiske saker): saker
# av typen eByggesak UTEN noen eiendomsreferanse i det hele tatt (verken fra
# Askers eget propertyIdentifications-felt eller fra gnr/bnr parset direkte
# fra tittelen, se parse_title_gnr_bnr) er i praksis henvendelser/generelle
# forespørsler, ikke reelle byggesaker. Dette scriptet henter derfor NØYAKTIG
# samme rådata som ../bygg/main.py (samme BYGG_TYPE_ID, samme journalspørring)
# og filtrerer til KUN de sakene der har_eiendomsreferanse er False.
#
# MERK bevisst valg: ../bygg/main.py er UENDRET og tar fortsatt med ALLE
# eByggesak-saker (inkludert disse henvendelsene) - denne mappa er en
# supplerende, filtrert VISNING, ikke en eksklusiv flytting. En henvendelse
# vil derfor finnes i BÅDE bygg- og henv-outputen samtidig (bevisst valgt av
# bruker for å unngå å endre eksisterende bygg-tall).
#
# VIKTIG BUG i dette API-et (verifisert ved testing, samme som Drammen):
# "proceedings"-spørringens fromDate/toDate-filter beregner totalCount riktig,
# men nodes-lista som returneres er IKKE filtrert på det tidsrommet (kan
# inneholde saker fra helt andre år - bekreftet: spurte etter 2026-07-13,
# fikk saker helt tilbake til 2026-06-15 i retur). "journals"-spørringens
# journalFromDate/journalToDate virker derimot korrekt, og eksakt
# saksnummer-match på proceedings er også pålitelig. Vi kan derfor ikke
# bisecte saker direkte (slik det først ble bygget, og slik Trondheim faktisk
# fungerer) - i stedet henter vi journalposter dato-basert, og slår opp hver
# unike sak de tilhører separat (parallellisert, se resolve_proceedings).
#
# Asker-arkivets fragment mangler feltet "mainDocumentNotPublishedReason" som
# finnes i Trondheim/Drammen sitt schema (fjernet fra spørringen under).

BASE_URL = "https://asker-bygg.innsynsportal.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "d3aab42c-a204-438d-8e99-5189ae2ff468"       # byggesak-postliste
BYGG_TYPE_ID = "9facfe95-81b3-4841-a535-e51ac6810155"  # "eByggesak"-type UUID
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Asker"
KOMMUNE_NR = 3203

START_DATE = date(2020, 1, 7)   # eldste byggesak funnet i portalen (20/30)

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4
RESOLVE_WORKERS = 8    # parallelle saksoppslag (ingen batch-endpoint finnes for eksakt-match)

HERE = Path(__file__).parent
OUTPUT_FILE = HERE / "asker_henv.json"

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
            "journalProceedingWhere": {"typeIdIn": [BYGG_TYPE_ID]},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(sequence_number=None, department_id_in=None):
    return {
        "listId": LIST_ID,
        "typeIdIn": [BYGG_TYPE_ID],
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
    (dict id -> node) i stedet for å returnere."""
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


def _collect_leading_gnr_bnr(tittel):
    """Samle ALLE ledende gnr/bnr-referanser i tittelen, uansett om de er
    komma-/"og"-separert INNENFOR ett segment (f.eks. "56/116, 58/325") eller
    bindestrek-separert på TVERS av flere segmenter (f.eks. "41/1 - 41/466 -
    Skjøtselssøknader..."). Brukes KUN til matrikkel-utrekk (ikke adresse -
    se extract_adresse for hvorfor adresseutrekket bruker en mer forsiktig
    ett-stegs variant i stedet)."""
    segments = _SEPARATOR.split(tittel)
    pairs = []
    for seg in segments:
        m = _GNR_BNR_LIST_PREFIX.match(seg)
        if not m:
            break
        for group in _GNR_BNR_GROUP.findall(m.group(0)):
            gnr, bnr = group.split("/")[:2]
            if (gnr, bnr) == ("0", "0") or (gnr, bnr) in pairs:
                continue
            pairs.append((gnr, bnr))
        if seg[m.end():].strip():
            break
    return pairs


def parse_title_gnr_bnr(tittel):
    """Trekk ut gnr/bnr fra tittelen, som FALLBACK når Askers eget API mangler
    propertyIdentifications for saken. Håndterer en ledende liste med flere
    eiendommer adskilt med komma, "og", ELLER bindestrek på tvers av flere
    segmenter (f.eks. "56/116, 58/325", "32/307 og 32/309", eller "41/1 -
    41/466 - Skjøtselssøknader...", se _collect_leading_gnr_bnr), og
    feste/seksjonsnummer (gnr/bnr/feste/seksjon) ved å bruke de to første
    tallene som gnr/bnr. Plassholderen "0/0" (brukt for infrastruktursaker
    uten egen eiendom, f.eks. bruer) er ikke en reell eiendom og utelates.
    Faller videre tilbake på et usøkt søk i de første 60 tegnene av tittelen
    (for å unngå falske treff lenger ute i beskrivelsen) for titler der
    adressen kommer FØR gnr/bnr, f.eks. "Myrveien 26A 40/604/0/12
    Forespørsel...", som verken den ledende-prefiks- eller bindestrek-
    varianten fanger opp.

    For dette henv-scriptet er det AKKURAT når denne funksjonen (og API-ets
    eget propertyIdentifications-felt) IKKE finner noe gnr/bnr i det hele
    tatt at saken regnes som en henvendelse - se filteret i run()."""
    if not tittel:
        return []
    pairs = _collect_leading_gnr_bnr(tittel)
    if pairs:
        return pairs
    m2 = _GNR_BNR_GROUP.search(tittel[:60])
    if not m2:
        return []
    for group in _GNR_BNR_GROUP.findall(m2.group(0)):
        gnr, bnr = group.split("/")[:2]
        if (gnr, bnr) == ("0", "0") or (gnr, bnr) in pairs:
            continue
        pairs.append((gnr, bnr))
    return pairs


def gnr_bnr_matrikkel(property_identifications, tittel=None):
    """Dedupe (propertyNr, useNr)-par (de dubleres i lista) -> gnr_bnr +
    matrikkelnr. Faller tilbake på gnr/bnr parset direkte fra tittelen
    (parse_title_gnr_bnr) når API-et ikke har noen propertyIdentifications
    for saken. Returnerer også om matrikkelen kom fra tittel-fallback eller
    fra API-et, slik at kilden er sporbar i output."""
    seen = []
    for pid in property_identifications or []:
        pair = (pid.get("propertyNr"), pid.get("useNr"))
        if pair not in seen and pair[0] is not None and pair[1] is not None:
            seen.append(pair)
    fra_tittel = False
    if not seen and tittel:
        seen = parse_title_gnr_bnr(tittel)
        fra_tittel = bool(seen)
    if not seen:
        return None, None, False
    gnr_bnr = "; ".join(f"{gnr}/{bnr}" for gnr, bnr in seen)
    matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{gnr}/{bnr}" for gnr, bnr in seen)
    return gnr_bnr, matrikkelnr, fra_tittel


# Tittelen er normalt "gnr/bnr[/feste/seksjon][, gnr/bnr...] Adresse -
# Beskrivelse", f.eks. "335/345 Fabrikkveien 34 - Innsyn i siste godkjente
# bygningstegninger". Kilden blander ofte inn ekstra ikke-adresse-info som
# gjør adressen usøkbar i eiendomsregisteret hvis den blir stående: parenteser
# med gammelt gnr/bnr eller gammelt navn ("(tidl. X)"), parsell-/leilighets-/
# hus-/bygg-nummer, "m.fl."-markøren, rene bygningslabels ("Hus A", "BT4"),
# samt beskrivelsestekst uten noen reell adresse i det hele tatt (f.eks.
# "Mulig ulovlighet", "Forespørsel om ..." - svært vanlig nettopp for
# henvendelser, som er hele poenget med denne mappa). extract_adresse() rydder
# bort alt dette og returnerer None i stedet for å gjette når ingenting reelt
# gjenstår. Kilden bruker begge separator-varianter (vanlig bindestrek "-" og
# tankestrek "–").
_SEPARATOR = re.compile(r"\s[-–]\s")
_GNR_BNR_GROUP = re.compile(r"\d+(?:/\d+){1,3}")
_GNR_BNR_LIST_PREFIX = re.compile(
    r"^\d+(?:/\d+){1,3}(?:(?:\s*,\s*|\s+og\s+)\d+(?:/\d+){1,3})*\s*"
)

_PAREN = re.compile(r"\s*\([^)]*\)\s*")
_TRAILING_LEIL = re.compile(r",?\s*leil(?:ighet)?\.?\s*\S+\s*$", re.IGNORECASE)
_TRAILING_PARSELL = re.compile(r",?\s*parsell\s+\w+(?:\s*(?:og|,)\s*\w+)*\s*$", re.IGNORECASE)
_TRAILING_HUS_BYGG = re.compile(r"\s+(?:hus|bygg)\s+\S+\s*$", re.IGNORECASE)
_MFL_EDGE = re.compile(r"(^m\.fl\.?\s+|\s+m\.fl\.?$)", re.IGNORECASE)
_BARE_LABEL = re.compile(
    r"^(?:(?:hus|bygg|blokk|byggetrinn)\b(?:\s+\S+)?|bt\d*)$", re.IGNORECASE
)
_ASTERISKS_ONLY = re.compile(r"^[\*\s]+$")
# Ord som markerer at "adressen" egentlig er en sakstype-/beskrivelsestekst
# uten noen reell adresse (bekreftet gjennom stikkprøve på ekte titler - disse
# ordene opptrer ALDRI som starten på en reell gateadresse i kilden).
_DESCRIPTION_MARKERS = re.compile(
    r"^(mulig|forespørsel|forslag|søknad|klage|varsel|melding|dispensasjon|"
    r"anmodning|henvendelse|riving|anleggelse|tomteopparbeidelse|utvidelse|"
    r"uteservering|ulovlighetsoppfølging|fasadeendring|tilbygg|nybygg|"
    r"påbygg|bruksendring|samlesak|innsyn|tillatelse|igangsetting|"
    r"ferdigattest|rammetillatelse)\b",
    re.IGNORECASE,
)


_OG_SPLIT = re.compile(r"^(.+?)\s+og\s+(.+)$")
_BARE_NUMBER_SUFFIX = re.compile(r"^\d+[A-Za-zæøåÆØÅ]?$")
_STREET_AND_NUMBER = re.compile(r"^(.*\D)\s*(\d+[A-Za-zæøåÆØÅ]?)$")


def _split_og_adresser(s):
    """Hvis kandidaten er to adresser bundet sammen med "og" (f.eks.
    "Langkroken 8E og 8F" eller "Vøyenmyra 28 og Vøyenmyra 30"), del dem opp
    til en semikolon-separert streng - samme konvensjon som gnr_bnr og
    matrikkelnr allerede bruker for flere verdier på én sak."""
    m = _OG_SPLIT.match(s)
    if not m:
        return s
    left, right = m.group(1).strip(), m.group(2).strip()
    if _BARE_NUMBER_SUFFIX.match(right):
        sm = _STREET_AND_NUMBER.match(left)
        if sm:
            street, left_num = sm.group(1).strip(), sm.group(2)
            return f"{street} {left_num}; {street} {right}"
    return f"{left}; {right}"


def _clean_adresse(kandidat):
    """Rydder en rå adresse-kandidat (se moduldoc over) og returnerer None i
    stedet for å gjette hvis ingenting reelt gjenstår."""
    s = _PAREN.sub(" ", kandidat)
    s = _TRAILING_LEIL.sub("", s)
    s = _TRAILING_PARSELL.sub("", s)
    s = _TRAILING_HUS_BYGG.sub("", s)
    s = _MFL_EDGE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip(" ,-")
    if not s or s.lower().rstrip(".") in ("m.fl", "mfl"):
        return None
    if _BARE_LABEL.match(s) or _ASTERISKS_ONLY.match(s):
        return None
    if _DESCRIPTION_MARKERS.match(s):
        return None
    return _split_og_adresser(s)


_BARE_GNR_BNR = re.compile(r"^\d+(?:/\d+){1,3}$")


def extract_adresse(tittel):
    """Skiller på første "-"/"–", fjerner en evt. ledende (komma- eller
    "og"-separert) liste med gnr/bnr-tallprefiks, og rydder resten via
    _clean_adresse(). Hvis gnr/bnr-prefikset spiser HELE det første segmentet
    (dvs. gnr/bnr er bindestrek-separert fra resten, f.eks. "41/59 -
    Brønnøyveien 9 - Ulovlig hogst..."), prøver vi ETT ekstra steg til neste
    segment - men kun hvis DET segmentet ikke selv er en ren gnr/bnr-
    referanse (da ville vi bare kappet oss videre inn i beskrivelsen, som er
    for usikkert å gjette på - se f.eks. "41/1 - 41/466 -
    Skjøtselssøknader..." der det ikke finnes noen adresse i det hele tatt).
    Ingen separator funnet, eller ingenting reelt igjen etter rydding ->
    ingen adresse i kilden, returner None i stedet for å gjette."""
    if not tittel:
        return None
    segments = _SEPARATOR.split(tittel)
    if len(segments) < 2:
        return None
    kandidat = _GNR_BNR_LIST_PREFIX.sub("", segments[0])
    if not kandidat.strip():
        neste = segments[1].strip()
        if _BARE_GNR_BNR.match(neste):
            return None
        kandidat = neste
    return _clean_adresse(kandidat)


def sak_url(saksnummer):
    params = json.dumps({"proceeding": {"sequenceNumber": saksnummer, "typeIdIn": [BYGG_TYPE_ID]}})
    return f"{PORTAL_URL}?params={quote(params, safe='')}"


def build_vedlegg(doc):
    typ = (doc.get("type") or {}).get("name")
    doc_id = doc.get("id")
    return {
        "tittel": doc.get("title"),
        "kategori": "Hoveddokument" if typ == "H" else "Vedlegg",
        "type": typ,
        "dokument_id": doc_id,
        "rekkefolge": doc.get("order"),
        "url": DOCUMENT_URL.format(doc_id) if doc_id and not doc.get("classified") else None,
    }


def build_journalpost(journal):
    return {
        "identifier": journal.get("id"),
        "dokument_id": journal.get("archiveId"),
        "sekvensnummer": journal.get("sequenceNumber"),
        "tittel": journal.get("title"),
        "type": (journal.get("type") or {}).get("name"),
        "type_beskrivelse": (journal.get("type") or {}).get("description"),
        "status": (journal.get("status") or {}).get("name"),
        "dato": journal.get("journalDate"),
        "dokumentdato": journal.get("documentDate"),
        "avsender": journal.get("senders") or [],
        "mottaker": journal.get("recipients") or [],
        "saksbehandler": journal.get("caseworkers") or [],
        "avdeling": (journal.get("department") or {}).get("name"),
        "publisert": not journal.get("unpublished", False),
        "gradert": bool(journal.get("classified")),
        "vedlegg": [build_vedlegg(d) for d in journal.get("documents") or []],
    }


def build_sak(proceeding, journalposter):
    gnr_bnr, matrikkelnr, matrikkel_fra_tittel = gnr_bnr_matrikkel(
        proceeding.get("propertyIdentifications"), proceeding.get("title")
    )
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
        "matrikkel_fra_tittel": matrikkel_fra_tittel,
        "har_eiendomsreferanse": bool(gnr_bnr),
        "dato": proceeding.get("date"),
        "saksbehandler": proceeding.get("caseworkers") or [],
        "avdeling": (proceeding.get("department") or {}).get("name"),
        "arkivsystem": (proceeding.get("archiveSystem") or {}).get("name"),
        "gradert": bool(proceeding.get("classified")),
        "url": sak_url(saksnummer) if saksnummer else PORTAL_URL,
        "journalposter": journalposter,
    }


def to_henvendelse(sak):
    """Overstyr sakstype til "Henvendelse" - den avledede klassifiseringen
    (se topp-kommentar for hvorfor eByggesak-saker uten eiendomsreferanse
    regnes som henvendelser). Gir konsistens med Trondheim/Bergen sine
    henv-mapper, der sakstype naturlig er "Henvendelse" fra deres egen API.
    Askers rå kildeverdi ("eByggesak") går ikke tapt - den beholdes i
    sakstype_kilde for sporbarhet."""
    ny = dict(sak)
    ny["sakstype_kilde"] = ny["sakstype"]
    ny["sakstype"] = "Henvendelse"
    return ny


def save(saker):
    tmp = OUTPUT_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(OUTPUT_FILE)


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


def run(start=START_DATE, end=None):
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    all_journals = {}
    print(f"Henter journalposter {start} .. {end} (rekursiv dato-halvering)…")
    fetch_journals_range(session, start, end, all_journals)

    proceedings_by_seq = resolve_proceedings(session, all_journals)
    journalposter_by_seq, ukjent_sak = group_journalposter(all_journals, proceedings_by_seq)
    alle_saker = [build_sak(p, journalposter_by_seq.get(seq, []))
                  for seq, p in proceedings_by_seq.items()]

    # Filtrer til KUN henvendelser: eByggesak-saker uten noen eiendomsreferanse
    # i det hele tatt (verken fra API-et eller fra tittel-fallback). Se
    # topp-kommentaren for hvorfor dette er den beste tilgjengelige
    # avgrensningen på en portal uten egen Henvendelsessak-type.
    saker = [to_henvendelse(s) for s in alle_saker if not s["har_eiendomsreferanse"]]
    save(saker)

    n_jp = sum(len(s["journalposter"]) for s in saker)
    print(f"Ferdig: {len(saker)} henvendelser (av {len(alle_saker)} eByggesak-saker totalt), "
          f"{n_jp} journalposter -> {OUTPUT_FILE} ({time.time() - start_time:.0f}s)")
    if ukjent_sak:
        print(f"OBS: {len(ukjent_sak)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")


if __name__ == "__main__":
    import sys
    # Valgfrie argumenter:
    #   python3 main.py                        -> full dump fra 2020-01-07
    #   python3 main.py 30                      -> kun siste 30 dager
    #   python3 main.py 2023-01-01 2023-03-31   -> egendefinert datointervall
    if len(sys.argv) > 2:
        run(start=date.fromisoformat(sys.argv[1]), end=date.fromisoformat(sys.argv[2]))
    elif len(sys.argv) > 1:
        n_days = int(sys.argv[1])
        end = datetime.now(ZoneInfo("Europe/Oslo")).date()
        start = end - timedelta(days=n_days - 1)
        run(start=start, end=end)
    else:
        run()
