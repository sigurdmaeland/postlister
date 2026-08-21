"""Delt bibliotek for Asker bygg/ulov/henv-scraperne.

Asker kjører "360 Plan & Build" via innsynsportal.no (GraphQL) - samme
plattform som Trondheim/Tromsø/Drammen, men med en VIKTIG BUG som gjør
fremgangsmåten annerledes: "proceedings"-spørringens fromDate/toDate-filter
beregner totalCount riktig, men nodes-lista som returneres er IKKE filtrert
på det tidsrommet (kan inneholde saker fra helt andre år - bekreftet:
spurte etter 2026-07-13, fikk saker helt tilbake til 2026-06-15 i retur).
"journals"-spørringens journalFromDate/journalToDate virker derimot
korrekt, og eksakt saksnummer-match på proceedings er også pålitelig. Vi
kan derfor ikke bisecte SAKER dag-for-dag (slik Trondheim gjør) - i stedet
henter vi JOURNALPOSTER dato-basert (rekursiv dato-halvering ned til under
MAX_LIMIT=100), og slår opp hver unike sak de tilhører separat via eksakt
saksnummer-match (parallellisert, se resolve_proceedings). Samme bug/samme
løsning gjelder Drammen.

Tre kilder:
  bygg  - egen GraphQL-type ("eByggesak")
  ulov  - egen GraphQL-type ("Ulovlighetsoppfølging")
  henv  - INGEN egen type finnes (portalen har kun 6 typer totalt, ingen
          heter Henvendelse). eByggesak-saker UTEN noen eiendomsreferanse i
          det hele tatt (verken fra API-ets propertyIdentifications eller
          fra gnr/bnr parset fra tittelen) regnes i praksis som
          henvendelser - se KILDER["henv"]["derived_from"] og
          to_henvendelse(). Dette er en supplerende, FILTRERT VISNING av
          bygg-dataene, ikke en eksklusiv flytting: en henvendelse finnes i
          BÅDE bygg- og henv-outputen samtidig (bevisst valgt for å ikke
          endre eksisterende bygg-tall). "Tilsyn" er kun et avdelingsnavn
          her, ikke en egen sakstype.

Asker-arkivets fragment mangler feltet "mainDocumentNotPublishedReason" som
finnes hos Trondheim/Drammen (fjernet fra QUERY under).

Adresseformat: tittelen er normalt "gnr/bnr[/feste/seksjon][, gnr/bnr...]
Adresse - Beskrivelse", f.eks. "335/345 Fabrikkveien 34 - Innsyn i siste
godkjente bygningstegninger". Kilden blander ofte inn ekstra ikke-adresse-
info (gammelt gnr/bnr i parentes, parsell-/leilighets-/hus-/bygg-nummer,
"m.fl.", rene bygningslabels, eller ren beskrivelsestekst uten adresse i det
hele tatt) - extract_adresse() rydder alt dette bort og returnerer None i
stedet for å gjette. Separator er både vanlig bindestrek "-" og tankestrek
"-". API-ets propertyIdentifications oppgir KUN gnr/bnr, aldri feste/
seksjon - gnr_bnr_matrikkel() beriker derfor matrikkelnr med feste/seksjon
parset fra tittelen når det finnes der (se parse_title_gnr_bnr).

Brukes av:
  - 2020-today_dump/{bygg,ulov,henv}/main.py (engangs historisk dump)
  - running_daily/{bygg,ulov,henv}/app/main.py (daglig endringslogg)
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://asker-bygg.innsynsportal.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "d3aab42c-a204-438d-8e99-5189ae2ff468"   # byggesak-postliste (delt for alle kilder)
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Asker"
KOMMUNE_NR = 3203

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4
RESOLVE_WORKERS = 8    # parallelle saksoppslag (ingen batch-endpoint finnes for eksakt-match)

BYGG_TYPE_ID = "9facfe95-81b3-4841-a535-e51ac6810155"   # "eByggesak"
ULOV_TYPE_ID = "3ff64378-f8e8-4b32-b712-cf8e6d8b3a3b"   # "Ulovlighetsoppfølging"

# NB: .strip() er nødvendig - serveren avviser query-tekst med avvikende
# whitespace, uansett om where-innholdet er gyldig.
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

# --------------------------------------------------------------------------- #
# KILDER
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="eByggesak",
        type_id=BYGG_TYPE_ID,
        start_date=date(2020, 1, 7),   # eldste byggesak (20/30)
        derived_from=None,
    ),
    "ulov": dict(
        sakstype_navn="Ulovlighetsoppfølging",
        type_id=ULOV_TYPE_ID,
        start_date=date(2021, 6, 16),   # eldste ulovlighetssak (21/2704)
        derived_from=None,
    ),
    "henv": dict(
        sakstype_navn="Henvendelse",
        type_id=BYGG_TYPE_ID,   # samme rådata som bygg - se modul-docstring
        start_date=date(2020, 1, 7),
        derived_from="bygg",   # filtrert visning, ikke egen GraphQL-type
    ),
}


# --------------------------------------------------------------------------- #
# gnr/bnr/matrikkelnr - API-et + tittel-fallback/berikelse
# --------------------------------------------------------------------------- #
_SEPARATOR = re.compile(r"\s[-–]\s")
_GNR_BNR_GROUP = re.compile(r"\d+(?:/\d+){1,3}")
_GNR_BNR_LIST_PREFIX = re.compile(
    r"^\d+(?:/\d+){1,3}(?:(?:\s*,\s*|\s+og\s+)\d+(?:/\d+){1,3})*\s*"
)


def _collect_leading_gnr_bnr(tittel):
    """Samle ALLE ledende gnr/bnr[/feste/seksjon]-referanser i tittelen,
    komma-/"og"-separert INNENFOR ett segment eller bindestrek-separert på
    TVERS av flere segmenter (f.eks. "41/1 - 41/466 - Skjøtselssøknader...").
    Returnerer (gnr, bnr, feste, seksjon)-tupler. Brukes KUN til matrikkel-
    utrekk (extract_adresse bruker en mer forsiktig ett-stegs variant)."""
    segments = _SEPARATOR.split(tittel)
    entries = []
    for seg in segments:
        m = _GNR_BNR_LIST_PREFIX.match(seg)
        if not m:
            break
        for group in _GNR_BNR_GROUP.findall(m.group(0)):
            nums = group.split("/")
            gnr, bnr = nums[0], nums[1]
            feste = nums[2] if len(nums) > 2 else None
            seksjon = nums[3] if len(nums) > 3 else None
            if (gnr, bnr) == ("0", "0") or (gnr, bnr, feste, seksjon) in entries:
                continue
            entries.append((gnr, bnr, feste, seksjon))
        if seg[m.end():].strip():
            break
    return entries


def parse_title_gnr_bnr(tittel):
    """Trekk ut gnr/bnr[/feste/seksjon] fra tittelen - FALLBACK når API-et
    mangler propertyIdentifications, og BERIKELSE av matrikkelnr selv når
    API-et har gnr/bnr (API-et oppgir aldri feste/seksjon; tittelen er
    eneste kilde til den presisjonen). Plassholderen "0/0" (infrastruktur-
    saker uten egen eiendom) utelates. Faller videre tilbake på et usøkt
    søk i de første 60 tegnene for titler der adressen kommer FØR gnr/bnr
    (f.eks. "Myrveien 26A 40/604/0/12 Forespørsel...")."""
    if not tittel:
        return []
    entries = _collect_leading_gnr_bnr(tittel)
    if entries:
        return entries
    m2 = _GNR_BNR_GROUP.search(tittel[:60])
    if not m2:
        return []
    entries = []
    for group in _GNR_BNR_GROUP.findall(m2.group(0)):
        nums = group.split("/")
        gnr, bnr = nums[0], nums[1]
        feste = nums[2] if len(nums) > 2 else None
        seksjon = nums[3] if len(nums) > 3 else None
        if (gnr, bnr) == ("0", "0") or (gnr, bnr, feste, seksjon) in entries:
            continue
        entries.append((gnr, bnr, feste, seksjon))
    return entries


def gnr_bnr_matrikkel(property_identifications, tittel=None):
    """Dedupe (propertyNr, useNr)-par -> gnr_bnr + matrikkelnr, beriket med
    feste/seksjon parset fra tittelen der det finnes (se parse_title_gnr_bnr
    og modul-docstring). gnr_bnr forblir alltid det enkle gnr/bnr-paret;
    matrikkelnr får én oppføring per seksjon når tittelen skiller mellom
    flere. Returnerer også om gnr/bnr kom fra tittel-fallback eller API-et
    (fra_tittel), slik at kilden er sporbar i output."""
    api_pairs = []
    for pid in property_identifications or []:
        pair = (pid.get("propertyNr"), pid.get("useNr"))
        if pair not in api_pairs and pair[0] is not None and pair[1] is not None:
            api_pairs.append(pair)

    title_entries = parse_title_gnr_bnr(tittel) if tittel else []
    title_by_pair = {}
    for gnr, bnr, feste, seksjon in title_entries:
        if feste is not None or seksjon is not None:
            title_by_pair.setdefault((gnr, bnr), []).append((feste, seksjon))

    fra_tittel = False
    if api_pairs:
        pairs = api_pairs
    else:
        pairs = []
        for gnr, bnr, _, _ in title_entries:
            if (gnr, bnr) == ("0", "0") or (gnr, bnr) in pairs:
                continue
            pairs.append((gnr, bnr))
        fra_tittel = bool(pairs)

    if not pairs:
        return None, None, False

    gnr_bnr = "; ".join(f"{gnr}/{bnr}" for gnr, bnr in pairs)

    matrikkel_parts = []
    for gnr, bnr in pairs:
        # API-et gir propertyNr/useNr som int, tittel-parsingen alltid str.
        utvidelser = title_by_pair.get((str(gnr), str(bnr)))
        if utvidelser:
            fra_tittel = True   # flagget reflekterer matrikkelnr, ikke bare gnr_bnr
            for feste, seksjon in utvidelser:
                # feste kan finnes uten seksjon (tittelen har bare 3 ledd,
                # "210/1/36") - IKKE skriv "/None" i så fall (bekreftet bug
                # på ekte data, 145 rammede saker - se rapport). Tar med
                # kun de leddene som faktisk finnes.
                delnr = [str(gnr), str(bnr)]
                if feste is not None:
                    delnr.append(str(feste))
                if seksjon is not None:
                    delnr.append(str(seksjon))
                matrikkel_parts.append(f"{KOMMUNE_NR}-" + "/".join(delnr))
        else:
            matrikkel_parts.append(f"{KOMMUNE_NR}-{gnr}/{bnr}")
    matrikkelnr = "; ".join(matrikkel_parts)

    return gnr_bnr, matrikkelnr, fra_tittel


# --------------------------------------------------------------------------- #
# Adresse fra sakstittel
# --------------------------------------------------------------------------- #
_PAREN = re.compile(r"\s*\([^)]*\)\s*")
_TRAILING_LEIL = re.compile(r",?\s*leil(?:ighet)?\.?\s*\S+\s*$", re.IGNORECASE)
_TRAILING_PARSELL = re.compile(r",?\s*parsell\s+\w+(?:\s*(?:og|,)\s*\w+)*\s*$", re.IGNORECASE)
_TRAILING_HUS_BYGG = re.compile(r"\s+(?:hus|bygg)\s+\S+\s*$", re.IGNORECASE)
_MFL_EDGE = re.compile(r"(^m\.fl\.?\s+|\s+m\.fl\.?$)", re.IGNORECASE)
# "snr"/"fnr" (seksjonsnr/festenr) og "tomt" er matrikkel-referanser, ikke
# gatenavn - "snr 8"/"snr. 8"/"tomt 2" er samme mønster som "hus 2"/"bygg A"
# (label + evt. ett tilleggstoken), bekreftet ved stikkprøve mot hele Asker-
# arkivet (81 "snr"-, 51 "fnr"- og 9 "tomt"-forekomster, ALLTID matrikkel-
# referanser, ALDRI gatenavn).
_BARE_LABEL = re.compile(
    r"^(?:(?:hus|bygg|blokk|byggetrinn|tomt|snr|fnr)\.?(?:\s+\S+)?|bt\d*)$",
    re.IGNORECASE,
)
_ASTERISKS_ONLY = re.compile(r"^[\*\s]+$")
# Ord som markerer at "adressen" egentlig er sakstype-/beskrivelsestekst uten
# noen reell adresse (bekreftet ved stikkprøve - opptrer aldri som starten
# på en reell gateadresse i kilden). Utvidet med en full gjennomgang av alle
# ~2200 tallfrie kandidater i hele Asker-arkivet (se modul-docstring) - de
# nye ordene under dekker >1000 av disse alene ("Forhåndskonferanse" 713,
# "Innsynsbegjæring" 50, "Ettergodkjenning" 28, osv.).
_DESCRIPTION_MARKERS = re.compile(
    r"^(mulig|forespørsel|forslag|søknad|klage|klagesak|varsel|melding|"
    r"dispensasjon|anmodning|henvendelse|riving|anleggelse|"
    r"tomteopparbeidelse|utvidelse|uteservering|ulovlighetsoppfølging|"
    r"fasadeendring|fasadeskilt|tilbygg|nybygg|påbygg|bruksendring|samlesak|"
    r"innsyn|innsynsbegjæring|innsynsforespørsel|tillatelse|brukstillatelse|"
    r"igangsetting|ferdigattest|rammetillatelse|forhåndskonferanse|"
    r"ettergodkjenn\w*|korrespondansemappe|nabomerknad\w*|nabovarsel|"
    r"høring|tilsyn|terrenginngrep|terrengendring|renseanlegg|"
    r"avløpsrenseanlegg|ombygging|grunnarbeider|arealoverføring|"
    r"svømmebasseng|bekymringsmelding|vesentlig|forenklet|"
    r"ingen\s+adresse|ukjent\s+adresse)\b",
    re.IGNORECASE,
)
# En reell adresse har alltid et husnummer (se modul-docstring: formatet er
# "gnr/bnr[...] Adresse - Beskrivelse", f.eks. "Fabrikkveien 34"). Uten noe
# tall i det hele tatt er kandidaten typisk en beskrivelsessetning ("Retting
# i matrikkel", "Nytt bygg", "Sikringstiltak etter stormflo"), et bart
# gatenavn uten nummer ("Kjøyafaret", "Kirkeveien"), et sted-/anleggsnavn
# ("Dikemark sykehus", "Holtnes brygge") eller en kryssbeskrivelse ("Krysset
# Nyveien / Bergeråsen") - ingen av disse er en reell adresse (bekreftet ved
# full gjennomgang av samtlige 750 tallfrie kandidater i hele Asker-arkivet).
# ETT unntak: et gårdsnavn ("Eltorn gård", "Asker prestegård", "Søndre
# Nærsnes Hovedgård") ER i Norge en gyldig postadresse uten husnummer -
# fanges av _GARDSNAVN_UTEN_NUMMER (9 forekomster i arkivet, alle reelle
# gårder, verifisert mot ekte data).
_GARDSNAVN_UTEN_NUMMER = re.compile(r"\b(?:gård|prestegård|hovedgård)\s*$", re.IGNORECASE)

_OG_SPLIT = re.compile(r"^(.+?)\s+og\s+(.+)$")
_BARE_NUMBER_SUFFIX = re.compile(r"^\d+[A-Za-zæøåÆØÅ]?$")
_STREET_AND_NUMBER = re.compile(r"^(.*\D)\s*(\d+[A-Za-zæøåÆØÅ]?)$")
_BARE_GNR_BNR = re.compile(r"^\d+(?:/\d+){1,3}$")


def _split_og_adresser(s):
    """To adresser bundet sammen med "og" (f.eks. "Langkroken 8E og 8F")
    deles opp til en semikolon-separert streng."""
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
    """Rydder en rå adresse-kandidat (se modul-docstring) og returnerer None
    i stedet for å gjette hvis ingenting reelt gjenstår."""
    s = _PAREN.sub(" ", kandidat)
    s = _TRAILING_LEIL.sub("", s)
    s = _TRAILING_PARSELL.sub("", s)
    s = _TRAILING_HUS_BYGG.sub("", s)
    s = _MFL_EDGE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip(" ,-")
    # "m. flere" (variant av "m.fl." med mellomrom og fullt utskrevet "flere")
    # - normaliser bort punktum/mellomrom og sjekk mot samme sett.
    if not s or re.sub(r"[.\s]+", "", s.lower()) in ("mfl", "mflere"):
        return None
    if _BARE_LABEL.match(s) or _ASTERISKS_ONLY.match(s):
        return None
    if _DESCRIPTION_MARKERS.match(s):
        return None
    # En reell adresse har alltid et husnummer - uten tall er kandidaten
    # enten en beskrivelsessetning, et bart gatenavn, et sted-/anleggsnavn
    # eller en kryssbeskrivelse, ikke en reell adresse (se
    # _GARDSNAVN_UTEN_NUMMER over for det ene bekreftede unntaket).
    if not re.search(r"\d", s) and not _GARDSNAVN_UTEN_NUMMER.search(s):
        return None
    return _split_og_adresser(s)


def extract_adresse(tittel):
    """Skiller på første "-"/"–", fjerner et evt. ledende gnr/bnr-prefiks, og
    rydder resten via _clean_adresse(). Hvis gnr/bnr-prefikset spiser HELE
    det første segmentet (bindestrek-separert fra resten, f.eks. "41/59 -
    Brønnøyveien 9 - Ulovlig hogst..."), prøves ETT ekstra steg til neste
    segment - men kun hvis det segmentet ikke selv er en ren gnr/bnr-
    referanse. Returnerer None hvis ingen separator finnes eller ingenting
    reelt gjenstår etter rydding."""
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


def search_post_journal(session, *, type_id, plimit, jlimit, proceedings_where, journals_where,
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
            "journalProceedingWhere": {"typeIdIn": [type_id]},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(type_id, sequence_number=None, department_id_in=None):
    return {
        "listId": LIST_ID,
        "typeIdIn": [type_id],
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
    """Hvis 'items' ble kuttet av MAX_LIMIT-grensen (færre enn 'total'), prøv
    å hente resten ved å filtrere per verdi av 'group_key' (department/type)
    blant dem vi allerede har sett."""
    if len(items) >= total:
        return items
    values = sorted({it[group_key]["id"] for it in items if it.get(group_key)})
    by_id = {it["id"]: it for it in items}
    for val in values:
        for extra in refetch(val):
            by_id[extra["id"]] = extra
    return list(by_id.values())


def _fetch_journals_leaf(session, type_id, from_iso, to_iso, all_journals):
    """Hent journalposter for et vindu lite nok til å ligge innenfor
    MAX_LIMIT. Prøver avdeling- og deretter type-etterfyll ved kutt."""
    data = search_post_journal(
        session, type_id=type_id, plimit=1, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(type_id),
        journals_where=base_journals_where(from_iso, to_iso),
    )
    journals = list(data["journals"]["nodes"])
    j_total = data["journals"]["totalCount"]

    journals = _refill_via(
        journals, j_total, "department",
        lambda dept: search_post_journal(
            session, type_id=type_id, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(type_id),
            journals_where=base_journals_where(from_iso, to_iso, department_id_in=[dept]),
            include_count=False,
        )["journals"]["nodes"],
    )
    journals = _refill_via(
        journals, j_total, "type",
        lambda tid: search_post_journal(
            session, type_id=type_id, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(type_id),
            journals_where=base_journals_where(from_iso, to_iso, type_id_in=[tid]),
            include_count=False,
        )["journals"]["nodes"],
    )
    if len(journals) < j_total:
        print(f"    ADVARSEL {from_iso}..{to_iso}: fant kun {len(journals)}/{j_total} journalposter "
              f"(servergrense, ingen flere avdelinger/typer å prøve)")
    for j in journals:
        all_journals[j["id"]] = j


def fetch_journals_range(session, type_id, from_date, to_date, all_journals):
    """Rekursiv dato-halvering på journalposter (eneste pålitelige
    datofiltrering, se modul-docstring). Fyller all_journals (dict
    id -> node) i stedet for å returnere."""
    from_iso, to_iso = from_date.isoformat(), to_date.isoformat()
    data = search_post_journal(
        session, type_id=type_id, plimit=1, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(type_id),
        journals_where=base_journals_where(from_iso, to_iso),
    )
    j_total = data["journals"]["totalCount"]

    if j_total > MAX_LIMIT and from_date != to_date:
        mid = from_date + (to_date - from_date) // 2
        fetch_journals_range(session, type_id, from_date, mid, all_journals)
        fetch_journals_range(session, type_id, mid + timedelta(days=1), to_date, all_journals)
        return

    _fetch_journals_leaf(session, type_id, from_iso, to_iso, all_journals)
    if j_total:
        print(f"  {from_iso}..{to_iso}: {j_total} journalposter")


def resolve_proceedings(session, type_id, journals):
    """Slå opp full sak-info for hver unike sak journalpostene tilhører
    (proceedings-datofilteret er upålitelig, se modul-docstring - eksakt
    saksnummer-match er derimot pålitelig)."""
    seqs = sorted({(j.get("proceeding") or {}).get("sequenceNumber") for j in journals} - {None})

    def lookup(seq):
        data = search_post_journal(
            session, type_id=type_id, plimit=3, jlimit=1,
            proceedings_where=base_proceedings_where(type_id, sequence_number=seq),
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


def build_sak(proceeding, journalposter, type_id):
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
        "url": sak_url(type_id, saksnummer) if saksnummer else PORTAL_URL,
        "journalposter": journalposter,
    }


def to_henvendelse(sak):
    """Overstyr sakstype til "Henvendelse" - den avledede klassifiseringen
    (se modul-docstring for hvorfor eByggesak-saker uten eiendomsreferanse
    regnes som henvendelser). Gir konsistens med Trondheim sin egen
    Henvendelse-type. Askers rå kildeverdi ("eByggesak") beholdes i
    sakstype_kilde for sporbarhet."""
    ny = dict(sak)
    ny["sakstype_kilde"] = ny["sakstype"]
    ny["sakstype"] = "Henvendelse"
    return ny


def kilde_type_id(kilde_key):
    """Kilde + dens faktiske type_id, via 'derived_from' for avledede kilder
    som "henv" (se modul-docstring)."""
    kilde = KILDER[kilde_key]
    fetch_key = kilde["derived_from"] or kilde_key
    return kilde, KILDER[fetch_key]["type_id"]


def filter_avledet(kilde, alle_saker):
    """Filtrer til avledet visning (se to_henvendelse()) hvis 'derived_from'
    er satt, ellers uendret."""
    if not kilde["derived_from"]:
        return alle_saker
    return [to_henvendelse(s) for s in alle_saker if not s["har_eiendomsreferanse"]]


def group_journalposter(all_journals, proceedings_by_seq):
    """Grupper journalposter på sak (via saksnummer fra journalpostens
    embedded 'proceeding'-felt - dette feltet er alltid korrekt, i motsetning
    til proceedings-spørringens datofilter). Journalposter uten en løst sak
    (oppslag feilet, f.eks. gradert) returneres for seg."""
    by_seq = {}
    ukjent = []
    for j in all_journals.values() if isinstance(all_journals, dict) else all_journals:
        seq = (j.get("proceeding") or {}).get("sequenceNumber")
        if seq in proceedings_by_seq:
            by_seq.setdefault(seq, []).append(build_journalpost(j))
        else:
            ukjent.append(j)
    return by_seq, ukjent


# --------------------------------------------------------------------------- #
# Engangs historisk dump (2020-today_dump) - dato-halvering + saksoppslag
# --------------------------------------------------------------------------- #
def save(saker, output_file):
    tmp = output_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(output_file)


def run_full_dump(kilde_key, output_file, start=None, end=None):
    """Full historisk dump. For "henv" (derived_from satt) hentes NØYAKTIG
    samme rådata som "bygg" og filtreres etterpå til kun saker uten
    eiendomsreferanse (se modul-docstring) - egen output_file, bygg-dumpen
    er upåvirket."""
    kilde, type_id = kilde_type_id(kilde_key)
    start = start or kilde["start_date"]
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    all_journals = {}
    print(f"Henter journalposter {start} .. {end} (rekursiv dato-halvering)…")
    fetch_journals_range(session, type_id, start, end, all_journals)

    proceedings_by_seq = resolve_proceedings(session, type_id, list(all_journals.values()))
    journalposter_by_seq, ukjent_sak = group_journalposter(all_journals, proceedings_by_seq)
    alle_saker = [build_sak(p, journalposter_by_seq.get(seq, []), type_id)
                  for seq, p in proceedings_by_seq.items()]

    saker = filter_avledet(kilde, alle_saker)
    if kilde["derived_from"]:
        print(f"Filtrert til {len(saker)} henvendelser (av {len(alle_saker)} eByggesak-saker totalt)")

    save(saker, output_file)

    n_jp = sum(len(s["journalposter"]) for s in saker)
    print(f"Ferdig: {len(saker)} saker, {n_jp} journalposter -> {output_file} "
          f"({time.time() - start_time:.0f}s)")
    if ukjent_sak:
        print(f"OBS: {len(ukjent_sak)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - bredt tilbakeblikk + seen-ids-fil
# --------------------------------------------------------------------------- #
LOOKBACK_DAYS = 14     # hvor mange dager tilbake hver kjøring skanner (saksbehandlere
                       # registrerer ikke alltid alt samme dag)
SEEN_ID_RETENTION_DAYS = LOOKBACK_DAYS * 3   # hvor lenge en id beholdes før den ryddes bort


def fetch_journals_window(session, type_id, from_date, to_date):
    """Rekursiv dato-halvering over LOOKBACK_DAYS-vinduet (samme mønster som
    run_full_dump) - et flatt enkeltkall holder kun for et 1-dagersvindu."""
    all_journals = {}

    def recurse(from_d, to_d):
        from_iso, to_iso = from_d.isoformat(), to_d.isoformat()
        data = search_post_journal(
            session, type_id=type_id, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(type_id),
            journals_where=base_journals_where(from_iso, to_iso),
        )
        j_total = data["journals"]["totalCount"]
        if j_total > MAX_LIMIT and from_d != to_d:
            mid = from_d + (to_d - from_d) // 2
            recurse(from_d, mid)
            recurse(mid + timedelta(days=1), to_d)
            return
        _fetch_journals_leaf(session, type_id, from_iso, to_iso, all_journals)

    recurse(from_date, to_date)
    return list(all_journals.values())


def load_seen_ids(seen_ids_file):
    """{journalpost-id: journalDate} for poster allerede skrevet til output i
    en tidligere kjøring - luker ut duplikater når LOOKBACK_DAYS-vinduet
    overlapper med det forrige kjøringer allerede har dekket."""
    if not seen_ids_file.exists():
        return {}
    try:
        return json.loads(seen_ids_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def save_seen_ids(seen, keep_after_iso, seen_ids_file):
    """Lagre oppdatert seen-liste, og rydd bort oppføringer utenfor
    skann-vinduet (kan uansett aldri dukke opp igjen)."""
    pruned = {jid: d for jid, d in seen.items() if d >= keep_after_iso}
    seen_ids_file.write_text(json.dumps(pruned, ensure_ascii=False), encoding="utf-8")


def seed_seen_ids_from_dump(dump_file, seen_ids_file):
    """Seeder seen_journalpost_ids.json fra en ferdig historisk dump, slik
    at run_daily() ikke feilaktig rapporterer journalposter som allerede
    ligger i dumpen som "nye" - risikoen er begrenset til LOOKBACK_DAYS
    (14 dager) bakover siden run_daily uansett kun ser på et rullerende
    vindu, men uten seeding vil ALT i vinduet ved første kjøring bli
    feilaktig flagget. Trygt å kjøre flere ganger (slår sammen med det som
    allerede er der - eksisterende, nyere oppføringer overstyrer ikke)."""
    with open(dump_file, encoding="utf-8") as f:
        saker = json.load(f)
    seen = load_seen_ids(seen_ids_file)
    fra_dump = {}
    for sak in saker:
        for jp in sak.get("journalposter") or []:
            if jp.get("identifier"):
                fra_dump[jp["identifier"]] = jp.get("dato")
    merged = {**fra_dump, **seen}
    seen_ids_file.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")
    return merged


def write_output(saker, ref_date, output_dir):
    output_dir.mkdir(exist_ok=True)
    (output_dir / f"saker_{ref_date}.json").write_text(
        json.dumps(saker, ensure_ascii=False, indent=2), encoding="utf-8")
    # Ingen egen "eldre sak fikk ny journalpost"-fil (i motsetning til
    # Trondheim): proceedings-datofilteret er upålitelig, så det er uansett
    # ingen pålitelig måte å skille "ny sak i dag" fra "eldre sak med ny
    # aktivitet" - alle berørte saker havner derfor i samme fil.
    print(f"Skrev {len(saker)} berørte saker (med nye journalposter) -> {output_dir}")


def run_daily(kilde_key, output_dir, seen_ids_file, day_back=1, lookback_days=LOOKBACK_DAYS):
    """Daglig endringslogg. For "henv" hentes samme rådata som "bygg" og
    filtreres etterpå (se run_full_dump/modul-docstring) - egen
    seen_ids_file per kilde."""
    kilde, type_id = kilde_type_id(kilde_key)
    session = make_session()
    ref = datetime.now(ZoneInfo("Europe/Oslo")).date() - timedelta(days=day_back)
    from_date = ref - timedelta(days=lookback_days - 1)
    to_iso = ref.isoformat()
    print(f"Skann-vindu: {from_date.isoformat()} .. {to_iso} (LOOKBACK_DAYS={lookback_days}, "
          f"referansedag={to_iso})")

    journals = fetch_journals_window(session, type_id, from_date, ref)
    seen = load_seen_ids(seen_ids_file)
    nye_journals = [j for j in journals if j["id"] not in seen]
    print(f"  {len(journals)} journalposter i skann-vinduet, {len(nye_journals)} er nye siden sist kjøring")

    proceedings_by_seq = resolve_proceedings(session, type_id, nye_journals)
    journals_by_seq = {}
    ukjent = []
    for j in nye_journals:
        seq = (j.get("proceeding") or {}).get("sequenceNumber")
        if seq in proceedings_by_seq:
            journals_by_seq.setdefault(seq, []).append(j)
        else:
            ukjent.append(j)

    alle_saker = [build_sak(p, [build_journalpost(j) for j in journals_by_seq.get(seq, [])], type_id)
                  for seq, p in proceedings_by_seq.items()]
    saker = filter_avledet(kilde, alle_saker)

    write_output(saker, to_iso, output_dir)

    for j in nye_journals:
        seen[j["id"]] = j.get("journalDate")
    keep_after = (ref - timedelta(days=SEEN_ID_RETENTION_DAYS)).isoformat()
    save_seen_ids(seen, keep_after, seen_ids_file)

    if ukjent:
        print(f"OBS: {len(ukjent)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")
