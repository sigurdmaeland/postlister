"""Delt bibliotek for Drammen byggesak-scraperne.

Drammen kjører "360 Plan & Build" via innsynsportal.no (GraphQL) - samme
plattform som Trondheim/Tromsø/Asker, med samme VIKTIGE BUG som Asker:
"proceedings"-spørringens fromDate/toDate-filter beregner totalCount riktig,
men nodes-lista er IKKE filtrert på det tidsrommet. Vi henter derfor
JOURNALPOSTER dato-basert (rekursiv dato-halvering, journalFromDate/
journalToDate virker korrekt) og slår opp hver unike sak separat via eksakt
saksnummer-match (parallellisert, se resolve_proceedings) - samme strategi
som Asker.

Ingen egen "Byggesak"-type finnes her (ulikt Trondheim) - byggesaker skilles
i stedet ut via subArchiveId. To HELT ATSKILTE arkiver/lister, ikke bare to
sakstyper:

  bygg      - "Produksjon" (2020-today_dump), én subArchiveId ("eByggesak").
              Den aktive lista - eneste som følges opp daglig.
  bygg_hist - Historiske, skannede papirsaker (1800-today_dump), egen
              LIST_ID, seks hardkodede sub-arkiv (ARKIVER) fra Drammen +
              tidligere Nedre Eiker og Svelvik (sammenslått 2020). I praksis
              lukket - ingen daglig endringslogg for denne. Saksnummer
              (sequenceNumber) er kun unikt INNENFOR ett sub-arkiv, så
              saker keyes alltid på (subarchive_id, saksnummer), og
              resultatfila på 'identifier' (den globalt unike sak-IDen).
              1. januar hvert år har unormalt mange journalposter (300-400)
              - ser ut som en plassholderdato brukt ved skanning når
              original dato mangler; avdeling er da nesten alltid "Ingen
              registrering" (nytteløst som etterfyllingsfilter), så
              sakstype (typeIdIn) brukes som fallback i stedet.

Adresseformat er ULIKT mellom de to arkivene:
  bygg      - adressen står alltid FØRST i tittelen (kan liste flere
              husnumre for samme gate, komma/"og"-separert), se
              extract_adresse().
  bygg_hist - tittelen er typisk "Gbnr. <gnr>/<bnr>[.] <Gate> <nr>[,] <Sted>
              \\n<sakstype>" (hovedarkivet "Byggesak" har derimot ALDRI en
              adresse, kun sakstype), se extract_adresse_hist().

Brukes av:
  - 2020-today_dump/bygg/main.py       (engangs dump, aktiv lista)
  - 1800-today_dump/bygg/main.py       (engangs dump, historisk arkiv)
  - running_daily/bygg/app/main.py     (daglig endringslogg, kun aktiv lista)
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
BASE_URL = "https://innsyn2020.drammen.kommune.no"
API_URL = f"{BASE_URL}/graphql"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Drammen"
KOMMUNE_NR = 3301

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4
RESOLVE_WORKERS = 8    # parallelle saksoppslag (ingen batch-endpoint finnes for eksakt-match)

# NB: .strip() er nødvendig - serveren avviser query-tekst med avvikende
# whitespace, uansett om where-innholdet er gyldig. Identisk med
# Trondheim/Asker sin QUERY (samme plattform/schema).
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

# De seks byggesaksarkivene i det historiske arkivet (funnet empirisk - se
# 1800-today_dump/bygg/main.py sin opprinnelige topp-kommentar for
# fremgangsmåte). GraphQL-introspeksjon er skrudd av i produksjon, så det
# fins ingen spørring som kan liste opp subArchive-verdiene direkte.
ARKIVER_HIST = {
    "858e1d6d-4226-4699-a2ba-604bb6e08386": "Byggesak",
    "9d22d647-9364-4ad8-8062-582fb571f10d": "NEK - Byggesaksarkiv 2008-2012",
    "88b87747-7be7-4a72-892b-417d20708119": "NEK - Byggesaksarkiv 2013-2016",
    "0914eab7-3318-44a8-b1d8-ebf244618534": "NEK - Byggesaksarkiv 2017-2018",
    "7eb1f9f5-732a-4e62-8bdd-0f2978efc60a": "SK - Byggesaksarkiv  2015-2018",  # dobbelt mellomrom i kilden, ikke en skrivefeil her
    "907d5b6c-0f51-4f16-9cba-edd00f08733a": "SK - Byggesaksarkiv 2017-2018",
}

# --------------------------------------------------------------------------- #
# KILDER
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        list_id="450d153d-62f7-4564-ab1b-60370477c471",
        subarchive_id="1053bcf0-41ec-4598-9e8e-a273447d625a",   # "eByggesak"
        start_date=date(2020, 1, 1),   # eldste journalpost i denne lista (2020-01-06)
        historisk=False,
    ),
    "bygg_hist": dict(
        sakstype_navn="Byggesak (historisk)",
        list_id="fb851964-3185-43eb-81ba-9ac75226dfa8",
        arkiver=ARKIVER_HIST,
        start_date=date(1800, 1, 1),
        historisk=True,
    ),
}


def _portal_url(list_id):
    return f"{BASE_URL}/postjournal-v2/{list_id}"


# --------------------------------------------------------------------------- #
# gnr/bnr/matrikkelnr - API-et (propertyIdentifications) + tittel-fallback
# --------------------------------------------------------------------------- #
# For "bygg" (aktiv lista, adresse-først-tittel): fanger gnr/bnr[/feste/
# seksjon] KUN når tallkjeden innledes av tittel-start, komma, bindestrek,
# åpningsparentes, "og", eller selve ordet "gbnr"/"gnr/bnr" (dekker lister
# som "110/132, 110/531 og 110/5010", men også et fåtall titler som bryter
# "adresse først"-konvensjonen og eksplisitt MERKER gnr/bnr-referansen, f.eks.
# "Gbnr. 12/73. Øvre Bakkefeltet, ..." - uten "gbnr"/"gnr/bnr" som eget
# utløserord ble disse liggende med gnr_bnr=null selv når propertyIdentifications
# fra API-et også var tomt, bekreftet på ekte data 2026-09-02, se rapport/
# HANDOVER.md) - IKKE når det er del av et sakstall/referansenummer i parentes
# ("(ref. BYGG-19/00245)": "19" innledes riktignok av en bindestrek, men et
# ekte gnr/bnr har ALDRI et bruksnummer med ledende null slik saksnummer-
# sekvenser har - se _har_ledende_null). Hele tallkjeden fanges (ikke bare de
# to første tallene) av samme grunn som Trondheim/Tromsø: et fåtall titler
# kan skrive kommunenummeret først (se _gnr_bnr_fra_tallkjede). NB: dette
# fallbacket er rent ADDITIVT (se gnr_bnr_matrikkel) - det fjerner eller
# overstyrer aldri en verdi som allerede kom fra propertyIdentifications,
# selv om den skulle vise seg å være feil (samme kildesystem-usikkerhet som
# er dokumentert for Asker) - det legger bare til det tittelen i tillegg
# klart sier.
_TITLE_GNR_BNR = re.compile(
    r"(?:^|[,\-(]|\bog\b|\bgbnr\.?|\bgnr\s*/\s*bnr\.?)\s*(\d{1,4}(?:/\d{1,5}){1,3})",
    re.IGNORECASE,
)


def _har_ledende_null(tall):
    return len(tall) > 1 and tall[0] == "0"


def _gnr_bnr_fra_tallkjede(tallkjede):
    """Tolk en tallkjede "A/B[/C[/D]]" som gnr/bnr - hopper over en ledende
    KOMMUNE_NR hvis kjeden starter med den (samme mønster som Trondheim/
    Tromsø)."""
    nums = [n.strip() for n in tallkjede.split("/")]
    if nums[0] == str(KOMMUNE_NR) and len(nums) >= 3:
        return nums[1], nums[2]
    return nums[0], nums[1]


def gnr_bnr_fra_tittel(tittel):
    """Fallback/tillegg for "bygg" (aktiv lista) - se _TITLE_GNR_BNR. gnr "0"
    er kildens plassholder for "ingen egen eiendom"; et tallpar med ledende
    null i gnr ELLER bnr er nesten alltid et saksreferansenummer, ikke en
    ekte matrikkel (bekreftet på ekte data, se modul-kommentar over) - begge
    utelates."""
    if not tittel:
        return []
    pairs = []
    for m in _TITLE_GNR_BNR.finditer(tittel):
        gnr, bnr = _gnr_bnr_fra_tallkjede(m.group(1))
        if gnr == "0" or _har_ledende_null(gnr) or _har_ledende_null(bnr):
            continue
        pair = f"{gnr}/{bnr}"
        if pair not in pairs:
            pairs.append(pair)
    return pairs


def gnr_bnr_matrikkel(property_identifications, tittel=None):
    """Dedupe (propertyNr, useNr)-par (de dubleres i lista) fra API-et,
    beriket additivt med gnr/bnr parset fra tittelen (se gnr_bnr_fra_tittel)
    - propertyIdentifications er ofte tomt selv når tittelen klart har
    gnr/bnr (bekreftet på ekte data, f.eks. "Ingen adresse, 110/132, 110/531
    og 110/5010 - , Oppmålingsforretning..." -> propertyIdentifications
    tomt). Brukes KUN for "bygg" (aktiv lista) - "bygg_hist" har sin egen,
    strukturelt annerledes tittel-fallback, se gnr_bnr_fra_tittel_hist."""
    pairs = []
    for pid in property_identifications or []:
        gnr, bnr = pid.get("propertyNr"), pid.get("useNr")
        if gnr is None or bnr is None:
            continue
        pair = f"{gnr}/{bnr}"
        if pair not in pairs:
            pairs.append(pair)
    for pair in gnr_bnr_fra_tittel(tittel):
        if pair not in pairs:
            pairs.append(pair)
    if not pairs:
        return None, None
    gnr_bnr = "; ".join(pairs)
    matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{p}" for p in pairs)
    return gnr_bnr, matrikkelnr


# --------------------------------------------------------------------------- #
# Adresse - "bygg" (aktiv lista): adressen står alltid først i tittelen
# --------------------------------------------------------------------------- #
_PARENS = re.compile(r"\s*\([^)]*\)")
_MFL = re.compile(r"\bm\.?\s*fl\.?|\bmed flere\b", re.IGNORECASE)
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
# Husnummer-token: ett tall (+evt. bokstav-suffiks), evt. med EN ELLER FLERE
# "-tall[bokstav]"-fortsettelser ("94-134", men også "22-24-26-28" - fire
# adresser i én tittel, funnet ved gjennomgang av samtlige adresseløse
# eByggesak-saker 2026-09-02, se rapport/HANDOVER.md - {0,} i stedet for det
# opprinnelige {0,1} er den eneste endringen, strengt mer permissiv enn før).
_TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ])){0,}"
_BARE_TOKEN_FULL = re.compile(rf"^{_TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_STREET_AND_NUMBER_RANGE = re.compile(rf"^(.+?)\s+({_TOKEN})$")
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")
_PAREN_CONTENT = re.compile(r"\(([^)]*)\)")
# Et lite mindretall titler bryter "adresse først"-konvensjonen og har i
# stedet et bart gnr/bnr FØRST, skilt med " - " ("218/84/0/0 - Valmueveien
# 5, ..."). Uten dette strippet leser dash_idx-kuttet i extract_adresse()
# feilaktig HELE resten (inkl. selve adressen) som beskrivelse og forkaster
# den (bekreftet på ekte data - se rapport).
_LEADING_MATRIKKEL_DASH = re.compile(r"^\d+/\d+(?:/\d+){0,2}\s*-\s*")
# Samme brudd på "adresse først", men med KOMMA i stedet for tankestrek som
# skille ("333/34, Svelvikveien 1052, ...", evt. med "Gbnr."-merking eller to
# gnr/bnr-par separert med "og") - funnet ved gjennomgang av samtlige 882
# eByggesak-saker uten adresse 2026-09-02 (se rapport/HANDOVER.md), 27 av
# disse hadde en fullt gjenkjennelig adresse rett etter dette prefikset.
_LEADING_MATRIKKEL_COMMA = re.compile(
    r"^(?:gbnr\.?\s*)?\d+/\d+(?:/\d+){0,2}(?:\s+og\s+\d+/\d+(?:/\d+){0,2})?\s*,\s*",
    re.IGNORECASE,
)
# Plansaker ("Detaljregulering for X"/"Områderegulering for X", evt. med et
# plan-ID-tall foran) har adressen EFTER dette prefikset, ikke i stedet for
# det - uten stripping blir "Detaljregulering for " en del av selve
# "adressen" (bekreftet på ekte data, f.eks. "Detaljregulering for
# Dalenveien 32" i stedet for "Dalenveien 32" - se rapport). NB: dette er en
# annen sakstype enn "Kommuneplanens arealdel" (se _LEADING_KOMMUNEPLAN
# under) - en detalj-/områderegulering gjelder alltid en konkret
# eiendom/adresse, mens en kommuneplan alltid gjelder HELE kommunen.
_LEADING_PLAN_PREFIX = re.compile(
    r"^(?:\d{4,10}\s+)?(?:detaljregulering|områderegulering)\s*(?:for\s+)?",
    re.IGNORECASE,
)
# "Kommuneplanens arealdel for <kommune>" gjelder per definisjon HELE
# kommunen, aldri en enkelteiendom - i motsetning til reguleringsplaner (se
# _LEADING_PLAN_PREFIX over) er det derfor riktig å svare None her i stedet
# for å prøve å lese ut noe som ser ut som en adresse (bekreftet på ekte
# data: "20210007 Kommuneplanens arealdel for Drammen kommune 2023-2035"
# ble tidligere feilaktig lest som om HELE denne teksten var adressen).
_LEADING_KOMMUNEPLAN = re.compile(r"^(?:\d{4,10}\s+)?kommuneplan\w*\b", re.IGNORECASE)
# En "gbnr NN/NN"-referanse midt i eller rett etter selve adressen (ikke
# fremst, se _LEADING_MATRIKKEL_COMMA for det tilfellet) ødelegger
# husnummer-treffet på slutten av segmentet ("Austadveien 77 A gbnr. 22/152"
# - "22/152" har en skråstrek og matcher ikke _TOKEN, så HELE segmentet
# feiler). Fjernes derfor uansett hvor den måtte stå i teksten (bekreftet på
# ekte data 2026-09-02, se rapport/HANDOVER.md - referansen er uansett aldri
# del av selve gateadressen).
_EMBEDDED_GBNR = re.compile(r"\s*\bgbnr\.?\s*\d+/\d+(?:/\d+){0,2}", re.IGNORECASE)
# Etter at et av prefiksene over er strippet av, er vi ikke lenger garantert
# at det som følger faktisk ER adressen (selve grunnpremisset for at
# _adresser_fra_tekst() stoler blindt på FØRSTE segment) - uten en ekstra
# sjekk her plukket koden opp rein beskrivelsestekst som "adresse" i noen
# tilfeller (f.eks. "etablering av telekommunikasjonsstolpe med høyde over
# 5m" - "5m" så ut som et husnummer). Krever stor forbokstav, ELLER et
# nummerert stedsnavn som ekte forekommer i Drammen ("1. Strøm terrasse 12",
# "3. Bera terrasse 2B") - se rapport 2026-09-02.
_LOOKS_LIKE_ADDRESS_START = re.compile(r"^(?:[A-ZÆØÅ]|\d+\.\s*[A-ZÆØÅ])")


def _normalize_nummer_bokstav(s):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks ("16 B" -> "16B")."""
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _adresser_fra_tekst(text, trust_first=True):
    """Går segment for segment (delt på komma/' og ') og plukker opp
    husnummer så lenge de fortsetter forrige gatenavn (bart tall/bokstav)
    eller er et nytt "Gatenavn Nummer" (stor forbokstav kreves for å skille
    fra beskrivelse). Stopper på første segment som er verken.

    'trust_first' styrer om det ALLERFØRSTE segmentet stoles på uten
    stor-forbokstav-sjekk (Drammens grunnkonvensjon: adressen står alltid
    aller først) - sett til False av extract_adresse() når et
    matrikkel-/plan-prefiks nettopp er strippet av, siden vi da ikke lenger
    kan stole på at det som er igjen faktisk ER adressen (se
    _LOOKS_LIKE_ADDRESS_START over)."""
    entries = []
    current_street = None
    last_number = None
    tokens = _PART_SPLIT.split(text)
    part0, seps_and_parts = tokens[0], tokens[1:]
    parts = [(None, part0)] + list(zip(seps_and_parts[0::2], seps_and_parts[1::2]))

    for i, (sep, part) in enumerate(parts):
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
        sm = _STREET_AND_NUMBER_RANGE.match(_normalize_nummer_bokstav(part))
        if sm and i == 0:
            gate_ok = True if trust_first else bool(_LOOKS_LIKE_ADDRESS_START.match(sm.group(1).strip()))
        elif sm:
            gate_ok = bool(_STARTS_UPPER.match(sm.group(1).strip()))
        else:
            gate_ok = False
        if sm and gate_ok:
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
    """Adressen står alltid aller først i tittelen (Drammens konvensjon), og
    kan liste flere husnumre for samme gate (komma/"og"-separert) - alle tas
    med. Eldre saker bruker noen ganger " - " i stedet for komma som skille
    mot beskrivelsen - kuttes derfor bort først. Innhold i parenteser
    (tilleggsinfo/saksreferanser) fjernes før oppdelingen, men prøves som
    fallback dersom hovedteksten ikke gir noen treff (adressen står
    unntaksvis inni parentesen, f.eks. "Eiendommen 209/6 (Drammensveien
    151)"). Et fåtall titler bryter "adresse først" med et gnr/bnr- eller
    plansaks-prefiks isteden (se _LEADING_MATRIKKEL_COMMA/_LEADING_PLAN_PREFIX)
    - disse strippes før selve adresseutrekket, med strengere sjekk av
    resultatet siden vi da ikke lenger har den vanlige garantien om at
    adressen står helt fremst. "Kommuneplanens arealdel"-saker gjelder alltid
    hele kommunen og gir aldri en adresse (_LEADING_KOMMUNEPLAN). Returnerer
    None hvis ikke noe gjenkjennelig husnummer finnes."""
    if not tittel:
        return None

    text = _PARENS.sub("", tittel)
    text = _MFL.sub("", text)
    # Noen titler har innledende mellomrom (" Gbnr. 39/32, ...") som ellers
    # ville hindret prefiks-regexene under fra å treffe i det hele tatt siden
    # de er forankret med "^" (fikset 2026-09-02, se HANDOVER.md).
    text = text.strip()
    if _LEADING_KOMMUNEPLAN.match(text):
        return None
    before = text
    text = _LEADING_MATRIKKEL_DASH.sub("", text)
    # Løkke i stedet for ett enkelt .sub(): et fåtall titler har MER ENN to
    # gnr/bnr-referanser komma-separert fremst ("10/144, 109/6011, Rødgata
    # 25, ..." - _LEADING_MATRIKKEL_COMMA sin "og"-variant dekker kun TO,
    # ikke en vilkårlig komma-liste) - kjør derfor gjentatte ganger til
    # ingenting mer strippes (bekreftet på ekte data 2026-09-02, se
    # HANDOVER.md).
    while True:
        stripped = _LEADING_MATRIKKEL_COMMA.sub("", text)
        if stripped == text:
            break
        text = stripped
    text = _LEADING_PLAN_PREFIX.sub("", text)
    trust_first = (text == before)
    # En "gbnr NN/NN"-referanse midt i teksten (ikke fremst - det er allerede
    # dekket av _LEADING_MATRIKKEL_COMMA over) endrer ikke HVOR adressen
    # starter, bare hva som står etter den, så dette påvirker bevisst ikke
    # trust_first.
    text = _EMBEDDED_GBNR.sub("", text)
    dash_idx = text.find(" - ")
    if dash_idx != -1:
        text = text[:dash_idx]

    uniq = _adresser_fra_tekst(text, trust_first=trust_first)

    if not uniq:
        for m in _PAREN_CONTENT.finditer(tittel):
            uniq = _adresser_fra_tekst(m.group(1), trust_first=trust_first)
            if uniq:
                break

    return "; ".join(uniq) if uniq else None


# --------------------------------------------------------------------------- #
# Adresse - "bygg_hist": Gbnr-merket adresse (hovedarkivet "Byggesak" har
# aldri en adresse i tittelen, kun sakstype - se modul-docstring)
# --------------------------------------------------------------------------- #
_LABELED_GNR = re.compile(
    r"^(?:[^\d]{0,40}?)\b(?:gbnr|gnr\s*/\s*bnr|gatenr)\.?\s*\d+(?:\s*/\s*\d+){0,3}",
    re.IGNORECASE,
)
_BARE_GNR = re.compile(r"^(?:[^\d]{0,40}?)\b\d+(?:\s*/\s*\d+){1,3}")
_GNR_TILLEGG_OGMFL = re.compile(r"\s*,?\s*(?:og|m\.fl\.?)\s*\d+(?:\s*/\s*\d+){0,3}", re.IGNORECASE)
_GNR_TILLEGG_BARE = re.compile(r"\s*,\s*\d+(?:\s*/\s*\d+){1,3}")
_GNR_EKSTRA = re.compile(r"\s*,?\s*(?:fnr|snr)\.?\s*\d+", re.IGNORECASE)
_GNR_SEP = re.compile(r"\s*[-–:,.]?\s*")
_GATE_NR = re.compile(r"^([A-Za-zÆØÅæøå][A-Za-zÆØÅæøå0-9.\-' ]*?\s\d+\s*[A-Za-z]?)\b")
# Noen titler har en ekstra saks-/matrikkel-henvisning RETT ETTER den første
# (som _GATE_NR da leser inn som om det var gatenavn+husnummer, siden det
# ikke fins noe skille å stoppe på), f.eks. "Ref. sak 99/3243" -> "...Ref.
# sak 99", "GNR 35 BNR. 7" -> "TIL GNR 35", "ANR. 23" (andelsnummer) -> "...
# ANR. 23". Disse ordene opptrer ALDRI i et ekte gatenavn (bekreftet på ekte
# data - se rapport) og forkastes derfor i stedet for å gjettes som adresse.
_REJECT_WORDS_HIST = re.compile(r"\b(?:ref|gnr|bnr|snr|fnr|anr|sak)\b\.?", re.IGNORECASE)
# Rene sakstype-/beskrivelsessetninger uten adresse kan tilfeldigvis ende på
# et tall som _GATE_NR leser som et husnummer - typisk et årstall i en
# skattetakst-/klagesak uten egen gateadresse, f.eks. "18/37 KLAGE PÅ
# EIENDOMSSKATTETAKST 2014" (bekreftet feilaktig lest som adresse "KLAGE PÅ
# EIENDOMSSKATTETAKST 2014" på ekte data - se rapport). Disse ordene opptrer
# ALDRI som starten på et ekte gatenavn i arkivet.
_DESCRIPTION_MARKERS_HIST = re.compile(
    r"^(?:klage|søknad|melding|varsel|tillatelse|dispensasjon|anmodning|"
    r"forespørsel|henvendelse|tilsyn|ulovlighet\w*|ferdigattest|"
    r"rammetillatelse|igangsettingstillatelse|oppmålingsforretning|"
    r"seksjonering\w*|skattetakst|eiendomsskattetakst|takst|bruksendring|"
    r"riving|nybygg|tilbygg|påbygg)\b",
    re.IGNORECASE,
)
# Et fåtall bygg_hist-titler har verken "Gbnr."-merket eller bar gnr/bnr
# først, men et RENT saksreferansenummer (ingen "/", altså ikke matrikkel)
# fulgt av " - adresse - beskrivelse", f.eks. "217134 - MARKVEIEN 26D -
# TILLATELSE TIL..." (bekreftet på ekte data - se rapport). gnr/bnr forblir
# None her (referansenummeret er ikke en matrikkel), men adressen skal
# fortsatt hentes ut.
_REF_NUM_PREFIX = re.compile(r"^\d{3,8}\s*-\s*")
# Et parentetisk innskudd midt i gnr/bnr-prefikset ("Gbnr. 42/534. (tidligere
# 42/1), Stensetalleen 1, ...") blokkerer _GATE_NR helt siden den krever en
# BOKSTAV som første tegn i det som er igjen etter prefikset - løses ved å
# fjerne parenteser tidlig, slik extract_adresse() (den ikke-historiske
# funksjonen) allerede gjør. Etterpå kan det stå igjen et par doble
# skilletegn der parentesen var (f.eks. ".," når _GNR_SEP kun rekker å
# konsumere det ene) - LEADING_JUNK_HIST fjerner ALLE slike før _GATE_NR
# prøves (fikset 2026-09-02, se HANDOVER.md).
_PARENS_HIST = re.compile(r"\s*\([^)]*\)")
_LEADING_JUNK_HIST = re.compile(r"^[\s,.\-–:]+")


def extract_adresse_hist(tittel):
    """Tittelen i det historiske arkivet er typisk "Gbnr. <gnr>/<bnr>[. ,]
    <Gate> <nr>[,.] <Sted>[.]\\n<sakstype>" (og for "SK - Byggesaksarkiv
    2015-2018" en ALL CAPS-variant med bindestrek: "<gnr>/<bnr> - <GATE>
    <NR> - <BESKRIVELSE>") - unntatt hovedarkivet "Byggesak" (858e1d6d…),
    der tittelen ALDRI er noe mer enn en generisk sakstype.

    Strategi: finn gnr/bnr-identifikatoren (evt. merket "Gbnr."/"Gatenr.",
    evt. med inntil 40 tegn innledende fritekst foran, f.eks. "Adkomst til "
    eller "NABOVARSEL FOR TILTAK PÅ EIENDOM "), hopp over eventuelle
    tilleggsidentifikatorer ("og 37/49", "m.fl.", flere komma-separerte
    gnr/bnr, "fnr. 140", "snr.09"), og tolk det som følger som "<gatenavn>
    <husnummer>[<bokstav>]". Gatenavnet må ha minst ett ord på 3+ bokstaver
    (luker ut falske treff som seksjonsnummer-referansen "S. 2" i "39/353,
    S. 2 OG 3, RESEKSJONERING"). Krever et husnummer for å returnere noe i
    det hele tatt."""
    if not tittel:
        return None
    tekst = " ".join(tittel.split())
    tekst = " ".join(_PARENS_HIST.sub("", tekst).split())
    m = _LABELED_GNR.match(tekst) or _BARE_GNR.match(tekst)
    if m:
        slutt = m.end()
        while True:
            m2 = _GNR_TILLEGG_OGMFL.match(tekst[slutt:]) or _GNR_TILLEGG_BARE.match(tekst[slutt:])
            if not m2:
                break
            slutt += m2.end()
        while True:
            m3 = _GNR_EKSTRA.match(tekst[slutt:])
            if not m3:
                break
            slutt += m3.end()
        sep = _GNR_SEP.match(tekst[slutt:])
        if sep:
            slutt += sep.end()
    else:
        # Ingen gnr/bnr-prefiks funnet - prøv det rene referansenummer-
        # formatet ("217134 - MARKVEIEN 26D - ...", se _REF_NUM_PREFIX).
        m_ref = _REF_NUM_PREFIX.match(tekst)
        if not m_ref:
            return None
        slutt = m_ref.end()
    rest = _LEADING_JUNK_HIST.sub("", tekst[slutt:])
    m4 = _GATE_NR.match(rest)
    if not m4:
        return None
    candidate = re.sub(r"\s+", " ", m4.group(1)).strip(" .,-")
    # Samme mellomrom-i-husnummer-suffiks-normalisering som extract_adresse()
    # (den ikke-historiske funksjonen) allerede har - denne funksjonen mangla
    # den, så "MØLLEVEIEN 3 B" ble aldri til "MØLLEVEIEN 3B" (fikset
    # 2026-09-02, se HANDOVER.md).
    candidate = _normalize_nummer_bokstav(candidate)
    if _REJECT_WORDS_HIST.search(candidate):
        return None
    if _DESCRIPTION_MARKERS_HIST.match(candidate):
        return None
    ord_ = re.findall(r"[A-Za-zÆØÅæøå]+", candidate)
    if not any(len(w) >= 3 for w in ord_):
        return None
    return candidate


def gnr_bnr_fra_tittel_hist(tittel):
    """Finn gnr/bnr-par i en bygg_hist-tittel - gjenbruker EKSAKT samme
    gjenkjenning av matrikkel-prefikset som extract_adresse_hist over (samme
    _LABELED_GNR/_BARE_GNR-match + og/mfl/fnr/snr-tilleggsløkke), men
    returnerer selve tallene i stedet for bare å bruke dem til å vite hvor
    adressen begynner. Fungerer UAVHENGIG av om adressen etterpå faktisk lar
    seg tolke - f.eks. "6/9/35 - KROKÅSEN - BRUKSENDRING" har ingen
    husnummer og gir adresse=None (se extract_adresse_hist), men gnr/bnr
    (6/9) skal likevel med. Rene referansenummer-titler (se _REF_NUM_PREFIX)
    har ALDRI et ekte gnr/bnr og gir tomt her, i tråd med det."""
    if not tittel:
        return []
    tekst = " ".join(tittel.split())
    m = _LABELED_GNR.match(tekst) or _BARE_GNR.match(tekst)
    if not m:
        return []
    matchet_tekst = tekst[:m.end()]
    slutt = m.end()
    while True:
        m2 = _GNR_TILLEGG_OGMFL.match(tekst[slutt:]) or _GNR_TILLEGG_BARE.match(tekst[slutt:])
        if not m2:
            break
        matchet_tekst += m2.group(0)
        slutt += m2.end()
    pairs = []
    for tallpar in re.findall(r"\d+(?:\s*/\s*\d+){1,3}", matchet_tekst):
        nums = [n.strip() for n in re.split(r"\s*/\s*", tallpar)]
        gnr, bnr = nums[0], nums[1]
        if gnr == "0" or _har_ledende_null(gnr) or _har_ledende_null(bnr):
            continue
        pair = f"{gnr}/{bnr}"
        if pair not in pairs:
            pairs.append(pair)
    return pairs


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


def search_post_journal(session, *, list_id, subarchive_id, plimit, jlimit,
                         proceedings_where, journals_where, include_count=True):
    body = {
        "operationName": "SearchPostJournal",
        "variables": {
            "proceedingsLimit": plimit,
            "proceedingsWhere": proceedings_where,
            "proceedingsOrderBy": "date_DESC",
            "journalsLimit": jlimit,
            "journalsWhere": journals_where,
            "journalDocumentsWhere": {"listId": list_id},
            "journalProceedingWhere": {"subArchiveId": subarchive_id},
            "journalsOrderBy": "journalDate_DESC",
            "includeCount": include_count,
        },
        "query": QUERY,
    }
    return _post(session, body)


def base_proceedings_where(list_id, subarchive_id, sequence_number=None, department_id_in=None):
    return {
        "listId": list_id,
        "subArchiveId": subarchive_id,
        "sequenceNumber": sequence_number,
        "departmentIdIn": department_id_in,
    }


def base_journals_where(list_id, from_iso, to_iso, department_id_in=None, type_id_in=None):
    return {
        "listId": list_id,
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


def _fetch_journals_leaf(session, list_id, subarchive_id, from_iso, to_iso, all_journals):
    """Hent journalposter for et vindu lite nok til å ligge innenfor
    MAX_LIMIT. Avdeling først (best på nyere/digitale saker), sakstype som
    ekstra fallback (avdeling er nesten alltid "Ingen registrering" på
    eldre, skannede saker - se modul-docstring om 1. januar-datoene)."""
    data = search_post_journal(
        session, list_id=list_id, subarchive_id=subarchive_id, plimit=1, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(list_id, subarchive_id),
        journals_where=base_journals_where(list_id, from_iso, to_iso),
    )
    journals = list(data["journals"]["nodes"])
    j_total = data["journals"]["totalCount"]

    journals = _refill_via(
        journals, j_total, "department",
        lambda dept: search_post_journal(
            session, list_id=list_id, subarchive_id=subarchive_id, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(list_id, subarchive_id),
            journals_where=base_journals_where(list_id, from_iso, to_iso, department_id_in=[dept]),
            include_count=False,
        )["journals"]["nodes"],
    )
    journals = _refill_via(
        journals, j_total, "type",
        lambda type_id: search_post_journal(
            session, list_id=list_id, subarchive_id=subarchive_id, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(list_id, subarchive_id),
            journals_where=base_journals_where(list_id, from_iso, to_iso, type_id_in=[type_id]),
            include_count=False,
        )["journals"]["nodes"],
    )
    if len(journals) < j_total:
        print(f"    ADVARSEL {from_iso}..{to_iso}: fant kun {len(journals)}/{j_total} journalposter "
              f"(servergrense, ingen flere avdelinger/typer å prøve)")
    for j in journals:
        all_journals[j["id"]] = j


def fetch_journals_range(session, list_id, subarchive_id, from_date, to_date, all_journals):
    """Rekursiv dato-halvering på journalposter (den eneste pålitelige
    datofiltreringen, se modul-docstring). Fyller all_journals (dict
    id -> node) i stedet for å returnere."""
    from_iso, to_iso = from_date.isoformat(), to_date.isoformat()
    data = search_post_journal(
        session, list_id=list_id, subarchive_id=subarchive_id, plimit=1, jlimit=MAX_LIMIT,
        proceedings_where=base_proceedings_where(list_id, subarchive_id),
        journals_where=base_journals_where(list_id, from_iso, to_iso),
    )
    j_total = data["journals"]["totalCount"]

    if j_total > MAX_LIMIT and from_date != to_date:
        mid = from_date + (to_date - from_date) // 2
        fetch_journals_range(session, list_id, subarchive_id, from_date, mid, all_journals)
        fetch_journals_range(session, list_id, subarchive_id, mid + timedelta(days=1), to_date, all_journals)
        return

    _fetch_journals_leaf(session, list_id, subarchive_id, from_iso, to_iso, all_journals)
    if j_total:
        print(f"  {from_iso}..{to_iso}: {j_total} journalposter")


def resolve_proceedings(session, list_id, subarchive_id, journals):
    """Slå opp full sak-info for hver unike sak journalpostene tilhører
    (proceedings-datofilteret er upålitelig, se modul-docstring - eksakt
    saksnummer-match er derimot pålitelig). NB for bygg_hist: saksnummer er
    kun unikt INNENFOR ett sub-arkiv, så oppslaget filtreres alltid på
    riktig subarchive_id i tillegg til saksnummeret."""
    seqs = sorted({(j.get("proceeding") or {}).get("sequenceNumber") for j in journals} - {None})

    def lookup(seq):
        data = search_post_journal(
            session, list_id=list_id, subarchive_id=subarchive_id, plimit=3, jlimit=1,
            proceedings_where=base_proceedings_where(list_id, subarchive_id, sequence_number=seq),
            journals_where=base_journals_where(list_id, None, None),
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


def sak_url(list_id, subarchive_id, saksnummer):
    params = json.dumps({"subArchiveId": subarchive_id, "proceeding": {"sequenceNumber": saksnummer}})
    return f"{_portal_url(list_id)}?params={quote(params, safe='')}"


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


def group_journalposter(all_journals, proceedings_by_seq):
    """Grupper journalposter på sak (via saksnummer fra journalpostens
    embedded 'proceeding'-felt - alltid korrekt, i motsetning til
    proceedings-spørringens datofilter). Journalposter uten en løst sak
    (oppslag feilet, f.eks. gradert) returneres for seg."""
    by_seq = {}
    ukjent = []
    for j in (all_journals.values() if isinstance(all_journals, dict) else all_journals):
        seq = (j.get("proceeding") or {}).get("sequenceNumber")
        if seq in proceedings_by_seq:
            by_seq.setdefault(seq, []).append(build_journalpost(j))
        else:
            ukjent.append(j)
    return by_seq, ukjent


def save(saker, output_file):
    tmp = output_file.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(saker, f, ensure_ascii=False, indent=2)
    tmp.replace(output_file)


def fetch_saker_for_period(session, list_id, subarchive_id, start, end):
    """Hent journalposter for [start, end] (dato-halvering) og slå dem opp
    til (proceedings_by_seq, journalposter_by_seq, ukjent_sak) - felles
    henteflyt for run_full_dump og run_full_dump_hist (samme sub-arkiv-
    parametrisering, se modul-docstring)."""
    all_journals = {}
    print(f"Henter journalposter {start} .. {end} (rekursiv dato-halvering)…")
    fetch_journals_range(session, list_id, subarchive_id, start, end, all_journals)
    proceedings_by_seq = resolve_proceedings(session, list_id, subarchive_id, list(all_journals.values()))
    journalposter_by_seq, ukjent_sak = group_journalposter(all_journals, proceedings_by_seq)
    return proceedings_by_seq, journalposter_by_seq, ukjent_sak


# --------------------------------------------------------------------------- #
# Engangs historisk dump - "bygg" (2020-today_dump, én subArchiveId)
# --------------------------------------------------------------------------- #
def build_sak(proceeding, journalposter, list_id, subarchive_id):
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(
        proceeding.get("propertyIdentifications"), proceeding.get("title")
    )
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
        "saksbehandler": proceeding.get("caseworkers") or [],
        "dato": proceeding.get("date"),
        "url": sak_url(list_id, subarchive_id, saksnummer) if saksnummer else _portal_url(list_id),
        "journalposter": journalposter,
    }


def run_full_dump(kilde_key, output_file, start=None, end=None):
    """Full historisk dump for "bygg" (dato-halvering + saksoppslag, se
    modul-docstring). Bruk run_full_dump_hist() for "bygg_hist"."""
    kilde = KILDER[kilde_key]
    list_id, subarchive_id = kilde["list_id"], kilde["subarchive_id"]
    start = start or kilde["start_date"]
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    proceedings_by_seq, journalposter_by_seq, ukjent_sak = fetch_saker_for_period(
        session, list_id, subarchive_id, start, end)
    saker = [build_sak(p, journalposter_by_seq.get(seq, []), list_id, subarchive_id)
             for seq, p in proceedings_by_seq.items()]
    save(saker, output_file)

    n_jp = sum(len(s["journalposter"]) for s in saker)
    print(f"Ferdig: {len(saker)} saker, {n_jp} journalposter -> {output_file} "
          f"({time.time() - start_time:.0f}s)")
    if ukjent_sak:
        print(f"OBS: {len(ukjent_sak)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")


# --------------------------------------------------------------------------- #
# Engangs historisk dump - "bygg_hist" (seks sub-arkiv, egen LIST_ID)
# --------------------------------------------------------------------------- #
def gnr_bnr_matrikkel_hist(property_identifications, tittel):
    """Som gnr_bnr_matrikkel(), men for "bygg_hist" - beriket additivt med
    gnr/bnr_fra_tittel_hist() i stedet for den generelle gnr_bnr_fra_tittel()
    (bygg_hist har en strukturelt annerledes tittel-dialekt, se modul-
    docstring og extract_adresse_hist)."""
    pairs = []
    for pid in property_identifications or []:
        gnr, bnr = pid.get("propertyNr"), pid.get("useNr")
        if gnr is None or bnr is None:
            continue
        pair = f"{gnr}/{bnr}"
        if pair not in pairs:
            pairs.append(pair)
    for pair in gnr_bnr_fra_tittel_hist(tittel):
        if pair not in pairs:
            pairs.append(pair)
    if not pairs:
        return None, None
    gnr_bnr = "; ".join(pairs)
    matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{p}" for p in pairs)
    return gnr_bnr, matrikkelnr


def build_sak_hist(proceeding, journalposter, list_id, subarchive_id):
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel_hist(
        proceeding.get("propertyIdentifications"), proceeding.get("title")
    )
    saksnummer = proceeding.get("sequenceNumber")
    return {
        "identifier": proceeding.get("id"),
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        # "arkiv" skiller mellom de seks sub-arkivene (se ARKIVER_HIST) -
        # "sakstype" er alltid "Byggesak" (alle seks er byggesaksarkiver,
        # subArchive-navnet forteller bare hvilket arkiv/periode).
        "arkiv": (proceeding.get("subArchive") or {}).get("name"),
        "saksnummer": saksnummer,
        "sakstype": "Byggesak",
        "sakstittel": proceeding.get("title"),
        "adresse": extract_adresse_hist(proceeding.get("title")),
        "gnr_bnr": gnr_bnr,
        "matrikkelnr": matrikkelnr,
        "saksbehandler": proceeding.get("caseworkers") or [],
        "dato": proceeding.get("date"),
        "url": sak_url(list_id, subarchive_id, saksnummer) if saksnummer else _portal_url(list_id),
        "journalposter": journalposter,
    }


def load_existing_by_identifier(output_file):
    """Leser tidligere lagret resultat, keyet på 'identifier' (den globalt
    unike sak-IDen, IKKE 'saksnummer' - saksnummer er kun unikt innenfor ett
    sub-arkiv, se modul-docstring), slik at scriptet kan kjøres i flere
    omganger på ulike årsintervaller uten å miste tidligere hentede saker."""
    if not output_file.exists():
        return {}
    try:
        return {s["identifier"]: s for s in json.loads(output_file.read_text(encoding="utf-8"))
                if s.get("identifier")}
    except Exception:  # noqa: BLE001
        return {}


def run_full_dump_hist(output_file, start=None, end=None, merge=True):
    """Full historisk dump av "bygg_hist" - itererer over alle seks
    sub-arkiv i ARKIVER_HIST og slår resultatene sammen (arkivene er for
    store til å hente i én kjøring; merge=True bygger videre på output_file
    på tvers av kall med ulike årsintervaller)."""
    kilde = KILDER["bygg_hist"]
    list_id = kilde["list_id"]
    start = start or kilde["start_date"]
    end = end or datetime.now(ZoneInfo("Europe/Oslo")).date()
    session = make_session()
    start_time = time.time()

    saker_by_id = load_existing_by_identifier(output_file) if merge else {}
    total_ukjent = 0

    for subarchive_id, arkiv_navn in kilde["arkiver"].items():
        print(f"\n=== {arkiv_navn} ===")
        proceedings_by_seq, journalposter_by_seq, ukjent_sak = fetch_saker_for_period(
            session, list_id, subarchive_id, start, end)
        nye_saker = {p["id"]: build_sak_hist(p, journalposter_by_seq.get(seq, []), list_id, subarchive_id)
                     for seq, p in proceedings_by_seq.items()}
        saker_by_id.update(nye_saker)
        total_ukjent += len(ukjent_sak)

        n_jp_nye = sum(len(s["journalposter"]) for s in nye_saker.values())
        print(f"{arkiv_navn}: {len(nye_saker)} saker i intervallet ({n_jp_nye} journalposter)")
        if ukjent_sak:
            print(f"  OBS: {len(ukjent_sak)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
                  f"og ble ikke tatt med.")

    saker = list(saker_by_id.values())
    save(saker, output_file)

    print(f"\nFerdig: {len(saker)} saker totalt lagret -> {output_file} "
          f"({time.time() - start_time:.0f}s)")
    if total_ukjent:
        print(f"OBS totalt: {total_ukjent} journalposter fikk ikke løst sin sak og ble ikke tatt med.")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - kun "bygg" (aktiv lista, se
# modul-docstring - det historiske arkivet er i praksis lukket)
# --------------------------------------------------------------------------- #
LOOKBACK_DAYS = 14     # saksbehandlere registrerer ikke alt samme dag
SEEN_ID_RETENTION_DAYS = LOOKBACK_DAYS * 3


def fetch_journals_window(session, list_id, subarchive_id, from_date, to_date):
    """Rekursiv dato-halvering over LOOKBACK_DAYS-vinduet (samme mønster som
    run_full_dump) - et flatt enkeltkall holder kun for et 1-dagersvindu."""
    all_journals = {}

    def recurse(from_d, to_d):
        from_iso, to_iso = from_d.isoformat(), to_d.isoformat()
        data = search_post_journal(
            session, list_id=list_id, subarchive_id=subarchive_id, plimit=1, jlimit=MAX_LIMIT,
            proceedings_where=base_proceedings_where(list_id, subarchive_id),
            journals_where=base_journals_where(list_id, from_iso, to_iso),
        )
        j_total = data["journals"]["totalCount"]
        if j_total > MAX_LIMIT and from_d != to_d:
            mid = from_d + (to_d - from_d) // 2
            recurse(from_d, mid)
            recurse(mid + timedelta(days=1), to_d)
            return
        _fetch_journals_leaf(session, list_id, subarchive_id, from_iso, to_iso, all_journals)

    recurse(from_date, to_date)
    return list(all_journals.values())


def load_seen_ids(seen_ids_file):
    """{journalpost-id: journalDate} for poster allerede skrevet til output
    i en tidligere kjøring - luker ut duplikater når LOOKBACK_DAYS-vinduet
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
    ligger i dumpen som "nye" - se Asker sin scraper_lib.py (samme
    mekanisme) for full forklaring. Trygt å kjøre flere ganger."""
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
    # Trondheim): proceedings-datofilteret er upålitelig, så det finnes
    # uansett ingen pålitelig måte å skille "ny sak i dag" fra "eldre sak
    # med ny aktivitet" - alle berørte saker havner derfor i samme fil.
    print(f"Skrev {len(saker)} berørte saker (med nye journalposter) -> {output_dir}")


def run_daily(kilde_key, output_dir, seen_ids_file, day_back=1, lookback_days=LOOKBACK_DAYS):
    """Daglig endringslogg. Kun ment for "bygg" - det historiske arkivet
    følges ikke opp daglig (se modul-docstring)."""
    kilde = KILDER[kilde_key]
    list_id, subarchive_id = kilde["list_id"], kilde["subarchive_id"]
    session = make_session()
    ref = datetime.now(ZoneInfo("Europe/Oslo")).date() - timedelta(days=day_back)
    from_date = ref - timedelta(days=lookback_days - 1)
    to_iso = ref.isoformat()
    print(f"Skann-vindu: {from_date.isoformat()} .. {to_iso} (LOOKBACK_DAYS={lookback_days}, "
          f"referansedag={to_iso})")

    journals = fetch_journals_window(session, list_id, subarchive_id, from_date, ref)
    seen = load_seen_ids(seen_ids_file)
    nye_journals = [j for j in journals if j["id"] not in seen]
    print(f"  {len(journals)} journalposter i skann-vinduet, {len(nye_journals)} er nye siden sist kjøring")

    proceedings_by_seq = resolve_proceedings(session, list_id, subarchive_id, nye_journals)
    journals_by_seq = {}
    ukjent = []
    for j in nye_journals:
        seq = (j.get("proceeding") or {}).get("sequenceNumber")
        if seq in proceedings_by_seq:
            journals_by_seq.setdefault(seq, []).append(j)
        else:
            ukjent.append(j)

    saker = [build_sak(p, [build_journalpost(j) for j in journals_by_seq.get(seq, [])], list_id, subarchive_id)
             for seq, p in proceedings_by_seq.items()]

    write_output(saker, to_iso, output_dir)

    for j in nye_journals:
        seen[j["id"]] = j.get("journalDate")
    keep_after = (ref - timedelta(days=SEEN_ID_RETENTION_DAYS)).isoformat()
    save_seen_ids(seen, keep_after, seen_ids_file)

    if ukjent:
        print(f"OBS: {len(ukjent)} journalposter fikk ikke løst sin sak (f.eks. gradert) "
              f"og ble ikke tatt med.")
