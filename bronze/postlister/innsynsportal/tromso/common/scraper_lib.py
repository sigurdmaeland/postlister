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
adressen, så et komma, så beskrivelsen. gnr/bnr hentes primært strukturert
fra proceeding.propertyIdentifications (samme felt som Trondheim/Asker
bruker), men dette feltet er ofte tomt i praksis selv når tittelen klart
har gnr/bnr (bekreftet på ekte data, f.eks. "Skårungevegen 10, 117/828,
Påbygg til bolig" -> propertyIdentifications tomt). gnr_bnr_matrikkel()
faller derfor tilbake på/beriker alltid additivt med gnr/bnr parset direkte
fra tittelen (se gnr_bnr_fra_tittel) - både det ledende matrikkelnr-
prefikset og eventuelle restaterte/ekstra gnr/bnr som følger etter (f.eks.
et opprinnelig/overordnet gnr/bnr i parentes for en utskilt enhet, "118/1748
(117/1723) - Nordheimvegen ..."). Et lite mindretall titler mangler
matrikkelnr-prefikset helt (rene firmanavn-tilsyn som "Tore Workinn AS,
tilsyn med kvalifikasjoner", eller bare "-") - disse behandles som
"ingen adresse" (samme avveining som andre steder i kodebasen).

Brukes av:
  - 2019-10-22-today_dump/{bygg,tilsyn,ulov,henv}/main.py (engangs historisk dump)
  - running_daily/{bygg,tilsyn,ulov,henv}/app/main.py     (daglig endringslogg)
"""

import gzip
import json
import os
import re
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
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

# Azure Blob - samme lagringskonto/container og samme mønster (Service
# Principal via env/.env, JSONL+gzip) som Kristiansand-pipelinen i dette
# repoet, se _azure_credential()/upload_til_azure() nedenfor. Tromsøs
# run_daily bruker et ekte datovindu (se WINDOW_DAYS/fetch_window) - det
# finnes derfor ingen snapshot/diff-tilstand å speile i Azure slik
# Kristiansand gjør (som mangler datofiltrering og må gjøre full sweep +
# snapshot-diff); kun de to opplastings-hookene under (endringslogg +
# full-dump) er relevante å portere hit.
AZURE_ACCOUNT_URL = "https://storaggen2eaccountprod.blob.core.windows.net"
AZURE_CONTAINER_NAME = "postlister"
AZURE_BASE_PATH = "bronze/tromso"

_SAKSTYPE_SLUG = {
    "Byggesak": "bygg",
    "Henvendelse": "henv",
    "Ulovlighetssak": "ulov",
    "Tilsynssak": "tilsyn",
}

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
# gnr/bnr/matrikkelnr - API-et (propertyIdentifications) + tittel-fallback
# --------------------------------------------------------------------------- #
# Fanger gnr/bnr[/feste[/seksjon]] KUN når tallkjeden innledes av tittel-start,
# komma, bindestrek eller åpningsparentes - IKKE når det er en del av selve
# gateadressen ("Fr.Langesgt. 19/21" er et hus-nummerspenn, ikke et
# matrikkelfelt: der står tallparet rett etter et gatenavn+mellomrom, uten
# komma/bindestrek/parentes foran - bekreftet ved stikkprøve at ekte gnr/bnr-
# oppføringer i denne kilden ALLTID har ett av disse skilletegnene foran seg,
# mens gate-husnummerspenn ALDRI har det). gnr "0" er kildens plassholder for
# "ingen egen eiendom" (forekommer aldri i ekte propertyIdentifications-data)
# og utelates. Hele tallkjeden fanges (ikke bare de første to tallene) fordi
# et fåtall titler skriver kommunenummeret FØRST i kjeden (se
# _gnr_bnr_fra_tallkjede - samme mønster bekreftet på ekte data i Trondheim,
# forsvarlig å beskytte mot her også siden formatet deles på samme plattform).
_TITLE_GNR_BNR = re.compile(r"(?:^|[,\-(])\s*(\d{1,4}(?:/\d{1,5}){1,3})")


def _gnr_bnr_fra_tallkjede(tallkjede):
    """Tolk en tallkjede "A/B[/C[/D]]" som gnr/bnr - hopper over en ledende
    KOMMUNE_NR hvis kjeden starter med den (se _TITLE_GNR_BNR over)."""
    nums = tallkjede.split("/")
    if nums[0] == str(KOMMUNE_NR) and len(nums) >= 3:
        return nums[1], nums[2]
    return nums[0], nums[1]


def gnr_bnr_fra_tittel(tittel):
    """Finn ALLE gnr/bnr-par i sakstittelen (uansett posisjon - ledende
    matrikkelnr, restatert/ekstra matrikkelnr i parentes, eller et matrikkelnr
    som følger etter adressen, f.eks. "Skårungevegen 10, 117/828, ..."). Brukt
    som fallback/tillegg til propertyIdentifications i gnr_bnr_matrikkel()."""
    if not tittel:
        return []
    pairs = []
    for m in _TITLE_GNR_BNR.finditer(tittel):
        gnr, bnr = _gnr_bnr_fra_tallkjede(m.group(1))
        if gnr == "0":
            continue
        pair = f"{gnr}/{bnr}"
        if pair not in pairs:
            pairs.append(pair)
    return pairs


def gnr_bnr_matrikkel(property_identifications, tittel=None):
    """Dedupe (propertyNr, useNr)-par (de dubleres ofte i lista) fra API-et,
    beriket additivt med gnr/bnr parset fra tittelen (se gnr_bnr_fra_tittel) -
    både som fallback når API-et ikke gir noe, og som tillegg når tittelen
    restaterer et EKSTRA gnr/bnr utover det API-et allerede oppgir (bekreftet
    trygt ved stikkprøve: ekstra tittel-par er alltid reelle tilleggs-
    matrikler, aldri feilaktige - se modul-docstring)."""
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
# Adresse fra sakstittel
# --------------------------------------------------------------------------- #
# Tittelen starter (nesten alltid) med matrikkelnr FØRST, uten skilletegn til
# selve adressen ("182/1/0/0 Sessøya 2, ..." / "115/132 Ringvegen 470, ...") -
# se modul-docstring. Denne strippes av FØR resten tolkes med samme
# komma-baserte adresseparser som Trondheim bruker (_top_level_split,
# _parse_address_parts, husnummer-validering) - se der for detaljerte
# docstrings om hvorfor hvert steg finnes.
# Matrikkelnr-prefikset kan gjentas (samme eller ulikt gnr/bnr restatert rett
# etter det første, evt. i parentes og/eller skilt med " - "/" – " -
# f.eks. "118/1748/0/0 (117/1723)  - Nordheimvegen 65 ..." eller
# "70/66/0/0 70/66 - Eidhøgda 3, ..."). Matcher derfor ALLE slike ledende
# gjentakelser i én omgang (+ i stedet for ett enkelt strip), ellers blir en
# restatert matrikkel liggende igjen og korrumperer/blokkerer adressen som
# følger (bekreftet på ekte data - se modul-docstring/rapport).
_LEADING_MATRIKKEL = re.compile(
    r"^(?:\(?\d+/\d+(?:/\d+(?:/\d+)?)?\)?\s+(?:[-–]\s+)?)+"
)

_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_TRAILING_PAREN = re.compile(r"(?:\s*\([^)]*\))+\s*$")
_TRAILING_MFL = re.compile(r"\s+(?:m\.?\s*fl\.?|med\s+flere)\s*$", re.IGNORECASE)
_INGEN_ADRESSE = re.compile(r"^ingen\s+adresse\b", re.IGNORECASE)
_JUNK_LABELS = {"henvendelse", "tilsyn", "ingen registrering", "eiendom", "ingen adresse",
                "ufordelte", "-"}
# "Etikett + tallreferanse" som strukturelt ser ut som "Gatenavn husnummer"
# (matcher _STREET_AND_NUMBER_RANGE) men er en vei-/plan-/programreferanse,
# ikke en reell adresse - "Fylkesvei 7772"/"Fylkesveg 862" (fylkeskommunalt
# veinummer), "Plan 1873"/"Reguleringsplan 717" (plan-id), "KIK 2025"
# (tilskuddsprogram + årstall). Bekreftet ved full gjennomgang av samtlige
# ~12500 poster i arkivet - 25 tilfeller, alle i denne kategorien, ingen
# falske positiver (ekte gatenavn i arkivet starter aldri med disse ordene).
_IKKE_ADRESSE_ETIKETT = re.compile(
    r"^(?:fylkesvei|fylkesveg|riksvei|riksveg|europavei|europaveg|"
    r"reguleringsplan|detaljregulering\w*|områderegulering\w*|"
    r"kommunedelplan|plan|kik)\s+\d",
    re.IGNORECASE,
)

TOKEN = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*-\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))?"
_BARE_TOKEN_FULL = re.compile(rf"^{TOKEN}$")
_BARE_LETTER_TOKEN = re.compile(r"^[A-Za-zæøåÆØÅ]$")
_STREET_AND_NUMBER_RANGE = re.compile(rf"^(.+?)\s+({TOKEN})$")
# "Gatenavn N1/N2[/N3...]" - flere husnummer på samme gate skilt med
# skråstrek i stedet for bindestrek/komma ("Bankgata 9/11", "Storgata
# 104/106", "Hjalmar Johansens gate 314/316") - IKKE samme token som brukes
# i _BARE_TOKEN_FULL/TOKEN over (holdt bevisst separat: hvis skråstrek ble
# tillatt der òg ville en etterfølgende komma-adskilt bar matrikkel-referanse,
# f.eks. "..., 18/700, ..." etter en allerede satt gatenavn, feilaktig blitt
# lest som en fortsettelse av samme gate). Disse behandles IKKE som ett
# husnummerspenn (i motsetning til bindestrek-varianten over), men som FLERE
# separate adresser - bekreftet ønsket av bruker for "Bankgata 9/11" ->
# "Bankgata 9; Bankgata 11".
_TOKEN_SLASH = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*/\s*\d+[A-Za-zæøåÆØÅ]?)+"
_STREET_AND_NUMBER_SLASH = re.compile(rf"^(.+?)\s+({_TOKEN_SLASH})$")
# Ord som strukturelt kan stå rett foran et skråstrek-tallpar men som
# signaliserer at tallparet er en gnr/bnr- eller saksnummer-referanse, IKKE
# et gatenavn med to husnummer - "Eiendom(men) 79/115", "på eiendom 115/55",
# "se sak 22/00672". Bekreftet på ekte data: ekte gatenavn i arkivet er ALDRI
# et av disse ordene (de er journal-/matrikkel-sjargong, ikke stedsnavn).
_NON_ADDRESS_LABEL_WORDS = {
    "eiendom", "eiendommen", "gnr", "bnr", "gbnr", "matrikkel", "matrikkelnr",
    "byggesak", "sak", "saksnr", "saksnummer",
}
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")
# Beskrivelsestekst starter noen ganger med stor bokstav også (norsk
# heltsetning) og kan tilfeldigvis ende på et tall ("... nummer 2",
# "... seksjon 1") - da matcher den ellers _STREET_AND_NUMBER_RANGE og
# _STARTS_UPPER og blir feilaktig lest som en ny adresse i en komma-/og-liste.
# Et midtstilt preposisjons-/beskrivelsesord er et pålitelig tegn på at det
# IKKE er et ekte gatenavn (bekreftet: null falske positiver mot de 182 ekte
# multi-adresse-tilfellene i arkivet) - samme prinsipp som Askers
# _PREPOSISJON_MIDT/_DESCRIPTION_MARKERS.
_DESCRIPTIVE_MIDTORD = re.compile(
    r"\b(?:av|om|for|til|med|på|vedrørende|angående|nummer)\b", re.IGNORECASE
)
# En "gatenavn"-kandidat i en komma-/og-fortsettelse som selv inneholder et
# LØSRIVET tallord ("Løfteinnretning NINR 1 1902") er ikke et ekte stedsnavn,
# men en beskrivelse med et internt referansenummer (NINR = identifikasjons-
# nummer for løfteinnretning i tilsynssaker) som tilfeldigvis ender på nok et
# tall og ellers matcher _STREET_AND_NUMBER_RANGE/_STREET_AND_NUMBER_SLASH +
# _STARTS_UPPER uten noe _DESCRIPTIVE_MIDTORD-preposisjon å fange det på.
# Ekte gatenavn har aldri et løsrevet tallord midt i seg. Bekreftet trygt ved
# full korpus-sammenligning (fikser 2 allerede lagrede feil av nøyaktig
# samme mønster - "Grønnegata 80; Løfteinnretning NINR 1 1902 05119" ->
# "Grønnegata 80" - uten å endre noen andre av de 182 ekte multi-adresse-
# tilfellene).
_BARE_DIGIT_WORD = re.compile(r"(?<!\S)\d+(?!\S)")


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


def _leading_matrikkel_numbers(tittel):
    """Alle tallene i et evt. ledende matrikkelnr-prefiks (se
    _LEADING_MATRIKKEL) - brukt av _parse_address_parts til å gjenkjenne når
    et skråstrek-tallpar lenger ut i tittelen bare RESTATER samme gnr/bnr som
    prefikset (ikke nye husnummer), f.eks. "96/24/0/0 Vengsøya 96/24" eller
    "17/1687/0/0 Isbjørnvegen 32. 17/1687" - se bruken i
    _STREET_AND_NUMBER_SLASH-grenen."""
    if not tittel:
        return frozenset()
    m = _LEADING_MATRIKKEL.match(tittel.strip())
    if not m:
        return frozenset()
    return frozenset(re.findall(r"\d+", m.group(0)))


def _parse_address_parts(s, leading_nums=frozenset()):
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
        if sm and (sep is None or (_STARTS_UPPER.match(sm.group(1).strip())
                                    and not _DESCRIPTIVE_MIDTORD.search(sm.group(1))
                                    and not _BARE_DIGIT_WORD.search(sm.group(1)))):
            current_street = sm.group(1).strip()
            norm = re.sub(r"\s*-\s*", "-", sm.group(2))
            entries.append(f"{current_street} {norm}")
            mnum = re.match(r"^(\d+)", norm)
            if mnum:
                last_number = mnum.group(1)
            continue
        sm_slash = _STREET_AND_NUMBER_SLASH.match(part)
        if sm_slash and (sep is None or (_STARTS_UPPER.match(sm_slash.group(1).strip())
                                          and not _DESCRIPTIVE_MIDTORD.search(sm_slash.group(1))
                                          and not _BARE_DIGIT_WORD.search(sm_slash.group(1)))):
            street_candidate = sm_slash.group(1).strip()
            nums = [n.strip() for n in sm_slash.group(2).split("/")]
            last_word = re.sub(r"\W+$", "", street_candidate).rsplit(" ", 1)[-1].lower()
            # Avvis restatert gnr/bnr (tallparet finnes allerede i det
            # ledende matrikkelnr-prefikset), etikett+tallreferanse
            # ("eiendom"/"sak"/... - se _NON_ADDRESS_LABEL_WORDS), og "0" som
            # husnummer (kildens placeholder, forekommer aldri i en ekte
            # adresse - samme prinsipp som gnr "0" i _TITLE_GNR_BNR).
            restatement = bool(leading_nums) and set(nums) <= leading_nums
            if ("0" not in nums and last_word not in _NON_ADDRESS_LABEL_WORDS
                    and not restatement):
                current_street = street_candidate
                for n in nums:
                    entries.append(f"{current_street} {n}")
                last_number = nums[-1]
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
        # Trailing-parentes fjernes FØR bindestrek-trunkering: en " - " som
        # ligger INNI en avsluttende parentes ("Nordheimvegen 63 (hus 1 -
        # BKS4)") skal ikke tolkes som en bindestrek-separator mot
        # beskrivelsen (det korrumperer/etterlater en ubalansert parentes,
        # f.eks. "Nordheimvegen 63 (hus 1" - bekreftet på ekte data).
        depaned = _TRAILING_PAREN.sub("", seg_stripped).strip()
        seg_proc = _truncate_at_separator_dash(depaned)
        cleaned = _TRAILING_MFL.sub("", seg_proc).strip()
        cleaned = cleaned.rstrip(" -").strip()   # fjern løs bindestrek-hale ("Presis Bolig AS -")
        if (cleaned and cleaned.lower() not in _JUNK_LABELS
                and not _IKKE_ADRESSE_ETIKETT.match(cleaned)):
            head = cleaned
            head_idx = idx
            break
        return None

    if head is None:
        return None

    tail = ",".join(segs[head_idx + 1:])
    full_candidate = _normalize_nummer_bokstav(head + ("," + tail if tail else ""))
    entries = _parse_address_parts(full_candidate, leading_nums=_leading_matrikkel_numbers(tittel))
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
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(
        proceeding.get("propertyIdentifications"), proceeding.get("title")
    )
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
    tittel = data.get("title")
    gnr_bnr, matrikkelnr = gnr_bnr_matrikkel(data.get("propertyIdentifications"), tittel)
    saksnummer = data.get("sequenceNumber")
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
# Azure Blob - opplasting av endringslogg + full-dump
# --------------------------------------------------------------------------- #
def _azure_credential():
    """Azure Blob-credential, med tre mulige kilder, forsøkt i denne
    rekkefølgen (identisk mønster som Kristiansand, se der for utfyllende
    docstring):
      1) Databricks secrets (scope "postlister"), hvis kjørt der.
      2) Service Principal via miljøvariabler - lastes fra bronze/postlister/
         .env (samme delte hemmeligheter/fil som resten av "postlister"-
         containeren, IKKE kommune-spesifikk).
      3) Egen Azure CLI-innlogging (`az login`) via AzureCliCredential.
    Kaster ValueError/RuntimeError med en tydelig melding hvis ingen av dem
    virker - upload_til_azure()/upload_full_dump_til_azure() fanger denne og
    hopper over opplastingen i stedet for å krasje hele den lokale
    skrape-kjøringen."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    except Exception:  # noqa: BLE001
        dbutils = None

    if dbutils is not None:
        get = lambda k: dbutils.secrets.get(scope="postlister", key=k)  # noqa: E731
    else:
        try:
            from dotenv import load_dotenv
            # Fila ligger på bronze/postlister/-nivå (delt for alle kommuner) -
            # let oppover til vi finner mappen som faktisk heter "postlister".
            for parent in Path(__file__).resolve().parents:
                if parent.name == "postlister":
                    load_dotenv(dotenv_path=parent / ".env")
                    break
        except Exception:  # noqa: BLE001
            pass
        get = lambda k: os.environ.get(k)  # noqa: E731

    client_id = get("SP-PIPELINE-POSTLISTER-CLIENT-ID")
    tenant_id = get("SP-PIPELINE-POSTLISTER-TENANT-ID")
    client_secret = get("SP-PIPELINE-POSTLISTER-CLIENT-SECRET")

    if client_id and tenant_id and client_secret:
        from azure.identity import ClientSecretCredential
        return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    # Ingen Service Principal satt opp - fall tilbake til egen az-login.
    try:
        from azure.identity import AzureCliCredential
        credential = AzureCliCredential()
        credential.get_token("https://storage.azure.com/.default")
        return credential
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "Mangler Azure-credential: ingen Service Principal satt (sett "
            "SP-PIPELINE-POSTLISTER-CLIENT-ID/-TENANT-ID/-CLIENT-SECRET i "
            "bronze/postlister/.env, se .env.example) OG 'az login' via Azure "
            f"CLI er ikke gjort/fungerer ikke ({e})"
        ) from e


def _last_opp_jsonl_gz(poster, blob_name, credential):
    """Serialiser en liste med poster til JSONL (én post per linje),
    gzip-komprimer, og last opp som én blob (overskriver hvis den allerede
    finnes - trygt å kjøre kjøringen på nytt for samme dato)."""
    from azure.storage.blob import BlobServiceClient

    jsonl = "\n".join(json.dumps(p, ensure_ascii=False) for p in poster)
    komprimert = gzip.compress(jsonl.encode("utf-8"))

    client = BlobServiceClient(AZURE_ACCOUNT_URL, credential=credential,
                                connection_timeout=TIMEOUT, read_timeout=TIMEOUT)
    blob = client.get_container_client(AZURE_CONTAINER_NAME).get_blob_client(blob_name)
    blob.upload_blob(komprimert, overwrite=True)
    return len(komprimert)


def _fallback_lagre_midlertidig(poster, filnavn):
    """Skriver en liste med poster til systemets midlertidige mappe (IKKE i
    repoet, og IKKE et sted som vokser ubegrenset) - brukes KUN som
    nødløsning når selve Azure-opplastingen feiler (manglende credentials,
    nettverksfeil), slik at dagens data ikke går helt tapt. Stien logges
    tydelig slik at filen kan følges opp/lastes opp på nytt manuelt - i
    motsetning til de gamle output/-filene blir denne ALDRI en permanent
    lokal kopi (OS-en rydder egen temp-mappe over tid)."""
    path = Path(tempfile.gettempdir()) / f"{filnavn}.json"
    try:
        path.write_text(json.dumps(poster, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  [Azure] lagret midlertidig lokalt i stedet (IKKE i repoet): {path}")
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KLARTE IKKE Å lagre fallback-kopi heller ({e}) - "
              f"disse {len(poster)} postene er tapt for denne kjøringen")


def upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype):
    """Laster dagens endringslogg (nye saker + nye journalposter-på-gamle-
    saker) DIREKTE til Azure Blob som JSONL+gzip - INGEN lokal fil skrives i
    repoet i det hele tatt (se write_output-docstring). Dette kjører daglig
    i det uendelige på tvers av fire sakstyper, og ett filpar per dag/
    sakstype ville vokst ubegrenset på disken over tid - selve poenget med
    denne funksjonen er å unngå akkurat det.

    Stikonvensjon (identisk med Kristiansand): bronze/tromso/
    load_type=incremental/date=<ref_date>/<slug>_saker-<ref_date>.jsonl.gz
    (+ <slug>_journalposter-<ref_date>.jsonl.gz).

    Feiler ALDRI hele kjøringen ved Azure-problemer (manglende credentials,
    nettverksfeil, osv.) - i stedet havner en midlertidig fallback-kopi
    UTENFOR repoet (se _fallback_lagre_midlertidig), slik at dagens data
    ikke går tapt selv om ingenting normalt skrives lokalt."""
    slug = _SAKSTYPE_SLUG.get(sakstype, sakstype.lower())
    try:
        credential = _azure_credential()
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KUNNE IKKE laste opp - {e}")
        for filnavn, poster in ((f"{slug}_saker-{ref_date}", nye_saker),
                                 (f"{slug}_journalposter-{ref_date}", nye_journalposter)):
            if poster:
                _fallback_lagre_midlertidig(poster, filnavn)
        return

    prefix = f"{AZURE_BASE_PATH}/load_type=incremental/date={ref_date}"
    for filnavn, poster in ((f"{slug}_saker", nye_saker), (f"{slug}_journalposter", nye_journalposter)):
        if not poster:
            continue
        blob_name = f"{prefix}/{filnavn}-{ref_date}.jsonl.gz"
        try:
            storrelse = _last_opp_jsonl_gz(poster, blob_name, credential)
            print(f"  [Azure] lastet opp {len(poster)} poster -> "
                  f"{AZURE_CONTAINER_NAME}/{blob_name} ({storrelse / 1024:.1f} KB)")
        except Exception as e:  # noqa: BLE001
            print(f"  [Azure] opplasting FEILET for {blob_name}: {e}")
            _fallback_lagre_midlertidig(poster, f"{filnavn}-{ref_date}")


def upload_full_dump_til_azure(poster, sakstype):
    """Laster HELE engangs-historikk-dumpen (all-i-en-fil fra run_full_dump(),
    som selv etter et delvis `python3 main.py <start> <end>`-kall alltid
    inneholder den FULLE akkumulerte tilstanden - se load_existing_saker/
    _merge_journalposter) opp til Azure Blob som JSONL+gzip - én blob for
    hele sakstypen.

    Stikonvensjon (identisk med Kristiansand): bronze/tromso/load_type=full/
    <slug>/dump_date=<i_dag>/<slug>_saker-full-<i_dag>.jsonl.gz - partisjonert
    på KJØREDATO (når dumpen ble tatt), ikke sakens egen dato, siden
    run_full_dump samler ALT i én fil i stedet for å partisjonere per måned
    slik Trondheim gjør.

    Feiler ALDRI (samme mønster som upload_til_azure) - den lokale filen fra
    run_full_dump() er alltid den autoritative kopien uansett."""
    if not poster:
        print("  [Azure] ingen poster å laste opp (tom dump)")
        return

    slug = _SAKSTYPE_SLUG.get(sakstype, sakstype.lower())
    try:
        credential = _azure_credential()
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] hopper over full-dump-opplasting - {e}")
        return

    i_dag = datetime.now(ZoneInfo("Europe/Oslo")).date()
    blob_name = f"{AZURE_BASE_PATH}/load_type=full/{slug}/dump_date={i_dag}/{slug}_saker-full-{i_dag}.jsonl.gz"
    try:
        storrelse = _last_opp_jsonl_gz(poster, blob_name, credential)
        print(f"  [Azure] lastet opp {len(poster)} poster (full dump) -> "
              f"{AZURE_CONTAINER_NAME}/{blob_name} ({storrelse / 1024:.1f} KB)")
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] full-dump-opplasting FEILET for {blob_name}: {e}")


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

    # Azure-opplasting er BEVISST frakoblet her - kjør run_full_dump() rent
    # lokalt (ingen forsøk på tilkobling). Funksjonen upload_full_dump_til_azure()
    # står fortsatt klar og er uendret - kall den manuelt når Azure-tilgang er
    # på plass, se retry_azure_upload.py i dump-mappa for mønsteret.
    # upload_full_dump_til_azure(list(saker_by_id.values()), sakstype)


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - datovindu (ekte datoer, ingen snapshot)
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 1   # dager bakover fra referansedatoen (1 = kun referansedagen)


def write_output(nye_saker, nye_journalposter, ref_date, sakstype):
    """Laster dagens endringslogg til Azure - se upload_til_azure(). Skriver
    BEVISST ingen lokal fil i repoet: dette kjører daglig i det uendelige på
    tvers av fire sakstyper, og en varig lokal kopi per dag ville vokst
    ubegrenset på disken over tid. Ved en mislykket Azure-opplasting havner
    en fallback-kopi i systemets midlertidige mappe i stedet (se
    _fallback_lagre_midlertidig), aldri i repoet."""
    print(f"{len(nye_saker)} nye saker, {len(nye_journalposter)} saker med nye journalposter i dag")
    upload_til_azure(nye_saker, nye_journalposter, ref_date, sakstype)


def run_daily(type_id, sakstype, day_back=1, window_days=WINDOW_DAYS):
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

    write_output(nye_saker, nye_journalposter, to_iso, sakstype)
