"""Delt bibliotek for Trondheim bygg/henv/tilsyn/ulov-scraperne.

Trondheim kjører "360 Plan & Build" via innsynsportal.no (GraphQL) - samme
plattform som Tromsø/Asker/Drammen. API-et støtter ikke offset-paginering
(load-more-queryene feiler alltid), og "Date"-feltene godtar kun yyyy-mm-dd.
Løsning: hent dag-for-dag med eneste queryen som faktisk fungerer
(SearchPostJournal, maks limit=100/felt) fra sakstypens startdato og til i
dag. De fleste dager er godt under 100 saker/journalposter; noen få dager
over grensen fylles etter med et departement-filter, ellers logges en
ADVARSEL. (Merk: Asker/Drammen har en ekstra bug der proceedings-datofilteret
lekker saker fra andre datoer - IKKE bekreftet for Trondheim, som derfor
beholder den enklere dag-for-dag-strategien uendret.)

Fire sakstyper, hver med egen GraphQL-type (ingen heuristikk trengs, ulikt
Asker som mangler egne henv/tilsyn-typer). Type-UUID-ene ble funnet ved å
samle opp (type.id, type.name)-par fra usortert sampling - ikke gjettet.
Sekvensnummer har prefiks "HENV-"/"ULOV-" i nyere saker, men eldre saker
(fra 2017) deler alle "BYGG-"-prefikset uansett type - kun type.id/type.name
skiller sakstype pålitelig.

Adresseformat: adressen står først i tittelen, evt. etter en "Eiendom(men)
gnr/bnr"-henvisning (også skrevet "Eieindom" - kjent skrivefeil i kilden).
Kan liste flere husnumre for samme gate (komma- og/eller "og"-liste) eller et
bindestrek-spenn ("2 - 6"). Eldre saker bruker noen ganger " - " som
separator i stedet for komma. Rene stikkord ("Henvendelse", "Tilsyn" osv.)
og "Ingen adresse..." gir None i stedet for en gjetning.

Brukes av:
  - 2017-03-24-today_dump/{bygg,henv,tilsyn,ulov}/main.py (engangs historisk dump)
  - running_daily/{bygg,henv,tilsyn,ulov}/app/main.py     (daglig endringslogg)
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
BASE_URL = "https://trondheim.innsynsportal.no"
API_URL = f"{BASE_URL}/graphql"
LIST_ID = "fc010204-2c07-4f3c-a8ef-879ec218111c"   # delt for alle fire sakstyper
PORTAL_URL = f"{BASE_URL}/postjournal-v2/{LIST_ID}"
DOCUMENT_URL = BASE_URL + "/file/{}"   # nedlastings-URL for dokumenter (kun ikke-graderte)

KOMMUNE = "Trondheim"
KOMMUNE_NR = 5001

# Azure Blob - samme lagringskonto/container og samme mønster (Service
# Principal via env/.env, JSONL+gzip) som Kristiansand/Tromsø-pipelinene i
# dette repoet, se _azure_credential()/upload_til_azure() nedenfor. Den
# daglige endringsloggen (se write_output) skriver BEVISST ingen lokal fil i
# repoet: den kjører daglig i det uendelige på tvers av fire sakstyper, og
# en varig lokal kopi per dag ville vokst ubegrenset på disken over tid.
# `kilde_key` (bygg/henv/tilsyn/ulov, se KILDER under) brukes DIREKTE som
# Azure-slug - ingen egen navnetabell trengs siden KILDER allerede er keyet
# på nøyaktig disse forkortelsene.
AZURE_ACCOUNT_URL = "https://storaggen2eaccountprod.blob.core.windows.net"
AZURE_CONTAINER_NAME = "postlister"
AZURE_BASE_PATH = "bronze/trondheim"

MAX_LIMIT = 100        # hard servergrense per felt ("Too large limit" over dette)
TIMEOUT = 60
RETRIES = 4

# NB: .strip() er nødvendig - serveren avviser query-tekst med avvikende
# whitespace, uansett om where-innholdet er gyldig. Identisk med
# Tromsø/Asker/Drammen sin QUERY (samme plattform/schema).
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
# KILDER - de fire sakstypene (egne GraphQL-typer, samme LIST_ID)
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        type_id="2de3bd79-704d-4738-8b05-5c7257623420",
        start_date=date(2017, 3, 24),   # eldste byggesak (BYGG-17/80001)
    ),
    "henv": dict(
        sakstype_navn="Henvendelse",
        type_id="a7c85533-e802-4aa5-bee2-46a24fbed199",
        start_date=date(2018, 6, 18),   # eldste henvendelse (BYGG-18/82521)
    ),
    "tilsyn": dict(
        sakstype_navn="Tilsynssak",
        type_id="7b2a9cb8-1948-4a28-93f1-e6e9d198ad68",
        start_date=date(2017, 5, 3),   # eldste tilsynssak (BYGG-17/82748)
    ),
    "ulov": dict(
        sakstype_navn="Ulovlighetssak",
        type_id="84e9e827-13cc-42d6-a1ea-929c820dfe58",
        start_date=date(2017, 4, 27),   # eldste ulovlighetssak (BYGG-17/80032)
    ),
}


# --------------------------------------------------------------------------- #
# gnr/bnr/matrikkelnr - API-et (propertyIdentifications) + tittel-fallback
# --------------------------------------------------------------------------- #
# Fanger gnr/bnr[/feste[/seksjon]] når tallkjeden innledes av tittel-start,
# komma, bindestrek, åpningsparentes, eller "eiendom(men)"-henvisningen (se
# _EIENDOM_PREFIX under - samme skrivefeilvariant "eieindom" dekkes her også)
# - IKKE når det er en del av selve gateadressen (et gate-husnummerspenn som
# "Fr.Langesgt. 19/21" har ALDRI et av disse skilletegnene/ordet foran seg,
# ulikt ekte gnr/bnr-oppføringer - bekreftet ved stikkprøve mot Tromsø/Asker,
# som deler samme kildeplattform og titteldialekt). gnr "0" er kildens
# plassholder for "ingen egen eiendom" og utelates. Hele tallkjeden fanges
# (ikke bare de første to) fordi et fåtall titler skriver kommunenummeret
# FØRST i kjeden ("5001/177/821/0/0" = KOMMUNE_NR/gnr/bnr/feste/seksjon, ikke
# gnr/bnr 5001/177 - bekreftet på ekte data, se _gnr_bnr_fra_tallkjede).
# Trigger-settet inkluderer også selve ordet "gnr/bnr"/"gbnr" (med evt.
# kolon rett etter, "gnr/bnr: 410/488") som label foran tallkjeden - samme
# fiks som Drammen fikk 2026-09-02 (se HANDOVER.md), lagt til her fordi
# "gnr/bnr: 410/488, Bakke bru, tilsynssak" ikke ble fanget av noen av de
# andre skilletegnene (kolon er ikke med i det opprinnelige settet).
_TITLE_GNR_BNR = re.compile(
    r"(?:^|[,\-(]|\beie(?:i)?ndom(?:men)?\s*\(?|\bgnr\s*/\s*bnr\.?:?|\bgbnr\.?:?)\s*(\d{1,4}(?:/\d{1,5}){1,3})",
    re.IGNORECASE,
)


def _gnr_bnr_fra_tallkjede(tallkjede):
    """Tolk en tallkjede "A/B[/C[/D]]" som gnr/bnr - hopper over en ledende
    KOMMUNE_NR hvis kjeden starter med den (se _TITLE_GNR_BNR over)."""
    nums = tallkjede.split("/")
    if nums[0] == str(KOMMUNE_NR) and len(nums) >= 3:
        return nums[1], nums[2]
    return nums[0], nums[1]


def gnr_bnr_fra_tittel(tittel):
    """Finn ALLE gnr/bnr-par i sakstittelen (ledende matrikkelnr, restatert/
    ekstra matrikkelnr i parentes, "eiendom(men) gnr/bnr"-henvisning, eller et
    matrikkelnr som følger etter adressen). Brukt som fallback/tillegg til
    propertyIdentifications i gnr_bnr_matrikkel()."""
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
    """Dedupe (propertyNr, useNr)-par (de dubleres i lista) fra API-et,
    beriket additivt med gnr/bnr parset fra tittelen (se gnr_bnr_fra_tittel) -
    både som fallback når API-et ikke gir noe, og som tillegg når tittelen
    restaterer et EKSTRA gnr/bnr utover det API-et allerede oppgir."""
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
_NUM_LETTER_SPACE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
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
# "Gatenavn N1/N2[/N3...]" - flere husnummer på samme gate skilt med
# skråstrek i stedet for bindestrek/komma ("Harald Hårfagres gate 6/8") -
# TOKEN over håndterer ALDRI skråstrek (kun bindestrek-spenn), så denne
# konvensjonen falt tidligere alltid gjennom hele parseren og ga None,
# uansett hvor riktig resten av tittelen så ut (bekreftet av bruker
# 2026-09-02, samme mønster/fiks som Tromsø fikk samme dag - egen fil/kode
# her, ikke delt, men strukturelt identisk siden begge bygger på samme
# Trondheim-avledede parser). Holdt bevisst SEPARAT fra TOKEN (ikke slått
# sammen): hvis skråstrek ble tillatt der òg, ville en senere komma-adskilt
# bar matrikkelreferanse feilaktig blitt lest som en fortsettelse av samme
# gate. Splitter til FLERE separate adresser (ikke ett spenn) - "6/8" ->
# "6; 8", bekreftet ønsket av bruker.
_TOKEN_SLASH = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*/\s*\d+[A-Za-zæøåÆØÅ]?)+"
_STREET_AND_NUMBER_SLASH = re.compile(rf"^(.+?)\s+({_TOKEN_SLASH})$")
# "Gatenavn N BOKSTAV1 + BOKSTAV2[+ BOKSTAV3...]" - flere bokstav-suffikser
# på SAMME husnummer skilt med "+" ("Nerviksvegen 28 A + B" -> "28A"/"28B" på
# samme hus, "Kvernvegen 11 E+F" -> "11E"/"11F"; bekreftet av bruker
# 2026-09-02). Skiller seg fra _TOKEN_SLASH over ved at det er ETT tall med
# FLERE bokstav-suffiks, ikke flere ulike tall. KUN ett tegn (bokstav) tillatt
# etter hver "+" (ikke et helt ord) - holder pattern'et fra å feilaktig matche
# beskrivelsestekst som "5 etasjer + kjeller"/"kjede + eneboliger" (sjekket
# mot alle 6 titler i arkivet som inneholder "+" - kun disse to var reelle
# adressetilfeller, resten er beskrivelser eller allerede en gnr/bnr-liste
# "28/7 + 28/3" som denne bevisst IKKE matcher siden det ikke er tall rett
# etter "+" der).
_TOKEN_PLUS = r"\d+[A-Za-zæøåÆØÅ]?(?:\s*\+\s*[A-Za-zæøåÆØÅ])+"
_STREET_AND_NUMBER_PLUS = re.compile(rf"^(.+?)\s+({_TOKEN_PLUS})$")
_PLUS_SPLIT = re.compile(r"\s*\+\s*")
# Ord/uttrykk som kan stå rett foran et tallpar/tall men som signaliserer at
# det IKKE er et gatenavn: journal-/matrikkel-sjargong ("eiendom(men/ene)"/
# "eieindom(men)" - kjent skrivefeil i kilden - /"gnr"/"bnr"/"gnr/bnr"/"gbnr"/
# "matrikkel(nr)"/"sak(snr/snummer)"/"byggesak") eller et prosjektnavn/
# byggefase-referanse ("byggetrinn N" - "Grilstad Marina B3 byggetrinn 3"
# er IKKE en adresse, bekreftet av bruker 2026-09-02; "Steinan Park
# byggetrinn 2"/"Søknad om ramme for byggetrinn 3" lest som falske TILLEGGS-
# adresser i en ellers riktig komma-liste, samme rotårsak). Regex (ikke et
# enkelt sett) fordi disse forekommer med ulik tegnsetting i kildedata -
# "gnr/bnr", "gnr./bnr.", "Gnr 24 Bnr 407" (mellomrom i stedet for skråstrek/
# komma - fanges av _last_word siden hvert ord sjekkes for seg). Sjekkes mot
# SISTE ord i "gatenavn"-kandidaten, uansett om den er det aller første
# elementet eller en komma-/og-fortsettelse (ulikt Tromsøs tilsvarende vern,
# som kun gjelder fortsettelser - her må også FØRSTE element sjekkes siden
# "byggetrinn"-bugen og flere "Gnr X Bnr Y skrevet som adresse"-bugger
# (bekreftet ekte, IKKE regresjoner - se full-korpus-verifisering 2026-09-02)
# slår til nettopp der, se _parse_address_parts).
_NON_ADDRESS_LAST_WORD = re.compile(
    r"^(?:eie(?:i)?ndom(?:men|mer|mene)?|gnr\.?\s*/\s*bnr\.?|gbnr\.?|gnr\.?|bnr\.?|"
    r"matrikkel(?:nr)?|sak(?:snr|snummer)?|byggesak|byggetrinn)$",
    re.IGNORECASE,
)
# "Eiendommen ... har ikke adresse, <gnr>/<bnr>" (segment inneholder denne
# frasen ET STED, ikke nødvendigvis som siste ord) er en eksplisitt uttalt
# ikke-adresse etterfulgt av en matrikkelreferanse - IKKE det samme mønsteret
# som _INGEN_ADRESSE over (som krever at STRENGEN STARTER med "ingen
# adresse"), bekreftet på ekte data ("Eiendommen har ikke adresse 200/50" ble
# feilaktig lest som to husnummer 200 og 50 før dette vernet).
_HAR_IKKE_ADRESSE = re.compile(r"\bikke\s+adresse\b", re.IGNORECASE)
_PART_SPLIT = re.compile(r"\s*(,|\bog\b)\s*")
_STARTS_UPPER = re.compile(r"^[A-ZÆØÅ]")
_STARTS_LETTER = re.compile(r"^[A-Za-zæøåÆØÅ]")


def _normalize_nummer_bokstav(s):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks ("50 A" -> "50A")."""
    return _NUM_LETTER_SPACE.sub(r"\1\2", s)


def _top_level_split(s, delim=","):
    """Del s på delim, men ignorer forekomster inni parenteser."""
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
    """Trunker til venstre for ' - ' med mindre streken binder sammen to
    tall/bokstav-tokens (da er den del av selve adressen, f.eks. '2 - 6')."""
    idx = seg.find(" - ")
    if idx == -1:
        return seg
    left, right = seg[:idx], seg[idx + 3:]
    left_m = re.search(r"(\d+[A-Za-zæøåÆØÅ]?)\s*$", left)
    right_m = re.match(r"\s*(\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ])\b", right)
    if left_m and right_m:
        return seg
    return left.strip()


def _last_word(s):
    """Siste "ord" i en gatenavn-kandidat, uten omsluttende tegnsetting (bl.a.
    en åpen parentes foran, "(eiendommen" - forekommer ved et par ubalanserte
    parenteser i kildedata) - brukt til å sjekke mot _NON_ADDRESS_LAST_WORD
    (journal-/matrikkel-sjargong eller "byggetrinn")."""
    word = re.sub(r"\W+$", "", s).rsplit(" ", 1)[-1]
    return re.sub(r"^\W+", "", word).lower()


def _parse_address_parts(s):
    """Tolker s som en liste av adresser atskilt med komma og/eller ' og '.
    Et bart tall/spenn arver gatenavnet fra forrige element, en bar bokstav
    arver gatenavn+tall, og et nytt 'Gatenavn nummer' (stor forbokstav
    kreves etter komma/og, for å skille fra beskrivelsestekst) starter en ny
    gate. Stopper så snart et element ikke passer noen av mønstrene."""
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
            street_candidate = sm.group(1).strip()
            # "byggetrinn N" (byggefase, ikke en gate) og "Gnr X Bnr Y"/
            # "Eiendommen ... har ikke adresse N" (matrikkelreferanse skrevet
            # med mellomrom i stedet for skråstrek, ser strukturelt ut som
            # "gatenavn husnummer") kan begge dukke opp her - avvis uansett om
            # dette er første element eller en fortsettelse, se
            # _NON_ADDRESS_LAST_WORD/_HAR_IKKE_ADRESSE. Alle disse
            # tilfellene ble bekreftet å IKKE være regresjoner (var allerede
            # feilaktige/garblede adresser før denne sjekken ble lagt til -
            # se full-korpus-verifisering 2026-09-02 i HANDOVER.md).
            if (_NON_ADDRESS_LAST_WORD.match(_last_word(street_candidate))
                    or _HAR_IKKE_ADRESSE.search(street_candidate)):
                break
            current_street = street_candidate
            norm = re.sub(r"\s*-\s*", "-", sm.group(2))
            entries.append(f"{current_street} {norm}")
            mnum = re.match(r"^(\d+)", norm)
            if mnum:
                last_number = mnum.group(1)
            continue
        sm_slash = _STREET_AND_NUMBER_SLASH.match(part)
        if sm_slash and (sep is None or _STARTS_UPPER.match(sm_slash.group(1).strip())):
            street_candidate = sm_slash.group(1).strip()
            nums = [n.strip() for n in sm_slash.group(2).split("/")]
            # Avvis: husnummer "0" (kildens placeholder, forekommer aldri i
            # en ekte adresse); etikett+tallreferanse/"byggetrinn"/"har ikke
            # adresse" foran tallparet; gatenavn-kandidater som selv starter
            # med et TALL (aldri et ekte gatenavn - fanger opp rene
            # tallkjede-referanser som "415/50og 415/62" der "og" er limt til
            # forrige tall uten mellomrom, så det ikke matcher noe kjent
            # skilletegn og ellers ville sett ut som et gatenavn); og
            # gatenavn-kandidater som ender på en bar bindestrek ("Smelteverket
            # 1 -" foran "415/178, dispensasjon..." - bindestreken hører til
            # en ETTERFØLGENDE gnr/bnr-henvisning, ikke til gatenavnet, se
            # "Smelteverket 1 - gnr/bnr 415/178" i samme datasett som bekrefter
            # 415/178 er matrikkelnr, ikke husnummer).
            if ("0" not in nums
                    and not _NON_ADDRESS_LAST_WORD.match(_last_word(street_candidate))
                    and not _HAR_IKKE_ADRESSE.search(street_candidate)
                    and _STARTS_LETTER.match(street_candidate)
                    and not street_candidate.endswith("-")):
                current_street = street_candidate
                for n in nums:
                    entries.append(f"{current_street} {n}")
                last_number = nums[-1]
                continue
        sm_plus = _STREET_AND_NUMBER_PLUS.match(part)
        if sm_plus and (sep is None or _STARTS_UPPER.match(sm_plus.group(1).strip())):
            street_candidate = sm_plus.group(1).strip()
            units = _PLUS_SPLIT.split(sm_plus.group(2))
            base_m = re.match(r"^(\d+)", units[0])
            if (base_m
                    and not _NON_ADDRESS_LAST_WORD.match(_last_word(street_candidate))
                    and not _HAR_IKKE_ADRESSE.search(street_candidate)
                    and _STARTS_LETTER.match(street_candidate)
                    and not street_candidate.endswith("-")):
                current_street = street_candidate
                base_num = base_m.group(1)
                entries.append(f"{current_street} {units[0]}")
                for u in units[1:]:
                    entries.append(f"{current_street} {base_num}{u}")
                last_number = base_num
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
    """Adressen står først i tittelen (evt. etter 'Eiendom(men)/Eieindom(men)
    gnr/bnr'), skilt med komma - se modul-docstring for detaljer om formatet.
    Renser bort "Ingen adresse..." -> None, eiendoms-prefikset, etterheng i
    parentes og " m.fl."/" med flere", og rene stikkord -> None. Returnerer
    None (i stedet for å gjette) hvis ingenting med et ekte husnummer
    gjenstår."""
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
            # kun en eiendoms-/matrikkel-henvisning - se videre i neste
            # komma-del etter selve adressen ("Eiendom 198/7, Kirkeringen 12, ...")
            continue
        return None

    if head is None:
        return None

    rest = ",".join(segs[head_idx + 1:])
    full_candidate = _normalize_nummer_bokstav(head + ("," + rest if rest else ""))
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
    rekkefølgen (identisk mønster som Kristiansand/Tromsø):
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
    motsetning til en varig lokal kopi blir denne ALDRI en permanent lokal
    fil (OS-en rydder egen temp-mappe over tid)."""
    path = Path(tempfile.gettempdir()) / f"{filnavn}.json"
    try:
        path.write_text(json.dumps(poster, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  [Azure] lagret midlertidig lokalt i stedet (IKKE i repoet): {path}")
    except Exception as e:  # noqa: BLE001
        print(f"  [Azure] KLARTE IKKE Å lagre fallback-kopi heller ({e}) - "
              f"disse {len(poster)} postene er tapt for denne kjøringen")


def upload_til_azure(nye_saker, nye_journalposter, ref_date, slug):
    """Laster dagens endringslogg (nye saker + nye journalposter-på-gamle-
    saker) DIREKTE til Azure Blob som JSONL+gzip - INGEN lokal fil skrives i
    repoet i det hele tatt (se write_output-docstring). Dette kjører daglig
    i det uendelige på tvers av fire sakstyper, og ett filpar per dag/
    sakstype ville vokst ubegrenset på disken over tid - selve poenget med
    denne funksjonen er å unngå akkurat det.

    Stikonvensjon (identisk med Kristiansand/Tromsø): bronze/trondheim/
    load_type=incremental/date=<ref_date>/<slug>_saker-<ref_date>.jsonl.gz
    (+ <slug>_journalposter-<ref_date>.jsonl.gz). `slug` er kilde_key
    (bygg/henv/tilsyn/ulov) direkte.

    Feiler ALDRI hele kjøringen ved Azure-problemer (manglende credentials,
    nettverksfeil, osv.) - i stedet havner en midlertidig fallback-kopi
    UTENFOR repoet (se _fallback_lagre_midlertidig), slik at dagens data
    ikke går tapt selv om ingenting normalt skrives lokalt."""
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


def upload_full_dump_til_azure(poster, slug):
    """Laster HELE engangs-historikk-dumpen (all-i-en-fil fra run_full_dump(),
    som selv etter et delvis `python3 main.py <start> <end>`-kall alltid
    inneholder den FULLE akkumulerte tilstanden - se load_existing_saker/
    _merge_journalposter) opp til Azure Blob som JSONL+gzip - én blob for
    hele sakstypen.

    Stikonvensjon (identisk med Kristiansand/Tromsø): bronze/trondheim/
    load_type=full/<slug>/dump_date=<i_dag>/<slug>_saker-full-<i_dag>.jsonl.gz
    - partisjonert på KJØREDATO (når dumpen ble tatt), ikke sakens egen dato,
    siden run_full_dump samler ALT i én fil i stedet for å partisjonere per
    måned.

    Feiler ALDRI (samme mønster som upload_til_azure) - den lokale filen fra
    run_full_dump() er alltid den autoritative kopien uansett (beholdes
    lokalt for gjenopptak på tvers av kall, ulikt den daglige endringsloggen)."""
    if not poster:
        print("  [Azure] ingen poster å laste opp (tom dump)")
        return

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
# Engangs historisk dump (2017-03-24-today_dump) - dag-for-dag, gjenopptakbar
#
# Wrapperne under slår opp kilde_key i KILDER, og selve dump-/endringslogg-
# motoren (listing, save_every-batching, state-dedup for run_full_dump;
# datovindu-uttrekk for run_daily) kaller Trondheims EGNE GraphQL-kall og
# respons-parsing (fetch_window, build_sak, build_journalpost,
# parent_sak_ref) direkte.
# --------------------------------------------------------------------------- #
def run_full_dump(kilde_key, output_file, start=None, end=None, save_every=30):
    """Full historisk dump (dag-for-dag - ingen offset-paginering, se
    modul-docstring for hvorfor). GJENOPPTAKBAR på tvers av kall: laster
    eksisterende saker fra output_file (load_existing_saker) og SLÅR SAMMEN
    med det som hentes i dette kallet (journalposter unionert per sak via
    _merge_journalposter, ikke overskrevet) - så en flerårig dump trygt kan
    deles opp i flere `python3 main.py <start> <end>`-kall med ulike,
    ikke-overlappende datointervaller uten å miste tidligere hentede
    saker/journalposter."""
    kilde = KILDER[kilde_key]
    type_id, sakstype = kilde["type_id"], kilde["sakstype_navn"]
    start = start or kilde["start_date"]
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
    # på plass.
    # upload_full_dump_til_azure(list(saker_by_id.values()), kilde_key)


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - datovindu (ekte datoer, ingen snapshot)
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 1   # dager bakover fra referansedatoen (1 = kun referansedagen)


def write_output(nye_saker, nye_journalposter, ref_date, slug):
    """Laster dagens endringslogg til Azure - se upload_til_azure(). Skriver
    BEVISST ingen lokal fil i repoet (se Azure-konstantene lenger opp i
    fila for hvorfor): en varig lokal kopi per dag ville vokst ubegrenset på
    disken over tid. Ved en mislykket Azure-opplasting havner en
    fallback-kopi i systemets midlertidige mappe i stedet (se
    _fallback_lagre_midlertidig), aldri i repoet."""
    print(f"{len(nye_saker)} nye saker, {len(nye_journalposter)} saker med nye journalposter i dag")
    upload_til_azure(nye_saker, nye_journalposter, ref_date, slug)


def run_daily(kilde_key, day_back=1, window_days=WINDOW_DAYS):
    """Daglig endringslogg - datobasert (ekte 'date'/'journalDate'-felt).
    'proceeding.date' ser ut til å være siste aktivitetsdato, ikke
    opprettelsesdato, så en sak som får en ny journalpost i dag dukker
    normalt selv opp blant dagens nye saker - 'nye_journalposter'-grenen er
    derfor sjelden truffet, men beholdes som fallback for eldre forelder-
    saker utenfor vinduet."""
    kilde = KILDER[kilde_key]
    type_id, sakstype = kilde["type_id"], kilde["sakstype_navn"]
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

    write_output(nye_saker, nye_journalposter, to_iso, kilde_key)
