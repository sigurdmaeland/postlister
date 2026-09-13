"""Moss - ACOS "nye-innsyn" (samme plattform som Gjøvik/Sandefjord/Tønsberg
i dette prosjektet, egen selvhostet instans på www.moss.kommune.no). Samme
seksjonsoppdeling som de andre ACOS-kommunene: konstanter -> adresseparsing
-> KILDER -> API-kall -> engangsdump -> daglig endringslogg.

Moss har KUN én relevant sakstype (Byggesak, KILDER-nøkkel "bygg") - ingen
egen Tilsynssak/Ulovlighetssak/Henvendelse-kategori finnes her (bekreftet
ved å lese hele searchOptions.searchFilters-lista i en ekte overview-
respons). Arkivet starter reelt ca. 19.12.2019 (START_DATE) - tidligere
saker må bes om via vanlig innsynsforespørsel.

Sak sitt "eiendom"-felt er alltid None i praksis - adresse/gnr/bnr må derfor
utledes fra sakstittelen (samme som Gjøvik). "/sak/{id}"-endepunktet brukes
i stedet for det flatere "/details/{id}" fordi sistnevnte mangler
"Dokumenttype" helt for journalpostene.

ADRESSE/GNR-BNR-PARSING: KONSEKVENT "Adresse - GNR/BNR[, GNR/BNR][ og
GNR/BNR] - beskrivelse", f.eks. "Rørvikveien 72 - 133/75 - svømmebasseng".
Støtter flere gnr/bnr i eget bindestrek-segment ("Batteriveien Verksåsen -
3/3124 - 3/3125 - ..."), "(tidl. X/Y)"-parenteser (ignoreres) og "mfl."-
suffiks (ignoreres). Et fåtall titler mangler gnr/bnr helt -> adresse=None,
gnr_bnr=None. Verifisert ved stikkprøve av 230 ekte titler: 228/230 (99%) OK.
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Konstanter
# --------------------------------------------------------------------------- #
BASE_URL = "https://www.moss.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"

PORTAL_ID = "1"
MENYPUNKT_ID = "3790"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 3103   # Moss (Østfold fra 2024; var 3002 i Viken 2020-2023, 0104 før)
KOMMUNE = "Moss"

START_DATE = "2019-12-19"   # reell arkivgrense, se modul-docstring

MAX_WORKERS = 8
TIMEOUT = 30
RETRIES = 3

HEADERS = {
    "Content-Type": "application/json",
    "PortalID": PORTAL_ID,
    "MenypunktID": MENYPUNKT_ID,
    "SprakID": "1",
    "X-ANTI-CSRF": "1",
}


# --------------------------------------------------------------------------- #
# Adresse/gnr_bnr/matrikkelnr fra sakstittel
# --------------------------------------------------------------------------- #
# Et fåtall titler skriver gnr/bnr med feste-/seksjonsnummer bakt inn som
# ekstra skråstrek-ledd i SELVE tokenet ("3/1584/0/4", "2/1040/0/1") i stedet
# for et eget "seksjon N"-suffiks (se _SEKSJON_SUFFIX under) - uten det andre
# valgfrie leddet her ble HELE segmentet avvist (kun de tre første tallene
# matchet, det siste "/N" ble stående uforbrukt og brøt "$"-ankeret), og både
# adresse og gnr/bnr gikk tapt (se rapport). `_TOKEN_RE` under plukker uansett
# kun ut de to første tallene som selve gnr/bnr-paret - feste-/seksjonsleddet
# tas ikke med i "gnr_bnr", samme forenkling som ellers i denne modulen.
_GNRBNR_TOKEN = r"\d+/\d+(?:/\d+){0,2}"
_TOKEN_RE = re.compile(r"(\d+)/(\d+)(?:/(\d+))?")

# Flere gnr/bnr-tall i samme segment skilles NESTEN alltid med ","/"og"
# ("167/163, 167/171"), men forekommer i ekte data også med KUN mellomrom
# imellom - typisk glemt komma ("167/163  167/171", "182/16 182/11") - eller
# med "&" ("2/1729 & 2/1617") - uten disse alternativene ble hele segmentet
# avvist som "ikke en gnr/bnr-liste" fordi teksten mellom tokenene ikke
# matchet noen av de kjente skilletegnene, og BÅDE adresse og gnr/bnr gikk
# tapt for hele tittelen (se rapport).
_GNRBNR_SEP = r"(?:\s*(?:,|og|&)\s*|\s+)"
# "seksjon N[, M][ og K]" (evt. forkortet "snr. N") kan følge rett etter
# gnr/bnr-tallet ("1/2080 seksjon 28", "2/2032 snr. 32") - uten denne
# ble HELE segmentet avvist siden halen ikke matchet noen kjent etterfølger,
# med samme konsekvens som over (se rapport).
_SEKSJON_SUFFIX = rf"(?:\s*(?:seksjon(?:snr)?|snr)\.?\s*\d+(?:{_GNRBNR_SEP}\d+)*)?"
# En senere gnr/bnr-referanse i lista kan mangle sin egen skråstrek helt
# ("2/1593 og 281595", der "281595" trolig er en tapt skråstrek et sted i
# et 6-sifret tall) - i stedet for å GJETTE hvor skråstreken skal inn (flere
# like plausible delinger finnes: "28/1595", "2/81595", "281/595" ...),
# godtas et slikt bart tall som en IGNORERT fortsettelse av lista - selve
# uttrekket (_TOKEN_RE lenger ned) plukker uansett kun ut ekte "tall/tall"-
# par, så det bare-tallet bidrar aldri til selve gnr_bnr-resultatet (se
# rapport). Kun tillatt som FORTSETTELSE (etter minst ett ekte par), ikke
# som selve det første leddet - ellers ville et helt urelatert tall (f.eks.
# et årstall) kunne trigge et falskt gnr/bnr-funn.
_GNRBNR_LIST_FULL_RE = re.compile(
    rf"^\s*(?:g(?:nr\s*/?\s*)?bnr\s*[:.]?\s*)?"
    rf"{_GNRBNR_TOKEN}(?:{_GNRBNR_SEP}(?:{_GNRBNR_TOKEN}|\d+))*"
    rf"{_SEKSJON_SUFFIX}"
    # "med flere" (fullt utskrevet) sidestilles med den forkortede "m.fl."-
    # varianten - uten dette ble hele segmentet avvist ("104/237 med flere",
    # se rapport).
    rf"\s*(?:m\.?\s*/?\s*fl\.?|med\s+flere)?"
    rf"\s*(?:\(\s*tidl\.?\s*{_GNRBNR_TOKEN}\s*\))?"
    # En ensom, avsluttende skråstrek ("151/46/") er en kjent skrivefeil i
    # kildedata (tredje matrikkelledd glemt/kuttet) - godtas og ignoreres
    # her i stedet for å forkaste hele segmentet (se rapport).
    rf"\s*/?\s*$",
    re.IGNORECASE,
)
_PAREN_RE = re.compile(r"\(.*?\)")
# Ekte bindestrek-skilletegn (mellomrom på minst én side), ikke et bart
# husnummer-spenn uten mellomrom ("40-44").
_SPLIT_RE = re.compile(r"(?:(?<=\s)-\s*|-(?=\s))")


def _er_gnrbnr_segment(seg):
    """Sjekker om et helt segment utelukkende er en gnr/bnr-liste."""
    if _GNRBNR_LIST_FULL_RE.match(seg):
        return True
    stripped = _PAREN_RE.sub("", seg).strip()
    return bool(stripped) and bool(_GNRBNR_LIST_FULL_RE.match(stripped))


# Kombinert liste-tokenizer for "flere adresser i samme tittel". Dekker tre
# beslektede mønstre i ÉN mekanisme: (1) "Gate N1, N2 og N3" - hvert
# listeledd har sitt EGET tall ("Horsens gate 3, 5 og 7"); (2)
# "Gate NUM BOKSTAV1 og BOKSTAV2" - alle bokstavene arver det SISTE tallet
# som ble sett, enten basen er skrevet med mellomrom ("Sauåsen 47 A og B")
# eller allerede sammenslått ("Abels gate 36A, B og C"); (3) FLERE
# tall-grupper på samme gate ("Voldskogen 1A, 1B og 1C og 2A, 2B og 2C") og
# eventuelt et HELT NYTT gatenavn eksplisitt gjentatt midt i lista
# ("Bråtengata 70A og B og 72 A og B , Syrinveien 5A og B og Syrinveien 7 A
# og B" - to ulike gater i én tittel, se rapport). Et bart ett-bokstavs
# listeledd ("og B") tolkes ALLTID som en fortsettelse av forrige tall/gate;
# et NYTT gatenavn krever minst to bokstaver (stor forbokstav + minst én
# liten) for å skille det fra et slikt suffiks - uten dette skillet ville
# f.eks. "B" i "... og B" blitt lest som et (tomt/ugyldig) gatenavn i stedet
# for en bokstav-fortsettelse. Et negativt lookahead "(?![A-Za-zæøåÆØÅ])"
# rett etter HVER bokstav-fangst (både bokstav1 og bokstav2) sikrer at kun
# et EKTE, FRITTSTÅENDE enkeltbokstav-suffiks fanges - uten dette ble "o" i
# skilleordet "og" (f.eks. i "5 og 6") lest som et falskt bokstavsuffiks på
# forrige tall, siden bokstavklassen isolert sett også matcher "o" (se
# rapport: dette brøt opp splitting av selv enkle rene tall-lister).
_LEDENDE_GATENAVN_RE = re.compile(r"^([A-ZÆØÅ][A-Za-zæøåÆØÅ.\- ]*?)\s+(?=\d)")
_NYTT_GATENAVN = r"[A-ZÆØÅ][a-zæøå][A-Za-zæøåÆØÅ.]*(?:\s+[A-Za-zæøåÆØÅ.]+)*"
_ADRESSE_LEDD_RE = re.compile(
    r"(?:^|\s*(?:,|og(?:\s*/\s*eller)?|&)\s*)"
    rf"(?:(?P<nygate>{_NYTT_GATENAVN})\s+(?=\d))?"
    r"(?:(?P<tall>\d+)(?:\s?(?P<bokstav1>[A-Za-zæøåÆØÅ])(?![A-Za-zæøåÆØÅ]))?"
    r"|(?P<bokstav2>[A-Za-zæøåÆØÅ])(?![A-Za-zæøåÆØÅ]))"
)
# Offisiell adresseform har ikke mellomrom mellom husnummer og bokstavsuffiks
# ("26 A" -> "26A") - anvendes til slutt uansett hvilken vei adressen ble
# bygget (se rapport: "Elgveien 26 A" sto igjen med mellomrommet).
_NUM_LETTER_SPACE_RE = re.compile(r"(\d)\s+([A-Za-zæøåÆØÅ])\b")


def _utvid_gatenavn_liste(gate, hale):
    """Tokeniserer halen etter et (første) gatenavn til enkelt-adresser, se
    _ADRESSE_LEDD_RE for mønstrene som dekkes. Stopper ved første "hull" i
    teksten som ikke matcher noe listeledd. Resten av halen fra der (typisk
    en beskrivende hale som "felt B1 og B2" etter en ellers gyldig tall-
    liste, "Rosenvinges vei 12, 14 og 16  felt B1 og B2", se rapport) godtas
    som en KASTBAR beskrivelse og ignoreres stille NÅR den starter med et
    småord (liten bokstav) - ikke fordi vi later som den ikke finnes, men
    fordi den strukturelt ikke kan være en fortsettelse av selve
    adresselista (jf. _strip_ledende_smaa_ord). Alt annet uforklart i halen
    (f.eks. et bart tall-spenn som "-44" i "40-44", eller et nytt gatenavn
    vi av en eller annen grunn ikke klarte å plukke opp) fører fortsatt til
    at HELE forsøket forkastes (ingen gjetting) - kalleren beholder da
    originalteksten uendret. Krever minst to utledede adresser - en enkelt,
    uendret adresse er ikke en "liste"."""
    resultat = []
    current_gate = gate
    current_num = None
    pos = 0
    for m in _ADRESSE_LEDD_RE.finditer(hale):
        if m.start() != pos:
            break
        pos = m.end()
        if m.group("nygate"):
            current_gate = m.group("nygate").strip()
        if m.group("tall"):
            current_num = m.group("tall")
            resultat.append(f"{current_gate} {current_num}{m.group('bokstav1') or ''}")
        else:
            if current_num is None:
                return None
            resultat.append(f"{current_gate} {current_num}{m.group('bokstav2')}")
    hale_rest = hale[pos:].strip()
    if hale_rest and not _STARTS_LOWER_ORD_RE.match(hale_rest):
        return None
    if len(resultat) < 2:
        return None
    return "; ".join(resultat)


def _utvid_flere_adresser(adresse):
    if not adresse:
        return adresse
    resultat = adresse
    m = _LEDENDE_GATENAVN_RE.match(adresse)
    if m:
        gate = m.group(1).strip()
        hale = adresse[m.end():]
        utvidet = _utvid_gatenavn_liste(gate, hale)
        if utvidet:
            resultat = utvidet
    return _NUM_LETTER_SPACE_RE.sub(r"\1\2", resultat)


# Et gnr/bnr-token kan mangle sitt vanlige bindestrek-skille mot adressen
# foran ("Kureveien 62 120/56" i stedet for "Kureveien 62 - 120/56") - løses
# ved å lete etter et token MIDT I et segment i stedet for å kreve at et
# HELT segment er token-lista (se _finn_gnrbnr_innbakt).
_EMBEDDED_TOKEN_RE = re.compile(rf"({_GNRBNR_TOKEN}(?:{_GNRBNR_SEP}{_GNRBNR_TOKEN})*)")


def _trekk_ut_innbakt_gnrbnr(segs, gnr_bnr_liste):
    """Brukes av hovedløkken FØR et sett adresse-kandidat-segmenter sendes
    til _ferdigstill_adresse. Et gnr/bnr-token kan mangle sitt eget
    bindestrek-skille fra adresseteksten FORAN når flere adresse+gnr/bnr-par
    er skrevet tett i samme tittel ("Karlstadveien 40-44 1/1512 -
    Strandpromenaden 81-87 - 1/1798 - ..." - "1/1512" hører til
    Karlstadveien-blokken, men mangler bindestreken som skulle skilt den fra
    selve adresseteksten, se rapport). Tokenet trekkes ut og legges til den
    delte gnr_bnr_liste-lista; kun adresseteksten FORAN tokenet beholdes i
    segmentet. Uten dette ble tokenet stående igjen som en del av selve
    adresseteksten ("Karlstadveien 40-44 1/1512"), og _ferdigstill_adresse
    fikk aldri sjansen til å gjenkjenne "Karlstadveien 40-44" som en
    selvstendig, gyldig adresse atskilt fra "Strandpromenaden 81-87".

    Tokenet trekkes KUN ut når teksten FORAN det er en SELVSTENDIG, gyldig
    adresse i seg selv (sjekket via _ferdigstill_adresse) - ikke når
    "tokenet" faktisk ER selve husnummeret, skrevet med skråstrek
    ("Peer Gynts vei 73/83", "Ryggeveien 78/80 og Marcus Thranes vei 22" -
    her hører "73/83"/"78/80" TIL adressen; "Peer Gynts vei"/"Ryggeveien"
    alene, uten dem, er ikke en gyldig adresse). Uten denne sjekken ble ekte
    skråstrek-husnummer feilaktig lest som et gnr/bnr-tall, og selve
    husnummeret gikk tapt fra adressen (se rapport)."""
    nye_segs = []
    for seg in segs:
        clean = _PAREN_RE.sub("", seg)
        m = _EMBEDDED_TOKEN_RE.search(clean)
        if m and m.start() > 0:
            prefiks = clean[: m.start()].strip()
            if prefiks and _ferdigstill_adresse(prefiks):
                for tm in _TOKEN_RE.finditer(m.group(1)):
                    par = f"{tm.group(1)}/{tm.group(2)}"
                    if par not in gnr_bnr_liste:
                        gnr_bnr_liste.append(par)
                nye_segs.append(prefiks)
                continue
        nye_segs.append(seg)
    return nye_segs


def _finn_gnrbnr_innbakt(segs):
    """Løsest fallback: gnr/bnr-token et sted MIDT I et segment, uten eget
    bindestrek-skille mot adressen foran i det hele tatt. Brukes kun når
    hoved-løkken (rene gnr/bnr-segmenter) ikke finner noe.

    Når tokenet står HELT FØRST i segmentet (m.start() == 0) er det ingen
    adressetekst FORAN det i DETTE segmentet - men adressen kan likevel stå
    i et TIDLIGERE segment ("Solgaard skog 1B - 3/2889 og overflatevann fra
    Solgard skog 2": tokenet "3/2889" står først i segment 1, mens adressen
    "Solgaard skog 1B" er hele segment 0). I så fall returneres
    `adresse_del=None` - kalleren bruker da kun de foranstående segmentene
    (`segs[:i]`) som adresse i stedet for å slå dem sammen med en (tom)
    tekst fra selve token-segmentet. Er tokenet FØRST i det aller FØRSTE
    segmentet (i == 0) finnes det ingen tidligere segmenter å hente adresse
    fra - da forkastes forsøket helt, som før."""
    for i, seg in enumerate(segs):
        kandidat = _PAREN_RE.sub("", seg)
        m = _EMBEDDED_TOKEN_RE.search(kandidat)
        if not m:
            continue
        if m.start() == 0:
            if i == 0:
                continue
            adresse_del = None
        else:
            adresse_del = kandidat[: m.start()].strip()
            if not adresse_del:
                continue
        gnr_bnr_liste = []
        for tm in _TOKEN_RE.finditer(m.group(1)):
            par = f"{tm.group(1)}/{tm.group(2)}"
            if par not in gnr_bnr_liste:
                gnr_bnr_liste.append(par)
        if not gnr_bnr_liste:
            continue
        return i, adresse_del, gnr_bnr_liste
    return None, None, None


# Et lite mindretall titler har INGEN gnr/bnr-henvisning noe sted - Moss'
# egen konvensjon ("Adresse - beskrivelse[ - beskrivelse...]") betyr at
# adressen da nesten alltid står i selve FØRSTE segmentet. Samme vakter mot
# falske positiver som Larvik/Ålesunds tilsvarende fallback (se der): ekte
# adresser starter med stor bokstav, ender på et husnummer-mønster, og er
# ikke bare et bart årstall.
_STARTS_UPPER_RE = re.compile(r"^[A-ZÆØÅ]")
# "(?<!\S)" (ikke rett etter et annet ikke-mellomrom-tegn) krever at
# tall-halen er sitt EGET ord, ikke en bokstav-forkortelse limt rett inntil
# et tall ("Galleri F15", "Rosenvinges vei KV3" - prosjekt-/felt-koder, ikke
# et gatenavn+husnummer) - uten denne sperren ble slike lest som gyldige
# adresser siden regexen bare sjekket at NOE tall stod til slutt, uansett
# hva som satt limt rett foran det (se rapport). Etter husnummeret tillates
# valgfrie gjentatte "-"/"/"-fortsettelser ("1-7", "20A-D", "2A/B/C/D",
# "152/154") siden dette er ekte, gyldige spenn/lister av husnummer - en
# tidligere, strammere variant uten dette avviste feilaktig alle slike
# spenn (funnet ved full korpus-regresjon: "Sørtunveien 1-7",
# "Bjørnåsveien 111-189" osv. ble feilaktig nullet ut).
_ENDS_WITH_HUSNR_RE = re.compile(
    r"(?<!\S)\d+[A-Za-zæøåÆØÅ]?"
    r"(?:\s*[-/]\s*(?:\d+[A-Za-zæøåÆØÅ]?|[A-Za-zæøåÆØÅ]))*"
    r"\s*$"
)
_TRAILING_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")
_ALLE_TALL_RE = re.compile(r"\d+[A-Za-zæøåÆØÅ]?")
_BART_AARSTALL_RE = re.compile(r"^(?:19|20)\d{2}$")
# "felt N" (delområde/byggetrinn i en utbygging) og "kapittel N" (henvisning
# til en forskrift/lov) er tall som strukturelt ser ut som et husnummer for
# _ENDS_WITH_HUSNR_RE, men er det aldri - begge funnet ved full
# korpus-verifisering av denne fallback-veien (se rapport: "Verkslia felt 5"
# og "Forurensningsforskriften kapittel 2" ble feilaktig lest som adresser).
# Byggefelt-koder har ofte en bokstav FØR selve tallet også ("felt B3 1D",
# "felt B3 1C" - "B3" er delfeltet, "1D"/"1C" er bygget innad i det - se
# rapport), derfor et valgfritt enkeltbokstav-ledd mellom selve ordet og
# tallet. Denne sjekken ble opprinnelig kun brukt av _gjett_adresse_uten_gbnr
# (ingen-gbnr-fallback) - nå også av _valider() i _ferdigstill_adresse, slik
# at samme felt-kode blir avvist selv når et EKTE gnr/bnr FANTES i tittelen
# for øvrig (se rapport: "Verksåsen felt B3 1D - 3/3124, 3/3125, 3/3126 -
# bygg D" ble feilaktig lest som adressen "Verksåsen felt B3 1D").
_IKKE_HUSNR_ETIKETT_RE = re.compile(
    r"\b(?:felt|kapittel)\s*[A-Za-zæøåÆØÅ]?\d", re.IGNORECASE
)


def _ser_ut_som_bare_aarstall(seg):
    tall = _ALLE_TALL_RE.findall(seg)
    return len(tall) == 1 and bool(_BART_AARSTALL_RE.match(tall[0]))


def _gjett_adresse_uten_gbnr(segs):
    """Ingen gbnr funnet noe sted i tittelen - prøv likevel FØRSTE segment
    som adresse (se begrunnelse over). En avsluttende parentes ("(Partall)",
    et tilleggsnotat om hvilke husnummer i spennet som gjelder) hører ikke
    til selve adressen og fjernes FØR sjekkene under. Den endelige
    husnummer-sjekken gjøres av _ferdigstill_adresse (den kalles der på
    resultatet ETTER en evt. bokstavliste-utvidelse - se dens docstring for
    hvorfor rekkefølgen må være slik)."""
    if not segs:
        return None
    kandidat = _TRAILING_PAREN_RE.sub("", segs[0]).strip()
    if not kandidat or not _STARTS_UPPER_RE.match(kandidat):
        return None
    if _ser_ut_som_bare_aarstall(kandidat):
        return None
    if _IKKE_HUSNR_ETIKETT_RE.search(kandidat):
        return None
    return kandidat


# Kun bokstav-innledede småord ("og", "til", "arealoverføring") skal
# strippes av _strip_ledende_smaa_ord - IKKE et segment som starter med et
# TALL ("410", andre halvdel av et bindestrek-husnummerspenn som "404 - 410"
# splittet i egne segmenter av _SPLIT_RE). Uten dette skillet ble et slikt
# bart tall-segment feilaktig tolket som "ingen adresse her i det hele tatt"
# (siden det ikke har noe ORD å strippe forbi) og hele spennet mistet sin
# andre halvdel (se rapport: "Ryggeveien 404 - 410 - Blomsholmveien 1 - 3"
# ble feilaktig kuttet til "Ryggeveien 404 - Blomsholmveien 1").
_STARTS_LOWER_ORD_RE = re.compile(r"^[a-zæøå]")


def _strip_ledende_smaa_ord(tekst):
    """Fjerner innledende småord (skille-/beskrivelsesord som "og", "til",
    "arealoverføring til") foran selve gatenavnet i et segment, når
    segmentet starter med et SLIKT småord (liten bokstav). Brukes for
    titler med FLERE adresse+gnr/bnr-blokker der andre (og senere) blokks
    adressetekst har fått med seg et lite beskrivelsesord fra foregående
    blokk, siden det ikke stod noen bindestrek imellom ("... - 2/1510 -
    arealoverføring til Ryggeveien 78 - 2/1763" - "arealoverføring til"
    hører til FØRSTE blokk, ikke selve adressen i andre blokk, se rapport).
    Fortsetter å strippe ord for ord til teksten enten IKKE lenger starter
    med et småord (stor bokstav ELLER et tall - se over) eller er tom (rent
    beskrivende, ingen adresse her - returnerer da None)."""
    while tekst and _STARTS_LOWER_ORD_RE.match(tekst):
        deler = tekst.split(None, 1)
        if len(deler) < 2:
            return None
        tekst = deler[1]
    return tekst or None


def _ferdigstill_adresse(segmenter):
    """Sluttbehandling av kandidat-adressetekst, brukt av ALLE tre stedene i
    parse_adresse_moss som bygger en adresse. `segmenter` kan være en enkelt
    streng eller en LISTE av rå, '-'-delte tekstbiter (flere biter oppstår
    når hovedløkken har mer enn ett segment FØR selve gnr/bnr-blokken, se
    parse_adresse_moss). Hvert segment får først et innledende småord-strip
    (se _strip_ledende_smaa_ord) - segmenter som allerede starter med stor
    bokstav (det vanlige tilfellet) er uendret av dette.

    Fjerner alltid en avsluttende parentes ("(Partall)", "(Sterudkvartalet)",
    "(tidligere Ryggeveien 33)" - et tilleggsnotat/kallenavn som ikke hører
    til selve adressen) - uten dette ble ekte spenn-adresser som "Hanna
    Jacobsens vei 52 til 80 (Partall)" og kallenavn-adresser som "Kongens
    gate 21 (Sterudkvartalet)" feilaktig forkastet, siden husnummer-sjekken
    traff den bokstavelige ")" på slutten (se rapport).

    Tre forsøk, i rekkefølge:
    (0) Hvert segment validert FOR SEG, men KUN forsøkt FØRST når det er mer
        enn ett segment OG ALLE starter med stor bokstav (dvs. hvert segment
        SER UT som en selvstendig, fullverdig adresse i seg selv - typisk to
        ulike gater limt sammen med bindestrek i stedet for komma/"og",
        "Karlstadveien 40-44 - Strandpromenaden 81-87"). Uten denne
        forrangen ville forsøk (1) under feilaktig godtatt HELE den
        sammenslåtte strengen som ÉN adresse, siden husnummer-sjekken der
        bare ser på HALEN (siste segment) og aldri oppdager at FØRSTE
        segment i seg selv var en fullverdig, annerledes adresse (se
        rapport). Segmenter som starter med et TALL ("410", andre halvdel av
        et bindestrek-husnummerspenn splittet i eget segment) er ALDRI
        selvstendige adresser alene - de faller da utenfor "alle starter med
        stor bokstav"-kravet og forsøk (1) prøves i stedet, se der.
    (1) Hele kandidaten samlet til én streng - dekker vanlige enkeltsegment-
        adresser og fler-adresse-mønstre som strekker seg over ETT segment
        (bokstav-/tall-lister, se _utvid_flere_adresser), OG multi-adresse-
        spenn som er blitt fragmentert av bindestrek-splitteren på tvers av
        FLERE segmenter ("Ryggeveien 404 - 410 - Blomsholmveien 1 - 3").
        Utvidelse skjer FØR validering (ikke omvendt): en bokstavliste som
        "Sauåsen 47 A og B" ender IKKE på et husnummer-mønster før den er
        utvidet (halen er en bar bokstav "B", ikke et tall) - hadde vi
        validert FØR utvidelse, ville denne (og enhver lignende liste) blitt
        forkastet feilaktig.
    (2) Hvert segment validert FOR SEG (samme logikk som forsøk 0 - kun
        forsøkt når det er mer enn ett segment OG forsøk 1 feilet, dvs. som
        siste utvei for kombinasjoner forsøk 0 ikke dekket). Segmenter UTEN
        noe tall i det hele tatt er rene beskrivelses- eller kallenavn-ledd
        ("Storebaug Gjestegård" etter "Storebaug 2", "Hammerdalen" etter
        "Vålerveien 93H", "Fjelltun" etter "Evjetangen 150") og droppes
        stille; et segment MED tall som likevel ikke validerer gjør at HELE
        kandidaten forkastes (ingen gjetting - kan være en ekte, men uklar,
        adresse). To gyldige adresse-segmenter (typisk to ulike gater, hver
        med sin egen tall-liste - "Gulspurvveien 1,2,...,12 -
        Rødstrupeveien 1,3,...,23") slås sammen med "; ".

    Rene sted-/gate-/gårdsnavn UTEN husnummer i det hele tatt ("Kaptein
    Eliesons vei", "Gatu Park", "Thorbjørnsrød gård") er IKKE en postadresse
    og skal forkastes til None her, selv om gnr/bnr for saken for øvrig er
    kjent og beholdes uendret av den kallende koden - tidligere ble ALL tekst
    foran et funnet gnr/bnr-segment godtatt som "adresse" uansett, uten noen
    sjekk på at det faktisk så ut som et gatenavn+husnummer (se rapport)."""
    if segmenter is None:
        segmenter = []
    elif isinstance(segmenter, str):
        segmenter = [segmenter]
    segmenter = [
        stripped
        for s in segmenter
        if s and s.strip()
        for stripped in [_strip_ledende_smaa_ord(s.strip())]
        if stripped
    ]
    if not segmenter:
        return None

    def _valider(tekst):
        utvidet = _utvid_flere_adresser(tekst)
        m_etikett = _IKKE_HUSNR_ETIKETT_RE.search(utvidet)
        if m_etikett:
            # Ikke forkast HELE kandidaten bare fordi en felt-/kapittel-kode
            # dukker opp et sted i den - teksten FORAN koden kan likevel
            # være en ekte, selvstendig adresse limt rett på uten
            # skilletegn ("Sponvika 1 Verkslia felt B3.3-F2" -> selve
            # adressen "Sponvika 1" står FØR "Verkslia felt B3.3-F2", se
            # rapport). Trunker ved kodens start og valider den (kortere)
            # resten fra bunnen av - dekker også et evt. gjenværende, rent
            # beskrivende ord uten tall like før selve koden ("Verkslia")
            # via samme ord-for-ord-strip under.
            utvidet = utvidet[: m_etikett.start()].rstrip(" -")
            if not utvidet:
                return None
            return _valider(utvidet)
        deler = [d.strip() for d in utvidet.split(";") if d.strip()]
        if deler and all(_ENDS_WITH_HUSNR_RE.search(d) for d in deler):
            return utvidet
        # Ender kandidaten IKKE gyldig: prøv å fjerne NØYAKTIG ETT
        # avsluttende ord (fra den OPPRINNELIGE, ikke-utvidede teksten) og
        # valider PÅ NYTT FRA BUNNEN - men KUN når teksten faktisk åpner med
        # et gjenkjennelig "gatenavn+tall"-mønster (samme sjekk som
        # _LEDENDE_GATENAVN_RE bruker for å starte en utvidelse) OG det
        # fjernede ordet selv ikke inneholder et tall (et tallbærende ord
        # kunne vært en reell del av adressen selv - da gis det opp fremfor
        # å gjette hvilket tall som er det "riktige"). Dekker ett enkelt
        # beskrivende restord limt rett bak en ekte adresse uten skilletegn
        # ("Sponvika 1 Verkslia" - "Verkslia" hører til feltkode-frasen
        # over, ikke selve adressen; "Evjetangen 150, 31 m.fl" - "m.fl" er
        # en "med flere"-hale, ikke et bokstavsuffiks; "Nøkkeland terrasse
        # 61 m.fl." samme mønster, se rapport).
        #
        # Begrenset til ETT ord OG et krav om gatenavn+tall-åpning (ikke en
        # løkke som spiser seg bakover gjennom en hel setning, og ikke
        # brukt på tekst uten noen gjenkjennelig adresse-åpning i det hele
        # tatt) - uten disse to sperrene ble en helt UBESLEKTET setning som
        # tilfeldigvis ender på et tall ("Stoppordre i henhold til
        # kulturminnelovens § 3 for tiltak/gravearbeid" - et lovhenvisnings-
        # paragrafnummer, ikke et husnummer) feilaktig godtatt som adresse
        # ved å spise seg bakover forbi "for tiltak/gravearbeid" (se
        # rapport - funnet og rettet FØR patching via full korpus-
        # regresjon).
        if _LEDENDE_GATENAVN_RE.match(tekst):
            ord = tekst.split()
            if len(ord) > 1 and not re.search(r"\d", ord[-1]):
                kortere = " ".join(ord[:-1])
                kortere_utvidet = _utvid_flere_adresser(kortere)
                if not _IKKE_HUSNR_ETIKETT_RE.search(kortere_utvidet):
                    deler2 = [d.strip() for d in kortere_utvidet.split(";") if d.strip()]
                    if deler2 and all(_ENDS_WITH_HUSNR_RE.search(d) for d in deler2):
                        return kortere_utvidet
        return None

    def _valider_hvert_segment_for_seg():
        if len(segmenter) <= 1:
            return None
        godkjente = []
        for seg in segmenter:
            seg = _TRAILING_PAREN_RE.sub("", seg).strip()
            if not seg or not re.search(r"\d", seg):
                continue  # rent beskrivende/kallenavn-ledd - ingen adresse her
            resultat = _valider(seg)
            if not resultat:
                return None  # tall til stede, men ugyldig - ingen gjetting
            godkjente.append(resultat)
        return "; ".join(godkjente) if godkjente else None

    # Kun segmenter med sitt EGET tall kan i det hele tatt være en selvstendig
    # adresse alene - et segment UTEN tall ("B", "Helgedal borettslag",
    # "delområde" er allerede strippet) er enten en ren bokstav-fortsettelse
    # av FORRIGE segments tall (kun forsøk (1)/tokenizeren kan gjenkjenne
    # dette riktig, jf. "Kongens gate 27A - B - C") eller et rent
    # kallenavn-/beskrivelsesledd - begge tilfeller MÅ gjennom forsøk (1)
    # først, ellers mister vi bokstav-fortsettelsen (eller kallenavn-
    # prefikset) helt (se rapport). Minst TO segmenter med hvert sitt tall,
    # alle med stor forbokstav, kreves derfor før forsøk (0) prøves.
    tallsegmenter = [s for s in segmenter if re.search(r"\d", s)]
    if len(tallsegmenter) > 1 and all(_STARTS_UPPER_RE.match(s) for s in tallsegmenter):
        resultat = _valider_hvert_segment_for_seg()
        if resultat:
            return resultat

    hel = _TRAILING_PAREN_RE.sub("", " - ".join(segmenter)).strip()
    if hel:
        resultat = _valider(hel)
        if resultat:
            return resultat

    resultat = _valider_hvert_segment_for_seg()
    if resultat:
        return resultat

    return None


def parse_adresse_moss(tittel, subtitle=None):
    """Splitter tittelen på ekte bindestrek-skilletegn, finner rene
    gnr/bnr-segmenter, og slår sammen alt FØR hvert slikt segment (eller
    -segment-run) til en adresse. Etterfølgende rene gnr/bnr-segmenter (som
    i Batteriveien-eksempelet i modul-docstring) slås sammen i samme
    gnr/bnr-liste.

    Et fåtall titler har FLERE adresse+gnr/bnr-blokker i samme tittel, f.eks.
    "Ryggeveien - 406/410 - Blomsholmveien 1-3 - 104/277 - utbedring av
    balkonger og terrasser" (to bygg, hver med egen adresse og eget
    matrikkelnummer). Løkken under fortsetter derfor å søke etter NYE
    gnr/bnr-blokker etter hver funnet blokk, i stedet for å stoppe ved den
    første - ellers forsvinner adresse+gnr/bnr nr. 2 (og ev. senere) helt."""
    if not tittel:
        return None, None, None

    segs = [s.strip() for s in _SPLIT_RE.split(tittel.strip())]
    segs = [s for s in segs if s]
    if not segs:
        return None, None, None

    adresser = []
    gnr_bnr_liste = []
    pos = 0
    fant_blokk = False

    while pos < len(segs):
        start = None
        for i in range(pos, len(segs)):
            if _er_gnrbnr_segment(segs[i]):
                start = i
                break
        if start is None:
            break

        end = start
        for j in range(start + 1, len(segs)):
            if _er_gnrbnr_segment(segs[j]):
                end = j
            else:
                break

        if start > pos:
            kandidat_segs = _trekk_ut_innbakt_gnrbnr(segs[pos:start], gnr_bnr_liste)
            adresser.append(_ferdigstill_adresse(kandidat_segs))

        for k in range(start, end + 1):
            clean = _PAREN_RE.sub("", segs[k])
            for m in _TOKEN_RE.finditer(clean):
                par = f"{m.group(1)}/{m.group(2)}"
                if par not in gnr_bnr_liste:
                    gnr_bnr_liste.append(par)

        fant_blokk = True
        pos = end + 1

    if not fant_blokk:
        # Ingen rent gnr/bnr-segment funnet - prøv et token INNBAKT i et
        # segment (glemt bindestrek-skille, se _finn_gnrbnr_innbakt) før vi
        # gir helt opp på gnr/bnr. Adressen fra denne veien er alltid KUN
        # teksten foran selve tokenet i det segmentet - resten av tittelen
        # (evt. senere segmenter) er beskrivelse og tas ikke med, i tråd med
        # hvordan hovedløkken over allerede skiller adresse fra beskrivelse.
        i, adresse_del, innbakt_liste = _finn_gnrbnr_innbakt(segs)
        if i is not None:
            forran = segs[:i]
            segmenter = forran + ([adresse_del] if adresse_del else [])
            adresse = _ferdigstill_adresse(segmenter)
            matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{p}" for p in innbakt_liste)
            return adresse, innbakt_liste, matrikkelnr
        # Fortsatt ingenting - tittelen har trolig ingen gnr/bnr-henvisning i
        # det hele tatt. Prøv å gjette adressen fra første segment likevel
        # (se _gjett_adresse_uten_gbnr for vaktene mot falske positiver).
        return _ferdigstill_adresse(_gjett_adresse_uten_gbnr(segs)), None, None

    adresser = [a for a in adresser if a]
    adresse = "; ".join(adresser) if adresser else None
    matrikkelnr = ("; ".join(f"{KOMMUNE_NR}-{p}" for p in gnr_bnr_liste)
                   if gnr_bnr_liste else None)
    return adresse, (gnr_bnr_liste or None), matrikkelnr


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg": dict(
        sakstype_navn="Byggesak",
        datasource=None,
        sakstype="at-8d581357__87af__4e98__a72a__536246692081-BS!z5TQjS",
        adresse_parser=parse_adresse_moss,
        hent_filer=True,
        periode="2019-nå",
    ),
}


# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(session, method, path, body=None, retries=RETRIES):
    """Delt retry-/feilhåndteringslogikk for _post og _get (identisk
    backoff og feilmelding - kun HTTP-verbet skiller dem)."""
    url = f"{API_BASE}/{path}"
    last_err = None
    for attempt in range(retries):
        try:
            if method == "POST":
                r = session.post(url, json=body, headers=HEADERS, timeout=TIMEOUT)
            else:
                r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{method} {path} feilet etter {retries} forsøk") from last_err


def _post(session, path, body, retries=RETRIES):
    return _request(session, "POST", path, body, retries)


def _get(session, path, retries=RETRIES):
    return _request(session, "GET", path, retries=retries)


def get_case_list(kilde_key, session=None, max_items=None):
    """Henter identifier+saksnummer+dato for saker i én kilde, nyeste først.
    max_items=N stopper paginering tidlig - brukes kun når man ikke også
    dato-filtrerer (se run_full_dump)."""
    kilde = KILDER[kilde_key]
    session = session or requests.Session()

    def body_for(page):
        kv = [{"key": "Dato", "value": "AllCases"},
              {"key": "Sakstype", "value": kilde["sakstype"]},
              {"key": "FilterContent", "value": "CaseOnly"},
              {"key": "pageSize", "value": str(PAGE_SIZE)},
              {"key": "page", "value": str(page)}]
        if kilde.get("datasource"):
            kv.insert(0, {"key": "Datasource", "value": kilde["datasource"]})
        return {"type": OVERVIEW_TYPE_SAK, "keyValues": kv}

    first = _post(session, "overview", body_for(1))
    si = first["content"]["searchItems"]
    items = list(si["items"])
    page_count = si["pageCount"]

    if max_items and len(items) >= max_items:
        return items[:max_items]

    for page in range(2, page_count + 1):
        d = _post(session, "overview", body_for(page))
        items.extend(d["content"]["searchItems"]["items"])
        if max_items and len(items) >= max_items:
            return items[:max_items]

    return items


def fetch_dokument_filer(session, dokument_identifier):
    """Henter fillommer for ETT dokument. sak.dokumenter[].antallVedlegg er
    alltid 0 uansett faktisk antall - dette kallet må derfor alltid gjøres
    per dokument, det kan ikke stoles på for å hoppe over tomme."""
    d = _get(session, f"dokument/{dokument_identifier}?fromMeeting=false")
    model = d.get("content", {}).get("model") or {}
    filer = []
    for gruppe in model.get("vedleggGruppe") or []:
        kategori = gruppe.get("title")
        for f in gruppe.get("vedlegg") or []:
            filer.append({
                "navn": f.get("title"),
                "kategori": kategori,
                "filtype": f.get("filtype"),
                "storrelse": f.get("filstorrelseFormatted"),
                "url": BASE_URL + f["fileUrl"] if f.get("fileUrl") else None,
            })
    return filer


def _finn_metadata(blocks, key):
    for b in blocks or []:
        if b.get("type") == "Metadata" and b.get("key") == key:
            return b.get("text") or b.get("title")
    return None


def _finn_property(blocks, tittel):
    for b in blocks or []:
        if b.get("type") == "PropertyList":
            for p in b.get("b") or []:
                if p.get("type") == "Property" and p.get("title") == tittel:
                    return p.get("text")
    return None


def fetch_case(session, kilde_key, identifier, hent_filer=None):
    """Henter og parser én sak. `hent_filer=None` bruker kildens egen
    standardverdi (se KILDER over)."""
    kilde = KILDER[kilde_key]
    if hent_filer is None:
        hent_filer = kilde["hent_filer"]

    try:
        d = _get(session, f"sak/{identifier}")
        body = d["content"]["body"]
    except Exception as e:  # noqa: BLE001
        return {"document_id": identifier, "error": str(e)}

    sakstittel = body.get("title")
    subtitle = body.get("subtitle")
    saksnummer = _finn_metadata(body.get("b"), "FRIENDLY_ID")
    props = body.get("props") or {}
    adresse, gnr_bnr_liste, matrikkelnr = kilde["adresse_parser"](sakstittel, subtitle)

    dokumenter = []
    for blokk in body.get("b") or []:
        if blokk.get("type") != "DocumentList":
            continue
        for doc in blokk.get("b") or []:
            if doc.get("type") != "AsyncDocument":
                continue
            doc_id = doc.get("identifier")
            doc_blocks = doc.get("b")

            filer = []
            if hent_filer:
                try:
                    filer = fetch_dokument_filer(session, doc_id)
                except Exception:  # noqa: BLE001
                    filer = []

            dokumenter.append({
                "journalpost_id": doc_id,
                "journalnummer": _finn_property(doc_blocks, "DokumentID"),
                "tittel": doc.get("title"),
                "dato": _finn_property(doc_blocks, "Dato"),
                "dokumenttype": _finn_property(doc_blocks, "Dokumenttype"),
                "dokumenttype_kode": _finn_metadata(doc_blocks, "DOCUMENT_TYPE_CODE"),
                "avsender": _finn_property(doc_blocks, "Avsender"),
                "mottaker": _finn_property(doc_blocks, "Mottaker"),
                "filer": filer,
            })

    return {
        "document_id": identifier,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnummer": saksnummer,
        "sakstype": kilde["sakstype_navn"],
        "sakstittel": sakstittel,
        "adresse": adresse,
        "gnr_bnr": "; ".join(gnr_bnr_liste) if gnr_bnr_liste else None,
        "matrikkelnr": matrikkelnr,
        "saksbehandler": props.get("saksbehandler"),
        "status": props.get("status"),
        "dato": props.get("dato"),
        "url": f"{BASE_URL}/alle-tjenester/innsyn/postliste-og-saksinnsyn/#/details/{identifier}",
        "dokumenter": dokumenter,
    }


# --------------------------------------------------------------------------- #
# Engangs historisk dump - gjenopptakbar kjøring
# --------------------------------------------------------------------------- #
def load_done(output_file):
    if not output_file.exists():
        return {}
    try:
        data = json.loads(output_file.read_text(encoding="utf-8"))
        return {d["document_id"]: d for d in data
                if "dokumenter" in d and "error" not in d}
    except Exception:  # noqa: BLE001
        return {}


def _write_json_atomic(data, path):
    """Skriver JSON atomisk (til .tmp, så rename) - brukes av både
    save_resultater (engangsdump) og save_state (daglig endringslogg)."""
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def save_resultater(results, output_file):
    _write_json_atomic(list(results.values()), output_file)


def _parse_iso_date(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


_DATO_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")


def _parse_dato(dato_str):
    if not dato_str:
        return None
    m = _DATO_RE.match(dato_str.strip())
    if not m:
        return None
    dag, maaned, aar = m.groups()
    try:
        return datetime(int(aar), int(maaned), int(dag)).date()
    except ValueError:
        return None


def run_full_dump(kilde_key, output_file, limit=None, from_date=None, to_date=None,
                   save_every=200, hent_filer=None):
    """Full historisk dump for én kilde. limit=N henter kun de N nyeste
    (stopper paginering tidlig, se get_case_list). from_date/to_date
    (YYYY-MM-DD) filtrerer LOKALT på sakslistas "dato"-felt - krever at hele
    lista hentes først."""
    if from_date or to_date:
        items = get_case_list(kilde_key)
    else:
        items = get_case_list(kilde_key, max_items=limit)
    if from_date or to_date:
        fra = _parse_iso_date(from_date) if isinstance(from_date, str) else from_date
        til = _parse_iso_date(to_date) if isinstance(to_date, str) else to_date
        filtrert = []
        for it in items:
            dato = _parse_dato((it.get("properties") or {}).get("dato"))
            if dato is None:
                continue
            if fra and dato < fra:
                continue
            if til and dato > til:
                continue
            filtrert.append(it)
        items = filtrert
    if limit:
        items = items[:limit]
    ids = [it["identifier"] for it in items]

    results = load_done(output_file)
    todo = [i for i in ids if i not in results]
    periode = f" (fra_dato={from_date}, til_dato={to_date})" if (from_date or to_date) else ""
    print(f"{len(ids)} saker totalt" + (f" (limit={limit})" if limit else "") + periode +
          f", {len(results)} allerede hentet, {len(todo)} gjenstår")

    session = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_case, session, kilde_key, i, hent_filer): i for i in todo}
        for fut in as_completed(futures):
            res = fut.result()
            results[res["document_id"]] = res
            done += 1
            if done % save_every == 0:
                save_resultater(results, output_file)
                print(f"  {done}/{len(todo)} hentet (lagret)")

    save_resultater(results, output_file)
    errors = sum(1 for r in results.values() if "error" in r)
    print(f"Ferdig: {len(results)} saker skrevet til {output_file} ({errors} feilet)")


# --------------------------------------------------------------------------- #
# Daglig endringslogg (running_daily) - liste-så-filtrer + "seen"-dedup.
# --------------------------------------------------------------------------- #
WINDOW_DAYS = 30
SEEN_RETENTION_DAYS = None


def get_recent_case_ids(kilde_key, cutoff_date):
    items = get_case_list(kilde_key)
    recent_ids = []
    for it in items:
        dato = _parse_dato((it.get("properties") or {}).get("dato"))
        if dato is None or dato >= cutoff_date:
            recent_ids.append(it["identifier"])
    return recent_ids, len(items)


def load_state(state_file):
    if not state_file.exists():
        return {"seen_saker": {}, "seen_journalposter": {}}
    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        state = {}
    state.setdefault("seen_saker", {})
    state.setdefault("seen_journalposter", {})
    return state


def save_state(state, state_file):
    _write_json_atomic(state, state_file)


def build_state_from_dump(dump_file, today_fn=None):
    today_fn = today_fn or _i_dag_oslo
    data = json.loads(Path(dump_file).read_text(encoding="utf-8"))
    i_dag = today_fn().isoformat()
    seen_saker, seen_jp = {}, {}
    for r in data:
        if "dokumenter" not in r:
            continue
        seen_saker[r["document_id"]] = i_dag
        for jp in r["dokumenter"]:
            if jp.get("journalpost_id"):
                seen_jp[jp["journalpost_id"]] = i_dag
    return {"seen_saker": seen_saker, "seen_journalposter": seen_jp}


def prune_seen(state, today):
    if SEEN_RETENTION_DAYS is not None:
        cutoff = (today - timedelta(days=SEEN_RETENTION_DAYS)).isoformat()
        state["seen_saker"] = {k: v for k, v in state["seen_saker"].items() if v >= cutoff}
        state["seen_journalposter"] = {k: v for k, v in state["seen_journalposter"].items() if v >= cutoff}


def upload_til_azure(saker_file, jp_file, ref_date, sakstype):
    print(f"  [Azure] opplasting ikke satt opp ennå - {saker_file.name} og "
          f"{jp_file.name} ({sakstype}, {ref_date}) ligger foreløpig kun lokalt.")


def write_changelog(nye_saker, nye_journalposter, ref_date, output_dir, sakstype):
    output_dir.mkdir(exist_ok=True)
    saker_file = output_dir / f"saker_{ref_date}.json"
    jp_file = output_dir / f"journalposter_{ref_date}.json"

    saker_file.write_text(json.dumps(nye_saker, ensure_ascii=False, indent=2), encoding="utf-8")
    jp_file.write_text(json.dumps(nye_journalposter, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Skrev {len(nye_saker)} nye saker  -> {saker_file}")
    print(f"Skrev {len(nye_journalposter)} saker med nye journalposter -> {jp_file}")
    upload_til_azure(saker_file, jp_file, ref_date, sakstype)


def _i_dag_oslo():
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def run_daily(kilde_key, state_file, output_dir, window_days=WINDOW_DAYS, today_fn=_i_dag_oslo):
    """Daglig endringslogg. `state_file` må allerede finnes (bygget via
    build_state_from_dump() fra en fullført historisk dump) før første
    kjøring."""
    kilde = KILDER[kilde_key]
    ref_date = today_fn()
    ref_iso = ref_date.isoformat()
    cutoff = ref_date - timedelta(days=window_days)
    state = load_state(state_file)
    seen_saker = state["seen_saker"]
    seen_jp = state["seen_journalposter"]

    kandidat_ids, n_totalt = get_recent_case_ids(kilde_key, cutoff)
    print(f"{n_totalt} saker totalt, {len(kandidat_ids)} innenfor "
          f"{window_days}-dagersvinduet (cutoff={cutoff.isoformat()}) hentes i detalj")

    session = requests.Session()
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_case, session, kilde_key, i) for i in kandidat_ids]
        for fut in as_completed(futures):
            results.append(fut.result())

    nye_saker = []
    nye_journalposter = []
    for r in results:
        if "dokumenter" not in r:
            continue
        cid = r["document_id"]
        nye_jp = [jp for jp in r["dokumenter"]
                  if jp.get("journalpost_id") and jp["journalpost_id"] not in seen_jp]

        if cid not in seen_saker:
            nye_saker.append(r)
        elif nye_jp:
            nye_journalposter.append({
                "parent_sak": {
                    "document_id": cid,
                    "saksnummer": r.get("saksnummer"),
                    "sakstype": r.get("sakstype"),
                    "sakstittel": r.get("sakstittel"),
                    "kommune": KOMMUNE,
                    "kommune_nr": KOMMUNE_NR,
                    "adresse": r.get("adresse"),
                    "gnr_bnr": r.get("gnr_bnr"),
                    "matrikkelnr": r.get("matrikkelnr"),
                    "url": r.get("url"),
                },
                "nye_journalposter": nye_jp,
            })

        seen_saker[cid] = ref_iso
        for jp in r["dokumenter"]:
            if jp.get("journalpost_id"):
                seen_jp[jp["journalpost_id"]] = ref_iso

    feilet = sum(1 for r in results if "error" in r)
    write_changelog(nye_saker, nye_journalposter, ref_iso, output_dir, kilde["sakstype_navn"])
    prune_seen(state, ref_date)
    save_state(state, state_file)
    print(f"Ferdig ({feilet}/{len(kandidat_ids)} feilet under henting).")
