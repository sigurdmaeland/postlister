"""Sandefjord - ACOS "nye-innsyn" (samme plattform som Gjøvik/Moss/Tønsberg
i dette prosjektet, egen selvhostet instans). Filen følger samme
seksjonsoppdeling som de andre ACOS-kommunene: konstanter -> adresseparsing
-> KILDER -> API-kall -> engangsdump -> daglig endringslogg.

Sandefjord har FEM datakilder (egen "Datasource"-nøkkel i API-body) siden
Andebu og Stokke ble innlemmet 01.01.2017 - hver med sitt eget arkiv og
tittelformat:
  - bygg_ny / tilsyn_ny : Sandefjord fra 2017-nå (dagens system, MED vedlegg)
  - bygg_2001_2016      : Sandefjord 2001-2016 (forrige system, UTEN vedlegg)
  - andebu               : Andebu 2007-2016 (UTEN vedlegg)
  - stokke                : Stokke 2012-2016 (UTEN vedlegg)
Stokke 1998-2011 er utelatt (bruker vurderte datagrunnlaget for tynt).
Uten "Datasource" i body ignoreres Sakstype-filteret stille - MÅ alltid med.

Kun bygg_ny/tilsyn_ny har vedlegg offentlig nedlastbare - de tre historiske
arkivene viser kun sak/journalpost-metadata.

ADRESSE/GNR-BNR-PARSING - fire ulike tittelformat, ett per datakilde:
  a) bygg_ny/tilsyn_ny: "Gbnr GNR/BNR[...] - Adresse - beskrivelse" - gnr/bnr
     alltid først. Støtter "og"-forkortet flerpunkts-notasjon ("114/82 og
     99") og placeholder "GBNR. /" (ingen eiendom -> adresse=None).
  b) bygg_2001_2016: fritekst, lite konsekvent. Tittelen er "GNR GNR/BNR -
     BESKRIVELSE -"; et eget subtitle-felt fra /sak/-responsen inneholder
     OFTE adressen, men ikke alltid (heuristikk: bruk subtitle som adresse
     kun hvis den "ser ut som" en gateadresse, ellers let i selve tittelen).
  c) andebu: "Adresse - gbnr/bgnr/gnr GNR/BNR - beskrivelse", skilletegn og
     nøkkelord (inkl. skrivefeilen "bgnr") varierer - løst med et generelt
     nøkkelord-søk hvor som helst i tittelen; adressen er teksten FØR treffet.
  d) stokke: "Adresse - beskrivelse[...] - gnr X bnr Y[, Y2][ fnr Z]" -
     utskrevet format (ikke slash), gnr/bnr til slutt. Adressen er alltid
     FØRSTE tittel-segment. To adresser skrevet "Gate N1 - N2" (mellomrom på
     begge sider av bindestrek) expanderes til "Gate N1; Gate N2".

Alle fire adresseparserne har signaturen (tittel, subtitle=None) ->
(adresse, gnr_bnr_liste, matrikkelnr), kalt uniformt via KILDER.

GNR-OFFSET VED SAMMENSLÅING: andebu/stokke sine titler bruker de GAMLE,
egne kommunenes gnr-numrering (fra før 01.01.2017). Et fast, dokumentert
tillegg (Andebu +200, Stokke +400 - se slektogdata.no sin oversikt over
gårdsnummer i Vestfold) er lagt til sitt gnr ved sammenslåingen inn i
Sandefjord (Sandefjord sitt eget gnr ble uendret, +0) - bnr endres aldri.
Uten dette tillegget ville matrikkelnr for disse to arkivene pekt på FEIL
eiendom under dagens KOMMUNE_NR. Se _med_gnr_offset og
parse_adresse_andebu/parse_adresse_stokke.
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
BASE_URL = "https://www.sandefjord.kommune.no"
API_BASE = f"{BASE_URL}/api/presentation/v2/nye-innsyn"

PORTAL_ID = "1"
MENYPUNKT_ID = "6093"

OVERVIEW_TYPE_SAK = 0
PAGE_SIZE = 100

KOMMUNE_NR = 3907   # gjeldende nummer (var 3804 2020-2023, 0710 før - se
                    # prosjektkonvensjon: historiske arkiv bruker også dette)
KOMMUNE = "Sandefjord"

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
# a) bygg_ny/tilsyn_ny: "Gbnr GNR/BNR[...] - Adresse - beskrivelse"
# --------------------------------------------------------------------------- #
_GBNR_PREFIX_RE = re.compile(r"^\s*g\s*bnr\.?\s*", re.IGNORECASE)
_FULL_TOKEN_RE = re.compile(r"^(\d+)/(\d+)(?:/(\d+)(?:/(\d+))?)?$")
_BARE_NUM_RE = re.compile(r"^\d+$")
_MFL_SUFFIX_RE = re.compile(r"\s*m\.?\s*/?\s*fl\.?\s*$", re.IGNORECASE)
_SUBSPLIT_RE = re.compile(r"\s*(?:,|og)\s*", re.IGNORECASE)
_GNRBNR_EXPR_RE = re.compile(
    r"^\d+/\d+(?:/\d+(?:/\d+)?)?"
    r"(?:\s*(?:,|og)\s*(?:\d+/\d+(?:/\d+(?:/\d+)?)?|\d+))*"
    r"\s*(?:m\.?\s*/?\s*fl\.?)?\s*$",
    re.IGNORECASE,
)
# Ekte bindestrek-skilletegn (mellomrom på minst én side), ikke et bart
# husnummer-spenn uten mellomrom ("40-44").
_SPLIT_RE = re.compile(r"(?:(?<=\s)-\s*|-(?=\s))")


def _split_tittel(tittel):
    segs = [s.strip() for s in _SPLIT_RE.split((tittel or "").strip())]
    return [s for s in segs if s]


def _dedup_append(liste, verdi):
    """Legger til `verdi` i `liste` hvis den ikke allerede er der - bevarer
    rekkefølge (brukes for gnr_bnr/matrikkel-samling, som ellers lett kunne
    fått duplikater ved flerpunkts-notasjon)."""
    if verdi not in liste:
        liste.append(verdi)


def _parse_gnrbnr_expr(seg):
    """'249/2 og 249/7', '114/82 og 99', '173/144/0/5' -> (gnr_bnr_liste, matrikkel_deler)."""
    seg = _MFL_SUFFIX_RE.sub("", seg).strip()
    parts = [p.strip() for p in _SUBSPLIT_RE.split(seg) if p.strip()]
    gnr_bnr, matrikkel = [], []
    last_gnr = None
    for p in parts:
        m = _FULL_TOKEN_RE.match(p)
        if m:
            gnr, bnr, feste, seksjon = m.groups()
            last_gnr = gnr
        elif _BARE_NUM_RE.match(p) and last_gnr:
            gnr, bnr, feste, seksjon = last_gnr, p, None, None
        else:
            continue
        par = f"{gnr}/{bnr}"
        _dedup_append(gnr_bnr, par)
        d = f"{gnr}/{bnr}/{feste or '0'}/{seksjon or '0'}" if (feste and feste != "0") or (seksjon and seksjon != "0") else par
        _dedup_append(matrikkel, d)
    return gnr_bnr, matrikkel


# --------------------------------------------------------------------------- #
# Adresse-validering/-normalisering, delt av parserne under.
# --------------------------------------------------------------------------- #
_ADDR_LIKE_RE = re.compile(r"[A-Za-zÆØÅæøå].*\d+\s*[A-Za-z]?\s*$")
_NUM_LETTER_SPACE_RE = re.compile(r"(\d+)\s+([A-Za-zæøåÆØÅ])\b")
_HAS_DIGIT_RE = re.compile(r"\d")


def _normaliser_nummer_bokstav(adresse):
    """Fjerner mellomrom mellom husnummer og bokstav-suffiks ("4 C" -> "4C") -
    ellers er adressen ikke søkbar i eiendomsregisteret."""
    if not adresse:
        return adresse
    return _NUM_LETTER_SPACE_RE.sub(r"\1\2", adresse)


def _gyldig_adresse(adresse):
    """En kandidat telles som reell adresse hvis den inneholder minst ett
    tall - filtrerer bort rene sted-/bygningsnavn uten husnummer."""
    return bool(adresse and _HAS_DIGIT_RE.search(adresse.strip()))


def _ferdigstill_adresse(adresse):
    if not adresse:
        return None
    adresse = adresse.strip().strip(",.;:").strip()
    adresse = _normaliser_nummer_bokstav(adresse)
    return adresse if _gyldig_adresse(adresse) else None


def _er_gnrbnr_segment(seg):
    """Skiller en ren gnr/bnr-fortsettelse (eget dash-segment) fra en adresse."""
    kandidat = _GBNR_PREFIX_RE.sub("", seg).strip()
    return bool(kandidat) and bool(_GNRBNR_EXPR_RE.match(kandidat))


def parse_adresse_ny(tittel, subtitle=None):
    """Sandefjord fra 2017-nå (Byggesak og Byggetilsynssak)."""
    segs = _split_tittel(tittel)
    if not segs:
        return None, None, None
    first = _GBNR_PREFIX_RE.sub("", segs[0]).strip()
    if not first:
        return None, None, None
    if first == "/":
        gnr_bnr, matrikkel = [], []  # placeholder "GBNR. /" - ingen eiendom
    elif _GNRBNR_EXPR_RE.match(first):
        gnr_bnr, matrikkel = _parse_gnrbnr_expr(first)
    else:
        return None, None, None
    # Ta det FØRSTE segmentet etter gnr/bnr-blokken som ser ut som en
    # husadresse (noen titler har flere stedsnavn-segmenter).
    adresse = None
    for kandidat in segs[1:]:
        if _er_gnrbnr_segment(kandidat):
            continue
        kandidat = _ferdigstill_adresse(kandidat)
        if kandidat:
            adresse = kandidat
            break
    matrikkelnr = ("; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel)
                   if matrikkel else None)
    return adresse, (gnr_bnr or None), matrikkelnr


# --------------------------------------------------------------------------- #
# b/c/d) Legacy-arkiver: gnr/bnr-nøkkelord funnet hvor som helst i tittelen
# --------------------------------------------------------------------------- #
_GNR_BNR_KEYWORDS_RE = re.compile(
    r"(?:g\s*b\s*nr|b\s*g\s*nr|gnr)\.?\s*-?\s*(\d+)"
    r"\s*[,/]?\s*-?\s*(?:bnr\.?)?\s*"
    r"(\d+(?:\s*(?:og|,)\s*\d+)*)"
    r"(?:\s*/\s*(\d+))?"
    r"(?:\s*fnr\.?\s*(\d+))?",
    re.IGNORECASE,
)
_BNR_GNR_REVERSED_RE = re.compile(r"bnr\.?\s*(\d+)\s*gnr\.?\s*-?\s*(\d+)", re.IGNORECASE)


def _med_gnr_offset(strenger, offset):
    """Legger et fast gnr-offset på gnr-delen (FØRSTE ledd) av hver
    "gnr/bnr[/feste/0]"-streng, uten å røre bnr/feste. Brukes for
    andebu/stokke, der saks-titlene bruker de GAMLE, egne kommunenes
    gnr-numrering fra før 01.01.2017-sammenslåingen med Sandefjord. Dagens
    offisielle matrikkelnr for disse eiendommene bruker et fast,
    dokumentert tillegg (Andebu +200, Stokke +400 - se f.eks.
    slektogdata.no sin oversikt over gårdsnummer i Vestfold; Sandefjord sitt
    eget gnr ble uendret, +0) - bnr endres aldri ved en sammenslåing."""
    ut = []
    for s in strenger:
        gnr, rest = s.split("/", 1)
        ny = f"{int(gnr) + offset}/{rest}"
        if ny not in ut:
            ut.append(ny)
    return ut


def _finn_gnrbnr_nokkelord(tittel, gnr_offset=0):
    """Finner ALLE gnr/bnr-nøkkelord-treff (kan være flere separate par under
    ulike gnr-numre). Returnerer (første match, kombinert gnr_bnr_liste,
    kombinert matrikkelnr).
    gnr_offset: fast tillegg på gnr-delen, brukt av andebu/stokke (se
    _med_gnr_offset)."""
    matches = list(_GNR_BNR_KEYWORDS_RE.finditer(tittel or ""))
    if matches:
        gnr_bnr, matrikkel = [], []
        for m in matches:
            gnr, bnr_list_str, feste_slash, feste_fnr = m.groups()
            feste = feste_slash or feste_fnr
            bnrs = [b.strip() for b in re.split(r"\s*(?:og|,)\s*", bnr_list_str) if b.strip()]
            for bnr in bnrs:
                par = f"{gnr}/{bnr}"
                _dedup_append(gnr_bnr, par)
                d = f"{gnr}/{bnr}/{feste}/0" if feste else par
                _dedup_append(matrikkel, d)
        if gnr_offset:
            gnr_bnr, matrikkel = _med_gnr_offset(gnr_bnr, gnr_offset), _med_gnr_offset(matrikkel, gnr_offset)
        matrikkelnr = "; ".join(f"{KOMMUNE_NR}-{d}" for d in matrikkel)
        return matches[0], gnr_bnr, matrikkelnr

    m2 = _BNR_GNR_REVERSED_RE.search(tittel or "")
    if m2:
        bnr, gnr = m2.groups()
        par = f"{int(gnr) + gnr_offset}/{bnr}"
        return m2, [par], f"{KOMMUNE_NR}-{par}"

    return None, None, None


def parse_adresse_gammel(tittel, subtitle=None):
    """Sandefjord 2001-2016 - se modul-docstring (b)."""
    m, gnr_bnr, matrikkelnr = _finn_gnrbnr_nokkelord(tittel)
    adresse = None
    if subtitle and _ADDR_LIKE_RE.match(subtitle.strip()):
        adresse = subtitle.strip()
    else:
        for seg in _split_tittel(tittel):
            if m and seg == (tittel or "")[m.start():m.end()].strip():
                continue
            candidate = re.sub(r"(?:g\s*b\s*nr|gnr)\.?\s*[\d/,\s]+", "", seg,
                                flags=re.IGNORECASE).strip()
            if candidate and _ADDR_LIKE_RE.match(candidate):
                adresse = candidate
                break
    return _normaliser_nummer_bokstav(adresse), gnr_bnr, matrikkelnr


def parse_adresse_andebu(tittel, subtitle=None):
    """Andebu 2007-2016 - adressen er teksten FØR gnr/bnr-nøkkelord-treffet,
    kun hvis den inneholder et husnummer (ellers stedsnavn/beskrivelse).
    gnr-delen får et fast +200-offset for den gamle Andebu-kommunens egen
    gnr-numrering (se _med_gnr_offset)."""
    m, gnr_bnr, matrikkelnr = _finn_gnrbnr_nokkelord(tittel, gnr_offset=200)
    if not m:
        return None, None, None
    kandidat = (tittel[:m.start()]).strip().rstrip(" -").strip() or None
    adresse = _ferdigstill_adresse(kandidat)
    return adresse, gnr_bnr, matrikkelnr


_GATE_NR_RE = re.compile(r"^(.+?)\s+(\d+[A-Za-zæøåÆØÅ]?)$")
_BARE_HUSNR_RE = re.compile(r"^\d+[A-Za-zæøåÆØÅ]?$")


def parse_adresse_stokke(tittel, subtitle=None):
    """Stokke 2012-2016 - adressen er FØRSTE tittel-segment (til forskjell
    fra Andebu), kun hvis det inneholder et husnummer. gnr-delen får et
    fast +400-offset for den gamle Stokke-kommunens egen gnr-numrering (se
    _med_gnr_offset)."""
    m, gnr_bnr, matrikkelnr = _finn_gnrbnr_nokkelord(tittel, gnr_offset=400)
    segs = _split_tittel(tittel)
    adresse = _ferdigstill_adresse(segs[0]) if segs else None
    if adresse and len(segs) > 1 and _BARE_HUSNR_RE.match(segs[1]):
        # "Vearveien 19 - 21 - ..." er to adresser på samme gate (mellomrom
        # på begge sider av bindestreken), ikke ett husnummer-spenn - skrives
        # "Gate N1; Gate N2" (samme konvensjon som matrikkelnr).
        gate_m = _GATE_NR_RE.match(adresse)
        if gate_m:
            adresse = f"{adresse}; {gate_m.group(1)} {segs[1]}"
    return adresse, gnr_bnr, matrikkelnr


# --------------------------------------------------------------------------- #
# Datakilde-register
# --------------------------------------------------------------------------- #
KILDER = {
    "bygg_ny": dict(
        sakstype_navn="Byggesak",
        datasource="9bb75e38-95d6-447c-9079-2b842a40a46a",
        sakstype="at-9bb75e38__95d6__447c__9079__2b842a40a46a-BS!uwUEuz",
        adresse_parser=parse_adresse_ny,
        hent_filer=True,
        periode="2017-nå",
    ),
    "tilsyn_ny": dict(
        sakstype_navn="Byggetilsynssak",
        datasource="9bb75e38-95d6-447c-9079-2b842a40a46a",
        sakstype="at-9bb75e38__95d6__447c__9079__2b842a40a46a-BST!OvQ1mA",
        adresse_parser=parse_adresse_ny,
        hent_filer=True,
        periode="2017-nå",
    ),
    "bygg_2001_2016": dict(
        sakstype_navn="Byggesak",
        datasource="3c3a9056-c75f-4d07-9cc0-c7d565bd93c0",
        sakstype="at-3c3a9056__c75f__4d07__9cc0__c7d565bd93c0-BYG!GSKmwA",
        adresse_parser=parse_adresse_gammel,
        hent_filer=False,
        periode="2001-2016",
    ),
    "andebu": dict(
        sakstype_navn="Byggesak",
        datasource="911b8818-5e3d-4f6b-9308-3d720e03455a",
        sakstype="at-911b8818__5e3d__4f6b__9308__3d720e03455a-BS!X9zWw8",
        adresse_parser=parse_adresse_andebu,
        hent_filer=False,
        periode="2007-2016 (Andebu)",
    ),
    "stokke": dict(
        sakstype_navn="Byggesak",
        datasource="42e415fe-10d1-4297-af49-7cbfa181afe0",
        sakstype="at-42e415fe__10d1__4297__af49__7cbfa181afe0-BS!xZTDrc",
        adresse_parser=parse_adresse_stokke,
        hent_filer=False,
        periode="2012-2016 (Stokke)",
    ),
}

# --------------------------------------------------------------------------- #
# API-kall
# --------------------------------------------------------------------------- #
def _request(method_fn, path, retries=RETRIES, **kwargs):
    """Delt retry/backoff-logikk for _post/_get."""
    url = f"{API_BASE}/{path}"
    last_err = None
    for attempt in range(retries):
        try:
            r = method_fn(url, headers=HEADERS, timeout=TIMEOUT, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{method_fn.__name__.upper()} {path} feilet etter {retries} forsøk") from last_err


def _post(session, path, body, retries=RETRIES):
    return _request(session.post, path, retries, json=body)


def _get(session, path, retries=RETRIES):
    return _request(session.get, path, retries)


def get_case_list(kilde_key, session=None, max_items=None):
    """Henter identifier+saksnummer+dato for saker i én datakilde, nyeste
    først. "Datasource" MÅ være med i body (se modul-docstring). max_items=N
    stopper paginering tidlig - brukes kun når man ikke også dato-filtrerer."""
    kilde = KILDER[kilde_key]
    session = session or requests.Session()

    def body_for(page):
        return {
            "type": OVERVIEW_TYPE_SAK,
            "keyValues": [
                {"key": "Datasource", "value": kilde["datasource"]},
                {"key": "Dato", "value": "AllCases"},
                {"key": "Sakstype", "value": kilde["sakstype"]},
                {"key": "FilterContent", "value": "CaseOnly"},
                {"key": "pageSize", "value": str(PAGE_SIZE)},
                {"key": "page", "value": str(page)},
            ],
        }

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
    """Henter fillommer for ETT dokument. Kun relevant for kilder med
    hent_filer=True (kun bygg_ny/tilsyn_ny - se modul-docstring)."""
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
        "url": f"{BASE_URL}/engasjer-deg/innsyn-og-apenhet/sok-etter-saker-og-dokumenter/#/details/{identifier}",
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


def _atomic_write_json(data, path):
    """Skriver JSON til en tmp-fil og bytter den inn atomisk - unngår
    korrupt fil ved avbrutt kjøring midt i skriving."""
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def save_resultater(results, output_file):
    _atomic_write_json(list(results.values()), output_file)


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
    """Full historisk dump for én datakilde. limit=N henter kun de N nyeste
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
# Daglig endringslogg (running_daily) - liste-så-filtrer + "seen"-dedup
# (KUN for bygg_ny/tilsyn_ny, de tre historiske arkivene er lukket og får
# aldri nye saker).
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
    _atomic_write_json(state, state_file)


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
