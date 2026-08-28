# Gjøvik Bronze Layer

## Oversikt

Bronze-laget for Gjøvik kommunes postliste. Kilden er en selvhostet ACOS "nye-innsyn"-instans (`www.gjovik.kommune.no`, portal-ID `2`) - samme plattform som Moss/Sandefjord/Tønsberg i dette prosjektet, men egen instans (ingen `Datasource`-nøkkel, ingen MenypunktID-header, POST i stedet for GET mot fildokument-endepunktet). Arkivet går tilbake til minst januar 2017.

Kun én sakstype dekkes foreløpig: **Byggesak** (`KILDER`-nøkkel `"bygg"`).

## Dataflyt

**Daglig endringslogg** (`running_daily/bygg/`) bruker liste-så-filtrer med et 30-dagersvindu og "seen"-dedup: `state.json` holder `seen_saker`/`seen_journalposter` (sist sett-dato per ID), bygget første gang via `build_state_from_dump()` fra den historiske dumpen. Hver kjøring henter kun saker med sist-endret-dato innenfor vinduet, og differ mot state for å finne `nye_saker` (ukjente `document_id`) og `nye_journalposter` (kjente saker med journalpost-ID-er vi ikke hadde).

Adresse-parsing (`parse_adresse_gjovik()`) trekker gate/husnummer ut av sakstittelen, som nesten alltid starter med matrikkelnummeret (`GNR/BNR[/FESTENR/SEKSJONSNR]`). Se docstringen øverst i `common/scraper_lib.py` for detaljer og kjente unntak.

**Ingen lokal JSON av endringsloggen**: `write_changelog()` skriver aldri saker/journalposter til disk i repoet. Landingssone velges automatisk (`_har_databricks_volume()`): en Unity Catalog Volume hvis koden kjører inne i Databricks og `/Volumes` finnes, ellers Azure Blob (`upload_til_azure()`). Ved en mislykket skriving havner en fallback-kopi i systemets midlertidige mappe (`_fallback_lagre_midlertidig`, utenfor repoet, aldri en permanent lokal fil), og state.json avanseres ikke (se sikkerhetsmekanismen under). `state_file` (`state.json`, dedup-indeksen) er det eneste som fortsatt ligger lokalt - se Kjente gap.

## Bronze-struktur

```
load_type=incremental/date=YYYY-MM-DD/
├── saker-YYYY-MM-DD.jsonl.gz          # nye saker
└── journalposter-YYYY-MM-DD.jsonl.gz  # nye journalposter på gamle saker
```

Format: gzippet JSONL, UTF-8. To mulige landingssoner, samme struktur i begge:

- **Unity Catalog Volume** (default inne i Databricks): `/Volumes/postlister/bronze/raw/gjovik/...`
- **Azure Blob** (lokalt, eller Databricks uten Volume satt opp): `bronze/gjovik/...` i samme konto/container som resten av prosjektet (`storaggen2eaccountprod`/`postlister`), eller din egen konto - se `DATABRICKS_SETUP.md`.

## Komponenter

- **`common/scraper_lib.py`** - kanonisk fil, all skrape- og parselogikk.
- **`2017-today_dump/bygg/main.py`** - engangs historisk dump, kjøres lokalt, importerer `common/` direkte. Skriver kun lokalt (`gjovik.json`) - laster foreløpig ikke opp til Azure (se Kjente gap).
- **`running_daily/bygg/`** - wheel (`postlister-gjovik-bygg`) med ett entry point (`gjovik-bygg-trigger`), som bruker en fysisk kopi av `scraper_lib.py` (`app/lib/scraper_lib.py`) - wheelen kjører fra installert pakke uten tilgang til resten av repoet, derfor kopi og ikke delt import.

## Sikkerhetsmekanisme i `run_daily()`

`state.json` avanseres **kun** hvis dagens endringslogg faktisk kom seg til bronze-laget (`write_changelog()` returnerer suksess-status fra enten Volume- eller Azure-skriving). Ved feil forblir state uendret, slik at samme saker/journalposter oppdages og forsøkes på nytt neste kjøring i stedet for å gå tapt stille. Samme mønster som Sandnes-oppsettet.

## Status

Både Volume- og Azure-opplastingen er nybygget denne runden - **ikke testet mot ekte Databricks/Azure ennå**. Wheelen bygger og importerer korrekt lokalt (verifisert), men er ikke kjørt i Databricks. Se `DATABRICKS_SETUP.md` for oppsett i eget workspace (fase 1: kun Databricks/Volume, fase 2: Azure Blob).

## Kjente, bevisst aksepterte gap

- `state.json` ligger kun lokalt i wheel-pakkens mappe, ikke speilet til Volume/Azure - på ephemeral Databricks-compute overlever den IKKE mellom jobbkjøringer med mindre den plasseres på et persistent volum/mount. Sandnes' tilsvarende snapshot ligger primært i Azure (`_state/<slug>_snapshot.json`) nettopp av denne grunnen; Gjøvik har ikke fått samme behandling ennå.
- `2017-today_dump/bygg/main.py` (historisk backfill) laster ikke opp til Volume eller Azure - kun lokal fil. Bærums tilsvarende dump-script laster opp til Azure (`bronze/baerum/load_type=full/...`).
- Ingen retry/backoff utover det som evt. finnes i selve API-kallene i `scraper_lib.py` - ingen eksponentiell backoff eller 429/5xx-spesifikk håndtering i `run_daily()`-løpet.
- Ingen deteksjon av redigerte/fjernede dokumenter (kun nye journalpost-ID-er fanges opp).

## Bygg og deploy

```bash
cd bronze/postlister/acos/gjovik/running_daily/bygg
rm -rf dist build *.egg-info
python -m build --wheel   # -> dist/postlister_gjovik_bygg-0.1.0-py3-none-any.whl
```

Ikke commit wheelen til git. Last opp `.whl` til Databricks og opprett én jobb (Python wheel-task, entry point `gjovik-bygg-trigger`). Historisk backfill kjøres én gang lokalt: `python 2017-today_dump/bygg/main.py`.

**Lokal test uten å røre Azure**: `run_daily()` har ingen innebygd `limit`/testkjørings-modus per nå (til forskjell fra Sandnes) - kjør heller mot en kopi av `state.json` for å unngå å avansere produksjonsstate ved lokal utprøving.

## Overvåking

`write_changelog()` printer antall nye saker/journalposter, `[Volume]`- eller `[Azure]`-linjer viser skrivestatus avhengig av landingssone, og `[State] IKKE avansert` betyr dagens skriving feilet.
