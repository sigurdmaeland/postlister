# sigurd-postlister

Skraper offentlige postlister/saksinnsyn (i hovedsak byggesaker) fra norske
kommuners nettbaserte innsynsløsninger, og lagrer resultatet som JSON i
`bronze/`-laget for videre bearbeiding.

## Mappestruktur

Kommunene er gruppert etter hvilken innsynsplattform/leverandør de bruker:

```
bronze/postlister/
  [plattform]/
    [kommune]/
      common/scraper_lib.py              -- all skrape- og parselogikk
      [periode]_dump/[sakstype]/main.py           -- engangs historisk dump
      running_daily/[sakstype]/app/main.py        -- daglig endringslogg
```

Hver kommune er selvstendig (ingen delt kode på tvers av kommuner), men
følger samme interne struktur: modul-docstring -> konstanter ->
adresseparsing -> `KILDER`-dict (der en kommune har flere sakstyper) ->
API-kall -> engangs historisk dump -> daglig endringslogg.

## Plattformer og kommuner

| Mappe | Plattform | Kommuner |
|---|---|---|
| `acos/` | ACOS "nye innsyn" (og Innsynpluss-varianten) | Gjøvik, Moss, Sandefjord, Tønsberg, Bærum, Ålesund (+ Skodje, Ørskog, Sandøy), Larvik |
| `opengov/` | OpenGov / 360online (HTML-scraping) | Kristiansand, Lillestrøm, Sandnes, Sarpsborg |
| `innsynsportal/` | 360 Plan & Build (innsynsportal.no, GraphQL) | Tromsø, Trondheim, Asker, Drammen |
| `einnsyn/` | eInnsyn (nasjonal plattform) | Oslo, Fredrikstad, Bodø (flere kommuner planlegges lagt til her) |

## Oppsett

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r bronze/postlister/requirements.txt
```

## Kjøre en scraper

**Engangs historisk dump** (fyller opp all historikk):

```sh
cd bronze/postlister/[plattform]/[kommune]/[periode]_dump/[sakstype]
python3 main.py
```

**Daglig endringslogg** (kjøres f.eks. som cronjobb/scheduled task):

```sh
cd bronze/postlister/[plattform]/[kommune]/running_daily/[sakstype]/app
python3 main.py            # standard: siste dag
python3 main.py 3          # siste 3 dager tilbake
```

Nøyaktige argumenter (datointervall, `--limit`, år osv.) varierer noe per
kommune ut fra hvor stort/gammelt arkivet er — se kommentarene øverst i
hver `main.py`.

## Data og lokal state

Output-JSON og state-filer (`state.json`, `seen_journalpost_ids.json`,
`snapshot.json`, `run.log`, `output/*.json`) er lagt til i `.gitignore`.
De regnes som regenererbar lokal cache, ikke kildekode — selve dataene
finnes fortsatt hos kommunene/portalene og kan hentes på nytt ved behov.

## Azure-opplasting

Rulles ut kommune for kommune. De fleste `scraper_lib.py` har fortsatt bare
en `upload_til_azure(...)`-hook som logger lokalt (ikke koblet til Azure).

**Kristiansand** (`opengov/kristiansand`) er første kommune med ekte
opplasting, satt opp etter samme mønster som easy-pipelines-repoet sin
Trondheim-pipeline (samme lagringskonto/container og samme Service
Principal-hemmeligheter):

- Konto/container: `storaggen2eaccountprod.blob.core.windows.net` / `postlister`
- Daglig endringslogg: `bronze/kristiansand/load_type=incremental/date=<dato>/<sakstype>_saker-<dato>.jsonl.gz`
  (+ `<sakstype>_journalposter-<dato>.jsonl.gz` for nye journalposter på gamle saker)
- Engangs historisk dump (`run_full_dump`, kun ved en reell full kjøring - ikke
  ved lokale `limit`-testkjøringer): `bronze/kristiansand/load_type=full/<sakstype>/dump_date=<dato>/<sakstype>_saker-full-<dato>.jsonl.gz`,
  én blob for hele sakstypen. Partisjonert på kjøredato, ikke sakens egen dato
  (Kristiansands "q="-søk gir alle saker i ett kall uten datofelt å
  partisjonere på per sak) - avviker derfor litt fra easy-pipelines/Trondheim
  sin `load_type=full/<type>/month=<YYYY-MM>/...`.
- Format: JSONL (én post per linje) + gzip
- Auth, i denne rekkefølgen:
  1. Databricks secrets (scope `postlister`), hvis kjørt der.
  2. Service Principal (`SP-PIPELINE-POSTLISTER-CLIENT-ID`/`-TENANT-ID`/
     `-CLIENT-SECRET`) fra en lokal, gitignored `bronze/postlister/.env`
     (kopier `bronze/postlister/.env.example`). Dette er ÉN Service
     Principal for hele `postlister`-containeren - **ikke**
     kommune-spesifikke credentials - så fila ligger delt på
     `bronze/postlister/`-nivå (samme sted som `requirements.txt`), ikke
     kopiert inn i hver kommunes `common/`-mappe.
  3. Egen brukerkonto via `az login` (Azure CLI), hvis ingen Service
     Principal er satt opp - krever at Azure CLI er installert, at
     `az login` er kjørt én gang, og at brukeren har rollen "Storage Blob
     Data Contributor" på storage-kontoen/containeren (Azure Portal ->
     kontoen -> "Access Control (IAM)"). Se kommentarene i
     `.env.example` for detaljer.

  Uten noen av disse hopper opplastingen bare over seg selv med en
  loggmelding - resten av kjøringen (lokal fil, state) påvirkes ikke.

### running_daily sitt state (snapshot) - Azure er kilden til sannhet

`running_daily` for Kristiansand er bygget på full-sveip + diff mot
gårsdagens snapshot (se modul-docstring i `scraper_lib.py` for hvorfor -
kort sagt: OpenGov-søket her har ikke noe pålitelig datofelt å filtrere på,
så vi må sveipe alt hver dag og sammenligne mot forrige kjøring for å finne
nye saker OG nye journalposter på gamle saker).

Siden jobben etter planen skal kjøre i Azure (ikke som en manuell
scriptkjøring på en enkelt PC), kan ikke gårsdagens snapshot bare ligge på
lokal disk - et annet/nytt kompute-miljø som plukker opp morgendagens
kjøring ville da ikke funnet den. `run_daily()` leser og skriver derfor
snapshotet til Azure Blob (`bronze/kristiansand/_state/<sakstype>_snapshot.json`,
plain JSON, overskrives hver kjøring) som PRIMÆR kilde - lokal
`running_daily/<type>/app/snapshot.json` er kun en best-effort fallback/cache
hvis Azure ikke er tilgjengelig. De lokale snapshot-filene som allerede
ligger i repoet er derfor uten betydning for produksjonskjøring.

De andre kommunene får samme oppsett etter hvert - `upload_til_azure(...)`
er allerede sentralisert i hver `scraper_lib.py` som ETT sted å koble det
på når turen kommer, og de vil lese fra samme delte `bronze/postlister/.env`.

### Databricks Job for running_daily - wheel-pakket (samme konvensjon som easy-pipelines)

Kristiansand sine 4 `running_daily/<sakstype>/`-mapper (bygg/henv/tilsyn/ulov)
er nå pakket som selvstendige Python-wheels, etter EKSAKT samme mønster som
Trondheim/Bergen/Bærum/Stavanger/Statsforvaltere/Oslo bruker i
`easy-pipelines`-repoet:

```
running_daily/<sakstype>/
  setup.py             -- pakkenavn postlister-kristiansand-<type>-bronze,
                           entry point kristiansand-<type>-trigger=app.main:trigger
  pyproject.toml        -- samme metadata, PEP 621-variant
  app/
    __init__.py
    main.py             -- SAKSNUMMER_PREFIKS/SAKSTYPE + trigger()/main(),
                           importerer fra .lib.scraper_lib (relativ import -
                           se testkjøring under)
    lib/
      __init__.py
      scraper_lib.py     -- FYSISK KOPI av ../../../common/scraper_lib.py
```

`app/lib/scraper_lib.py` er en bevisst duplisering (matcher hvordan
easy-pipelines gjør det for Trondheim m.fl.) - når kjernelogikken i
`common/scraper_lib.py` endres, må alle 4 kopiene oppdateres samtidig:

```sh
cd bronze/postlister/opengov/kristiansand
for t in bygg henv tilsyn ulov; do
  cp common/scraper_lib.py running_daily/$t/app/lib/scraper_lib.py
done
```

**Lokal testkjøring** endrer seg med wheel-pakkingen: siden `app/main.py` nå
bruker en relativ import (`from .lib.scraper_lib import run_daily`), må den
kjøres som en pakke - ikke direkte som et script (`python3 main.py` vil gi
`ImportError: attempted relative import`). Kjør i stedet fra pakkeroten
(mappen med `setup.py`, ikke `app/`):

```sh
cd bronze/postlister/opengov/kristiansand/running_daily/bygg
python3 -c "from app.main import trigger; trigger()"       # full sweep
python3 -c "from app.main import main; main(limit=30)"     # rask test, 30 saker
```

**Bygge wheelen lokalt** (samme som CI gjør):

```sh
cd bronze/postlister/opengov/kristiansand/running_daily/bygg
python3 -m build --wheel   # krever `pip install build`
ls dist/*.whl
```

**CI**: `.github/workflows/build_upload_wheel.yml` er en EGEN lokal kopi av
easy-pipelines sin reusable workflow (valgt fremfor cross-repo `uses:` for å
unngå en ekstra cross-repo Actions-tillatelse fra Mathias/Adrian) - bygger
wheelen og laster den opp til samme Azure Blob-container
(`postlister/bronze/wheels/kristiansand-<type>/...`). 4 trigger-workflows
(`wheel_postlister_kristiansand_<type>.yml`) kaller den, én per sakstype,
trigget på push til `setup.py` eller manuelt (`workflow_dispatch`).

Krever disse GitHub-repo-secrets i `sigurd-postlister` (samme navn som
easy-pipelines bruker - spør Mathias/Adrian hvis de ikke allerede er delt på
org-nivå): `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`,
`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_CONTAINER`.

**Manuelle steg som gjenstår** (udokumentert i kode i easy-pipelines også -
kun UI-steg per konvensjonen der):

1. Etter en grønn CI-kjøring ligger wheelen i Azure Blob under
   `postlister/bronze/wheels/kristiansand-<type>/`. Denne må lastes opp til
   Databricks (Workspace-bibliotek/volume) - spør Mathias/Adrian om
   nøyaktig sti hvis usikker.
2. Opprett én Databricks Job med 4 "Python wheel"-tasks (én per sakstype),
   entry point `kristiansand-<type>-trigger`, med daglig schedule.
3. Seed Azure-snapshotene (`bronze/kristiansand/_state/<type>_snapshot.json`)
   fra den ferske full-dumpen FØR jobben kjøres første gang i produksjon, se
   seeding-instruksjonene i `common/README` / chat-historikk.
