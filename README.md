# sigurd-postlister

Skraper offentlige postlister/saksinnsyn (byggesaker, tilsyn, ulovlighetssaker,
henvendelser m.m.) fra norske kommuners nettbaserte innsynsløsninger, og
lagrer resultatet som JSON i `bronze/`-laget for videre bearbeiding. Ingen
silver-transform finnes ennå - dette repoet dekker kun innhenting/parsing.

For løpende status - dekningstall per kommune, kjente feil og fikser, hvilke
dumper som eventuelt må kjøres på nytt lokalt - se **[HANDOVER.md](HANDOVER.md)**,
som er den aktivt vedlikeholdte statusloggen for prosjektet. Denne fila er en
stabil oversikt over strukturen; HANDOVER.md er der de ferske detaljene bor.

## Mappestruktur

Kommunene er gruppert etter hvilken innsynsplattform/leverandør de bruker:

```
bronze/postlister/
  requirements.txt
  [plattform]/
    [kommune]/
      common/scraper_lib.py                    -- all skrape- og parselogikk
      [periode]_dump/[sakstype]/main.py         -- engangs historisk dump
      running_daily/[sakstype]/app/main.py      -- daglig endringslogg
scripts/
  dump_stats.py                                 -- dekningsstatistikk på tvers av alle dumper
```

Hver kommune er selvstendig (ingen delt kode på tvers av kommuner), men
følger samme interne struktur i `common/scraper_lib.py`: modul-docstring ->
konstanter -> adresse-/gnr-bnr-parsing -> `KILDER`-dict (der en kommune har
flere sakstyper) -> API-/HTML-kall -> engangs historisk dump -> daglig
endringslogg.

## Plattformer og kommuner

| Mappe | Plattform | Kommuner |
|---|---|---|
| `acos/` | ACOS "nye innsyn" (selvhostet, og Innsynpluss-fellesvert for Larvik) | Gjøvik, Larvik, Moss, Sandefjord, Tønsberg, Ålesund (dekker også de tidligere kommunene Ørskog, Sandøy og Skodje, slått sammen inn i Ålesund 01.01.2020 - skrapes fortsatt som egne historiske arkiv side om side med dagens Ålesund) |
| `opengov/` | OpenGov / 360online (HTML-scraping) | Kristiansand, Lillestrøm, Sandnes, Sarpsborg |
| `innsynsportal/` | 360 Plan & Build (GraphQL) | Asker, Drammen, Tromsø, Trondheim |
| `einnsyn/` | eInnsyn.no (nasjonal plattform) | Bodø, Fredrikstad, Oslo |

Se HANDOVER.md for hvilke sakstyper (bygg/tilsyn/ulov/henv) hver enkelt
kommune faktisk har, og gjeldende dekningstall.

## Oppsett

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r bronze/postlister/requirements.txt
```

## Kjøre en scraper

**Engangs historisk dump** (fyller opp all historikk, gjenopptakbar - hopper
over saker som allerede ligger i output-fila):

```sh
cd bronze/postlister/[plattform]/[kommune]/[periode]_dump/[sakstype]
python3 main.py                        # hele arkivet
python3 main.py 40                     # smoketest: kun de 40 nyeste sakene
python3 main.py 2020-01-01 2020-01-31  # kun saker i et datointervall (der støttet)
```

**Daglig endringslogg** - de fleste kommuner kjøres direkte som script:

```sh
cd bronze/postlister/[plattform]/[kommune]/running_daily/[sakstype]/app
python3 main.py
```

**Unntak - wheel-pakket for Databricks** (Gjøvik, Kristiansand, Lillestrøm,
Sandnes): `app/main.py` her bruker relativ import (`from .lib.scraper_lib
import ...`) og kan derfor ikke kjøres direkte med `python3 main.py`. Kjør i
stedet som pakke, fra mappen med `setup.py` (ikke fra `app/`):

```sh
cd bronze/postlister/[plattform]/[kommune]/running_daily/[sakstype evt. utelatt]
python3 -c "from app.main import trigger; trigger()"       # full sveip
python3 -c "from app.main import main; main(limit=30)"     # rask test, 30 saker
```

For disse fire ligger det en FYSISK kopi av `common/scraper_lib.py` i
`running_daily/[sakstype]/app/lib/scraper_lib.py` (samme mønster som
easy-pipelines-repoet). Endres kjernelogikken i `common/scraper_lib.py`, må
kopien(e) oppdateres samtidig - kopier filen på nytt til hver
`app/lib/scraper_lib.py` for kommunen.

Nøyaktige argumenter (datointervall, `limit`, år osv.) varierer noe per
kommune ut fra hvor stort/gammelt arkivet er - se kommentarene øverst i hver
`main.py`.

## Statistikk / dekning

`scripts/dump_stats.py` teller antall saker og andel med utfylt adresse,
gnr/bnr og matrikkelnr per dump-fil:

```sh
python3 scripts/dump_stats.py --scan            # finner automatisk alle dump-filer under bronze/postlister/
python3 scripts/dump_stats.py <fil1.json> ...    # eller spesifikke filer
```

Skriver også maskinlesbare tall til `scripts/dump_stats.json`. Kjør denne på
nytt etter enhver ny full dump - tallene i HANDOVER.md kan bli utdatert fort.

## Data og lokal state

Output-JSON og state-filer (`state.json`, `seen_journalpost_ids.json`,
`snapshot.json`, `run.log`, `output/*.json`, alt under `*_dump/**`) er lagt
til i `.gitignore`. De regnes som regenererbar lokal cache, ikke kildekode -
selve dataene finnes fortsatt hos kommunene/portalene og kan hentes på nytt
ved behov.

## Azure-opplasting

Rulles ut kommune for kommune, og er ikke ferdig for alle. Hver
`scraper_lib.py` har en `upload_til_azure(...)`-hook sentralisert ett sted,
klar til å kobles på når turen kommer - men `run_full_dump()` kjører
BEVISST frakoblet fra Azure for de fleste kommuner akkurat nå (kun lokal
fil), slik at lokale test-/full-kjøringer ikke ved et uhell laster opp
ufullstendige data. Se den enkelte kommunes `common/scraper_lib.py`-docstring
og HANDOVER.md for hvor langt akkurat den kommunen har kommet i
Azure-utrullingen, credential-oppsett (Service Principal/`.env` vs.
Databricks secrets vs. `az login`) og wheel-pakking for Databricks-deploy.
