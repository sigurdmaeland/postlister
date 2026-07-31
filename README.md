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

Ikke implementert ennå. Hver `scraper_lib.py` har en `upload_til_azure(...)`
-hook som foreløpig bare logger lokalt, med planlagt målsti:

```
bronze/{kommune}/{sakstype}/load_type=incremental/date=<dato>/
```
