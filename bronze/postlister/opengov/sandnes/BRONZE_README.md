# Sandnes Bronze Layer

## Oversikt

Bronze-laget for Sandnes kommunes postliste. Kilden er OpenGov/360online-portalen, portal-slug `SANDNESPB`. Data finnes fra november 2022.

**Daglig endringslogg** dekker fire sakstyper:

| Sakstype       | Prefiks   |
|----------------|-----------|
| Byggesak       | `BYGG-`   |
| Henvendelse    | `HENV-`   |
| Ulovlighetssak | `ULOV-`   |
| Tilsynssak     | `TILSYN-` |

## Dataflyt

Portalen har verken dato-filter eller API/feed for "nytt siden sist" (bekreftet: `?casetypeid=...` gir stille bare siste to måneder). Løsningen er `?q=<PREFIKS>-`, som viser alle år - men det betyr at **hele sakslista sveipes på nytt hver dag**, og dagens resultat diffes mot gårsdagens snapshot: `nye_saker` (helt nye `document_id`) og `nye_journalposter` (kjente saker med journalpost-ID-er vi ikke hadde).

Sandnes' sakstittel (h2 på detaljsiden) har ofte en ekte andre linje med en redundant oppsummering av adresse+matrikkelnr (f.eks. "Skogvokterveien 24, 33/603/0/0" - rekkefølgen varierer). Denne linja fjernes fra det lagrede tittel-feltet og brukes kun som fallback når selve adresse-panelet er tomt (`address_from_andre_linje()`).

## Azure-struktur

```
bronze/sandnes/
├── load_type=full/<slug>/dump_date=YYYY-MM-DD/<slug>_saker-full-YYYY-MM-DD.jsonl.gz
├── load_type=incremental/date=YYYY-MM-DD/
│   ├── <slug>_saker-YYYY-MM-DD.jsonl.gz          # nye saker
│   └── <slug>_journalposter-YYYY-MM-DD.jsonl.gz  # nye journalposter på gamle saker
└── _state/<slug>_snapshot.json                    # siste kjente tilstand (overskrives)
```

`<slug>` = `bygg`/`henv`/`ulov`/`tilsyn`. Format: gzippet JSONL, UTF-8.

## Komponenter

- **`common/scraper_lib.py`** - kanonisk fil, all skrapelogikk.
- **`2022-today_dump/{bygg,henv,ulov,tilsyn}/main.py`** - engangs historisk dump, kjøres lokalt, importerer `common/` direkte.
- **`running_daily/`** - én wheel (`postlister-sandnes-bronze`) med fire entry points (`sandnes-{bygg,henv,ulov,tilsyn}-trigger`), som deler én fysisk kopi av `scraper_lib.py` (`app/lib/`).

Ny sakstype: legg til i `SAKSTYPER`-dicten i `app/main.py` + én `trigger_*()`-funksjon + én linje i `pyproject.toml`/`setup.py` + slug i `_SAKSTYPE_SLUG` i `common/scraper_lib.py`. Ikke noe nytt wheel-prosjekt.

## Sikkerhetsmekanismer i `run_daily()`

1. `limit` (kun lokal testing) rører **aldri** Azure eller snapshotet.
2. Snapshotet avanseres **kun** hvis dagens opplasting faktisk lyktes.
3. Snapshotet er primært i Azure (`_state/<slug>_snapshot.json`), ikke bare lokalt.

Samme mønster som Lillestrøm-oppsettet.

## Status

Azure-opplastingen er nylig bygget og verifisert med mock-baserte tester (alle fem sikkerhetsscenarioer treffer forventet oppførsel), men **ikke testet mot ekte Azure ennå** - kontoen som bygger dette har foreløpig ikke skrivetilgang til `bronze/sandnes/`.

## Kjente, bevisst aksepterte gap

- Ingen circuit-breaker ved høy feilrate i dagens sveip.
- Svak retry/backoff (3 forsøk, ingen User-Agent, ingen eksponentiell backoff eller 429/5xx-håndtering).
- Ingen låsing mot samtidige snapshot-skriv.
- Ingen deteksjon av redigerte/fjernede dokumenter (kun nye journalpost-ID-er fanges opp).

## Bygg og deploy

```bash
cd bronze/postlister/opengov/sandnes/running_daily
rm -rf dist build *.egg-info
python -m build --wheel   # -> dist/postlister_sandnes_bronze-0.1.0-py3-none-any.whl
```

Ikke commit wheelen til git. Last opp `.whl` til Databricks og opprett fire jobber (Python wheel-task), én per entry point. Historisk backfill kjøres én gang lokalt: `python 2022-today_dump/<sakstype>/main.py`.

**Lokal test uten å røre Azure**: `python3 -c "from app.main import main; main('bygg', limit=30)"` (fra `running_daily/`) - gir `[Testkjøring]`-output.

## Overvåking

`write_changelog()` printer antall nye saker/journalposter, `[Azure]`-linjer viser opplastingsstatus, og `[Snapshot] IKKE avansert` betyr gårsdagens opplasting feilet.
