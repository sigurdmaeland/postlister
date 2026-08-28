# Lillestrøm Bronze Layer

## Oversikt

Bronze-laget for Lillestrøm kommunes postliste. Kilden er OpenGov/360online-portalen (`opengov.360online.com/Cases/LILLESTROM`). Fire sakstyper hentes med 100 % identisk logikk - eneste forskjell er saksnummer-prefikset man søker på:

| Sakstype       | Prefiks   |
|----------------|-----------|
| Byggesak       | `BYGG-`   |
| Henvendelse    | `HENV-`   |
| Ulovlighetssak | `ULOV-`   |
| Tilsynssak     | `TILSYN-` |

## Dataflyt

Portalen har verken dato-filter eller API/feed for "nytt siden sist" (`?casetypeid=...` viser stille bare siste to måneder, basert på opprettelsesdato). Løsningen er `?q=<PREFIKS>-`, som viser alle år - men det betyr at **hele sakslista sveipes på nytt hver dag**, og dagens resultat diffes mot gårsdagens snapshot for å finne det som faktisk er nytt: `nye_saker` (helt nye `document_id`) og `nye_journalposter` (kjente saker med journalpost-ID-er vi ikke hadde). Dette er det som fanger opp gamle, fortsatt åpne saker som får ny post.

Adresse/gnr-bnr ryddes og struktureres før lagring (portalens rådata er duplisert og rotete, og mangler ofte feste/seksjon), i stedet for å lagres helt uendret - `clean_adresser()`, `clean_gnr_bnr()` og `build_matrikkelnr()` gjør jobben, med fallback-gjetting fra sakstittelen når adresse-panelet er tomt.

## Azure-struktur

```
bronze/lillestrom/
├── load_type=full/<slug>/dump_date=YYYY-MM-DD/<slug>_saker-full-YYYY-MM-DD.jsonl.gz
├── load_type=incremental/date=YYYY-MM-DD/
│   ├── <slug>_saker-YYYY-MM-DD.jsonl.gz          # nye saker
│   └── <slug>_journalposter-YYYY-MM-DD.jsonl.gz  # nye journalposter på gamle saker
└── _state/<slug>_snapshot.json                    # siste kjente tilstand (overskrives)
```

`<slug>` = `bygg`/`henv`/`ulov`/`tilsyn`. Format: gzippet JSONL, UTF-8.

## Komponenter

- **`common/scraper_lib.py`** - kanonisk fil, all skrapelogikk (henting, parsing, adresseopprydding, Azure-opplasting, snapshot-diff).
- **`2020-today_dump/{bygg,henv,ulov,tilsyn}/main.py`** - engangs historisk dump, kjøres lokalt, importerer `common/` direkte.
- **`running_daily/`** - én wheel (`postlister-lillestrom-bronze`) med fire entry points (`lillestrom-{bygg,henv,ulov,tilsyn}-trigger`), som deler én fysisk kopi av `scraper_lib.py` (`app/lib/`).

Ny sakstype: legg til i `SAKSTYPER`-dicten i `app/main.py` + én `trigger_*()`-funksjon + én linje i `pyproject.toml`/`setup.py` + slug i `_SAKSTYPE_SLUG`. Ikke noe nytt wheel-prosjekt.

## Sikkerhetsmekanismer i `run_daily()`

1. `limit` (kun lokal testing) rører **aldri** Azure eller snapshotet.
2. Snapshotet avanseres **kun** hvis dagens opplasting faktisk lyktes - ellers forsøkes samme data på nytt neste kjøring i stedet for å gå tapt.
3. Snapshotet er primært i Azure (`_state/<slug>_snapshot.json`), ikke bare lokalt, siden Databricks-compute ikke nødvendigvis har lokal disk mellom kjøringer.

## Kjente, bevisst aksepterte gap

- Ingen circuit-breaker ved høy feilrate i dagens sveip.
- Svak retry/backoff (3 forsøk, ingen User-Agent, ingen eksponentiell backoff eller 429/5xx-håndtering).
- Ingen låsing mot samtidige snapshot-skriv.
- Ingen deteksjon av redigerte/fjernede dokumenter (kun nye journalpost-ID-er fanges opp).
- `bronze/postlister/.env.example` mangler ennå, selv om `_azure_credential()` viser til den.

## Bygg og deploy

```bash
cd bronze/postlister/opengov/lillestrom/running_daily
rm -rf dist build *.egg-info
python -m build --wheel   # -> dist/postlister_lillestrom_bronze-0.1.0-py3-none-any.whl
```

Ikke commit wheelen til git. Last opp `.whl` til Databricks og opprett fire jobber (Python wheel-task), én per entry point - vurder å forskyve kjøretidene noe siden alle fire treffer samme portal. Historisk backfill kjøres én gang lokalt: `python 2020-today_dump/<sakstype>/main.py`.

**Lokal test uten å røre Azure**: `python3 -c "from app.main import main; main('bygg', limit=30)"` (fra `running_daily/`) - gir `[Testkjøring]`-output.

## Overvåking

`write_changelog()` printer antall nye saker/journalposter, `[Azure]`-linjer viser opplastingsstatus, og `[Snapshot] IKKE avansert` betyr gårsdagens opplasting feilet - bør sjekkes før det hoper seg opp.
