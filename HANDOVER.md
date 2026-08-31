# Handover: postlister-prosjektet

Skrevet 2026-08-31 idet prosjektet parkeres. Formålet med dette dokumentet er alt som *ikke* står i koden - status, kjente feller, og hva jeg ville gjort videre.

## Kort oppsummering

18 kommuner er skrapet i ulik grad, fordelt på fire plattformer (ACOS, eInnsyn, Innsynsportal/360, OpenGov/360online), hver med egen `common/scraper_lib.py` og en delt medaljong-struktur (`<dump>_dump/` for historisk backfill, `running_daily/` for den daglige endringsloggen). Kun **4 av 18** kommuner (Gjøvik, Kristiansand, Lillestrøm, Sandnes) er wheel-pakket for Databricks-deploy. **Ingen** kommune har en silver-transform ennå - bronze er alt som finnes i dette repoet.

**Det største enkeltfunnet fra denne kartleggingen**: for flertallet av kommunene dekker de lagrede JSON-dump-filene bare noen dager til uker, til tross for at mappenavnet lover full historikk (f.eks. `alesund-2010-2019_dump` inneholder i praksis kun data fra april-mai 2011). Dette er trolig rester av test-/limit-kjøringer som aldri ble erstattet med en ekte full kjøring. Se tabellen under - "ekte datospenn" er ofte mye smalere enn mappenavnet antyder. **Ikke stol på mappenavn alene - sjekk faktisk min/max-dato i filen før du antar en dump er komplett.**

## Status og kjente problemer per kommune

| Kommune | Plattform | Status | Saker | Gnr/bnr-dekning | Wheel/daglig |
|---|---|---|---|---|---|
| Gjøvik | ACOS | Ferdig skrapet* | 6376 | 98,9% | Ja (uverifisert i prod, se eget avsnitt) |
| Ålesund | ACOS | Halvveis (smalt vindu) | 478 | 99% | Nei |
| Bærum | ACOS | Ikke begynt | 0 | - | Nei |
| Larvik | ACOS | Halvveis (smalt vindu) | 153 | 93,5% | Nei |
| Moss | ACOS | Halvveis (smalt vindu) | 50 | 100% | Nei |
| Sandefjord | ACOS | Halvveis (smalt vindu) | 110 | 94,5% | Nei |
| Tønsberg | ACOS | Halvveis (svært lite data) | 14 | 93% | Nei |
| Bodø | eInnsyn | Halvveis (dump ser komplett ut) | 9556 | 4,6%** | Nei |
| Fredrikstad | eInnsyn | Halvveis (dump ser komplett ut) | 5758 | 87,1% | Nei |
| Oslo | eInnsyn | Halvveis (kun ~3 mnd, ikke reell "2018-today") | 1151 | 4,6%** | Nei |
| Asker | Innsynsportal | Halvveis (bygg komplett, henv=0% gnr/bnr) | 24 279 | 94,9% | Nei |
| Drammen | Innsynsportal | Halvveis (høyest datakvalitet av alle) | 56 680 | 99,5% | Nei |
| Tromsø | Innsynsportal | Halvveis (alle 4 sakstyper ser komplette ut) | 12 621 | 95,6% | Nei |
| Trondheim | Innsynsportal | Halvveis (bygg aktiv, henv/ulov/tilsyn stanset 2023) | 29 919 | 98,6% | Nei |
| Kristiansand | OpenGov | Halvveis (bygg/henv kun 2026-data) | 1967 | 98,1% | Ja |
| Lillestrøm | OpenGov | Halvveis (bygg/henv kun 2026-data) | 1700 | 96,9% | Ja |
| Sandnes | OpenGov | Halvveis (bygg nesten komplett) | 4160 | 99,8% | Ja (Azure uverifisert) |
| Sarpsborg | OpenGov | Halvveis (bygg komplett, ingen wheel) | 2650 | 99,7% | Nei |

\* Gjøvik er eneste kommune som oppfyller "ferdig skrapet" strengt (full dump + bygget wheel), men egen BRONZE_README flagger selv at cloud-opplasting er utestet.
\** Bodøs og Oslos lave gnr/bnr-dekning er **dokumentert forventet oppførsel** i koden, ikke en bug - de fleste titler i disse kildene oppgir bare gateadresse, ikke matrikkelnummer.

### Kjente datakvalitetsproblemer, kommune for kommune

- **Ålesund**: seks under-arkiver (nå/2020-23/2010-19 + ørskog/sandøy/skodje). Skodje har et helt annet tittelformat uten gateadresse (kun gnr/bnr). GNR-OFFSET er hardkodet for ørskog/sandøy/skodje pga. kommunesammenslåing - matrikkelnr peker feil hvis offset fjernes.
- **Bærum**: API-et mangler eiendom/matrikkel-felt helt. Gnr/bnr må geokodes via Geonorge med fallback til tittel. Aldri kjørt - ingen lokal data.
- **Gjøvik**: matrikkelnr står nesten alltid først i tittel, varierende skilletegn. Se `DATABRICKS_ASSISTANT_BRIEF.md` og `DATABRICKS_SETUP.md` for cloud-status.
- **Larvik**: annen rekkefølge enn Moss/Ålesund (gbnr sist). Docstring oppgir 88,2% adresse-treff, 96,5% gbnr-match på stikkprøve. Kjent "hale" av uløste edge-cases (adresselister med 3+ adresser uten gbnr).
- **Moss**: "eiendom"-feltet fra API er alltid tomt i praksis - alt hentes fra tittel. Håndterer flere gnr/bnr-blokker i samme tittel.
- **Sandefjord**: fire ulike tittelformat på tvers av bygg_ny/tilsyn_ny/bygg_2001_2016/andebu/stokke. Andebu har kildens egen skrivefeil "bgnr". GNR-OFFSET for andebu/stokke (gamle Vestfold-gårdsnumre).
- **Tønsberg**: mest inkonsekvente tittelformat av alle ACOS-kommuner (tre delformater i Re-arkivet alene). Minst data av alle 18 kommuner (14 saker).
- **Bodø**: adressevalidering er bevisst *ikke* en kuratert liste - omgjort etter en konkret feilrapport (11 eksempler på både falske positiver og negativer).
- **Fredrikstad**: gnr/bnr er nesten alltid eksplisitt merket med ordet "Eiendom" i tittelen - mer presist enn Oslos frittstående tallpar-søk. En kjent bug med bnr-lister ("Gnr 303, bnr 873, 875, 888...") er beskrevet og fikset.
- **Oslo**: samme gnr/bnr-begrensning som Bodø (dokumentert, ikke bug). Omfattende liste over kjente ikke-adresse-mønstre (parentetiske historiske adresser, "m.fl.", "Seksjon N").
- **Asker og Drammen (delt bug)**: "proceedings-datofilteret er upålitelig" - løsningen er å hente journalposter dato-basert og matche sak via saksnummer i stedet for å stole på sakens eget datofilter. Samme fiks gjelder begge kommuner. Asker hadde tidligere en bug som ga feilaktig "/None" i matrikkelnr for 145 saker (nå fikset).
- **Tromsø**: matrikkelnr først i tittel, som Sarpsborg/Gjøvik. Beste helhetlige datakvalitet av alle 18 kommuner.
- **Trondheim**: API støtter ikke offset-paginering - henter dag-for-dag (maks 100/felt), med departement-filter-fallback for dager over grensen. **Viktig**: henv/ulov/tilsyn sine lagrede dump-filer stopper i 2023 selv om `running_daily/` for disse har nyere output datert juli 2026 - dvs. selve full-dump-filen ble aldri re-kjørt etter 2023, men den daglige jobben later til å kjøre. Bør undersøkes.
- **Kristiansand**: platform mangler datofilter for "nytt siden sist" - løses med full daglig sweep + diff mot snapshot. Adressefelt er en linjeliste med duplikater som må ryddes.
- **Lillestrøm**: BRONZE_README dokumenterer flere aksepterte gap - ingen circuit-breaker ved høy feilrate, svak retry/backoff (3 forsøk, ingen backoff/429-håndtering), ingen låsing mot samtidige snapshot-skriv, `.env.example` manglet ved siste sjekk selv om koden refererer til den.
- **Sandnes**: BRONZE_README sier eksplisitt at Azure-opplasting er "verifisert med mock-baserte tester... men IKKE testet mot ekte Azure - kontoen som bygger dette har foreløpig ikke skrivetilgang".
- **Sarpsborg**: matrikkelnr først i tittel (motsatt av Lillestrøm/Kristiansand). Adressepanelet på detaljsiden gir oftest adresse+gnr/bnr direkte - mindre avhengig av tittel-parsing enn de andre OpenGov-kommunene. Eneste OpenGov-kommune uten wheel, og kun bygg-sakstype er implementert (ingen henv/ulov/tilsyn).

## Hvordan kjøre en scraper lokalt

Mønsteret er likt for alle kommuner (Gjøvik er mest komplett og gjør fin referanse):

**Historisk dump** (engangs, kjøres lokalt, skriver kun til lokal fil):
```bash
cd bronze/postlister/<plattform>/<kommune>
python3 <dump-mappe>/<sakstype>/main.py
```

**Daglig endringslogg** (wheel-pakket, kun for Gjøvik/Kristiansand/Lillestrøm/Sandnes):
```bash
cd bronze/postlister/<plattform>/<kommune>/running_daily[/<sakstype>]
pip3 install -e .
python3 -c "from app.main import trigger; trigger()"
```
Krever at `state.json` finnes (bygget fra historisk dump via `build_state_from_dump()`) og at Azure-credentials er tilgjengelig - se `bronze/postlister/.env.example` (kopier til `.env`, fyll inn Service Principal, eller kjør `az login` for isolert personlig Azure-konto).

For kommuner uten wheel-pakking: kjør `running_daily/<sakstype>/main.py` direkte (importerer `common/` relativt, ikke ment for Databricks-deploy ennå).

## Gjøvik: Databricks/Azure-forsøket (denne økten)

Målet var å få Gjøviks daglige jobb til å kjøre helt automatisk. Kort forløp:

1. Bygget en dobbel landingssone i `write_changelog()`: Unity Catalog Volume når koden kjører inne i Databricks (`_har_databricks_volume()`), ellers Azure Blob.
2. Fikk PAT-basert tilgang til Databricks fra terminalen (`.claude/skills/query-databricks-delta/`) - kontoen har workspace admin, ikke account admin, så Service Principal/native Claude-connector var blokkert (krever account admin-opprettet OAuth-app).
3. Prøvde å kjøre selve scrapingen inne i Databricks. **Blokkert**: workspacet er hard-låst til kun serverless compute (bekreftet - cluster policies finnes, men jobb-cluster-oppretting feiler likevel). Serverless har ikke internett-tilgang til eksterne API-er - jobben feilet med DNS-oppløsningsfeil mot `www.gjovik.kommune.no`.
4. Pivoterte til å kjøre scrapingen utenfor Databricks (GitHub Actions, siden koden allerede ligger der) og lande data i Azure Blob i stedet - se `.github/workflows/gjovik-daily.yml`. **Denne er skrevet, men ALDRI kjørt/verifisert** - prosjektet ble parkert før en kollega rakk å gi Azure Service Principal-credentials.
5. Alternativ verdt å vurdere: Databricks har et Files API som kan laste rett til en Unity Catalog Volume via PAT (ikke Service Principal) - ville fjernet Azure-avhengigheten for GitHub Actions-jobben helt. Ikke bygget, kun diskutert.

Se `bronze/postlister/acos/gjovik/DATABRICKS_SETUP.md` og `DATABRICKS_ASSISTANT_BRIEF.md` for full detalj.

## Ukjent, uforklart anomali - sjekk dette før du committer noe

To ganger i løpet av denne økten har filer som er committet i git blitt fysisk slettet fra disk *uten* at det var meg (Claude) eller brukeren som gjorde det bevisst - først alle 7 filer under `bronze/postlister/acos/baerum/`, siden fem CI-workflow-filer (`build_upload_wheel.yml` + 4 Kristiansand-workflows) og `bronze/postlister/.env.example`. Alle ble gjenopprettet med `git restore` før commit. Årsaken er **ikke funnet**. Sjekk `git status` grundig før du committer noe stort - hvis filer mangler som burde finnes, kjør `git restore <path>` i stedet for å anta det er en tilsiktet sletting.

## Hva jeg ville gjort videre, i prioritert rekkefølge

1. **Kjøre ekte fulle historiske dumper** for de ~13 kommunene som nå bare har smale test-vinduer, til tross for mappenavn som lover full historikk. Dette er det største gjenstående arbeidet i hele prosjektet.
2. **Verifisere Gjøvik-pipelinen end-to-end**: skaffe Azure SP-credentials (eller bygg Databricks Files API-varianten) og faktisk kjøre `gjovik-daily.yml` en gang.
3. **Undersøke Trondheims stansede henv/ulov/tilsyn-dumper** (stoppet 2023, men daglig jobb later til å kjøre) - mismatch bør forstås før noen stoler på dataene.
4. **Wheel-pakke og automatisere** de resterende 14 kommunene, etter samme mønster som Gjøvik/Kristiansand/Lillestrøm/Sandnes.
5. **Bygge silver-transform** - ingen kommune har dette ennå. `easy-pipelines`-repoet har et etablert mønster for eInnsyn-plattformen (Bergen/Stavanger/Oslo/Statsforvaltere) som kan brukes som referanse for struktur, men ikke kopieres direkte siden det er en annen kildeplattform.
6. Rydde opp i misvisende mappenavn (`20XX-today_dump` som i praksis er en ukes data) - enten kjør ekte dump eller gi mappa et navn som ikke lover noe den ikke leverer.
