# Gjøvik: bruksanvisning for å koble bronze-wheelen til ditt eget Databricks-workspace

Dette er en ren gjøre-selv-guide - ingen av stegene under krever at Claude er koblet til noe.

Utgangspunkt: du har workspace admin (ikke account admin) på ditt eget personlige Databricks-workspace, og bronze-wheelen (`postlister-gjovik-bygg`) er allerede bygget i `running_daily/bygg/dist/`.

To faser: **Fase 1** bruker kun Databricks (Unity Catalog Volume som landingssone) - ingen Azure-oppsett nødvendig. **Fase 2** bytter til Azure Blob når du er klar for det (samme sti-konvensjon, koden velger automatisk).

## 0. Hva du ender opp med

- En daglig Databricks-jobb som henter endringsloggen for Gjøvik og skriver den som gzippet JSONL - til en Unity Catalog Volume i fase 1, til Azure Blob i fase 2.
- En Unity Catalog-katalog (`postlister`) i ditt eget workspace, klar til å ta imot data - men foreløpig **tom** i `silver`, siden silver-transformen (bronze → Delta-tabeller) ikke er bygget ennå (se punkt 9).
- Mulighet til å spørre workspacet ditt fra terminalen med `query-databricks-delta`-skillen.

---

## FASE 1: kun Databricks (anbefalt startpunkt)

Koden velger automatisk landingssone: hvis den kjører inne i Databricks og finner `/Volumes`, skriver den dit (`_har_databricks_volume()`/`_skriv_til_volume_changelog()` i `common/scraper_lib.py`) - ingen Azure-credentials involvert i det hele tatt. Kjører den lokalt (uten `/Volumes`), faller den tilbake til Azure Blob (fase 2).

### 1. Databricks: Unity Catalog - katalog, skjema og volume

I Databricks-workspacet, åpne **SQL Editor** (eller en notebook) og kjør:

```sql
CREATE CATALOG IF NOT EXISTS postlister;
CREATE SCHEMA IF NOT EXISTS postlister.bronze;
CREATE SCHEMA IF NOT EXISTS postlister.silver;
CREATE VOLUME IF NOT EXISTS postlister.bronze.raw;
```

Hvis dette feiler med en tilgangsfeil: sjekk at du faktisk er workspace admin (brukernavn → **Admin Settings**, ikke **Manage account**). På et workspace som er Unity Catalog-aktivert automatisk (standard for alle opprettet etter november 2023) eier workspace admin default-katalogen og kan opprette nye kataloger/volumer uten videre.

Sti-konvensjonen koden bruker er `/Volumes/postlister/bronze/raw/gjovik/...` - `gjovik`-undermappen opprettes automatisk av jobben selv, du trenger ikke lage den manuelt.

### 2. Databricks: compute

Sjekk om workspacet ditt har **Serverless** tilgjengelig (Compute-siden i menyen) - da slipper du å administrere en cluster selv. Hvis ikke: opprett en enkel single-node cluster (Compute → Create compute), noter **Cluster ID** fra URL-en (`.../clusters/<cluster-id>`).

### 3. Last opp wheelen og opprett jobben

1. Gå til **Workspace** → naviger til en mappe → **Upload** → velg `running_daily/bygg/dist/postlister_gjovik_bygg-0.1.0-py3-none-any.whl`.
2. Gå til **Workflows** → **Create Job**.
3. Task type: **Python Wheel**.
4. Package name: `postlister-gjovik-bygg`
5. Entry point: `gjovik-bygg-trigger`
6. Dependent library: pek på wheel-filen du lastet opp.
7. Cluster: velg serverless eller clusteret fra punkt 2.
8. Schedule: daglig, f.eks. kl. 02:00.
9. Lagre og kjør jobben manuelt én gang for å teste.

Se `[Volume]`-linjene i jobbens kjørelogg for skrivestatus - `[Volume] skrev ... poster ->` betyr suksess.

### 4. Kjør den historiske engangsdumpen (lokalt, ikke i Databricks)

`2017-today_dump/bygg/main.py` kjøres én gang lokalt (ikke som Databricks-jobb) for å etablere `gjovik.json` og `state.json`:

```bash
cd bronze/postlister/acos/gjovik
python3 2017-today_dump/bygg/main.py
```

**OBS**: dette scriptet laster foreløpig kun lokalt, ikke opp til noen landingssone (se punkt 9). `state.json` bygges fra denne dumpen via `build_state_from_dump()` - kopier den til `running_daily/bygg/app/state.json` før første jobbkjøring, ellers behandler jobben alt som "nytt" første gang.

### 5. Verifiser at data faktisk landet

I samme SQL Editor:

```sql
LIST '/Volumes/postlister/bronze/raw/gjovik/load_type=incremental/';
```

eller bla i UI-et under **Catalog** → `postlister` → `bronze` → **Volumes** → `raw`.

---

## FASE 2: bytt til Azure Blob (når du er klar for det)

### 6. Azure: Service Principal for blob-opplasting

**Hvis du vil bruke den delte prosjekt-kontoen** (`storaggen2eaccountprod`, container `postlister`): du trenger noen med tilgang til den Azure-tenanten til å opprette en App Registration med `Storage Blob Data Contributor`-rolle på kontoen, og gi deg client-id/tenant-id/client-secret.

**Hvis du vil leke isolert i din egen Azure-konto:**
1. Opprett en Storage Account (Standard, StorageV2) og en container, f.eks. `postlister`.
2. Opprett en App Registration i Entra ID (Azure AD) → noter **Application (client) ID** og **Directory (tenant) ID**.
3. Lag et client secret under "Certificates & secrets" på App Registration-en.
4. Gi App Registration-en rollen **Storage Blob Data Contributor** på storage-kontoen (IAM → Add role assignment).
5. Endre disse konstantene i `common/scraper_lib.py` (og synk kopien i `running_daily/bygg/app/lib/scraper_lib.py`, se punkt 8) til din egen konto:
   ```python
   AZURE_ACCOUNT_URL = "https://<din-storage-konto>.blob.core.windows.net"
   AZURE_CONTAINER_NAME = "<din-container>"
   AZURE_BASE_PATH = "bronze/gjovik"
   ```

### 7. Databricks: secret scope for Azure-credentials

Koden plukker automatisk opp credentials fra et secret scope kalt `postlister` når den kjører inne i Databricks (via `dbutils.secrets.get`) - men kun hvis den IKKE finner `/Volumes` (fase 1 har alltid forrang). Vil du faktisk teste Azure-veien fra en Databricks-jobb, må du derfor kjøre den fra et workspace/cluster uten Volumes montert, eller bevisst fjerne/omdøpe volumet midlertidig. For lokal kjøring er dette ikke et problem - der finnes `/Volumes` uansett ikke.

```bash
databricks secrets create-scope postlister
databricks secrets put-secret postlister SP-PIPELINE-POSTLISTER-CLIENT-ID
databricks secrets put-secret postlister SP-PIPELINE-POSTLISTER-TENANT-ID
databricks secrets put-secret postlister SP-PIPELINE-POSTLISTER-CLIENT-SECRET
```

For lokal kjøring: legg de samme tre verdiene i `bronze/postlister/.env` i stedet.

### 8. Synk og bygg wheelen på nytt

```bash
cd bronze/postlister/acos/gjovik
cp common/scraper_lib.py running_daily/bygg/app/lib/scraper_lib.py
cd running_daily/bygg && rm -rf dist build *.egg-info && python -m build --wheel
```

---

## 9. Verifiser fra terminalen din (query-databricks-delta)

```bash
cd .claude/skills/query-databricks-delta
cp connection.example.json connection.json
# fyll inn DATABRICKS_HOST + DATABRICKS_TOKEN (PAT)
pip install databricks-connect databricks-sdk
python3 scripts/query_delta.py list-tables --catalog postlister --schema bronze
```

(Dette lister Delta-*tabeller* - siden bronze-dataene per nå kun ligger som rå JSONL.gz-filer i Volume/Blob, ikke Delta-tabeller, vil `list-tables` være tom inntil silver-transformen finnes. Bruk `LIST`-SQL-en i punkt 5 for å se selve filene.)

## 10. Det som IKKE er bygget ennå

- **Silver-transform**: ingenting leser bronze-filene (Volume eller Azure) og skriver til `postlister.silver` Delta-tabeller ennå.
- **Historisk dump til bronze**: `2017-today_dump/bygg/main.py` skriver kun lokal fil, ingen opplasting til Volume eller Azure slik daglig-jobben nå gjør.

Si fra når du vil ha hjelp med disse to.
