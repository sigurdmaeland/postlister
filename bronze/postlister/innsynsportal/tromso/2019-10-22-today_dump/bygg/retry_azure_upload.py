"""Engangs-hjelpeskript: last opp en allerede fullført full-dump til Azure
på nytt, UTEN å kjøre hele skrapingen på nytt (523s+ for Byggesak).

Bruk denne når run_full_dump() selv har lyktes med å skrive tromso_bygg.json
lokalt, men selve Azure-opplastingen feilet i etterkant (f.eks.
AuthorizationPermissionMismatch fordi kontoen brukt til `az login` ikke har
rollen "Storage Blob Data Contributor" (eller tilsvarende) tildelt på
storage-kontoen/containeren i Azure Portal -> Access Control (IAM), evt. at
tildelingen er scoped til feil sti).

Kjør: python3 retry_azure_upload.py   (fra denne mappa)
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from scraper_lib import upload_full_dump_til_azure  # noqa: E402

OUTPUT_FILE = Path(__file__).parent / "tromso_bygg.json"

if __name__ == "__main__":
    poster = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
    print(f"Leste {len(poster)} saker fra {OUTPUT_FILE} - prøver opplasting til Azure på nytt...")
    upload_full_dump_til_azure(poster, "Byggesak")
