import requests
import json
import base64
from pathlib import Path
from urllib.parse import quote
import re
import time
from datetime import date, datetime, timedelta
import gzip
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

#This script fetches all cases with "BYGG-202" filter at https://www.bergen.kommune.no/omkommunen/offentlig-innsyn/innsynplanogbyggesak/saksinnsyn?q=%22BYGG-202%22

BASE_URL = "https://www.bergen.kommune.no/innsynplanogbyggesak/api"
CASE_BASE_URL = "https://www.bergen.kommune.no/omkommunen/offentlig-innsyn/innsynplanogbyggesak/saksinnsyn/sak/"

KOMMUNE_NR = 4601 #BERGEN
KOMMUNE = "Bergen"
# ROWS =  #n rows when filtering '"BYGG-202"'
TEKST = '"HENV-202"'
cases_url = f"{BASE_URL}/saker"
params_for_rows = {
    "tekst": TEKST,
    "orderBy": "statusDato",
    "asc": "false"
} 

start_time = time.time()

row_response = requests.get(cases_url, params=params_for_rows) #get n rows 
row_response.raise_for_status()
ROWS = row_response.json().get('numTotal')

params = {
    "tekst": TEKST,
    "rows": ROWS,
    "orderBy": "statusDato",
    "asc": "false"
}

resp = requests.get(cases_url, params=params)
resp.raise_for_status()
cases = resp.json().get("items", [])


results = []
count = 0

def extract_address_from_title(title: str) -> str:
    
    if not title:
        return None
    match = re.match(r'^\d+(?:/\d+){1,4}\s+([^,]+)', title)
    if match:
        address = match.group(1).strip()
        # Check if the "address" is just numbers/slashes (like "128/69")
        # If so, it's not a real address
        if re.match(r'^\d+(?:/\d+)*$', address):
            return None
        return address
    return None


# 2. Iterate cases and retrieve documents + pdfs
for case in cases:
    case_number = case.get("saksnr")
    case_url = CASE_BASE_URL + case_number
    case_title = case.get("tittel", "")

    match = re.match(r"^[\d/]+", case_title)
    gnr_bnr_fnr_snr = match.group(0) if match else None
    matrikkel_number = f"{KOMMUNE_NR}-{gnr_bnr_fnr_snr}" if gnr_bnr_fnr_snr else None
    adresse_navn = extract_address_from_title(case_title)
    

    count += 1
    elapsed = time.time() - start_time
    avg_per_case = elapsed / count
    remaining = (len(cases) - count) * avg_per_case

    print(f"{count}/{len(cases)} - Retrieving documents for {case_number} ... "
        f"(elapsed: {elapsed:.1f}s, est. remaining: {remaining:.1f}s)")
    
    document_resp = requests.get(f"{BASE_URL}/dokumenter", params={"saksnr": case_number})
    if document_resp.status_code != 200:
        print(f"  Error ({document_resp.status_code}) at {case_number}")
        continue

    document_data = document_resp.json().get("items", [])
    documents = []

    for d in document_data:
        files = d.get("filer", [])
        files_info = []
        
        for f in files:
            try:
                jnr = d.get("jnr")
                source_id = f.get("kildeid")
                filename = f.get("filnavn", "")
                if not jnr or not source_id or not filename:
                    continue

                files_info.append({
                "filnavn": filename,
            })

            except Exception as e:
                print(f"Error building file url: {e}")
                continue

        documents.append({
            "saksurl": case_url,
            "tittel": d.get("tittel"),
            "type": d.get("type"),
            "dokdato": d.get("dokdato"),
            "journaldato": d.get("journaldato"),
            "jp_nummer": d.get("doknr"),
            "avsendere": d.get('avsendere'),
            "mottakere": d.get('mottakere'),
            "dokumenter": files_info,

        })

  
    # --- Samle for JSON-lagring ---
    results.append({
        "count": count,
        "kommune": KOMMUNE,
        "kommune_nr": KOMMUNE_NR,
        "saksnr": case_number,
        "matrikkelnr": matrikkel_number,
        "saksurl": case_url,
        "sakstype": case.get("sakstypenavn"),
        "tittel": case.get("tittel"),
        "adresse_navn": adresse_navn,
        "status": case.get("status"),
        "gnrBnr": case.get("gnrBnr"),
        "journalposter": documents
    })

# --- Lagre lokalt ---
# output_path = Path(__file__).parent / f"json_data/HENV_bergen_postliste_2022-{date.today()}.json"
# with open(output_path, "w", encoding="utf-8") as f:
#     json.dump(results, f, ensure_ascii=False, indent=2)

# print(f"\n✅ Lagret {len(results)} saker til {output_path}")


def to_azure_container(bronze_data, day_back_from_today=1, ACCOUNT_URL = 'https://storaggen2eaccountprod.blob.core.windows.net', AZURE_CONTAINER_NAME='postlister', DATA_PATH='bronze/bergen/load_type=full') -> None:
    einnsyn_reference_date = date.today() - timedelta(days=day_back_from_today)  # Checks for publiseringer with a date == yesterday in yyyy-mm-dd format
    
    if not bronze_data:
        print(f"No data fetched for {einnsyn_reference_date}")
        return

    # Write bronze data to JSON file (TEST)
    # output_file = f"json_data/stavanger_bronze_data_{einnsyn_reference_date}.json"
    # with open(output_file, 'w', encoding='utf-8') as f:
    #     json.dump(bronze_data, f, ensure_ascii=False, indent=2)

    # Upload to Azure Blob Storage

    
    container_name = AZURE_CONTAINER_NAME

    if ACCOUNT_URL:
        try:
            default_credential = DefaultAzureCredential()
            client = BlobServiceClient(ACCOUNT_URL, credential=default_credential)

            # Get container client
            container_client = client.get_container_client(container_name)

            # Create blob name with full path and date, separated into saker and journalposter as this is the structured retrieved from the api
            blob_name = f"{DATA_PATH}/date={einnsyn_reference_date}/batch_henv-2022-{einnsyn_reference_date}.jsonl.gz"
            

            # Convert data to JSONL format (each record on a separate line)
            json_l_lines = []
            
            # Add new_saker
            for sak in bronze_data:
                json_l_lines.append(json.dumps(sak, ensure_ascii=False))
            

            jsonl_data = '\n'.join(json_l_lines)
            

            # Compress with gzip
            compressed_data = gzip.compress(jsonl_data.encode('utf-8'))
            

            # Upload to blob storage
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(compressed_data, overwrite=True)

            print(f"\nData uploaded to Azure Blob Storage: {container_name}/{blob_name}")
        except Exception as e:
            print(f"Error uploading to Azure: {e}")
    else:
        print("\nAZURE_STORAGE_ACCOUNT_URL not set, skipping Azure upload")


to_azure_container(results)