from __future__ import annotations
import requests
import json
import base64
from pathlib import Path
from urllib.parse import quote
import re
import time
from datetime import date, datetime, timedelta
import gzip
from azure.storage.blob import BlobServiceClient
import os
from .config import get_azure_credential
import asyncio
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

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

def extract_tiltakstype_from_title(title: str) -> str:
    """Extract tiltakstype from title"""
    if not title:
        return None
    
    # List of tiltakstyper to search for (case-insensitive)
    tiltakstyper = [
        "Bruksendring",
        "Fasadeendring", 
        "Nybygg",
        "Tilbygg",
        "Påbygg",
        "Anlegg",
        "Konstruksjon",
        "Forhåndskonferanse",
        "Riving",
        "Dispensasjon"
    ]
    
    # Convert title to lowercase for comparison
    title_lower = title.lower()

    
    # Search for each tiltakstype
    for tiltakstype in tiltakstyper:
        if tiltakstype.lower() in title_lower:
            return tiltakstype
    
    return None

def extract_dokumenttype_from_filename(filnavn: str) -> str:
    """Extract dokumenttype from filename, handling æøå variations and word boundaries"""
    if not filnavn:
        return None
    
    
    filnavn_upper = filnavn.upper()
    
    # Replace common æøå encoding variations
    filnavn_normalized = filnavn_upper.replace('Æ', 'AE').replace('Ø', 'O').replace('Å', 'A')
    
    # Define document type mappings
    dokumenttyper = {
        "Søknad": ["SOK", "SOKNAD", "SOK-ET", "ETTRINNSSOKNAD", "RAMMESOKNAD", 
                   "SOK-DISP", "SOK-ES", "SOK-FA", "SOK-IG", "SOK-MB", "SOK-RS", "SOK-TA",
                   "SØK", "SØKNAD", "SØK-ET", "ETTRINNSSØKNAD", "RAMMESØKNAD",
                   "SØK-DISP", "SØK-ES", "SØK-FA", "SØK-IG", "SØK-MB", "SØK-RS", "SØK-TA"],
        "Ansvar": ["ANKO", "ANSVKONT", "GJENNOMFORINGSPLAN", "GJENNOMFØRINGSPLAN"],
        "Kart og situasjonsplan": ["KART"],
        "Tegninger": ["TEGN", "TEGNING"],
        "Vedtak": ["VED", "VED-FA", "VEDTAK"]
    }
    
    # Helper function to check if keyword has valid boundaries
    def has_valid_boundaries(text, keyword, start_pos):
        # Check character before keyword (if not at start)
        if start_pos > 0:
            char_before = text[start_pos - 1]
            if char_before not in [' ', '-', '_']:
                return False
        
        # Check character after keyword (if not at end)
        end_pos = start_pos + len(keyword)
        if end_pos < len(text):
            char_after = text[end_pos]
            if char_after not in [' ', '-', '_', '.', ',']:
                return False
        
        return True
    
    # Search for each document type
    for dokumenttype, keywords in dokumenttyper.items():
        for keyword in keywords:
            # Check in original filename
            pos = filnavn_upper.find(keyword)
            if pos != -1 and has_valid_boundaries(filnavn_upper, keyword, pos):
                return dokumenttype
            
            # Check in normalized filename
            pos = filnavn_normalized.find(keyword)
            if pos != -1 and has_valid_boundaries(filnavn_normalized, keyword, pos):
                return dokumenttype
    
    return None


def sanitize_filename(filename: str) -> str:
# All dots and commas must be replaced with underscores(_) in order for url to work, 
    #This may apply to more characters, but by 20.10.25 I have identified these two
        #replaced "/" with underscores 20.11.25
    filename_no_dot = filename.replace(".", "_").replace(",", "_").replace("/", "_")
    return filename_no_dot

def fetch_bergen_cases(date):
    try:
        BASE_URL = "https://www.bergen.kommune.no/innsynplanogbyggesak/api"
        CASE_BASE_URL = "https://www.bergen.kommune.no/omkommunen/offentlig-innsyn/innsynplanogbyggesak/saksinnsyn/sak/"

        KOMMUNE_NR = 4601 #BERGEN
        KOMMUNE = "Bergen"

        # ROWS =  #n rows when filtering '"BYGG-202"'
        TEKST = '"ULOV-202"'
        cases_url = f"{BASE_URL}/saker"
        params_for_rows = {
            "tekst": TEKST,
            "orderBy": "statusDato",
            "asc": "false"
        } 

        # yesterday = datetime.now() - timedelta(days=1)
        # yesterday_timestamp = int(yesterday.timestamp() * 1000)  # Convert to milliseconds

        start_time = time.time() #not related to the yesterday logic, only for console log

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

        # Get yesterday's date
        raw_cases = resp.json().get("items", [])
        
    
        date_start = int((date + timedelta(days=-14)).timestamp() * 1000)  # 30 days back from the date we have set to check
        date_end = int((date + timedelta(days=1)).timestamp() * 1000)  # Today at 00:00:00
        
        # Filter cases where statusDato falls within yesterday's date range
        cases = [
            case for case in raw_cases 
            if date_start <= case.get("statusDato", 0) < date_end
        ]


        results = []
        count = 0
        # 2. Iterate cases and retrieve documents + pdfs
        for case in cases:
            case_number = case.get("saksnr")
            case_url = CASE_BASE_URL + case_number
            case_title = case.get("tittel", "")

            match = re.match(r"^[\d/]+", case_title)
            gnr_bnr_fnr_snr = match.group(0) if match else None
            matrikkel_number = f"{KOMMUNE_NR}-{gnr_bnr_fnr_snr}" if gnr_bnr_fnr_snr else None
            adresse_navn = extract_address_from_title(case_title)
            tiltakstype = extract_tiltakstype_from_title(case_title) 

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

                        dokumenttype = extract_dokumenttype_from_filename(filename)
                        # Correct URL-encoding and p-parameter
                        encoded_name = quote(sanitize_filename(filename))

                        p = base64.b64encode(f"/saksinnsyn/sak/{case_number}".encode()).decode()

                        url = f"{BASE_URL}/fil/{jnr}/{source_id}/{encoded_name}?p={p}"

                        if f.get("publisert") == True:
                            files_info.append({
                                "filnavn": filename,
                                "filtype": f.get("filtype"),
                                "publisert": f.get("publisert"),
                                "dokumenttype": dokumenttype,
                                "url": url
                            })
                        else:
                            files_info.append({
                            "filnavn": filename,
                            "filtype": f.get("filtype"),
                            "publisert": f.get("publisert"),
                            "dokumenttype": dokumenttype,
                            "url": case_url
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
                "tiltakstype": tiltakstype,
                "tittel": case.get("tittel"),
                "adresse_navn": adresse_navn,
                "status": case.get("status"),
                "gnrBnr": case.get("gnrBnr"),
                "soker": case.get("soker"),
                "tiltakshaver": case.get("tiltakshaver"),
                "journalposter": documents
            })

        return results
    
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return []
    

async def main(day_back_from_today=1, ACCOUNT_URL='https://storaggen2eaccountprod.blob.core.windows.net', AZURE_CONTAINER_NAME='postlister', DATA_PATH='bronze/bergen/load_type=incremental'):

    bergen_reference_date = datetime.now(ZoneInfo('Europe/Oslo')).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=day_back_from_today)
    bronze_data = fetch_bergen_cases(bergen_reference_date)

    if not bronze_data:
        print(f"No data fetched for {bergen_reference_date}")
        return        

    container_name = AZURE_CONTAINER_NAME

    if ACCOUNT_URL:
        try:
            clientsecretcredential = get_azure_credential()
            client = BlobServiceClient(ACCOUNT_URL, credential=clientsecretcredential)

            # Get container client
            container_client = client.get_container_client(container_name)

            # Create blob name with full path and date, separated into saker and journalposter as this is the structured retrieved from the api
            blob_name = f"{DATA_PATH}/date={bergen_reference_date.strftime("%Y-%m-%d")}/ulov_saker-{bergen_reference_date.strftime("%Y-%m-%d")}.jsonl.gz"
            

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

_executor = ThreadPoolExecutor(max_workers=1)

def run_async(coro):
    """
    Run a coroutine from sync code.
    - If no loop is running in this thread: uses asyncio.run().
    - If a loop is already running (Databricks / Jupyter-like contexts): runs in a new loop in a separate thread.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    return _executor.submit(lambda: asyncio.run(coro)).result()

def trigger():
    run_async(main())