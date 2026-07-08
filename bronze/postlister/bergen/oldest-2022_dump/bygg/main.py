import requests
import json
import base64
from pathlib import Path
from urllib.parse import quote
import re
import time
import os
from datetime import date, datetime, timedelta
import gzip
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


#This script iterates through each year form the oldest Bergen byggesaker (2000) up until 2022
BASE_URL = "https://www.bergen.kommune.no/innsynplanogbyggesak/api"
SAK_BASE_URL = "https://www.bergen.kommune.no/omkommunen/offentlig-innsyn/innsynplanogbyggesak/saksinnsyn/sak/"
KOMMUNE_NR = 4601 #BERGEN
KOMMUNE = "Bergen"

#These functions fetches the saker
def api_fetch(year: int, index: int) -> dict:
    search_filter: str = f'{year}{index}byggesak'

    n_rows: int = _get_n_rows(search_filter)
    saker_data: list = _get_data(n_rows, search_filter)

    saker_obj: dict = {'year': year, 'search_index': index, 'search_filter': search_filter, 'saker_data': saker_data}
    return saker_obj

def _get_n_rows(search_filter: str) -> int:
    row_response = requests.get(f'{BASE_URL}/saker', params= {
        "tekst": search_filter,
        "orderBy": "statusDato",
        "asc": "false"
    })
    row_response.raise_for_status()
    n_rows = row_response.json().get('numTotal')
    
    n_rows_for_testing = 100
    return n_rows

def _get_data(n_rows: int, search_filter: str) -> list:
    data_response = requests.get(f'{BASE_URL}/saker', params={
        "tekst": search_filter,
        "rows": n_rows,
        "orderBy": "statusDato",
        "asc": "false"
    })
    data_response.raise_for_status()
    saker_data: list = data_response.json().get('items', [])
    return saker_data

#These functions takes the retrieved saker and fetches their journalposter and documents
#Then it returns the clean saker object with all data
def create_clean_saker_object(raw_data_object: dict) -> dict:
    raw_saker: list = raw_data_object.get('saker_data')
    clean_saker: list = []

    try:
        sak_count = 1
        for sak in raw_saker:
            if sak.get('saksnr').startswith('BYGG-'): #Discard newer cases that we already have
                continue
            
            clean_sak: dict = _assemble_sak(sak)
            clean_saker.append(clean_sak) if clean_sak and isinstance(clean_sak, dict) else None
            print(f'({sak_count}/{len(raw_saker)}) - Saving sak: {clean_sak.get('saksnr')}')
            sak_count += 1
        return {'aar': raw_data_object.get('year'),
                 'sokeindeks': raw_data_object.get('search_index'),
                   'sokefilter': raw_data_object.get('search_filter'),
                     'saker': clean_saker}

    except Exception as e:
        print(f"Error processing and cleaning saker: {e}")
        return {}

def _assemble_sak(sak: dict) -> dict:
    kommune = 'BERGEN'
    kommune_nr = '4601'
    saksnr: str = sak.get('saksnr', '')
    # matrikkelnr = 
    saksurl = f'{SAK_BASE_URL}{saksnr}'
    sakstype: str = sak.get('sakstypenavn', '')
    tittel: str = sak.get('tittel', '')
    # tiltak = _extract_tiltak_from_title(tittel)  #I have some logic for extracting this for title, but will not be used I think
    adresse_navn: list = sak.get('adresse', [])
    status: str = sak.get('status', '')
    avsluttet_dato: int = sak.get('avsluttetdato', 0)
    status_dato: int = sak.get('statusDato', 0)
    gnr_bnr: list = sak.get('gnrBnr', [])
    soker: list = sak.get('soker', [])
    tiltakshaver: str = sak.get('tiltakshaver', '')
    journalposter: list = _get_journalposter(saksnr)


    clean_sak = {
        'kommune': kommune,
        'kommune_nr': kommune_nr,
        'saksnr': saksnr,
        # 'matrikkelnr': '', the db uses the list item gnr_bnr instead
        'saksurl': saksurl,
        'sakstype': sakstype,
        'tittel': tittel,
        # 'tiltak': '', available in the title after the adress
        'adresse_navn': adresse_navn,
        'status': status,
        'avsluttet_dato': avsluttet_dato,
        'status_dato': status_dato,
        'gnr_bnr': gnr_bnr,
        'soker': soker,
        'tiltakshaver': tiltakshaver,
        'journalposter': journalposter
    }

    return clean_sak

# def _extract_tiltak_from_title(tittel: str) -> str:
#     if not sakstittel:
#         return ""

#     # Remove trailing period and extra whitespace
#     sakstittel = tittel.strip().rstrip('.')

#     # Pattern 1: Split on 2+ spaces (most common)
#     # "Arna gnr 302 bnr 22  Hundhaugen 9   Nybygg garasje"
#     parts = re.split(r'\s{2,}', sakstittel)
#     if len(parts) >= 2:
#         tiltak = parts[-1].strip().rstrip('.')
#         # Only return if it looks like a tiltak (not just a number or gnr/bnr)
#         if tiltak and not re.match(r'^\d+[/-]?\d*$', tiltak):
#             return tiltak

#     # Pattern 2: Period separator (single space before period)
#     # "Ytrebygda gnr 35 bnr 191 Søreidneset. Sjøhus."
#     if '. ' in sakstittel:
#         parts = sakstittel.split('. ')
#         if len(parts) >= 2:
#             tiltak = parts[-1].strip().rstrip('.')
#             if tiltak:
#                 return tiltak

#     # Pattern 3: After street address with number
#     # "Arna gnr 307 bnr 112 Nordstrandvegen 36 Skifte vindu"
#     pattern = r'(?:[A-ZÆØÅ][\wæøåéá]+(?:vei|veien|veg|vegen|gate|gaten|lia|åsen|flaten|hagen|neset|torget|kollen)?\s+\d+[A-Z-]*(?:\s+m/?fl)?[,.]?)\s+(.+)$'
#     match = re.search(pattern, sakstittel, re.IGNORECASE)
#     if match:
#         return match.group(1).strip().rstrip('.')

#     # Pattern 4: Comma separator
#     # "167/982/0/0 Støletorget 10, fasadeendring bolig"
#     if ',' in sakstittel:
#         parts = sakstittel.split(',')
#         if len(parts) == 2:
#             tiltak = parts[1].strip().rstrip('.')
#             if tiltak:
#                 return tiltak

#     # Pattern 5: After gnr/bnr when no clear address
#     pattern2 = r'gnr?\s*\d+\s*(?:bnr?|/-)\s*\d+(?:[/,]\d+)*\s+(?:[A-ZÆØÅ][\wæøåéá]+\s+\d+[A-Z]*\s+)?(.+)$'
#     match2 = re.search(pattern2, sakstittel, re.IGNORECASE)
#     if match2:
#         potential_tiltak = match2.group(1).strip().rstrip('.')
#         # Don't return if it looks like it's still part of the address
#         if not re.match(r'^[A-ZÆØÅ][\wæøåéá]+\s+\d+', potential_tiltak):
#             return potential_tiltak

#     return ""    

def _get_journalposter(saksnr: str) -> list:
    jp_list_clean = []
    try:
        jp_response = requests.get(f"{BASE_URL}/dokumenter", params={"saksnr": saksnr})
        if jp_response.status_code != 200:
            print(f"Error({jp_response.status_code}) at {saksnr}")
            return []
        
        n_jp: int = jp_response.json().get('numTotal', 0)
        raw_jp_list: list = jp_response.json().get('items', [])

        if len(raw_jp_list) < 1:
            return []
        
        for jp in raw_jp_list:
            jp_list_clean.append(_assemble_jp(jp))

        return jp_list_clean
    
    except Exception as e:
        print(e)
        return []

def _assemble_jp(jp: dict) -> dict:
    jp_nr: int = jp.get('doknr')
    jp_tittel: str = jp.get('tittel', '')
    ut_inn: str = jp.get('type', '')
    dokdato: int = jp.get('dokdato')
    journaldato: int = jp.get('journaldato')
    dokumenter: list = jp.get('filer', [{'filnavn': ''}])
    
    clean_jp = {
        'jp_nr': jp_nr,
        'jp_tittel': jp_tittel,
        'ut_inn': ut_inn,
        'dokdato': dokdato,
        'journaldato': journaldato,
        'dokumenter': [dokument.get('filnavn', '') for dokument in dokumenter]
    }
    return clean_jp


#This function saves each batch to json
# def save_batch_to_json(data_object: list, year: int) -> None:
#     """
#     Save each batch as a separate JSON file named by year and index.
#     Example: json_data/2005_1.json, json_data/2005_2.json
#     """

    
#     filename = f"json_data/{year}.json"
    
#     with open(filename, 'w', encoding='utf-8') as f:
#         json.dump(data_object, f, indent=2, ensure_ascii=False)
    
#     print(f"Saved: {filename}")


def to_azure_container(bronze_data, year, day_back_from_today=1, ACCOUNT_URL = 'https://storaggen2eaccountprod.blob.core.windows.net', AZURE_CONTAINER_NAME='postlister', DATA_PATH='bronze/bergen/load_type=full') -> None:
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
            blob_name = f"{DATA_PATH}/date={einnsyn_reference_date}/batch_bygg-{year}.jsonl.gz"
            
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



def main() -> None:
    output_dir = "json_data"
    
    # Clean up existing files (optional - remove if you want to keep old data)
    # if os.path.exists(output_dir):
    #     for file in os.listdir(output_dir):
    #         if file.endswith('.json'):
    #             os.remove(os.path.join(output_dir, file))
    #     print(f"Cleaned existing JSON files in {output_dir}")
    
    # Ensure directory exists
    # os.makedirs(output_dir, exist_ok=True)

    years_range = range(2000, 2023)
    total_batches = len(years_range) * 10
    batch_count = 0
    start_time = time.time()

    #Save clean batches to json based on year and their index(1 or 2)
    for year in years_range:
        clean_saker_extended = []
        for i in range(0, 10):
            batch_count += 1
            try:
                #Fetch from API
                raw_saker_data_object: dict = api_fetch(year, i)

                #variables for print
                search_filter = raw_saker_data_object.get('search_filter')
                n_saker = len(raw_saker_data_object.get('saker_data', []))
                elapsed = time.time() - start_time
                avg_per_batch = elapsed / batch_count
                remaining = (total_batches - batch_count) * avg_per_batch
                print('=' * 30)
                print('=' * 30)
                print(f"Batch count: {batch_count}/{total_batches} - {search_filter} ({n_saker} saker) ... "
                      f"(elapsed: {elapsed:.1f}s, est. remaining: {remaining:.1f}s)")

                #Clean and structure data
                clean_saker_object: dict = create_clean_saker_object(raw_saker_data_object)
                clean_saker_extended.extend(clean_saker_object.get('saker'))

            except Exception as e:
                print(f"Error fetching year {year}, index {i}: {e}")
                continue

            #Save to json file
            # save_batch_to_json(clean_saker_extended, year)
        to_azure_container(bronze_data=clean_saker_extended, year=year)
        print('=' * 30)
        print('=' * 30)


if __name__ == '__main__':
    main()

   