import psycopg2
import json
from datetime import datetime
import uuid
from io import StringIO
import os
from dotenv import load_dotenv
from pathlib import Path
import re


def import_json_data(aar: int) -> dict:
    with open(f'json_data/{aar}.json') as json_file:
        result: list = json.load(json_file)
        return result    

#Prepare saker for the sak table
def assemble_tables(raw_saker: list, year: int) -> dict:
    clean_saker = []
    clean_sokere = []
    clean_sak_soker = []
    clean_matrikkeldata = []
    clean_sak_matrikkel = []
    clean_jp = []
    clean_dokument = []

    if len(raw_saker) < 1:
        return clean_saker
    
    for raw_sak in raw_saker:
        if raw_sak.get('saksnr').startswith(('BYGG-', 'HENV-', 'ULOV-', 'TILSYN-')):
            continue

        else:
            clean_sak: dict = _assemble_sak(raw_sak, year)
            clean_saker.append(clean_sak.get('data'))

            clean_soker: dict  = _assemble_soker(clean_sak.get('uid'), raw_sak.get('soker'))
            clean_sokere.extend(clean_soker.get('soker_data'))
            clean_sak_soker.extend(clean_soker.get('sak_soker_data'))

            clean_matrikkel: dict = _assemble_matrikkeldata(
                clean_sak.get('uid'),
                raw_sak
            )
            clean_matrikkeldata.extend(clean_matrikkel.get('matrikkel_data'))
            clean_sak_matrikkel.extend(clean_matrikkel.get('sak_matrikkel_data'))

            jp_and_dokumenter: dict = _assemble_journalposter(clean_sak.get('uid'), raw_sak.get('journalposter'))
            clean_jp.extend(jp_and_dokumenter.get('jp_data'))
            clean_dokument.extend(jp_and_dokumenter.get('dokument_data'))

    return {'clean_saker': clean_saker,
            'clean_sokere': clean_sokere,
            'clean_sak_soker': clean_sak_soker,
            'clean_matrikkeldata': clean_matrikkeldata,
            'clean_sak_matrikkel': clean_sak_matrikkel,
            'clean_jp': clean_jp,
            'clean_dokument': clean_dokument}
        
def _assemble_sak(raw_sak: dict, year: int) -> dict:
    sak_uid = str(uuid.uuid4())
    sak_dates: dict = _calculate_sak_dates(raw_sak)
    format_saksnr_year = raw_sak.get('saksnr')[:4]
    format_saksnr_number = raw_sak.get('saksnr')[4:]
    formatted_saksnr = f"BYGG-{format_saksnr_year}/{format_saksnr_number}"

    
    return {
        'uid': sak_uid,
        'data': [
            sak_uid,
            _normalize_spaces(raw_sak.get("tittel")),
            raw_sak.get("sakstype"),
            year,
            format_saksnr_number,
            raw_sak.get('adresse_navn')[0],
            raw_sak.get("tiltakshaver"),
            sak_dates.get('dato_forste'),
            sak_dates.get('dato_siste'),
            raw_sak.get("status"),
            raw_sak.get('saksurl'),
            '',
            raw_sak.get('kommune').upper(),
            formatted_saksnr
        ]
    }

def _calculate_sak_dates(sak: dict) -> dict:
    last_dokdato = sak.get('journalposter')[0].get('dokdato') if sak.get('journalposter') else None
    first_dokdato = sak.get('journalposter')[-1].get('dokdato') if sak.get('journalposter') else None

    forste = datetime.fromtimestamp(first_dokdato/1000).strftime('%Y-%m-%d') if first_dokdato else None
    siste = datetime.fromtimestamp(last_dokdato/1000).strftime('%Y-%m-%d') if last_dokdato else None

    return {
        'dato_forste': forste,
        'dato_siste': siste
    }

def _normalize_spaces(text: str) -> str:
    """
    Replace multiple consecutive spaces (2+) with a single space.
    
    Example:
        "Åsane    209 -3    Øvre Våganeset 30.   Nybygg terrasssehus"
        -> "Åsane 209 -3 Øvre Våganeset 30. Nybygg terrasssehus"
    """
    if not text:
        return ""
    
    # Replace 2 or more spaces with a single space
    normalized = re.sub(r'\s{2,}', ' ', text)
    
    return normalized.strip()

def _assemble_soker(sak_uid: str, sokere: list) -> dict:
    
    if not sokere or len(sokere) < 1:
        return {'soker_data': [], 'sak_soker_data': []}
    
    soker_data = []
    sak_soker_data = []

    for soker in sokere:
        soker_uid = str(uuid.uuid4())
        soker_data.append([soker_uid, soker])
        sak_soker_data.append([sak_uid, soker_uid])

    return {'soker_data': soker_data, 'sak_soker_data': sak_soker_data}

def _assemble_matrikkeldata(sak_uid: str, sak: dict) -> dict:
    
    if not sak.get('gnr_bnr') or len(sak.get('gnr_bnr')) < 1:
        return {'matrikkel_data': [], 'sak_matrikkel_data': []}
    
    matrikkel_data = []
    sak_matrikkel_data = []

    for gnr_bnr in sak.get('gnr_bnr'):
        matrikkelnummer = f'{sak.get('kommune_nr')}-{gnr_bnr}'
        matrikkel_uid = str(uuid.uuid4())
        matrikkel_data.append([
            matrikkel_uid,
            sak.get('kommune'),
            sak.get('kommune_nr'),
            gnr_bnr.split('/')[0],
            gnr_bnr.split('/')[-1],
            None,
            None,
            matrikkelnummer])
        sak_matrikkel_data.append([sak_uid, matrikkel_uid])
    return{'matrikkel_data': matrikkel_data, 'sak_matrikkel_data': sak_matrikkel_data}

def _assemble_journalposter(sak_uid: str, journalposter: list) -> dict:
    
    if not journalposter or len(journalposter) < 1:
        return {'jp_data': [], 'dokument_data': []}
    
    jp_data = []
    dokument_data = []

    for jp in journalposter:
        jp_uid = str(uuid.uuid4())

        jp_data.append([
            jp_uid,
            sak_uid,
            jp.get('jp_tittel'),
            jp.get('jp_nr'),
            datetime.fromtimestamp(jp.get('dokdato')/1000).strftime('%Y-%m-%d') if jp.get('dokdato') else None,
            jp.get('ut_inn'),
            None, #dont have avsendere
            None #dont have mottakere
        ])

        for i, dok in enumerate(jp.get('dokumenter', []), start=1):
            dok_uid = str(uuid.uuid4())
            dokument_data.append([
                dok_uid,
                jp_uid,
                dok, #filnavn
               '', #dont have dokumenttype
                i,
                None #dont have url
            ])

    return {'jp_data': jp_data, 'dokument_data': dokument_data}


def batch_copy(cur, clean_tables: dict) -> None:
    
    # COPY saker
    copy_from_data(cur, 'postlister_sak', 
                   ['uid', 'tittel', 'type', 'aar', 'nummer', 'adresse_navn', 
                    'tiltakshaver', 'dato_forste', 'dato_siste', 'statuskode', 
                     'url', 'tiltakstype', 'kommune_navn', 'saksnummer'],
                   clean_tables.get('clean_saker'))
    
    # COPY soker
    copy_from_data(cur, 'postliste_soker', 
                   ['uid', 'navn'],
                   clean_tables.get('clean_sokere'))
    
    # COPY sak_soker junction
    copy_from_data(cur, 'postliste_sak_soker', 
                   ['sak_uid', 'soker_uid'],
                   clean_tables.get('clean_sak_soker'))
    
    # COPY matrikkel
    copy_from_data(cur, 'postliste_matrikkel', 
                   ['uid', 'kommunenavn', 'kommunenr', 'gardsnr', 
                    'bruksnr', 'festenr', 'seksjonsnr', 'matrikkelnummer'],
                   clean_tables.get('clean_matrikkeldata'))
    
    # COPY sak_matrikkel junction
    copy_from_data(cur, 'postliste_sak_matrikkel', 
                   ['sak_uid', 'matrikkel_uid'],
                   clean_tables.get('clean_sak_matrikkel'))
    
    # COPY journalposter
    copy_from_data(cur, 'postlister_journalpost', 
                   ['uid', 'sak_uid', 'tittel', 'nummer', 'dato', 'avsenderkode', 'avsendere', 'mottakere'],
                   clean_tables.get('clean_jp'))
    
    # COPY dokumenter
    copy_from_data(cur, 'postlister_dokument', 
                   ['uid', 'journalpost_uid', 'tittel', 'type', 'nummer', 'url'],
                   clean_tables.get('clean_dokument'))

def copy_from_data(cur, table, columns, data):
    """Use COPY to bulk insert data"""
    if not data:
        return
    
    # Create CSV in memory
    buffer = StringIO()
    for row in data:
        # Convert None to \N (PostgreSQL NULL in COPY)
        csv_row = '\t'.join(
            '\\N' if val is None else str(val).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            for val in row
        )
        buffer.write(csv_row + '\n')
    
    buffer.seek(0)
    cur.copy_from(buffer, table, columns=columns, null='\\N')


def main():

    env_path = '../gold/running_daily/.env'
    load_dotenv(dotenv_path=env_path)

    DB_CONFIG = {
        'hostname': os.getenv('DB_HOSTNAME'),
        'database': os.getenv('DB_DATABASE'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'options': f"-c search_path={os.getenv('DB_SEARCH_PATH', 'public')}"
    }

    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG.get('hostname'),
            dbname=DB_CONFIG.get('database'),
            user=DB_CONFIG.get('user'),
            password=DB_CONFIG.get('password'),
            port= DB_CONFIG.get('port'), 
            options=DB_CONFIG.get('options')
        )
    
        cur = conn.cursor()
        
        
        #The json file contains a list of objects from the initial fetch
        #where each fetch is contained in one of these objects
        #The db insert will be done in the same way, with each of these objects treated as a separate batch
        for year in range(2000, 2023):
            
            try:
                saker_from_json: list = import_json_data(year)
                

                if len(saker_from_json) < 1:
                    continue
                else:
                    clean_tables: dict = assemble_tables(saker_from_json, year)

                    print(f"COPY inserting batch...")
                    batch_copy(cur, clean_tables)
                    conn.commit()
                    print(f"Batch complete!")

            except Exception as e:
                print(f"Error fetching year {year}: {e}")
                continue

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    main()