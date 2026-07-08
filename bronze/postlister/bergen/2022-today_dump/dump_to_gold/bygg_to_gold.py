import psycopg2
import json
from datetime import datetime
import uuid
from io import StringIO
import os
from dotenv import load_dotenv
from pathlib import Path



def import_test_data():
    with open('../json_data/BYGG_bergen_postliste_2022-2025-11-02.json') as testfile:
        result = json.load(testfile)
        return result

        
def calculate_time(sak):
    last_dokdato = sak.get('journalposter')[0].get('dokdato') if sak.get('journalposter') else None
    first_dokdato = sak.get('journalposter')[-1].get('dokdato') if sak.get('journalposter') else None
    
    forste = datetime.fromtimestamp(first_dokdato/1000).strftime('%Y-%m-%d') if first_dokdato else None
    siste = datetime.fromtimestamp(last_dokdato/1000).strftime('%Y-%m-%d') if last_dokdato else None
    aar = sak.get('saksnr', '').split('-')[1].split('/')[0] if '-' in sak.get('saksnr', '') and '/' in sak.get('saksnr', '') else None


    return {
        "dato_forste": forste,
        "dato_siste": siste,
        "aar": aar
    }


def prepare_sak(sak, sak_datoer):
    sak_uid = str(uuid.uuid4())
    return {
        'uid': sak_uid,
        'data': [
            sak_uid,
            sak.get("tittel"),
            sak.get("sakstype"),
            sak_datoer.get('aar'),
            int(sak.get("saksnr", "0").split("/")[-1]) if sak.get("saksnr") else None,
            sak.get("adresse_navn"),
            sak.get("tiltakshaver"),
            sak_datoer.get('dato_forste'),
            sak_datoer.get('dato_siste'),
            sak.get("status"),
            sak.get('saksurl'),
            sak.get('tiltakstype'),
            sak.get('kommune').upper(),
            sak.get('saksnr')
        ]
    }


def prepare_soker(sak_uid, sokere):
    if not sokere:
        return [], []
    
    soker_data = []
    sak_soker_data = []
    
    for soker in sokere:
        soker_uid = str(uuid.uuid4())
        soker_data.append([soker_uid, soker])
        sak_soker_data.append([sak_uid, soker_uid])
    
    return soker_data, sak_soker_data


def prepare_matrikkel(sak_uid, sak_kommunenavn, sak_kommunenr, sak_matrikkelnr, sak_gnrBnr):
    if not sak_matrikkelnr and not sak_gnrBnr:
        return [], []
    
    matrikkelnr = []
    gnr = []
    bnr = []
    festenr = []
    seksjonsnr = []

    if (not sak_gnrBnr or len(sak_gnrBnr) < 2) and sak_matrikkelnr:
        matrikkelnr.append(sak_matrikkelnr)
        parts = sak_matrikkelnr.split('-')
        if len(parts) == 2:
            remaining = parts[1].split('/')
            if len(remaining) >= 1:
                gnr.append(remaining[0])
            if len(remaining) >= 2:
                bnr.append(remaining[1])
            if len(remaining) >= 3 and remaining[2] != None:
                festenr.append(remaining[2])
            if len(remaining) >= 4 and remaining[3] != None:
                seksjonsnr.append(remaining[3])
    else:
        if sak_gnrBnr:
            
            for gnr_bnr_str in sak_gnrBnr:
                matrikkelnr.append(f'{sak_kommunenr}-{gnr_bnr_str}')
                parts = gnr_bnr_str.split('/')
                if len(parts) >= 1:
                    gnr.append(parts[0])
                if len(parts) >= 2:
                    bnr.append(parts[1])
    
    if not matrikkelnr:
        return [], []
    
    matrikkel_data = []
    sak_matrikkel_data = []
    
    for i in range(len(matrikkelnr)):
        matrikkel_uid = str(uuid.uuid4())
        matrikkel_data.append([
            matrikkel_uid,
            sak_kommunenavn,
            int(sak_kommunenr),
            int(gnr[i]) if gnr[i] else None,
            int(bnr[i]) if bnr[i] else None,
            int(festenr[i]) if i < len(festenr) and festenr[i] else None,
            int(seksjonsnr[i]) if i < len(seksjonsnr) and seksjonsnr[i] else None,
            matrikkelnr[i]
        ])
        sak_matrikkel_data.append([sak_uid, matrikkel_uid])
    
    return matrikkel_data, sak_matrikkel_data


def prepare_journalposter(sak_uid, journalposter):
    jp_data = []
    dokument_data = []
    
    for jp in journalposter:
        jp_uid = str(uuid.uuid4())
        jp_data.append([
            jp_uid,
            sak_uid,
            jp.get("tittel"),
            int(jp.get("jp_nummer")),
            datetime.fromtimestamp(jp.get('dokdato')/1000).strftime('%Y-%m-%d') if jp.get('dokdato') else None,
            jp.get("type"),
            ",".join(jp.get('avsendere')) if jp.get('avsendere') else None,
            ",".join(jp.get('mottakere')) if jp.get('mottakere') else None
        ])
        
        for i, dok in enumerate(jp.get('dokumenter', []), start=1):
            dok_uid = str(uuid.uuid4())
            dokument_data.append([
                dok_uid,
                jp_uid,
                dok.get("filnavn"),
                dok.get('dokumenttype'),
                i,
                dok.get("url") if dok.get('publisert') else None
            ])
    
    return jp_data, dokument_data


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


def batch_copy(cur, saker_data, soker_data, sak_soker_data, matrikkel_data, 
               sak_matrikkel_data, jp_data, dokument_data):
    
    # COPY saker
    copy_from_data(cur, 'postlister_sak', 
                   ['uid', 'tittel', 'type', 'aar', 'nummer', 'adresse_navn', 
                    'tiltakshaver', 'dato_forste', 'dato_siste', 'statuskode', 
                     'url', 'tiltakstype', 'kommune_navn', 'saksnummer'],
                   saker_data)
    
    # COPY soker
    copy_from_data(cur, 'postliste_soker', 
                   ['uid', 'navn'],
                   soker_data)
    
    # COPY sak_soker junction
    copy_from_data(cur, 'postliste_sak_soker', 
                   ['sak_uid', 'soker_uid'],
                   sak_soker_data)
    
    # COPY matrikkel
    copy_from_data(cur, 'postliste_matrikkel', 
                   ['uid', 'kommunenavn', 'kommunenr', 'gardsnr', 
                    'bruksnr', 'festenr', 'seksjonsnr', 'matrikkelnummer'],
                   matrikkel_data)
    
    # COPY sak_matrikkel junction
    copy_from_data(cur, 'postliste_sak_matrikkel', 
                   ['sak_uid', 'matrikkel_uid'],
                   sak_matrikkel_data)
    
    # COPY journalposter
    copy_from_data(cur, 'postlister_journalpost', 
                   ['uid', 'sak_uid', 'tittel', 'nummer', 'dato', 'avsenderkode', 'avsendere', 'mottakere'],
                   jp_data)
    
    # COPY dokumenter
    copy_from_data(cur, 'postlister_dokument', 
                   ['uid', 'journalpost_uid', 'tittel', 'type', 'nummer', 'url'],
                   dokument_data)


def main():

    env_path = Path(__file__).parent / 'running_daily' / '.env'
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
        
        print("Loading test data...")
        saker_liste = import_test_data()
        
        BATCH_SIZE = 20000  # Can be larger with COPY
        total_saker = len(saker_liste)
        
        for batch_start in range(0, total_saker, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_saker)
            batch = saker_liste[batch_start:batch_end]
            
            print(f"\nProcessing batch {batch_start//BATCH_SIZE + 1} ({batch_start+1}-{batch_end}/{total_saker})...")
            
            # Prepare batch data
            all_saker = []
            all_soker = []
            all_sak_soker = []
            all_matrikkel = []
            all_sak_matrikkel = []
            all_jp = []
            all_dokument = []
            
            for sak in batch:
                sak_datoer = calculate_time(sak)
                sak_info = prepare_sak(sak, sak_datoer)
                sak_uid = sak_info['uid']
                all_saker.append(sak_info['data'])
                
                soker_data, sak_soker_data = prepare_soker(sak_uid, sak.get('soker'))
                all_soker.extend(soker_data)
                all_sak_soker.extend(sak_soker_data)
                
                matrikkel_data, sak_matrikkel_data = prepare_matrikkel(
                    sak_uid, 
                    sak.get('kommune').upper(), 
                    sak.get('kommune_nr'), 
                    sak.get('matrikkelnr'), 
                    sak.get('gnrBnr')
                )
                all_matrikkel.extend(matrikkel_data)
                all_sak_matrikkel.extend(sak_matrikkel_data)
                
                jp_data, dokument_data = prepare_journalposter(sak_uid, sak.get('journalposter', []))
                all_jp.extend(jp_data)
                all_dokument.extend(dokument_data)
            
            # COPY batch
            print(f"COPY inserting batch...")
            batch_copy(cur, all_saker, all_soker, all_sak_soker, all_matrikkel, 
                      all_sak_matrikkel, all_jp, all_dokument)
            conn.commit()
            print(f"Batch complete!")
        
        print(f"\nSuccessfully inserted all {total_saker} saker!")
        
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