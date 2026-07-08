import psycopg2
import json
from datetime import datetime, date
import uuid
from io import StringIO
from uuid import UUID
import re


def import_data(): #fetches the json data from the file at ../Bergen which matches the current date
    with open(f'../bergen_postliste_{date.today()}.json') as testfile:
        result = json.load(testfile)
        return result

def calculate_time(sak):
    first_dokdato = sak.get('journalposter')[-1].get('dokdato') if sak.get('journalposter') else None
    last_dokdato = sak.get('journalposter')[0].get('dokdato') if sak.get('journalposter') else None
    
    forste = datetime.fromtimestamp(first_dokdato/1000).strftime('%Y-%m-%d') if first_dokdato else None
    siste = datetime.fromtimestamp(last_dokdato/1000).strftime('%Y-%m-%d') if last_dokdato else None
    aar = sak.get('saksnr', '').split('-')[1].split('/')[0] if '-' in sak.get('saksnr', '') and '/' in sak.get('saksnr', '') else None

    return {
        "dato_forste": forste,
        "dato_siste": siste,
        "aar": aar
    }

def prepare_sak(sak, sak_datoer, kilde_id):
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
            sak.get('saksnr'),
            kilde_id
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

def prepare_matrikkel(sak_uid, sak_kommunenavn, sak_kommunenr, sak_matrikkelnr, sak_gnrBnr) -> dict:
    if not sak_matrikkelnr and not sak_gnrBnr:
        return {}
    
    matrikkelnr = []
    gnr = []
    bnr = []
    festenr = []
    seksjonsnr = []
    #Use the matrikkelnr assembled from the title during the fetch if applicable, as this might contain  festenr and seksjonsnr as well
    if (not sak_gnrBnr or len(sak_gnrBnr) < 2) and sak_matrikkelnr:
        
        parts = sak_matrikkelnr.split('-')
        if len(parts) == 2:
            remaining = parts[1].split('/')
            #append all values or 0 if it is not available
            gnr_temp = remaining[0] if len(remaining) >= 1 else "0"

            bnr_temp = remaining[1] if len(remaining) >= 2 else "0"

            festenr_temp = remaining[2] if len(remaining) >= 3 else "0"

            seksjonsnr_temp = remaining[3] if len(remaining) >= 4 else "0"

            matrikkelnr_str = f"{sak_kommunenr}-{gnr_temp}/{bnr_temp}/{festenr_temp}/{seksjonsnr_temp}"
            #Avoid creating duplicate matrikkelnumbers for the same case, which would create one sak pointing to duplicate matrikkel uids
            if matrikkelnr_str not in matrikkelnr:
                matrikkelnr.append(matrikkelnr_str)
                gnr.append(gnr_temp)
                bnr.append(bnr_temp)
                festenr.append(festenr_temp)
                seksjonsnr.append(seksjonsnr_temp)
    else:
        if sak_gnrBnr:
            
            for gnr_bnr_str in sak_gnrBnr:
                
                parts = gnr_bnr_str.split('/')
                
                gnr_temp = (parts[0]) if len(parts) >= 1 else "0"

                bnr_temp = (parts[1]) if len(parts) >= 2 else "0"

                festenr_temp = "0"

                seksjonsnr_temp = "0"
                matrikkelnr_str = f"{sak_kommunenr}-{gnr_temp}/{bnr_temp}/{festenr_temp}/{seksjonsnr_temp}"
                if matrikkelnr_str not in matrikkelnr:
                    matrikkelnr.append(matrikkelnr_str)
                    gnr.append(gnr_temp)
                    bnr.append(bnr_temp)
                    festenr.append(festenr_temp)
                    seksjonsnr.append(seksjonsnr_temp)
    
    matrikkel_data = []
    
    
    for i in range(len(matrikkelnr)):
        matrikkel_uid = str(uuid.uuid4())
        matrikkel_data.append([
            matrikkel_uid,
            sak_kommunenavn,
            int(sak_kommunenr),
            int(gnr[i]) if gnr[i] else 0,
            int(bnr[i]) if bnr[i] else 0,
            int(festenr[i]) if i < len(festenr) and festenr[i] else 0,
            int(seksjonsnr[i]) if i < len(seksjonsnr) and seksjonsnr[i] else 0,
            matrikkelnr[i]
        ])
        
    
    return {'matrikkel_data': matrikkel_data, 'sak_uid': sak_uid}


def handle_duplicate_matrikkelnummer(temp_matrikkler: list[dict], cur) -> tuple[list, list]:
    """
    Checks for duplicate matrikkelnummer both in the db and in the fetch
    and build the matrikkel and sak_matrikkel table data
    """
    if not temp_matrikkler:
        return ([], [])

    matrikkel_info = []
    # Pattern: kommunenr-gnr/bnr/festenr/seksjonsnr (e.g., '4601-141/188/0/0')
    matrikkel_pattern = re.compile(r'^\d+-\d+/\d+/\d+/\d+$')

    for sak in temp_matrikkler:
        matrikkel_data = sak.get('matrikkel_data')
        if not matrikkel_data:
            continue

        for matrikkel in matrikkel_data:
            matrikkel_value = matrikkel[-1]
            # Only append if it matches the expected format
            if matrikkel_pattern.match(matrikkel_value):
                matrikkel_info.append(matrikkel_value)
    
    if len(matrikkel_info) < 1:
        return ([], [])

    # Create placeholders for single column IN query
    placeholders = ','.join(['(%s)'] * len(matrikkel_info))

    #Find existing matrikkelnr and their uids
    cur.execute(f"""
        SELECT uid, matrikkelnummer
        FROM postliste_matrikkel
        WHERE matrikkelnummer IN ({placeholders})
    """, matrikkel_info)

    uid_map = {}
    for uid, matrikkelnr in cur.fetchall():
        uid_map[matrikkelnr] = uid

    print('')
    print('========================================================================')
    print('========================================================================')
    print('MATRIKKEL DUPLICATE CHECK')
    print('========================================================================')
    print(f"Total matrikkel entries in batch: {len(matrikkel_info)}")
    print(f"Unique matrikkel strings in batch: {len(set(matrikkel_info))}")
    print(f"Already exists in database: {len(uid_map)}")

    #Separate existing and new matrikkeldata

    
    sak_matrikkel = []
    new_matrikkel = []
    

    # Track matrikkel from current batch to avoid duplicates
    seen_in_batch = {}  # {matrikkelnr: uid}

    for sak in temp_matrikkler:
        sak_uid = sak.get('sak_uid')
        matrikkel_data = sak.get('matrikkel_data')
        if not matrikkel_data:
            continue
        for matrikkel in matrikkel_data:
            matrikkel_value = matrikkel[-1]
            
            if matrikkel_value in uid_map:
                #Matrikkel exists in the db
                sak_matrikkel.append([sak_uid, uid_map[matrikkel_value]]) #corresponds to the sak, matrikkel uids

            elif matrikkel_value in seen_in_batch:
                # Duplicate in current batch, reuse the UID
                sak_matrikkel.append([sak_uid, seen_in_batch[matrikkel_value]])

            else:
                #New matrikkelnr
                matrikkel_uid = matrikkel[0]
                new_matrikkel.append(matrikkel)
                sak_matrikkel.append([sak_uid, matrikkel_uid])
                seen_in_batch[matrikkel_value] = matrikkel_uid

    print(f"Duplicates found in current batch: {len(matrikkel_info) - len(seen_in_batch) - len(uid_map)}")
    print(f"New unique matrikkel to insert: {len(new_matrikkel)}")
    print(f"Total sak-matrikkel junction entries: {len(sak_matrikkel)}")
    print('========================================================================')
    print('========================================================================')
    print('')

    return (new_matrikkel, sak_matrikkel)
    


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
                    'url', 'tiltakstype', 'kommune_navn', 'saksnummer', 'kilde'],
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

# json data looks like e.g "saksnr": "BYGG-2025/16577"
#to assess if the case already exist in the database we use kommunenr + year(here 2025) + end of saksnr (here 16577)
def locate_existing_sak(all_data, cur, kilde_id):
    
    if not all_data:
        return all_data
    
    # Extract kommune, year and number tuples
    cases_info = {}  # {saksnr: (kommune, year, number)}
    
    for sak in all_data:
        saksnr = sak.get('saksnr')
        kommune = sak.get('kommune').upper()
        
        if saksnr and kommune and '/' in saksnr:
        
            cases_info[saksnr] = (kommune, saksnr, kilde_id)
    
    if not cases_info:
        return all_data
    
    # Get all existing kommune+year+number combos in ONE query
    placeholders = ','.join(['(%s,%s,%s)'] * len(cases_info))
    values = [val for pair in cases_info.values() for val in pair]
    

    cur.execute(f"""
        SELECT kommune_navn, saksnummer, uid 
        FROM postlister_sak 
        WHERE (kommune_navn, saksnummer, kilde) IN ({placeholders})
    """, values)
    
    
    uid_map = {}
    for kom, sr, uid in cur.fetchall():
        uid_map[(kom, sr)] = uid
    
    existing_saksnr = {
        saksnr for saksnr, (kom, sr, _) in cases_info.items() 
        if (kom, sr) in uid_map
    }

    existing_saker = []
    for sak in all_data: 
        saksnr = sak.get('saksnr')
        if saksnr in existing_saksnr:
            sak_copy = sak.copy()
            kom, sr, _ = cases_info[saksnr]
            sak_copy['db_uid'] = str(uid_map[(kom, sr)])
            existing_saker.append(sak_copy)
    
    new_saker = [sak for sak in all_data if sak.get('saksnr') not in existing_saksnr]
    
    return existing_saker, new_saker

#Updates existing sak and inserts any new journalposts
def update_existing_sak(existing_saker, cur):
    count = 1
    for sak in existing_saker:
        print('')
        print('========================================================================')
        print('========================================================================')
        print(f"({count}/{len(existing_saker)}) Updating sak: \"{sak.get('saksnr')}\" -- \"{sak.get('tittel')}\"")
        print(f"{sak.get('saksurl')}")
        
        count += 1
        update_sak_script = """ 
        UPDATE postlister_sak 
        SET dato_siste = %s, 
            statuskode = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE uid = %s
        """
        sak_time = calculate_time(sak)
        
        cur.execute(
            update_sak_script,
            (
                sak_time.get('dato_siste'),
                sak.get("status"),
                sak.get('db_uid')
            )
        )

        jp_info = {}
        sak_uid = sak.get('db_uid')
        
        for jp in sak.get('journalposter'):
            jp_info[jp.get('jp_nummer')] = (jp.get('jp_nummer'), sak_uid)
        
        if not jp_info:
            continue
            
        placeholders = ','.join(['(%s,%s)'] * len(jp_info))
        values = [val for pair in jp_info.values() for val in pair]
        
        cur.execute(f"""
            SELECT nummer, sak_uid 
            FROM postlister_journalpost 
            WHERE (nummer, sak_uid) IN ({placeholders})
        """, values)
        
        existing_jp = set(cur.fetchall())
        
        new_journalposter = [
            jp for jp in sak.get('journalposter')
            if (jp.get('jp_nummer'), sak_uid) not in existing_jp
        ]
        
        print(f"Added {len(new_journalposter)} new jp to {sak.get('tittel')}")
        if new_journalposter and len(new_journalposter) > 0:
            for njp in new_journalposter:
                print(f"    -Added jp: {njp.get('tittel')} with the following documents:")
                if njp.get('dokumenter') and len(njp.get('dokumenter')) > 0:
                    for dok in njp.get('dokumenter'):
                        print(f"        -{dok.get('filnavn')}")
                        if  dok.get('publisert') and dok.get('url'):
                            print(f"        -{dok.get('url')}")

        sak['journalposter'] = new_journalposter

        insert_jp_script = """
            INSERT INTO postlister_journalpost (uid, sak_uid, tittel, nummer, dato, avsenderkode, avsendere, mottakere)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        for jp in new_journalposter:
            jp_uid = str(uuid.uuid4())
            cur.execute(
                insert_jp_script, (
                    jp_uid,
                    sak_uid,
                    jp.get("tittel"),
                    int(jp.get("jp_nummer")),
                    datetime.fromtimestamp(jp.get('dokdato')/1000).strftime('%Y-%m-%d') if jp.get('dokdato') else None,
                    jp.get("type"),
                    ",".join(jp.get('avsendere')) if jp.get('avsendere') else None,
                    ",".join(jp.get('mottakere')) if jp.get('mottakere') else None
                )
            )
            insert_dokument(jp_uid, jp, cur)

#inserts all documents in new journalposts
def insert_dokument(jp_uid, jp, cur): 
    insert_dokument_script = """
        INSERT INTO postlister_dokument (uid, journalpost_uid, tittel, type, nummer, url)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    for i, dok in enumerate(jp.get('dokumenter', []), start=1):
        dok_uid = str(uuid.uuid4())  
        cur.execute(
            insert_dokument_script, (
                dok_uid,
                jp_uid,
                dok.get("filnavn"),
                dok.get('dokumenttype') if dok.get('dokumenttype') else None,  
                i,  
                dok.get("url") if dok.get('publisert') else None
            )
        )




def handle_db_operations(all_data, DB_CONFIG, kilde_id):

    cur = None
    conn = None

    try:
        conn = psycopg2.connect(
            host=DB_CONFIG.get('hostname', 'localhost'),
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port'],
            options=DB_CONFIG['options']
        )
        cur = conn.cursor()    
        # all_data = import_data() #if import manually 
        
        existing_saker, new_saker = locate_existing_sak(all_data, cur, kilde_id)
        print('')
        print('')
        print('========================================================================')
        print('========================================================================')
        print(f"Existing saker to update: {len(existing_saker)}")
        print('========================================================================')
        print('========================================================================')
        print('')
        print('')

        update_existing_sak(existing_saker, cur) if len(existing_saker) > 0 else print('No existing saker required updating')
        conn.commit()

        print('========================================================================')
        print('========================================================================')
        print(f"\nSuccessfully updated all {len(existing_saker)} existing saker!")
        print('========================================================================')
        print('========================================================================')

        def _insert_new_saker(new_saker):
            BATCH_SIZE = 20000  # Can be larger with COPY
            total_saker = len(new_saker)        
            for batch_start in range(0, total_saker, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_saker)
                batch = new_saker[batch_start:batch_end]
                
                print(f"\nProcessing batch {batch_start//BATCH_SIZE + 1} ({batch_start+1}-{batch_end}/{total_saker})...")
                
                # Prepare batch data
                all_saker = []
                all_soker = []
                all_sak_soker = []
                temp_matrikkel = [] #contains the matrikkel and sak_matrikkel data befor dupe check
                all_jp = []
                all_dokument = []
                
                for sak in batch:
                    sak_datoer = calculate_time(sak)
                    sak_info = prepare_sak(sak, sak_datoer, kilde_id)
                    sak_uid = sak_info['uid']
                    all_saker.append(sak_info['data'])
                    
                    soker_data, sak_soker_data = prepare_soker(sak_uid, sak.get('soker'))
                    all_soker.extend(soker_data)
                    all_sak_soker.extend(sak_soker_data)
                    
                    matrikkel_data = prepare_matrikkel(
                        sak_uid, 
                        sak.get('kommune').upper(), 
                        sak.get('kommune_nr'), 
                        sak.get('matrikkelnr'), 
                        sak.get('gnrBnr')
                    )
                    temp_matrikkel.append(matrikkel_data)
                    
                    
                    jp_data, dokument_data = prepare_journalposter(sak_uid, sak.get('journalposter', []))
                    all_jp.extend(jp_data)
                    all_dokument.extend(dokument_data)
                    print('========================================================================')
                    print('========================================================================')
                    print(f"Inserting new sak: \"{sak.get('saksnr')}\" -- \"{sak.get('tittel')}\"")
                    print(sak.get('saksurl'))
                    if sak.get('journalposter'):
                        print(f"Containing {len(sak.get('journalposter'))} journalposter")
                    
                clean_matrikkel_and_sak: tuple[list, list] = handle_duplicate_matrikkelnummer(temp_matrikkel, cur)
                all_matrikkel = clean_matrikkel_and_sak[0]
                all_sak_matrikkel = clean_matrikkel_and_sak[1]
                # COPY batch
                print(f"COPY inserting batch...")
                batch_copy(cur, all_saker, all_soker, all_sak_soker, all_matrikkel, 
                        all_sak_matrikkel, all_jp, all_dokument)
                conn.commit()
                print(f"Batch complete!")
            
            print('========================================================================')
            print('========================================================================')
            print(f"\nSuccessfully inserted all {total_saker} new saker!")        

        print('')
        print('')
        print('========================================================================')
        print('========================================================================')
        print(f'New saker to insert: {len(new_saker)}')
        print('========================================================================')
        print('========================================================================')

        _insert_new_saker(new_saker) if len(new_saker) > 0 else print('No new saker')

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

# if __name__ == '__main__':
#     handle_db_operations()