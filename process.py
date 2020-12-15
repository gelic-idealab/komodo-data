import os
import sys
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# import db configs
from config import *

CAPTURES_DIR='./captures'
CAPTURE_FILE_TYPES = ['int', 'pos']

INTERACTION_TABLE_COLUMNS = ['seq', 'session_id', 'client_id', 'source_id', 'target_id', 'interaction_type', 'global_seq']
POSITION_TABLE_COLUMNS = ['seq', 'session_id', 'client_id', 'entity_id', 'entity_type', 'scale', 'rotx', 'roty', 'rotz', 'rotw', 'posx', 'posy', 'posz', 'global_seq']

if not os.path.exists(CAPTURES_DIR):
    os.mkdir(CAPTURES_DIR)

# create db connection 
try:
    # dialect+driver://username:password@host:port/database
    # TODO(rob): use config file https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.engine_from_config
    connection_string = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_string)

except Exception as e:
    print(e)
    sys.exit(1)

def check_for_unprocessed_captures():
    # get ids of existing capture files
    existing = list(set([f.split('.')[0] for f in os.listdir(CAPTURES_DIR)]))

    # get ids of captures already processed
    with engine.connect() as conn:
        query = """
                SELECT capture_id
                FROM captures
                """

        result = conn.execute(query)
        processed = list(set([r[0] for r in result]))

    unprocessed = []
    for id in existing:
        if id not in processed:
            unprocessed.append(id)

    return unprocessed

def process_file(id, file):
    print("processing file:", file)
    try:
        with open(file, 'rb') as f:
            if file.endswith('.int'):
                npdata = np.fromfile(f, dtype=np.int32)
                df = pd.DataFrame(npdata.reshape(-1,7))
                df.columns = INTERACTION_TABLE_COLUMNS
                df['capture_id'] = id
                with engine.connect() as conn:
                    df.to_sql('interactions', conn, if_exists='append', index=False)

            if file.endswith('.pos'):
                npdata = np.fromfile(f, dtype=np.float32)
                df = pd.DataFrame(npdata.reshape(-1,14))
                df.columns = POSITION_TABLE_COLUMNS
                with engine.connect() as conn:
                    df.to_sql('positions', conn, if_exists='append', index=False)
        
        print('Done.')
    except Exception as e:
        print(e)

def mark_as_processed(capture_id):
    try:
        print("marking as processed:", capture_id)
        session_id = int(capture_id.split('_')[0])
        start = int(capture_id.split('_')[1])
        processed = int(time.time())
        query = f"INSERT INTO captures (capture_id, session_id, start, processed) VALUES('{capture_id}', {session_id}, {start}, {processed})"

        with engine.connect() as conn:
            result = conn.execute(query)

    except Exception as e:
        print(e)

def agg_interactions():
    with engine.connect() as conn:
        query = """
        INSERT INTO KP_Interactions
        SELECT
            i.capture_id,
            c.start as capture_start,
            i.session_id,
            client_id,
            source_id,
            target_id, 
            it.type,
            count(*) as count
        FROM komodo.interactions i
        JOIN komodo.interaction_types it ON i.interaction_type = it.id
        JOIN komodo.captures c ON i.capture_id = c.capture_id
        GROUP BY capture_id, c.start, session_id, client_id, source_id, target_id, it.type
        ORDER BY capture_id, c.start, session_id, client_id, source_id, target_id, it.type;
        """
        result = conn.execute(query)
        return
        
if __name__ == "__main__":
    # infinite poll & process
    while True:
        unprocessed = check_for_unprocessed_captures() # ids are like 1_1595630966721
        if len(unprocessed) > 0:
            print('captures to process:', unprocessed)
            for id in unprocessed:
                for t in CAPTURE_FILE_TYPES:
                    file = os.path.join(CAPTURES_DIR, '.'.join([id, t]))
                    process_file(id, file)
                mark_as_processed(id)

            # aggregate with new interaction data and insert into table for portal
            agg_interactions()

        else:
            print('nothing to process', time.strftime("%H:%M:%S", time.localtime()))
            # rinse & repeat
            time.sleep(10)



