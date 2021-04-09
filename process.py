import os
import sys
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# import db configs
from config import *

if len(CAPTURES_DIR) <= 0:
    print("No captures directory declared in config.py. Exiting.")
    sys.exit(1)

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
    # get ids of unprocessed captures
    with engine.connect() as conn:
        query = """
                SELECT capture_id
                FROM captures
                WHERE end IS NOT NULL AND processed IS NULL
                ORDER BY start
                """

        result = conn.execute(query)
        ready = list([r[0] for r in result])

    return ready

def process_file(id, file):
    print("Processing file:", file)
    try:
        with open(file, 'rb') as f:
            if os.path.basename(file) == 'int':
                npdata = np.fromfile(f, dtype=np.int32)
                df = pd.DataFrame(npdata.reshape(-1,7))
                df.columns = INTERACTION_TABLE_COLUMNS
                df['capture_id'] = id
                with engine.connect() as conn:
                    df.to_sql('interactions', conn, if_exists='append', index=False)

            if os.path.basename(file) == 'pos':
                npdata = np.fromfile(f, dtype=np.float32)
                df = pd.DataFrame(npdata.reshape(-1,14))
                df.columns = POSITION_TABLE_COLUMNS
                with engine.connect() as conn:
                    df.to_sql('positions', conn, if_exists='append', index=False)
        
        return True
        
        print('Done.')
    except Exception as e:
        print(e)
        return False

def mark_as_processed(capture_id, success):
    try:
        print("Successfully processed", capture_id, success)
        if success:
            processed = int(time.time())
        else:
            processed = 0
        
        query = text("UPDATE captures SET processed = :p WHERE capture_id = :ci")

        with engine.connect() as conn:
            result = conn.execute(query, {'p': processed, 'ci': capture_id})

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
            count(*) as count
        FROM komodo.interactions i
        JOIN komodo.captures c ON i.capture_id = c.capture_id
        GROUP BY capture_id, c.start, session_id, client_id, source_id, target_id
        ORDER BY capture_id, c.start, session_id, client_id, source_id, target_id;
        """
        result = conn.execute(query)
        return
        
if __name__ == "__main__":

    # infinite poll & process
    while True:
        ready = check_for_unprocessed_captures()
        if len(ready) > 0:
            print("Ready to process:", ready)
            for id in ready:
                success = True
                session = id.split("_")[0]
                capture = id.split("_")[1]
                for filetype in CAPTURE_FILE_TYPES:
                    file = os.path.join(CAPTURES_DIR, session, capture, filetype)
                    success = success and process_file(id, file)
                mark_as_processed(id, success)

            # aggregate with new interaction data and insert into table for portal
            # agg_interactions()

        else:
            print('Nothing to process', time.strftime("%H:%M:%S", time.localtime()))
            # rinse & repeat
            time.sleep(10)



