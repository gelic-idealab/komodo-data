import os
import sys
import time
import json
from numpy.core.defchararray import count
import pandas as pd
import numpy as np
import unittest as ut
from sqlalchemy import create_engine, text, types

# import db configs
from config import *

CAPTURE_FILE_NAME = 'data'

if len(CAPTURES_DIR) <= 0:
    print("No captures directory declared in config.py. Exiting.")
    sys.exit(1)

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

def aggregate_interaction_type(session_id, interaction_type):
    try: 
        # aggregate by interaction types 
        with engine.connect() as conn:
            query = text("""
            DROP TABLE IF EXISTS `komodo`.`aggregate_interaction`;
            """
            )

            conn.execute(query)
            
            query = text("""
            CREATE TABLE `komodo`.`aggregate_interaction` (client_id int not null,
            primary key (client_id),
            interaction_count int not null);
            """
            )

            conn.execute(query)

            query = text("""
            INSERT INTO aggregate_interaction 
            SELECT client_id, count(message) as interaction_count
            FROM data
            WHERE message->'$.interactionType' = :interaction_type and session_id= :session_id
            group by client_id;
            """
            )

            conn.execute(query,{"session_id":session_id, "interaction_type":interaction_type})
    except:
        return False
        
    # uncomment this to test the output 
    # query = text("""
    # SELECT * 
    # FROM aggregate_interaction;
    # """
    # )
    # result = conn.execute(query)
    # count = [r[1] for r in result]
    # sum = np.sum(count)
    # print(sum == 13)
    # sys.exit() 
        
    return True

def aggregate_user(session_id,client_id):
    # aggregate by users
    try:
        with engine.connect() as conn:
            query = text("""
            DROP TABLE IF EXISTS `komodo`.`aggregate_user`;
            """
            )

            conn.execute(query)
            query = text("""
            CREATE TABLE if not exists aggregate_user 
            (
            entity_type int not null,
            primary key (entity_type),
            user_count int not null
            );
            """
            )

            conn.execute(query)
            query = text("""
            INSERT INTO aggregate_user 
            SELECT message->'$.entityType' as entity_type, count(*) as count
            FROM data
            WHERE message->'$.clientId' = :client_id and session_id = :session_id and message->'$.entityType' is not null
            group by entity_type;

            """
            )

            conn.execute(query,{"session_id":session_id, "client_id":client_id})
    except:
        return False

    # uncomment this to test the output 
    # query = text("""
    # SELECT * 
    # FROM aggregate_user;
    # """
    # )
    # result = conn.execute(query)
    # count = [r[1:] for r in result]
    # sum = np.sum(count)
    # print(sum == 50510)
    # sys.exit() 

    return True


def process_file(id, file):
    print("Processing file:", file)
    try:
        # TODO(rob): check if file exists. 
        df = pd.read_json(file, dtype={'capture_id': types.String}) 
        # explicitly set capture_id data type because the "_" character is valid syntax for python ints, and will read it as such and omit the "_". 
        
        with engine.connect() as conn:
            df.to_sql('data', conn, if_exists='append', index=False, dtype={'message': types.JSON}) # explicitly set the message data type, otherwise the insert will fail. 
        print('Done.')        
        return True
        
    except Exception as e:
        print(f"Error processing file: {file}: {e}")
        return False

def mark_as_processed(capture_id, success):
    try:
        if success:
            print("Successfully processed", capture_id)
            processed = int(time.time())
        else:
            print("Failed to process capture:", capture_id)
            processed = 0
        
        query = text("UPDATE captures SET processed = :p WHERE capture_id = :ci")
        with engine.connect() as conn:
            result = conn.execute(query, {'p': processed, 'ci': capture_id})

    except Exception as e:
        print(e)

        
if __name__ == "__main__":

    # infinite poll & process
    while True:
        # will be moving to unit test files
        # user_flag = aggregate_user(126,5) 
        # if user_flag:
        #     print("User aggregation succeeded!")
        # else:
        #     print("User aggregation failed!")

        # interaction_flag = aggregate_interaction_type(126, 1) 
        # if interaction_flag:
        #     print("Interaction aggregation succeeded!")
        # else:
        #     print("Interaction aggregation failed!")

        ready = check_for_unprocessed_captures()
        if len(ready) > 0:
            print("Ready to process:", ready)
            for id in ready:
                session = id.split("_")[0]
                capture = id.split("_")[1]
                file = os.path.join(CAPTURES_DIR, session, capture, CAPTURE_FILE_NAME)
                success = process_file(id, file)
                mark_as_processed(id, success)

        else:
            print('Nothing to process', time.strftime("%H:%M:%S", time.localtime()))
            # rinse & repeat
            time.sleep(10)



