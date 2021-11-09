import os
import os.path
import sys
import time
import json
from numpy.core.defchararray import count, index
import pandas as pd
import numpy as np
import unittest as ut
from sqlalchemy import create_engine, text, types
from sqlalchemy.sql.expression import null

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

def aggregate_interaction_type(session_id, interaction_type,request_id):
    try: 
        # aggregate by interaction types 
        with engine.connect() as conn:
            with conn.begin(): 
                query = text("""
                DROP TABLE IF EXISTS `aggregate_interaction`;
                """
                )

                conn.execute(query)

            with conn.begin(): 
                query = text("""
                CREATE TABLE `aggregate_interaction` 
                (
                client_id int not null,
                primary key (client_id),
                interaction_count int not null);
                """
                )

                conn.execute(query)

            with conn.begin(): 
                query = text("""
                INSERT INTO aggregate_interaction 
                SELECT client_id, count(message) as interaction_count
                FROM data
                WHERE message->'$.interactionType' = :interaction_type and session_id= :session_id
                group by client_id;
                """
                )

                conn.execute(query,{"session_id":session_id, "interaction_type":interaction_type})

            with conn.begin():
                query = text("""
                SELECT * 
                FROM aggregate_interaction;
                """
                )
                result = conn.execute(query)
                count = [r[0:] for r in result]
                print(count)
                df = pd.DataFrame(count, columns = ['client_id','interaction_count'])
                filename = str("aggregate_interaction_" + time.strftime('%Y-%m-%d %H-%S') + ".csv")
                df.to_csv(filename,index=False)
                print("aggregate_interaction csv file downloaded!") 

                file_path = os.path.abspath(filename)
                update_data_request(request_id, 1, file_path)
                return True

    except Exception as e:
        print(e)
        return False
    

def aggregate_user(session_id,client_id,request_id):
    # aggregate by users
    try:
        with engine.connect()as conn:
            with conn.begin(): 
                query = text("""
                DROP TABLE IF EXISTS `aggregate_user`;
                """
                )

                conn.execute(query)
        
            with conn.begin(): 
                query = text("""
                CREATE TABLE if not exists `aggregate_user`
                (
                entity_type varchar(20) not null,
                primary key (entity_type),
                user_count int not null
                );
                """
                )

                conn.execute(query)

            with conn.begin(): 
                query = text("""
                INSERT INTO aggregate_user 
                SELECT message->'$.entityType' as entity_type, count(*) as count
                FROM data
                WHERE message->'$.clientId' = :client_id and session_id = :session_id and `type` = 'sync'
                group by entity_type;

                """
                )

                conn.execute(query,{"session_id":session_id, "client_id":client_id})

            with conn.begin(): 
                query = text("""
                UPDATE komodo.aggregate_user
                SET entity_type = replace(replace(replace(replace(entity_type, 0, 'head'), 1, 'left_hand'), 2, 'right_hand'), 3 ,'spawned_entity');
                """
                )

                conn.execute(query)
            with conn.begin():
                query = text("""
                SELECT * 
                FROM aggregate_user;
                """
                )
                result = conn.execute(query)
                count = [r[0:] for r in result]
                df = pd.DataFrame(count, columns = ['entity_type','user_count'])
                filename = str("aggregate_user_" + time.strftime('%Y-%m-%d %H-%S') + ".csv")
                df.to_csv(filename,index=False)
                file_path = os.path.abspath(filename)
                print("aggregate_user csv file downloaded!") 
                update_data_request(request_id, 1, file_path)

                return True
    except Exception as e:
        print(e)
        return False


def process_file(id, file):
    print("Processing file:", file)
    try:
        if (not os.path.isfile(file)):
            print(f"Error processing file: {file}: file does not exist")
            return False
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


def check_for_data_requests_table():
    # check if data_requests exist, if not, create one 
    try:
        with engine.connect() as conn:
            query = """
                    show tables like 'data_requests';
                    """
            result = conn.execute(query)
            exist = list([r[0] for r in result])

        if not (bool(exist)):
            with engine.connect()as conn:
                with conn.begin(): 
                    query = text("""
                    CREATE TABLE if not exists `data_requests`
                    (
                    request_id int NOT NULL AUTO_INCREMENT,
                    processed_capture_id varchar(50) not null,
                    who_requested int not null,
                    aggregation_function varchar(50) not null,
                    is_it_fulfilled int,
                    url varchar(255),
                    message json,
                    file_location varchar(255),
                    primary key (request_id)
                    );
                    """
                    )

                    conn.execute(query)

                with conn.begin(): 
                    query = text("""
                    INSERT INTO data_requests (`processed_capture_id`, `who_requested`, `aggregation_function`, `is_it_fulfilled`,`message`)
                    VALUES ('126_1630443513898', 2, 'aggregate_interaction_type', 0,'{"sessionId": 126, "clientId": 5, "captureId": 1, "type": "aggregate interaction type", "interactionType": 1,"entityType": 0}');
                    """
                    )
                    conn.execute(query)
            print("data_requests table created.")
            return True
        else: 
            print("data_requests table exists.")
            return True
    except Exception as e:
        print(e)
        return False


def aggregation_file_download():
    with engine.connect() as conn:
        with conn.begin(): 
            query = text("""
            select request_id, aggregation_function, is_it_fulfilled, message->'$.clientId' as client_id,
	                message->'$.sessionId'  as session_id, message->'$.entityType'  as entity_type,
	                message->'$.interactionType'  as interaction_type
            from data_requests
            where is_it_fulfilled = 0
            order by request_id;
            """
            )
            result = conn.execute(query)
            count = [r[0:] for r in result]
            temp_df = pd.DataFrame(count, columns = ['request_id','aggregation_function','is_it_fulfilled','client_id','session_id','entity_type','interaction_type'])
            temp_df.set_index("request_id",inplace = True)
            for index, row in temp_df.iterrows():
                request_id = index
                # parse all inputs 
                aggregation_function = row['aggregation_function']
                is_it_fulfilled = row['is_it_fulfilled']
                client_id = row['client_id']
                session_id = row['session_id']
                entity_type = row['entity_type']
                interaction_type = row['interaction_type']

                if aggregation_function == "aggregate_interaction_type":
                    if (session_id!= "null" and interaction_type!= "null"):
                        print(session_id,interaction_type,request_id)
                        aggregate_interaction_type(session_id,interaction_type,request_id)
                    else: 
                        print("Argument(s) for aggregate_interaction not valid!")
                if aggregation_function == "aggregate_user":
                    if (client_id!= "null" and session_id!= "null"):
                        aggregate_user(session_id,client_id,request_id)
                    else: 
                        print("Argument(s) for aggregate_user not valid!")
                   

def update_data_request(request_id,fulfilled_flag,file_location):
    try:
        query = text("""
                    UPDATE `data_requests`
                    SET is_it_fulfilled = :f, file_location = :fl
                    where request_id = :ri;
                    """)
        with engine.connect() as conn:
            result = conn.execute(query, {'f': fulfilled_flag, 'fl': file_location,'ri':request_id})

    except Exception as e:
        print(e)


if __name__ == "__main__":

    # infinite poll & process
    while True:
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
        
        # check data_request table and direct to respective functions
        if check_for_data_requests_table():
            aggregation_file_download()




