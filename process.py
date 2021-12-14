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

# for each client, aggregate each interaction type and show counts in one session
def aggregate_interaction_type(session_id, interaction_type, request_id):
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

            # insert query results to aggregation_interaction table
            with conn.begin(): 
                query = text("""
                INSERT INTO aggregate_interaction 
                SELECT client_id, count(message) as interaction_count
                FROM data
                WHERE message->'$.interactionType' = :interaction_type and session_id= :session_id
                GROUP BY client_id;
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

                # result to dataframe
                df = pd.DataFrame(count, columns = ['client_id','interaction_count'])
                filename = str("aggregate_interaction_" + time.strftime('%Y-%m-%d %H-%S') + ".csv")
                df.to_csv(filename,index=False)
                print("aggregate_interaction csv file downloaded!") 

                # grab and add file location back to data_request table
                file_path = os.path.abspath(filename)
                update_data_request(request_id, 1, file_path)

                # return True if aggregation function completed and csv got downloaded
                return True

    except Exception as e:
        # return False and print error messages
        print(e)
        return False
    
# for each entity_type, show each client's activity in one session
def aggregate_user(session_id, client_id, request_id):
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

            # insert query result to table
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
                SET 
                entity_type = replace(replace(replace(REPLACE(entity_type, 0, 'head'), 1, 'left_hand'), 2, 'right_hand'), 3 ,'spawned_entity');
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

                # result to dataframe
                df = pd.DataFrame(count, columns = ['entity_type','user_count'])
                filename = str("aggregate_user_" + time.strftime('%Y-%m-%d %H-%S') + ".csv")
                df.to_csv(filename,index=False)

                # grab and add file back to data_request table
                file_path = os.path.abspath(filename)
                print("aggregate_user csv file downloaded!") 
                update_data_request(request_id, 1, file_path)
                
                # return True if aggregation function completed and csv got downloaded
                return True

    except Exception as e:
        # return False and print error messages
        print(e)
        return False

# calculate user energy for each entity type
def user_energy(session_id,client_id, entity_type,request_id):
    try:
        with engine.connect() as conn:
            query = text("""
            SELECT client_id, session_id, timestamp,entity_type, energy
            FROM 
	            (SELECT session_id, client_id, message->'$.entityType' as entity_type,
			            message->'$.pos' as position, 
			            SQRT(POWER( message->'$.pos.x' - LAG(message->'$.pos.x',1) OVER (order by seq),2)+
			            POWER( message->'$.pos.y' - LAG(message->'$.pos.y',1) OVER (order by seq),2)+
			            POWER( message->'$.pos.z' - LAG(message->'$.pos.z',1) OVER (order by seq),2))/(ts - LAG(ts,1) OVER (order by seq)) AS energy,
			            ts AS timestamp, seq
	            FROM data
	            WHERE message->'$.clientId' = :client_id AND session_id = :session_id AND `type` = 'sync' 
	            ORDER BY seq) AS user_energy
            WHERE energy IS NOT NULL AND entity_type = :entity_type
            ORDER BY entity_type, energy DESC;
            """
            )

            result = conn.execute(query,{"session_id":session_id, "client_id":client_id, "entity_type":entity_type})
            count = [r[0:] for r in result]

            # record results in a dataframe
            df = pd.DataFrame(count, columns = ['client_d','session_id','timestamp','entity_type','energy'])
            filename = str("user_energy_" + time.strftime('%Y-%m-%d %H-%S') + ".csv")
            df.to_csv(filename,index=False)

            # grab and add file back to data_request table
            file_path = os.path.abspath(filename)            
            print("user energy csv file downloaded!")
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
            with conn.begin():  
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
                    request_id int not null AUTO_INCREMENT,
                    processed_capture_id varchar(50) not null,
                    who_requested int not null,
                    aggregation_function varchar(50) not null,
                    is_it_fulfilled int,
                    url varchar(255),
                    message JSON,
                    file_location varchar(255),
                    primary key (request_id)
                    );
                    """
                    )

                    conn.execute(query)

                with conn.begin(): 
                    query = text("""
                    INSERT INTO data_requests (`processed_capture_id`, `who_requested`, `aggregation_function`, `is_it_fulfilled`,`message`)
                    VALUES ('666_9999999999999', 2, 'aggregate_user', 1,'{"sessionId": null, "clientId": 888, "captureId": 777, "type": "test function", "interactionType": 1,"entityType": 0}');
                    """
                    )
                    conn.execute(query)

            print("data_requests table created.")
            return True
        else: 
            print("data_requests table exists.")
            return True

    except Exception as e:
        # return False and print error messages
        print(e)
        return False


def aggregation_file_download():
    with engine.connect() as conn:
        with conn.begin(): 
            query = text("""
            SELECT request_id, aggregation_function, is_it_fulfilled, message->'$.clientId' as client_id,
	                message->'$.sessionId'  as session_id, message->'$.entityType'  as entity_type,
	                message->'$.interactionType'  as interaction_type
            FROM data_requests
            WHERE is_it_fulfilled = 0
            ORDER BY request_id;
            """
            )
            result = conn.execute(query)
            count = [r[0:] for r in result]

            temp_df = pd.DataFrame(count, columns = ['request_id','aggregation_function','is_it_fulfilled','client_id','session_id','entity_type','interaction_type'])
            temp_df.set_index("request_id",inplace = True)

            # iterate rows in data_request table
            for index, row in temp_df.iterrows():
                request_id = index
                # parse all inputs 
                aggregation_function = row['aggregation_function']
                is_it_fulfilled = row['is_it_fulfilled']
                client_id = row['client_id']
                session_id = row['session_id']
                entity_type = row['entity_type']
                interaction_type = row['interaction_type']
                print(aggregation_function)

                # direct rows to functions and download CSV
                if aggregation_function == "aggregate_interaction_type":
                    if (session_id != "null" and interaction_type != "null"):
                        aggregate_interaction_type(session_id,interaction_type,request_id)
                    else: 
                        print("Argument(s) for aggregate_interaction not valid!")
                if aggregation_function == "aggregate_user":
                    print(session_id, client_id)
                    if (client_id != "null" and session_id != "null"):
                        aggregate_user(session_id,client_id,request_id)
                    else: 
                        print("Argument(s) for aggregate_user not valid!")
                if aggregation_function == "user_energy":
                    if (entity_type != "null" and client_id!= "null"):
                        label= user_energy(session_id,client_id, entity_type,request_id)
                    else: 
                        print("Argument(s) for user_energy not valid!")
                   

def update_data_request(request_id,fulfilled_flag,file_location):
    # update fulfilled flag to 1, once aggregation function completed and csv files got downloaded
    try:
        query = text("""
                    UPDATE `data_requests`
                    SET is_it_fulfilled = :f, file_location = :fl
                    WHERE request_id = :ri;
                    """)
        with engine.connect() as conn:
            result = conn.execute(query, {'f': fulfilled_flag, 'fl': file_location,'ri':request_id})

    except Exception as e:
        print(e)


 # show how stroke_id, stroke_type were used between timestamps
def drawing_pattern():
    try: 
        with engine.connect() as conn:
            with conn.begin(): 
                query = text("""
                SELECT ts AS timestamp,
		               count(message->'$.strokeType') AS stroke_type_count, 
		               count(message->'$.strokeId') AS stroke_id_count
                FROM data
                GROUP BY ts
                ORDER BY stroke_type_count DESC;
                """
                )

                conn.execute(query)

                # get result and return 
                result = conn.execute(query)
                count = [r[0:] for r in result]

                return True

    except Exception as e:
        # return False and print error messages
        print(e)
        return False
        

# Get information when multiple users appear within a diameter
def user_proximity(diameter):
    try: 
        with engine.connect() as conn:
            with conn.begin(): 
                query = text("""
                SELECT ts, client_id, position, distance, capture_id, session_id
                FROM(
                    SELECT client_id, message->'$.pos' AS position, 
                            SQRT(POWER( message->'$.pos.x' - LAG(message->'$.pos.x',1) OVER (order by ts,message->'$.pos'),2)+
                            POWER( message->'$.pos.y' - LAG(message->'$.pos.y',1) OVER (order by ts,message->'$.pos'),2)+
                            POWER( message->'$.pos.z' - LAG(message->'$.pos.z',1) OVER (order by ts,message->'$.pos'),2)) AS distance,
                            capture_id, session_id, ts
                    FROM data
                    WHERE ts IN (SELECT ts
                                FROM data
                                GROUP BY ts
                                HAVING count(distinct client_id) > 1)
                    ORDER BY ts, position
                    ) temp
                    WHERE distance > 0 AND distance < (:diameter)
                    ORDER BY distance;
                """
                )

                conn.execute(query,{"diameter":diameter})

                # get result and return 
                result = conn.execute(query)

                return True

    except Exception as e:
        # return False and print error messages
        print(e)
        return False

if __name__ == "__main__":
    # get result flag for checking data_request table
    data_request_flag = check_for_data_requests_table()

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
        if data_request_flag:
            aggregation_file_download()




