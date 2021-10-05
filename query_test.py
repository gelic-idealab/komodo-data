##File query_test.py
import unittest
import numpy as np
from unittest.case import TestCase
import process 
from sqlalchemy import create_engine, text, types
import os
import os.path
import sys
import time
import json
from numpy.core.defchararray import count
import pandas as pd

# import db configs
from config import *


class TestQuery(unittest.TestCase):
    def test_query(self):
        
        CAPTURE_FILE_NAME = 'data'

        if len(CAPTURES_DIR) <= 0:
            print("No captures directory declared in config.py. Exiting.")
            sys.exit(1)

        if not os.path.exists(CAPTURES_DIR):
            os.mkdir(CAPTURES_DIR)

        # create db connection 
        try:
            # dialect+driver://username:password@host:port/database
            connection_string = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
            engine = create_engine(connection_string)
        except Exception as e:
            print(e)
            sys.exit(1)
        
        with engine.connect() as conn:
            query = text("""
            SELECT * 
            FROM aggregate_interaction;
            """
            )
            result = conn.execute(query)
            count = [r[1] for r in result]
            sum = np.sum(count)

        with engine.connect() as conn:
            query = text("""
            SELECT * 
            FROM aggregate_user;
            """
            )
            result = conn.execute(query)
            count = [r[1:] for r in result]
            sum2 = np.sum(count)
        
        with engine.connect() as conn:
            query = text("""
            select session_id, entity_type, timestamp, energy,       
		            row_number() over (partition by entity_type order by energy desc) as energy_rank 
            from 
	        (   select session_id, client_id, message->'$.entityType' as entity_type,
			        message->'$.pos' as position, 
			        SQRT(POWER( message->'$.pos.x' - LAG(message->'$.pos.x',1) OVER (order by seq),2)+
			        POWER( message->'$.pos.y' - LAG(message->'$.pos.y',1) OVER (order by seq),2)+
			        POWER( message->'$.pos.z' - LAG(message->'$.pos.z',1) OVER (order by seq),2))/(ts - LAG(ts,1) OVER (order by seq)) as energy,
			        ts as timestamp, seq
	        from data
	        where message->'$.clientId' = 5 and session_id = 126 and `type` = 'sync' 
	        order by seq) as user_energy
            where energy is not null
            order by energy_rank, entity_type, energy DESC;

            """
            )

            result = conn.execute(query)
            count = [r[0:] for r in result]
            df = pd.DataFrame (count, columns = ['session_id','timestamp','entity_type','energy','energy_rank'])
            print(df.head(15))

        
        user_flag = process.aggregate_user(126,5) 
        if user_flag and (sum2 == 33594):
            print("User aggregation succeeded!")
        else:
            print("User aggregation failed!")

        interaction_flag = process.aggregate_interaction_type(126, 1) 
        if interaction_flag and (sum == 2238):
            print("Interaction aggregation succeeded!")
        else:
            print("Interaction aggregation failed!")
        
        
