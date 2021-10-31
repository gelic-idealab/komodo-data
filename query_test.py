##File query_test.py
import unittest 
from unittest import TestCase
import numpy as np
from numpy.testing._private.utils import assert_equal
import process 
from sqlalchemy import create_engine, text, over
import os
import os.path
import sys
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
        
        interaction_flag = process.aggregate_interaction_type(126, 1) 

        with engine.connect() as conn:
            with conn.begin(): 
                query = text("""
                SELECT * 
                FROM aggregate_interaction;
                """
                )
                result = conn.execute(query)
                count = [r[1] for r in result]
                sum = np.sum(count)

        user_flag = process.aggregate_user(126,5) 

        with engine.connect() as conn:
            with conn.begin(): 
                query = text("""
                SELECT * 
                FROM aggregate_user;
                """
                )
                result = conn.execute(query)
                count = [r[1:] for r in result]
                sum2 = np.sum(count)
        
        with engine.connect() as conn:
            with conn.begin(): 
                # query = 
                # conn.execute(query)
                query = text("""
                select client_id, session_id, timestamp,entity_type, energy
                from 
	                (select session_id, client_id, message->'$.entityType' as entity_type,\
			                message->'$.pos' as position, \
			                SQRT(POWER( message->'$.pos.x' - LAG(message->'$.pos.x',1) OVER (order by seq),2)+\
			                POWER( message->'$.pos.y' - LAG(message->'$.pos.y',1) OVER (order by seq),2)+\
			                POWER( message->'$.pos.z' - LAG(message->'$.pos.z',1) OVER (order by seq),2))/(ts - LAG(ts,1) OVER (order by seq)) as energy,\
			                ts as timestamp, seq
	                from data
	                where message->'$.clientId' = 5 and session_id = 126 and `type` = 'sync' 
	                order by seq) as user_energy
                where energy is not null
                order by entity_type, energy DESC;

            """
            )
                conn.execute(query)


            result = conn.execute(query)
            count = [r[0:] for r in result]
            df = pd.DataFrame(count, columns = ['session_id','timestamp','entity_type','energy','energy_rank'])
            df.to_csv('energy_out.csv',index=False)
            out_list = df.head(5).values.tolist()
            

            test = [[5,126,1630443609231,'0',0.536178417303133],
                    [5,126,1630443614316,'0',0.47170262033491217],
                    [5,126,1630443612993,'0',0.4564820648139896],
                    [5,126,1630443617111,'0',0.45187208310036364],
                    [5,126,1630443610928,'0',0.44620929903013185]]

        # aggregate_interaction
        self.assertEqual(sum, 3357)
        # aggregate_user
        self.assertEqual(sum2, 50391)
        # user_energy
        self.assertEqual(out_list, test)

