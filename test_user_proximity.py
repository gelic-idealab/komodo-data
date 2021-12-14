##File query_test.py
import unittest 
from unittest import TestCase
import numpy as np
from numpy.testing._private.utils import assert_equal
from sqlalchemy.sql.sqltypes import BIGINT, JSON, Integer
import process 
from sqlalchemy import create_engine, text
from sqlalchemy.sql import *
import os
import os.path
import sys
from numpy.core.defchararray import count
import pandas as pd
import time


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
                    WHERE distance > 0 AND distance < 1
                    ORDER BY distance;
                """
                )
                result = conn.execute(query)
                count = [r[0] for r in result]
                
        expected_list = [1630443517509, 1630443643045, 1630443643969, 1630443676792, 1630443611381]
        self.assertEqual(count[0:5], expected_list)




