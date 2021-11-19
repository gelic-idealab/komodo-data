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
        
        #interaction testing
        interaction_flag = process.aggregate_interaction_type(126, 1, 1) 

        with engine.connect() as conn:
            with conn.begin(): 
                query = text("""
                SELECT * 
                FROM aggregate_interaction;
                """
                )
                result = conn.execute(query)
                count = [r[1:] for r in result]
                sum = np.sum(count)

        # aggregate_interaction
        self.assertEqual(sum, 1288) 



