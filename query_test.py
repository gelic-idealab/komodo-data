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
        
        user_flag = process.aggregate_user(126,5) 
        if user_flag and (sum2 == 16797):
            print("User aggregation succeeded!")
        else:
            print("User aggregation failed!")

        interaction_flag = process.aggregate_interaction_type(126, 1) 
        if interaction_flag and (sum == 1119):
            print("Interaction aggregation succeeded!")
        else:
            print("Interaction aggregation failed!")
        
        # self.assertEqual(process.aggregate_interaction_type(126,4),13)
        # self.assertEqual(process.aggregate_user(126,2), 50510)
        
