import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
import random

def get_count(mdb, cid, num_list):
  count = 0
  sorted_mdb = mdb.query("cid in @cid").sort_values(by=['date'], ascending=False)
  for index, row in sorted_mdb.iterrows():
        if row['buying_recommendation'] == num_list[0] or row['buying_recommendation'] == num_list[1]:
            count += 1
        else:
            return count
  return count