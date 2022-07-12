import requests
import json
import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
from events import events
from inputs import all_inputs
import random
import pandas as pd
# df = pd.read_csv('peers.csv')
# print(df.cid[18511])

file_path = f'{os.getcwd()}/mdb.parquet'

url = "https://api.solitics.com/rest/events/v2"
username = 'melkon@deshe.ai'
password = '12345678' 

MAIN_OUTPUTS = []


for input in all_inputs:
  for i in input:
    output = events(file_path, i)
    if output:
      MAIN_OUTPUTS.append(output)
      break
  
for i in MAIN_OUTPUTS:
  payload = json.dumps({
    "isPublicEvent": True,
    "eventType": "Portfolio",
    "uniqueEventId": random.randint(100000, 1000000),
    "eventAttributes": {
      "text": i
    }
  })

  headers = {
    'Content-Type': 'application/json'
  }
  response = requests.request("POST", url, headers=headers, auth=(username, password), data=payload)
  print(response.status_code)