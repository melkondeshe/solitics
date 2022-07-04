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


file_path = f'{os.getcwd()}/mdb.parquet'

url = "https://api.solitics.com/rest/events/v2"
username = 'melkon@deshe.ai'
password = '12345678' 

MAIN_OUTPUTS = []


for input in all_inputs:
  for i in input:
    output = events(file_path, i)
    
    print(212222222222222, output)
    if output:
      print(11111111111, output)
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