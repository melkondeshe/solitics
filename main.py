import requests
import json
import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
import random
from functions import my_portfolio, top_3_industry, top_7_sector, peers, large_market_cap
from inputs import portfolio_inputs, top_3_industry_inputs, top_7_industry_inputs, peers_inputs, large_market_cap_inputs


file_path = f'{os.getcwd()}/mdb.parquet'

url = "https://api.solitics.com/rest/events/v2"
username = 'melkon@deshe.ai'
password = '12345678' 

MAIN_OUTPUTS = []


for input in portfolio_inputs:
  output = my_portfolio(file_path, input)
  if output:
    print(output)
    MAIN_OUTPUTS.append(output)
    break


# for input in top_3_industry_inputs:
#   output = top_3_industry(file_path, input)
#   if output:
#     MAIN_OUTPUTS.append(output)
#     break

# for input in top_7_industry_inputs:
#   output = top_7_sector(file_path, input)
#   if output:
#     MAIN_OUTPUTS.append(output)
#     break

# for input in peers_inputs:
#   output = peers(file_path, input)
#   if output:
#     MAIN_OUTPUTS.append(output)
#     break

# for input in large_market_cap_inputs:
#   output = large_market_cap(file_path, input)
#   if output:
#     MAIN_OUTPUTS.append(output)
#     break


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