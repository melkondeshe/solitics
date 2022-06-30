import requests
import json
import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
import random
from functions import my_portfolio, top_3_industry, top_7_sector, peers, large_market_cap


portfolio_inputs = [
  {
    'rec_1' : 4,
    'rec_2' : 5, 
    'rec_was_1' : 3,
    'rec_was_2' : 2,
    'rec_was_3' : 1,
    'final_assessment': True,
    'f_a_status' : False,
    'text':'my_portfolio_1_1'
},
{
    'rec_1' : 4,
    'rec_2' : 5, 
    'rec_was_1' : 4,
    'rec_was_2' : 5,
    'rec_was_3' : None,
    'final_assessment': True,
    'f_a_status' : False,
    'text':'my_portfolio_1_2'
},
{
    'rec_1' : 4,
    'rec_2' : 5, 
    'rec_was_1' : 3,
    'rec_was_2' : None,
    'rec_was_3' : None,
    'final_assessment': True,
    'f_a_status' : False,
    'text':'my_portfolio_1_3'
},
{
    'rec_1' : 1,
    'rec_2' : 2, 
    'rec_was_1' : 1,
    'rec_was_2' : 2,
    'rec_was_3' : None,
    'final_assessment': True,
    'f_a_status' : True,
    'text':'my_portfolio_1_4'
},
{
    'rec_1' : 1,
    'rec_2' : 2, 
    'rec_was_1' : 4,
    'rec_was_2' : 5,
    'rec_was_3' : None,
    'final_assessment': True,
    'f_a_status' : True,
    'text':'my_portfolio_1_5'
},
{
    'rec_1' : 1,
    'rec_2' : 2, 
    'rec_was_1' : 3,
    'rec_was_2' : None,
    'rec_was_3' : None,
    'final_assessment': True,
    'f_a_status' : True,
    'text':'my_portfolio_1_6'
},
{
    'rec_1' : 4,
    'rec_2' : 5, 
    'rec_was_1' : 1,
    'rec_was_2' : 2,
    'rec_was_3' : 3,
    'final_assessment': False,
    'f_a_status' : True,
    'text':'my_portfolio_1_7'
},
{
    'rec_1' : 4,
    'rec_2' : 5, 
    'rec_was_1' : 4,
    'rec_was_2' : 5,
    'rec_was_3' : None,
    'final_assessment': False,
    'f_a_status' : True,
    'text':'my_portfolio_1_8'
},
{
    'rec_1' : 1,
    'rec_2' : 2, 
    'rec_was_1' : 4,
    'rec_was_2' : 5,
    'rec_was_3' : None,
    'final_assessment': False,
    'f_a_status' : True,
    'text':'my_portfolio_1_9'
},
{
    'rec_1' : 1,
    'rec_2' : 2, 
    'rec_was_1' : 3,
    'rec_was_2' : None,
    'rec_was_3' : None,
    'final_assessment': False,
    'f_a_status' : True,
    'text':'my_portfolio_1_10'
},
{
    'rec_1' : 1,
    'rec_2' : 2, 
    'rec_was_1' : 1,
    'rec_was_2' : 2,
    'rec_was_3' : None,
    'final_assessment': False,
    'f_a_status' : True,
    'text':'my_portfolio_1_11'
},
]

top_3_industry_inputs = [
  {
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,  
  'part_of_top3': True,
  'text':'text_1'
},
{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,  
  'part_of_top3': True,
  'text':'text_2'
},
{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,  
  'part_of_top3': True,
  'text':'text_3'
},
]

top_7_industry_inputs = [
  {
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,  
  'part_of_top7': True,
  'text':'text_1'
},
{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : None,
  'rec_was_2' : None,
  'rec_was_3' : None,  
  'part_of_top7': True,
  'text':'text_2'
},
{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,  
  'part_of_top7': True,
  'text':'text_3'
},
]

peers_inputs = [

  {
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_1'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,  
  'final_assessment': True,
  'f_a_status': False,
  'text':'text_2'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': False,
  'text':'text_3'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': False,
  'text':'text_4'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,
  'final_assessment': True, 
  'f_a_status': True,
  'text':'text_5'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': True,
  'text':'text_6'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': True,
  'text':'text_7'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : 3,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_8'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_9'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_10'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_11'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_12'
}]

large_market_cap_inputs = [
{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,  
  'final_assessment': True,
  'f_a_status': False,
  'text':'text_1'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': False,
  'text':'text_2'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': False,
  'text':'text_3'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,
  'final_assessment': True, 
  'f_a_status': True,
  'text':'text_4'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': True,
  'text':'text_5'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,
  'final_assessment': True,
  'f_a_status': True,
  'text':'text_6'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : 3,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_7'
},{
  'rec_1' : 4,
  'rec_2' : 5, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_8'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 4,
  'rec_was_2' : 5,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_9'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 3,
  'rec_was_2' : None,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_10'
},{
  'rec_1' : 1,
  'rec_2' : 2, 
  'rec_was_1' : 1,
  'rec_was_2' : 2,
  'rec_was_3' : None,
  'final_assessment': False,
  'f_a_status': True,
  'text':'text_11'
}]

file_path = f'{os.getcwd()}/mdb_new.parquet'

url = "https://api.solitics.com/rest/events/v2"
username = 'melkon@deshe.ai'
password = '12345678' 

MAIN_OUTPUTS = []
# for input in portfolio_inputs:
#   output = my_portfolio(file_path, input)
#   if output:
#     MAIN_OUTPUTS.append(output)
#     break


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

for input in peers_inputs:
  output = peers(file_path, input)
  if output:
    MAIN_OUTPUTS.append(output)
    break

# for input in large_market_cap_inputs:
#   output = large_market_cap(file_path, input)
#   if output:
#     MAIN_OUTPUTS.append(output)
#     break

for i in MAIN_OUTPUTS:
  payload = json.dumps({
    "isPublicEvent": True,
    "eventType": "Top 3",
    "uniqueEventId": "123456",
    "eventDate": "2020-10-22 11:57:46.769 UTC",
    "eventAmount": 10000,
    "eventStatus": "New",
    "eventAttributes": {
      "text": i,
      "textAttributes":{
        'peer_name':'peer_name',
      }
    }
  })

  headers = {
    'Content-Type': 'application/json'
  }
  response = requests.request("POST", url, headers=headers, auth=(username, password), data=payload)
  print(response.status_code)