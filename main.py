import requests
import json
import os
from events import events
from inputs import all_inputs
import random
import datetime
from datetime import timedelta
# df = pd.read_csv('peers.csv')

mdb_path = f"{os.getcwd()}/mdb.parquet"
peers_path = f"{os.getcwd()}/peers.parquet"


url = "https://api.solitics.com/rest/events/v2"
username = "melkon@deshe.ai"
password = "12345678"

MAIN_OUTPUTS = []



for input in all_inputs:
    for i in input:
        output = events(mdb_path, peers_path, i)
        if output:
            MAIN_OUTPUTS.append(output)
            break


for i in MAIN_OUTPUTS:
    for j in i:
        payload = json.dumps(j)
        headers = {"Content-Type": "application/json"}
        response = requests.request(
            "POST", url, headers=headers, auth=(username, password), data=payload
        )
        print(response.status_code)