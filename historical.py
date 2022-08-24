from xmlrpc.client import boolean
import os
from historical_events import events
from historical_text import col_names
from inputs import all_inputs
import datetime
from datetime import timedelta
from datetime import datetime as dt
import pandas as pd

file_path = f"{os.getcwd()}/mdb.parquet"

url = "https://api.solitics.com/rest/events/v2"
username = "melkon@deshe.ai"
password = "12345678"

MAIN_OUTPUTS = []
day = datetime.date(2021, 8, 24)

for days in range(365, -1, -7):
    for input in all_inputs:
        mdb = pd.read_parquet(file_path)
        peers = pd.read_parquet(f"{os.getcwd()}/peers.parquet")
        now_date = dt.now().date()
        mdb["date"] = pd.to_datetime(mdb["date"]).dt.date
        # mdb  = mdb.query('cid in [18511]')
        #   checking 1st rule
        internal_rule_1 = mdb.loc[(mdb["date"] > day) & (mdb["date"] <= day+timedelta(7))]
        for i in input:
            print(f"*** Started event {i['event']} ***")
            if internal_rule_1.empty:
                break
            output = events(file_path, i, day,mdb,internal_rule_1, peers)
            if type(output) == boolean:
                output = pd.DataFrame()
            MAIN_OUTPUTS.append(output)
            droped_duplicates = pd.concat([output, internal_rule_1]).drop_duplicates(subset=col_names, keep='first')
            
            not_generated_outputs = droped_duplicates
            if 'generated_text' in droped_duplicates:
                not_generated_outputs = droped_duplicates[droped_duplicates['generated_text'].isna()]
            
            internal_rule_1 = not_generated_outputs
    day += timedelta(7)
    if day >= datetime.date(2022, 8, 24):
        break
final_df = pd.concat(MAIN_OUTPUTS)
final_df.to_csv('output_2.csv')


# for i in MAIN_OUTPUTS:
#     for j in i:
#         payload = json.dumps(j)
#         headers = {"Content-Type": "application/json"}
#         response = requests.request(
#             "POST", url, headers=headers, auth=(username, password), data=payload
#         )
#         print(response.status_code)
