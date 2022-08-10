import pandas as pd
import os


mdb = pd.read_parquet(f"{os.getcwd()}/mdb.parquet")
output = pd.read_csv(f"{os.getcwd()}/output.csv")
c = output.loc[output['cid'] == 701285914]
# c =c.drop_duplicates(subset=['date', 'event_types', 'cid', ])
o = output.duplicated(subset=['event_types', 'date', 'cid'])
l = pd.concat([output, o])
l.to_csv('duplicates.csv')
print(c[['event_types', 'date']])