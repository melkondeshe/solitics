from re import L
import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
import random
from functions import get_count

def events(file_path, input):
    mdb = pd.read_parquet(file_path)
    peers = pd.read_parquet(f'{os.getcwd()}/peers.parquet')
    print(peers)
    now_date = datetime.now().date()
    mdb['date'] = pd.to_datetime(mdb['date']).dt.date
    #   checking 1st rule
    internal_rule_1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
    #   checking 2nd rule
    internal_rule_2 = internal_rule_1[(internal_rule_1.buying_recommendation == input['rec_1']) | (internal_rule_1.buying_recommendation == input['rec_2'])]
    #checking 3th rule
    query_cid = mdb.query("cid in @internal_rule_2.cid")
    sort_date = query_cid.sort_values(by=['cid', 'date'], ascending=False).reset_index(drop=True)
    cids = {}
    for index, row in sort_date.iterrows():
        if not row['cid'] in cids:
            cids[row['cid']] = []
        cids[row['cid']].append({'date':row['date'],'buying_recommendation':row['buying_recommendation']})
    appended_data = []
    for index, row in internal_rule_2.iterrows():
        for i in range(len(cids[row['cid']]) - 1):
            if cids[row['cid']][i]['date'] == row['date']:
                if (cids[row['cid']][i+1]['buying_recommendation'] == input['rec_was_1']) or (cids[row['cid']][i+1]['buying_recommendation'] == input['rec_was_2']) or (cids[row['cid']][i+1]['buying_recommendation'] == input['rec_was_3']):
                    appended_data.append({'cid':row['cid'] ,'date':cids[row['cid']][i]['date']})
                break
    cid_date = pd.DataFrame.from_records(appended_data)
    internal_rule_3 = internal_rule_2.query('cid in @cid_date.cid & date in @cid_date.date')
    #   checking 4th rule
    top_percent= None
    if input['event'] == 'portfolio' or  input['event'] == 'peers' or input['event'] == 'large_market_cap':
        sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=input['f_a_status'])[:10]
        top_list = [1,5,10]
        for i in top_list:
            top = sorted_mdb[0:i]
            final = internal_rule_3.query("name in @top.cid")
            if final.empty:
                return False
            else:
                top_percent = i
                break
    if input['event'] == 'top_3_industry':
        print('top3')
        sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=False)[:3]
    if input['event'] == 'top_7_sector':
        print('top4')
        sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=False)[:7]
    final = internal_rule_3.query("cid in @sorted_mdb.cid")
    if final.empty:
        return False
    number_of_last_underperform_in_a_row = get_count(mdb, [final.iloc[0]['cid']], [1,2])
    number_of_last_buy_and_strongbuy_in_a_row = get_count(mdb, [final.iloc[0]['cid']], [4,5])
    print(number_of_last_underperform_in_a_row, number_of_last_buy_and_strongbuy_in_a_row)
    top_7_sector_comany_name = final.iloc[0]['name']
    top_3_industry_comany_name = final.iloc[0]['name']
    name_of_most_positive_dataCollectionTypeNme = mdb.loc[mdb['cid'] == final.iloc[0]['cid']].sort_values(by=['cash_flow', 'income_statement', 'balance_sheet'], ascending=False)
    name = final.iloc[0]['name']
    industry = final.iloc[0]['industry']
    sector = final.iloc[0]['sector']
    region = final.iloc[0]['region']
    country = final.iloc[0]['country']
    isin = final.iloc[0]['isin']
    positive = random.choice(text.positive)
    negative = random.choice(text.negative)
    neutral = random.choice(text.neutral)
    
    # TODO: Need to clarify and update increase and decreased values
    increased_decreased=None
    most_positive_param_name = int(final.sort_values(by=['assets_assetsc_raw'], ascending=False).iloc[0]['assets_assetsc_raw'])
    most_positive_param_pct = int(final.sort_values(by=['assets_assetsc_pct'], ascending=False).iloc[0]['assets_assetsc_pct'])
    most_positive_param_coeff = int(final.sort_values(by=['assets_assetsc'], ascending=False).iloc[0]['assets_assetsc'])
    most_negative_param_name = int(final.sort_values(by=['assets_assetsc_raw'], ascending=True).iloc[0]['assets_assetsc_raw'])
    most_negative_param_pct = int(final.sort_values(by=['assets_assetsc_pct'], ascending=True).iloc[0]['assets_assetsc_pct'])
    most_negative_param_coeff = int(final.sort_values(by=['assets_assetsc'], ascending=True).iloc[0]['assets_assetsc'])
    if final.iloc[0]['instance'] == 'Original':
        report_type = 'Official'
    else:
        report_type = final.iloc[0]['instance']
    generated_text = text.generate_text(event=input['event'], text_version=input['text_version'],importance=input['importance'], peer_name=name, 
        top_7_sector_comany_name=top_7_sector_comany_name, top_3_industry_comany_name=top_3_industry_comany_name, company_name=name, positive_adjective=positive, negative_adjective=negative, 
        percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
        most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
        most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
        most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct, name_of_most_positive_dataCollectionTypeNme=name_of_most_positive_dataCollectionTypeNme.iloc[0],
        most_negative_param_coeff=most_negative_param_coeff, industry=industry, sector=sector, region=region, country=country,
        isin=isin)
    return generated_text