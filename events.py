from re import L
import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
import random

# filtered array [{cid:"apple", date:2029-12-12...}, {cid:'tesla'}]
# 1) filter mdb with each element of filterd array by cid 
# [ {date:2020-12-12}, {date:2029-12-12}...]
# 2) order by date
# 3) get index by date
# 4) index - 1 - previous row = 


def events(file_path, input):
    mdb = pd.read_parquet(file_path)
    now_date = datetime.now().date()
    mdb['date'] = pd.to_datetime(mdb['date']).dt.date
    #   checking 1st rule
    internal_rule_1 = now_date - mdb['date'] <= timedelta(200)
    #   checking 2nd rule
    internal_rule_2 = (mdb.buying_recommendation == input['rec_1']) | (mdb.buying_recommendation == input['rec_2'])
    #checking 3th rule
    # x = mdb.loc[:, ['cid']].join(other=mdb.query("cid in @sorted_mdb.cid").sort_values(by=['cid', 'year', 'quarter']).groupby(by='cid', sort=False).shift(periods=-1)).groupby(by='cid', sort=False).tail(n=2).dropna(subset=mdb.columns[1:])
    # print(x,'x')
    print(internal_rule_2)
    query_cid = mdb.query("cid in @internal_rule_2.cid")
    sort_date = query_cid.sort_values(by=['cid', 'date'], ascending=False).reset_index(drop=True)
    # sorted_mdb = mdb.query("cid in @internal_rule_2.cid").sort_values(by=['cid','date']).groupby(['cid']).shift(-1).tail(1)
    # print(sorted_mdb)
    a
    filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @internal_rule_2.cid")
    mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
    internal_rule_3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
    top_percent = 0
    compared = internal_rule_2.query("cid in @internal_rule_3.cid")
    sorted_mdb = compared
    final = compared
    print(input['event'])
    #   checking 4th rule
    if input['event'] == 'portfolio' or  input['event'] == 'peers' or input['event'] == 'large_market_cap':
        print(input['f_a_status'])
        sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=input['f_a_status'])[:10]
        top_list = [1,5,10]
        for i in top_list:
            top = sorted_mdb[0:i]
            final = compared.query("name in @top.name")
            print(final)
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
    final = compared.query("cid in @sorted_mdb.cid")
    print(final)
    if final.empty:
        return False

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
    number_of_last_underperform_in_a_row = None
    number_of_last_buy_and_strongbuy_in_a_row = None
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