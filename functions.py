
import pandas as pd
import os 
from datetime import datetime
from datetime import timedelta
import text
import random

def my_portfolio(file_path, input):
  
  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()

  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
#   checking 1st rule
  internal_rule_1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
#   checking 2nd rule
  internal_rule_2 = internal_rule_1[(internal_rule_1.buying_recommendation == input['rec_1']) | (internal_rule_1.buying_recommendation == input['rec_2'])]
#   checking 3nd rule
  sort_date = mdb.sort_values(by=['cid', 'date'], ascending=False).reset_index(drop=True)
  query_cid = sort_date.query("cid in @internal_rule_2.cid")
  print(query_cid[['date', 'cid']])
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @internal_rule_2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  internal_rule_3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  top_percent = 0
  compared = internal_rule_2.query("cid in @internal_rule_3.cid")
  final = compared
  if input['final_assessment']:
    #   checking 3th rule
    sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=input['f_a_status'])[:10]
    top_list = [1,5,10]
    for i in top_list:
      top = sorted_mdb[0:i]
      final = compared.query("name in @top.name")
      if final.empty:
        return False
      else:
        top_percent = i
        break

  name = final.iloc[0]['name']
  industry = final.iloc[0]['industry']
  sector = final.iloc[0]['sector']
  region = final.iloc[0]['region']
  country = final.iloc[0]['country']
  isin = final.iloc[0]['isin']
  positive = random.choice(text.positive)
  negative = random.choice(text.negative)
  neutral = random.choice(text.neutral)
  most_positive_param_name = int(final.sort_values(by=['assets_assetsc_raw'], ascending=False).iloc[0]['assets_assetsc_raw'])
  most_positive_param_pct = int(final.sort_values(by=['assets_assetsc_pct'], ascending=False).iloc[0]['assets_assetsc_pct'])
  most_positive_param_coeff = int(final.sort_values(by=['assets_assetsc'], ascending=False).iloc[0]['assets_assetsc'])
  most_negative_param_name = int(final.sort_values(by=['assets_assetsc_raw'], ascending=True).iloc[0]['assets_assetsc_raw'])
  most_negative_param_pct = int(final.sort_values(by=['assets_assetsc_pct'], ascending=True).iloc[0]['assets_assetsc_pct'])
  most_negative_param_coeff = int(final.sort_values(by=['assets_assetsc'], ascending=True).iloc[0]['assets_assetsc'])
  increased_decreased=None
  number_of_last_underperform_in_a_row = None
  number_of_last_buy_and_strongbuy_in_a_row = None
  if final.iloc[0]['instance'] == 'Original':
    report_type = 'Official'
  else:
    report_type = final.iloc[0]['instance']
  generated_text = text.my_portfolio_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff, industry=industry, sector=sector, region=region, country=country,
                   isin=isin)
  return generated_text


def top_3_industry(file_path, input):

  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  internal_rule_1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  internal_rule_2 = internal_rule_1[(internal_rule_1.buying_recommendation == input['rec_1']) | (internal_rule_1.buying_recommendation == input['rec_2'])]
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @internal_rule_2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  internal_rule_3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=False)[:3]
  compared = internal_rule_2.query("cid in @internal_rule_3.cid")
  final = compared.query("cid in @sorted_mdb.cid")

  if final.empty:
      return False

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
  top_3_industry_comany_name = final.iloc[0]['name']
  if final.iloc[0]['instance'] == 'Original':
    report_type = 'Official'
  else:
    report_type = final.iloc[0]['instance']
  industry = final.iloc[0]['industry']
  generated_text = text.top_3_industry_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                  report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                  most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                  most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                  most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                  most_negative_param_coeff=most_negative_param_coeff, top_3_industry_comany_name=top_3_industry_comany_name, industry=industry, sector=sector, region=region, country=country,
                  isin=isin)
  return generated_text

def top_7_sector(file_path, input):

  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  internal_rule_1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  internal_rule_2 = internal_rule_1[(internal_rule_1.buying_recommendation == input['rec_1']) | (internal_rule_1.buying_recommendation == input['rec_2'])]
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @internal_rule_2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  internal_rule_3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=False)[:7]
  compared = internal_rule_2.query("cid in @internal_rule_3.cid")
  final = compared.query("cid in @sorted_mdb.cid")
  if final.empty:
      return False
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
  increased_decreased=None
  number_of_last_buy_and_strongbuy_in_a_row = None
  most_positive_param_name = int(final.sort_values(by=['assets_assetsc_raw'], ascending=False).iloc[0]['assets_assetsc_raw'])
  most_positive_param_pct = int(final.sort_values(by=['assets_assetsc_pct'], ascending=False).iloc[0]['assets_assetsc_pct'])
  most_positive_param_coeff = int(final.sort_values(by=['assets_assetsc'], ascending=False).iloc[0]['assets_assetsc'])
  most_negative_param_name = int(final.sort_values(by=['assets_assetsc_raw'], ascending=True).iloc[0]['assets_assetsc_raw'])
  most_negative_param_pct = int(final.sort_values(by=['assets_assetsc_pct'], ascending=True).iloc[0]['assets_assetsc_pct'])
  most_negative_param_coeff = int(final.sort_values(by=['assets_assetsc'], ascending=True).iloc[0]['assets_assetsc'])
  top_7_sector_comany_name = final.iloc[0]['name']
  if final.iloc[0]['instance'] == 'Original':
    report_type = 'Official'
  else:
    report_type = final.iloc[0]['instance']
  sector = final.iloc[0]['sector']
  generated_text = text.top_7_sector_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff, top_7_sector_comany_name=top_7_sector_comany_name, sector=sector,
                   industry=industry, region=region, country=country,
                   isin=isin)
  return generated_text

 
def peers(file_path, input):
  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  internal_rule_1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  internal_rule_2 = internal_rule_1[(internal_rule_1.buying_recommendation == input['rec_1']) | (internal_rule_1.buying_recommendation == input['rec_2'])]
  top_percent = 0
  # if 'rule_3' in input.keys():
  #   print
  # else:
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @internal_rule_2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  internal_rule_3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  print(internal_rule_3.last_report_of_company)
  
  compared = internal_rule_2.query("name in @internal_rule_3.name")
  if input['final_assessment']:
    sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=input['f_a_status'])[:10]
    top_list = [1,5,10]
    for i in top_list:
      top = sorted_mdb[0:i]
      final = compared.query("name in @top.name")
      if final.empty:
        return False
      else:
        top_percent = i
        continue
  else:
    final = internal_rule_3

  name = final.iloc[0]['name']
  industry = final.iloc[0]['industry']
  sector = final.iloc[0]['sector']
  region = final.iloc[0]['region']
  country = final.iloc[0]['country']
  isin = final.iloc[0]['isin']
  positive = random.choice(text.positive)
  negative = random.choice(text.negative)
  neutral = random.choice(text.neutral)
  name_of_most_positive_dataCollectionTypeNme = mdb.loc[mdb['cid'] == final.iloc[0]['cid']].sort_values(by=['cash_flow', 'income_statement', 'balance_sheet'], ascending=False)
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
  generated_text = text.peer_text(text_version=input['text'], peer_name=name, company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff, name_of_most_positive_dataCollectionTypeNme=name_of_most_positive_dataCollectionTypeNme.iloc[0],
                   industry=industry, sector=sector, region=region, country=country,
                   isin=isin)
  return generated_text

def large_market_cap(file_path, input):
  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  internal_rule_1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  internal_rule_2 = internal_rule_1[(internal_rule_1.buying_recommendation == input['rec_1']) | (internal_rule_1.buying_recommendation == input['rec_2'])]
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @internal_rule_2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  internal_rule_3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  top_percent = 0
  if input['final_assessment']:
    compared = internal_rule_2.query("name in @internal_rule_3.name")
    sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=input['f_a_status'])[:10]
    top_list = [1,5,10]
    for i in top_list:
      top = sorted_mdb[0:i]
      final = compared.query("name in @top.name")
      if final.empty:
        return False
      else:
        top_percent = i
        continue
  else:
    final = internal_rule_3

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
  generated_text = text.large_market_cap_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff, industry=industry, sector=sector, region=region, country=country,
                   isin=isin)
  return generated_text