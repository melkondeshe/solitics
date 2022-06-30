
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
  InternalRule1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
#   checking 2nd rule
  InternalRule2 = InternalRule1[(InternalRule1.buying_recommendation == input['rec_1']) | (InternalRule1.buying_recommendation == input['rec_2'])]
  
#   checking 3nd rule
  grouped_mdb = mdb.groupby('cid')
  print(grouped_mdb.get_group(584335428))
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @InternalRule2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  InternalRule3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  top_percent = 0
  compared = InternalRule2.query("cid in @InternalRule3.cid")
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
  else:
    final = compared

  name = final.iloc[0]['name']
  positive = random.choice(text.positive)
  negative = random.choice(text.negative)
  neutral = random.choice(text.neutral)
  most_positive_param_name = final.sort_values(by=['assets_assetsc_raw'], ascending=False).iloc[0]
  most_positive_param_pct = final.sort_values(by=['assets_assetsc_pct'], ascending=False).iloc[0]
  most_positive_param_coeff = final.sort_values(by=['assets_assetsc'], ascending=False).iloc[0]
  most_negative_param_name = final.sort_values(by=['assets_assetsc_raw'], ascending=True).iloc[0]
  most_negative_param_pct = final.sort_values(by=['assets_assetsc_pct'], ascending=True).iloc[0]
  most_negative_param_coeff = final.sort_values(by=['assets_assetsc'], ascending=True).iloc[0]
  increased_decreased=None
  number_of_last_underperform_in_a_row = None
  number_of_last_buy_and_strongbuy_in_a_row = None
  report_type = final.iloc[0]['instance']
  generated_text = text.my_portfolio_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff)
  return generated_text


def top_3_industry(file_path, input):
    mdb = pd.read_parquet(file_path)
    now_date = datetime.now().date()
    mdb['date'] = pd.to_datetime(mdb['date']).dt.date
    InternalRule1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
    InternalRule2 = InternalRule1[(InternalRule1.buying_recommendation == input['rec_1']) | (InternalRule1.buying_recommendation == input['rec_2'])]
    filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @InternalRule2.cid")
    mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
    InternalRule3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
    sorted_mdb = mdb.sort_values(by=['final_assessment'], ascending=False)[:3]
    compared = InternalRule2.query("cid in @InternalRule3.cid")
    final = compared.query("cid in @sorted_mdb.cid")
    if final.empty:
        return False

        # 1) get all the mdb idustries  
        # 2) order them by final assestment
        # 3) cut top 3
        # 4) 

    name = final.iloc[0]['name']
    positive = random.choice(text.positive)
    negative = random.choice(text.negative)
    neutral = random.choice(text.neutral)
    number_of_last_underperform_in_a_row = None
    number_of_last_buy_and_strongbuy_in_a_row = None
    increased_decreased=None
    most_positive_param_name = None
    most_positive_param_pct=None
    most_positive_param_coeff=None
    most_negative_param_name=None
    most_negative_param_pct=None
    most_negative_param_coeff=None
    top_3_industry_comany_name = None
    report_type = final.iloc[0]['instance']
    industry = final.iloc[0]['industry']
    generated_text = text.top_3_industry_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                    report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                    most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                    most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                    most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                    most_negative_param_coeff=most_negative_param_coeff, top_3_industry_comany_name=top_3_industry_comany_name, industry=industry)
    return generated_text

def top_7_sector(file_path, input):
  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  InternalRule1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  InternalRule2 = InternalRule1[(InternalRule1.buying_recommendation == input['rec_1']) | (InternalRule1.buying_recommendation == input['rec_2'])]
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @InternalRule2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  InternalRule3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  
  final = InternalRule3
  name = final.iloc[0]['name']
  positive = random.choice(text.positive)
  negative = random.choice(text.negative)
  neutral = random.choice(text.neutral)
  number_of_last_underperform_in_a_row = None
  most_positive_param_name = None
  increased_decreased=None
  most_positive_param_pct=None
  most_positive_param_coeff=None
  number_of_last_buy_and_strongbuy_in_a_row = None
  most_negative_param_name=None
  most_negative_param_pct=None
  most_negative_param_coeff=None
  top_7_sector_comany_name = None
  report_type = final.iloc[0]['instance']
  sector = final.iloc[0]['sector']
  generated_text = text.top_7_sector_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff, top_7_sector_comany_name=top_7_sector_comany_name, sector=sector)
  return generated_text

 
def peers(file_path, input):
  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  InternalRule1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  InternalRule2 = InternalRule1[(InternalRule1.buying_recommendation == input['rec_1']) | (InternalRule1.buying_recommendation == input['rec_2'])]
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @InternalRule2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  InternalRule3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  top_percent = 0
  print(mdb.columns)
  if input['final_assessment']:
    compared = InternalRule2.query("name in @InternalRule3.name")
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
    final = InternalRule3

  name = final.iloc[0]['name']
  positive = random.choice(text.positive)
  negative = random.choice(text.negative)
  neutral = random.choice(text.neutral)
  name_of_most_positive_dataCollectionTypeNme = mdb.loc[mdb['cid'] == final.iloc[0]['cid']].sort_values(by=['cash_flow', 'income_statement', 'balance_sheet'], ascending=False)
  number_of_last_underperform_in_a_row = None
  number_of_last_buy_and_strongbuy_in_a_row = None
  increased_decreased=None
  most_positive_param_name = None
  most_positive_param_pct=None
  most_positive_param_coeff=None
  most_negative_param_name=None
  most_negative_param_pct=None
  most_negative_param_coeff=None
  report_type = final.iloc[0]['instance']
  generated_text = text.peer_text(text_version=input['text'], peer_name=name, company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff, name_of_most_positive_dataCollectionTypeNme=name_of_most_positive_dataCollectionTypeNme.iloc[0])
  return generated_text

def large_market_cap(file_path, input):
  mdb = pd.read_parquet(file_path)
  now_date = datetime.now().date()
  mdb['date'] = pd.to_datetime(mdb['date']).dt.date
  InternalRule1 = mdb.loc[now_date - mdb['date'] <= timedelta(200)]
  InternalRule2 = InternalRule1[(InternalRule1.buying_recommendation == input['rec_1']) | (InternalRule1.buying_recommendation == input['rec_2'])]
  filtered_mdb = mdb.sort_values(by=['cid', 'date'], ascending=False).query("cid in @InternalRule2.cid")
  mdb_by_date = filtered_mdb.loc[(now_date - filtered_mdb['date'] >= timedelta(200)) & (now_date - filtered_mdb['date'] <= timedelta(400))]
  InternalRule3 = mdb_by_date[(mdb_by_date.buying_recommendation == input['rec_was_1']) | (mdb_by_date.buying_recommendation == input['rec_was_2'])| (mdb_by_date.buying_recommendation == input['rec_was_3'])]
  top_percent = 0
  print(mdb.columns)
  if input['final_assessment']:
    compared = InternalRule2.query("name in @InternalRule3.name")
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
    final = InternalRule3

  name = final.iloc[0]['name']
  positive = random.choice(text.positive)
  negative = random.choice(text.negative)
  neutral = random.choice(text.neutral)
  number_of_last_underperform_in_a_row = None
  most_positive_param_name = None
  increased_decreased=None
  most_positive_param_pct=None
  most_positive_param_coeff=None
  number_of_last_buy_and_strongbuy_in_a_row = None
  most_negative_param_name=None
  most_negative_param_pct=None
  most_negative_param_coeff=None
  report_type = final.iloc[0]['instance']
  generated_text = text.large_market_cap_text(text_version=input['text'], company_name=name, positive_adjective=positive, negative_adjective=negative, 
                   percent=top_percent, report_type=report_type, number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
                   most_positive_param_name=most_positive_param_name, increased_decreased=increased_decreased, most_positive_param_pct=most_positive_param_pct,
                   most_positive_param_coeff=most_positive_param_coeff, number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
                   most_negative_param_name=most_negative_param_name, most_negative_param_pct=most_negative_param_pct,
                   most_negative_param_coeff=most_negative_param_coeff)
  return generated_text