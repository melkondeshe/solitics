import pandas as pd
import os
from datetime import datetime
from datetime import timedelta
import text
import random
from functions import get_count


def events(file_path, input):
    mdb = pd.read_parquet(file_path)

    print(f"*** Started event {input['event']} ***")

    peers = pd.read_parquet(f"{os.getcwd()}/peers.parquet")
    now_date = datetime.now().date()
    mdb["date"] = pd.to_datetime(mdb["date"]).dt.date

    #   checking 1st rule
    internal_rule_1 = mdb.loc[now_date - mdb["date"] <= timedelta(input["days"])]
    #   checking 2nd rule
    internal_rule_2 = internal_rule_1[
        internal_rule_1.buying_recommendation.isin(input["recommendation"])
    ]

    # checking 3th rule
    # get from mdb cids which equal cids from internal_rule_2
    query_cid = mdb.query("cid in @internal_rule_2.cid")
    # sort by cid and date
    sort_date = query_cid.sort_values(by=["cid", "date"], ascending=False).reset_index(
        drop=True
    )
    cids = {}
    # create an dict with cid, date and buying_recommendation
    for _, row in sort_date.iterrows():
        if not row["cid"] in cids:
            cids[row["cid"]] = []
        cids[row["cid"]].append(
            {"date": row["date"], "buying_recommendation": row["buying_recommendation"]}
        )
    appended_data = []
    # get from internal_rule_2 all rows which previus row equal to recommendation_was value
    for _, row in internal_rule_2.iterrows():
        for i in range(len(cids[row["cid"]]) - 1):
            if cids[row["cid"]][i]["date"] == row["date"]:
                if (
                    (
                        cids[row["cid"]][i + 1]["buying_recommendation"]
                        in input["recommendation_was"]
                    )
                    or (
                        cids[row["cid"]][i + 1]["buying_recommendation"]
                        in input["recommendation_was"]
                    )
                    or (
                        cids[row["cid"]][i + 1]["buying_recommendation"]
                        in input["recommendation_was"]
                    )
                ):
                    appended_data.append(
                        {"cid": row["cid"], "date": cids[row["cid"]][i]["date"]}
                    )
                break
    # turn list into dataframe
    cid_date = pd.DataFrame.from_records(appended_data)
    # check if df is empty return False, else get from internal_rule_2 all data which equal to cid_date
    if cid_date.empty:
        return False
    else:
        internal_rule_3 = internal_rule_2.query(
            "cid in @cid_date.cid & date in @cid_date.date"
        )
    # checking 4th rule
    final_output = []
    # check if 4 rule exist and if peers exist in 4 rule, if not internal_rule_3 became to final 
    if "has_peers_filtering" in input:
        if not input["has_peers_filtering"]:
            final = internal_rule_3
        else:
            for _, row in internal_rule_3.iterrows():
                # get from peers all cids which equal to internal_rule_3
                peers = peers.loc[peers.cid == row["cid"]]
                # get from mdb all cids which equal to peers competitorcompanyid
                competitors_cids = mdb.query("cid in @peers.competitorcompanyid")

                # Checking if it row exists inside competitors
                if row["name"] not in competitors_cids.name:
                    # if not - add to sort & calculate top percentage
                    row_df = pd.DataFrame(row).transpose()
                    competitors_cids = pd.concat([competitors_cids,row_df], ignore_index=True)
                competitors_cids = competitors_cids.sort_values(
                    by=["final_assessment"], ascending=input['is_final_assessment_ascending']
                )[:10]

                # TODO: 
                top_list = [1, 5, 10]
                for i in top_list:
                    top_competitors = competitors_cids[0:i]
                    final = top_competitors.loc[top_competitors["name"] == row["name"]]
                    if not final.empty:
                        final["top_percent"] = i
                        final_output.append(final)
                        break

    if final_output:
        final = pd.concat(final_output)
    # 4 rule with top3 and top7 
    if 'event_type_top' in input:
        sorted_mdb = mdb.sort_values(by=["final_assessment"], ascending=False)[:input['event_type_top']]
        final_step = internal_rule_3.query("cid in @sorted_mdb.cid")
        if  final_step.empty:
            return False
        final_first = final_step.iloc[0]
        final = pd.DataFrame(final_first).transpose()
        
    if final.empty:
        # if no result found go to the next step
        return False
    # generate all values for text and put it in text
    generated_texts = []
    for index, row in final.iterrows():
        row_cid = [row["cid"]]
        number_of_last_underperform_in_a_row = get_count(mdb, row_cid, [1, 2])
        number_of_last_buy_and_strongbuy_in_a_row = get_count(mdb, row_cid, [4, 5])
        top_7_sector_comany_name = row["name"]
        top_3_industry_comany_name = row["name"]


        name_of_most_positive_dataCollectionTypeNme = mdb.query("cid in @row_cid").sort_values(
            by=["cash_flow", "income_statement", "balance_sheet"], ascending=False
        )
        name = row["name"]
        industry = row["industry"]
        sector = row["sector"]
        region = row["region"]
        country = row["country"]
        isin = row["isin"]
        positive = random.choice(text.positive)
        negative = random.choice(text.negative)

        # TODO: Do we need it?
        neutral = random.choice(text.neutral)

        # TODO: Need to clarify and update increase and decreased values
        increased_decreased = None
        most_positive_param_name = int(
            final.sort_values(by=["assets_assetsc_raw"], ascending=False).iloc[0][
                "assets_assetsc_raw"
            ]
        )
        most_positive_param_pct = int(
            final.sort_values(by=["assets_assetsc_pct"], ascending=False).iloc[0][
                "assets_assetsc_pct"
            ]
        )
        most_positive_param_coeff = int(
            final.sort_values(by=["assets_assetsc"], ascending=False).iloc[0][
                "assets_assetsc"
            ]
        )
        most_negative_param_name = int(
            final.sort_values(by=["assets_assetsc_raw"], ascending=True).iloc[0][
                "assets_assetsc_raw"
            ]
        )
        most_negative_param_pct = int(
            final.sort_values(by=["assets_assetsc_pct"], ascending=True).iloc[0][
                "assets_assetsc_pct"
            ]
        )
        most_negative_param_coeff = int(
            final.sort_values(by=["assets_assetsc"], ascending=True).iloc[0][
                "assets_assetsc"
            ]
        )
        if row["instance"] == "Original":
            report_type = "Official"
        else:
            report_type = row["instance"]
        generated_text = text.generate_text(
            event=input["event"],
            text_version=input["text_version"],
            importance=input["importance"],
            peer_name=name,
            top_7_sector_comany_name=top_7_sector_comany_name,
            top_3_industry_comany_name=top_3_industry_comany_name,
            company_name=name,
            positive_adjective=positive,
            negative_adjective=negative,
            percent= row['top_percent'] if "top_percent" in row else None,
            report_type=report_type,
            number_of_last_underperform_in_a_row=number_of_last_underperform_in_a_row,
            most_positive_param_name=most_positive_param_name,
            increased_decreased=increased_decreased,
            most_positive_param_pct=most_positive_param_pct,
            most_positive_param_coeff=most_positive_param_coeff,
            number_of_last_buy_and_strongbuy_in_a_row=number_of_last_buy_and_strongbuy_in_a_row,
            most_negative_param_name=most_negative_param_name,
            most_negative_param_pct=most_negative_param_pct,
            name_of_most_positive_dataCollectionTypeNme=name_of_most_positive_dataCollectionTypeNme.iloc[
                0
            ]['name'],
            most_negative_param_coeff=most_negative_param_coeff,
            industry=industry,
            sector=sector,
            region=region,
            country=country,
            isin=isin,
        )
        generated_texts.append(generated_text)


    return generated_texts
