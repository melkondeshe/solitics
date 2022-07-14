positive = [
    "Great",
    "Excellent",
    "Impressive",
    "Splendid",
    "Very good",
    "Sensational",
    "Magnificent",
    "Outstanding",
    "Spectacular",
    "Extraordinary",
]
# strong_buy = ['Sensational', 'Magnificent', 'Outstanding', 'Spectacular', 'Extraordinary']
underperform = ["disappointing", "inferior", "very bad", "unsatisfactory", "poor"]
underperform_and_total_assessment_lower_than_50 = [
    "terrible",
    "horrible",
    "very bad",
    "inadequate",
]
negative = [
    "disappointing",
    "inferior",
    "very bad",
    "unsatisfactory",
    "poor",
    "terrible",
    "horrible",
    "very bad",
    "inadequate",
]
hold = ["medicare", "not very impressive", "fair", "average"]
neutral = ["medicare", "not very", "impressive", "fair", "average"]


def generate_text(**kwargs):
    version = {
        "portfolio": {
            "text_1": f"{kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with very {kwargs['positive_adjective']} fundamentals compeared to their pervious report which was {kwargs['negative_adjective']}, the new report places {kwargs['company_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_2": f"{kwargs['company_name']} which is part of your portflio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with {kwargs['positive_adjective']} fundamentals following the pervious report, which was also {kwargs['positive_adjective']} which places {kwargs['company_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_3": f"{kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with {kwargs['positive_adjective']} fundamentals that place {kwargs['company_name']} in the top {1/5/10}% relative to all its competitors. [CTA]",
            "text_4": f"{kwargs['company_name']} which is part of your portfolio has just released another {kwargs['negative_adjective']} {kwargs['report_type']} report, with a continued trend of the last {kwargs['number_of_last_underperform_in_a_row']}  quarters, which places {kwargs['company_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_5": f"{kwargs['company_name']} which is part of your portfolio has just released {kwargs['negative_adjective']} {kwargs['report_type']} report with {kwargs['negative_adjective']} fundamentals compared pervious report, which was {kwargs['positive_adjective']}. Unfortunately the new report places {kwargs['company_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_6": f"{kwargs['company_name']} which is part of your portfolio has just released {kwargs['negative_adjective']} {kwargs['report_type']} report with {kwargs['negative_adjective']} fundamentals which places {kwargs['company_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_7": f"{kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, compared to their previous report which was {kwargs['negative_adjective']}. One of the figures to be noted is their {kwargs['most_positive_param_name']}, which {kwargs['increased_decreased']} by {kwargs['most_positive_param_pct']}%, which historically have a {kwargs['most_positive_param_coeff']} chance of impacting the stock price.",
            "text_8": f"{kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, like their previous report which was also {kwargs['negative_adjective']}. This trend which has continued for the last {kwargs['number_of_last_buy_and_strongbuy_in_a_row']} quarters indicates for strong and solid fundamentals. [CTA]",
            "text_9": f"{kwargs['company_name']} which is part of your portfolio has just released a {kwargs['negative_adjective']} {kwargs['report_type']} report compared to the previous report, which was {kwargs['positive_adjective']}. It's a warning sign might indicate that a negative trend may be starting. [CTA]",
            "text_10": f"{kwargs['company_name']} which is part of your portfolio has just released a {kwargs['negative_adjective']} {kwargs['report_type']} report. One of the parameters that stood out was {kwargs['most_negative_param_name']} which {kwargs['increased_decreased']} by {kwargs['most_negative_param_pct']}%, which historically has a {kwargs['most_negative_param_coeff']}% chance of impacting the stock price.",
            "text_11": f"{kwargs['company_name']} which is part of your portfolio has just released very {kwargs['negative_adjective']} {kwargs['report_type']} report, like their previous report which was also {kwargs['negative_adjective']}. This trend which has continued for the last {kwargs['number_of_last_underperform_in_a_row']} quarters indicates negative momentum and uncerainty. [CTA]",
        },
        "top_3_industry": {
            "text_1": f"{kwargs['top_3_industry_comany_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, placing it among the top 3 {kwargs['industry']} stocks. One of the figures to be noted is their {kwargs['most_positive_param_name']} which decreased by {kwargs['most_positive_param_pct']}%, which historically have a {kwargs['most_positive_param_coeff']}% chance of impacting the stock price.",
            "text_2": f"{kwargs['top_3_industry_comany_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, like their previous report which was also {kwargs['positive_adjective']}. This trend has continued for the last {kwargs['number_of_last_buy_and_strongbuy_in_a_row']} quarters, placing {kwargs['top_3_industry_comany_name']} among the top 3 {kwargs['industry']} stocks. [CTA]",
            "text_3": f"{kwargs['top_3_industry_comany_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, compared the previous report which was {kwargs['negative_adjective']}. This improvement in {kwargs['top_3_industry_comany_name']} fundamnetals placing it among the top 3 {kwargs['industry']} stocks. [CTA]",
        },
        "top_7_sector": {
            "text_1": f"{kwargs['top_7_sector_comany_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, placing it among the top 7 {kwargs['sector']} stocks. One of the figures to be noted is their {kwargs['most_positive_param_name']} which decreased by {kwargs['most_positive_param_pct']}%, which historically have a {kwargs['most_positive_param_coeff']}% chance of impacting the stock price.",
            "text_2": f"{kwargs['top_7_sector_comany_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, like their previous report which was also {kwargs['positive_adjective']}. This trend has continued for the last {kwargs['number_of_last_buy_and_strongbuy_in_a_row']} quarters, placing {kwargs['top_7_sector_comany_name']} among the top 7 {kwargs['sector']} stocks. [CTA]",
            "text_3": f"{kwargs['top_7_sector_comany_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, compared the previous report which was {kwargs['negative_adjective']}. This improvement in {kwargs['top_7_sector_comany_name']} fundamnetals placing it among the top 7 {kwargs['sector']} stocks. [CTA]",
        },
        "peers": {
            "text_1": f"{kwargs['peer_name']} has just released an {kwargs['positive_adjective']} {kwargs['report_type']} report compared to {kwargs['company_name']}, which is part of your portfolio. The significant difference between the two was the {kwargs['name_of_most_positive_dataCollectionTypeNme']}, which was {kwargs['positive_adjective']} better in Samsung's report [CTA]",
            "text_2": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with very {kwargs['positive_adjective']} fundamentals compeared to their pervious report which was {kwargs['negative_adjective']}, the new report place {kwargs['company_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_3": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with {kwargs['positive_adjective']} fundamentals following the pervious report, which was also {kwargs['positive_adjective']} which places {kwargs['peer_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_4": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with {kwargs['positive_adjective']} fundamentals that place {kwargs['peer_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_5": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released another {kwargs['negative_adjective']} {kwargs['report_type']} report, with a continued trend of the last {kwargs['number_of_last_underperform_in_a_row']}  quarters, which places {kwargs['peer_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_6": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released {kwargs['negative_adjective']} {kwargs['report_type']} report with {kwargs['negative_adjective']} fundamentals compared pervious report, which was {kwargs['positive_adjective']}. Unfortunately the new report places {kwargs['peer_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_7": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released {kwargs['negative_adjective']} {kwargs['report_type']} report with {kwargs['negative_adjective']} fundamentals what places {kwargs['peer_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_8": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, compared to their previous report which was {kwargs['negative_adjective']}. One of the figures to be noted is their {kwargs['most_positive_param_name']}, which {kwargs['increased_decreased']} by {kwargs['most_positive_param_pct']}%, which historically have a {kwargs['most_positive_param_coeff']} chance of impacting the stock price.",
            "text_9": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, join their previous report which was also {kwargs['negative_adjective']}. This trend which continue for the last {kwargs['number_of_last_buy_and_strongbuy_in_a_row']} quarters indicates for a strong and solid fundamentals. [CTA]",
            "text_10": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['negative_adjective']} {kwargs['report_type']} report compared to the previous report, which was {kwargs['positive_adjective']}. It's a warning sign might indicate that negative trend may be starting. [CTA]",
            "text_11": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released a {kwargs['negative_adjective']} {kwargs['report_type']} report. One of the parameters that stood out was {kwargs['most_negative_param_name']} which {kwargs['increased_decreased']} by {kwargs['most_negative_param_pct']}%. It's a warning sign that might indicate a negative momentum and uncertainty. [CTA]",
            "text_12": f"{kwargs['peer_name']}, a direct competitor of {kwargs['company_name']} which is part of your portfolio has just released very {kwargs['negative_adjective']} {kwargs['report_type']} report, join their previous report which was also {kwargs['negative_adjective']}. This trend which continue for the last {kwargs['number_of_last_underperform_in_a_row']} quarters indicates for a negative momentum and uncerainty. [CTA]",
        },
        "large_market_cap": {
            "text_1": f"{kwargs['company_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with very {kwargs['positive_adjective']} fundamentals compeared to their pervious report which was {kwargs['negative_adjective']}, the new report places {kwargs['company_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_2": f"{kwargs['company_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with {kwargs['positive_adjective']} fundamentals following the pervious report, which was also {kwargs['positive_adjective']} which places {kwargs['company_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_3": f"{kwargs['company_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report with {kwargs['positive_adjective']} fundamentals that places {kwargs['company_name']} in the top {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_4": f"{kwargs['company_name']} has just released another {kwargs['negative_adjective']} {kwargs['report_type']} report,continuing the trend of the last {kwargs['number_of_last_underperform_in_a_row']}  quarters, which places {kwargs['company_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_5": f"{kwargs['company_name']} has just released {kwargs['negative_adjective']} {kwargs['report_type']} report with {kwargs['negative_adjective']} fundamentals compared to the pervious report, which was {kwargs['positive_adjective']}. Unfortunately the new report places {kwargs['company_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_6": f"{kwargs['company_name']} has just released {kwargs['negative_adjective']} {kwargs['report_type']} report with {kwargs['negative_adjective']} fundamentals placing {kwargs['company_name']} in the bottom {kwargs['percent']}% relative to all its competitors. [CTA]",
            "text_7": f"{kwargs['company_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, compared to their previous report which was {kwargs['negative_adjective']}. One of the figures to be noted is their {kwargs['most_positive_param_name']}, which {kwargs['increased_decreased']} by {kwargs['most_positive_param_pct']}%, which historically have a {kwargs['most_positive_param_coeff']} chance of impacting the stock price.",
            "text_8": f"{kwargs['company_name']} has just released a {kwargs['positive_adjective']} {kwargs['report_type']} report, join their previous report which was also {kwargs['negative_adjective']}. This trend which continue for the last {kwargs['number_of_last_buy_and_strongbuy_in_a_row']} quarters indicates for a strong and solid fundamentals. [CTA]",
            "text_9": f"{kwargs['company_name']} has just released a {kwargs['negative_adjective']} {kwargs['report_type']} report compared to the previous report, which was {kwargs['positive_adjective']}. It's a warning sign which might indicate that negative trend may be starting. [CTA]",
            "text_10": f"{kwargs['company_name']} has just released a {kwargs['negative_adjective']} {kwargs['report_type']} report. One of the parameters that stood out was {kwargs['most_negative_param_name']} which {kwargs['increased_decreased']} by {kwargs['most_negative_param_pct']}%, which historically has a {kwargs['most_negative_param_coeff']}% chance of impacting the stock price.",
            "text_11": f"{kwargs['company_name']} has just released a very {kwargs['negative_adjective']} {kwargs['report_type']} report, like their previous report which was also {kwargs['negative_adjective']}. This trend which has continued for the last {kwargs['number_of_last_underperform_in_a_row']} quarters indicates negative momentum and uncerainty. [CTA]",
        },
    }
    event = kwargs["event"]
    text_version = kwargs["text_version"]
    body = {
        "type": kwargs["event"],
        "company": kwargs["company_name"],
        "company_isin": kwargs["isin"],
        "peer_isin": "msft_isin",
        "sector": kwargs["sector"],
        "industry": kwargs["industry"],
        "country": kwargs["country"],
        "region": kwargs["region"],
        "report_type": kwargs["report_type"],
        "body": version[event][text_version],
        "importance": kwargs["importance"],
    }
    return body
