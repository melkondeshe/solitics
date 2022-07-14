all_inputs = [
    [
        {
            # For 2rd rule
            ### rule will be passed if recommendation of row is recommendation or rec_2
            "recommendation": [4, 5],
            # For 3rd rule
            ### rule will be passed if previous recommendation of row was recommendation_was or recommendation_was or rec_was_3
            "recommendation_was": [3,2,1],
            # For 4th rule
            ### rule will be passed if one of the peers is in the top 1/5/10
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "portfolio",
            "text_version": "text_1",
            "importance": 1,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "portfolio",
            "text_version": "text_2",
            "importance": 1,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "portfolio",
            "text_version": "text_3",
            "importance": 1,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_4",
            "importance": 1,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_5",
            "importance": 1,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_6",
            "importance": 1,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,3],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_7",
            "importance": 1,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_8",
            "importance": 1,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_9",
            "importance": 1,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_10",
            "importance": 1,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "portfolio",
            "text_version": "text_11",
            "importance": 1,
        },
    ],
    [
        {
            "recommendation": [4,5],
            "recommendation_was": [3,None,None],
            "part_of_top3": True,
            "event": "top_3_industry",
            "text_version": "text_1",
            "importance": 2,
        },
        {
            "recommendation": [4, 5],
            "recommendation_was": [4,5,None],
            "part_of_top3": True,
            "event": "top_3_industry",
            "text_version": "text_2",
            "importance": 2,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,None],
            "part_of_top3": True,
            "event": "top_3_industry",
            "text_version": "text_3",
            "importance": 2,
        },
    ],
    [
        {
            "recommendation": [4,5],
            "recommendation_was": [3,None,None],
            "part_of_top7": True,
            "event": "top_7_sector",
            "text_version": "text_1",
            "importance": 3,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [None,None,None],
            "part_of_top7": True,
            "event": "top_7_sector",
            "text_version": "text_2",
            "importance": 3,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,None],
            "part_of_top7": True,
            "event": "top_7_sector",
            "text_version": "text_3",
            "importance": 3,
        },
    ],
    [
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "rule_3": True,
            "event": "peers",
            "text_version": "text_1",
            "importance": 4,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "peers",
            "text_version": "text_2",
            "importance": 4,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "peers",
            "text_version": "text_3",
            "importance": 4,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "peers",
            "text_version": "text_4",
            "importance": 4,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_5",
            "importance": 4,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_6",
            "importance": 4,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_7",
            "importance": 4,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,3],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_8",
            "importance": 4,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_9",
            "importance": 4,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_10",
            "importance": 4,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_11",
            "importance": 4,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "peers",
            "text_version": "text_12",
            "importance": 4,
        },
    ],
    [
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "large_market_cap",
            "text_version": "text_1",
            "importance": 5,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "large_market_cap",
            "text_version": "text_2",
            "importance": 5,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": False,
            "event": "large_market_cap",
            "text_version": "text_3",
            "importance": 5,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_4",
            "importance": 5,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_5",
            "importance": 5,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": True,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_6",
            "importance": 5,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [1,2,3],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_7",
            "importance": 5,
        },
        {
            "recommendation": [4,5],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_8",
            "importance": 5,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [4,5,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_9",
            "importance": 5,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [3,None,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_10",
            "importance": 5,
        },
        {
            "recommendation": [1,2],
            "recommendation_was": [1,2,None],
            "has_peers_filtering": False,
            "is_final_assessment_ascending": True,
            "event": "large_market_cap",
            "text_version": "text_11",
            "importance": 5,
        },
    ],
]
