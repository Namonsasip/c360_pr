from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l2.to_l2_nodes import *




def geo_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l2_number_of_bs_used,
                ["l1_geo_number_of_bs_used_for_l2_geo_number_of_bs_used",
                 "params:l2_number_of_bs_used"],
                # "xxxx"
                "l2_geo_number_of_bs_used"
                # "l1_int_geo_number_of_bs_used"
            ),


            node(
                l2_number_of_location_with_transactions,
                ["l1_geo_number_of_location_with_transactions_for_l2_geo_number_of_location_with_transactions",
                 "params:l2_number_of_location_with_transactions"],
                "l2_geo_number_of_location_with_transactions"
            ),

            node(
                l2_geo_voice_distance_daily,
                ["l1_geo_voice_distance_daily_intermediate_for_l2_geo_voice_distance_daily",
                 "params:l2_voice_distance_daily"],
                "l2_geo_voice_distance_daily"

            ),

            node(
                l2_first_data_session_cell_identifier_weekly,
                ["l1_geo_first_data_session_cell_identifier_daily_for_l2_geo_first_data_session_cell_identifier_weekly",
                 "params:l2_first_data_session_cell_identifier"],
                "l2_geo_first_data_session_cell_identifier_weekly"

            ),

            node(
                l2_geo_data_distance_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_distance_weekly",
                 "params:l2_data_distance_weekly"],
                "l2_geo_data_distance_weekly"

            ),

            node(
                l2_geo_data_distance_weekday_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_distance_weekday_weekly",
                 "params:l2_data_distance_weekday_weekly"],
                "l2_geo_data_distance_weekday_weekly"

            ),

            node(
                l2_geo_data_distance_weekend_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_distance_weekend_weekly",
                 "params:l2_data_distance_weekend_weekly"],
                "l2_geo_data_distance_weekend_weekly"

            ),

            node(  # weekday 434,437,443,449,452,455,458,461,464
                l2_geo_data_frequent_cell_weekday_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_frequent_cell_weekday_weekly",
                 "params:l2_data_frequent_cell_weekday_weekly"],
                "l2_geo_data_frequent_cell_weekday_weekly"

            ),

            node(  # weekend 435,439,445,450,453,456,459,462,465
                l2_geo_data_frequent_cell_weekend_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_frequent_cell_weekend_weekly",
                 "params:l2_data_frequent_cell_weekend_weekly"],
                "l2_geo_data_frequent_cell_weekend_weekly"

            ),

            node(  # all 436, 441,447,451,454,457,460,463
                l2_geo_data_frequent_cell_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_frequent_cell_weekly",
                 "params:l2_data_frequent_cell_weekly"],
                "l2_geo_data_frequent_cell_weekly"

            ),



            node(
                l2_geo_call_count_location_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_call_count_location_weekly",
                 "l1_geo_favorite_cell_master_table",
                 "params:l2_geo_call_count_location_weekly"],
                "l2_geo_call_count_location_weekly"

            ),

            node(
                l2_geo_data_traffic_location_weekly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_traffic_location_weekly",
                 "l1_geo_favorite_cell_master_table",
                 "params:l2_geo_data_traffic_location_weekly"],
                "l2_geo_data_traffic_location_weekly"

            ),

            node(
                l2_geo_data_share_location_weekly,
                ["l1_geo_voice_distance_daily_intermediate_for_l1_geo_data_share_location_daily",
                 "l1_geo_favorite_cell_master_table",
                 "params:l2_geo_data_share_location_weekly"],
                "l2_geo_data_share_location_weekly"
            ),

        ], name="geo_to_l2_pipeline"
    )



#backup
# node(  # 4g_weekday 438,444
#     l2_geo_data_frequent_cell_4g_weekday_weekly,
#     ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_frequent_cell_4g_weekday_weekly",
#      "params:l2_data_frequent_cell_4g_weekday_weekly"],
#     "l2_geo_data_frequent_cell_4g_weekday_weekly"
#
# ),
#
# node(  # 4g_weekend 440,446
#     l2_geo_data_frequent_cell_4g_weekend_weekly,
#     ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_frequent_cell_4g_weekend_weekly",
#      "params:l2_data_frequent_cell_4g_weekend_weekly"],
#     "l2_geo_data_frequent_cell_4g_weekend_weekly"
#
# ),
#
# node(  # 4g_all 442,448
#     l2_geo_data_frequent_cell_4g_weekly,
#     ["l1_geo_usage_sum_data_location_dow_intermediate_for_l2_geo_data_frequent_cell_4g_weekly",
#      "params:l2_data_frequent_cell_4g_weekly"],
#     "l2_geo_data_frequent_cell_4g_weekly"
#
# ),