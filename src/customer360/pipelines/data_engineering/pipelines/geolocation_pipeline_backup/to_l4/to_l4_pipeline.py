from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes_backup.to_l4.to_l4_nodes import *




def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                l4_rolling_window,
                ["l2_geo_number_of_bs_used_for_l4_geo_number_of_bs_used",
                 "params:l4_number_of_bs_used"],
                "l4_geo_number_of_bs_used"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_number_of_location_with_transactions_for_l4_geo_number_of_location_with_transactions",
                 "params:l4_number_of_location_with_transactions"],
                "l4_geo_number_of_location_with_transactions"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_voice_distance_daily_for_l4_geo_voice_distance_daily",
                 "params:l4_voice_distance_daily"],
                "l4_geo_voice_distance_daily"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_data_distance_weekly_for_l4_geo_data_distance",
                 "params:l4_data_distance_monthly"],
                "l4_geo_data_distance"

            ),

            # node(
            #     l4_rolling_window,
            #     ["l2_geo_data_distance_weekday_weekly_for_l4_geo_data_distance_weekday",
            #      "params:l4_data_distance_monthly"],
            #     "l4_geo_data_distance_weekday"
            #
            # ),

            node(
                l4_rolling_window,
                ["l2_geo_data_distance_weekend_weekly_for_l4_geo_data_distance_weekend",
                 "params:l4_data_distance_monthly"],
                "l4_geo_data_distance_weekend"

            ),

            # node(  # weekday 434,437,443,449,452,455,458,461,464
            #     l4_rolling_window,
            #     ["l2_geo_data_frequent_cell_weekday_weekly_for_l4_geo_data_frequent_cell_weekday",
            #      "params:l4_data_frequent_cell_weekday"],
            #     "l4_geo_data_frequent_cell_weekday"
            #
            # ),
            #
            node(  # weekend 435,439,445,450,453,456,459,462,465
                l4_rolling_window,
                ["l2_geo_data_frequent_cell_weekend_weekly_for_l4_geo_data_frequent_cell_weekend",
                 "params:l4_data_frequent_cell_weekend"],
                "l4_geo_data_frequent_cell_weekend"

            ),

            node(  # all 436, 441,447,451,454,457,460,463
                l4_rolling_window,
                ["l2_geo_data_frequent_cell_weekly_for_l4_geo_data_frequent_cell",
                 "params:l4_data_frequent_cell"],
                "l4_geo_data_frequent_cell"

            ),



            node(
                l4_rolling_window,
                ["l2_geo_call_count_location_weekly_for_l4_geo_call_count_location",
                 "params:l4_geo_call_count_location"],
                "l4_geo_call_count_location"

            ),

            node(
                l4_rolling_window,
                ["l2_geo_data_traffic_location_weekly_for_l4_geo_data_traffic_location",
                 "params:l4_geo_data_traffic_location"],
                "l4_geo_data_traffic_location"

            ),

            node(
                l4_rolling_window,
                ["l2_geo_data_share_location_weekly",
                 "params:l4_geo_data_share_location"],
                "l4_geo_data_share_location"
            ),

        ], name="geo_to_l4_pipeline"
    )



# backup
# node(  # 4g_weekday 438,444
#     l4_rolling_window,
#     ["l2_geo_data_frequent_cell_4g_weekday_weekly_for_l4_geo_data_frequent_cell_4g_weekday",
#      "params:l4_data_frequent_cell_4g_weekday"],
#     "l4_geo_data_frequent_cell_4g_weekday"
#
# ),
#
# node(  # 4g_weekend 440,446
#     l4_rolling_window,
#     ["l2_geo_data_frequent_cell_4g_weekend_weekly_for_l4_geo_data_frequent_cell_4g_weekend",
#      "params:l4_data_frequent_cell_4g_weekend"],
#     "l4_geo_data_frequent_cell_4g_weekend"
#
# ),
#
# node(  # 4g_all 442,448
#     l4_rolling_window,
#     ["l2_geo_data_frequent_cell_4g_weekly_for_l4_geo_data_frequent_cell_4g",
#      "params:l4_data_frequent_cell_4g"],
#     "l4_geo_data_frequent_cell_4g"
#
# ),