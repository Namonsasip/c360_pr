from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes_backup.to_l1.to_l1_nodes import *


def geo_to_l1_intermediate_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l1_geo_favorite_cell_master_table,
                ["l0_usage_sum_voice_location_daily_for_l1_geo_favorite_cell_master_table",
                 "l0_usage_sum_data_location_daily_for_l1_geo_favorite_cell_master_table"],
                "l1_geo_favorite_cell_master_table"
            ),

            node(
                l1_geo_voice_distance_daily_intermediate,  # added cgi_partial
                ["l0_usage_sum_voice_location_daily_for_l1_geo_voice_distance_daily_intermediate",
                 ],
                "l1_geo_voice_distance_daily_intermediate"

            ),

            node(
                l1_usage_sum_data_location_dow_intermediate,  # added cgi_partial
                ["l0_usage_sum_data_location_daily_for_l1_geo_usage_sum_data_location_dow_intermediate",
                 ],
                "l1_geo_usage_sum_data_location_dow_intermediate"

            ),
        ],
        name="geo_to_l1_intermediate_pipeline"
    )


def geo_to_l1_pipeline(**kwargs):
    return Pipeline(
        [

            # number_of_bs_used
            node(
                l1_geo_number_of_bs_used,
                ["l0_geo_cust_cell_visit_time_daily_for_l1_geo_number_of_bs_used",
                 "l0_mst_cell_masterplan",
                 "params:l1_number_of_bs_used"],
                "l1_geo_number_of_bs_used"
            ),

            node(
                l1_number_of_location_with_transactions,
                ["l1_geo_voice_distance_daily_intermediate",
                 "l1_geo_usage_sum_data_location_dow_intermediate",
                 "params:l1_number_of_location_with_transactions"],
                # "xxxxxxxxxx"
                "l1_geo_number_of_location_with_transactions"
            ),

            node(
                l1_geo_voice_distance_daily,
                ["l1_geo_voice_distance_daily_intermediate_for_l1_geo_voice_distance_daily",
                 "params:l1_voice_distance_daily"],
                "l1_geo_voice_distance_daily"

            ),

            node(
                l1_first_data_session_cell_identifier_daily,  # added cgi_partial
                ["l0_usage_sum_data_location_daily_for_l1_geo_first_data_session_cell_identifier_daily",
                 "params:l1_first_data_session_cell_identifier"],
                "l1_geo_first_data_session_cell_identifier_daily"

            ),

            node(
                l1_geo_data_distance_daily,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l1_geo_data_distance_daily",
                 "params:l1_data_distance_daily"],
                "l1_geo_data_distance_daily"

            ),

            node(
                l1_geo_data_distance_weekday_daily,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l1_geo_data_distance_weekday_daily",
                 "params:l1_data_distance_weekday_daily"],
                "l1_geo_data_distance_weekday_daily"

            ),

            node(
                l1_geo_data_distance_weekend_daily,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l1_geo_data_distance_weekend_daily",
                 "params:l1_data_distance_weekend_daily"],
                "l1_geo_data_distance_weekend_daily"

            ),

            node(
                l1_geo_call_count_location_daily,
                ["l1_geo_voice_distance_daily_intermediate_for_l1_geo_call_count_location_daily",
                 "l1_geo_favorite_cell_master_table",
                 "params:l1_geo_call_count_location_daily"],
                "l1_geo_call_count_location_daily"
            ),

            node(
                l1_geo_data_traffic_location_daily,
                ["l1_geo_voice_distance_daily_intermediate_for_l1_geo_data_traffic_location_daily",
                 "l1_geo_favorite_cell_master_table",
                 "params:l1_geo_data_traffic_location_daily"],
                "l1_geo_data_traffic_location_daily"
            ),

            node(
                l1_geo_data_share_location_daily,
                ["l1_geo_voice_distance_daily_intermediate_for_l1_geo_data_share_location_daily",
                 "l1_geo_favorite_cell_master_table",
                 "params:l1_geo_data_share_location_daily"],
                "l1_geo_data_share_location_daily"
            ),

        ], name="geo_to_l1_pipeline"
    )


def geo_to_l1_union_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l1_union_features,
                ["l1_geo_number_of_location_with_transactions",
                 "l1_geo_voice_distance_daily",
                 "l1_geo_first_data_session_cell_identifier_daily",
                 "l1_geo_data_distance_daily",
                 "l1_geo_data_distance_weekday_daily",
                 "l1_geo_data_distance_weekend_daily",
                 "l1_geo_call_count_location_daily",
                 "l1_geo_data_traffic_location_daily",
                 "l1_geo_data_share_location_daily",
                 ],   # "l1_geo_number_of_bs_used",  imsi
                "l1_geo_union_features_daily"
            ),
        ], name="geo_to_l1_union_pipeline"
    )

# # Number of Location_id with transactions  <<<<<<<< backup
# node(
#     l1_number_of_location_with_transactions,
#     ["l0_geo_footfall_daily_for_l1_geo_number_of_location_with_transactions",
#      "l0_mst_cell_masterplan_for_l1_geo_number_of_location_with_transactions",
#      "params:l1_number_of_location_with_transactions"],
#     "l1_geo_number_of_location_with_transactions"
# ),
