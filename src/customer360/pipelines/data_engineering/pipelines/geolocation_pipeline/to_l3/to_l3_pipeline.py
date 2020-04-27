from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *




def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                l3_geo_voice_distance_daily,
                ["l1_geo_voice_distance_daily_intermediate_for_l3_geo_voice_distance_daily",
                 "params:l3_voice_distance_daily"],
                "l3_geo_voice_distance_daily"

            ),

            node(
                l3_first_data_session_cell_identifier_monthly,
                ["l1_geo_first_data_session_cell_identifier_daily_for_l3_geo_first_data_session_cell_identifier_monthly",
                 "params:l3_first_data_session_cell_identifier"],
                "l3_geo_first_data_session_cell_identifier_monthly"

            ),

            node(
                l3_geo_data_distance_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_distance_monthly",
                 "params:l3_data_distance_monthly"],
                "l3_geo_data_distance_monthly"

            ),

            node(
                l3_geo_data_distance_weekday_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_distance_weekday_monthly",
                 "params:l3_data_distance_weekday_monthly"],
                "l3_geo_data_distance_weekday_monthly"

            ),

            node(
                l3_geo_data_distance_weekend_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_distance_weekend_monthly",
                 "params:l3_data_distance_weekend_monthly"],
                "l3_geo_data_distance_weekend_monthly"

            ),

            node(
                l3_geo_home_work_location_id,
                ["l0_locals_homework_for_l3_geo_home_work_location_id",
                 "params:l3_geo_home_work_location_id"],
                "l3_geo_home_work_location_id"

            ),

            node(  # weekday 434,437,443,449,452,455,458,461,464
                l3_geo_data_frequent_cell_weekday_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_frequent_cell_weekday_monthly",
                 "params:l3_data_frequent_cell_weekday_monthly"],
                "32_geo_data_frequent_cell_weekday_monthly"

            ),

            node(  # weekend 435,439,445,450,453,456,459,462,465
                l3_geo_data_frequent_cell_weekend_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_frequent_cell_weekend_monthly",
                 "params:l3_data_frequent_cell_weekend_monthly"],
                "l3_geo_data_frequent_cell_weekend_monthly"

            ),

            node(  # all 436, 441,447,451,454,457,460,463
                l3_geo_data_frequent_cell_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_frequent_cell_monthly",
                 "params:l3_data_frequent_cell_monthly"],
                "l3_geo_data_frequent_cell_monthly"

            ),

            node(  # 4g_weekday 438,444
                l3_geo_data_frequent_cell_4g_weekday_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_frequent_cell_4g_weekday_monthly",
                 "params:l3_data_frequent_cell_4g_weekday_monthly"],
                "l3_geo_data_frequent_cell_4g_weekday_monthly"

            ),

            node(  # 4g_weekend 440,446
                l3_geo_data_frequent_cell_4g_weekend_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_frequent_cell_4g_weekend_monthly",
                 "params:l3_data_frequent_cell_4g_weekend_monthly"],
                "l3_geo_data_frequent_cell_4g_weekend_monthly"

            ),

            node(  # 4g_all 442,448
                l3_geo_data_frequent_cell_4g_monthly,
                ["l1_geo_usage_sum_data_location_dow_intermediate_for_l3_geo_data_frequent_cell_4g_monthly",
                 "params:l3_data_frequent_cell_4g_monthly"],
                "l3_geo_data_frequent_cell_4g_monthly"

            ),

        ], name="geo_to_l3_pipeline"
    )
