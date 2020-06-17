from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l2.to_l2_nodes import *




def geo_to_l2_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                l2_geo_time_spent_by_location_weekly,
                ["l1_geo_time_spent_by_location_daily_for_l2_geo_time_spent_by_location_weekly",
                 "params:l2_geo_time_spent_by_location_weekly"
                 ],
                "l2_geo_time_spent_by_location_weekly"
            ),

            node(
                l2_geo_area_from_ais_store_weekly,
                ["l1_geo_area_from_ais_store_daily_for_l2_geo_area_from_ais_store_weekly",
                 "params:l2_area_from_ais_store_weekly"
                 ],
                "l2_geo_area_from_ais_store_weekly"
            ),

            node(
                l2_geo_area_from_competitor_store_weekly,
                ["l1_geo_area_from_competitor_store_daily_for_l2_geo_area_from_competitor_store_weekly",
                 "params:l2_area_from_competitor_store_weekly"
                 ],
                "l2_geo_area_from_competitor_store_weekly"
            ),
            ##==============================Update 2020-06-12 by Thatt529==========================================##

            ###total_distance_km###
            node(
                l2_geo_total_distance_km_weekly,
                ["l1_geo_total_distance_km_daily_for_l2_geo_total_distance_km_weekly",
                 "params:l2_geo_total_distance_km_weekly"
                 ],
                "l2_geo_total_distance_km_weekly"
            ),

            ###Traffic_fav_location###
            # node(
            #     l2_geo_use_traffic_home_work_weekly,
            #     ["l1_geo_use_traffic_home_work",
            #      "params:l2_geo_use_traffic_home_work_weekly"
            #      ],
            #     "l2_geo_use_traffic_home_work_weekly"
            # ),

            ###Number_of_base_station###
            node(
                l2_geo_data_count_location_weekly,
                ["l1_int_geo_cust_cell_visit_time_for_l1_geo_number_of_bs_used",
                 "params:l2_geo_data_count_location_weekly"],
                "l2_geo_data_count_location_weekly"
            ),

            ###feature_sum_voice_location###
            node(
                l2_geo_call_home_work_location_weekly,
                ["l1_geo_call_location_home_work_daily_for_l2_geo_call_location_home_work_weekly",
                 "params:l2_geo_call_location_home_work_weekly"
                 ],
                "l2_geo_call_location_home_work_weekly"
            ),

            # ==============================Update 2020-06-15 by Thatt529==========================================##

            ##Top_3_cells_on_voice_usage###
            node(
                l2_geo_top3_cells_on_voice_usage,
                ["l1_geo_top3_cells_on_voice_usage",
                 "params:l2_geo_top3_cells_on_voice_usage"
                 ],
                "l2_geo_top3_cells_on_voice_usage"
            ),

            node(
                l2_geo_cust_subseqently_distance_weekly,
                ["l1_geo_cust_subseqently_distance_daily_for_l2_geo_cust_subseqently_distance_weekly",
                 "params:l2_geo_cust_subseqently_distance_weekly"
                 ],
                "l2_geo_cust_subseqently_distance_weekly"
            )

        ], name="geo_to_l2_pipeline"
    )

