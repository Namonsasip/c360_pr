from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *




def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l3_geo_time_spent_by_location_monthly,
                ["l1_geo_time_spent_by_location_daily_for_l3_geo_time_spent_by_location_monthly",
                 "params:l3_geo_time_spent_by_location_monthly"
                 ],
                "l3_geo_time_spent_by_location_monthly"
            ),

            node(
                l3_geo_area_from_ais_store_monthly,
                ["l1_geo_area_from_ais_store_daily_for_l3_geo_area_from_ais_store_monthly",
                 "params:l3_area_from_ais_store_monthly"
                 ],
                "l3_geo_area_from_ais_store_monthly"
            ),

            node(
                l3_geo_area_from_competitor_store_monthly,
                ["l1_geo_area_from_competitor_store_daily_for_l3_geo_area_from_competitor_store_monthly",
                 "params:l3_area_from_competitor_store_monthly"
                 ],
                "l3_geo_area_from_competitor_store_monthly"
            ),
            ##==============================Update 2020-06-12 by Thatt529==========================================##

            ###total_distance_km###
            node(
                l3_geo_total_distance_km_monthly,
                ["l1_geo_total_distance_km_daily_for_l3_geo_total_distance_km_monthly",
                 "params:l3_geo_total_distance_km_monthly"
                 ],
                "l3_geo_total_distance_km_monthly"
            ),

            ###Traffic_fav_location###
            # node(
            #     l3_geo_use_Share_traffic_monthly,
            #     ["l3_geo_use_traffic_home_work_monthly_for_l3_geo_Share_traffic_monthly",
            #      "params:l3_geo_use_traffic_home_work_weekly"
            #      ],
            #     "l3_geo_Share_traffic_monthly"
            # ),

            ###feature_sum_voice_location###
            node(
                l3_geo_call_location_home_work_monthly,
                ["l1_geo_call_location_home_work_daily_for_l3_geo_call_location_home_work_monthly",
                 "params:l3_geo_call_location_home_work_monthly"
                 ],
                "l3_geo_call_location_home_work_monthly"
            ),

            # ==============================Update 2020-06-15 by Thatt529==========================================##

            ##Top_3_cells_on_voice_usage###
            node(
                l3_geo_top3_cells_on_voice_usage,
                ["l1_geo_top3_cells_on_voice_usage",
                 "params:l3_geo_top3_cells_on_voice_usage"
                 ],
                "l3_geo_top3_cells_on_voice_usage"
            ),

            ##==============================Update 2020-06-17 by Thatt529==========================================##

            ##distance_top_call###
            node(
                node_from_config,
                ["l1_geo_distance_top_call",
                 "params:l3_geo_distance_top_call"
                 ],
                "l3_geo_distance_top_call"
            ),

            # 47 The favourite location
            node(
                l3_the_favourite_locations_monthly,
                ["l1_the_favourite_locations_daily"],
                "l3_the_favourite_locations_monthly"
            )

        ], name="geo_to_l3_pipeline"
    )
