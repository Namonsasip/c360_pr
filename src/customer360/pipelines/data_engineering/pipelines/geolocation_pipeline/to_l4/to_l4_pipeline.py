from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *
from customer360.utilities.re_usable_functions import l1_massive_processing


def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_time_spent_by_location_weekly",
                 "params:l4_geo_time_spent_by_location"
                 ],
                "l4_geo_time_spent_by_location"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_time_spent_by_store_weekly",
                 "params:l4_geo_time_spent_by_store"
                 ],
                "l4_geo_time_spent_by_store"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_count_visit_by_location_weekly",
                 "params:l4_geo_count_visit_by_location"
                 ],
                "l4_geo_count_visit_by_location"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_total_distance_km_weekly",
                 "params:l4_geo_total_distance_km"
                 ],
                "l4_geo_total_distance_km"
            ),

            ### FINISH
            node(
                node_from_config,
                ["l3_geo_home_work_location_id_monthly",
                 "params:l4_geo_home_work_location_id"
                 ],
                "l4_geo_home_work_location_id"
            ),

            ### FINISH
            node(
                node_from_config,
                ["l3_geo_top3_visit_exclude_hw_monthly",
                 "params:l4_geo_top3_visit_exclude_hw"
                 ],
                "l4_geo_top3_visit_exclude_hw"
            ),

            ### FINISH
            node(
                node_from_config,
                ["l3_geo_work_area_center_average_monthly",
                 "params:l4_geo_work_area_center_average"
                 ],
                "l4_geo_work_area_center_average"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l3_geo_home_weekday_city_citizens_monthly",
                 "params:l4_geo_home_weekday_city_citizens"
                 ],
                "l4_geo_home_weekday_city_citizens"
            ),

            # ### WAIT
            # node(
            #     node_from_config,
            #     ["l3_geo_visit_ais_store_location_monthly",
            #      "params:l4_geo_visit_ais_store_location"
            #      ],
            #     "l4_geo_visit_ais_store_location"
            # ),

            # ### WAIT
            # node(
            #     l4_geo_top3_voice_location,
            #     ["l2_geo_top3_voice_location_weekly",
            #      "params:l4_geo_top3_voice_location"
            #      ],
            #     "l4_geo_top3_voice_location"
            # ),

            # ### WAIT
            # node(
            #     node_from_config,
            #     ["l3_geo_use_traffic_favorite_location_monthly",
            #      "params:l4_geo_use_traffic_favorite_location"
            #      ],
            #     "l4_geo_use_traffic_favorite_location"
            # ),


        ], name="geo_to_l4_pipeline"
    )