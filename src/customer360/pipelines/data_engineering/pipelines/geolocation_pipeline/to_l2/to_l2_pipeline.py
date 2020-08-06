from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l2.to_l2_nodes import *


def geo_to_l2_pipeline(**kwargs):
    return Pipeline(
        [

            ### WAIT
            node(
                l2_geo_time_spent_by_location_weekly,
                ["l1_geo_time_spent_by_location_daily",
                 "params:l2_geo_time_spent_by_location_weekly"
                 ],
                "l2_geo_time_spent_by_location_weekly"
            ),
            # node(
            #     l2_geo_time_spent_by_location_weekly,
            #     ["l1_geo_time_spent_by_location_daily_for_l2_geo_time_spent_by_location_weekly",
            #      "params:l2_geo_time_spent_by_location_weekly"
            #      ],
            #     "l2_geo_time_spent_by_location_weekly"
            # ),

            ### WAIT
            node(
                l2_geo_time_spent_by_store_weekly,
                ["l1_geo_time_spent_by_store_daily",
                 "params:l2_geo_time_spent_by_store_weekly"
                 ],
                "l2_geo_time_spent_by_store_weekly"
            ),
            # node(
            #     l2_geo_area_from_ais_store_weekly,
            #     ["l1_geo_area_from_ais_store_daily_for_l2_geo_area_from_ais_store_weekly",
            #      "params:l2_area_from_ais_store_weekly"
            #      ],
            #     "l2_geo_area_from_ais_store_weekly"
            # ),
            # node(
            #     l2_geo_area_from_competitor_store_weekly,
            #     ["l1_geo_area_from_competitor_store_daily_for_l2_geo_area_from_competitor_store_weekly",
            #      "params:l2_area_from_competitor_store_weekly"
            #      ],
            #     "l2_geo_area_from_competitor_store_weekly"
            # ),

            ### WAIT
            node(
                l2_geo_count_visit_by_location_weekly,
                ["l1_geo_count_visit_by_location_daily",
                 "params:l2_geo_count_visit_by_location_weekly"
                 ],
                "l2_geo_count_visit_by_location_weekly"
            ),
            # node(
            #     l2_geo_data_count_location_weekly,
            #     ["l1_geo_number_of_bs_used",
            #      "params:l2_geo_number_of_base_station_weekly"
            #      ],
            #     "l2_geo_number_of_base_station_weekly"
            # ),

            ### WAIT
            node(
                l2_geo_total_distance_km_weekly,
                ["l1_geo_total_distance_km_daily",
                 "params:l2_geo_total_distance_km_weekly"
                 ],
                "l2_geo_total_distance_km_weekly"
            ),
            # node(
            #     l2_geo_total_distance_km_weekly,
            #     ["l1_geo_total_distance_km_daily_for_l2_geo_total_distance_km_weekly",
            #      "params:l2_geo_total_distance_km_weekly"
            #      ],
            #     "l2_geo_total_distance_km_weekly"
            # ),
            # node(
            #     l2_geo_cust_subseqently_distance_weekly,
            #     ["l1_geo_cust_subseqently_distance_daily_for_l2_geo_cust_subseqently_distance_weekly",
            #      "params:l2_geo_cust_subseqently_distance_weekly"
            #      ],
            #     "l2_geo_cust_subseqently_distance_weekly"
            # ),

            ### WAIT
            node(
                l2_geo_data_session_location_weekly,
                ["l1_geo_data_session_location_daily",
                 "params:l2_geo_data_session_location_weekly"
                 ],
                "l2_geo_data_session_location_weekly"
            ),
            # ### runnig flag == 11
            # # 47 The favourite location
            # node(
            #     l2_the_favourite_locations_weekly,
            #     ["l1_the_favourite_locations_daily"],
            #     "l2_the_favourite_locations_weekly"
            # ),

            ### WAIT
            node(
                l2_geo_top3_voice_location_weekly,
                ["l1_geo_top3_voice_location_daily",
                 "params:l2_geo_top3_voice_location_weekly"
                 ],
                "l2_geo_top3_voice_location_weekly"
            ),
            # # Top_3_cells_on_voice_usage###
            # node(
            #     l2_geo_top3_cells_on_voice_usage,
            #     ["l1_geo_top3_cells_on_voice_usage_for_l2_geo_top3_cells_on_voice_usage",
            #      "params:l2_geo_top3_cells_on_voice_usage"
            #      ],
            #     "l2_geo_top3_cells_on_voice_usage"
            # ),
            #===========================================================================================



            ### FINISH
            ### runnig flag == 9
            ##distance_top_call###
            node(
                l2_geo_distance_top_call,
                "l1_geo_distance_top_call",
                "l2_geo_distance_top_call"
            ),

            ### FINISH
            ### runnig flag == 3
            # 27 Same favourite location for weekend and weekday
            node(
                massive_processing_with_l2_same_favourite_location_weekend_weekday_weekly,
                ["l0_geo_cust_cell_visit_time_for_l2_same_favourite_location_weekend_weekday"],
                "l2_same_favourite_location_weekend_weekday_weekly"
            )

        ], name="geo_to_l2_pipeline"
    )




