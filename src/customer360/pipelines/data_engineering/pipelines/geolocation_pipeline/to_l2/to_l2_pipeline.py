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

            ### WAIT
            node(
                l2_geo_time_spent_by_store_weekly,
                ["l1_geo_time_spent_by_store_daily",
                 "params:l2_geo_time_spent_by_store_weekly"
                 ],
                "l2_geo_time_spent_by_store_weekly"
            ),

            ### WAIT
            node(
                l2_geo_count_visit_by_location_weekly,
                ["l1_geo_count_visit_by_location_daily",
                 "params:l2_geo_count_visit_by_location_weekly"
                 ],
                "l2_geo_count_visit_by_location_weekly"
            ),

            ### WAIT
            node(
                l2_geo_total_distance_km_weekly,
                ["l1_geo_total_distance_km_daily",
                 "params:l2_geo_total_distance_km_weekly"
                 ],
                "l2_geo_total_distance_km_weekly"
            ),

            # ### WAIT
            # node(
            #     int_l2_geo_top3_voice_location_weekly,
            #     ["l1_geo_top3_voice_location_daily",
            #      "params:int_l2_geo_top3_voice_location_weekly"
            #      ],
            #     "int_l2_geo_top3_voice_location_weekly"
            # ),
            # node(
            #     l2_geo_top3_voice_location_weekly,
            #     ["int_l2_geo_top3_voice_location_weekly",
            #      "params:l2_geo_top3_voice_location_weekly"
            #      ],
            #     "l2_geo_top3_voice_location_weekly"
            # ),

            # ### WAIT
            # node(
            #     l2_geo_data_session_location_weekly,
            #     ["l1_geo_data_session_location_daily",
            #      "params:l2_geo_data_session_location_weekly"
            #      ],
            #     "l2_geo_data_session_location_weekly"
            # ),
            # node(
            #     l2_geo_most_frequently_used_location_weekly,
            #     ["l2_geo_data_session_location_weekly",
            #      "params:l2_geo_most_frequently_used_location_weekly"
            #      ],
            #     "l2_geo_most_frequently_used_location_weekly"
            # ),

            # Calculate unique cell
            # ### WAIT
            # node(
            #     l2_geo_count_data_session_by_location_weekly,
            #     ["l1_geo_data_session_location_daily",
            #      "params:l2_geo_count_data_session_by_location_weekly"
            #      ],
            #     "l2_geo_count_data_session_by_location_weekly"
            # ),

            # ### WAIT
            # node(
            #     int_l2_customer_profile_imsi_daily_feature,
            #     ["l1_customer_profile_imsi_daily_feature",
            #      "params:int_l2_customer_profile_imsi_daily_feature"
            #      ],
            #     "int_l2_customer_profile_imsi_daily_feature"
            # ),

        ], name="geo_to_l2_pipeline"
    )




