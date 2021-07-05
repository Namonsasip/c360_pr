from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l1.to_l1_nodes import *


def geo_to_l1_pipeline_to_run():
    return Pipeline(
        [

            ### FINISH
            node(
                massive_processing_with_l1_geo_time_spent_by_location_daily,
                ["l0_geo_cust_location_visit_hr_for_l1_geo_time_spent_by_location_daily",
                 "params:l1_geo_time_spent_by_location_daily"
                 ],
                "l1_geo_time_spent_by_location_daily"

            ),

            ### FINISH
            node(
                massive_processing_with_l1_geo_count_visit_by_location_daily,
                ["l0_geo_cust_cell_visit_time_daily_for_l1_geo_count_visit_by_location_daily",
                 "params:l1_geo_count_visit_by_location_daily"
                 ],
                "l1_geo_count_visit_by_location_daily"
            ),

            ### FINISH
            node(
                massive_processing_with_l1_geo_total_distance_km_daily,
                ["l0_geo_cust_cell_visit_time_for_l1_geo_total_distance_km_daily",
                 "params:l1_geo_total_distance_km_daily"
                 ],
                "l1_geo_total_distance_km_daily"
            ),

            ### FINISH
            ### Move to customer profile
            # node(
            #     massive_processing_with_l1_customer_profile_imsi_daily_feature,
            #     ["l0_customer_profile_imsi_daily_feature",
            #      "params:l1_customer_profile_imsi_daily_feature"
            #      ],
            #     "l1_customer_profile_imsi_daily_feature"
            # ),

            ### FINISH
            node(
                massive_processing_with_l1_geo_data_session_location_daily,
                ["l0_usage_sum_data_location_daily_for_l1_geo_data_session_location_daily",
                 "l0_geo_mst_cell_masterplan_master",
                 "l1_customer_profile_imsi_daily_feature_for_l1_geo_data_session_location_daily",
                 "params:l1_geo_data_session_location_daily"
                 ],
                "l1_geo_data_session_location_daily"
            ),

        ], name="geo_to_l1_pipeline_to_run"
    )


def geo_to_l1_pipeline_master():
    return Pipeline(
        [
            ### FINISH
            node(
                l1_geo_mst_location_near_shop_master,
                ["l0_geo_mst_cell_masterplan_master",
                 "l0_geo_mst_lm_poi_shape_master"
                 ],
                "l1_geo_mst_location_near_shop_master"
            ),

            ### FINISH
            node(
                l1_geo_mst_location_ais_shop_master,
                ["l0_geo_mst_lm_poi_shape_master"
                 ],
                "l1_geo_mst_location_ais_shop_master"
            ),

            # ### WAIT
            # node(
            #     l1_geo_mst_cell_masterplan_master,
            #     ["l0_geo_mst_cell_masterplan_master"
            #      ],
            #     "l1_geo_mst_cell_masterplan_master"
            # )

        ], name="geo_to_l1_pipeline_master"
    )


def geo_to_l1_pipeline(**kwargs):
    return Pipeline(
        [

            ### FINISH
            node(
                massive_processing_with_l1_geo_time_spent_by_store_daily,
                ["l1_geo_time_spent_by_location_daily_for_l1_geo_time_spent_by_store_daily",
                 "l1_geo_mst_location_near_shop_master",
                 "params:l1_geo_time_spent_by_store_daily"
                 ],
                "l1_geo_time_spent_by_store_daily"
            ),

            ### FINISH
            node(
                massive_processing_with_l1_geo_visit_ais_store_location_daily,
                ["l1_geo_time_spent_by_location_daily_for_l1_geo_visit_ais_store_location_daily",
                 "l1_geo_mst_location_ais_shop_master",
                 "params:l1_geo_visit_ais_store_location_daily"
                 ],
                "l1_geo_visit_ais_store_location_daily"
            ),

            ### FINISH
            node(
                massive_processing_with_l1_geo_top3_voice_location_daily,
                ["l0_usage_sum_voice_location_daily_for_l1_geo_top3_cells_on_voice_usage",
                 "l0_geo_mst_cell_masterplan_master",
                 "l1_customer_profile_imsi_daily_feature_for_l1_geo_top3_voice_location_daily",
                 "params:l1_geo_top3_voice_location_daily"
                 ],
                "l1_geo_top3_voice_location_daily"
            ),

            ### FINISH
            node(
                massive_processing_with_l1_geo_count_data_session_by_location_daily,
                ["l1_geo_data_session_location_daily_for_l1_geo_count_data_session_by_location_daily",
                 "params:l1_geo_count_data_session_by_location_daily"
                 ],
                "l1_geo_count_data_session_by_location_daily"
            )

        ], name="geo_to_l1_pipeline"
    )
