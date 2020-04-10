from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l1.to_l1_nodes import *


def geo_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            # number_of_bs_used
            node(
                l1_int_number_of_bs_used,
                ["l0_geo_footfall_daily",
                 ],
                "l1_int_geo_cust_cell_visit_time_daily"
            ),

            # number_of_bs_used
            node(
                node_from_config,
                ["l1_int_geo_cust_cell_visit_time_daily",
                 "params:l1_number_of_bs_used"],
                "l1_geo_number_of_bs_used"
            ),

            # Number of Location_id with transactions
            node(
                l1_number_of_location_with_transactions,
                ["l0_geo_footfall_daily", "l0_mst_cell_masterplan",
                 "params:l1_number_of_location_with_transactions"],
                "l1_geo_number_of_location_with_transactions"
            ),

            node(
                l1_geo_voice_distance_daily_intermediate,
                ["l0_usage_sum_voice_location_daily",
                 ],
                "l1_geo_voice_distance_daily_intermediate"

            ),

            node(
                l1_geo_voice_distance_daily,
                ["l1_geo_voice_distance_daily_intermediate",
                 "params:l1_voice_distance_daily"],
                "l1_geo_voice_distance_daily"

            ),

        ], name="geo_to_l1_pipeline"
    )
