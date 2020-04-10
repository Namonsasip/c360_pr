from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *




def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                l4_rolling_window,
                ["l2_geo_number_of_bs_used",
                 "params:l4_number_of_bs_used"],
                "l4_geo_number_of_bs_used"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_number_of_location_with_transactions",
                 "params:l4_number_of_location_with_transactions"],
                "l4_geo_number_of_location_with_transactions"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_voice_distance_daily",
                 "params:l4_voice_distance_daily"],
                "l4_geo_voice_distance_daily"
            ),

        ], name="geo_to_l4_pipeline"
    )
