from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *




def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l3_number_of_bs_used,
                ["l0_geo_cust_cell_visit_time_daily",
                 ],
                "l2_int_geo_cust_cell_visit_time_daily"
            ),

            node(
                node_from_config,
                ["l2_int_geo_cust_cell_visit_time_daily",
                 "params:l3_number_of_bs_used"],
                "l3_geo_cust_cell_visit_time_daily"
            ),

            node(
                l3_geo_voice_distance_daily,
                ["l1_geo_voice_distance_daily_intermediate",
                 "params:l3_voice_distance_daily"],
                "l3_geo_voice_distance_daily"

            ),

        ], name="geo_to_l3_pipeline"
    )
