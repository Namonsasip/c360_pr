from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l2.to_l2_nodes import *




def geo_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l2_number_of_bs_used,
                ["l1_geo_number_of_bs_used",
                 ],
                "l1_int_geo_cust_cell_visit_time_daily"
            ),

            node(
                node_from_config,
                ["l1_int_geo_cust_cell_visit_time_daily",
                 "params:l2_number_of_bs_used"],
                "l2_geo_number_of_bs_used"
            ),

            node(
                l2_number_of_location_with_transactions,
                ["l1_geo_number_of_location_with_transactions",
                 "params:l2_number_of_location_with_transactions"],
                "l2_geo_number_of_location_with_transactions"
            ),


        ], name="geo_to_l2_pipeline"
    )
