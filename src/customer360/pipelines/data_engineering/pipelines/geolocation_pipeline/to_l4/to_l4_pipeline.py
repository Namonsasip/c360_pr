from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *




def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                l4_rolling_window,
                ["l2_geo_cust_cell_visit_time_daily",
                 "params:l4_number_of_bs_used"],
                "l4_geo_cust_cell_visit_time_daily"
            ),
        ], name="geo_to_l4_pipeline"
    )
