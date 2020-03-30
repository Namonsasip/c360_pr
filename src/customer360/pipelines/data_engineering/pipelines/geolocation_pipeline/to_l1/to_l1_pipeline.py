from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l1.to_l1_nodes import *





def geo_cell_visit_time_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l0_int_number_of_bs_used,
                ["l0_geo_cust_cell_visit_time_daily",
                 ],
                "l0_int_geo_cust_cell_visit_time_daily"
            ),

            node(
                node_from_config,
                ["l0_int_geo_cust_cell_visit_time_daily",
                 "params:l1_number_of_bs_used"],
                "l1_geo_cust_cell_visit_time_daily"
            ),
            node(
                node_from_config,
                ["l0_int_geo_cust_cell_visit_time_daily",
                 "params:l2_number_of_bs_used"],
                "l2_geo_cust_cell_visit_time_daily"
            ),
            node(
                node_from_config,
                ["l0_int_geo_cust_cell_visit_time_daily",
                 "params:l3_number_of_bs_used"],
                "l3_geo_cust_cell_visit_time_daily"
            ),
        ], name="geo_cell_visit_time_pipeline"
    )
