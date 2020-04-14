from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.product_nodes.to_l4.to_l4_nodes import *

def product_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_product_activated_deactivated_features",
                 "params:l4_product_activated_deactivated_features"],
                "int_l4_product_activated_deactivated_features"
            ),
            node(
                add_l4_product_ratio_features,
                ["int_l4_product_activated_deactivated_features"],
                "l4_product_activated_deactivated_features"
            )
        ]
    )
