from kedro.pipeline import Pipeline, node
from src.customer360.pipelines.data_engineering.nodes.product_nodes.to_l4.to_l4_nodes import *


def product_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            node(rolling_window_product,
                 [
                     "l2_product_activated_deactivated_features_weekly",
                     "params:l4_product_activated_deactivated_features_first",
                     "params:l4_product_activated_deactivated_features_second"
                 ],
                 "int_l4_product_activated_deactivated_features"
                 ),
            node(
                add_l4_product_ratio_features,
                ["int_l4_product_activated_deactivated_features"],
                "l4_product_activated_deactivated_features"
            ),
            node(get_product_package_promotion_group_tariff_features,
                 'l2_product_package_promotion_group_tariff_weekly', 
                 'l4_product_package_promotion_group_tariff_features')
                 #ffff
        ]
    )
