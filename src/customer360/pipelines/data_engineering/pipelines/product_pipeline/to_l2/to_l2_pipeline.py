from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.product_nodes.to_l2.to_l2_nodes import *


def product_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                get_activated_deactivated_features,
                ["l1_product_active_customer_promotion_features_prepaid_postpaid_for_l2_product_activated_deactivated_features_weekly",
                 "l0_product_pru_m_package_master_group_for_weekly",
                 "l0_product_pru_m_ontop_master_for_weekly",
                 "l0_product_ru_m_main_promotion_cvm_proj_for_weekly",
                 "l0_product_ru_m_ontop_promotion_cvm_proj_for_weekly"],
                "l2_product_activated_deactivated_features_weekly"
            ),
            node(get_product_package_promotion_group_tariff_weekly,
                 'l0_product_pru_m_package_master_group_for_l2_product_package_promotion_group_tariff',
                 'l2_product_package_promotion_group_tariff_weekly')
            #-------
        ]
    )
