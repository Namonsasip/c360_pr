from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.product_nodes.to_l2.to_l2_nodes import *


def product_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            # Directly from L0 because it is already ingested weekly
            node(
                get_activated_deactivated_features,
                ["l0_product_customer_promotion_daily",
                 "l0_product_pru_m_package_master_group",
                 "l0_product_pru_m_ontop_master",
                 "l0_product_ru_m_main_promotion_cvm_proj",
                 "l0_product_ru_m_ontop_promotion_cvm_proj"],
                "l2_product_activated_deactivated_features"
            )
        ]
    )
