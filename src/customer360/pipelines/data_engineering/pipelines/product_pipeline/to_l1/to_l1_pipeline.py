from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.utilities.re_usable_functions import l1_massive_processing
from src.customer360.pipelines.data_engineering.nodes.product_nodes.to_l1.to_l1_nodes import *


def product_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                dac_product_customer_promotion_for_daily,
                "l0_product_customer_promotion_for_daily",
                "int_l1_product_active_customer_promotion_features"
            ),
            node(
                l1_massive_processing,
                ["int_l1_product_active_customer_promotion_features",
                 "params:int_l1_product_active_customer_promotion_features",
                 "l1_customer_profile_union_daily_feature_for_int_l1_product_active_customer_promotion_features"],
                "int_l1_product_active_customer_promotion_features_temp"
            ),
            node(
                join_with_master_package,
                ["int_l1_product_active_customer_promotion_features_temp",
                 "l0_product_pru_m_package_master_group_for_daily",
                 "l0_product_pru_m_ontop_master_for_daily",
                 "l0_product_ru_m_main_promotion_cvm_proj_for_daily",
                 "l0_product_ru_m_ontop_promotion_cvm_proj_for_daily"],
                "l1_product_active_customer_promotion_features_daily"
            ),

            node(
                dac_product_fbb_a_customer_promotion_current_for_daily,
                "l0_product_fbb_a_customer_promotion_current_for_daily",
                "int_l1_product_active_fbb_customer_features_daily"
            ),
            node(
                l1_massive_processing,
                ["int_l1_product_active_fbb_customer_features_daily",
                 "params:l1_product_active_fbb_customer_features",
                 "l1_customer_profile_union_daily_feature_for_l1_product_active_fbb_customer_features_daily"],
                "l1_product_active_fbb_customer_features_daily"
            )

        ]
    )
