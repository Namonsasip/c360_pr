from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l3.to_l3_nodes import *


def loyalty_to_l3_pipeline(**kwargs):
    """
    :param kwargs:
    :return:
    """
    return Pipeline(
        [
            node(
                build_loyalty_point_balance_statuses_monthly,
                ["l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily",
                 "l1_loyalty_priv_point_bonus_ba_daily",
                 "l1_loyalty_priv_point_ba_daily",
                 "params:l3_loyalty_point_balance_statuses_monthly"],
                "l3_loyalty_point_balance_statuses_monthly"
            ),

        ]
    )
