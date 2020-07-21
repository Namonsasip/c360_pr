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
                loyalty_number_of_points_balance,
                [
                 "l3_customer_profile_union_monthly_feature_for_l3_loyalty_point_balance_statuses_monthly",
                 "l0_loyalty_drm_s_aunjai_register_distinct_sub_monthly_for_l3_loyalty_point_balance_statuses_monthly",
                 "params:l3_loyalty_point_balance_statuses_monthly"
                ],
                "l3_loyalty_point_balance_statuses_monthly"
            ),

        ]
    )
