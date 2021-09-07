from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l1.to_l1_nodes import *


def loyalty_to_l1_pipeline(**kwargs):
    """
    :param kwargs:
    :return:
    """
    return Pipeline(
        [
            # Number of services for each category
            node(
                loyalty_number_of_services_for_each_category,
                ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_services",
                 "l0_loyalty_drm_t_privilege_success"],
                "l1_loyalty_number_of_services_daily"
            ),
            #
            # # Number of rewards spent for each category
            # node(
            #     loyalty_number_of_rewards_redeemed_for_each_category,
            #     ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_rewards",
            #      "l0_loyalty_drm_t_aunjai_point_collection_daily_for_l1_loyalty_number_of_rewards"],
            #     "l1_loyalty_number_of_rewards_daily"
            # ),
            #
            # # Number of points spend for each category
            # node(
            #     loyalty_number_of_points_spend_for_each_category,
            #     ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_points_spend",
            #      "l0_loyalty_priv_point_transaction"],
            #     "l1_loyalty_number_of_points_spend_daily"
            # ),

        ]
    )
