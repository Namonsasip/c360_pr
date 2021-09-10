from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l2.to_l2_nodes import *


def loyalty_to_l2_pipeline(**kwargs):
    """
    :param kwargs:
    :return:
    """
    return Pipeline(
        [

            # Number of services for each category
            node(
                build_loyalty_number_of_services_weekly,
                ["l1_loyalty_number_of_services_daily",
                 "params:exception_partition_list_for_l1_loyalty_number_of_services_daily",
                 "l0_loyalty_priv_project",
                 "l0_loyalty_priv_category",
                 "params:l2_loyalty_number_of_services_weekly"],
                "l2_loyalty_number_of_services_weekly"
            ),

            # Number of redeemed rewards in each category
            node(
                build_loyalty_number_of_rewards_redeemed_weekly,
                ["l1_loyalty_number_of_rewards_daily",
                 "params:exception_partition_list_for_l1_loyalty_number_of_rewards_daily",
                 "l0_loyalty_priv_project",
                 "l0_loyalty_priv_category",
                 "params:l2_loyalty_number_of_rewards_redeemed_weekly"],
                "l2_loyalty_number_of_rewards_redeemed_weekly"
            ),
            # Number of spend points in each category
            # node(
            #     build_loyalty_number_of_points_spend_weekly,
            #     ["l1_loyalty_number_of_points_spend_daily",
            #      "params:exception_partition_list_for_l1_loyalty_number_of_points_spend_daily",
            #      "l0_loyalty_priv_project",
            #      "l0_loyalty_priv_category",
            #      "params:l2_loyalty_number_of_points_spend_weekly"],
            #     "l2_loyalty_number_of_points_spend_weekly"
            # )

        ]
    )
