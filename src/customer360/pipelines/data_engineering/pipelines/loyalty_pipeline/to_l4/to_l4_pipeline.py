from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def loyalty_to_l4_weekly_pipeline(**kwargs):
    """
    :param kwargs:
    :return:
    """
    return Pipeline(
        [
            # Number of services for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_services_weekly",
                 "params:l4_loyalty_number_of_services_features"],
                "l4_loyalty_number_of_services_features"
            ),
            # Number of rewards for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_rewards_redeemed_weekly",
                 "params:l4_loyalty_number_of_rewards_redeemed_features"],
                "l4_loyalty_number_of_rewards_redeemed_features"
            ),
            # Number of points spend for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_points_spend_weekly",
                 "params:l4_loyalty_number_of_points_spend_features"],
                "l4_loyalty_number_of_points_spend_features"
            )
        ]
    )


def loyalty_to_l4_monthly_pipeline(**kwargs):
    """
    :param kwargs:
    :return:
    """
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l3_loyalty_point_balance_statuses_monthly",
                 "params:l4_loyalty_point_balance_statuses_features"],
                "l4_loyalty_point_balance_statuses_features"
            )
        ]
    )
