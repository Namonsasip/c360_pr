from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def loyalty_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            # Number of services for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_services_weekly",
                 "params:l4_loyalty_number_of_services_features"],
                "l4_loyalty_number_of_services_weekly"
            ),

            # Number of rewards for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_rewards_redeemed_weekly",
                 "params:l4_loyalty_number_of_rewards_redeemed_features"],
                "l4_loyalty_number_of_rewards_redeemed_features"
            ),
            #
            # # Number of points spend for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_points_spend_for_l4_loyalty_number_of_points_spend",
                 "params:l4_rolling_window_loyalty_number_of_points_spend"],
                "l4_loyalty_number_of_points_spend"
            ),
        ]
    )
