from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *

def loyalty_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            # Number of services for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_services",
                 "params:l4_rolling_window_loyalty_number_of_services"],
                 "l4_loyalty_number_of_services"
            ),

            # Number of rewards for each category
            node(
                l4_rolling_window,
                ["l2_loyalty_number_of_rewards",
                 "params:l4_rolling_window_loyalty_number_of_rewards"],
                "l4_loyalty_number_of_rewards"
            ),
            #
            # # Number of points spend for each category
            # node(
            #     l4_rolling_window,
            #     ["l2_loyalty_number_of_points_spend",
            #      "params:l4_rolling_window_loyalty_number_of_points_spend"],
            #     "l4_loyalty_number_of_points_spend"
            # ),
        ]
    )