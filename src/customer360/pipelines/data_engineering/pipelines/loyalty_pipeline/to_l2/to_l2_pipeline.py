from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config

def loyalty_to_l2_pipeline(**kwargs):
    return Pipeline(
        [

            # Number of services for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_services",
                 "params:l2_loyalty_number_of_services_weekly"],
                 "l2_loyalty_number_of_services"
            ),

            # Number of rewards for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_rewards",
                 "params:l2_loyalty_number_of_rewards_weekly"],
                "l2_loyalty_number_of_rewards"
            ),

            # Number of points spend for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_points_spend",
                 "params:l2_loyalty_number_of_points_spend_weekly"],
                "l2_loyalty_number_of_points_spend"
            ),
        ]
    )