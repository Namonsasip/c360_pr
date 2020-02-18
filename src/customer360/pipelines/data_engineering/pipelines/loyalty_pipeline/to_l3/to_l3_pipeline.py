from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config

def loyalty_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            # Number of services for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_services",
                 "params:l3_loyalty_number_of_services_monthly"],
                 "l3_loyalty_number_of_services"
            ),

            # Number of rewards for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_rewards",
                 "params:l3_loyalty_number_of_rewards_monthly"],
                "l3_loyalty_number_of_rewards"
            ),

            # Number of points spend for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_points_spend",
                 "params:l3_loyalty_number_of_points_spend_monthly"],
                "l3_loyalty_number_of_points_spend"
            ),
        ]
    )