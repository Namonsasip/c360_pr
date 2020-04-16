from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l2.to_l2_nodes import *

def loyalty_to_l2_pipeline(**kwargs):
    return Pipeline(
        [

            # Number of services for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_services_for_l2_loyalty_number_of_services",
                 "params:l2_loyalty_number_of_services_weekly"],
                 "l2_loyalty_number_of_services"
            ),

            # # Number of rewards for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_rewards_for_l2_loyalty_number_of_rewards",
                 "params:l2_loyalty_number_of_rewards_weekly"],
                "l2_loyalty_number_of_rewards"
            ),
            #
            # # Number of points spend for each category
            node(
                node_from_config,
                ["l1_loyalty_number_of_points_spend_for_l2_loyalty_number_of_points_spend",
                 "params:l2_loyalty_number_of_points_spend_weekly"],
                "l2_loyalty_number_of_points_spend"
            ),

            #Serenade class weekly
            node(
                loyalty_serenade_class,
                ["l0_loyalty_priv_customer_profile_for_l2_loyalty_serenade_class",
                 "l1_customer_profile_union_daily_feature_for_l2_loyalty_serenade_class",
                 "params:l2_loyalty_serenade_class_weekly"],
                "l2_loyalty_serenade_class"
            ),
        ]
    )
