from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l1.to_l1_nodes import *



def loyalty_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            # Join daily privilege data with customer profile along with priv_project
            # node(
            #     daily_privilege_or_aunjai_data_with_customer_profile,
            #     ["l1_customer_profile_union_daily_feature",
            #      "l0_loyalty_drm_t_privilege_success",
            #      "l0_loyalty_priv_project"],
            #     "privilege_data_with_customer_profile"
            # ),

            # Number of services for each category
            node(
                loyalty_number_of_services_for_each_category,
                ["l1_customer_profile_union_daily_feature",
                 "l0_loyalty_drm_t_privilege_success",
                 "l0_loyalty_priv_project",
                 "params:l1_loyalty_number_of_services_daily"],
                 "l1_loyalty_number_of_services"
            ),

            # Join daily aunjai points data with customer profile along with priv_project
            # node(
            #     daily_privilege_or_aunjai_data_with_customer_profile,
            #     ["l1_customer_profile_union_daily_feature",
            #      "l0_loyalty_drm_t_aunjai_point_collection_daily",
            #      "l0_loyalty_priv_project"],
            #     "aunjai_points_data_with_customer_profile"
            # ),

            # Number of rewards for each category
            node(
                loyalty_number_of_rewards_for_each_category,
                ["l1_customer_profile_union_daily_feature",
                 "l0_loyalty_drm_t_aunjai_point_collection_daily",
                 "l0_loyalty_priv_project",
                 "params:l1_loyalty_number_of_rewards_daily"],
                "l1_loyalty_number_of_rewards"
            ),

            # Number of points spend for each category
            # node(
            #     daily_privilege_or_aunjai_data_with_priv_points,
            #     ["aunjai_points_data_with_customer_profile",
            #      "l0_loyalty_priv_point_transaction"],
            #     "aunjai_points_data_with_reward_points_spend"
            # ),
            node(
                loyalty_number_of_points_spend_for_each_category,
                ["l1_customer_profile_union_daily_feature",
                 "l0_loyalty_drm_t_aunjai_point_collection_daily",
                 "l0_loyalty_priv_project",
                 "l0_loyalty_priv_point_transaction",
                 "params:l1_loyalty_number_of_points_spend_daily"],
                "l1_loyalty_number_of_points_spend"
            ),
        ]
    )