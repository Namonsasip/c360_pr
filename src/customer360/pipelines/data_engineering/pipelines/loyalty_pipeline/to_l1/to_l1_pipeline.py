from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l1.to_l1_nodes import *


def loyalty_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                loyalty_number_of_services_for_each_category,
                ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_services",
                 "l0_loyalty_drm_t_privilege_success"],
                "l1_loyalty_number_of_services_daily"
            ),

            # Number of rewards spent for each category
            node(
                loyalty_number_of_rewards_redeemed_for_each_category,
                ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_rewards",
                 "l0_loyalty_drm_t_aunjai_point_collection_daily_for_l1_loyalty_number_of_rewards"],
                "l1_loyalty_number_of_rewards_redeemed_daily"
            ),

            # Number of points spend for each category
            node(
                loyalty_number_of_points_spend_for_each_category,
                ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_points_spend",
                 "l0_loyalty_priv_point_transaction"], "l1_loyalty_number_of_points_spend_daily"
            ),

            # # Number of rewards for each category
            # node(
            #     loyalty_number_of_rewards_for_each_category,
            #     ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_rewards",
            #      "l0_loyalty_drm_t_aunjai_point_collection_daily_for_l1_loyalty_number_of_rewards",
            #      "l0_loyalty_priv_project_for_l1_loyalty_number_of_rewards",
            #      "params:l1_loyalty_number_of_rewards_daily"],
            #     "l1_loyalty_number_of_rewards"
            # ),
            #
            # ## Number of points spend for each category
            # ## node(
            # ##     daily_privilege_or_aunjai_data_with_priv_points,
            # ##     ["aunjai_points_data_with_customer_profile",
            # ##      "l0_loyalty_priv_point_transaction"],
            # ##     "aunjai_points_data_with_reward_points_spend"
            # ## ),
            #
            # node(
            #     loyalty_number_of_points_spend_for_each_category,
            #     ["l1_customer_profile_union_daily_feature_for_l1_loyalty_number_of_points_spend",
            #      "l0_loyalty_drm_t_aunjai_point_collection_daily_for_l1_loyalty_number_of_points_spend",
            #      "l0_loyalty_priv_project_for_l1_loyalty_number_of_points_spend",
            #      "l0_loyalty_priv_point_transaction",
            #      "params:l1_loyalty_number_of_points_spend_daily"],
            #     "l1_loyalty_number_of_points_spend"
            # ),


        ]
    )
