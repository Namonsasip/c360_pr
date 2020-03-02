from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *

def device_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            # Weekly handset configuration related features

            node(
                device_summary_with_configuration,
                ["l0_devices_summary_customer_handset",
                 "l0_devices_handset_configurations"],
                "device_summary_with_config"
            ),

            # node(
            #     device_current_configuration_weekly,
            #     ["l1_customer_profile_union_daily_feature",
            #      "device_summary_with_config",
            #      "params:l2_device_handset_summary_with_configuration"],
            #     "l2_device_handset_summary_with_configuration_weekly"
            # ),

            # Weekly number of phone updates
            node(
                device_number_of_phone_updates_weekly,
                ["l1_customer_profile_union_daily_feature",
                 "device_summary_with_config",
                 "params:l2_device_number_of_phone_updates"],
                "l2_device_number_of_phone_updates_weekly"
            ),

            # Weekly most used device
            # node(
            #     device_most_used_weekly,
            #     ["l1_customer_profile_union_daily_feature",
            #      "device_summary_with_config",
            #      "params:l2_device_most_used"],
            #     "l2_device_most_used_weekly"
            # ),

            # Weekly previous configurations features
            node(
                node_from_config,
                ["device_summary_with_config",
                 "params:l2_previous_device_features_with_config_ranked"],
                "l2_previous_device_handset_summary_with_configuration_weekly_1"
            ),
            node(
                device_previous_configurations_weekly,
                ["l1_customer_profile_union_daily_feature",
                 "l2_previous_device_handset_summary_with_configuration_weekly_1",
                 "params:l2_previous_device_features_with_config"],
                "l2_previous_device_handset_summary_with_configuration_weekly"
            ),
        ]
    )