from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *

def device_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            # Weekly handset configuration related features
            node(
                device_features_with_config,
                ["l0_devices_summary_customer_handset",
                 "l0_devices_handset_configurations"],
                "l2_device_handset_summary_with_configuration_weekly_1"
            ),
            node(
                node_from_config,
                ["l2_device_handset_summary_with_configuration_weekly_1",
                 "params:l2_device_handset_summary_with_configuration"],
                "l2_device_handset_summary_with_configuration_weekly_2"
            ),
            node(
                filter,
                ["l2_device_handset_summary_with_configuration_weekly_2"],
                "l2_device_handset_summary_with_configuration_weekly"
            ),

            # Weekly most used device
            node(
                derive_month_and_week,
                ["l0_devices_summary_customer_handset"],
                "l2_device_most_used_intermediate_weekly_1"
            ),
            node(
                node_from_config,
                ["l2_device_most_used_intermediate_weekly_1",
                 "params:l2_device_most_used_initial"],
                "l2_device_most_used_intermediate_weekly"
            ),
            node(
                node_from_config,
                ["l2_device_most_used_intermediate_weekly",
                 "params:l2_device_most_used"],
                "l2_device_most_used_weekly"
            ),

            # Weekly previous configurations features
            node(
                derive_month_and_week,
                ["l0_devices_summary_customer_handset"],
                "l2_previous_device_handset_summary_with_configuration_weekly_intermediate"
            ),
            node(
                node_from_config,
                ["l2_previous_device_handset_summary_with_configuration_weekly_intermediate",
                 "params:l2_previous_device_features_with_config_ranked"],
                "l2_previous_device_handset_summary_with_configuration_weekly_1"
            ),
            node(
                node_from_config,
                ["l2_previous_device_handset_summary_with_configuration_weekly_1",
                 "params:l2_previous_device_features_with_config"],
                "l2_previous_device_handset_summary_with_configuration_weekly"
            ),
        ]
    )