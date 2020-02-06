from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *

def device_to_l3_pipeline(**kwargs):

    return Pipeline(
        [
            # Monthly handset configurations features
            node(
                node_from_config,
                ["l2_device_handset_summary_with_configuration_weekly",
                 "params:l3_device_handset_summary_with_configuration"],
                "l3_device_handset_summary_with_configuration_monthly"
            ),

            # Monthly previous configurations features
            node(
                node_from_config,
                ["l2_previous_device_handset_summary_with_configuration_weekly_intermediate",
                 "params:l3_previous_device_features_with_config_ranked"],
                "l3_previous_device_handset_summary_with_configuration_monthly_1"
            ),
            node(
                node_from_config,
                ["l3_previous_device_handset_summary_with_configuration_monthly_1",
                 "params:l3_previous_device_features_with_config"],
                "l3_previous_device_handset_summary_with_configuration_monthly"
            ),

            # Monthly most used device
            node(
                node_from_config,
                ["l2_device_most_used_intermediate_weekly",
                 "params:l3_device_most_used_initial"],
                "l3_device_most_used_intermediate_monthly"
            ),
            node(
                node_from_config,
                ["l3_device_most_used_intermediate_monthly",
                 "params:l3_device_most_used"],
                "l3_device_most_used_monthly"
            ),
        ]
    )