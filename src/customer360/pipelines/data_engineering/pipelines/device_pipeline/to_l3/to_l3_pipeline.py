from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *
from customer360.pipelines.data_engineering.nodes.device_nodes.to_l3.to_l3_nodes import *


def device_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                device_summary_with_configuration,
                ["l0_devices_summary_customer_handset",
                 "l0_devices_handset_configurations"],
                "device_summary_with_config"
            ),

            # Monthly current device configs
            node(
                device_current_configurations_monthly,
                ["l2_device_handset_summary_with_configuration_weekly",
                 "params:l3_device_handset_summary_with_configuration"],
                "l3_device_handset_summary_with_configuration_monthly"
            ),

            # Monthly number of phone updates
            node(
                device_number_of_phone_updates_monthly,
                ["l2_device_number_of_phone_updates_weekly",
                 "params:l3_device_number_of_phone_updates"],
                "l3_device_number_of_phone_updates_monthly"
            ),

            # Monthly previous configurations features
            node(
                node_from_config,
                ["device_summary_with_config",
                 "params:l3_previous_device_features_with_config_ranked"],
                "l3_previous_device_handset_summary_with_configuration_monthly_1"
            ),
            node(
                device_previous_configurations_monthly,
                ["l3_customer_profile_include_1mo_non_active",
                 "l3_previous_device_handset_summary_with_configuration_monthly_1",
                 "params:l3_previous_device_features_with_config"],
                "l3_previous_device_handset_summary_with_configuration_monthly"
            ),

            # Monthly most used device
            node(
                device_most_used_monthly,
                ["l3_customer_profile_include_1mo_non_active",
                 "device_summary_with_config",
                 "params:l3_device_most_used"],
                "l3_device_most_used_monthly"
            ),
        ]
    )
