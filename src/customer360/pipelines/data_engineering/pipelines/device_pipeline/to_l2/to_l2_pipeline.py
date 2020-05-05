from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *


def device_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            # Weekly handset configuration related features
            node(
                dac_for_device_summary_with_configuration,
                ["l0_devices_handset_configurations"],
                "int_l0_devices_handset_configurations"
            ),
            node(
                device_summary_with_configuration,
                ["l1_devices_summary_customer_handset_daily",
                 "int_l0_devices_handset_configurations"],
                "device_summary_with_config"
            ),

            # Weekly number of phone updates
            # not using this now -- Ankit commented this out
            # node(
            #     device_number_of_phone_updates_weekly,
            #     ["device_summary_with_config",
            #      "params:l2_device_number_of_phone_updates"],
            #     "l2_device_number_of_phone_updates_weekly"
            # ),

            node(
                node_from_config,
                ["device_summary_with_config",
                 "params:l2_device_summary_with_config"],
                "l2_device_summary_with_config_weekly"
            ),
            #
            # # Weekly previous configurations features
            # node(
            #     node_from_config,
            #     ["device_summary_with_config",
            #      "params:l2_previous_device_features_with_config_ranked"],
            #     "l2_previous_device_handset_summary_with_configuration_weekly_1"
            # ),
            # node(
            #     device_previous_configurations_weekly,
            #     ["l1_customer_profile_union_daily_feature",
            #      "l2_previous_device_handset_summary_with_configuration_weekly_1",
            #      "params:l2_previous_device_features_with_config"],
            #     "l2_previous_device_handset_summary_with_configuration_weekly"
            # ),
        ], name="device_to_l2_pipeline"
    )
