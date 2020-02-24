from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l3.to_l3_nodes import *

def device_to_l3_pipeline(**kwargs):

    return Pipeline(
        [
            # # Monthly handset configuration related features
            # node(
            #     device_summary_with_customer_profile,
            #     ["l1_customer_profile_union_daily_feature",
            #      "l0_devices_summary_customer_handset"],
            #     "device_handset_summary_with_customer_profile"
            # ),
            #
            # node(
            #     device_summary_with_config,
            #     ["device_handset_summary_with_customer_profile",
            #      "l0_devices_handset_configurations"],
            #     "l3_device_handset_summary_with_configuration_monthly_1"
            # ),
            #
            # node(
            #     node_from_config,
            #     ["l3_device_handset_summary_with_configuration_monthly_1",
            #      "params:l3_device_handset_summary_with_configuration"],
            #     "l3_device_handset_summary_with_configuration_monthly_2"
            # ),
            # node(
            #     filter,
            #     ["l3_device_handset_summary_with_configuration_monthly_2"],
            #     "l3_device_handset_summary_with_configuration_monthly"
            # ),
            #
            # # Monthly number of phone updates
            # node(
            #     device_number_of_phone_updates_monthly,
            #     ["l3_device_handset_summary_with_configuration_monthly_1",
            #      "params:l3_device_number_of_phone_updates"],
            #     "l3_device_number_of_phone_updates_monthly"
            # ),
            #
            # # Monthly previous configurations features
            # node(
            #     node_from_config,
            #     ["device_handset_summary_with_customer_profile",
            #      "params:l3_previous_device_features_with_config_ranked"],
            #     "l3_previous_device_handset_summary_with_configuration_monthly_2"
            # ),
            # node(
            #     device_previous_configurations_monthly,
            #     ["l3_previous_device_handset_summary_with_configuration_monthly_2",
            #      "params:l3_previous_device_features_with_config"],
            #     "l3_previous_device_handset_summary_with_configuration_monthly"
            # ),
            #
            # # Monthly most used device
            # node(
            #     node_from_config,
            #     ["device_handset_summary_with_customer_profile",
            #      "params:l3_device_most_used_initial"],
            #     "l3_device_most_used_intermediate_monthly"
            # ),
            # node(
            #     device_most_used_monthly,
            #     ["l3_device_most_used_intermediate_monthly",
            #      "params:l3_device_most_used"],
            #     "l3_device_most_used_monthly"
            # ),
        ]
    )