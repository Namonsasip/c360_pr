from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l3.to_l3_nodes import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *

def device_to_l3_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l2_device_handset_summary_with_configuration_weekly","params:l3_device_handset_summary_with_configuration"],
                "l3_device_handset_summary_with_configuration_monthly"
            ),
            node(
                derive_month_and_week,
                ["l0_devices_summary_customer_handset"],
                "l3_previous_device_handset_summary_with_configuration_monthly_1"
            ),
            node(
                node_from_config,
                ["l3_previous_device_handset_summary_with_configuration_monthly_1","params:l3_previous_device_features_with_config"],
                "l3_previous_device_handset_summary_with_configuration_monthly_2"
            ),
            node(
                previous_device_features_with_config_sort,
                ["l3_previous_device_handset_summary_with_configuration_monthly_2"],
                "l3_previous_device_handset_summary_with_configuration_monthly"
            ),
            node(
                node_from_config,
                ["l2_device_most_used_intermediate_weekly","params:l3_device_most_used_1"],
                "l3_device_most_used_intermediate_monthly_1"
            ),
            node(
                node_from_config,
                ["l3_device_most_used_intermediate_monthly_1", "params:l3_device_most_used_2"],
                "l3_device_most_used_intermediate_monthly"
            ),
            node(
                node_from_config,
                ["l3_device_most_used_intermediate_monthly","params:l3_device_most_used"],
                "l3_device_most_used_monthly"
            ),
        ]
    )