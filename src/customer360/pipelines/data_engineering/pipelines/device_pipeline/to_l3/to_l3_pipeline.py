from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l3.to_l3_nodes import *


def device_to_l3_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l2_device_handset_summary_with_configuration_weekly","params:l3_device_handset_summary_with_configuration"],
                "l3_device_handset_summary_with_configuration_monthly"
            ),
            node(
                previous_device_features_with_config,
                ["l0_devices_summary_customer_handset"],
                "l3_previous_device_handset_summary_with_configuration_monthly"
            ),
        ]
    )