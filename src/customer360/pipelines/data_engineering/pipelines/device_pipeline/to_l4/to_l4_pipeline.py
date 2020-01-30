from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l4.to_l4_nodes import *


def device_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l3_device_handset_summary_with_configuration_monthly","params:l4_device_handset_summary_with_configuration"],
                "l4_device_rolling_window_handset_summary_with_configuration"
            ),
            node(
                previous_device_with_configuration,
                ["l3_previous_device_handset_summary_with_configuration_monthly"],
                "l4_previous_device_rolling_window_handset_summary_with_configuration"
            ),
        ]
    )