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
                node_from_config,
                ["l3_previous_device_handset_summary_with_configuration_monthly","params:l4_previous_device_with_config_1"],
                "l4_previous_device_rolling_window_handset_summary_with_configuration_1"
            ),
            node(
                node_from_config,
                ["l4_previous_device_rolling_window_handset_summary_with_configuration_1","params:l4_previous_device_with_config_2"],
                "l4_previous_device_rolling_window_handset_summary_with_configuration_2"
            ),
            node(
                node_from_config,
                ["l4_previous_device_rolling_window_handset_summary_with_configuration_2",
                 "params:l4_previous_device_with_config_3"],
                "l4_previous_device_rolling_window_handset_summary_with_configuration_3"
            ),
            node(
                node_from_config,
                ["l4_previous_device_rolling_window_handset_summary_with_configuration_3",
                 "params:l4_previous_device_with_config"],
                "l4_previous_device_rolling_window_handset_summary_with_configuration"
            ),
            node(
                node_from_config,
                ["l3_device_most_used_intermediate_monthly",
                 "params:l4_device_most_used_1"],
                "l4_device_rolling_window_most_used_1"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_1",
                 "params:l4_device_most_used_2"],
                "l4_device_rolling_window_most_used_2"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_2",
                 "params:l4_device_most_used_3"],
                "l4_device_rolling_window_most_used_3"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_3",
                 "params:l4_device_most_used"],
                "l4_device_rolling_window_most_used"
            ),
        ]
    )