from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l4.to_l4_nodes import *


def device_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
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
                l4_rolling_window,
                ["l2_device_most_used_intermediate_weekly",
                 "params:l4_device_most_used_1"],
                "l4_device_rolling_window_most_used_1"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_1",
                 "params:l4_device_most_used_with_ranks"],
                "l4_device_rolling_window_most_used_2"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_2",
                 "params:l4_device_most_used_last_week"],
                "l4_device_rolling_window_most_used_last_week"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_2",
                 "params:l4_device_most_used_last_two_week"],
                "l4_device_rolling_window_most_used_last_two_week"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_2",
                 "params:l4_device_most_used_last_month"],
                "l4_device_rolling_window_most_used_last_month"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_2",
                 "params:l4_device_most_used_last_three_month"],
                "l4_device_rolling_window_most_used_last_three_month"
            ),
            node(
                joined_data_for_most_used_device,
                ["l4_device_rolling_window_most_used_last_week",
                 "l4_device_rolling_window_most_used_last_two_week",
                 "l4_device_rolling_window_most_used_last_month",
                 "l4_device_rolling_window_most_used_last_three_month"],
                "l4_device_rolling_window_most_used"
            ),
        ]
    )