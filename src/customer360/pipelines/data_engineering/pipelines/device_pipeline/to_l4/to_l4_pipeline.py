from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def device_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l3_device_handset_summary_with_configuration_monthly","params:l4_device_handset_summary_with_configuration"],
                "l4_device_rolling_window_handset_summary_with_configuration"
            ),
        ]
    )