from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def device_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            # Most used device
            node(
                l4_rolling_window,
                ["l2_device_most_used_intermediate_weekly",
                 "params:l4_device_most_used_initial"],
                "l4_device_rolling_window_most_used_1"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_device_rolling_window_most_used_1",
                 "params:l4_device_most_used_with_ranks"],
                "l4_device_rolling_window_most_used_2"
            ),
            node(
                node_from_config,
                ["l4_device_rolling_window_most_used_2",
                 "params:l4_device_most_used"],
                "l4_device_rolling_window_most_used"
            ),
        ]
    )