from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l4.to_l4_nodes import *


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

            # Number of phone updates
            node(
                l4_rolling_window,
                ["l2_device_number_of_phone_updates_weekly",
                 "params:l4_device_number_of_phone_updates"],
                "l4_device_rolling_window_number_phone_updates"
            ),

            # Write L2 and L3 in L4
            node(
                read_df,
                ["l2_device_handset_summary_with_configuration_weekly"],
                "l4_device_handset_summary_with_configuration_weekly"
            ),
            node(
                read_df,
                ["l3_device_handset_summary_with_configuration_monthly"],
                "l4_device_handset_summary_with_configuration_monthly"
            ),

            node(
                read_df,
                ["l2_previous_device_handset_summary_with_configuration_weekly"],
                "l4_previous_device_handset_summary_with_configuration_weekly"
            ),
            node(
                read_df,
                ["l3_previous_device_handset_summary_with_configuration_monthly"],
                "l4_previous_device_handset_summary_with_configuration_monthly"
            ),
        ]
    )