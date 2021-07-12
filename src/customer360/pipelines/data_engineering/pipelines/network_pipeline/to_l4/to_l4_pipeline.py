from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def network_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_network_voice_features",
                 "params:l4_network_voice_features"],
                "l4_network_voice_features"
            ),

            node(
                l4_rolling_window,
                ["l2_network_good_and_bad_cells_features",
                 "params:l4_network_good_and_bad_cells_features"],
                "l4_network_good_and_bad_cells_features"
            ),
            node(
                l4_rolling_window,
                ["l2_network_share_of_3g_time_in_total_time",
                 "params:l4_network_share_of_3g_time_in_total_time"],
                "l4_network_share_of_3g_time_in_total_time"
            ),

            node(
                l4_rolling_window,
                ["l2_network_data_traffic_features",
                 "params:l4_network_data_traffic_features"],
                "l4_network_data_traffic_features"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_cei_voice_qoe_incoming",
                    "params:l4_network_cei_voice_qoe_incoming"
                ],
                "l4_network_cei_voice_qoe_incoming"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_cei_voice_qoe_outgoing",
                    "params:l4_network_cei_voice_qoe_outgoing"
                ],
                "l4_network_cei_voice_qoe_outgoing"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_voice_cei",
                    "params:l4_network_voice_cei"
                ],
                "l4_network_voice_cei"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_data_cei",
                    "params:l4_network_data_cei"
                ],
                "l4_network_data_cei"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:l4_network_failed_outgoing_call_attempt_and_call_drop_3g"
                ],
                "l4_network_failed_outgoing_call_attempt_and_call_drop_3g"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:l4_network_failed_incoming_call_attempt_and_call_drop_4g"
                ],
                "l4_network_failed_incoming_call_attempt_and_call_drop_4g"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_start_delay_and_success_rate_features",
                    "params:l4_network_start_delay_and_success_rate_features"
                ],
                "l4_network_start_delay_and_success_rate_features"
            ),
            node(
                l4_rolling_window,
                [
                    "l2_network_failed_calls_home_location",
                    "params:l4_network_failed_calls_home_location"
                ],
                "l4_network_failed_calls_home_location"
            )

        ]
    )
