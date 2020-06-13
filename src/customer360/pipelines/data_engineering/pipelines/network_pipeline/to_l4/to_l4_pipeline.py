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
                ["l2_network_data_cqi",
                 "params:l4_network_data_cqi"],
                "l4_network_data_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_im_cqi",
                 "params:l4_network_im_cqi"],
                "l4_network_im_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_streaming_cqi",
                 "params:l4_network_streaming_cqi"],
                "l4_network_streaming_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_web_cqi",
                 "params:l4_network_web_cqi"],
                "l4_network_web_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_voip_cqi",
                 "params:l4_network_voip_cqi"],
                "l4_network_voip_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_volte_cqi",
                 "params:l4_network_volte_cqi"],
                "l4_network_volte_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_user_cqi",
                 "params:l4_network_user_cqi"],
                "l4_network_user_cqi"
            ),
            node(
                l4_rolling_window,
                ["l2_network_file_transfer_cqi",
                 "params:l4_network_file_transfer_cqi"],
                "l4_network_file_transfer_cqi"
            ),
        ]
    )
