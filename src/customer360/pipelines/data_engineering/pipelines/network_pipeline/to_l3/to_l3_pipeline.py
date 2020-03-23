from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def network_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l1_network_voice_features",
                 "params:int_l3_network_voice_features"],
                "int_l3_network_voice_features"
            ),

            node(
                node_from_config,
                ["int_l3_network_voice_features",
                 "params:l3_network_voice_features"],
                "l3_network_voice_features"
            ),

            node(
                node_from_config,
                ["l1_network_good_and_bad_cells_features",
                 "params:l3_network_good_and_bad_cells_features"],
                "l3_network_good_and_bad_cells_features"
            ),
            node(
                node_from_config,
                ["l1_network_share_of_3g_time_in_total_time",
                 "params:l3_network_share_of_3g_time_in_total_time"],
                "l3_network_share_of_3g_time_in_total_time"
            ),

            node(
                node_from_config,
                ["l1_network_data_traffic_features",
                 "params:l3_network_data_traffic_features"],
                "l3_network_data_traffic_features"
            ),

            node(
                node_from_config,
                ["l1_network_data_cqi",
                 "params:l3_network_data_cqi"],
                "l3_network_data_cqi"
            ),
            node(
                node_from_config,
                ["l1_network_im_cqi",
                 "params:l3_network_im_cqi"],
                "l3_network_im_cqi"
            ),
            node(
                node_from_config,
                ["l1_network_streaming_cqi",
                 "params:l3_network_streaming_cqi"],
                "l3_network_streaming_cqi"
            ),
            node(
                node_from_config,
                ["l1_network_web_cqi",
                 "params:l3_network_web_cqi"],
                "l3_network_web_cqi"
            ),
            node(
                node_from_config,
                ["l1_network_voip_cqi",
                 "params:l3_network_voip_cqi"],
                "l3_network_voip_cqi"
            ),
            node(
                node_from_config,
                ["l1_network_volte_cqi",
                 "params:l3_network_volte_cqi"],
                "l3_network_volte_cqi"
            ),
        ]
    )
