from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.network_nodes.to_l2.to_l2_nodes import *


def network_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l1_network_voice_features_for_l2_network_voice_features",
                 "params:int_l2_network_voice_features"],
                "int_l2_network_voice_features"
            ),

            node(
                build_l2_network_voice_features,
                ["int_l2_network_voice_features",
                 "params:l2_network_voice_features",
                 "params:exception_partition_list_for_l1_network_voice_features_for_l2_network_voice_features"],
                "l2_network_voice_features"
            ),

            node(
                build_l2_network_good_and_bad_cells_features,
                ["l1_network_good_and_bad_cells_features_for_l2_network_good_and_bad_cells_features",
                 "params:l2_network_good_and_bad_cells_features"],
                "l2_network_good_and_bad_cells_features"
            ),
            node(
                build_l2_network_share_of_3g_time_in_total_time,
                ["l1_network_share_of_3g_time_in_total_time_for_l2_network_share_of_3g_time_in_total_time",
                 "params:l2_network_share_of_3g_time_in_total_time",
                 "params:exception_partition_list_for_l1_network_share_of_3g_time_in_total_time_for_l2_network_share_of_3g_time_in_total_time"],
                "l2_network_share_of_3g_time_in_total_time"
            ),

            node(
                build_l2_network_data_traffic_features,
                ["l1_network_data_traffic_features_for_l2_network_data_traffic_features",
                 "params:l2_network_data_traffic_features",
                 "params:exception_partition_list_for_l1_network_data_traffic_features_for_l2_network_data_traffic_features"],
                "l2_network_data_traffic_features"
            ),

            node(
                build_l2_network_data_cqi,
                ["l1_network_data_cqi_for_l2_network_data_cqi",
                 "params:l2_network_data_cqi",
                 "params:exception_partition_list_for_l1_network_data_cqi_for_l2_network_data_cqi"],
                "l2_network_data_cqi"
            ),
            node(
                build_l2_network_im_cqi,
                ["l1_network_im_cqi_for_l2_network_im_cqi",
                 "params:l2_network_im_cqi"],
                "l2_network_im_cqi"
            ),
            node(
                build_l2_network_streaming_cqi,
                ["l1_network_streaming_cqi_for_l2_network_streaming_cqi",
                 "params:l2_network_streaming_cqi"],
                "l2_network_streaming_cqi"
            ),
            node(
                build_l2_network_web_cqi,
                ["l1_network_web_cqi_for_l2_network_web_cqi",
                 "params:l2_network_web_cqi",
                 "params:exception_partition_list_for_l1_network_web_cqi_for_l2_network_web_cqi"],
                "l2_network_web_cqi"
            ),
            node(
                build_l2_network_voip_cqi,
                ["l1_network_voip_cqi_for_l2_network_voip_cqi",
                 "params:l2_network_voip_cqi"],
                "l2_network_voip_cqi"
            ),
            node(
                build_l2_network_volte_cqi,
                ["l1_network_volte_cqi_for_l2_network_volte_cqi",
                 "params:l2_network_volte_cqi"],
                "l2_network_volte_cqi"
            ),
            node(
                build_l2_network_user_cqi,
                ["l1_network_user_cqi_for_l2_network_user_cqi",
                 "params:l2_network_user_cqi"],
                "l2_network_user_cqi"
            ),
            node(
                build_l2_network_file_transfer_cqi,
                ["l1_network_file_transfer_cqi_for_l2_network_file_transfer_cqi",
                 "params:l2_network_file_transfer_cqi"],
                "l2_network_file_transfer_cqi"
            ),
        ]
    )
