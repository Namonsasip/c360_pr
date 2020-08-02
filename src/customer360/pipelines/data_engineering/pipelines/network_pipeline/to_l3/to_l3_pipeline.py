from kedro.pipeline import Pipeline, node
from customer360.pipelines.data_engineering.nodes.network_nodes.to_l3.to_l3_nodes import *


def network_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                dac_for_voice_features,
                ["l1_network_voice_features_for_l3_network_voice_features",
                 "params:exception_partition_list_for_l1_network_voice_features_for_l3_network_voice_features"],
                "int_l1_network_voice_features_for_l3_network_voice_features"
            ),
            node(
                node_from_config,
                ["int_l1_network_voice_features_for_l3_network_voice_features",
                 "params:int_l3_network_voice_features"],
                "int_l3_network_voice_features"
            ),

            node(
                node_from_config,
                ["int_l3_network_voice_features",
                 "params:l3_network_voice_features"],
                "l3_network_voice_features"
            ),

            # node(
            #     build_l3_network_good_and_bad_cells_features,
            #     ["l1_network_good_and_bad_cells_features_for_l3_network_good_and_bad_cells_features",
            #      "params:l3_network_good_and_bad_cells_features"],
            #     "l3_network_good_and_bad_cells_features"
            # ),
            node(
                build_l3_network_share_of_3g_time_in_total_time,
                ["l1_network_share_of_3g_time_in_total_time_for_l3_network_share_of_3g_time_in_total_time",
                 "params:l3_network_share_of_3g_time_in_total_time"],
                "l3_network_share_of_3g_time_in_total_time"
            ),

            node(
                build_l3_network_data_traffic_features,
                ["l1_network_data_traffic_features_for_l3_network_data_traffic_features",
                 "params:l3_network_data_traffic_features"],
                "l3_network_data_traffic_features"
            ),

            ## node(
            ##     build_l3_network_data_cqi,
            ##     ["l1_network_data_cqi_for_l3_network_data_cqi",
            ##      "params:l3_network_data_cqi"],
            ##     "l3_network_data_cqi"
            ## ),
            node(
                build_l3_network_im_cqi,
                ["l1_network_im_cqi_for_l3_network_im_cqi",
                 "params:l3_network_im_cqi",
                 "params:exception_partition_list_for_l1_network_im_cqi_for_l3_network_im_cqi"],
                "l3_network_im_cqi"
            ),
            # node(
            #     build_l3_network_streaming_cqi,
            #     ["l1_network_streaming_cqi_for_l3_network_streaming_cqi",
            #      "params:l3_network_streaming_cqi",
            #      "params:exception_partition_list_for_l1_network_streaming_cqi_for_l3_network_streaming_cqi"],
            #     "l3_network_streaming_cqi"
            # ),
            node(
                build_l3_network_web_cqi,
                ["l1_network_web_cqi_for_l3_network_web_cqi",
                 "params:l3_network_web_cqi",
                 "params:exception_partition_list_for_l1_network_web_cqi_for_l3_network_web_cqi"],
                "l3_network_web_cqi"
            ),
            node(
                build_l3_network_voip_cqi,
                ["l1_network_voip_cqi_for_l3_network_voip_cqi",
                 "params:l3_network_voip_cqi",
                 "params:exception_partition_list_for_l1_network_voip_cqi_for_l3_network_voip_cqi"],
                "l3_network_voip_cqi"
            ),
            # node(
            #     build_l3_network_volte_cqi,
            #     ["l1_network_volte_cqi_for_l3_network_volte_cqi",
            #      "params:l3_network_volte_cqi",
            #      "params:exception_partition_list_for_l1_network_volte_cqi_for_l3_network_volte_cqi"],
            #     "l3_network_volte_cqi"
            # ),
            node(
                build_l3_network_user_cqi,
                ["l1_network_user_cqi_for_l3_network_user_cqi",
                 "params:l3_network_user_cqi",
                 "params:exception_partition_list_for_l1_network_user_cqi_for_l3_network_user_cqi"],
                "l3_network_user_cqi"
            ),
            node(
                build_l3_network_file_transfer_cqi,
                ["l1_network_file_transfer_cqi_for_l3_network_file_transfer_cqi",
                 "params:l3_network_file_transfer_cqi",
                 "params:exception_partition_list_for_l1_network_file_transfer_cqi_for_l3_network_file_transfer_cqi"],
                "l3_network_file_transfer_cqi"
            ),
            node(
                build_l3_network_features,
                [
                    "l1_network_cei_voice_qoe_incoming_for_l3_network_cei_voice_qoe_incoming",
                    "params:l3_network_cei_voice_qoe_incoming",
                    "params:l3_network_cei_voice_qoe_incoming_tbl",
                    "params:exception_partition_list_for_l3_network_cei_voice_qoe_incoming"
                ],
                "l3_network_cei_voice_qoe_incoming"
            ),
            # node(
            #     build_l3_network_features,
            #     [
            #         "l1_network_cei_voice_qoe_outgoing_for_l3_network_cei_voice_qoe_outgoing",
            #         "params:l3_network_cei_voice_qoe_outgoing",
            #         "params:l3_network_cei_voice_qoe_outgoing_tbl",
            #         "params:exception_partition_list_for_l3_network_cei_voice_qoe_outgoing"
            #     ],
            #     "l3_network_cei_voice_qoe_outgoing"
            # ),
            # node(
            #     build_l3_network_features,
            #     [
            #         "l1_network_voice_cei_for_l3_network_voice_cei",
            #         "params:l3_network_voice_cei",
            #         "params:l3_network_voice_cei_tbl",
            #         "params:exception_partition_list_for_l3_network_voice_cei"
            #     ],
            #     "l3_network_voice_cei"
            # ),
            # node(
            #     build_l3_network_features,
            #     [
            #         "l1_network_data_cei_for_l3_network_data_cei",
            #         "params:l3_network_data_cei",
            #         "params:l3_network_data_cei_tbl",
            #         "params:exception_partition_list_for_l3_network_data_cei"
            #     ],
            #     "l3_network_data_cei"
            # ),
            node(
                build_l3_network_features_lookback,
                [
                    "l1_network_failed_outgoing_call_attempt_and_call_drop_3g_for_l3_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:int_l3_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:l3_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:l3_network_failed_outgoing_call_attempt_and_call_drop_3g_tbl",
                    "params:exception_partition_list_for_l3_network_failed_outgoing_call_attempt_and_call_drop_3g"
                ],
                "l3_network_failed_outgoing_call_attempt_and_call_drop_3g"
            ),
            node(
                build_l3_network_features_lookback,
                [
                    "l1_network_failed_incoming_call_attempt_and_call_drop_4g_for_l3_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:int_l3_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:l3_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:l3_network_failed_incoming_call_attempt_and_call_drop_4g_tbl",
                    "params:exception_partition_list_for_l3_network_failed_incoming_call_attempt_and_call_drop_4g"
                ],
                "l3_network_failed_incoming_call_attempt_and_call_drop_4g"
            ),
            # node(
            #     build_l3_network_features,
            #     [
            #         "l1_network_start_delay_and_success_rate_features_for_l3_network_start_delay_and_success_rate_features",
            #         "params:l3_network_start_delay_and_success_rate_features",
            #         "params:l3_network_start_delay_and_success_rate_features_tbl",
            #         "params:exception_partition_list_for_l3_network_start_delay_and_success_rate_features"
            #     ],
            #     "l3_network_start_delay_and_success_rate_features"
            # ),
            # node(
            #     build_l3_network_features,
            #     [
            #         "l1_network_failed_calls_home_location_for_l3_network_failed_calls_home_location",
            #         "params:l3_network_failed_calls_home_location",
            #         "params:l3_network_failed_calls_home_location_tbl",
            #         "params:exception_partition_list_for_l3_network_failed_calls_home_location"
            #     ],
            #     "l3_network_failed_calls_home_location"
            # )
        ]
    )
