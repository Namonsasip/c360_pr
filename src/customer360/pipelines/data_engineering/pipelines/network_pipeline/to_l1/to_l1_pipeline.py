from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.network_nodes.to_l1.to_l1_nodes import *
from customer360.utilities.config_parser import *


def network_geo_home_work_location_master_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_geo_home_work_location_master,
                ["l4_geo_home_work_location_id_master_for_l1_network_failed_calls_home_location",
                 "l0_geo_mst_cell_masterplan_for_l1_network_failed_calls_home_location"],
                "l1_network_geo_home_work_location_master"
            ),
        ]
    )


def network_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_voice_features",
                 "params:int_l1_network_voice_features"],
                "int_l1_network_voice_features"
            ),
            node(
                build_network_voice_features,
                ["int_l1_network_voice_features",
                 "params:l1_network_voice_features",
                 "l1_customer_profile_union_daily_feature_for_l1_network_voice_features",
                 "params:exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day"],
                "l1_network_voice_features"
            ),
            node(
                build_network_share_of_3g_time_in_total_time,
                ["l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time",
                 "params:l1_network_share_of_3g_time_in_total_time",
                 "l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time"],
                "l1_network_share_of_3g_time_in_total_time"
            ),       
            node(
                build_network_voice_data_features,
                [
                    "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_start_delay_and_success_rate_features",
                    "l1_customer_profile_union_daily_feature_for_l1_network_start_delay_and_success_rate_features",
                    "params:l1_network_start_delay_and_success_rate_features",
                    "params:l1_network_start_delay_and_success_rate_features_tbl",
                    "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day"],
                "l1_network_start_delay_and_success_rate_features"
            ),
            # node(
            #     build_network_failed_calls_home_location,
            #     [
            #         "l1_network_geo_home_work_location_master",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_failed_calls_home_location",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_failed_calls_home_location",
            #         "l1_customer_profile_union_daily_feature_for_l1_network_failed_calls_home_location",
            #         "params:l1_network_failed_calls_home_location",
            #         "params:exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day",
            #         "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day"
            #     ],
            #     "l1_network_failed_calls_home_location"
            # ),
            # node(
            #     build_network_good_and_bad_cells_features,
            #     [
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features",
            #         "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features",
            
            #         "l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features",
            #         "l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features",
            
            #         "l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features",
            #         "params:l1_network_good_and_bad_cells_features",
            #         "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day",
            #         "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day",
            #         "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day",
            #         "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day",
            #         "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day",
            #         "params:exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day"
            #     ],
            #     "l1_network_good_and_bad_cells_features"
            # ),
        ]
    )

def network_cei_voice_qoe_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_network_cei_voice_qoe_incoming,
                [
                    "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_cei_voice_qoe_incoming",
                    "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_cei_voice_qoe_incoming",
                    "l0_network_xdr_cs_cdr_ims_mt_call_leg_sip_for_l1_network_cei_voice_qoe_incoming",
                    "l1_customer_profile_union_daily_feature_for_l1_network_cei_voice_qoe_incoming",
                    "params:l1_network_cei_voice_qoe_incoming",
                    "params:exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day",
                    "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day"],
                "l1_network_cei_voice_qoe_incoming"
            ),
            node(
                build_network_cei_voice_qoe_outgoing,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_cei_voice_qoe_outgoing",
                 "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_cei_voice_qoe_outgoing",
                 "l1_customer_profile_union_daily_feature_for_l1_network_cei_voice_qoe_outgoing",
                 "params:l1_network_cei_voice_qoe_outgoing",
                 "params:exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day",
                 "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day"],
                "l1_network_cei_voice_qoe_outgoing"
            ),
        ]
    )

def network_data_traffic_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_usage_sum_data_location_daily_for_l1_network_data_traffic_features",
                 "params:int_l1_network_data_traffic_features"],
                "int_l1_network_data_traffic_features"
            ),
            node(
                build_network_data_traffic_features,
                ["int_l1_network_data_traffic_features",
                 "params:l1_network_data_traffic_features",
                 "l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features"],
                "l1_network_data_traffic_features"
            ),
        ]
    )

def network_failed_call_attempt_and_call_drop_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                [
                    "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:int_l1_network_failed_outgoing_call_attempt_and_call_drop_3g"
                ],
                "int_l1_network_failed_outgoing_call_attempt_and_call_drop_3g"
            ),
            node(
                build_network_lookback_voice_data_features,
                [
                    "int_l1_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "l1_customer_profile_union_daily_feature_for_l1_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:l1_network_failed_outgoing_call_attempt_and_call_drop_3g",
                    "params:l1_network_failed_outgoing_call_attempt_and_call_drop_3g_tbl",
                    "params:exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day"],
                "l1_network_failed_outgoing_call_attempt_and_call_drop_3g"
            ),
            node(
                node_from_config,
                [
                    "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:int_l1_network_failed_incoming_call_attempt_and_call_drop_4g"
                ],
                "int_l1_network_failed_incoming_call_attempt_and_call_drop_4g"
            ),
            node(
                build_network_lookback_voice_data_features,
                [
                    "int_l1_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "l1_customer_profile_union_daily_feature_for_l1_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:l1_network_failed_incoming_call_attempt_and_call_drop_4g",
                    "params:l1_network_failed_incoming_call_attempt_and_call_drop_4g_tbl",
                    "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day"],
                "l1_network_failed_incoming_call_attempt_and_call_drop_4g"
            ),
        ]
    )

def network_cqi_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_network_im_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi",
                 "params:l1_network_im_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_im_cqi",
                 "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day"],
                "l1_network_im_cqi"
            ),

            # node(
            #     build_network_streaming_cqi,
            #     ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi",
            #      "params:l1_network_streaming_cqi",
            #      "l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi",
            #      "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day"],
            #     "l1_network_streaming_cqi"
            # ),

            node(
                build_network_web_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi",
                 "params:l1_network_web_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_web_cqi",
                 "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day"],
                "l1_network_web_cqi"
            ),

            node(
                build_network_voip_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi",
                 "params:l1_network_voip_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi",
                 "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day"],
                "l1_network_voip_cqi"
            ),

            node(
                build_network_volte_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi",
                 "params:l1_network_volte_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi",
                 "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day"],
                "l1_network_volte_cqi"
            ),

            node(
                build_network_user_cqi,
                ["l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi",
                 "params:l1_network_user_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_user_cqi",
                 "params:exception_partition_for_l1_network_user_cqi"],
                "l1_network_user_cqi"
            ),
            # node(
            #     build_network_file_transfer_cqi,
            #     ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_fileaccess_1day_for_l1_network_file_transfer_cqi",
            #      "params:l1_network_file_transfer_cqi",
            #      "l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi",
            #      "params:exception_partitions_list_for_l1_network_file_transfer_cqi"],
            #     "l1_network_file_transfer_cqi"
            # ),
        ]
    )
def network_cei_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_network_voice_data_features,
                ["l0_network_sdr_dyn_cea_cei_voiceqoe_usr_1day_for_l1_network_voice_cei",
                 "l1_customer_profile_union_daily_feature_for_l1_network_voice_cei",
                 "params:l1_network_voice_cei",
                 "params:l1_network_voice_cei_tbl",
                 "params:exception_partitions_list_for_network_sdr_dyn_cea_cei_voiceqoe_usr_1day"],
                "l1_network_voice_cei"
            ),
            node(
                build_network_voice_data_features,
                ["l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cei",
                 "l1_customer_profile_union_daily_feature_for_l1_network_data_cei",
                 "params:l1_network_data_cei",
                 "params:l1_network_data_cei_tbl",
                 "params:exception_partitions_for_l1_network_data_cei"],
                "l1_network_data_cei"
            ),    
        ]
    )