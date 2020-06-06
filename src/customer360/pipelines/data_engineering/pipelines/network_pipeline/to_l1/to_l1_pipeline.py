from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.network_nodes.to_l1.to_l1_nodes import *
from customer360.utilities.config_parser import *


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
                 "l1_customer_profile_union_daily_feature_for_l1_network_voice_features"],
                "l1_network_voice_features"
            ),

            node(build_network_good_and_bad_cells_features, [
                "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features",
                "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features",
                "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features",
                "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features",
                "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features",
                "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features",

                "l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features",
                "l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features",

                "l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features",
                "params:l1_network_good_and_bad_cells_features",
            ], "l1_network_good_and_bad_cells_features"),

            # node(
            #     get_good_and_bad_cells_for_each_customer,
            #     ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features",
            #      "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features",
            #      "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features",
            #      "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features",
            #      "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features",
            #      "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features"],
            #     "int_l1_network_good_and_bad_cells_features"
            # ),
            # node(
            #     get_transaction_on_good_and_bad_cells,
            #     ["int_l1_network_good_and_bad_cells_features",
            #      "l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features",
            #      "l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features"],
            #     "int_l1_network_get_transaction_on_good_and_bad_cells"
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l1_network_get_transaction_on_good_and_bad_cells",
            #      "params:l1_network_good_and_bad_cells_features",
            #      "l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features"],
            #     "l1_network_good_and_bad_cells_features"
            # ),

            node(
                build_network_share_of_3g_time_in_total_time,
                ["l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time",
                 "params:l1_network_share_of_3g_time_in_total_time",
                 "l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time"],
                "l1_network_share_of_3g_time_in_total_time"
            ),

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

            node(
                build_network_data_cqi,
                ["l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi",
                 "params:l1_network_data_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_data_cqi"],
                "l1_network_data_cqi"
            ),

            node(
                build_network_im_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi",
                 "params:l1_network_im_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_im_cqi"],
                "l1_network_im_cqi"
            ),

            node(
                build_network_streaming_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi",
                 "params:l1_network_streaming_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi"],
                "l1_network_streaming_cqi"
            ),

            node(
                build_network_web_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi",
                 "params:l1_network_web_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_web_cqi"],
                "l1_network_web_cqi"
            ),

            node(
                build_network_voip_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi",
                 "params:l1_network_voip_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi"],
                "l1_network_voip_cqi"
            ),

            node(
                build_network_volte_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi",
                 "params:l1_network_volte_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi"],
                "l1_network_volte_cqi"
            ),

            node(
                build_network_user_cqi,
                ["l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi",
                 "params:l1_network_user_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_user_cqi"],
                "l1_network_user_cqi"
            ),

            node(
                build_network_file_transfer_cqi,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_fileaccess_1day_for_l1_network_file_transfer_cqi",
                 "params:l1_network_file_transfer_cqi",
                 "l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi"],
                "l1_network_file_transfer_cqi"
            ),
        ]
    )
