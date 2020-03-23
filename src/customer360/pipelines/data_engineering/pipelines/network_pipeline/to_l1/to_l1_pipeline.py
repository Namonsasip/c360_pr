from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.utilities.re_usable_functions import l1_massive_processing
from src.customer360.pipelines.data_engineering.nodes.network_nodes.to_l1.to_l1_nodes import *


def network_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_network_sdr_dyn_cea_cei_cei_cei_qoe_cell_usr_voice_1day",
                 "params:int_l1_network_voice_features"],
                "int_l1_network_voice_features"
            ),

            node(
                l1_massive_processing,
                ["int_l1_network_voice_features",
                 "params:l1_network_voice_features",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_voice_features"
            ),

            node(
                get_good_and_bad_cells_for_each_customer,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day",
                 "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day",
                 "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day",
                 "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day",
                 "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day",
                 "l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day"],
                "int_l1_network_good_and_bad_cells_features"
            ),
            node(
                l1_massive_processing,
                ["int_l1_network_good_and_bad_cells_features",
                 "params:l1_network_good_and_bad_cells_features",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_good_and_bad_cells_features"
            ),

            node(
                l1_massive_processing,
                ["l0_usage_sum_voice_location_daily",
                 "params:l1_network_share_of_3g_time_in_total_time",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_share_of_3g_time_in_total_time"
            ),

            node(
                node_from_config,
                ["l0_usage_sum_data_location_daily",
                 "params:int_l1_network_data_traffic_features"],
                "int_l1_network_data_traffic_features"
            ),
            node(
                l1_massive_processing,
                ["int_l1_network_data_traffic_features",
                 "params:l1_network_data_traffic_features",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_data_traffic_features"
            ),

            node(
                l1_massive_processing,
                ["l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day",
                 "params:l1_network_data_cqi",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_data_cqi"
            ),

            node(
                l1_massive_processing,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day",
                 "params:l1_network_im_cqi",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_im_cqi"
            ),

            node(
                l1_massive_processing,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day",
                 "params:l1_network_streaming_cqi",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_streaming_cqi"
            ),

            node(
                l1_massive_processing,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day",
                 "params:l1_network_web_cqi",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_web_cqi"
            ),

            node(
                l1_massive_processing,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day",
                 "params:l1_network_voip_cqi",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_voip_cqi"
            ),

            node(
                l1_massive_processing,
                ["l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day",
                 "params:l1_network_volte_cqi",
                 "l1_customer_profile_union_daily_feature"],
                "l1_network_volte_cqi"
            )
        ]
    )
