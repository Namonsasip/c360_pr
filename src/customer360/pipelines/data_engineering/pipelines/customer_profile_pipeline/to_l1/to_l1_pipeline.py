# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import node_from_config
from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l1.to_l1_nodes import *
from customer360.utilities.re_usable_functions import add_start_of_week_and_month


def customer_profile_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                union_daily_cust_profile,
                [
                    "l0_customer_profile_profile_customer_profile_pre_current",
                    "l0_customer_profile_profile_customer_profile_post_current",
                    "l0_customer_profile_profile_customer_profile_post_non_mobile_current_non_mobile_current",
                    "params:l1_customer_profile_union_daily_feature"
                 ],
                    "l1_customer_profile_union_daily_temp1"
            ),
            node(
                row_number_func,
                [
                    "l1_customer_profile_union_daily_temp1",
                    "l0_customer_profile_mnp_request_port_for_l1_customer_profile_union_daily_feature",
                    "l0_product_offering_pps_for_l1_customer_profile_union_daily_feature"
                 ],
                [
                    "int_l1_customer_profile_union_daily_temp1",
                    "l1_customer_profile_mnp_request_port_in_row",
                    "l1_customer_profile_mnp_request_port_out_row",
                    "l1_product_offering_pps_row"
                 ]
            ),
            node(
                add_feature_profile_with_join_table,
                [
                    "int_l1_customer_profile_union_daily_temp1",
                    "l1_customer_profile_mnp_request_port_in_row",
                    "l1_customer_profile_mnp_request_port_out_row"
                 ],
                    "l1_feature_profile_with_join_table_temp1"
            ),
            node(
                add_feature_profile_with_join_table1,
                [
                    "l1_feature_profile_with_join_table_temp1",
                    "l0_product_offering_for_l1_customer_profile_union_daily_feature",
                    "l1_product_offering_pps_row"
                 ],
                    "l1_feature_profile_with_join_table1_temp2"
            ),

            node(
                add_feature_profile_with_join_table1_1,
                [
                    "l1_feature_profile_with_join_table1_temp2",
                    "l0_product_offering_for_l1_customer_profile_union_daily_feature"
                 ],
                    "l1_feature_profile_with_join_table_temp2"
            ),
            node(
                add_feature_profile_with_join_table2,
                [
                    "l1_feature_profile_with_join_table_temp2",
                    "l0_customer_profile_ru_t_mobile_same_id_card_for_l1_customer_profile_union_daily_feature",
                    "l0_product_drm_resenade_package_master_for_l1_customer_profile_union_daily_feature",
                    "l0_product_ru_m_mkt_promo_group_master_for_l1_customer_profile_union_daily_feature",
                    "l0_product_pru_m_package_master_group_for_l1_customer_profile_union_daily_feature"
                 ],
                    "l1_customer_profile_union_daily_temp2"
            ),
            node(
                generate_modified_subscription_identifier,
                [
                    "l1_customer_profile_union_daily_temp2"
                ],
                    "int_modified_sub_id_l1_customer_profile_union_daily_feature"
            ),
            node(
                add_feature_lot5,
                [
                    "l0_customer_profile_fbb_t_active_sub_summary_detail_for_l1_customer_profile_union_daily_feature",
                    "int_modified_sub_id_l1_customer_profile_union_daily_feature"
                ],
                    "l1_customer_profile_union_daily_temp3"
            ),
            node(
                func_filter_date,
                [
                    "l1_customer_profile_union_daily_temp3",
                    "l0_profile_service_order_profile_pps_for_l1_customer_profile_union_daily_feature"
                ],
                [
                    "int_l1_customer_profile_union_daily_temp3",
                    "l1_profile_service_order_profile_pps_filter"
                ]
            ),
            node(
                func_master_table,
                [
                    "int_l1_customer_profile_union_daily_temp3",
                    "l0_touchpoints_service_order_profile_for_l1_customer_profile_union_daily_feature",
                    "l0_profile_prepaid_identn_profile_hist_for_l1_customer_profile_union_daily_feature"
                ],
                [
                    "int1_l1_customer_profile_union_daily_temp3",
                    "l1_touchpoints_service_order_profile_flag_master",
                    "l1_profile_prepaid_identn_profile_hist_row_num"
                ]
            ),
            node(
                row_number_func1,
                [
                    "int1_l1_customer_profile_union_daily_temp3",
                    "l1_touchpoints_service_order_profile_flag_master",
                    "l1_profile_service_order_profile_pps_filter",
                    "l0_profile_customer_profile_cm_t_newsub_postpaid_for_l1_customer_profile_union_daily_feature",
                    "l0_profile_prepaid_identification_for_l1_customer_profile_union_daily_feature"
                 ],
                [
                    "int2_l1_customer_profile_union_daily_temp3",
                    "l1_touchpoints_service_order_profile_row_num",
                    "l1_profile_service_order_profile_pps_row_num",
                    "l1_profile_customer_profile_cm_t_newsub_postpaid_row_num",
                    "l1_profile_prepaid_identification_row_num"
                 ]
            ),
            node(
                def_feature_lot7_func,
                [
                    "int2_l1_customer_profile_union_daily_temp3",
                    "l1_touchpoints_service_order_profile_row_num",
                    "l1_profile_service_order_profile_pps_row_num"
                 ],
                [
                    "l1_feature_lot7_temp1",
                    "l1_service_pre_post_temp"
                 ]
            ),
            node(
                def_feature_lot7_func1,
                [
                    "l1_touchpoints_service_order_profile_flag_master",
                    "l1_feature_lot7_temp1"
                 ],
                    "l1_feature_lot7_temp3"
            ),
            node(
                def_feature_lot7_func2,
                [
                    "l1_feature_lot7_temp3",
                    "l1_service_pre_post_temp",
                    "l1_feature_lot7_temp1"
                 ],
                    "l1_feature_lot7_temp4"
            ),
            node(
                def_feature_lot7_func3,
                [
                    "l1_feature_lot7_temp4",
                    "l1_profile_customer_profile_cm_t_newsub_postpaid_row_num"
                 ],
                    "l1_feature_lot7_temp5"
            ),
            node(
                def_feature_lot7_func4,
                [
                    "l1_feature_lot7_temp5",
                    "l1_profile_prepaid_identification_row_num",
                    "l1_profile_prepaid_identn_profile_hist_row_num"
                 ],
                    "l1_customer_profile_union_daily_temp4"
            ),
            node(
                add_start_of_week_and_month,
                [
                    "l1_customer_profile_union_daily_temp4",
                    "params:customer_profile_partition_col"
                 ],
                    "l1_customer_profile_union_daily_feature"
            ),
            ### move from geo
            node(
                massive_processing_with_l1_customer_profile_imsi_daily_feature,
                ["l0_customer_profile_imsi_daily_feature",
                 "params:l1_customer_profile_imsi_daily_feature"
                 ],
                "l1_customer_profile_imsi_daily_feature"
            ),
        ]
    )

