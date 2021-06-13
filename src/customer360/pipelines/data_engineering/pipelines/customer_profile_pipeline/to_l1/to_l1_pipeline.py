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
            # node(
            #     union_daily_cust_profile,
            #     ["l0_customer_profile_profile_customer_profile_pre_current",
            #      "l0_customer_profile_profile_customer_profile_post_current",
            #      "l0_customer_profile_profile_customer_profile_post_non_mobile_current_non_mobile_current",
            #      "params:l1_customer_profile_union_daily_feature"],
            #      "int_l1_customer_profile_union_daily_feature_2"
            # ),
            # node(
            #     add_feature_profile_with_join_table,
            #     ["int_l1_customer_profile_union_daily_feature_2",
            #      "l1_customer_profile_mnp_order_for_union_daily_feature",
            #      "l0_product_offering_for_l1_customer_profile_union_daily_feature",
            #      "l0_product_offering_pps_for_l1_customer_profile_union_daily_feature",
            #      "l0_customer_profile_ru_t_mobile_same_id_card_for_l1_customer_profile_union_daily_feature",
            #      "l0_product_drm_resenade_package_master_for_l1_customer_profile_union_daily_feature",
            #      "l0_product_ru_m_mkt_promo_group_master_for_l1_customer_profile_union_daily_feature",
            #      "l0_product_pru_m_package_master_group_for_l1_customer_profile_union_daily_feature"
            #      ],
            #     "int_l1_customer_profile_union_daily_feature_3"
            # ),
            # node(
            #     generate_modified_subscription_identifier,
            #     ["int_l1_customer_profile_union_daily_feature_3"],
            #     "int_modified_sub_id_l1_customer_profile_union_daily_feature"
            # ),
            # node(
            #     add_feature_lot5,
            #     ["l0_customer_profile_fbb_t_active_sub_summary_detail_for_l1_customer_profile_union_daily_feature",
            #      "int_modified_sub_id_l1_customer_profile_union_daily_feature"],
            #     "int_l1_customer_profile_union_daily_feature_4"
            # ),
            node(
                def_feature_lot7,
                ["int_l1_customer_profile_union_daily_feature_4",
                 "l1_customer_profile_order_change_charge_post_type_for_union_daily_feature",
                 "l1_customer_profile_order_change_charge_pre_type_for_union_daily_feature",
                 "l0_profile_customer_profile_cm_t_newsub_postpaid_for_l1_customer_profile_union_daily_feature",
                 "l1_customer_profile_pp_iden_for_union_daily_feature"
                 ],
                "int_l1_customer_profile_union_daily_feature_5"
            ),
            node(
                add_start_of_week_and_month,
                ["int_l1_customer_profile_union_daily_feature_5",
                 "params:customer_profile_partition_col"],
                "l1_customer_profile_union_daily_feature"
            ),
            ####################################################################################################
            # node(
            #     test_order_change_charge_type,
            #     ["l0_touchpoints_service_order_profile_for_test"],
            #      # "params:l1_customer_profile_order_change_charge_post_type_for_union_daily_feature"],
            #     "l1_customer_profile_order_change_charge_post_type_for_union_daily_feature"
            # ),
            # node(
            #     test_order_change_charge_type_pre,
            #     ["l0_profile_service_order_profile_pps_for_test"],
            #     # "params:l1_customer_profile_order_change_charge_post_type_for_union_daily_feature"],
            #     "l1_customer_profile_order_change_charge_pre_type_for_union_daily_feature"
            # ),
            # node(
            #     test_mnp_order,
            #     ["l0_customer_profile_mnp_request_port_for_test"],
            #     # "params:l1_customer_profile_order_change_charge_post_type_for_union_daily_feature"],
            #     "l1_customer_profile_mnp_order_for_union_daily_feature"
            # ),
            node(
                test_prepaid_iden,
                ["l0_profile_prepaid_identification_for_l1_customer_profile_union_daily_feature",
                 "l0_profile_prepaid_identn_profile_hist_for_l1_customer_profile_union_daily_feature"],
                # "params:l1_customer_profile_order_change_charge_post_type_for_union_daily_feature"],
                "l1_customer_profile_pp_iden_for_union_daily_feature"
            ),
        ]
    )

