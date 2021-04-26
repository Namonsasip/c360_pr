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
from customer360.utilities.re_usable_functions import l1_massive_processing
from src.customer360.pipelines.data_engineering.nodes.touchpoints_nodes.to_l1.to_l1_nodes import *


def touchpoints_to_l1_pipeline_contact(**kwargs):
    return Pipeline(
        [
            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_touchpoints_acc_qmt_transaction",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_shop_features",
                 "params:l1_touchpoints_contact_shop_features_tbl",
                 "params:exception_partition_list_for_l0_touchpoints_acc_qmt_transaction_for_l1_touchpoints_contact_shop_features"],
                ["int_l0_touchpoints_acc_qmt_transaction",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_shop_features"]
            ),
            node(
                l1_touchpoints_contact_shop_features,
                ["int_l0_touchpoints_acc_qmt_transaction",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_shop_features"],
                 "l1_touchpoints_contact_shop_features"
            ),

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_online_acc_ai_chatbot_summary",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_aunjai_chatbot_features",
                 "params:l1_touchpoints_aunjai_chatbot_features_tbl",
                 "params:exception_partition_list_for_l0_online_acc_ai_chatbot_summary_for_l1_touchpoints_aunjai_chatbot_features"],
                ["int_l0_online_acc_ai_chatbot_summary",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_aunjai_chatbot_features"]
            ),

            node(
                l1_touchpoints_aunjai_chatbot_features,
                ["int_l0_online_acc_ai_chatbot_summary",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_aunjai_chatbot_features"],
                "l1_touchpoints_aunjai_chatbot_features"
            ),

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_touchpoints_acc_oa_log",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_call_center_features",
                 "params:l1_touchpoints_contact_call_center_features_tbl",
                 "params:exception_partition_list_for_l0_touchpoints_acc_oa_log_for_l1_touchpoints_contact_call_center_features"],
                ["int_l0_touchpoints_acc_oa_log",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_call_center_features"]
            ),

            node(
                l1_touchpoints_contact_call_center_features,
                ["int_l0_touchpoints_acc_oa_log",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_call_center_features"],
                "l1_touchpoints_contact_call_center_features"
            ),

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_touchpoints_myais_distinct_sub_daily",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_myais_features",
                 "params:l1_touchpoints_contact_myais_features_tbl",
                 "params:exception_partition_list_for_l0_touchpoints_myais_distinct_sub_daily_for_l1_touchpoints_contact_myais_features"],
                ["int_l0_touchpoints_myais_distinct_sub_daily",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_myais_features"]
            ),
            node(
                l1_touchpoints_contact_myais_features,
                ["int_l0_touchpoints_myais_distinct_sub_daily",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_contact_myais_features"
                 ],
                "l1_touchpoints_contact_myais_features"
            ),
        ]
    )


def touchpoints_to_l1_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_usage_call_relation_sum_daily_for_l1_touchpoints_to_call_center_features",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_to_call_center_features",
                 "params:l1_touchpoints_to_call_center_features_tbl",
                 "params:exception_partition_list_for_l0_usage_call_relation_sum_daily_for_l1_touchpoints_to_call_center_features"],
                ["int_l0_usage_call_relation_sum_daily_for_l1_touchpoints_to_call_center_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_to_call_center_features"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_usage_call_relation_sum_daily_for_l1_touchpoints_to_call_center_features",
                 "params:l1_touchpoints_to_call_center_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_to_call_center_features"],
                "l1_touchpoints_to_call_center_features"
            ),

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_usage_call_relation_sum_daily_for_l1_touchpoints_from_call_center_features",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_from_call_center_features",
                 "params:l1_touchpoints_from_call_center_features_tbl",
                 "params:exception_partition_list_for_l0_usage_call_relation_sum_daily_for_l1_touchpoints_from_call_center_features"],
                ["int_l0_usage_call_relation_sum_daily_for_l1_touchpoints_from_call_center_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_from_call_center_features"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_usage_call_relation_sum_daily_for_l1_touchpoints_from_call_center_features",
                 "params:l1_touchpoints_from_call_center_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_from_call_center_features"],
                "l1_touchpoints_from_call_center_features"
            ),

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_complaints_ais_nim_work",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_nim_work_features",
                 "params:l1_touchpoints_nim_work_features_tbl"],
                ["int_l0_complaints_ais_nim_work",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_nim_work_features"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_complaints_ais_nim_work",
                 "params:l1_touchpoints_nim_work_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_nim_work_features"],
                "l1_touchpoints_nim_work_features"
            ),

            node(
                dac_for_touchpoints_to_l1_intermediate_pipeline,
                ["l0_touchpoints_acc_ivr_log_daily",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_ivr_features",
                 "params:l1_touchpoints_ivr_features_tbl"],
                ["int_l0_touchpoints_acc_ivr_log_daily",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_ivr_features"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_touchpoints_acc_ivr_log_daily",
                 "params:l1_touchpoints_ivr_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_touchpoints_ivr_features"],
                "l1_touchpoints_ivr_features"
            )
        ]
    )
