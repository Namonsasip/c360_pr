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
from customer360.utilities.re_usable_functions import l1_massive_processing

from src.customer360.pipelines.data_engineering.nodes.complaints_nodes.to_l1.to_l1_nodes import \
    dac_for_complaints_to_l1_pipeline, l1_complaints_survey_after_store_visit, l1_complaints_survey_after_call, \
    l1_complaints_survey_after_myais, l1_complaints_survey_after_chatbot


def complaints_to_l1_pipeline_survey(**kwargs):
    return Pipeline(
        [
            node(
                dac_for_complaints_to_l1_pipeline,
                ["l0_complaints_acc_ai_chatbot_survey",
                 "l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_chatbot",
                 "params:l1_complaints_survey_after_chatbot_tbl",
                 "params:exception_partition_list_for_l0_complaints_acc_ai_chatbot_survey_daily"],
                ["int_l0_complaints_acc_ai_chatbot_survey",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_chatbot"]
            ),
            node(
                l1_complaints_survey_after_chatbot,
                ["int_l0_complaints_acc_ai_chatbot_survey",
                 "params:l1_complaints_survey_after_chatbot",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_chatbot"
                 ],
                "l1_complaints_survey_after_chatbot"
            ),
            # node(
            #     dac_for_complaints_to_l1_pipeline,
            #     ["l0_complaints_acc_atsr_outbound_survey_after_call",
            #      "l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_call",
            #      "params:l1_complaints_survey_after_call_tbl",
            #      "params:exception_partition_list_for_l0_complaints_acc_atsr_outbound_survey_after_call"],
            #     ["int_l0_complaints_acc_atsr_outbound_survey_after_call",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_call"]
            # ),
            # node(
            #     l1_complaints_survey_after_call,
            #     [
            #         "int_l0_complaints_acc_atsr_outbound_survey_after_call",
            #         "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_call"
            #     ],
            #     "l1_complaints_survey_after_call"
            # ),
            #
            # node(
            #     dac_for_complaints_to_l1_pipeline,
            #     ["l0_complaints_complaints_acc_qmt_csi",
            #      "l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_store_visit",
            #      "params:l1_complaints_survey_after_store_visit_tbl",
            #      "params:exception_partition_list_for_l0_complaints_acc_qmt_csi_daily"],
            #     ["int_l0_complaints_complaints_acc_qmt_csi",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_store_visit"]
            # ),
            # node(
            #     l1_complaints_survey_after_store_visit,
            #     ["int_l0_complaints_complaints_acc_qmt_csi",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_store_visit"
            #      ],
            #     "l1_complaints_survey_after_store_visit"
            # ),

            # node(
            #     dac_for_complaints_to_l1_pipeline,
            #     ["l0_complaints_myais_es_log_survey_daily",
            #      "l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_myais",
            #      "params:l1_complaints_survey_after_myais_tbl",
            #      "params:exception_partition_list_for_l0_complaints_myais_es_log_survey_daily"],
            #     ["int_l0_complaints_myais_es_log_survey_daily",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_myais"]
            # ),
            # node(
            #     l1_complaints_survey_after_myais,
            #     ["int_l0_complaints_myais_es_log_survey_daily",
            #      "params:l1_complaints_survey_after_myais",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_complaints_survey_after_myais"
            #      ],
            #     "l1_complaints_survey_after_myais"
            # ),

        ]
    )


def complaints_to_l1_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                dac_for_complaints_to_l1_pipeline,
                ["l0_usage_call_relation_sum_daily_for_l1_complaints_call_to_competitor_features",
                 "l1_customer_profile_union_daily_feature_for_l1_complaints_call_to_competitor_features",
                 "params:l1_complaints_call_to_competitor_features_tbl",
                 "params:exception_partition_list_for_l0_usage_call_relation_sum_daily"],
                ["int_l0_usage_call_relation_sum_daily_for_l1_complaints_call_to_competitor_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_call_to_competitor_features"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_usage_call_relation_sum_daily_for_l1_complaints_call_to_competitor_features",
                 "params:l1_complaints_call_to_competitor_features",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_call_to_competitor_features"],
                "l1_complaints_call_to_competitor_features"
            ),

            node(
                dac_for_complaints_to_l1_pipeline,
                ["l0_complaints_acc_atsr_outbound_daily",
                 "l1_customer_profile_union_daily_feature_for_l1_complaints_nps_after_call",
                 "params:l1_complaints_nps_after_call_tbl",
                 "params:exception_partition_list_for_l0_complaints_acc_atsr_outbound_daily"],
                ["int_l0_complaints_acc_atsr_outbound_daily",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_nps_after_call"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_complaints_acc_atsr_outbound_daily",
                 "params:l1_complaints_nps_after_call",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_nps_after_call"],
                "l1_complaints_nps_after_call"
            ),

            node(
                dac_for_complaints_to_l1_pipeline,
                ["l0_complaints_acc_qmt_csi_daily",
                 "l1_customer_profile_union_daily_feature_for_l1_complaints_nps_after_store_visit",
                 "params:l1_complaints_nps_after_store_visit_tbl",
                 "params:exception_partition_list_for_l0_complaints_acc_qmt_csi_daily"],
                ["int_l0_complaints_acc_qmt_csi_daily",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_nps_after_store_visit"]
            ),
            node(
                l1_massive_processing,
                ["int_l0_complaints_acc_qmt_csi_daily",
                 "params:l1_complaints_nps_after_store_visit",
                 "int_l1_customer_profile_union_daily_feature_for_l1_complaints_nps_after_store_visit"],
                "l1_complaints_nps_after_store_visit"
            ),
            node(
                dac_for_complaints_to_l1_pipeline,
                [
                    "l0_streamig_ida_mobile_domain_summary_daily_for_customer_satisfaction",
                    "l1_customer_profile_union_daily_feature_for_l1_complaints_traffic_to_dtac_web_resources",
                    "params:l1_complaints_traffic_to_dtac_web_resources_tbl",
                    "params:exception_partition_list_for_l0_streamig_ida_mobile_domain_summary_daily_for_customer_satisfaction",
                ],
                [
                    "int_l0_streamig_ida_mobile_domain_summary_daily_for_customer_satisfaction",
                    "int_l1_customer_profile_union_daily_feature_for_l1_complaints_traffic_to_dtac_web_resources"
                ]
            ),
            node(
                l1_massive_processing,
                [
                    "int_l0_streamig_ida_mobile_domain_summary_daily_for_customer_satisfaction",
                    "params:l1_complaints_traffic_to_dtac_web_resources",
                    "int_l1_customer_profile_union_daily_feature_for_l1_complaints_traffic_to_dtac_web_resources"
                ],
                "l1_complaints_traffic_to_dtac_web_resources"
            ),

        ]
    )
