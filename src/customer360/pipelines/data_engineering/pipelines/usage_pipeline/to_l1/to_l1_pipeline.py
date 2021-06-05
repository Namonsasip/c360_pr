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
"""Example code for the nodes in the example pipeline. This code is meant
just for illustrating basic Kedro features.

PLEASE DELETE THIS FILE ONCE YOU START WORKING ON YOUR OWN PROJECT!
"""

from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import node_from_config
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1 import build_data_for_prepaid_postpaid_vas, \
    usage_outgoing_call_pipeline, merge_all_dataset_to_one_table, usage_data_prepaid_pipeline, \
    usage_incoming_call_pipeline, usage_outgoing_ir_call_pipeline, usage_incoming_ir_call_pipeline, \
    usage_data_postpaid_pipeline, usage_data_postpaid_roaming

from src.customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import \
    usage_favourite_number_master_pipeline, l1_usage_last_idd_features_join_profile


def usage_to_l1_pipeline_last_idd_features(**kwargs):
    return Pipeline(
        [
            node(
                l1_usage_last_idd_features_join_profile,
                [
                    "l0_usage_call_relation_sum_daily",
                    "l1_customer_profile_union_daily_feature_for_l1_usage_last_idd_features",
                    "params:l1_usage_last_idd_features"
                ],  "l1_usage_last_idd_features"
            ),
        ]
    )


def usage_create_master_data_for_favourite_feature(**kwargs):
    return Pipeline(
        [
            node(
                usage_favourite_number_master_pipeline,
                [
                    "l0_usage_call_relation_sum_daily_outgoing_for_favorites_features_master_table",
                    "params:l1_usage_favourite_number_master"
                ],  "l1_usage_favourite_number_master"
            ),
        ]
    )


def usage_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                usage_outgoing_ir_call_pipeline,
                ["l0_usage_call_relation_sum_ir_daily_outgoing",
                 "l1_usage_favourite_number_master",
                 "params:l1_usage_outgoing_call_relation_sum_ir_daily"],
                "l1_usage_outgoing_call_relation_sum_ir_daily"
            ),
            node(
                usage_incoming_ir_call_pipeline,
                ["l0_usage_call_relation_sum_ir_daily_incoming",
                 "l1_usage_favourite_number_master",
                 "params:l1_usage_incoming_call_relation_sum_ir_daily"],
                "l1_usage_incoming_call_relation_sum_ir_daily"
            ),

            node(
                usage_data_prepaid_pipeline,
                ["l0_usage_ru_a_gprs_cbs_usage_daily",
                 "params:l1_usage_ru_a_gprs_cbs_usage_daily",
                 "params:exception_partition_list_for_l0_usage_ru_a_gprs_cbs_usage_daily"],
                "l1_usage_ru_a_gprs_cbs_usage_daily"
            ),

            node(
                usage_data_postpaid_pipeline,
                ["l0_usage_ru_a_vas_postpaid_usg_daily",
                 "params:l1_usage_ru_a_vas_postpaid_usg_daily"],
                "l1_usage_ru_a_vas_postpaid_usg_daily"
            ),
            node(usage_data_postpaid_roaming,
                 ["l0_usage_ir_a_usg_daily_for_postpaid",
                  "params:l1_usage_data_postpaid_roaming"],
                 "l1_usage_data_postpaid_roaming"

                 ),
            node(
                build_data_for_prepaid_postpaid_vas,
                ['l0_usage_pps_v_ru_a_vas_nonvoice_daily',
                 'l0_usage_ru_a_vas_postpaid_usg_daily_prepaid_postpaid_merged'],
                'vas_postpaid_prepaid_merged_stg'
            ),

            node(
                node_from_config,
                ['vas_postpaid_prepaid_merged_stg',
                 "params:l1_usage_ru_a_vas_postpaid_prepaid_daily"],
                'l1_usage_ru_a_vas_postpaid_prepaid_daily'
            ),

            node(
                usage_outgoing_call_pipeline,
                ["l0_usage_call_relation_sum_daily_outgoing",
                 "l1_usage_favourite_number_master",
                 "params:l1_usage_outgoing_call_relation_sum_daily",
                 "params:exception_partition_list_for_l0_usage_call_relation_sum_daily_outgoing"
                 ],
                "l1_usage_outgoing_call_relation_sum_daily"
            ),
            node(
                usage_incoming_call_pipeline,
                ["l0_usage_call_relation_sum_daily_incoming",
                 "l1_usage_favourite_number_master",
                 "params:l1_usage_incoming_call_relation_sum_daily",
                 "params:exception_partition_list_for_l0_usage_call_relation_sum_daily_incoming"],
                "l1_usage_incoming_call_relation_sum_daily"
            ),
            node(merge_all_dataset_to_one_table, [
                'l1_usage_outgoing_call_relation_sum_daily', 'l1_usage_incoming_call_relation_sum_daily',
                'l1_usage_outgoing_call_relation_sum_ir_daily', 'l1_usage_incoming_call_relation_sum_ir_daily',
                'l1_usage_ru_a_gprs_cbs_usage_daily', 'l1_usage_ru_a_vas_postpaid_usg_daily',
                'l1_usage_ru_a_vas_postpaid_prepaid_daily', 'l1_usage_data_postpaid_roaming',
                'l1_customer_profile_union_daily_feature_for_usage',
                # "params:exception_partition_list_for_l0_usage_call_relation_sum_daily_outgoing",
                # "params:exception_partition_list_for_l0_usage_call_relation_sum_daily_incoming",
                # "params:exception_partition_list_for_l0_usage_ru_a_gprs_cbs_usage_daily"
            ],
                 'l1_usage_postpaid_prepaid_daily'
                 ),
        ], name="usage_to_l1_pipeline"
    )