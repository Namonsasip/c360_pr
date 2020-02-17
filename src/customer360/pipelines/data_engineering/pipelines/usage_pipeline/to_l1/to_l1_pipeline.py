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
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1 import \
    merge_incoming_outgoing_calls_with_customer_dim, \
    merge_prepaid_postpaid_data_usage, merge_roaming_incoming_outgoing_calls, build_data_for_prepaid_postpaid_vas, \
    merge_with_customer_df, merge_all_dataset_to_one_table


def usage_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_usage_call_relation_sum_daily",
                 "params:l1_usage_outgoing_call_relation_sum_daily"],
                "l1_usage_outgoing_call_relation_sum_daily"
            ),
            node(
                node_from_config,
                ["l0_usage_call_relation_sum_daily",
                 "params:l1_usage_incoming_call_relation_sum_daily"],
                "l1_usage_incoming_call_relation_sum_daily"
            ),
            # node(
            #     merge_incoming_outgoing_calls_with_customer_dim, ['l1_usage_outgoing_call_relation_sum_daily_stg',
            #                                                       'l1_usage_incoming_call_relation_sum_daily_stg',
            #                                                       'l3_customer_profile_include_1mo_non_active'],
            #     'l1_usage_call_relation_sum_daily'
            # ),
            node(
                node_from_config,
                ["l0_usage_call_relation_sum_ir_daily",
                 "params:l1_usage_outgoing_call_relation_sum_ir_daily"],
                "l1_usage_outgoing_call_relation_sum_ir_daily"
            ),
            node(
                node_from_config,
                ["l0_usage_call_relation_sum_ir_daily",
                 "params:l1_usage_incoming_call_relation_sum_ir_daily"],
                "l1_usage_incoming_call_relation_sum_ir_daily"
            ),

            # node(
            #     merge_roaming_incoming_outgoing_calls, ['l1_usage_outgoing_call_relation_sum_ir_daily_stg',
            #                                             'l1_usage_incoming_call_relation_sum_ir_daily_stg',
            #                                             'l3_customer_profile_include_1mo_non_active'],
            #     'l1_usage_call_relation_sum_ir_daily'
            # ),
            node(
                node_from_config,
                ["l0_usage_ru_a_gprs_cbs_usage_daily",
                 "params:l1_usage_ru_a_gprs_cbs_usage_daily"],
                "l1_usage_ru_a_gprs_cbs_usage_daily_stg"
            ),
            node(
                node_from_config,
                ["l0_usage_ru_a_vas_postpaid_usg_daily",
                 "params:l1_usage_ru_a_vas_postpaid_usg_daily"],
                "l1_usage_ru_a_vas_postpaid_usg_daily_stg"
            ),
            # node(
            #     merge_prepaid_postpaid_data_usage, ['l1_usage_ru_a_gprs_cbs_usage_daily_stg',
            #                                         'l1_usage_ru_a_vas_postpaid_usg_daily_stg',
            #                                         'l3_customer_profile_include_1mo_non_active'],
            #     'l1_usage_data_prepaid_postpaid_daily'
            # ),
            node(
                build_data_for_prepaid_postpaid_vas, ['l0_usage_pps_v_ru_a_vas_nonvoice_daily',
                                                      'l0_usage_ru_a_vas_postpaid_usg_daily'],
                'vas_postpaid_prepaid_merged_stg'
            ),
            node(
                node_from_config, ['vas_postpaid_prepaid_merged_stg',
                                   "params:l1_usage_ru_a_vas_postpaid_prepaid_daily"],
                'l1_usage_ru_a_vas_postpaid_prepaid_daily'
            ),
            # node(
            #     merge_with_customer_df, ['l1_usage_ru_a_vas_postpaid_prepaid_daily_stg',
            #                              "l3_customer_profile_include_1mo_non_active"],
            #     'l1_usage_ru_a_vas_postpaid_prepaid_daily'
            # )
            node(merge_all_dataset_to_one_table, [
                'l1_usage_outgoing_call_relation_sum_daily', 'l1_usage_incoming_call_relation_sum_daily',
                'l1_usage_outgoing_call_relation_sum_ir_daily', 'l1_usage_incoming_call_relation_sum_ir_daily',
                'l1_usage_ru_a_gprs_cbs_usage_daily_stg', 'l1_usage_ru_a_vas_postpaid_usg_daily_stg',
                'l1_usage_ru_a_vas_postpaid_prepaid_daily', 'l1_customer_profile_union_daily_feature'
            ],
                 'l1_usage_postpaid_prepaid_daily'
            )
        ], name="usage_to_l1_pipeline"
    )
