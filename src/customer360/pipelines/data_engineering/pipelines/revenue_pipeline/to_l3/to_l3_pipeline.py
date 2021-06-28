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

from customer360.pipelines.data_engineering.nodes.revenue_nodes.to_l3.to_l3_nodes import *
from customer360.utilities.config_parser import node_from_config


def revenue_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                revenue_postpaid_ru_f_sum,
                ["l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
                 "params:l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly"],
                "l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly"
            ),
            # node(merge_with_customer_postpaid_df,
            #      ['l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_stg',
            #       'l3_customer_profile_union_monthly_feature_for_l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'
            #       ],
            #      'l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'
            #      ),

            node(
                revenue_prepaid_ru_f_sum,
                ["l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                 "params:l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"],
                "l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"
            ),
            # node(merge_with_customer_prepaid_df, [
            #     'l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_stg',
            #     'l3_customer_profile_union_monthly_feature_for_l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'
            # ],
            #      'l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'
            #      ),

################################ feature add norm ################################ 2021-05-17
            node(
                node_from_config,
                ['l0_revenue_postpaid_ru_f_sum_revenue_by_service',
                 'params:l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly'],
                'l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_stg'
            ),
            node(
                l3_rename_sub_id_to_subscription_identifier,
                ['l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_stg',
                 'params:l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly'],
                'l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly'
            ),

            node(
                l3_merge_vat_with_revenue_prepaid_pru_f_revenue_allocate_usage,
                ['l0_revenue_prepaid_pru_f_revenue_allocate_usage',
                 'params:l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly'],
                'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_stg'
            ),
            node(
                node_from_config,
                ['l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_stg',
                 'params:l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly'],
                'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly'
            ),

            node(
                node_from_config,
                ['l0_revenue_postpaid_ru_f_sum_rev_mth_by_promo_class',
                 'params:l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly'],
                'l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_stg'
            ),
            node(
                l3_rename_sub_id_to_subscription_identifier,
                ['l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_stg',
                 'params:l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly'],
                'l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly'
            ),

            node(
                l3_merge_vat_with_revenue_prepaid_pru_f_active_sub_cross_mao_mao,
                ['l0_revenue_prepaid_pru_f_active_sub_cross_mao_mao',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_stg'
            ),
            node(
                node_from_config,
                ['l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_stg',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly'
            ),

            node(
                l3_merge_vat_with_revenue_pre_pru_f_active_mao_mao_m_pre_rev_allocate_usg,
                ['l0_revenue_prepaid_pru_f_active_sub_cross_mao_mao',
                 'l0_revenue_prepaid_pru_f_revenue_allocate_usage',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly_stg'
            ),
            node(
                node_from_config,
                ['l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly_stg',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly'
            ),
        ], name="revenue_to_l3_pipeline"
    )


def revenue_feature_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ['l0_revenue_postpaid_ru_f_sum_revenue_by_service_revenue',
                 'params:l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_revenue'],
                'l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_revenue_stg'
            ),
            node(
                l3_rename_sub_id_to_subscription_identifier,
                ['l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_revenue_stg',
                 'params:l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_revenue'],
                'l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_revenue_stg1'
            ),
            node(
                node_from_config,
                ['l0_revenue_prepaid_pru_f_revenue_allocate_usage_revenue',
                 'params:l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_revenue'],
                'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_revenue_stg'
             ),
            node(
                l3_rename_c360_subscription_identifier_to_subscription_identifier,
                ['l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_revenue_stg',
                 'params:l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_revenue'],
                'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_revenue_stg1'
            ),
            node(
                l3_merge_postpaid_ru_f_sum_revenue_by_service_with_prepaid_pru_f_revenue_allocate_usage,
                ['l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_revenue_stg1',
                 'l3_revenue_features_postpaid_ru_f_sum_revenue_by_service_monthly_revenue_stg1',
                 'params:l3_revenue_features_for_prepaid_postpaid_revenue'],
                'l3_revenue_features_for_prepaid_postpaid_revenue_stg'
            ),
            node(
                node_from_config,
                ['l3_revenue_features_for_prepaid_postpaid_revenue_stg',
                 'params:l3_revenue_features_for_prepaid_postpaid_revenue'],
                'l3_revenue_features_for_prepaid_postpaid_revenue'
            ),
            ###################### REVENUE_PACKAGE ############################
            node(
                node_from_config,
                ['l0_revenue_postpaid_ru_f_sum_rev_mth_by_promo_class_package',
                 'params:l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_package'],
                'l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_package_stg'
            ),
            node(
                l3_rename_sub_id_to_subscription_identifier,
                ['l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_package_stg',
                 'params:l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_package'],
                'l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_package_stg1'
            ),

            node(
                node_from_config,
                ['l0_revenue_prepaid_pru_f_revenue_allocate_usage_package',
                 'params:l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_package_main_ontop'],
                'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_package_main_ontop_stg'
            ),
            node(
                l3_rename_c360_subscription_identifier_to_subscription_identifier,
                ['l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_package_main_ontop_stg',
                 'params:l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_package_main_ontop'],
                'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_package_main_ontop_stg1'
            ),

            node(
                node_from_config,
                ['l0_revenue_prepaid_pru_f_active_sub_cross_mao_mao_package',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_usage_ontop'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_usage_ontop_stg'
            ),
            node(
                l3_rename_c360_subscription_identifier_to_subscription_identifier,
                ['l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_usage_ontop_stg',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_usage_ontop'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_usage_ontop_stg1'
            ),

            node(
                l3_merge_vat_with_revenue_pre_pru_f_active_mao_mao_m_pre_rev_allocate_usg,
                ['l0_revenue_prepaid_pru_f_active_sub_cross_mao_mao',
                 'l0_revenue_prepaid_pru_f_revenue_allocate_usage_package',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly_stg'
            ),
            node(
                node_from_config,
                ['l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly_stg',
                 'params:l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly'],
                'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly_stg1'
            ),

            node(
                l3_merge_postpaid_revenue_and_prepaid_revenue_pacakage,
                ['l3_revenue_features_postpaid_ru_f_sum_rev_mth_by_promo_class_monthly_package_stg1',
                 'l3_revenue_features_prepaid_pru_f_revenue_allocate_usage_monthly_package_main_ontop_stg1',
                 'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_monthly_usage_ontop_stg1',
                 'l3_revenue_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly_stg1',
                 'params:l3_revenue_features_for_postpaid_and_prepaid_revenue_package'],
                'l3_revenue_features_for_postpaid_and_prepaid_revenue_package_stg'
            ),
            node(
                node_from_config,
                ['l3_revenue_features_for_postpaid_and_prepaid_revenue_package_stg',
                 'params:l3_revenue_features_for_postpaid_and_prepaid_revenue_package'],
                'l3_revenue_features_for_postpaid_and_prepaid_revenue_package'
            ),
        ]
    )
