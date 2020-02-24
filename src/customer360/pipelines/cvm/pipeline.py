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

from .nodes import \
    create_l5_cvm_one_day_users_table, \
    create_l5_cvm_users_sample_table, \
    add_ard_targets, \
    add_churn_targets, \
    create_l5_cvm_features_one_day_joined, \
    create_l5_cvm_one_day_train_test, \
    create_l5_cvm_features_targets_one_day


def create_cvm_prepare_data_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                create_l5_cvm_one_day_users_table,
                ["l3_customer_profile_include_1mo_non_active",
                 "l0_product_product_pru_m_package_master_group",
                 "parameters"],
                "l5_cvm_one_day_users_table",
                name="create_l5_cvm_one_day_users_table"
            ),
            node(
                add_ard_targets,
                ["l5_cvm_one_day_users_table",
                 "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                 "parameters"],
                "l5_cvm_ard_one_day_targets",
                name="create_l5_cvm_ard_one_day_targets"
            ),
            node(
                add_churn_targets,
                ["l5_cvm_one_day_users_table",
                 "l4_usage_prepaid_postpaid_daily_features",
                 "parameters"],
                "l5_cvm_churn_one_day_targets",
                name="create_l5_cvm_churn_one_day_targets"
            ),
            node(
                create_l5_cvm_one_day_train_test,
                ["l5_cvm_features_targets_one_day",
                 "parameters"],
                "l5_cvm_one_day_train_test",
                name="create_l5_cvm_one_day_train_test"
            ),
            node(
                create_l5_cvm_features_one_day_joined,
                ["l5_cvm_one_day_users_table",
                 "l3_customer_profile_include_1mo_non_active",
                 "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"],
                "l5_cvm_features_one_day_joined",
                name="create_l5_cvm_features_one_day_joined"
            ),
            node(
                create_l5_cvm_features_targets_one_day,
                ["l5_cvm_features_one_day_joined",
                 "l5_cvm_churn_one_day_targets",
                 "l5_cvm_ard_one_day_targets"],
                "l5_cvm_features_targets_one_day",
                name="create_l5_cvm_features_targets_one_day"
            ),
        ]
    )


def create_cvm_prepare_sample_data_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                create_l5_cvm_users_sample_table,
                "l5_cvm_one_day_users_table",
                "l5_cvm_one_day_users_sample_table",
                name="create_l5_cvm_users_sample_table"
            ),
            node(
                add_ard_targets,
                ["l5_cvm_one_day_users_sample_table",
                 "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                 "parameters"],
                "l5_cvm_ard_one_day_targets_sample",
                name="create_l5_cvm_ard_one_day_targets_sample"
            ),
            node(
                add_churn_targets,
                ["l5_cvm_one_day_users_sample_table",
                 "l4_usage_prepaid_postpaid_daily_features",
                 "parameters"],
                "l5_cvm_churn_one_day_targets",
                name="create_l5_cvm_churn_one_day_targets_sample"
            ),
            node(
                create_l5_cvm_one_day_train_test,
                ["l5_cvm_features_targets_one_day_sample",
                 "parameters"],
                "l5_cvm_one_day_train_test_sample",
                name="create_l5_cvm_one_day_train_test_sample"
            ),
            node(
                create_l5_cvm_features_one_day_joined,
                ["l5_cvm_one_day_users_sample_table",
                 "l3_customer_profile_include_1mo_non_active",
                 "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"],
                "l5_cvm_features_one_day_joined_sample",
                name="create_l5_cvm_features_one_day_joined_sample"
            ),
            node(
                create_l5_cvm_features_targets_one_day,
                ["l5_cvm_features_one_day_joined_sample",
                 "l5_cvm_churn_one_day_targets",
                 "l5_cvm_ard_one_day_targets_sample"],
                "l5_cvm_features_targets_one_day_sample",
                name="create_l5_cvm_features_targets_one_day_sample"
            ),
        ]
    )
