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
from customer360.pipelines.cvm.dev.nodes import (
    create_dev_version,
    create_dev_version_users_only,
)
from customer360.pipelines.cvm.data_prep.nodes import (
    create_l5_cvm_one_day_users_table,
    add_ard_targets,
    add_churn_targets,
    create_l5_cvm_features_one_day_joined,
    create_l5_cvm_one_day_train_test,
    subs_date_join,
)


def create_cvm_inputs_dev(**kwargs):
    return Pipeline(
        [
            node(
                create_dev_version,
                ["l3_customer_profile_include_1mo_non_active", "parameters"],
                "l3_customer_profile_include_1mo_non_active_dev",
                name="create_l3_customer_profile_include_1mo_non_active_dev",
            ),
            node(
                create_dev_version_users_only,
                [
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "parameters",
                ],
                "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_dev",
                name="create_l4_revenue_prepaid_ru_f_sum_revenue"
                + "_by_service_monthly_dev",
            ),
            node(
                create_dev_version_users_only,
                ["l4_usage_prepaid_postpaid_daily_features", "parameters"],
                "l4_usage_prepaid_postpaid_daily_features_dev",
                name="create_l4_usage_prepaid_postpaid_daily_features_dev",
            ),
            node(
                create_dev_version_users_only,
                ["l4_daily_feature_topup_and_volume", "parameters"],
                "l4_daily_feature_topup_and_volume_dev",
                name="create_l4_daily_feature_topup_and_volume_dev",
            ),
        ]
    )


def create_cvm_prepare_data_dev(**kwargs):
    return Pipeline(
        [
            node(
                create_l5_cvm_one_day_users_table,
                [
                    "l3_customer_profile_include_1mo_non_active_dev",
                    "l0_product_product_pru_m_package_master_group",
                    "parameters",
                ],
                "l5_cvm_one_day_users_table_dev",
                name="create_l5_cvm_one_day_users_table_dev",
            ),
            node(
                add_ard_targets,
                [
                    "l5_cvm_one_day_users_table_dev",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_dev",
                    "parameters",
                    "params:chosen_date",
                ],
                "l5_cvm_ard_one_day_targets_dev",
                name="create_l5_cvm_ard_one_day_targets_dev",
            ),
            node(
                add_churn_targets,
                [
                    "l5_cvm_one_day_users_table_dev",
                    "l4_usage_prepaid_postpaid_daily_features_dev",
                    "parameters",
                ],
                "l5_cvm_churn_one_day_targets_dev",
                name="create_l5_cvm_churn_one_day_targets_dev",
            ),
            node(
                create_l5_cvm_one_day_train_test,
                ["l5_cvm_features_targets_one_day_dev", "parameters"],
                ["l5_cvm_one_day_train_dev", "l5_cvm_one_day_test_dev"],
                name="create_l5_cvm_one_day_train_test_dev",
            ),
            node(
                create_l5_cvm_features_one_day_joined,
                [
                    "l5_cvm_one_day_users_table_dev",
                    "l3_customer_profile_include_1mo_non_active_dev",
                    "l4_daily_feature_topup_and_volume_dev",
                    "l4_usage_prepaid_postpaid_daily_features_dev",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_dev",
                ],
                "l5_cvm_features_one_day_joined_dev",
                name="create_l5_cvm_features_one_day_joined_dev",
            ),
            node(
                subs_date_join,
                [
                    "l5_cvm_features_one_day_joined_dev",
                    "l5_cvm_churn_one_day_targets_dev",
                    "l5_cvm_ard_one_day_targets_dev",
                ],
                "l5_cvm_features_targets_one_day_dev",
                name="create_l5_cvm_features_targets_one_day_dev",
            ),
        ]
    )
