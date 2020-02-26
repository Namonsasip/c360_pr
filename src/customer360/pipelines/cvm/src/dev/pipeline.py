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
from src.customer360.pipelines.cvm.src.dev.nodes import create_dev_version


def create_cvm_dev_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                create_dev_version,
                ["l3_customer_profile_include_1mo_non_active",
                 "parameters"],
                "l3_customer_profile_include_1mo_non_active_dev",
                name="create_l3_customer_profile_include_1mo_non_active_dev"
            ),
            node(
                create_dev_version,
                ["l0_product_product_pru_m_package_master_group",
                 "parameters"],
                "l0_product_product_pru_m_package_master_group_dev",
                name="create_l0_product_product_pru_m_package_master_group_dev"
            ),
            node(
                create_dev_version,
                ["l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                 "parameters"],
                "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_dev",
                name="create_l4_revenue_prepaid_ru_f_sum_revenue_by_service" + \
                     "_monthly_dev"
            ),
            node(
                create_dev_version,
                ["l4_usage_prepaid_postpaid_daily_features",
                 "parameters"],
                "l4_usage_prepaid_postpaid_daily_features_dev",
                name="create_l4_usage_prepaid_postpaid_daily_features_dev"
            ),
            node(
                create_dev_version,
                ["l5_cvm_one_day_users_table",
                 "parameters"],
                "l5_cvm_one_day_users_table_dev",
                name="create_l5_cvm_one_day_users_table_dev"
            ),
            node(
                create_dev_version,
                ["l5_cvm_ard_one_day_targets",
                 "parameters"],
                "l5_cvm_ard_one_day_targets_dev",
                name="create_l5_cvm_ard_one_day_targets_dev"
            ),
            node(
                create_dev_version,
                ["l5_cvm_churn_one_day_targets",
                 "parameters"],
                "l5_cvm_churn_one_day_targets_dev",
                name="create_l5_cvm_churn_one_day_targets_dev"
            ),
            node(
                create_dev_version,
                ["l5_cvm_features_one_day_joined",
                 "parameters"],
                "l5_cvm_features_one_day_joined_dev",
                name="create_l5_cvm_features_one_day_joined_dev"
            ),
            node(
                create_dev_version,
                ["l5_cvm_features_targets_one_day",
                 "parameters"],
                "l5_cvm_features_targets_one_day_dev",
                name="create_l5_cvm_features_targets_one_day_dev"
            ),
            #             node(
            #                 create_dev_version,
            #                 ["l5_cvm_one_day_train",
            #                  "parameters"],
            #                 "l5_cvm_one_day_train_dev",
            #                 name="create_l5_cvm_one_day_train_dev"
            #             ),
            #             node(
            #                 create_dev_version,
            #                 ["l5_cvm_one_day_test",
            #                  "parameters"],
            #                 "l5_cvm_one_day_test_dev",
            #                 name="create_l5_cvm_one_day_test_dev"
            #             ),
        ]
    )
