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

from customer360.pipelines.cvm.data_prep.nodes import (
    create_l5_cvm_one_day_users_table,
    add_ard_targets,
    add_churn_targets,
    create_l5_cvm_one_day_train_test,
    subs_date_join,
    create_sample_dataset,
)
from customer360.pipelines.cvm.src.utils.get_suffix import get_suffix


def create_cvm_prepare_inputs_samples(sample_type: str) -> Pipeline:
    """ Creates samples for input datasets.

    Args:
        sample_type: "dev" for dev samples and "sample" for sample.
    """

    suffix = get_suffix(sample_type)

    return Pipeline(
        [
            node(
                create_sample_dataset,
                [
                    "l3_customer_profile_include_1mo_non_active",
                    "params:subscription_id_suffix" + suffix,
                ],
                "l3_customer_profile_include_1mo_non_active" + suffix,
                name="create_l3_customer_profile_include_1mo_non_active" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "params:subscription_id_suffix" + suffix,
                ],
                "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly" + suffix,
                name="create_l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"
                + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l4_usage_prepaid_postpaid_daily_features",
                    "params:subscription_id_suffix" + suffix,
                ],
                "l4_usage_prepaid_postpaid_daily_features" + suffix,
                name="create_l4_usage_prepaid_postpaid_daily_features" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l4_daily_feature_topup_and_volume",
                    "params:subscription_id_suffix" + suffix,
                ],
                "l4_daily_feature_topup_and_volume" + suffix,
                name="create_l4_daily_feature_topup_and_volume" + suffix,
            ),
        ]
    )


def create_cvm_prepare_data(sample_type: str = None):
    """ Creates pipeline preparing data. Can create data pipeline for full dataset or
    given sample_type.

    Args:
        sample_type: sample type to use. Dev sample for "dev", Sample for "sample", full
        dataset for None (default).

    Returns:
        Kedro pipeline.
    """

    suffix = get_suffix(sample_type)

    return Pipeline(
        [
            node(
                create_l5_cvm_one_day_users_table,
                [
                    "l3_customer_profile_include_1mo_non_active" + suffix,
                    "l0_product_product_pru_m_package_master_group",
                    "parameters",
                ],
                "l5_cvm_one_day_users_table" + suffix,
                name="create_l5_cvm_one_day_users_table" + suffix,
            ),
            node(
                add_ard_targets,
                [
                    "l5_cvm_one_day_users_table" + suffix,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly" + suffix,
                    "parameters",
                    "params:chosen_date",
                ],
                "l5_cvm_ard_one_day_targets" + suffix,
                name="create_l5_cvm_ard_one_day_targets" + suffix,
            ),
            node(
                add_churn_targets,
                [
                    "l5_cvm_one_day_users_table" + suffix,
                    "l4_usage_prepaid_postpaid_daily_features" + suffix,
                    "parameters",
                ],
                "l5_cvm_churn_one_day_targets" + suffix,
                name="create_l5_cvm_churn_one_day_targets" + suffix,
            ),
            node(
                create_l5_cvm_one_day_train_test,
                ["l5_cvm_features_targets_one_day" + suffix, "parameters"],
                ["l5_cvm_one_day_train" + suffix, "l5_cvm_one_day_test" + suffix],
                name="create_l5_cvm_one_day_train_test" + suffix,
            ),
            node(
                subs_date_join,
                [
                    "l5_cvm_one_day_users_table" + suffix,
                    "l3_customer_profile_include_1mo_non_active" + suffix,
                    "l4_daily_feature_topup_and_volume" + suffix,
                    "l4_usage_prepaid_postpaid_daily_features" + suffix,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly" + suffix,
                ],
                "l5_cvm_features_one_day_joined" + suffix,
                name="create_l5_cvm_features_one_day_joined" + suffix,
            ),
            node(
                subs_date_join,
                [
                    "l5_cvm_features_one_day_joined" + suffix,
                    "l5_cvm_churn_one_day_targets" + suffix,
                    "l5_cvm_ard_one_day_targets" + suffix,
                ],
                "l5_cvm_features_targets_one_day" + suffix,
                name="create_l5_cvm_features_targets_one_day" + suffix,
            ),
        ]
    )
