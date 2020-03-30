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

from cvm.data_prep.nodes import (
    create_sample_dataset,
    create_l5_cvm_one_day_users_table,
    add_ard_targets,
    add_churn_targets,
    subs_date_join,
    add_macrosegments,
    create_l5_cvm_one_day_train_test,
    add_volatility_scores,
)
from cvm.src.utils.get_suffix import get_suffix, is_scoring


def create_cvm_prepare_inputs_samples(sample_type: str) -> Pipeline:
    """ Creates samples for input datasets.

    Args:
        sample_type: "dev" for dev samples and "sample" for sample.
    """

    suffix = get_suffix(sample_type)
    if is_scoring(sample_type):
        sampling_params = "params:scoring_sampling"
        date_params = "params:scoring_date"
    else:
        sampling_params = "params:training_sampling"
        date_params = "params:chosen_date"

    return Pipeline(
        [
            node(
                create_sample_dataset,
                [
                    "l3_device_most_used_monthly",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l3_device_most_used_monthly" + suffix,
                name="create_l3_device_most_used_monthly" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l3_touchpoints_to_call_center_features",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l3_touchpoints_to_call_center_features" + suffix,
                name="create_l3_touchpoints_to_call_center_features" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l3_streaming_fav_video_service_by_visit_count_feature",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l3_streaming_fav_video_service_by_visit_count_feature" + suffix,
                name="create_l3_streaming_fav_video_service_by_visit_count_feature"
                + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l3_customer_profile_include_1mo_non_active",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l3_customer_profile_include_1mo_non_active" + suffix,
                name="create_l3_customer_profile_include_1mo_non_active" + suffix,
            ),
            node(
                create_l5_cvm_one_day_users_table,
                [
                    "l3_customer_profile_include_1mo_non_active" + suffix,
                    "l0_product_product_pru_m_package_master_group",
                    date_params,
                ],
                "l5_cvm_one_day_users_full_table" + suffix,
                name="create_l5_cvm_one_day_users_full_table" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l5_cvm_one_day_users_full_table" + suffix,
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l5_cvm_one_day_users_table" + suffix,
                name="create_l5_cvm_one_day_users_table" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly" + suffix,
                name="create_l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"
                + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l4_usage_prepaid_postpaid_daily_features",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l4_usage_prepaid_postpaid_daily_features" + suffix,
                name="create_l4_usage_prepaid_postpaid_daily_features" + suffix,
            ),
            node(
                create_sample_dataset,
                [
                    "l4_daily_feature_topup_and_volume",
                    sampling_params,
                    "params:subscription_id_suffix" + suffix,
                    date_params,
                ],
                "l4_daily_feature_topup_and_volume" + suffix,
                name="create_l4_daily_feature_topup_and_volume" + suffix,
            ),
        ]
    )


def create_cvm_targets(sample_type: str = None):
    """ Creates pipeline preparing targets. Can create data pipeline for full dataset or
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
                add_ard_targets,
                [
                    "l5_cvm_one_day_users_table" + suffix,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
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
                    "l4_usage_prepaid_postpaid_daily_features",
                    "parameters",
                ],
                "l5_cvm_churn_one_day_targets" + suffix,
                name="create_l5_cvm_churn_one_day_targets" + suffix,
            ),
        ]
    )


def create_cvm_training_data(sample_type: str = None):
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
                subs_date_join,
                [
                    "parameters",
                    "l5_cvm_one_day_users_table" + suffix,
                    "l3_customer_profile_include_1mo_non_active" + suffix,
                    "l4_daily_feature_topup_and_volume" + suffix,
                    "l4_usage_prepaid_postpaid_daily_features" + suffix,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly" + suffix,
                    "l5_cvm_churn_one_day_targets" + suffix,
                    "l5_cvm_ard_one_day_targets" + suffix,
                ],
                "l5_cvm_features_targets_one_day" + suffix,
                name="create_l5_cvm_features_targets_one_day" + suffix,
            ),
            node(
                add_macrosegments,
                "l5_cvm_features_targets_one_day" + suffix,
                "l5_cvm_selected_features_one_day_joined_macrosegments" + suffix,
                name="create_l5_cvm_selected_features_one_day_joined_macrosegments"
                + suffix,
            ),
            node(
                create_l5_cvm_one_day_train_test,
                [
                    "l5_cvm_selected_features_one_day_joined_macrosegments" + suffix,
                    "parameters",
                ],
                ["l5_cvm_one_day_train" + suffix, "l5_cvm_one_day_test" + suffix],
                name="create_l5_cvm_one_day_train_test" + suffix,
            ),
        ]
    )


def create_cvm_scoring_data(sample_type: str = None):
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
                subs_date_join,
                [
                    "parameters",
                    "l5_cvm_one_day_users_table" + suffix,
                    "l3_customer_profile_include_1mo_non_active" + suffix,
                    "l4_daily_feature_topup_and_volume" + suffix,
                    "l4_usage_prepaid_postpaid_daily_features" + suffix,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly" + suffix,
                ],
                "l5_cvm_features" + suffix,
                name="create_l5_cvm_features" + suffix,
            ),
            node(
                add_macrosegments,
                "l5_cvm_features" + suffix,
                "l5_cvm_selected_features_one_day_joined_macrosegments" + suffix,
                name="create_l5_cvm_selected_features_one_day_joined_macrosegments"
                + suffix,
            ),
            node(
                add_volatility_scores,
                [
                    "l5_cvm_selected_features_one_day_joined_macrosegments" + suffix,
                    "l3_customer_profile_include_1mo_non_active",
                    "parameters",
                ],
                "l5_cvm_volatility" + suffix,
                name="create_l5_cvm_volatility" + suffix,
            ),
        ]
    )
