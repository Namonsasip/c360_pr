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
    create_users_from_active_users,
    add_ard_targets,
    add_churn_targets,
    subs_date_join,
    add_macrosegments,
    train_test_split,
    add_volatility_scores,
    create_users_from_cgtg,
    subs_date_join_important_only,
)
from cvm.src.utils.get_suffix import get_suffix


def create_users_from_tg(sample_type: str) -> Pipeline:
    """ Creates users table to use during training / scoring using predefined target
    group.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.
    """

    return Pipeline(
        [
            node(
                create_users_from_cgtg,
                ["cvm_prepaid_customer_groups"],
                "cvm_users_list_" + sample_type,
                name="create_users_list_tgcg_" + sample_type,
            ),
        ]
    )


def create_users_from_active(sample_type: str) -> Pipeline:
    """ Creates users table to use during training / scoring using list of active users.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.
    """

    return Pipeline(
        [
            node(
                create_sample_dataset,
                [
                    "l3_customer_profile_include_1mo_non_active",
                    "params:" + sample_type,
                ],
                "l3_customer_profile_include_1mo_non_active_" + sample_type,
                name="create_l3_customer_profile_include_1mo_non_active_" + sample_type,
            ),
            node(
                create_users_from_active_users,
                [
                    "l3_customer_profile_include_1mo_non_active_" + sample_type,
                    "l0_product_product_pru_m_package_master_group",
                    "params:" + sample_type,
                ],
                "cvm_users_list_" + sample_type,
                name="create_cvm_users_list_active_users_" + sample_type,
            ),
        ]
    )


def sample_inputs(sample_type: str) -> Pipeline:
    """ Creates samples for input datasets.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.
    """

    datasets_to_sample = [
        "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        "l4_usage_prepaid_postpaid_daily_features",
        "l4_daily_feature_topup_and_volume",
    ]

    nodes_list = [
        node(
            create_sample_dataset,
            [dataset_name, "params:" + sample_type],
            dataset_name + "_" + sample_type,
            name="sample_" + dataset_name + "_ " + sample_type,
        )
        for dataset_name in datasets_to_sample
    ]

    return Pipeline(nodes_list)


def create_cvm_targets(sample_type: str):
    """ Creates pipeline preparing targets. Can create data pipeline for full dataset or
    given sample_type.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """

    return Pipeline(
        [
            node(
                add_ard_targets,
                [
                    "cvm_users_list_" + sample_type,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "parameters",
                    "params:" + sample_type,
                ],
                "ard_targets_" + sample_type,
                name="create_ard_targets_" + sample_type,
            ),
            node(
                add_churn_targets,
                [
                    "cvm_users_list_" + sample_type,
                    "l4_usage_prepaid_postpaid_daily_features",
                    "parameters",
                    "params:" + sample_type,
                ],
                "churn_targets_" + sample_type,
                name="create_churn_targets_" + sample_type,
            ),
        ]
    )


def prepare_features_macrosegments(sample_type: str):
    """ Creates pipeline preparing data with all features and macrosegments.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """
    return Pipeline(
        [
            node(
                subs_date_join_important_only,
                [
                    "important_columns",
                    "parameters",
                    "cvm_users_list_" + sample_type,
                    "l3_customer_profile_include_1mo_non_active_" + sample_type,
                    "l4_daily_feature_topup_and_volume_" + sample_type,
                    "l4_usage_prepaid_postpaid_daily_features_" + sample_type,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_"
                    + sample_type,
                    "churn_targets_" + sample_type,
                    "ard_targets_" + sample_type,
                ],
                "features_targets_" + sample_type,
                name="create_features_targets_" + sample_type,
            ),
            node(
                add_macrosegments,
                "features_targets_" + sample_type,
                "features_macrosegments_" + sample_type,
                name="create_features_macrosegments_" + sample_type,
            ),
        ]
    )


def create_cvm_training_data(sample_type: str):
    """ Creates pipeline preparing data. Can create data pipeline for full dataset or
    given sample_type.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """

    return prepare_features_macrosegments(sample_type) + Pipeline(
        [
            node(
                train_test_split,
                ["features_macrosegments_" + sample_type, "parameters"],
                ["train_sample_" + sample_type, "test_sample" + sample_type],
                name="create_train_test_split_" + sample_type,
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
