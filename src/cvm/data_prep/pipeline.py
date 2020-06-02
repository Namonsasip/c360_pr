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
import re

from kedro.pipeline import Pipeline, node

from cvm.data_prep.nodes import (
    add_ard_targets,
    add_churn_targets,
    add_macrosegments,
    create_sample_dataset,
    create_users_from_active_users,
    create_users_from_cgtg,
    feature_selection_all_target,
    subs_date_join,
    subs_date_join_important_only,
    train_test_split,
)
from cvm.preprocessing.nodes import pipeline_fit
from cvm.src.temporary_fixes.sub_id_replace import get_mapped_dataset_name
from cvm.temp.nodes import map_sub_ids
from cvm.temp.pipeline import map_sub_ids_of_input_datasets


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
                [
                    "cvm_prepaid_customer_groups",
                    "params:{}".format(sample_type),
                    "parameters",
                ],
                "cvm_users_list_" + sample_type,
                name="create_users_list_tgcg_" + sample_type,
            ),
            node(
                func=map_sub_ids,
                inputs=[
                    "create_cvm_users_list_active_users_" + sample_type,
                    "sub_id_mapping",
                ],
                outputs=get_mapped_dataset_name(
                    "create_cvm_users_list_active_users_" + sample_type, sample_type
                ),
                name="map_sub_ids_for_{}".format(
                    "create_cvm_users_list_active_users_" + sample_type
                ),
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
                    "l3_customer_profile_include_1mo_non_active_sub_ids_mapped",
                    "parameters",
                    "params:" + sample_type,
                ],
                "active_users_sample_" + sample_type,
                name="create_active_users_sample_" + sample_type,
            ),
            node(
                create_users_from_active_users,
                [
                    "active_users_sample_" + sample_type,
                    "l0_product_pru_m_package_master_group_for_daily",
                    "params:" + sample_type,
                    "parameters",
                ],
                "cvm_users_list_" + sample_type,
                name="create_cvm_users_list_active_users_" + sample_type,
            ),
            node(
                func=map_sub_ids,
                inputs=[
                    "create_cvm_users_list_active_users_" + sample_type,
                    "sub_id_mapping",
                ],
                outputs=get_mapped_dataset_name(
                    "create_cvm_users_list_active_users_" + sample_type, sample_type
                ),
                name="map_sub_ids_for_{}".format(
                    "create_cvm_users_list_active_users_" + sample_type
                ),
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
        "l3_customer_profile_include_1mo_non_active",
        "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        "l4_usage_prepaid_postpaid_daily_features",
        "l4_daily_feature_topup_and_volume",
        "l4_usage_postpaid_prepaid_weekly_features_sum",
        "l4_touchpokints_to_call_center_features",
    ]

    nodes_list = [
        node(
            create_sample_dataset,
            [dataset_name, "parameters", "params:" + sample_type],
            re.sub("_no_inc", "", dataset_name) + "_" + sample_type,
            name="sample_" + dataset_name + "_" + sample_type,
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

    if sample_type == "training":
        targets_datasets = [
            "churn_targets_" + sample_type,
            "ard_targets_" + sample_type,
        ]
    else:
        targets_datasets = []
    return Pipeline(
        [
            node(
                subs_date_join_important_only,
                [
                    "important_columns",
                    "parameters",
                    "cvm_users_list_sub_ids_mapped_" + sample_type,
                    "l3_customer_profile_include_1mo_non_active_sub_ids_mapped_"
                    + sample_type,
                    "l4_daily_feature_topup_and_volume_sub_ids_mapped_" + sample_type,
                    "l4_usage_prepaid_postpaid_daily_features_sub_ids_mapped_"
                    + sample_type,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_"
                    + "sub_ids_mapped_"
                    + sample_type,
                    "l4_usage_postpaid_prepaid_weekly_features_sum_sub_ids_mapped_"
                    + sample_type,
                    "l4_touchpoints_to_call_center_features_sub_ids_mapped_"
                    + sample_type,
                ]
                + targets_datasets,
                "features_targets_" + sample_type,
                name="create_features_targets_" + sample_type,
            ),
            node(
                add_macrosegments,
                ["features_targets_" + sample_type, "parameters"],
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
                ["train_sample_" + sample_type, "test_sample_" + sample_type],
                name="create_train_test_split_" + sample_type,
            ),
        ]
    )


def create_cvm_important_columns():
    """ Uses training samples as inputs, runs feature extraction procedure.

    Returns:
        Kedro pipeline.
    """

    sample_type = "fe"
    targets_datasets = [
        "churn_targets_" + sample_type,
        "ard_targets_" + sample_type,
    ]

    return Pipeline(
        {
            node(
                subs_date_join,
                [
                    "parameters",
                    "cvm_users_list_" + sample_type,
                    "l3_customer_profile_include_1mo_non_active_" + sample_type,
                    "l4_daily_feature_topup_and_volume_" + sample_type,
                    "l4_usage_prepaid_postpaid_daily_features_" + sample_type,
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_"
                    + sample_type,
                    "l4_usage_postpaid_prepaid_weekly_features_sum_" + sample_type,
                    "l4_touchpoints_to_call_center_features_" + sample_type,
                ]
                + targets_datasets,
                "features_targets_fe",
                name="create_features_targets_fe",
            ),
            node(
                add_macrosegments,
                ["features_targets_fe", "parameters"],
                "features_macrosegments_fe",
                name="create_features_macrosegments_fe",
            ),
            node(
                lambda df, parameters: pipeline_fit(df, [], parameters),
                ["features_macrosegments_fe", "parameters"],
                [
                    "sample_preprocessed_fe",
                    "preprocessing_pipeline_fe",
                    "null_columns_fe",
                ],
                name="preprocessing_fit_fe",
            ),
            node(
                feature_selection_all_target,
                ["sample_preprocessed_fe", "parameters"],
                "important_columns",
                name="feature_selection",
            ),
        }
    )


training_data_prepare = (
    create_users_from_active("training")
    + sample_inputs("training")
    + create_cvm_targets("training")
    + prepare_features_macrosegments("training")
    + create_cvm_training_data("training")
)


def scoring_data_prepare(sample_type: str) -> Pipeline:
    """ Create pipeline generating input data for scoring

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """
    return (
        sample_inputs(sample_type)
        + map_sub_ids_of_input_datasets(sample_type)
        + create_users_from_active(sample_type)
        + prepare_features_macrosegments(sample_type)
    )


extract_features = (
    create_users_from_active("fe")
    + sample_inputs("fe")
    + create_cvm_targets("fe")
    + create_cvm_important_columns()
)
rfe_only = create_cvm_important_columns()


def create_cvm_targets_huaw_exp():
    """ Create pipeline generating 90 days churn column used for experiment.

    Returns:
        Kedro pipeline.
    """

    return Pipeline(
        [
            node(
                add_churn_targets,
                [
                    "cvm_users_list_huaw_experiment",
                    "l4_usage_prepaid_postpaid_daily_features_sub_ids_mapped",
                    "parameters",
                    "params:huaw_experiment",
                ],
                "churn_targets_huaw_experiment",
                name="create_churn_targets_huaw_experiment",
            ),
        ]
    )
