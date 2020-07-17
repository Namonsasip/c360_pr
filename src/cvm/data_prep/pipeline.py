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

from cvm.data_prep.nodes import (
    add_ard_targets,
    add_churn_targets,
    create_pred_sample,
    feature_selection_all_target,
    get_macrosegments,
    get_micro_macrosegments,
    subs_date_join,
    train_test_split,
)
from cvm.preprocessing.nodes import pipeline_fit
from kedro.pipeline import Pipeline, node


def join_raw_features(sample_type: str) -> Pipeline:
    """ Join used tables to create master table with all C360 features.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list
         created for training.
    """
    inputs_to_join = [
        "cvm_users_list_{}",
        "l4_usage_prepaid_postpaid_daily_features_{}",
        "l3_customer_profile_include_1mo_non_active_{}",
        "l4_daily_feature_topup_and_volume_{}",
        "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_{}",
        "l4_usage_postpaid_prepaid_weekly_features_sum_{}",
        "l4_touchpoints_to_call_center_features_{}",
    ]
    inputs_to_join = [
        input_dataset.format(sample_type) for input_dataset in inputs_to_join
    ]
    return Pipeline(
        [
            node(
                func=subs_date_join,
                inputs=["parameters"] + inputs_to_join,
                outputs="raw_features_{}".format(sample_type),
                name="create_raw_features_{}".format(sample_type),
            )
        ]
    )


def create_training_sample(sample_type: str):
    """ Creates pipeline preparing training sample. Can create data pipeline for full
    dataset or given sample_type.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """

    return Pipeline(
        [
            node(
                func=add_ard_targets,
                inputs=[
                    "cvm_users_list_{}".format(sample_type),
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "parameters",
                    "params:{}".format(sample_type),
                ],
                outputs="ard_targets_{}".format(sample_type),
                name="create_ard_targets",
            ),
            node(
                func=add_churn_targets,
                inputs=[
                    "cvm_users_list_{}".format(sample_type),
                    "l4_usage_prepaid_postpaid_daily_features",
                    "parameters",
                    "params:{}".format(sample_type),
                ],
                outputs="churn_targets_{}".format(sample_type),
                name="create_churn_targets",
            ),
            node(
                func=get_macrosegments,
                inputs=[
                    "raw_features_{}".format(sample_type),
                    "l3_customer_profile_include_1mo_non_active_{}".format(sample_type),
                    "parameters",
                ],
                outputs="macrosegments_{}".format(sample_type),
                name="create_macrosegments",
            ),
            node(
                func=subs_date_join,
                inputs=[
                    "parameters",
                    "cvm_users_list_{}".format(sample_type),
                    "raw_features_{}".format(sample_type),
                    "ard_targets_{}".format(sample_type),
                    "churn_targets_{}".format(sample_type),
                    "macrosegments_{}".format(sample_type),
                ],
                outputs="training_features_{}".format(sample_type),
                name="create_training_sample",
            ),
            node(
                func=train_test_split,
                inputs=["training_features_{}".format(sample_type), "parameters"],
                outputs=[
                    "train_sample_{}".format(sample_type),
                    "test_sample_{}".format(sample_type),
                ],
                name="split_train_test",
            ),
        ]
    )


def create_cvm_microsegments(sample_type: str) -> Pipeline:
    """ Creates pipeline creating macrosegments and microsegments for scoring purposes.
    Uses microsegment history to make them more stable.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """
    inputs = [
        "parameters",
        "raw_features_{}",
        "l3_customer_profile_include_1mo_non_active_{}",
        "l3_customer_profile_include_1mo_non_active",
        "microsegments_macrosegments_history_input_{}",
    ]
    inputs = [dataset.format(sample_type) for dataset in inputs]
    outputs = [
        "microsegments_macrosegments_history_output_{}",
        "microsegments_macrosegments_{}",
    ]
    outputs = [dataset.format(sample_type) for dataset in outputs]
    return Pipeline(
        [
            node(
                func=get_micro_macrosegments,
                inputs=inputs,
                outputs=outputs,
                name="create_microsegments",
            )
        ]
    )


def create_prediction_sample(sample_type: str) -> Pipeline:
    """ Creates table for scoring.

    Args:
        sample_type: "scoring" if list created for scoring, "scoring_experiment" if list
            created for scoring_experiment.
    """
    inputs = [
        "raw_features_{}",
        "microsegments_macrosegments_{}",
        "l3_customer_profile_include_1mo_non_active",
        "important_columns",
        "parameters",
    ]
    inputs = [dataset.format(sample_type) for dataset in inputs]
    return Pipeline(
        [
            node(
                func=create_pred_sample,
                inputs=inputs,
                outputs="prediction_sample_{}".format(sample_type),
                name="create_prediction_sample",
            )
        ]
    )


def create_cvm_important_columns():
    """ Uses training samples as inputs, runs feature extraction procedure.

    Returns:
        Kedro pipeline.
    """

    return Pipeline(
        [
            node(
                func=pipeline_fit,
                inputs=["train_sample_fe", "parameters"],
                outputs=[
                    "train_sample_preprocessed_fe",
                    "preprocessing_pipeline_fe",
                    "null_columns",
                ],
                name="preprocessing_fit_fe",
            ),
            node(
                func=feature_selection_all_target,
                inputs=["train_sample_preprocessed_fe", "parameters"],
                outputs="important_columns",
                name="feature_selection",
            ),
        ]
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
        join_raw_features(sample_type)
        + create_cvm_microsegments(sample_type)
        + create_prediction_sample(sample_type)
    )


def training_data_prepare(sample_type: str) -> Pipeline:
    """ Create pipeline generating input data for training

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

    Returns:
        Kedro pipeline.
    """
    return join_raw_features(sample_type) + create_training_sample(sample_type)
