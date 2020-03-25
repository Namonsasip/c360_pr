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

from cvm.preprocessing.nodes import (
    pipeline1_transform,
    pipeline1_fit,
    feature_selection_all_target,
)
from cvm.src.utils.get_suffix import get_suffix


def create_cvm_preprocessing(sample_type: str = None) -> Pipeline:
    """ Creates pipeline preprocessing data. Can create data pipeline for full dataset
     or given sample_type.

     Args:
         sample_type: sample type to use. Dev sample for "dev", Sample for "sample",
          full dataset for None (default).

     Returns:
         Kedro pipeline.
     """

    suffix = get_suffix(sample_type)

    return Pipeline(
        [
            node(
                pipeline1_fit,
                ["l5_cvm_one_day_train" + suffix, "parameters"],
                [
                    "l5_cvm_one_day_train_preprocessed" + suffix,
                    "preprocessing_pipeline" + suffix,
                ],
                name="create_l5_cvm_one_day_train_preprocessed" + suffix,
            ),
            node(
                pipeline1_transform,
                [
                    "l5_cvm_one_day_test" + suffix,
                    "preprocessing_pipeline" + suffix,
                    "parameters",
                ],
                "l5_cvm_one_day_test_preprocessed" + suffix,
                name="create_l5_cvm_one_day_test_preprocessed" + suffix,
            ),
        ]
    )


def create_cvm_preprocessing_scoring(
    sample_type: str = None, training_sample_type: str = None,
) -> Pipeline:
    """ Creates pipeline preprocessing data for scoring purposes.
    Can create data pipeline for full dataset or given sample_type.

     Args:
         sample_type: sample type to use. Dev sample for "dev", Sample for "sample",
          full dataset for None (default).
         training_sample_type: same as sample_type, but defines type of training
          sample for preprocessing pipeline used.
     Returns:
         Kedro pipeline.
     """

    suffix = get_suffix(sample_type)
    training_suffix = get_suffix(training_sample_type)

    return Pipeline(
        [
            node(
                pipeline1_transform,
                [
                    "l5_cvm_volatility" + suffix,
                    "preprocessing_pipeline" + training_suffix,
                    "parameters",
                ],
                "l5_cvm_one_day_preprocessed" + suffix,
                name="create_l5_cvm_one_day_preprocessed" + suffix,
            ),
        ]
    )


def create_cvm_feature_extraction(sample_type: str = None) -> Pipeline:
    """ Creates pipeline for feature extraction.

     Args:
         sample_type: sample type to use. Dev sample for "dev", Sample for "sample",
          full dataset for None (default).

     Returns:
         Kedro pipeline.
     """

    suffix = get_suffix(sample_type)

    return Pipeline(
        [
            node(
                feature_selection_all_target,
                ["l5_cvm_one_day_train_preprocessed" + suffix, "parameters"],
                "important_columns",
                name="feature_selection_l5_cvm_one_day_train_preprocessed" + suffix,
            ),
        ]
    )
