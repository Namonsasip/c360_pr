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

from cvm.preprocessing.nodes import pipeline_fit, pipeline_transform


def preprocessing_fit() -> Pipeline:
    """ Creates pipeline preprocessing data. Can create data pipeline for full dataset
     or given sample_type.

     Returns:
         Kedro pipeline.
     """
    sample_type = "training"
    return Pipeline(
        [
            node(
                pipeline_fit,
                ["train_sample_" + sample_type, "important_columns", "parameters"],
                [
                    "train_sample_preprocessed_" + sample_type,
                    "preprocessing_pipeline",
                    "null_columns",
                ],
                name="preprocessing_fit_" + sample_type,
            ),
            node(
                pipeline_transform,
                [
                    "test_sample_" + sample_type,
                    "important_columns",
                    "preprocessing_pipeline",
                    "parameters",
                    "null_columns",
                ],
                "test_sample_preprocessed_" + sample_type,
                name="create_test_sample_preprocessed_" + sample_type,
            ),
        ]
    )


def preprocessing_transform() -> Pipeline:
    """ Creates pipeline preprocessing data for scoring purposes.
    Can create data pipeline for full dataset or given sample_type.

     Returns:
         Kedro pipeline.
     """

    return Pipeline(
        [
            node(
                pipeline_transform,
                [
                    "features_macrosegments_scoring",
                    "important_columns",
                    "preprocessing_pipeline",
                    "parameters",
                    "null_columns",
                ],
                "sample_preprocessed_scoring",
                name="preprocessing_transform_scoring_sample",
            ),
        ]
    )
