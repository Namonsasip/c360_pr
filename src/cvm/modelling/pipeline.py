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

from cvm.modelling.nodes import predict_rf, train_rf, validate_log_rf


def train_model() -> Pipeline:
    """ Creates model training and validating pipeline.

      Returns:
          Kedro pipeline.
      """

    sample_type = "training"
    return Pipeline(
        [
            node(
                train_rf,
                ["train_sample_preprocessed_" + sample_type, "parameters"],
                "random_forest",
                name="create_random_forest_" + sample_type,
            ),
            node(
                predict_rf,
                [
                    "test_sample_preprocessed_" + sample_type,
                    "random_forest",
                    "parameters",
                ],
                "test_sample_predictions_" + sample_type,
                name="test_sample_predict_" + sample_type,
            ),
            node(
                validate_log_rf,
                [
                    "random_forest",
                    "test_sample_predictions_" + sample_type,
                    "parameters",
                ],
                None,
                name="validate_" + sample_type,
            ),
        ]
    )


def score_model() -> Pipeline:
    """ Creates prediction pipeline.

      Returns:
          Kedro pipeline.
      """

    return Pipeline(
        [
            node(
                predict_rf,
                ["sample_preprocessed_scoring", "random_forest", "parameters"],
                "propensity_scores",
                name="create_propensity_scores",
            ),
        ]
    )
