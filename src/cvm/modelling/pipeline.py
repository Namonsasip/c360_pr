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

from cvm.modelling.nodes import train_rf, predict_rf
from cvm.src.utils.get_suffix import get_suffix


def create_train_model(sample_type: str = None) -> Pipeline:
    """ Creates prediction pipeline.

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
                train_rf,
                ["l5_cvm_one_day_train_preprocessed" + suffix, "parameters"],
                "random_forest" + suffix,
                name="create_random_forest" + suffix,
            ),
        ]
    )


def create_predictions(
    sample_type: str = None,
    training_sample_type: str = None,
    input_dataset_name: str = None,
) -> Pipeline:
    """ Creates prediction pipeline.

      Args:
          sample_type: sample type to use. Dev sample for "dev", Sample for "sample",
              full dataset for None (default).
          training_sample_type: same as sample_type, but defines type of training
              sample for models used.
          input_dataset_name: uses given dataset as input, overrides other arguments.

      Returns:
          Kedro pipeline.
      """

    suffix = get_suffix(sample_type)
    training_suffix = get_suffix(training_sample_type)
    if input_dataset_name is None:
        input_name = "l5_cvm_one_day_preprocessed" + suffix
    else:
        input_name = input_dataset_name

    return Pipeline(
        [
            node(
                predict_rf,
                [input_name, "random_forest" + training_suffix, "parameters"],
                "l5_cvm_one_day_predictions" + suffix,
                name="create_l5_cvm_one_day_predictions" + suffix,
            ),
        ]
    )
