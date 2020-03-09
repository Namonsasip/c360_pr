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
from src.customer360.pipelines.cvm.modelling.nodes import (
    train_rf,
    create_shap_for_rf,
    train_xgb,
    predict_xgb,
)


def create_train_model(**kwargs):
    return Pipeline(
        [
            node(
                train_rf,
                ["l5_cvm_one_day_train_preprocessed_dev", "parameters"],
                "random_forest_dev",
                name="create_random_forest",
            ),
            node(
                create_shap_for_rf,
                [
                    "random_forest_dev",
                    "l5_cvm_one_day_test_preprocessed_dev",
                    "parameters",
                ],
                "shap_dev",
                name="create_shap",
            ),
            node(
                train_xgb,
                ["l5_cvm_one_day_train_preprocessed_dev", "parameters"],
                "xgb_dev",
                name="create_xgb",
            ),
        ]
    )


def create_predictions(sample_type: str) -> Pipeline:
    """ Creates prediction pipeline.

      Args:
          sample_type: sample type to use. Dev sample for "dev", Sample for "sample",
          full dataset for None (default).

      Returns:
          Kedro pipeline.
      """

    if sample_type not in ["dev", "sample", None]:
        raise Exception("Sample type {} not implemented".format(sample_type))

    if sample_type is not None:
        suffix = "_" + sample_type
    else:
        suffix = ""

    return Pipeline(
        [
            node(
                predict_xgb,
                ["l5_cvm_one_day_train_preprocessed" + suffix, "xgb_dev", "parameters"],
                ["l5_cvm_one_day_predictions" + suffix],
                name="create_l5_cvm_one_day_predictions" + suffix,
            )
        ]
    )
