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
import logging
from typing import Any, Dict

from cvm.src.models.get_pandas_train_test_sample import get_pandas_train_test_sample
from cvm.src.utils.utils import iterate_over_usecases_macrosegments_targets
from pyspark.sql import DataFrame


def train_sklearn(
    df: DataFrame, parameters: Dict[str, Any], sklearn_model: Any
) -> object:
    """ Create sklearn model given the table to train on.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.
        sklearn_model: initialized sklearn model.

    Returns:
        Dictionary of trained models.
    """
    log = logging.getLogger(__name__)

    def _train_for_macrosegment_target(use_case_chosen, macrosegment, target_chosen):
        log.info(
            "Training model for {} target, {} macrosegment.".format(
                target_chosen, macrosegment
            )
        )

        X, y = get_pandas_train_test_sample(
            df, parameters, target_chosen, use_case_chosen, macrosegment
        )
        y = y.values.ravel()
        log.info("Created training sample for macrosegment")

        model_fitted = sklearn_model.fit(X, y)
        log.info("Model trained")
        model_fitted.feature_names = list(X.columns.values)
        model_fitted.sample_size = X.shape[0]
        return model_fitted

    models = iterate_over_usecases_macrosegments_targets(
        _train_for_macrosegment_target, parameters
    )

    return models
