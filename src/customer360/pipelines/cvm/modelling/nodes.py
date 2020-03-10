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

from pyspark.sql import DataFrame
from typing import Dict, Any
from sklearn.ensemble import RandomForestClassifier
import xgboost
import logging
import numpy as np
from customer360.pipelines.cvm.src.models.get_pandas_train_test_sample import (
    get_pandas_train_test_sample,
)
from customer360.pipelines.cvm.src.models.predict import pyspark_predict_xgb
from customer360.pipelines.cvm.src.utils.list_targets import list_targets
import shap


def train_rf(df: DataFrame, parameters: Dict[str, Any]) -> RandomForestClassifier:
    """ Create random forest model given the table to train on.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.

    Returns:
        Random forest classifier.
    """
    target_cols = list_targets(parameters)

    log = logging.getLogger(__name__)

    models = {}
    for target_chosen in target_cols:

        log.info("Training random forest model for {} target.".format(target_chosen))

        X, y = get_pandas_train_test_sample(df, parameters, target_chosen)

        rf = RandomForestClassifier(n_estimators=100, random_state=100)
        y = np.as_array(y)
        rf_fitted = rf.fit(X, y)
        models[target_chosen] = rf_fitted

    return models


def create_shap_for_rf(
    rf: RandomForestClassifier, df_test: DataFrame, parameters: Dict[str, Any]
):
    """ Create SHAP plot for a given model.

    Args:
        df_test: Test set used for SHAP.
        rf: Given model.
        parameters: parameters defined in parameters.yml.
    """

    target_cols = list_targets(parameters)
    X_test = df_test.drop(*target_cols).toPandas()
    shap_values = shap.KernelExplainer(lambda x: rf.predict_proba(x), X_test)
    summ_plot = shap.summary_plot(shap_values, X_test)

    return summ_plot


def train_xgb(df: DataFrame, parameters: Dict[str, Any]) -> Dict[str, xgboost.Booster]:
    """ Create xgboost models for given the table to train on and all targets.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.

    Returns:
        Xgboost classifier.
    """
    target_cols = list_targets(parameters)

    log = logging.getLogger(__name__)

    models = {}
    for target_chosen in target_cols:

        log.info("Training xgboost model for {} target.".format(target_chosen))

        X, y = get_pandas_train_test_sample(df, parameters, target_chosen)

        xgb_model = xgboost.train(
            {"learning_rate": 0.01}, xgboost.DMatrix(X, label=y["target"]), 100
        )
        models[target_chosen] = xgb_model

    return models


def predict_xgb(
    df: DataFrame, xgb_models: Dict[str, xgboost.Booster], parameters: Dict[str, Any]
) -> DataFrame:
    """ Uses saved xgboost models to create propensity scores for given table.

    Args:
        df: Table with features.
        xgb_models: Saved dictionary of models for different targets.
        parameters: parameters defined in parameters.yml.
    Returns:
        Table with propensity scores.
    """

    predictions = pyspark_predict_xgb(df, xgb_models, parameters)
    return predictions
