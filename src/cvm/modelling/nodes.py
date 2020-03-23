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
from cvm.src.models import get_pandas_train_test_sample
from cvm.src.models import (
    pyspark_predict_xgb,
    pyspark_predict_rf,
    predict_rf_pandas,
)
from cvm.src.models import get_metrics
from cvm.src.utils import list_targets
import shap
import pandas as pd


def train_rf(df: DataFrame, parameters: Dict[str, Any]) -> RandomForestClassifier:
    """ Create random forest model given the table to train on.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.

    Returns:
        Random forest classifier.
    """
    target_cols = list_targets(parameters, case_split=True)
    macrosegments = parameters["macrosegments"]

    log = logging.getLogger(__name__)

    models = {}

    def _train_for_macrosegment_target(use_case, macrosegment, target_chosen):
        log.info(
            "Training model for {} target, {} macrosegment.".format(
                target_chosen, macrosegment
            )
        )

        X, y = get_pandas_train_test_sample(
            df, parameters, target_chosen, use_case, macrosegment
        )

        rf = RandomForestClassifier(n_estimators=100, random_state=100)
        y = y.values.ravel()
        return rf.fit(X, y)

    def _train_for_macrosegment(use_case, macrosegment):
        models = {}
        for target_chosen in target_cols[use_case]:
            models[target_chosen] = _train_for_macrosegment_target(
                use_case, macrosegment, target_chosen
            )
        return models

    def _train_for_usecase(use_case):
        models = {}
        for macrosegment in macrosegments[use_case]:
            models[macrosegment] = _train_for_macrosegment(use_case, macrosegment)
        return models

    models = {}
    for use_case in parameters["targets"]:
        models[use_case] = _train_for_usecase(use_case)

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


def predict_rf(
    df: DataFrame,
    rf_models: Dict[str, RandomForestClassifier],
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Uses saved Random Forest models to create propensity scores for given table.

    Args:
        df: Table with features.
        rf_models: Saved dictionary of models for different targets.
        parameters: parameters defined in parameters.yml.
    Returns:
        Table with propensity scores.
    """

    predictions = pyspark_predict_rf(df, rf_models, parameters)
    return predictions


def validate_rf(
    df: DataFrame,
    rf_models: Dict[str, RandomForestClassifier],
    parameters: Dict[str, Any],
) -> Dict[str, Any]:
    """ Runs predictions on validation datasets. Does not use pyspark to perform
     predictions. Calculates metrics to assess models performances.

    Args:
        df: DataFrame to predict for.
        rf_models: Given models to validate.
        parameters: parameters defined in parameters.yml.
    Returns:
        Dictionary with metrics for models.
    """
    target_cols = list_targets(parameters)
    predictions = predict_rf_pandas(df, rf_models, parameters)
    models_metrics = {}
    for target_chosen in target_cols:
        target_filter = pd.notna(predictions[target_chosen])
        predictions_for_target_chosen = predictions[target_filter]
        true_val = predictions_for_target_chosen[target_chosen]
        pred_score = predictions_for_target_chosen[target_chosen + "_pred"]
        models_metrics[target_chosen] = get_metrics(true_val, pred_score)

    log = logging.getLogger(__name__)
    log.info("Models metrics: {}".format(models_metrics))
    return models_metrics
