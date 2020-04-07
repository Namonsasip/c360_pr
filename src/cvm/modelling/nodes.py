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
import uuid

import matplotlib
import pai
import pandas
from pyspark.sql import DataFrame
from typing import Dict, Any, Union, List
from sklearn.ensemble import RandomForestClassifier
import xgboost
import logging
from cvm.src.models.get_pandas_train_test_sample import get_pandas_train_test_sample
from cvm.src.models.predict import (
    pyspark_predict_rf,
    pyspark_predict_xgb,
)
from cvm.src.models.validate import get_metrics_and_plots
from cvm.src.utils.list_targets import list_targets
from cvm.src.utils.utils import iterate_over_usecases_macrosegments_targets


def train_rf(
    df: DataFrame, parameters: Dict[str, Any]
) -> Dict[Any, Dict[Any, Dict[Union[str, Any], object]]]:
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

    def _train_for_macrosegment_target(use_case_chosen, macrosegment, target_chosen):
        log.info(
            "Training model for {} target, {} macrosegment.".format(
                target_chosen, macrosegment
            )
        )

        X, y = get_pandas_train_test_sample(
            df, parameters, target_chosen, use_case_chosen, macrosegment
        )

        rf = RandomForestClassifier(n_estimators=100, random_state=100)
        y = y.values.ravel()

        rf_fitted = rf.fit(X, y)
        rf_fitted.feature_names = list(X.columns.values)
        rf_fitted.sample_size = X.shape[0]
        return rf_fitted

    def _train_for_macrosegment(use_case_chosen, macrosegment):
        macrosegment_models = {}
        for target_chosen in target_cols[use_case_chosen]:
            macrosegment_models[target_chosen] = _train_for_macrosegment_target(
                use_case_chosen, macrosegment, target_chosen
            )
        return macrosegment_models

    def _train_for_usecase(use_chosen):
        usecase_models = {}
        for macrosegment in macrosegments[use_chosen]:
            usecase_models[macrosegment] = _train_for_macrosegment(
                use_chosen, macrosegment
            )
        return usecase_models

    models = {}
    for use_case in parameters["targets"]:
        models[use_case] = _train_for_usecase(use_case)

    return models


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


def validate_rf(df: DataFrame, parameters: Dict[str, Any],) -> Dict[str, Any]:
    """ Runs validation on validation datasets.
        Calculates metrics to assess models performances.

    Args:
        df: DataFrame to calculate metrics for, must include targets and models
          predictions.
        parameters: parameters defined in parameters.yml.
    Returns:
        Dictionary with metrics for models.
    """

    log = logging.getLogger(__name__)
    target_cols_use_case_split = list_targets(parameters, case_split=True)
    macrosegments = parameters["macrosegments"]

    def _validate_macrosegment_target(pd_df, target_chosen):
        log.info("Validating {} target predictions.".format(target_chosen))

        pd_filter = (
            ~pd_df[target_chosen].isnull() & ~pd_df[target_chosen + "_pred"].isnull()
        )
        pd_df_filtered = pd_df[pd_filter]
        true_val = pd_df_filtered[target_chosen]
        pred_score = pd_df_filtered[target_chosen + "_pred"]
        return get_metrics_and_plots(true_val, pred_score)

    def _validate_macrosegment(df_validate, use_case_chosen, macrosegment):
        log.info("Validating {} macrosegments.".format(macrosegment))
        if macrosegment != "global":
            df_validate = df_validate.filter(
                "{}_macrosegment == '{}'".format(use_case_chosen, macrosegment)
            )
        pd_df = df_validate.toPandas()
        macrosegment_models_diags = {}
        for target_chosen in target_cols_use_case_split[use_case_chosen]:
            macrosegment_models_diags[target_chosen] = _validate_macrosegment_target(
                pd_df, target_chosen
            )
        return macrosegment_models_diags

    def _validate_usecase(df_validate, use_case_chosen):
        log.info("Validating {} use case.".format(use_case_chosen))
        use_case_diags = {}

        for macrosegment in macrosegments[use_case_chosen] + ["global"]:
            use_case_diags[macrosegment] = _validate_macrosegment(
                df_validate, use_case_chosen, macrosegment
            )
        return use_case_diags

    models_diags = {}
    for use_case in parameters["targets"]:
        models_diags[use_case] = _validate_usecase(df, use_case)

    return models_diags


def log_pai_rf(
    rf_models: Dict[str, RandomForestClassifier],
    models_diags: Dict[str, Any],
    parameters: Dict[str, Any],
):
    """Logs models diagnostics to PAI.

    Args:
        rf_models: Saved dictionary of models for different targets.
        models_diags: Dictionary with plots and diagnostics metrics.
        parameters: parameters defined in parameters.yml.
    """

    def _log_one_model(
        rf_model: RandomForestClassifier,
        metrics: Dict[str, Any],
        tags: List[str],
        precision_recall_table: pandas.DataFrame,
        roc_plot: matplotlib.pyplot.subfigure,
        precision_recall_plot: matplotlib.pyplot.subfigure,
    ):
        """ Logs only one model.

        Args:
            rf_model: Saved model.
            metrics: models metrics.
            tags: List of tags, eg ard, churn60
            precision_recall_table: Table with precision and recall per percentile.
            roc_plot: ROC of the model.
            precision_recall_plot: Precision - recall plot.
        """

        pai.start_run(tags=tags)
        pai.log_model(rf_model)
        pai.log_features(rf_model.feature_names, rf_model.feature_importances_)
        metrics["features_num"] = len(rf_model.feature_names)
        metrics["sample_size"] = rf_model.sample_size
        pai.log_metrics(metrics)
        pai.log_params(rf_model.get_params())
        pai.log_artifacts(precision_recall_table)
        pai.log_artifacts(roc_plot)
        pai.log_artifacts(precision_recall_plot)
        pai.end_run()

    def _fun_to_iterate(usecase, macrosegment, target):
        def pick_from_dict(d):
            return d[usecase][macrosegment][target]

        rf_model = pick_from_dict(rf_models)
        metrics = pick_from_dict(models_diags["metrics"])
        precision_recall_table = pick_from_dict(models_diags["pr_table"])
        roc_plot = pick_from_dict(models_diags["roc"])
        precision_recall_plot = pick_from_dict(models_diags["pr_plot"])
        tags = [usecase, macrosegment, target]
        _log_one_model(
            rf_model,
            metrics,
            tags,
            precision_recall_table,
            roc_plot,
            precision_recall_plot,
        )

    experiment_name = parameters["pai_experiment_name"]
    if experiment_name == "":
        experiment_name = str(uuid.uuid4())
    pai.set_config(
        experiment=experiment_name,
        storage_runs=parameters["pai_runs_path"],
        storage_artifacts=parameters["pai_artifacts_path"],
    )
    iterate_over_usecases_macrosegments_targets(_fun_to_iterate, parameters)
