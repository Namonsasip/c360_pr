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
import uuid
from typing import Any, Dict, List

import matplotlib
import matplotlib.pyplot
import numpy
import pai
import pandas
from cvm.src.utils.list_targets import list_targets
from cvm.src.utils.utils import iterate_over_usecases_macrosegments_targets
from matplotlib.backends.backend_template import FigureCanvas
from matplotlib.figure import Figure
from pyspark.sql import DataFrame
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier


def round_to_float(x):
    return round(float(x), 2)


def get_auc(true_val, pred_score):
    metrics_to_return = {
        "auc": round_to_float(metrics.roc_auc_score(true_val, pred_score))
    }
    return metrics_to_return


def get_precision_recall_per_percentile(
    true_val, pred_score,
):
    assert true_val.shape == pred_score.shape
    ntiles = 100

    trues_preds = pandas.DataFrame(
        {"true_val": true_val, "pred_score": pred_score}
    ).sort_values(by="pred_score")
    vectors_length = true_val.shape[0]

    indices_to_pick = (
        numpy.arange(1, ntiles - 1) * (vectors_length - 2) / (ntiles - 1)
    ).round()
    trues_preds = trues_preds.iloc[indices_to_pick]

    precision, recall, _ = metrics.precision_recall_curve(
        trues_preds["true_val"], trues_preds["pred_score"]
    )

    return pandas.DataFrame(
        {
            "precision": precision,
            "recall": recall,
            "quantile": numpy.arange(recall.shape[0] + 1, 1, -1)
            / (recall.shape[0] + 1),
        }
    )


def get_precision_recall_for_quantiles(precision_recall, quantiles):
    models_metrics = {}
    for q in quantiles:
        interesting_row = (
            precision_recall[precision_recall["quantile"] > q]
            .sort_values(by="quantile")
            .iloc[0]
        )
        precision = interesting_row["precision"]
        recall = interesting_row["recall"]
        models_metrics[f"precision_{q}"] = precision
        models_metrics[f"recall_{q}"] = recall
    return models_metrics


def get_roc_curve(true_val, pred_score):
    fpr, tpr, _ = metrics.roc_curve(true_val, pred_score)
    matplotlib.pyplot.style.use("dark_background")
    fig = Figure()
    FigureCanvas(fig)
    roc = fig.add_subplot(111)
    roc.plot(fpr, tpr, color="lightblue")
    roc.plot([0, 1], [0, 1], color="cyan", linestyle="--")

    return roc


def get_precision_recall_plot(precision_recall):
    matplotlib.pyplot.style.use("dark_background")
    fig = Figure()
    FigureCanvas(fig)
    pr = fig.add_subplot(111)
    pr.plot(
        precision_recall["quantile"], precision_recall["precision"], color="lightblue"
    )
    pr.plot(precision_recall["quantile"], precision_recall["recall"], color="lightgray")

    return pr


def get_metrics_and_plots(true_val, pred_score):
    metrics_to_return = get_auc(true_val, pred_score)
    precision_recall = get_precision_recall_per_percentile(true_val, pred_score)
    metrics_to_return.update(
        get_precision_recall_for_quantiles(precision_recall, [0.05, 0.1, 0.9, 0.95])
    )
    roc = get_roc_curve(true_val, pred_score)
    pr = get_precision_recall_plot(precision_recall)
    return {
        "metrics": metrics_to_return,
        "pr_table": precision_recall,
        "roc": roc,
        "pr_plot": pr,
    }


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

        for macrosegment in list(macrosegments[use_case_chosen]) + ["global"]:
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
        models_metrics: Dict[str, Any],
        tags: List[str],
        precision_recall_table: pandas.DataFrame,
        roc_plot: Any,
        precision_recall_plot: Any,
    ):
        """ Logs only one model.

        Args:
            rf_model: Saved model.
            models_metrics: models metrics.
            tags: List of tags, eg ard, churn60
            precision_recall_table: Table with precision and recall per percentile.
            roc_plot: ROC of the model.
            precision_recall_plot: Precision - recall plot.
        """

        pai.start_run(tags=tags)
        if rf_model is not None:
            pai.log_model(rf_model)
            pai.log_features(rf_model.feature_names, rf_model.feature_importances_)
            models_metrics["features_num"] = len(rf_model.feature_names)
            models_metrics["sample_size"] = rf_model.sample_size
        pai.log_metrics(models_metrics)
        pai.log_artifacts({"precision_recall_table": precision_recall_table})
        pai.log_artifacts({"ROC": roc_plot})
        pai.log_artifacts({"precision_recall_plot": precision_recall_plot})
        pai.log_artifacts({"parameters": parameters})
        pai.end_run()

    def _fun_to_iterate(usecase, macrosegment, target):
        def pick_from_dict(d):
            if macrosegment == "global" and "global" not in d[usecase]:
                return None
            return d[usecase][macrosegment][target]

        rf_model = pick_from_dict(rf_models)
        models_diags_umt = pick_from_dict(models_diags)
        models_metrics = models_diags_umt["metrics"]
        precision_recall_table = models_diags_umt["pr_table"]
        roc_plot = models_diags_umt["roc"]
        precision_recall_plot = models_diags_umt["pr_plot"]
        tags = [usecase, macrosegment, target]
        _log_one_model(
            rf_model,
            models_metrics,
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
    iterate_over_usecases_macrosegments_targets(
        _fun_to_iterate, parameters, add_global_macrosegment=True
    )
