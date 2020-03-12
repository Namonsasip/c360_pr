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
from typing import Dict, Any

from sklearn.ensemble import RandomForestClassifier

from customer360.pipelines.cvm.src.models.predict import predict_rf_pandas
from customer360.pipelines.cvm.src.utils.list_targets import list_targets
from sklearn import metrics
from math import floor
from pyspark.sql import DataFrame


def get_auc(true_val, pred_score):
    metrics_to_return = {"auc": metrics.roc_auc_score(true_val, pred_score)}
    return metrics_to_return


def get_tpr_fpr(true_val, pred_score, quantile_thresholds=[0.01, 0.05, 0.1, 0.2]):
    n = len(true_val)
    metrics_to_return = {}
    fpr, tpr, _ = metrics.roc_curve(true_val, pred_score)
    for quantile_threshold in quantile_thresholds:
        quantile_n = floor(n * quantile_threshold)
        metrics_to_return["fpr_{}".format(quantile_threshold)] = fpr[quantile_n]
        metrics_to_return["tpr_{}".format(quantile_threshold)] = tpr[quantile_n]
    return metrics_to_return


def get_metrics(true_val, pred_score):
    metrics_to_return = get_auc(true_val, pred_score)
    metrics_to_return.update(get_tpr_fpr(true_val), pred_score)
    return metrics_to_return


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
        true_val = predictions[target_chosen]
        pred_score = predictions[target_chosen + "_pred"]
        models_metrics[target_chosen] = get_metrics(true_val, pred_score)

    log = logging.getLogger(__name__)
    log.info("Models metrics: {}".format(models_metrics))
    return models_metrics
