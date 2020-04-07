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
import numpy
import pandas
from sklearn import metrics
import matplotlib.pyplot


def round_to_float(x):
    return round(float(x), 2)


def get_auc(true_val, pred_score):
    metrics_to_return = {
        "auc": round_to_float(metrics.roc_auc_score(true_val, pred_score))
    }
    return metrics_to_return


def get_tpr_fpr(
    true_val, pred_score, thresholds=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
):
    metrics_to_return = {}
    fpr, tpr, roc_thresholds = metrics.roc_curve(true_val, pred_score)

    def get_threshold_index(all_thresholds, threshold_chosen):
        thresholds_greater = [thr for thr in all_thresholds if thr > threshold_chosen]
        return len(thresholds_greater) - 1

    for threshold in thresholds:
        threshold_index = get_threshold_index(roc_thresholds, threshold)
        metrics_to_return["fpr_{}".format(str(threshold))] = round_to_float(
            fpr[threshold_index]
        )
        metrics_to_return["tpr_{}".format(str(threshold))] = round_to_float(
            tpr[threshold_index]
        )
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
            "quantile": numpy.arange(1, recall.shape[0] + 1) / recall.shape[0],
        }
    )


def get_precision_recall_for_quantiles(precision_recall, quantiles):
    models_metrics = {}
    for q in quantiles:
        filter_value = min([quan for quan in precision_recall.quantile if quan > q])
        precision = precision_recall[precision_recall.quantile == filter_value][
            "precision"
        ][0]
        recall = precision_recall[precision_recall.quantile == filter_value]["recall"][
            0
        ]
        models_metrics[f"precision_{q}"] = precision
        models_metrics[f"recall_{q}"] = recall
    return models_metrics


def get_roc_curve(true_val, pred_score):
    fpr, tpr = metrics.roc_curve(true_val, pred_score)
    matplotlib.pyplot.style.use("dark_background")
    _, roc = matplotlib.pyplot.subplots()
    roc.plot(fpr, tpr, color="lightblue")
    roc.plot([0, 1], [0, 1], color="cyan", linestyle="--")
    roc.xlim([0.0, 1.0])
    roc.ylim([0.0, 1.05])
    roc.xlabel("False Positive Rate")
    roc.ylabel("True Positive Rate")
    roc.title("Receiver operating characteristic example")
    roc.legend(loc="lower right")

    return roc


def get_precision_recall_plot(precision_recall):
    matplotlib.pyplot.style.use("dark_background")
    _, pr = matplotlib.pyplot.subplots()
    pr.plot(
        precision_recall["quantile"], precision_recall["precision"], color="lightblue"
    )
    pr.plot(precision_recall["quantile"], precision_recall["recall"], color="lightgray")
    pr.xlim([0.0, 1.0])
    pr.ylim([0.0, 1.05])
    pr.xlabel("Percentile cut-off")
    pr.ylabel("Metrics value")
    pr.title("Precision - recall curve")
    pr.legend(loc="lower right")

    return pr


def get_metrics_and_plots(true_val, pred_score):
    metrics_to_return = get_auc(true_val, pred_score)
    precision_recall = get_precision_recall_per_percentile(true_val, pred_score)
    metrics_to_return.update(
        get_precision_recall_for_quantiles(precision_recall, [0.05, 0.1])
    )
    roc = get_roc_curve(true_val, pred_score)
    pr = get_precision_recall_plot(true_val, pred_score)
    return {
        "metrics": metrics_to_return,
        "pr_table": precision_recall,
        "roc": roc,
        "pr_plot": pr,
    }
