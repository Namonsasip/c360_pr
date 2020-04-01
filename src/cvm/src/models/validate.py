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

from sklearn import metrics


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


def get_metrics(true_val, pred_score):
    metrics_to_return = get_auc(true_val, pred_score)
    metrics_to_return.update(get_tpr_fpr(true_val, pred_score))
    return metrics_to_return
