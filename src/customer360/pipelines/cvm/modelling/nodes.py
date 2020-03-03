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
from sklearn.ensemble import RandomForestClassifier
from src.customer360.pipelines.cvm.src.list_targets import list_targets
import shap


def train_rf(df: DataFrame) -> RandomForestClassifier:
    """ Create random forest model given the table to train on.

    Args:
        df: Training preprocessed sample.

    Returns:
        Random forest classifier.
    """
    target_chosen = "dilution1"
    target_cols = list_targets()
    df = df.filter("{} is not null".format(target_chosen))
    X = df.drop(*target_cols).toPandas()
    y = df.select(target_chosen).toPandas()
    rf = RandomForestClassifier(n_estimators=100, random_state=100)
    rf_fitted = rf.fit(X, y)

    return rf_fitted


def create_shap_for_rf(rf: RandomForestClassifier, df_test: DataFrame):
    """ Create SHAP plot for a given model.

    Args:
        df_test: Test set used for SHAP.
        rf: Given model.
    """

    target_cols = list_targets()
    X_test = df_test.drop(*target_cols).toPandas()
    # convert to float
    for col_name in X_test.columns:
        X_test[col_name] = X_test[col_name].astype(float)
    shap_values = shap.KernelExplainer(rf.predict, X_test)
    summ_plot = shap.summary_plot(shap_values, X_test)

    return summ_plot
