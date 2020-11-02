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

from typing import Any, Dict

from sklearn.ensemble import RandomForestClassifier

from cvm.src.models.predict import pyspark_predict_rf
from cvm.src.models.train import train_sklearn
from cvm.src.models.validate import log_pai_rf, validate_rf
from cvm.src.utils.deploy_table import deploy_table_to_path
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, when
import pyspark.sql.functions as func


def train_rf(df: DataFrame, parameters: Dict[str, Any]) -> object:
    """ Create random forest model given the table to train on.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.

    Returns:
        Random forest classifier.
    """

    def rf_cons():
        return RandomForestClassifier(n_estimators=100, random_state=100)

    return train_sklearn(df, parameters, rf_cons)


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


def validate_log_rf(
    rf_models: Dict[str, RandomForestClassifier],
    df: DataFrame,
    parameters: Dict[str, Any],
) -> None:
    """ Validates given model on test dataset and log everything to PAI.

    Args:
        rf_models: Saved models.
        df: DataFrame to calculate metrics for, must include targets and models
          predictions.
        parameters: parameters defined in parameters.yml.
    """

    model_diags = validate_rf(df, parameters)
    log_pai_rf(rf_models, model_diags, parameters)


def export_scores(
    df: DataFrame, df_profile: DataFrame, parameters: Dict[str, Any],
) -> None:
    """ Uses saved propensity scores to deploy table.

    Args:
        df: Propensity scores table.
        df_profile: Customer profile table contain all keys.
        parameters: parameters defined in parameters.yml.
    """
    date_filter = df_profile.selectExpr("MAX(partition_month)").collect()[0][0]
    df_profile = df_profile.filter(f"partition_month == '{date_filter}'")
    df_join = df.join(df_profile, ["subscription_identifier"], "left_outer")

    # Aggregate scores
    df_join = df_join.withColumn(
        "dilution_score", col("dilution1_pred") + col("dilution2_pred")
    )
    df_join = df_join.withColumn(
        "churn_score",
        col("churn5_pred")
        + col("churn15_pred")
        + col("churn30_pred")
        + col("churn45_pred")
        + col("churn60_pred"),
    )

    # Ranking by percentile
    w1 = Window.partitionBy().orderBy(df_join["dilution_score"].desc())
    w2 = Window.partitionBy().orderBy(df_join["churn_score"].desc())
    df_join = df_join.select(
        "*", func.percent_rank().over(w1).alias("dilution_percentile")
    )
    df_join = df_join.select(
        "*", func.percent_rank().over(w2).alias("churn_percentile")
    )

    # Create flag by threshold columns
    df_join = df_join.withColumn(
        "dilution_group",
        when(col("dilution_percentile") <= 0.2, "High").otherwise("Low"),
    )
    df_join = df_join.withColumn(
        "churn_group", when(col("churn_percentile") <= 0.05, "High").otherwise("Low")
    )

    # Select columns and deploy table
    df_out = df_join.select(
        "key_date",
        "subscription_identifier",
        "access_method_num",
        "old_subscription_identifier",
        "register_date",
        "dilution_score",
        "dilution_group" "churn_score",
        "churn_group",
    )
    deploy_table_to_path(df_out, parameters, "scoring_all")
