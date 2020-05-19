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

import pandas
from cvm.src.targets.churn_targets import add_days
from cvm.src.treatments.rules import MultipleTreatments
from cvm.src.utils.utils import get_today
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as func


def get_recently_contacted(
    parameters: Dict[str, Any], treatments_history: DataFrame,
) -> DataFrame:
    """ Fetches users that were recently contacted.

    Args:
        parameters: parameters defined in parameters.yml.
        treatments_history: Table with history of treatments.
    Returns:
        Filtered propensities.
    """

    today = get_today(parameters)
    recent_past_date = add_days(today, -parameters["treatment_cadence"])
    recent_users = (
        treatments_history.filter(
            f"key_date >= '{recent_past_date}' and key_date < '{today}'"
        )
        .select("subscription_identifier")
        .distinct()
    )
    return recent_users


def get_treatments_propositions(
    propensities: DataFrame,
    features_macrosegments_scoring: DataFrame,
    parameters: Dict[str, Any],
    treatments_history: DataFrame,
    microsegments: DataFrame,
) -> DataFrame:
    """ Generate treatments propositions basing on rules treatment.

    Args:
        propensities: scores created by models.
        features_macrosegments_scoring: features used to run conditions on.
        parameters: parameters defined in parameters.yml.
        treatments_history: table with history of treatments.
        microsegments: users and microsegments table.
    Returns:
        Table with users, microsegments and treatments chosen.
    """
    # create treatments propositions
    recently_contacted = get_recently_contacted(parameters, treatments_history)
    propensities_with_features = propensities.join(
        features_macrosegments_scoring, on="subscription_identifier", how="left"
    ).join(microsegments, on="subscription_identifier", how="left")
    treatments_dict = parameters["treatment_rules"]
    SparkContext.getOrCreate().setCheckpointDir(parameters["spark_checkpoint_path"])
    treatments = MultipleTreatments(treatments_dict)
    treatments_propositions = treatments.apply_treatments(
        propensities_with_features, recently_contacted
    )
    # change output format
    treatments_propositions_pandas = (
        treatments_propositions.withColumnRenamed("treatment_name", "use_case")
        .withColumn("date", func.lit(get_today(parameters)))
        .withColumn(
            "microsegment",
            func.when(
                func.col("use_case") == "churn",
                func.col("churn_microsegment").otherwise("ard_microsegment"),
            ),
        )
        .withColumn(
            "macrosegment",
            func.when(
                func.col("use_case") == "churn",
                func.col("churn_macrosegment").otherwise("ard_macrosegment"),
            ),
        )
        .select(
            [
                "microsegment",
                "subscription_identifier",
                "macrosegment",
                "use_case",
                "campaign_code",
                "date",
            ]
        )
        .toPandas()
    )
    return treatments_propositions_pandas


def update_history_with_treatments_propositions(
    treatments_propositions: DataFrame,
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Add treatments propositions to treatments history.

    Args:
        treatments_propositions: DataFrame with users to be targeted and treatments
        chosen.
        treatments_history: Table with history of treatments.
        parameters: parameters defined in parameters.yml.
    Returns:
        Updated `treatments_history`.
    """

    logging.info("Updating treatments history")
    # today = get_today(parameters)
    # treatments_history = treatments_history.filter(f"key_date != '{today}'").union(
    #     treatments_propositions.withColumn("key_date", func.lit(today)).select(
    #         treatments_history.columns
    #     )
    # )
    return treatments_history


def serve_treatments_chosen(
    treatments_propositions: DataFrame, parameters: Dict[str, Any],
) -> pandas.DataFrame:
    """ Saves the csv with treatments basing on the recent entries in treatments
    history.

    Args:
        treatments_propositions: Table with history of treatments.
        parameters: parameters defined in parameters.yml.
    """

    treatments_df = treatments_propositions.filter("campaign_code != 'no_treatment'")
    today = get_today(parameters)
    if treatments_df.count() == 0:
        raise Exception(f"No treatments found for {today}")
    treatments_df = treatments_df.withColumn("date", func.lit(today))
    return treatments_df.toPandas()
