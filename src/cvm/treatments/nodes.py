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
from datetime import date
from typing import Dict, Any, Tuple

import pandas
from pyspark.sql import DataFrame
from pyspark.sql import functions as func

from customer360.utilities.spark_util import get_spark_session
from cvm.src.targets.churn_targets import add_days
from cvm.src.utils.treatments import (
    add_volatility_scores,
    add_microsegment_features,
    define_microsegments,
    filter_cutoffs,
)


def prepare_microsegments(
    raw_features: DataFrame, reve: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Add microsegments columns.

    Args:
        raw_features: Table with users to add microsegments to and pre - preprocessing
            features.
        reve: Table with monthly revenue. Assumes using l3 profile table.
        parameters: parameters defined in parameters.yml.
    """

    vol = add_volatility_scores(raw_features, reve, parameters)
    micro_features = add_microsegment_features(raw_features, parameters).join(
        vol, "subscription_identifier"
    )
    return define_microsegments(micro_features, parameters)


def get_target_users(propensities: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Filters given propensities table according to cutoffs given.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
    """

    return filter_cutoffs(propensities, parameters)


def produce_treatments(
    target_users: DataFrame,
    microsegments: DataFrame,
    treatment_dictionary: DataFrame,
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
) -> Tuple[DataFrame, DataFrame]:
    """ Combine filtered users table, microsegments and the treatments assigned to
    microsegments.

    Args:
        target_users: List of users to target with treatment.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
        treatments_history: Table with history of treatments.
        parameters: parameters defined in parameters.yml.
    """

    # get treatments propositions
    df = (
        target_users.join(microsegments, on="subscription_identifier", how="left")
        .withColumn(
            "macrosegment",
            func.when(
                func.col("use_case") == "ard", func.col("ard_macrosegment")
            ).otherwise(func.col("churn_macrosegment")),
        )
        .withColumn(
            "microsegment",
            func.when(
                func.col("use_case") == "ard", func.col("ard_microsegment")
            ).otherwise(func.col("churn_microsegment")),
        )
        .select(["subscription_identifier", "use_case", "macrosegment", "microsegment"])
    ).toPandas()
    treatment_dictionary = treatment_dictionary[
        ["macrosegment", "microsegment", "campaign_code1"]
    ]
    treatments = pandas.merge(
        df, treatment_dictionary, on=["microsegment", "macrosegment"], how="left"
    )
    treatments_df = get_spark_session().createDataFrame(
        treatments,
        schema="""
        subscription_identifier:string,
        use_case:string,
        macrosegment:string,
        microsegment:string,
        campaign_code1:string
        """,
    )

    # filter those that were recently targeted
    today = date.today().strftime("%Y-%m-%d")
    recent_past_date = add_days(today, -parameters["treatment_cadence"])

    recent_history = (
        treatments_history.filter(f"key_date >= '{recent_past_date}'")
        .select(["subscription_identifier", "use_case"])
        .distinct()
    )
    treatments_df = treatments_df.join(
        recent_history, on=["subscription_identifier", "use_case"], how="left_anti"
    )

    # update history
    to_append = treatments_df.withColumn("key_date", func.lit(today))
    treatments_history = treatments_history.union(to_append)

    return treatments_df, treatments_history
