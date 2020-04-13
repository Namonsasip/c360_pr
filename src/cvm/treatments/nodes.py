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
from typing import Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as func

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
    target_users: DataFrame, microsegments: DataFrame, treatment_dictionary: DataFrame,
) -> DataFrame:
    """ Combine filtered users table, microsegments and the treatments assigned to
    microsegments.

    Args:
        target_users: List of users to target with treatment.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
    """

    treatment_dictionary = DataFrame(treatment_dictionary)
    df = (
        target_users.join(microsegments, on="subscription_identifier", how="left")
        .withColumn(
            "macrosegment",
            func.when(
                func.col("use_case") == "churn", func.col("ard_macrosegment")
            ).otherwise(func.col("churn_macrosegment")),
        )
        .withColumn(
            "microsegment",
            func.when(
                func.col("use_case") == "churn", func.col("ard_microsegment")
            ).otherwise(func.col("churn_microsegment")),
        )
        .join(treatment_dictionary, on=["macrosegment", "microsegment"])
        .select(
            [
                "subscription_identifier",
                "use_case",
                "macrosegment",
                "microsegment",
                "campaign_code1",
            ]
        )
    )
    return df
