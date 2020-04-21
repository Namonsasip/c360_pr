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
from typing import Any, Dict, Tuple

import pandas

from cvm.src.utils.treatments import (
    add_microsegment_features,
    add_volatility_scores,
    define_microsegments,
    generate_treatments_chosen,
)
from pyspark.sql import DataFrame


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


def produce_treatments(
    propensities: DataFrame,
    microsegments: DataFrame,
    treatment_dictionary: pandas.DataFrame,
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
) -> Tuple[DataFrame, DataFrame]:
    """  Generates treatments and updated treatments history.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
        treatments_history: Table with history of treatments.
    """

    return generate_treatments_chosen(
        propensities,
        microsegments,
        treatment_dictionary,
        treatments_history,
        parameters,
    )


def deploy_contact(parameters: Dict[str, Any], df: DataFrame,):
    """ Copy list from df to the target path for campaign targeting

    Args:
        parameters: parameters defined in parameters.yml.
        df: DataFrame with treatment per customer.

    """
    created_date = date.today()
    df = df.withColumn("data_date", func.lit(created_date))
    df = df.selectExpr("data_date", "subscription_identifier as crm_subscription_id", "campaign_code as dummy01")
    file_name = parameters["output_path_ard"]+"_{}".format(
        created_date.strftime("%Y%m%d080000"))
    df.repartition(1).write.option("sep", "|").option("header", "true").option("mode", "overwrite").csv(file_name)

    return 0

