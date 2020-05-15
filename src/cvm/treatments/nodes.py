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
from typing import Any, Dict, Tuple

import pandas
from cvm.src.treatments.deploy_treatments import deploy_contact, prepare_campaigns_table
from cvm.src.treatments.microsegments import (
    add_microsegment_features,
    add_volatility_scores,
    define_microsegments,
)
from cvm.src.treatments.treatments_build import (
    get_treatments_propositions,
    update_history_with_treatments_propositions,
)
from pyspark.sql import DataFrame


def prepare_microsegments(
    raw_features: DataFrame,
    reve: DataFrame,
    parameters: Dict[str, Any],
    reduce_cols: bool = True,
) -> DataFrame:
    """ Add microsegments columns.

    Args:
        reduce_cols: should columns be reduced only to key columns, microsegment and
            microsegments.
        raw_features: Table with users to add microsegments to and pre - preprocessing
            features.
        reve: Table with monthly revenue. Assumes using l3 profile table.
        parameters: parameters defined in parameters.yml.
    """

    vol = add_volatility_scores(raw_features, reve, parameters)
    micro_features = add_microsegment_features(raw_features, parameters).join(
        vol, "subscription_identifier"
    )
    return define_microsegments(micro_features, parameters, reduce_cols)


def produce_treatments(
    propensities: DataFrame,
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
    features_macrosegments_scoring: DataFrame,
    microsegments: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """  Generates treatments and updated treatments history.

    Args:
        microsegments: users and microsegments table.
        features_macrosegments_scoring: table with features to run rules on.
        parameters: parameters defined in parameters.yml.
        propensities: table with propensities.
        treatments_history: Table with history of treatments.
    """
    treatments_propositions = get_treatments_propositions(
        propensities,
        features_macrosegments_scoring,
        parameters,
        treatments_history,
        microsegments,
    )
    treatments_history = update_history_with_treatments_propositions(
        treatments_propositions, treatments_history, parameters
    )
    return treatments_propositions, treatments_history


def deploy_treatments(
    treatments_chosen: pandas.DataFrame, parameters: Dict[str, Any],
):
    """ Send the treatments to the campaign team.

    Args:
        treatments_chosen: List of users and campaigns chosen for all use cases.:
        parameters: parameters defined in parameters.yml.
    """

    skip_node = parameters["treatment_output"]["skip_sending"] == "yes"
    if not skip_node:
        use_cases = parameters["targets"]
        for use_case in use_cases:
            campaign_table_prepared = prepare_campaigns_table(
                treatments_chosen, use_case
            )
            deploy_contact(campaign_table_prepared, parameters, use_case)
    else:
        logging.info("Sending treatments skipped")
