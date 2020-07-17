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

from cvm.src.targets.churn_targets import add_days
from cvm.src.treatments.rules import MultipleTreatments
from cvm.src.treatments.treatment_features import (
    add_call_center_features,
    add_churn_ard_optimizer_features,
    add_inactivity_days_num,
    add_other_sim_card_features,
)
from cvm.src.utils.utils import get_today, join_multiple, pick_one_per_subscriber
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


def treatments_featurize(
    propensities: DataFrame,
    prediction_sample: DataFrame,
    microsegments: DataFrame,
    recent_profile: DataFrame,
    main_packs: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Prepare table with users and features needed for treatments generation

    Args:
        microsegments: table with microsegments.
        prediction_sample: table with all features needed.
        propensities: scores created by models.
        recent_profile: table with users' national ids, only last date.
        main_packs: table describing prepaid main packages.
        parameters: parameters defined in parameters.yml.
    """
    propensities_with_features = join_multiple(
        ["subscription_identifier"], propensities, prediction_sample, microsegments
    )
    treatments_features = add_other_sim_card_features(
        propensities_with_features, recent_profile, main_packs, parameters
    )
    treatments_features = add_call_center_features(treatments_features)
    treatments_features = add_churn_ard_optimizer_features(
        treatments_features, propensities, parameters
    )
    treatments_features = add_inactivity_days_num(treatments_features, parameters)
    return treatments_features


def get_treatments_propositions(
    parameters: Dict[str, Any],
    treatments_history: DataFrame,
    treatments_features: DataFrame,
    users: DataFrame,
) -> DataFrame:
    """ Generate treatments propositions basing on rules treatment.

    Args:
        treatments_features: features used for treatments, output of treatment
            featurizer.
        parameters: parameters defined in parameters.yml.
        treatments_history: table with history of treatments.
        users: table with users and dates to create targets for, used to map to old sub
            id.
    Returns:
        Table with users, microsegments and treatments chosen.
    """
    # create treatments propositions
    recently_contacted = get_recently_contacted(parameters, treatments_history)
    # apply treatment rules
    treatments = MultipleTreatments(parameters["treatment_rules"])
    treatments_propositions = treatments.apply_treatments(
        treatments_features, recently_contacted
    )
    users = users.select("old_subscription_identifier", "subscription_identifier")
    # change output format
    treatments_propositions = (
        treatments_propositions.withColumn("date", func.lit(get_today(parameters)))
        .withColumn(
            "microsegment",
            func.when(
                func.col("use_case") == "churn", func.col("churn_microsegment")
            ).otherwise(func.col("ard_microsegment")),
        )
        .withColumn(
            "macrosegment",
            func.when(
                func.col("use_case") == "churn", func.col("churn_macrosegment")
            ).otherwise(func.col("ard_macrosegment")),
        )
        .join(users, on="subscription_identifier")
        .select(
            [
                "microsegment",
                "subscription_identifier",
                "old_subscription_identifier",
                "macrosegment",
                "use_case",
                "campaign_code",
                "date",
                "treatment_name",
            ]
        )
    )
    return pick_one_per_subscriber(treatments_propositions)


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
    skip = parameters["treatment_output"]["skip_sending"] == "yes"
    if skip:
        logging.getLogger(__name__).info("Skipping updating treatments history")
        return treatments_history
    else:
        logging.getLogger(__name__).info("Updating treatments history")
        today = get_today(parameters)
        treatments_history = treatments_history.filter(
            f"key_date != '{today}'"
        ).unionByName(
            treatments_propositions.withColumn("key_date", func.lit(today)).select(
                treatments_history.columns
            )
        )
        return treatments_history
