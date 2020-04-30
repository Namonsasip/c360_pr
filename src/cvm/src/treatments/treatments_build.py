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
import functools
import logging
from datetime import date
from typing import Any, Dict, Tuple

import pandas

from customer360.utilities.spark_util import get_spark_session
from cvm.src.targets.churn_targets import add_days
from cvm.src.utils.parametrized_features import build_feature_from_parameters
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as func


def get_today(parameters: Dict[str, Any],) -> str:
    """ Returns scoring date if specified in parameters or today's date otherwise.

    Args:
        parameters: parameters defined in parameters.yml.
    """
    chosen_date = parameters["scoring"]["chosen_date"]
    if chosen_date == "" or chosen_date is None:
        today = date.today().strftime("%Y-%m-%d")
    else:
        today = chosen_date
    return today


def treatments_propositions_for_ard_churn(
    propensities: DataFrame,
    parameters: Dict[str, Any],
    blacklisted_users: DataFrame,
    microsegments: DataFrame,
    treatment_dictionary: DataFrame,
    order_policy: Column,
    use_case: str,
) -> DataFrame:
    """ Prepares treatments for ard and churn usecases.

    Args:
        use_case: either "churn" or "ard".
        propensities: table with propensities and macrosegments.
        parameters: parameters defined in parameters.yml.
        blacklisted_users: Table with users that cannot be targeted with treatment.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
        order_policy: column that will be used to order the users. Top users will be
            picked for treatment.
    Returns:
        Treatments chosen for use case.
    """

    logging.info("Adding {} treatments".format(use_case))
    treatment_size = parameters["treatment_sizes"][use_case]
    microsegments = microsegments.select(
        ["subscription_identifier", "ard_microsegment", "churn_microsegment"]
    )
    blacklisted_users = blacklisted_users.select("subscription_identifier")
    users_chosen = (
        # add microsegments
        propensities.join(microsegments, on="subscription_identifier")
        # prepare order column
        .withColumn("order_col", order_policy)
        # prepare column names
        .selectExpr(
            "subscription_identifier",
            "{}_macrosegment as macrosegment".format(use_case),
            "{}_microsegment as microsegment".format(use_case),
            "order_col",
            "'{}' as use_case".format(use_case),
        )
        # just in case
        .filter("macrosegment is not null and microsegment is not null")
        # drop blacklisted users
        .join(blacklisted_users, on="subscription_identifier", how="left_anti")
        # pick top users
        .orderBy("order_col", ascending=False)
        .limit(treatment_size)
        .drop("order_col")
    )

    # prepare treatments
    treatment_dictionary = treatment_dictionary.filter(
        "use_case == '{}'".format(use_case)
    ).selectExpr("microsegment", "campaign_code")

    # add treatments by picking a random from treatments dictionary
    treatments_chosen = users_chosen.join(
        treatment_dictionary, on="microsegment", how="left"
    ).withColumn("r", func.rand())
    random_window = Window.partitionBy("subscription_identifier").orderBy("r")
    treatments_chosen = (
        treatments_chosen.withColumn("rn", func.row_number().over(random_window))
        .filter("rn == 1")
        .drop(*["r", "rn"])
    )

    # check if all microsegments have treatments
    if treatments_chosen.filter("campaign_code is NULL").count() > 0:
        raise Exception("Campaign codes for some microsegments missing")

    return treatments_chosen


def treatments_propositions_from_rules(
    propensities: DataFrame,
    blacklisted_users: DataFrame,
    treatment_rules: Dict[str, Any],
    features_macrosegments_scoring: DataFrame,
    use_case: str = "rules",
    microsegment: str = "rules",
    macrosegment: str = "rules",
) -> DataFrame:
    """ Create treatments propositions from manual rules.

    Args:
        propensities: table with propensities and macrosegments.
        blacklisted_users: table with users that cannot be targeted with treatment.
        treatment_rules: manually defined rules to assign treatment.
        features_macrosegments_scoring: table with features to run rules on.
        use_case: value to return as use_case in output.
        microsegment: value to return as microsegment in output.
        macrosegment: value to return as macrosegment in output.
    """

    logging.info("Adding rule based treatments")
    blacklisted_users = blacklisted_users.select("subscription_identifier")
    propensities_with_features = propensities.join(
        features_macrosegments_scoring, on="subscription_identifier"
    ).join(blacklisted_users, on="subscription_identifier", how="anti-left")

    rule_based_treatments = (
        build_feature_from_parameters(
            propensities_with_features, "campaign_code", treatment_rules
        )
        .selectExpr(
            "subscription_identifier",
            "'{}' as macrosegment".format(macrosegment),
            "'{}' as microsegment".format(microsegment),
            "'{}' as use_case".format(use_case),
            "campaign_code",
        )
        .filter("campaign_code is not null")
    )

    return rule_based_treatments


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
    parameters: Dict[str, Any],
    microsegments: DataFrame,
    treatment_dictionary: DataFrame,
    treatments_history: DataFrame,
    features_macrosegments_scoring: DataFrame,
    treatment_rules: Dict[str, Any] = None,
) -> DataFrame:
    """ Generate treatments propositions for rule based treatments, churn and ard.

    Args:
        features_macrosegments_scoring: table with features to run rules on.
        microsegments: List of users and assigned microsegments.
        parameters: parameters defined in parameters.yml.
        propensities: table with propensities and macrosegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
        treatment_rules: manually defined rules to assign treatment.
        treatments_history: Table with history of treatments.
    Returns:
        Table with users, microsegments and treatments chosen.
    """

    def _add_to_blacklist(blacklisted_users_new, blacklisted_users_old=None):
        """Updates blacklisted users with new entries"""
        blacklisted_users_new = blacklisted_users_new.select("subscription_identifier")
        if blacklisted_users_old is None:
            return blacklisted_users_new
        else:
            return blacklisted_users_old.union(blacklisted_users_new)

    logging.info("Generating treatments")
    all_treatments = []
    recently_contacted = get_recently_contacted(parameters, treatments_history)
    blacklisted_users = _add_to_blacklist(recently_contacted)
    if treatment_rules is not None:
        rules_treatments = treatments_propositions_from_rules(
            propensities,
            blacklisted_users,
            treatment_rules,
            features_macrosegments_scoring,
        )
        all_treatments.append(rules_treatments)
        blacklisted_users = _add_to_blacklist(recently_contacted, rules_treatments)
    churn_order = (
        func.col("churn5_pred")
        + func.col("churn15_pred")
        + func.col("churn30_pred")
        + func.col("churn45_pred")
        + func.col("churn60_pred")
    )
    churn_treatments = treatments_propositions_for_ard_churn(
        propensities,
        parameters,
        blacklisted_users,
        microsegments,
        treatment_dictionary,
        churn_order,
        "churn",
    )
    all_treatments.append(churn_treatments)
    blacklisted_users = _add_to_blacklist(churn_treatments, blacklisted_users)
    ard_order = func.col("dilution1_pred") + func.col("dilution2_pred")
    ard_treatments = treatments_propositions_for_ard_churn(
        propensities,
        parameters,
        blacklisted_users,
        microsegments,
        treatment_dictionary,
        ard_order,
        "ard",
    )
    all_treatments.append(ard_treatments)
    return functools.reduce(lambda df1, df2: df1.union(df2), all_treatments)


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
    today = get_today(parameters)
    return treatments_history.filter(f"key_date != '{today}'").union(
        treatments_propositions.withColumn("key_date", func.lit(today)).select(
            treatments_history.columns
        )
    )


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


def convert_treatments_dictionary_to_sparkdf(
    treatment_dictionary_pd: pandas.DataFrame,
) -> DataFrame:
    """ Creates spark DataFrame treatments dictionary.

    Args:
        treatment_dictionary_pd: Table of microsegment to treatment mapping in pandas.
    """

    schema = """
        use_case:string,
        microsegment:string,
        treatment_variant:int,
        campaign_code:string
        """
    return get_spark_session().createDataFrame(treatment_dictionary_pd, schema=schema)


def generate_treatments_chosen(
    propensities: DataFrame,
    microsegments: DataFrame,
    treatment_dictionary_pd: pandas.DataFrame,
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
    features_macrosegments_scoring: DataFrame,
    treatment_rules: Dict[str, Any] = None,
) -> Tuple[pandas.DataFrame, DataFrame]:
    """ Function combines above functions to generate treatments and update treatments
    history.

    Args:
        features_macrosegments_scoring: table with features to run rules on.
        microsegments: List of users and assigned microsegments.
        parameters: parameters defined in parameters.yml.
        propensities: table with propensities.
        treatment_dictionary_pd: Table of microsegment to treatment mapping in pandas.
        treatment_rules: manually defined rules to assign treatment.
        treatments_history: Table with history of treatments.
    Returns:
        Pandas DataFrame with chosen campaigns and updated history DataFrame.
    """
    treatments_dictionary = convert_treatments_dictionary_to_sparkdf(
        treatment_dictionary_pd
    )
    treatments_propositions = get_treatments_propositions(
        propensities,
        parameters,
        microsegments,
        treatments_dictionary,
        treatments_history,
        features_macrosegments_scoring,
        treatment_rules,
    )
    treatments_history = update_history_with_treatments_propositions(
        treatments_propositions, treatments_history, parameters
    )
    treatments_chosen = serve_treatments_chosen(treatments_propositions, parameters)
    return treatments_chosen, treatments_history
