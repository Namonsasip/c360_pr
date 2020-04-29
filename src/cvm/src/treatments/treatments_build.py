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
from cvm.src.utils.utils import return_column_as_list
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as func


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
    ).selectExpr("{}_microsegment as microsegment", "campaign_code")

    # add treatments by picking a random from treatments dictionary
    treatments_chosen = users_chosen.join(
        treatment_dictionary, on="microsegment", how="left"
    ).withColumn("r", func.rand())
    random_window = Window.partitionBy("subscription_identifier").orderby("r")
    treatments_chosen = (
        treatments_chosen.withColumn("rn", func.row_number().over(random_window))
        .filter("rn == 1")
        .drop(["r", "rn"])
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


def get_targets_list_per_use_case(
    propensities: DataFrame, parameters: Dict[str, Any], blacklisted_users: DataFrame,
) -> DataFrame:
    """ Updates treatments history with treatments target users. The list is generated
        basing on presumed size of treatment groups and some order which is a function
        of propensities.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
        blacklisted_users: Table with users that cannot be targeted with treatment.
    Returns:
        List of users to target with use case to target user with. DOES NOT add
        treatments themselves.
    """

    # define order columns
    churn_order = (
        func.col("churn5_pred")
        + func.col("churn15_pred")
        + func.col("churn30_pred")
        + func.col("churn45_pred")
        + func.col("churn60_pred")
    )
    ard_order = func.col("dilution1_pred") + func.col("dilution2_pred")

    # apply orders
    propensities = propensities.withColumn("churn_order", churn_order).withColumn(
        "ard_order", ard_order
    )

    # filter blacklisted
    propensities = propensities.join(
        blacklisted_users, on="subscription_identifier", how="left_anti"
    )

    # pick users to treat
    treatment_sizes = parameters["treatment_sizes"]

    def _pick_top_n_rows(df, col_to_sort_on, n):
        return (
            df.orderBy(col_to_sort_on, ascending=False)
            .limit(n)
            .select("subscription_identifier")
            .distinct()
        )

    churn_users = _pick_top_n_rows(
        propensities, "churn_order", treatment_sizes["churn"]
    )
    propensities_no_churn = propensities.join(
        churn_users, on="subscription_identifier", how="left_anti"
    )
    ard_users = _pick_top_n_rows(
        propensities_no_churn, "ard_order", treatment_sizes["ard"]
    )

    # combine the users
    churn_users = churn_users.withColumn("use_case", func.lit("churn"))
    ard_users = ard_users.withColumn("use_case", func.lit("ard"))
    df = churn_users.union(ard_users)
    logging.info(f"{df.count()} users picked for treatment")

    return df


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

    today = date.today().strftime("%Y-%m-%d")
    recent_past_date = add_days(today, -parameters["treatment_cadence"])
    recent_users = (
        treatments_history.filter(f"key_date >= '{recent_past_date}'")
        .select("subscription_identifier")
        .distinct()
    )
    return recent_users


def get_treatments_propositions(
    target_users: DataFrame,
    microsegments: DataFrame,
    treatment_dictionary: DataFrame,
    add_no_treatment: bool = False,
) -> DataFrame:
    """ Combine filtered users table, microsegments and the treatments assigned to
    microsegments.

    Args:
        target_users: List of users to target with treatment.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
        add_no_treatment: If `True` then new treatment called 'no_treatment' is added to
            create a control group.
    Returns:
        Table with users, microsegments and treatments chosen.
    """

    # join with microsegments
    target_users = target_users.join(
        microsegments, on="subscription_identifier", how="left"
    )

    def _choose_column_basing_on_usecase(df, colname):
        is_ard = func.col("use_case") == "ard"
        pick_ard = func.col(f"ard_{colname}")
        pick_churn = func.col(f"churn_{colname}")
        return df.withColumn(
            colname, func.when(is_ard, pick_ard).otherwise(pick_churn),
        )

    target_users = _choose_column_basing_on_usecase(target_users, "microsegment")
    target_users = _choose_column_basing_on_usecase(target_users, "macrosegment")
    target_users = target_users.filter("microsegment is not null")

    def _add_random_column_from_values(tab, values, colname):
        """ Adds a new column to DataFrame which consists of randomly picked elements
            of values"""
        n = len(values)
        # setup when statement
        tab = tab.withColumn("val_ind", func.floor(func.rand() * n))
        whens = [func.when(func.col("val_ind") == i, values[i]) for i in range(0, n)]
        whens = func.coalesce(*whens)

        # choose values
        tab = tab.withColumn(colname, whens)
        tab = tab.drop("val_ind")

        return tab

    def _pick_treatments_for_microsegment(microsegment_chosen):
        """ Picks treatments for target users using treatment dictionary"""
        chosen_microsegment_treatments = return_column_as_list(
            treatment_dictionary.filter(f"microsegment == '{microsegment_chosen}'"),
            "campaign_code",
            True,
        )
        if len(chosen_microsegment_treatments) == 0:
            raise Exception(f"No treatment for {microsegment_chosen} found")
        target_users_in_microsegment = target_users.filter(
            f"microsegment == '{microsegment_chosen}'"
        )
        if len(chosen_microsegment_treatments) == 1:
            target_users_in_microsegment = target_users_in_microsegment.withColumn(
                "campaign_code", func.lit(chosen_microsegment_treatments[0])
            )
        else:
            if add_no_treatment:
                chosen_microsegment_treatments += ["no_treatment"]
            target_users_in_microsegment = _add_random_column_from_values(
                target_users_in_microsegment,
                chosen_microsegment_treatments,
                "campaign_code",
            )
        return target_users_in_microsegment

    microsegments = return_column_as_list(target_users, "microsegment", True)
    treatments_per_microsegments = [
        _pick_treatments_for_microsegment(microsegment)
        for microsegment in microsegments
    ]
    target_users_with_treatments = functools.reduce(
        lambda df1, df2: df1.union(df2), treatments_per_microsegments,
    )

    cols_to_pick = [
        "subscription_identifier",
        "use_case",
        "macrosegment",
        "microsegment",
        "campaign_code",
    ]
    return target_users_with_treatments.select(cols_to_pick)


def update_history_with_treatments_propositions(
    treatments_propositions: DataFrame, treatments_history: DataFrame,
) -> DataFrame:
    """ Add treatments propositions to treatments history.

    Args:
        treatments_propositions: DataFrame with users to be targeted and treatments
        chosen.
        treatments_history: Table with history of treatments.
    Returns:
        Updated `treatments_history`.
    """

    today = date.today().strftime("%Y-%m-%d")
    return treatments_history.filter(f"key_date != '{today}'").union(
        treatments_propositions.withColumn("key_date", func.lit(today)).select(
            treatments_history.columns
        )
    )


def serve_treatments_chosen(treatments_propositions: DataFrame) -> pandas.DataFrame:
    """ Saves the csv with treatments basing on the recent entries in treatments
    history.

    Args:
        treatments_propositions: Table with history of treatments.
    """

    treatments_df = treatments_propositions.filter("campaign_code != 'no_treatment'")
    today = date.today().strftime("%Y-%m-%d")
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
) -> Tuple[pandas.DataFrame, DataFrame]:
    """ Function combines above functions to generate treatments and update treatments
    history.

    Args:
        propensities: table with propensities.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary_pd: Table of microsegment to treatment mapping in pandas.
        treatments_history: Table with history of treatments.
        parameters: parameters defined in parameters.yml.
    Returns:
        Pandas DataFrame with chosen campaigns and updated history DataFrame.
    """

    targets_list_per_use_case = get_targets_list_per_use_case(
        propensities, parameters, treatments_history
    )
    treatments_dictionary = convert_treatments_dictionary_to_sparkdf(
        treatment_dictionary_pd
    )
    treatments_propositions = get_treatments_propositions(
        targets_list_per_use_case, microsegments, treatments_dictionary
    )
    treatments_history = update_history_with_treatments_propositions(
        treatments_propositions, treatments_history
    )
    treatments_chosen = serve_treatments_chosen(treatments_propositions)

    return treatments_chosen, treatments_history
