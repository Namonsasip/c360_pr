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
from typing import Any, Dict

import pandas

from customer360.utilities.spark_util import get_spark_session
from cvm.src.targets.churn_targets import add_days
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.utils import df_to_list, impute_from_parameters
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as func


def add_microsegment_features(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """Create features to create microsegments on.

    Args:
        df: DataFrame with raw (not preprocessed) features.
        parameters: parameters defined in parameters.yml.
    """

    log = logging.getLogger(__name__)
    log.info("Adding microsegments features")

    df = df.withColumn(
        "arpu_dynamic",
        df.sum_rev_arpu_total_revenue_monthly_last_month
        / df.sum_rev_arpu_total_revenue_monthly_last_three_month,
    )
    df = df.withColumn(
        "arpu_data_share",
        df.sum_rev_arpu_total_gprs_net_revenue_monthly_last_three_month
        / df.sum_rev_arpu_total_revenue_monthly_last_three_month,
    )
    df = df.withColumn(
        "data_to_voice_consumption",
        df.sum_usg_outgoing_data_volume_daily_last_thirty_day
        / df.sum_usg_outgoing_total_call_duration_daily_last_thirty_day,
    )
    df = df.withColumn(
        "data_weekend_consumption_share",
        df.sum_usg_data_weekend_usage_sum_weekly_last_four_week
        / (
            df.sum_usg_data_weekend_usage_sum_weekly_last_four_week
            + df.sum_usg_data_weekday_usage_sum_weekly_last_four_week
        ),
    )
    df = df.withColumn(
        "voice_weekend_consumption_share",
        df.sum_usg_outgoing_weekend_number_calls_sum_weekly_last_four_week
        / (
            df.sum_usg_outgoing_weekend_number_calls_sum_weekly_last_four_week
            + df.sum_usg_outgoing_weekday_number_calls_sum_weekly_last_four_week
        ),
    )

    key_date = df.select("key_date").first()[0]
    date_10days_ago = add_days(key_date, -10)
    df = df.withColumn(
        "action_in_last_10days",
        func.when(df.last_activity_date.isNull(), 0)
        .when(date_10days_ago > df.last_activity_date, 0)
        .otherwise(1),
    )

    return df


def define_microsegments(df: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Adds microsegment columns to given tables. Microsegments are used to connect
    with treatments.

    Args:
        df: DataFrame with all microsegment features.
        parameters: parameters defined in parameters.yml.
    """

    def get_when_then_clause_for_one_microsegment(conditions_list, microsegment_name):
        """Prepares when - then clause to create one microsegment."""
        when_str = " and ".join([f"({cond})" for cond in conditions_list])
        return "when " + when_str + " then '" + microsegment_name + "'"

    def get_when_then_cause(microsegments_dict, microsegments_colname):
        """Prepare when - then clause to create microsegment column for one use case."""
        when_then_clauses = [
            get_when_then_clause_for_one_microsegment(
                microsegments_dict[microsegment_name], microsegment_name
            )
            for microsegment_name in microsegments_dict
        ]
        return (
            "case "
            + "\n".join(when_then_clauses)
            + " else NULL end as "
            + microsegments_colname
        )

    def add_microsegments(
        df_with_microsegment_features, microsegments_dict, microsegments_colname
    ):
        """Calculates microsegment column for one use case."""
        case_when_clause = get_when_then_cause(
            microsegments_dict, microsegments_colname
        )
        return df_with_microsegment_features.selectExpr("*", case_when_clause)

    log = logging.getLogger(__name__)
    log.info("Defining microsegments")

    df = impute_from_parameters(df, parameters)

    microsegment_defs = parameters["microsegments"]
    for use_case in microsegment_defs:
        df = add_microsegments(
            df, microsegment_defs[use_case], use_case + "_microsegment",
        )

    cols_to_pick = [
        "subscription_identifier",
        "key_date",
        "ard_macrosegment",
        "churn_macrosegment",
        "churn_microsegment",
        "ard_microsegment",
    ]

    return df.select(cols_to_pick)


def mark_propensities_as_low_high(
    propensities: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Each propensity is mapped to 'H' if greater then given threshold and to 'L'
        otherwise.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
    """

    log = logging.getLogger(__name__)
    log.info("Mapping propensities to H / L")

    cutoffs = parameters["cutoffs"]

    def _map_usecase_macro_target(
        df, use_case_chosen, macrosegment_chosen, target_chosen
    ):
        cutoff_local = cutoffs[use_case_chosen][macrosegment_chosen][target_chosen]
        new_colname = target_chosen + "_lh"
        propensity_colname = target_chosen + "_pred"
        macrosegment_colname = use_case_chosen + "_macrosegment"

        macrosegment_cond = func.col(macrosegment_colname) == macrosegment_chosen
        H_cond = func.col(propensity_colname) >= cutoff_local
        L_cond = func.col(propensity_colname) < cutoff_local
        if new_colname not in df.columns:
            df = df.withColumn(new_colname, func.lit(None))
        when_clause = (
            func.when(macrosegment_cond & H_cond, "H")
            .when(macrosegment_cond & L_cond, "L")
            .otherwise(func.col(new_colname))
        )

        return df.withColumn(new_colname, when_clause)

    def _map_usecase_macro(df, use_case_chosen, macrosegment_chosen):
        for target_chosen in cutoffs[use_case_chosen][macrosegment_chosen]:
            df = _map_usecase_macro_target(
                df, use_case_chosen, macrosegment_chosen, target_chosen
            )
        return df

    def _map_usecase(df, use_case_chosen):
        for macrosegment_chosen in cutoffs[use_case_chosen]:
            df = _map_usecase_macro(df, use_case_chosen, macrosegment_chosen)
        return df

    for use_case in cutoffs:
        propensities = _map_usecase(propensities, use_case)

    return propensities


def filter_cutoffs(propensities: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Filters given propensities table according to cutoffs given.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
    """

    log = logging.getLogger(__name__)
    log.info("Choosing users to target with treatment")

    cutoffs = parameters["cutoffs"]

    def _get_filter_for_macro(use_case_chosen, macrosegment):
        """ Return string that filters out users of specific macrosegment with all
        propensities higher then cutoffs."""
        cutoffs_local = cutoffs[use_case_chosen][macrosegment]
        filter_str = f"{use_case_chosen}_macrosegment == '{macrosegment}'"
        for target in cutoffs_local:
            filter_str += f" and ({target}_pred >= {cutoffs_local[target]})"
        return filter_str

    # propensity above threshold for all targets
    def _get_filter_for_use_case(use_case_chosen):
        """ Returns filter string that filter out users of all macrosegments with all
        propensities higher than cutoffs."""
        macrosegments = cutoffs[use_case_chosen]
        filter_str = ""
        for macrosegment in macrosegments:
            macrosegment_filter_str = _get_filter_for_macro(
                use_case_chosen, macrosegment
            )
            filter_str += f"({macrosegment_filter_str}) or "
        filter_str = filter_str[:-4]
        return filter_str

    dfs = []
    churn_filter_str = _get_filter_for_use_case("churn")
    dfs.append(
        propensities.filter(churn_filter_str)
        .select("subscription_identifier")
        .withColumn("use_case", func.lit("churn"))
    )
    ard_filter_str = _get_filter_for_use_case("ard")
    ard_filter_str = f"({ard_filter_str}) and not ({churn_filter_str})"
    dfs.append(
        propensities.filter(ard_filter_str)
        .select("subscription_identifier")
        .withColumn("use_case", func.lit("ard"))
    )

    def union_dfs(df1, df2):
        return df1.union(df2)

    return functools.reduce(union_dfs, dfs)


def add_volatility_scores(
    users: DataFrame, reve: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """Create volatility score for given set of users.

    Args:
        users: DataFrame with users, subscription_identifier column will be
        used.
        Rest will be kept.
        reve: Monthly revenue data.
        parameters: parameters defined in parameters.yml.
    """

    vol_length = parameters["volatility_length"]
    reve_col = "norms_net_revenue"

    reve = prepare_key_columns(reve)
    users = prepare_key_columns(users)

    reve_cols_to_pick = parameters["key_columns"] + [reve_col]
    reve = reve.select(reve_cols_to_pick)
    reve_users_window = Window.partitionBy("subscription_identifier")
    reve = (
        reve.withColumn("reve_history", func.count("key_date").over(reve_users_window))
        .filter("reve_history >= {}".format(vol_length))
        .drop("reve_history")
    )
    vol_window = Window.partitionBy("subscription_identifier").orderBy(
        reve["key_date"].desc()
    )
    reve = reve.withColumn("month_id", func.rank().over(vol_window)).filter(
        "month_id <= {}".format(vol_length)
    )
    volatility = reve.groupby("subscription_identifier").agg(
        func.stddev(func.log(1 + func.col(reve_col))).alias("volatility")
    )

    users = users.join(volatility, "subscription_identifier", "left")
    users = users.fillna({"volatility": 0})
    cols_to_pick = ["subscription_identifier", "volatility"]
    users = users.select(cols_to_pick)

    return users


def get_targets_list_per_use_case(
    propensities: DataFrame, parameters: Dict[str, Any], treatments_history: DataFrame,
) -> DataFrame:
    """ Updates treatments history with treatments target users. The list is generated
        basing on presumed size of treatment groups and some order which is a function
        of propensities.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
        treatments_history: Table with history of treatments.
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

    # filter those that were recently targeted
    propensities = remove_recently_contacted(
        propensities, parameters, treatments_history
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

    return churn_users.union(ard_users)


def remove_recently_contacted(
    propensities: DataFrame, parameters: Dict[str, Any], treatments_history: DataFrame,
) -> DataFrame:
    """ Removes users that were recently contacted.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
        treatments_history: Table with history of treatments.
    Returns:
        Filtered propensities.
    """

    today = date.today().strftime("%Y-%m-%d")
    recent_past_date = add_days(today, -parameters["treatment_cadence"])

    recent_history = (
        treatments_history.filter(f"key_date >= '{recent_past_date}'")
        .select(["subscription_identifier", "use_case"])
        .distinct()
    )
    return propensities.join(
        recent_history, on=["subscription_identifier"], how="left_anti"
    )


def get_treatments_propositions(
    target_users: DataFrame, microsegments: DataFrame, treatment_dictionary: DataFrame,
) -> DataFrame:
    """ Combine filtered users table, microsegments and the treatments assigned to
    microsegments.

    Args:
        target_users: List of users to target with treatment.
        microsegments: List of users and assigned microsegments.
        treatment_dictionary: Table of microsegment to treatment mapping.
    Returns:
        Table with users, microsegments and treatments chosen. For multiple treatments
        per microsegments supplied, treatment is chosen randomly from supplied with
        "no_treatment" option added for tracking test and learn.
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

    def _add_random_column_from_values(tab, values, colname):
        """ Adds a new column to DataFrame which consists of randomly picked elements
            of values"""
        n = len(values)
        # setup when statement
        tab = tab.withColumn("val_ind", func.floor(func.rand() * n))
        whens = [func.when(func.col("val_ind") == i, values[i]) for i in range(0, n)]
        whens = functools.reduce(lambda x1, x2: x1.x2, whens)

        # choose values
        tab = tab.withColumn(colname, whens)
        tab = tab.drop("val_ind")

        return tab

    def _pick_treatments_for_microsegment(microsegment_chosen):
        """ Picks treatments for target users using treatment dictionary"""
        chosen_microsegment_treatments = df_to_list(
            treatment_dictionary.filter(
                f"microsegment == '{microsegment_chosen}'"
            ).select("campaign_code")
        )
        target_users_in_microsegment = target_users.filter(
            f"microsegment == '{microsegment_chosen}'"
        )
        if len(chosen_microsegment_treatments) == 1:
            target_users_in_microsegment = target_users_in_microsegment.withColumn(
                "campaign_code", func.lit(chosen_microsegment_treatments[0])
            )
        else:
            target_users_in_microsegment = _add_random_column_from_values(
                target_users_in_microsegment,
                chosen_microsegment_treatments + ["no_treatment"],
                "campaign_code",
            )
        return target_users_in_microsegment

    microsegments = df_to_list(target_users.select("microsegment").distinct())
    target_users_with_treatments = functools.reduce(
        lambda df1, df2: df1.union(df2),
        map(_pick_treatments_for_microsegment, microsegments),
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
        treatments_propositions.withColumn("key_date", func.lit(today))
    )


def serve_treatments_chosen(treatments_history: DataFrame) -> pandas.DataFrame:
    """ Saves the csv with treatments basing on the recent entries in treatments
    history.

    Args:
        treatments_history: Table with history of treatments.
    """

    today = date.today().strftime("%Y-%m-%d")
    treatments_from_history = treatments_history.filter(f"key_date == '{today}'")
    if treatments_from_history.count() == 0:
        raise Exception(f"No treatments found for {today}")
    treatments_df = treatments_from_history.filter("campaign_code != 'no_treatment'")
    return treatments_df.toPandas()


def convert_treatments_dictionary_to_sparkdf(
    treatment_dictionary_pd: pandas.DataFrame,
) -> DataFrame:
    """ Creates spark DataFrame treatments dictionary.

    Args:
        treatment_dictionary_pd: Table of microsegment to treatment mapping in pandas.
    """

    schema = """
        subscription_identifier:string,
        use_case:string,
        macrosegment:string,
        microsegment:string,
        campaign_code:string
        """
    return get_spark_session().createDataFrame(treatment_dictionary_pd, schema=schema)
