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
from typing import Dict, Any

from pyspark.sql import DataFrame, functions as func, Window

from cvm.src.targets.churn_targets import add_days
from cvm.src.utils.prepare_key_columns import prepare_key_columns


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

    df = df.fillna(parameters["feature_default_values"])

    microsegment_defs = parameters["microsegments"]
    for use_case in microsegment_defs:
        df = add_microsegments(
            df, microsegment_defs[use_case], use_case + "_microsegment",
        )

    return df


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
        filter_str = f"{use_case_chosen}_macrosegment == {macrosegment}"
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
    for use_case in cutoffs:
        use_case_filter_str = _get_filter_for_use_case(use_case)
        dfs.append(
            propensities.filter(use_case_filter_str)
            .select("subscription_identifier")
            .withColumn("use_case", func.lit(use_case))
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
