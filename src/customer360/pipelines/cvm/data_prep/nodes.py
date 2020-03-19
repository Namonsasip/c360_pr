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

from typing import Any, Dict, List
from pyspark.sql import DataFrame
import functools
import pyspark.sql.functions as func
from pyspark.sql import Window

from customer360.pipelines.cvm.src.targets.ard_targets import get_ard_targets
from customer360.pipelines.cvm.src.targets.churn_targets import (
    get_churn_targets,
    filter_usage,
)
from customer360.pipelines.cvm.src.utils.setup_names import setup_names


def create_l5_cvm_one_day_users_table(
    profile: DataFrame, main_packs: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """Create l5_cvm_one_day_users_table - one day table of users used for
    training and validating.

    Args:
        profile: monthly customer profiles.
        main_packs: pre-paid main packages description.
        parameters: parameters defined in parameters.yml.
    """

    date_chosen = parameters["chosen_date"]
    users = profile.filter("partition_month == '{}'".format(date_chosen))
    users = users.filter(
        "charge_type == 'Pre-paid' \
         AND subscription_status == 'SA' \
         AND subscription_identifier is not null \
         AND subscription_identifier not in ('null', 'NA') \
         AND cust_active_this_month = 'Y'"
    )
    users = users.filter("subscriber_tenure >= 4")

    main_packs = main_packs.filter(
        "promotion_group_tariff not in ('SIM 2 Fly', \
         'Net SIM', 'Traveller SIM')"
    )
    main_packs = main_packs.select("package_id").withColumnRenamed(
        "package_id", "current_package_id"
    )
    users = users.join(main_packs, ["current_package_id"], "inner")
    columns_to_pick = ["partition_month", "subscription_identifier"]
    users = users.select(columns_to_pick)
    users.withColumnRenamed("partition_month", "key_date")

    return users.distinct()


def add_ard_targets(
    users: DataFrame, reve: DataFrame, parameters: Dict[str, Any], chosen_date: str
) -> DataFrame:
    """ Create table with ARPU drop targets.

    Args:
        users: Table with users and dates to create targets for.
        reve: Table with revenue stats.
        parameters: parameters defined in parameters.yml.
        chosen_date: Date for which targets will be created.
    Returns:
        Table with ARD targets.
    """

    local_parameters = parameters["targets"]["ard"]
    users = setup_names(users)
    ard_target_tables = [
        get_ard_targets(users, reve, local_parameters[targets], chosen_date)
        for targets in local_parameters
    ]

    def join_targets(df1, df2):
        return df1.join(df2, ["key_date", "subscription_identifier"], "full")

    return functools.reduce(join_targets, ard_target_tables)


def add_churn_targets(
    users: DataFrame, usage: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """ Create table with churn targets.

    Args:
        users: Table with users and dates to create targets for.
        usage: Table with usage stats.
        parameters: parameters defined in parameters.yml.

    Returns:
        Table with churn targets.
    """

    local_parameters = parameters["targets"]["churn"]
    chosen_date = parameters["chosen_date"]

    users = setup_names(users)
    usage = setup_names(usage)
    usage = filter_usage(users, usage, parameters)
    churn_target_tables = [
        get_churn_targets(users, usage, local_parameters[targets], chosen_date)
        for targets in local_parameters
    ]

    def join_targets(df1, df2):
        return df1.join(df2, ["key_date", "subscription_identifier"], "full")

    return functools.reduce(join_targets, churn_target_tables)


def create_l5_cvm_one_day_train_test(
    targets_features: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """Adds train-test column to features-targets table. Train share defined in
    parameters.

    Args:
        targets_features: Table with features and targets.
        parameters: parameters defined in parameters.yml.

    Returns:
        targets_features table with extra column with train / test flag.
    """

    train_share = parameters["l5_cvm_one_day_train_test"]["train_share"]

    # add train test flag
    train_test = targets_features.withColumn(
        "train_test", func.when(func.rand() <= train_share, "train").otherwise("test")
    )
    train = train_test.filter("train_test == 'train'").drop("train_test")
    test = train_test.filter("train_test == 'test'").drop("train_test")

    return train, test


def subs_date_join_important_only(
    important_param: List[Any], parameters: Dict[str, Any], *args: DataFrame,
) -> DataFrame:
    """ Left join all tables with important variables by given keys.

    Args:
        important_param: List of important volumns.
        parameters: parameters defined in parameters.yml.
        *args: Tables to join.
    Returns:
        Left joined and filtered tables.
    """

    keys = parameters["key_columns"]
    segments = parameters["segment_columns"]
    tables = [setup_names(tab) for tab in args]

    def filter_column(df, filter_list):
        cols_to_drop = [
            col_name for col_name in df.columns if col_name not in filter_list
        ]
        return df.drop(*cols_to_drop)

    tables = [filter_column(tab, important_param + keys + segments) for tab in tables]

    def join_on(df1, df2):
        cols_to_drop = [col_name for col_name in df1.columns if col_name in df2.columns]
        cols_to_drop = list(set(cols_to_drop) - set(keys))
        df2 = df2.drop(*cols_to_drop)
        return df1.join(df2, keys, "left")

    return functools.reduce(join_on, tables)


def subs_date_join(parameters: Dict[str, Any], *args: DataFrame,) -> DataFrame:
    """ Left join all tables by given keys.

    Args:
        parameters: parameters defined in parameters.yml.
        *args: Tables to join.
    Returns:
        Left joined tables.
    """

    keys = parameters["key_columns"]
    tables = [setup_names(tab) for tab in args]

    def join_on(df1, df2):
        cols_to_drop = [col_name for col_name in df1.columns if col_name in df2.columns]
        cols_to_drop = list(set(cols_to_drop) - set(keys))
        df2 = df2.drop(*cols_to_drop)
        return df1.join(df2, keys, "left")

    return functools.reduce(join_on, tables)


def create_sample_dataset(df: DataFrame, subscription_id_suffix: str) -> DataFrame:
    """ Create dev sample of given table. Dev sample is super small sample. Takes only
    users with certain subscription_identifier suffix.

    Args:
        subscription_id_suffix: suffix to filter subscription_identifier with.
        df: given table.
    Returns:
        Dev sample of table.
    """

    suffix_length = len(subscription_id_suffix)
    df = df.withColumn(
        "subscription_identifier_last_letter",
        df.subscription_identifier.substr(-suffix_length, suffix_length),
    )
    subs_filter = "subscription_identifier_last_letter == '{}'".format(
        subscription_id_suffix
    )

    df = df.filter(subs_filter).drop("subscription_identifier_last_letter")

    return df


def add_macrosegments(df: DataFrame,) -> DataFrame:
    """ Add macrosegments columns.

    Args:
        df: DataFrame with all features.
    Returns:
        Input DataFrame with extra column marking macrosegment.
    """

    df = df.withColumn(
        "ard_macrosegment",
        func.when(
            (func.col("sum_rev_arpu_total_revenue_monthly_last_month") < 50)
            & (func.col("subscriber_tenure") >= 12),
            "low_arpu_high_tenure",
        )
        .when(
            (func.col("sum_rev_arpu_total_revenue_monthly_last_month") >= 50)
            & (func.col("subscriber_tenure") < 12),
            "high_arpu_low_tenure",
        )
        .when(
            (func.col("sum_rev_arpu_total_revenue_monthly_last_month") >= 50)
            & (func.col("subscriber_tenure") >= 12),
            "high_arpu_high_tenure",
        )
        .otherwise("low_arpu_low_tenure"),
    )
    df = df.withColumn(
        "churn_macrosegment",
        func.when(
            func.col("sum_rev_arpu_total_revenue_monthly_last_month") > 0,
            "positive_arpu",
        ).otherwise("zero_arpu"),
    )
    return df


def add_volatility_scores(
    users: DataFrame, reve: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """Create volatility score for given set of users.

    Args:
        users: DataFrame with users, subscription_identifier column will be used.
        reve: Monthly revenue data.
        parameters: parameters defined in parameters.yml.
    """

    vol_length = parameters["volatility_length"]

    reve = setup_names(reve)
    users = setup_names(users)

    reve_cols_to_pick = parameters["key_columns"] + "rev_arpu_total_revenue"
    reve = reve.select(reve_cols_to_pick)
    reve_users_window = Window.partitionBy("subscription_identifier")
    reve = (
        reve.withColumn("reve_history", func.count().over(reve_users_window))
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
        func.stddev("rev_arpu_total_revenue").alias("volatility")
    )

    users = users.select("subscription_identifier").join(
        volatility, "subscription_identifier", "left"
    )

    return users
