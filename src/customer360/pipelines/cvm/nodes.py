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

from customer360.pipelines.cvm.src.targets.ard_targets import get_ard_targets
from customer360.pipelines.cvm.src.targets.churn_targets import \
    get_churn_targets
from customer360.pipelines.cvm.src import setup_names


def create_l5_cvm_one_day_users_table(
        profile: DataFrame,
        main_packs: DataFrame,
        parameters: Dict[str, Any]
) -> DataFrame:
    """Create l5_cvm_one_day_users_table - one day table of users used for training and
    validating.

    Args:
        profile: monthly customer profiles.
        main_packs: pre-paid main packages description.
        parameters: parameters defined in parameters.yml.
    """

    date_chosen = parameters["l5_cvm_one_day_users_table"]["date_chosen"]
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
         'Net SIM', 'Traveller SIM')")
    main_packs = main_packs.select('package_id'). \
        withColumnRenamed('package_id', 'current_package_id')
    users = users.join(main_packs, ['current_package_id'], 'inner')
    columns_to_pick = ['partition_month', 'subscription_identifier']
    users = users.select(columns_to_pick)
    users.withColumnRenamed("partition_month", "key_date")

    return users


def create_l5_cvm_users_sample_table(
        users: DataFrame
) -> DataFrame:
    """Sample long term users to create development sample. Users with at least
    5 months of activity and subscription_identifier ending with 'A' are chosen.

    Args:
        users: Monthly user table.

    Returns:
        Table with subscription_identifiers.
    """

    users_months_count = users.groupby("subscription_identifier").count()
    long_term_users = users_months_count.filter("count == 5").select(
        "subscription_identifier").distinct()
    long_term_users = long_term_users.withColumn(
        "subscription_identifier_last_letter",
        long_term_users.subscription_identifier.substr(-1, 1))
    long_term_users = long_term_users.filter(
        "subscription_identifier_last_letter == 'A'")
    long_term_users = long_term_users.select("subscription_identifier")
    users = long_term_users.join(users, ["subscription_identifier"])

    return users


def add_ard_targets(
        users: DataFrame,
        reve: DataFrame,
        parameters: Dict[str, Any]
) -> DataFrame:
    """ Create table with ARPU drop targets.

    Args:
        users: Table with users and dates to create targets for.
        reve: Table with revenue stats.
        parameters: parameters defined in parameters.yml.
    Returns:
        Table with ARD targets.
    """

    local_parameters = parameters["ard_targets"]["targets"]
    users = setup_names(users)
    ard_target_tables = [get_ard_targets(users, reve, local_parameters[targets])
                         for targets in local_parameters]

    def join_targets(df1, df2):
        return df1.join(
            df2,
            ["key_date", "subscription_identifier"],
            "full"
        )

    return functools.reduce(join_targets, ard_target_tables)


def add_churn_targets(
        users: DataFrame,
        usage: DataFrame,
        parameters: Dict[str, Any]
) -> DataFrame:
    """ Create table with churn targets.

    Args:
        users: Table with users and dates to create targets for.
        usage: Table with usage stats.
        parameters: parameters defined in parameters.yml.

    Returns:
        Table with churn targets.
    """

    local_parameters = parameters["churn_targets"]["targets"]
    users = setup_names(users)
    ard_target_tables = [get_churn_targets(users, usage,
                                           local_parameters[targets])
                         for targets in local_parameters]

    def join_targets(df1, df2):
        return df1.join(
            df2,
            ["key_date", "subscription_identifier"],
            "full"
        )

    return functools.reduce(join_targets, ard_target_tables)


def create_l5_cvm_one_day_train_test(
        targets_features: DataFrame,
        parameters: Dict[str, Any],
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
        "train_test",
        func.when(func.rand() <= train_share, "train").otherwise("test")
    )

    return train_test


def create_l5_cvm_features_one_day_joined(
        users: DataFrame,
        *args: DataFrame
) -> DataFrame:
    """ Creates table with one_day features for given users.

    Args:
        users: Table with users and dates to join features for.
        *args: Tables with features.

    Returns:
        Table with one_day features for given users.
    """

    feature_tables = args

    # for every table make sure the month column is called 'start_of_month'
    cols_to_be_renamed = "partition_month"
    rename = lambda df: df.withColumnRenamed(cols_to_be_renamed,
                                             "key_date")
    feature_tables = [rename(feature_table) for feature_table in feature_tables]
    users = rename(users)

    # join the tables
    keys = ["key_date", "subscription_identifier"]
    join_on = lambda df1, df2: df1.join(df2, keys, "left")
    features_joined = functools.reduce(join_on, feature_tables, users)

    return features_joined


def subs_date_join(
        *args: DataFrame,
) -> DataFrame:
    """ Left join all tables by given keys.

    Args:
        *args: Tables to join.
    Returns:
        Left joined tables.
    """

    keys = ["key_date", "subscription_identifier"]

    def join_on(df1, df2):
        return df1.join(df2, keys, "left")

    return functools.reduce(join_on, args)
