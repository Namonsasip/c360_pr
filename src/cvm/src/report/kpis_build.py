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

import pyspark.sql.functions as func
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from pyspark.sql import DataFrame


def add_arpus(users_report: DataFrame, reve: DataFrame, min_date: str,) -> DataFrame:
    """ Adds a column with daily revenues for given time range. Imputes the data with
        0 if needed.

    Args:
        users_report: table with users to create report for.
        reve: table with daily arpus.
        min_date: minimum date of report.
    """
    logging.info("Adding ARPUs")
    key_columns = [
        "subscription_identifier",
        "key_date",
    ]
    reve_to_pick = [
        "rev_arpu_total_net_rev",
        "rev_arpu_voice",
        "rev_arpu_data_rev",
    ]
    reve = (
        prepare_key_columns(reve)
        .select(key_columns + reve_to_pick)
        .filter("key_date >= '{}'".format(min_date))
    )
    filling_dict = {reve: 0 for reve in reve_to_pick}
    return users_report.join(reve, on="subscription_identifier", how="left").fillna(
        filling_dict
    )


def add_status(users_dates: DataFrame, profile_table: DataFrame,) -> DataFrame:
    """ Adds a column with subscriber status.

    Args:
        users_dates: table with users and dates to create report for.
        profile_table: table with statuses for given users and dates.
    """

    logging.info("Adding subscription status")
    profile_table = prepare_key_columns(profile_table)
    key_columns = ["subscription_identifier", "key_date"]
    cols_to_pick = ["subscription_status"]
    profile_table = prepare_key_columns(profile_table).select(
        key_columns + cols_to_pick
    )
    return users_dates.join(profile_table, on=key_columns, how="left")


def add_inactivity(
    users_dates: DataFrame, usage: DataFrame, inactivity_length: int, min_date: str
) -> DataFrame:
    """ Adds a column with flag indicating inactivity basing on usage.

    Args:
        min_date: minimum date of report.
        users_dates: table with users and dates to create report for.
        usage: table with last activity date.
        inactivity_length: days of inactivity to be considered inactive.
    """
    key_columns = ["subscription_identifier", "key_date"]
    last_action_date_col = "last_activity_date"
    new_col_name = "inactive_{}_days".format(inactivity_length)
    logging.info("Adding {}".format(new_col_name))

    last_action_date_is_null = func.col(last_action_date_col).isNull()
    activity_not_recent = func.col(last_action_date_col) < func.date_add(
        func.col("key_date"), -inactivity_length
    )
    new_col_when = func.when(
        last_action_date_is_null | activity_not_recent, 1
    ).otherwise(0)

    usage = (
        prepare_key_columns(usage)
        .select(key_columns + [last_action_date_col])
        .filter("key_date >= '{}'".format(min_date))
    )
    return (
        users_dates.join(usage, on=key_columns, how="left")
        .withColumn(new_col_name, new_col_when)
        .withColumn(
            "last_activity_date_is_null",
            func.when(last_action_date_is_null, 1).otherwise(0),
        )
        .drop(last_action_date_col)
    )


def add_network_churn(network_churn: DataFrame, users: DataFrame) -> DataFrame:
    """ Adds a column with churn defined by network churn table.

    Args:
        network_churn: table with users that churned, not from C360.
        users: table with users.
    """
    logging.getLogger(__name__).info("Adding network based churn")
    network_churn = (
        network_churn.withColumn(
            "key_date", func.date_format(func.col("day_code"), "yyyy-MM-dd")
        )
        .withColumn(
            "subscription_identifier",
            func.concat(
                func.col("access_method_num"),
                func.lit("-"),
                func.date_format(func.col("register_date"), "yyyyMMdd"),
            ),
        )
        .select(["churn_type", "subscription_identifier", "key_date"])
    )
    churns = (
        network_churn.filter("churn_type in ('Terminate', 'CT')")
        .drop("churn_type")
        .withColumn("terminated", func.lit(1))
    )
    reactivates = (
        network_churn.filter("churn_type == 'Reactive'")
        .drop("churn_type")
        .withColumn("reactive", func.lit(1))
    )
    users = (
        users.join(churns, on=["subscription_identifier", "key_date"], how="left")
        .withColumn("terminated", func.coalesce(func.col("terminated"), func.lit(0)))
        .join(reactivates, on=["subscription_identifier", "key_date"], how="left")
        .withColumn("reactive", func.coalesce(func.col("reactive"), func.lit(0)))
    )
    return users


def add_prepaid_no_activity_daily(
    prepaid_no_activity_daily: DataFrame, users: DataFrame, min_date: str
) -> DataFrame:
    """ Adds lack of activity columns based on `prepaid_no_activity_daily` table.

    Args:
        prepaid_no_activity_daily: table with daily markers for lack of activity for
            prepaid users.
        users: table to extend with no activity columns.
        min_date: minimal date of report.
    """
    cols_to_pick = [
        "in_no_activity_n_days",
        "no_activity_n_days",
        "out_no_activity_n_days",
        "analytic_id",
        "register_date",
        "date_id",
    ]

    def is_positive(col_name):
        """ Return 1 if `col_name` is positive, 0 otherwise."""
        return func.when(func.col(col_name) > 0, 1).otherwise(0)

    no_activity = (
        prepaid_no_activity_daily.select(cols_to_pick)
        .withColumnRenamed("date_id", "key_date")
        .filter("key_date >= '{}'".format(min_date))
        .withColumn("prepaid_no_activity", is_positive("no_activity_n_days"))
        .withColumn("prepaid_no_activity_in", is_positive("in_no_activity_n_days"))
        .withColumn("prepaid_no_activity_out", is_positive("out_no_activity_n_days"))
    )
    return users.join(
        no_activity, on=["analytic_id", "register_date", "key_date"], how="left"
    )
