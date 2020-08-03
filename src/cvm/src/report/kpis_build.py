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

import pandas
import pyspark.sql.functions as func
from cvm.src.report.report_features import (
    add_prepaid_sub_id,
    get_all_dates_between,
    timestamp_to_date,
)
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.utils import drop_duplicated_columns
from pyspark.sql import DataFrame, Window

from src.customer360.utilities.spark_util import get_spark_session


def add_l0_arpu(df: DataFrame, l0_arpu: DataFrame) -> DataFrame:
    """ Add arpu based on l0 greenplum data.

    Args:
        df: table to join arpu to.
        l0_arpu: l0 greenplum arpu data.
    """
    logging.getLogger(__name__).info("Adding l0 arpu")
    reve_col = "total_net_tariff_revenue"
    arpu = (
        add_prepaid_sub_id(l0_arpu)
        .withColumn("key_date", timestamp_to_date("day_id"))
        .select(["subscription_identifier", "key_date", reve_col])
        .withColumnRenamed(reve_col, "total_net_revenue_tariff_revenue_from_l0")
    )
    return df.join(arpu, on=["subscription_identifier", "key_date"], how="left")


def prepare_users_dates(
    users_report: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """ Prepare list of users and dates to populate.

    Args:
        users_report: list of users.
        parameters: parameters defined in parameters.yml.
    """
    min_date = parameters["build_report"]["min_date"]
    max_date = parameters["build_report"]["max_date"]
    dates = get_all_dates_between(min_date, max_date)
    return users_report.crossJoin(dates)


def add_arpus(df: DataFrame, reve: DataFrame, min_date: str,) -> DataFrame:
    """ Adds a column with daily revenues for given time range. Imputes the data with
        0 if needed.

    Args:
        df: table with users to create report for.
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
    reve = prepare_key_columns(reve).select(key_columns + reve_to_pick)
    filling_dict = {reve: 0 for reve in reve_to_pick}
    return df.join(reve, on=key_columns, how="left").fillna(filling_dict)


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
        drop_duplicated_columns(users_dates.join(usage, on=key_columns, how="left"))
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


def create_contacts_table(
    campaign_codes_to_groups_mapping: pandas.DataFrame,
    campaign_tracking_contact_list_pre: DataFrame,
    min_date: str,
    max_date: str,
    v2_switch_date: str,
) -> DataFrame:
    """ Creates table listing ard, churn, bau campaign contacts and responses between
    given dates.

    Args:
        v2_switch_date: when TG/CG groups were switched to V2.
        campaign_codes_to_groups_mapping: table with campaign codes and tags.
        campaign_tracking_contact_list_pre: table with contacts and responses.
        min_date: minimum date.
        max_date: maximum date.

    """
    # prepare campaign_codes_to_groups_mapping
    campaign_codes_mapping = (
        get_spark_session()
        .createDataFrame(campaign_codes_to_groups_mapping)
        .withColumnRenamed("CHILD_CODE", "campaign_code")
        .withColumnRenamed("TAG", "campaign_code_tag")
    )

    # nullify treatment groups before/after the change date
    after_switch = func.col("key_date") >= v2_switch_date
    old_target_group = func.col("target_group").isin(["BAU", "CG", "TG"])
    new_target_group = func.col("target_group").isin(
        ["BAU_2020_CVM_V2", "CG_2020_CVM_V2", "TG_2020_CVM_V2"]
    )
    target_group_switched = (
        func.when(after_switch & new_target_group, func.col("target_group"))
        .when(~after_switch & old_target_group, func.col("target_group"))
        .otherwise(func.lit(None))
    )
    # prepare campaign_tracking_contact_list_pre
    contacts = (
        # take care of dates
        campaign_tracking_contact_list_pre.withColumn(
            "contact_date", func.date_format(func.col("contact_date"), "yyyy-MM-dd")
        )
        .filter("contact_date >= '{}'".format(min_date))
        .filter("contact_date <= '{}'".format(max_date))
        # take care of campaign codes
        .withColumnRenamed("campaign_child_code", "campaign_code")
        .join(campaign_codes_mapping, on="campaign_code")
        # take care of sub id
        .withColumnRenamed("subscription_identifier", "old_subscription_identifier")
        # add switched target group
        .withColumn("target_group_switched", target_group_switched)
        # limit columns
        .select(
            [
                "contact_date",
                "response",
                "campaign_code",
                "campaign_code_tag",
                "old_subscription_identifier",
            ]
        )
    )

    return contacts


def get_most_recent_micro_macrosegment(microsegments_history: DataFrame) -> DataFrame:
    """ Return microsegments and macrosegments for every user found in
    `microsegments_history`. If more then one returns the most recent one.

    Args:
        microsegments_history: table with microsegments and macrosegments assignment
        history.
    """
    time_order = Window.partitionBy("subscription_identifier").orderBy("key_date")
    return (
        microsegments_history.withColumn("rn", func.row_number().over(time_order))
        .filter("rn == 1")
        .drop("rn")
    )
