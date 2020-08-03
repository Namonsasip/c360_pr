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
from cvm.src.report.kpis_build import (
    add_arpus,
    add_inactivity,
    add_l0_arpu,
    add_network_churn,
    add_prepaid_no_activity_daily,
    add_status,
    create_contacts_table,
    prepare_users_dates,
)
from cvm.src.utils.utils import get_today
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as func


def prepare_users(
    customer_groups: DataFrame, sub_id_mapping: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:

    """ Creates users table for reporting purposes.

    Args:
        sub_id_mapping: mapping old to new sub id.
        customer_groups: Table with target, control and bau groups.
        parameters: parameters defined in parameters.yml.
    """
    today = get_today(parameters)
    df = (
        customer_groups.filter(
            "target_group in ('TG_2020_CVM_V2', 'CG_2020_CVM_V2', 'BAU_2020_CVM_V2', "
            "'TG', 'CG', 'BAU')"
        )
        .select(["crm_sub_id", "target_group", "analytic_id", "register_date"])
        .distinct()
        .withColumn("key_date", func.lit(today))
        .withColumnRenamed("crm_sub_id", "old_subscription_identifier")
        .join(sub_id_mapping, on="old_subscription_identifier")
    )
    return df


def build_daily_kpis(
    users_report: DataFrame,
    microsegments: DataFrame,
    network_churn: DataFrame,
    reve: DataFrame,
    profile_table: DataFrame,
    usage: DataFrame,
    parameters: Dict[str, Any],
    prepaid_no_activity_daily: DataFrame,
    l0_arpu: DataFrame,
) -> DataFrame:
    """ Build daily kpis table.

    Args:
        l0_arpu: arpu data from greenplum.
        prepaid_no_activity_daily: cloud table with lack of activity for prepaid.
        network_churn: table with users that churned, not from C360.
        microsegments: table with macrosegments and microsegments.
        parameters: parameters defined in parameters.yml.
        reve: table with monthly revenue. Assumes using l3 profile table.
        users_report: table with users to create report for.
        profile_table: table with subscriber statuses.
        usage: table with last activity date.
    """
    logging.info("Building daily kpis")
    report_parameters = parameters["build_report"]
    if "key_date" in users_report.columns:
        users_report = users_report.drop("key_date")
    df = prepare_users_dates(users_report, parameters)
    df = add_arpus(df, reve, report_parameters["min_date"])
    df = add_prepaid_no_activity_daily(
        prepaid_no_activity_daily, df, report_parameters["min_date"]
    )
    df = add_status(df, profile_table)
    df = df.join(microsegments, on="subscription_identifier", how="left")
    df = add_network_churn(network_churn, df)
    df = add_l0_arpu(df, l0_arpu)
    inactivity_lengths = report_parameters["inactivity_lengths"]
    for inactivity_length in inactivity_lengths:
        df = add_inactivity(df, usage, inactivity_length, report_parameters["min_date"])
    return df


def build_contacts_table(
    campaign_codes_to_groups_mapping: pandas.DataFrame,
    campaign_tracking_contact_list_pre: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Create table with all campaign contacts and responses for ard, churn, bau
    campaigns between defined dates.

    Args:
        campaign_codes_to_groups_mapping: table with campaign codes and tags.
        campaign_tracking_contact_list_pre: table with contacts and responses.
        parameters: parameters defined in parameters.yml.
    """
    min_date = parameters["build_report"]["min_date"]
    max_date = parameters["build_report"]["max_date"]
    v2_switch_date = parameters["build_report"]["v2_switch_date"]
    logging.getLogger(__name__).info(
        "Creating contacts from {} to {}".format(min_date, max_date)
    )
    return create_contacts_table(
        campaign_codes_to_groups_mapping,
        campaign_tracking_contact_list_pre,
        min_date,
        max_date,
        v2_switch_date,
    )


def create_contact_kpis(
    daily_kpis: DataFrame,
    contacts: DataFrame,
    users_report: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Calculates kpis that involve contact data, so statistics like arpu uplift after
    contact, take rate, number of customers targeted etc.

    Args:
        daily_kpis: table with kpis values for each day.
        contacts: table with contacts, responses and campaign code tag.
        users_report: list of users included in report with multiple ids.
        parameters: parameters defined in parameters.yml.

    """
    log = logging.getLogger(__name__)
    report_parameters = parameters["build_report"]
    # add not targeted people to contacts table
    log.info("Adding not contacted users")
    not_contacted_base_date = report_parameters["not_contacted_users_base_date"]
    not_contacted_users = users_report.join(
        contacts, on="old_subscription_identifier", how="left_anti"
    )
    contacts_extension = (
        not_contacted_users.select("old_subscription_identifier")
        .distinct()
        .withColumn("contact_date", func.lit(not_contacted_base_date))
        .withColumn("response", func.lit("not_contacted"))
        .withColumn("campaign_code", func.lit("not_contacted"))
        .withColumn("campaign_code_tag", func.lit("not_contacted"))
    )
    contacts_extended = contacts.unionByName(contacts_extension).withColumnRenamed(
        "contact_date", "key_date"
    )
    # add kpis
    log.info("Creating contact kpis")
    contacts_with_kpis = (
        daily_kpis.join(
            contacts_extended,
            on=["old_subscription_identifier", "key_date"],
            how="full",
        )
        .withColumn("network_churn", func.col("terminated") - func.col("reactive"))
        .withColumn(
            "status_SA",
            func.when(func.col("subscription_status") == "SA", 1).otherwise(0),
        )
    )
    # calculate before - after kpis
    user_window = Window.partitionBy("subscription_identifier").orderBy("key_date")
    uplift_period = report_parameters["uplift_period"]
    before = user_window.rowsBetween(-uplift_period, -1)
    after = user_window.rowsBetween(1, uplift_period)
    kpis = report_parameters["kpis_to_summarize"]
    for kpi in kpis:
        contacts_with_kpis = contacts_with_kpis.withColumn(
            "{}_before".format(kpi), func.avg(func.col(kpi)).over(before)
        ).withColumn("{}_after".format(kpi), func.avg(func.col(kpi)).over(after))
    return contacts_with_kpis


def summarize_contact_kpis(
    contact_kpis: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """ Summarizes contact kpis to target group level.

    Args:
        contact_kpis: kpis related to campaign contact.
        parameters: parameters defined in parameters.yml.
    """
    log = logging.getLogger(__name__)
    # summarize
    log.info("Summarizing kpis")
    report_parameters = parameters["build_report"]
    kpis = report_parameters["kpis_to_summarize"]
    kpis_stats = ["{}_before".format(kpi) for kpi in kpis] + [
        "{}_after".format(kpi) for kpi in kpis
    ]
    campaign_sent = func.when(
        (~func.col("campaign_code").isNull())
        & (func.col("campaign_code") != "not_contacted"),
        1,
    ).otherwise(0)
    kpis_summarized = (
        contact_kpis.filter("campaign_code is not null")
        .groupby(["target_group_switched", "campaign_code_tag", "response"])
        .agg(
            func.countDistinct("subscription_identifier").alias(
                "distinct_sub_ids_number"
            ),
            *[func.avg(kpi_stat).alias(kpi_stat) for kpi_stat in kpis_stats],
            func.sum(campaign_sent).alias("campaigns_sent"),
        )
    )
    return kpis_summarized
