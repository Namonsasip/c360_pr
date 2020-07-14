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

from cvm.src.report.kpis_build import (
    add_arpus,
    add_inactivity,
    add_network_churn,
    add_status,
)
from cvm.src.utils.utils import get_today
from pyspark.sql import DataFrame
from pyspark.sql import functions as func

from src.cvm.src.report.kpis_build import add_prepaid_no_activity_daily


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
) -> DataFrame:
    """ Build daily kpis table.

    Args:
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
    df = add_arpus(users_report, reve, report_parameters["min_date"])
    df = add_prepaid_no_activity_daily(
        prepaid_no_activity_daily, df, report_parameters["min_date"]
    )
    df = add_status(df, profile_table)
    df = df.join(microsegments, on="subscription_identifier")
    df = add_network_churn(network_churn, df)
    inactivity_lengths = report_parameters["inactivity_lengths"]
    for inactivity_length in inactivity_lengths:
        df = add_inactivity(df, usage, inactivity_length, report_parameters["min_date"])
    return df
