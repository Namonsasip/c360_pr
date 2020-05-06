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
    reve_to_pick = ["subscription_identifier", "key_date", "rev_arpu_total_net_rev"]
    reve = (
        prepare_key_columns(reve)
        .select(reve_to_pick)
        .filter("key_date >= '{}'".format(min_date))
    )
    return users_report.join(reve, on="subscription_identifier", how="left").fillna(
        {"rev_arpu_total_net_rev": 0}
    )


def add_status(users_dates: DataFrame, profile_table: DataFrame,) -> DataFrame:
    """ Adds a column with subscriber status.

    Args:
        users_dates: table with users and dates to create report for.
        profile_table: table with statuses for given users and dates.
    """

    key_columns = ["subscription_identifier", "key_date"]
    cols_to_pick = ["subscription_status"]
    profile_table = prepare_key_columns(profile_table).select(
        key_columns + cols_to_pick
    )
    return users_dates.join(profile_table, on=key_columns, how="left")


def add_inactivity(
    users_dates: DataFrame, usage: DataFrame, inactivity_length: int
) -> DataFrame:
    """ Adds a column with flag indicating inactivity basing on usage.

    Args:
        users_dates: table with users and dates to create report for.
        usage: table with last activity date.
        inactivity_length: days of inactivity to be considered inactive.
    """
    key_columns = ["subscription_identifier", "key_date"]
    last_action_date_col = "max_usg_last_action_date_daily_last_ninety_day"
    new_col_name = "inactive_{}_days".format(inactivity_length)

    last_action_date_is_null = func.col(last_action_date_col).isNull()
    activity_not_recent = func.col(last_action_date_col) < func.date_add(
        func.col("key_date"), -inactivity_length
    )
    new_col_when = func.when(
        last_action_date_is_null | activity_not_recent, 0
    ).otherwise(1)

    usage = prepare_key_columns(usage).select(key_columns + [last_action_date_col])
    return (
        users_dates.join(usage, on=key_columns, how="left")
        .withColumn(new_col_name, new_col_when)
        .drop(last_action_date_col)
    )
