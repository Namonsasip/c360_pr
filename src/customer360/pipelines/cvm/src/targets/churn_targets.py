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


from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from typing import Any, Dict


def get_churn_targets(
        users: DataFrame,
        usage: DataFrame,
        target_parameters: Dict[str, Any]
) -> DataFrame:
    """ Create table with one churn target.

    Args:
        users: Table with users and dates to create targets for.
        usage: Table with usage stats.
        target_parameters: parameters for given target.

    Returns:
        Table with single churn target.
    """

    colname = target_parameters["colname"]
    inactivity_length = target_parameters["inactivity_length"]
    blindspot = target_parameters["blindspot"]

    usage_cols = [
        "event_partition_date",
        "subscription_identifier",
        "max_usg_last_action_date_daily_last_ninety_day",
    ]
    usage = usage.select(usage_cols)

    usage = usage.withColumnRenamed("event_partition_date", "target_date")
    usage = usage.withColumnRenamed(
        "max_usg_last_action_date_daily_last_ninety_day", "last_activity_date")
    users = users.withColumnRenamed("partition_month", "key_date")

    # key to join by
    usage = usage.withColumn(
        "key_date",
        func.date_sub(usage.target_date, blindspot + inactivity_length)
    )

    # setup activity flag
    usage = usage.withColumn(
        "inactivity_start",
        func.date_sub(usage.target_date, inactivity_length)
    )

    # setup flag
    usage = usage.withColumn(
        colname,
        func.when(usage.last_activity_date.isNull(), "churn")
            .when(usage.inactivity_start > usage.last_activity_date, "churn")
            .otherwise("no_churn")
    )
    to_select = [colname, "key_date", "subscription_identifier"]
    usage = usage.select(to_select)

    users_churn = users.join(
        usage,
        ["key_date", "subscription_identifier"],
        "left"
    )

    return users_churn


def get_min_max_churn_horizon(
        target_parameters: Dict[str, Any],
) -> int:

    horizons = [target["inactivity_length"] + target["blindspot"] for target in
                target_parameters]
    return min(horizons), max(horizons)
