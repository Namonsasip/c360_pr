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


def get_ard_targets(
    users: DataFrame, reve: DataFrame, target_parameters: Dict[str, Any]
) -> DataFrame:
    """ Create table with one ARPU drop target.

    Args:
        users: Table with users and dates to create targets for.
        reve: Table with revenue stats.
        target_parameters: parameters for given target.

    Returns:
        Table with single ARPU drop target.
    """

    length = target_parameters["length"]
    drop = target_parameters["drop"]
    target_colname = target_parameters["colname"]

    if length == 1:
        arpu_col = "sum_rev_arpu_total_revenue_monthly_last_month"
    elif length == 3:
        arpu_col = "sum_rev_arpu_total_revenue_monthly_last_three_month"
    else:
        raise Exception("No implementation for length = {0}".format(length))

    # Setup table with dates and arpu
    cols_to_pick = ["subscription_identifier", "key_date", arpu_col]
    reve = reve.withColumnRenamed("start_of_month", "key_date")
    reve_arpu_only = reve.select(cols_to_pick)
    reve_arpu_only = reve_arpu_only.withColumnRenamed(arpu_col, "reve")
    # Pick only interesting users
    reve_arpu_only = users.join(
        reve_arpu_only, ["subscription_identifier", "key_date"], "left"
    )

    # Setup reve before and after
    reve_arpu_before_after = reve_arpu_only.withColumnRenamed(
        "key_date", "key_date_before"
    )
    reve_arpu_before_after = reve_arpu_before_after.withColumn(
        "key_date", func.add_months(reve_arpu_before_after.key_date_before, length)
    )
    reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
        "reve", "reve_before"
    )

    reve_arpu_before_after = reve_arpu_before_after.join(
        reve_arpu_only, ["key_date", "subscription_identifier"]
    )

    reve_arpu_before_after = reve_arpu_before_after.drop("key_date")
    reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
        "reve", "reve_after"
    )

    # Add the label
    reve_arpu_before_after = reve_arpu_before_after.withColumn(
        "reve_after_perc",
        reve_arpu_before_after.reve_after / reve_arpu_before_after.reve_before,
    )
    target_col = func.when(
        1 - reve_arpu_before_after.reve_after_perc >= drop, "drop"
    ).otherwise("no_drop")
    reve_arpu_before_after = reve_arpu_before_after.withColumn("target", target_col)
    reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
        "key_date_before", "key_date"
    )

    cols_to_select = ["target", "key_date", "subscription_identifier"]
    reve_arpu_before_after = reve_arpu_before_after.select(cols_to_select)

    reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
        "target", target_colname
    )

    return reve_arpu_before_after
