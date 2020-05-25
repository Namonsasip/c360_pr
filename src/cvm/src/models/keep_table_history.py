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
from typing import Any, Dict, Tuple

from cvm.src.targets.churn_targets import add_days
from cvm.src.utils.incremental_manipulation import get_latest_date
from cvm.src.utils.utils import get_today
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def pop_most_recent(
    history_df: DataFrame,
    update_df: DataFrame,
    recalculate_period_days: int,
    parameters: Dict[str, Any],
    today: str = None,
) -> Tuple[DataFrame, DataFrame]:
    """ Returns recent data if found in `history_df`. Recent data is defined as
    `recalculate_period_days` days from `today`. If no recent data then `update_df` is
    used to return and update history.

    Function allows for lazy evaluation of `update_df` only if needed and keeping track
    of history.

    Args:
        history_df: table with history, must contain `date_created` column.
        update_df: table (can be not materialized) with recent data.
        parameters: parameters defined in parameters.yml.
        recalculate_period_days: number of days after which data is recalculated.
        today: date for which return the data.
    Returns:
        Updated history and recent data.
    """

    if today is None:
        today = get_today(parameters)
    recent_date = add_days(today, -recalculate_period_days)
    history_before_today = history_df.filter(
        "date_created <= '{}'".format(recent_date, today)
    )
    most_recent_date_in_history = get_latest_date(
        history_before_today, date_col_name="date_created"
    )
    recent_history_found = most_recent_date_in_history >= recent_date
    if recent_history_found:
        logging.info("Using entry from {}".format(most_recent_date_in_history))
        return (
            history_before_today,
            history_before_today.filter(
                "date_created == '{}'".format(most_recent_date_in_history)
            ).drop("date_created"),
        )
    else:
        logging.info("No recent entry found, recalculating")
        history_updated = history_df.append(
            update_df.withColumn("date_created", lit(today))
        )
        return history_updated, update_df
