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

from cvm.src.utils.list_operations import list_intersection


def prepare_key_columns(df: DataFrame,) -> DataFrame:
    """ Prepare key columns - setup names and modify if necessary.

    Args:
        df: given DataFrame.
    Returns:
        DataFrame with changed column names.
    """

    key_date_columns = [
        "partition_month",
        "event_partition_date",
        "start_of_month",
        "start_of_week",
    ]

    if len(list_intersection(df.columns, key_date_columns)) > 1:
        raise Exception("More then one date column found.")

    if "start_of_month" in df.columns:
        df = df.withColumn("start_of_month", func.add_months(df.start_of_month, 1))

    if "start_of_week" in df.columns:
        df = df.withColumn("start_of_week", func.add_weeks(df.start_of_week, 1))

    for key_date_column in key_date_columns:
        df = df.withColumnRenamed(key_date_column, "key_date")

    df = df.withColumnRenamed(
        "max_usg_last_action_date_daily_last_ninety_day", "last_activity_date"
    )

    return df
