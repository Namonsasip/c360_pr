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
from typing import Dict, Any


def create_dev_version(df: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Create dev sample of given table. Dev sample is super small sample.

    Args:
        parameters: parameters defined in parameters.yml.
        df: given table.
    Returns:
        Dev sample of table.
    """

    chosen_date = parameters["dev_parameters"]["chosen_date"]
    subscription_id_suffix = parameters["dev_parameters"]["subscription_id_suffix"]

    dates_cols = ["partition_month", "event_partition_date", "start_of_month"]
    dates_intersect = set(dates_cols) & set(df.columns)
    dates_col = dates_intersect.pop()

    df = df.withColumn(
        "subscription_identifier_last_letter", df.subscription_identifier.substr(-2, 2)
    )
    subs_filter = "subscription_identifier_last_letter == '{}'".format(
        subscription_id_suffix
    )
    dates_filter = "{} == '{}'".format(dates_col, chosen_date)

    df = (
        df.filter(subs_filter)
        .filter(dates_filter)
        .drop("subscription_identifier_last_letter")
    )

    return df
