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
from typing import Any, Dict, List

from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.utils import get_today
from pyspark.sql import DataFrame
from pyspark.sql import functions as func


def get_latest_date(
    df: DataFrame, maximum_date: str = None, date_col_name: str = "key_date"
) -> str:
    """ Returns last date available in given dataset.

    Args:
        date_col_name: name of column with date.
        df: Given DataFrame.
        maximum_date: later dates will be ignored, if None then latest date present
            is returned.
    """

    df = prepare_key_columns(df)
    if date_col_name not in df.columns:
        raise Exception("Date column not found.")

    if maximum_date is not None and maximum_date not in ["today", ""]:
        df = df.filter("{} <= '{}'".format(date_col_name, maximum_date))
    max_date = df.select(date_col_name).agg(func.max(date_col_name)).collect()[0][0]
    return max_date


def filter_latest_date(
    df: DataFrame, parameters: Dict[str, Any], maximum_date: str = None
) -> DataFrame:
    """ Filters given DataFrame, leaving only latest date entries.

    Args:
        df:  Input DataFrame.
        maximum_date: later dates will be ignored, if None then latest date present
            is returned.
        parameters: parameters defined in parameters.yml.
    """

    log = logging.getLogger(__name__)

    df = prepare_key_columns(df)
    latest_date = get_latest_date(df, maximum_date)
    df = df.filter("key_date == '{}'".format(latest_date))

    log.info(
        "Filtering most recent entries, entries from {} found.".format(latest_date)
    )

    if maximum_date is None or maximum_date in ["today", ""]:
        today = get_today(parameters)
    else:
        today = maximum_date
    df = df.withColumn("key_date", func.lit(today))

    return df


def filter_users(df: DataFrame, subscription_id_hash_suffix: List[str]) -> DataFrame:
    """ Create dev sample of given table basing on users' subscription_identifiers.

    Args:
        subscription_id_hash_suffix: suffix to filter subscription_identifier with.
        df: given table.
    Returns:
        Sample of table.
    """

    log = logging.getLogger(__name__)

    if subscription_id_hash_suffix is None:
        return df
    if not isinstance(subscription_id_hash_suffix, list):
        subscription_id_hash_suffix = [subscription_id_hash_suffix]

    if "subscription_identifier" not in df.columns:
        raise Exception("Column subscription_identifier not found.")

    suffixes_in_quotes = [
        "'{}'".format(suffix) for suffix in subscription_id_hash_suffix
    ]
    suffixes_sql = "(" + ", ".join(suffixes_in_quotes) + ")"
    log.info(f"Filtering users, suffixes chosen: {suffixes_sql}")
    suffix_length = len(subscription_id_hash_suffix[0])
    df = (
        # add a hash
        df.withColumn(
            "sub_id_hash", func.sha2(func.col("subscription_identifier"), 256)
        )
        # add a hash suffix
        .withColumn(
            "sub_id_hash_suffix",
            func.col("sub_id_hash").substr(-suffix_length, suffix_length),
        )
        # run the filter
        .filter("sub_id_hash_suffix in {}".format(suffixes_sql)).drop(
            *["sub_id_hash", "sub_id_hash_suffix"]
        )
    )

    return df
