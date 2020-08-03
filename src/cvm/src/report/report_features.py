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
import pandas
import pyspark.sql.functions as func
from customer360.utilities.spark_util import get_spark_session
from pyspark.sql import DataFrame


def timestamp_to_date(column_name: str, date_format: str = "yyyy-MM-dd"):
    """ Transforms timestamp to date of used format.

    Args:
        column_name: column to transform.
        date_format: output date format.
    """
    return func.date_format(func.col(column_name), date_format)


def add_prepaid_sub_id(
    df: DataFrame,
    access_method_column_name: str = "access_method_num",
    register_date_column_name: str = "register_date",
) -> DataFrame:
    """ Creates prepaid subscription identifier out of provided columns.

    Args:
        df: table to modify.
        access_method_column_name: name of column with access method num.
        register_date_column_name: name of column with register date.
    """
    date_formatted = timestamp_to_date(register_date_column_name, "yyyyMMdd")
    access_method_num = func.col(access_method_column_name)
    prepaid_sub_id = func.concat(access_method_num, func.lit("-"), date_formatted)
    return df.withColumn("subscription_identifier", prepaid_sub_id)


def get_all_dates_between(min_date: str, max_date: str) -> DataFrame:
    """ Get one column DataFrame with all dates in between dates supplied.

    Args:
        min_date: the earlier date.
        max_date: the later date.
    """
    pd_date_range = pandas.date_range(min_date, max_date)
    pd_data_frame = pandas.DataFrame({"key_date": pd_date_range})
    return (
        get_spark_session()
        .createDataFrame(pd_data_frame)
        .withColumn("key_date", timestamp_to_date("key_date"))
    )
