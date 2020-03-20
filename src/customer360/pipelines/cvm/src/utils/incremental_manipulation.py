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

from customer360.pipelines.cvm.src.utils.prepare_key_columns import prepare_key_columns


def get_latest_date(df: DataFrame, maximum_date: str = None) -> str:
    """ Returns last date available in given dataset.

    Args:
        df: Given DataFrame.
        maximum_date: later dates will be ignored, if None then latest date present
            is returned.
    """

    df = prepare_key_columns(df)
    if "key_date" not in df.columns:
        raise Exception("Date column not found.")

    if maximum_date is not None:
        df = df.filter("key_date <= '{}'".filter(maximum_date))

    return df.select("key_date").collect()[0][0]


def filter_latest_date(df: DataFrame) -> DataFrame:
    """ Filters given DataFrame, leaving only latest date entries.

    Args:
        df:  Input DataFrame.
    """

    df = prepare_key_columns(df)
    latest_date = get_latest_date(df)
    return df.filter("key_date == '{}'".format(latest_date))
