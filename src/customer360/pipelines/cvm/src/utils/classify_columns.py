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
from typing import List, Dict, Any, Iterable

from customer360.pipelines.cvm.src.utils.list_operations import (
    list_sub,
    list_intersection,
)
from customer360.pipelines.cvm.src.utils.list_targets import list_targets


def list_categorical(df: DataFrame) -> List[str]:
    """ List categorical variables in a given DataFrame.

    Args:
        df: given DataFrame.

    Returns:
        List of categorical variables.
    """
    return [item[0] for item in df.dtypes if item[1].startswith("string")]


# noinspection PyDictCreation
def classify_columns(df: DataFrame, parameters: Dict[str, Any]) -> Dict[str, Iterable]:
    """ For every column in given DataFrame determines whether it is numerical /
    categorical etc column.

    Args:
        df: input DataFrame.
        parameters: parameters defined in parameters.yml.
    Returns:
        Dictionary with one element per category of column and list of columns of that
        category.
    """

    columns_cats = {}
    columns_cats["target"] = list_targets(parameters)
    columns_cats["key"] = parameters["key_columns"]
    columns_cats["segment"] = parameters["segment_columns"]
    columns_cats["categorical"] = list_sub(
        list_categorical(df), columns_cats["target"] + columns_cats["key"]
    )
    columns_cats["numerical"] = list_sub(
        df.columns,
        columns_cats["categorical"] + columns_cats["target"] + columns_cats["key"],
    )

    # make sure only columns are chosen
    for columns_cat in columns_cats:
        columns_cats[columns_cat] = list_intersection(
            columns_cats[columns_cat], df.columns
        )

    return columns_cats
