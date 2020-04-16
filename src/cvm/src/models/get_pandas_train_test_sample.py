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

from typing import Any, Dict, Tuple

import pandas as pd

from cvm.src.utils.list_operations import list_intersection
from cvm.src.utils.list_targets import list_targets
from pyspark.sql import DataFrame


def get_pandas_train_test_sample(
    df: DataFrame,
    parameters: Dict[str, Any],
    target_chosen: str = None,
    use_case_chosen: str = None,
    macrosegments_chosen: str = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """ Setup pandas prediction / train sample for given pyspark DataFrame.

    Args:
        df: DataFrame to create sample from.
        parameters: parameters defined in parameters.yml.
        target_chosen: For creating training sample, column name to create sample for.
            None for prediction, does not include target column.
        use_case_chosen: chosen use case
        macrosegments_chosen: macrosegment chosen
    Returns:
        Pandas DataFrames that can be used for prediction / training.
    """

    if use_case_chosen is not None and macrosegments_chosen is not None:
        macrosegment_col = use_case_chosen + "_macrosegment"
        df = df.filter("{} == '{}'".format(macrosegment_col, macrosegments_chosen))

    if target_chosen is not None:
        df = df.filter("{} is not null".format(target_chosen))
        y = df.select(target_chosen).withColumnRenamed(target_chosen, "target")
        y = y.toPandas()
    else:
        y = None

    target_cols = list_targets(parameters)
    key_columns = parameters["key_columns"]
    segments_columns = parameters["segment_columns"]
    to_drop = list_intersection(
        df.columns, target_cols + key_columns + segments_columns + ["volatility"]
    )
    X = df.drop(*to_drop).toPandas()

    return X, y
