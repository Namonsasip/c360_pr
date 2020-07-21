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
import functools
from datetime import datetime
from typing import Any, Callable, Dict, List

import pytz

import pyspark.sql.functions as func
from customer360.utilities.spark_util import get_spark_session
from cvm.src.utils.list_targets import list_targets
from pyspark.sql import DataFrame, Window


def iterate_over_usecases_macrosegments_targets(
    fun: Callable, parameters: Dict[str, Any], add_global_macrosegment: bool = False
) -> object:
    """Iterates fun over every usecase, macrosegment, target.

    Args:
        fun: function to call, assumes to input usecase, macrosegment, target
        parameters: parameters defined in parameters.yml.
        add_global_macrosegment: if True then special macrosegment is added - `global`.
    """

    def _iter_for_macrosegment(use_case_chosen, macrosegment_chosen):
        fun_values = {}
        for target_chosen in target_cols[use_case_chosen]:
            fun_values[target_chosen] = fun(
                use_case_chosen, macrosegment_chosen, target_chosen
            )
        return fun_values

    def _iter_for_usecase(use_chosen):
        fun_values = {}
        all_macrosegments = list(macrosegments[use_chosen])
        if add_global_macrosegment:
            all_macrosegments += ["global"]
        for macrosegment_chosen in all_macrosegments:
            fun_values[macrosegment_chosen] = _iter_for_macrosegment(
                use_chosen, macrosegment_chosen
            )
        return fun_values

    target_cols = list_targets(parameters, case_split=True)
    macrosegments = parameters["macrosegments"]

    fun_vals = {}
    for use_case in parameters["targets"]:
        fun_vals[use_case] = _iter_for_usecase(use_case)

    return fun_vals


def get_clean_important_variables(
    important_param: List[Any], parameters: Dict[str, Any],
) -> List[Any]:
    """ Returns important variables from before preprocessing stage.

    Args:
        important_param: List of important columns.
        parameters: parameters defined in parameters.yml.
    """
    suffix_list = parameters["preprocessing_suffixes"]
    for suffix in suffix_list:
        important_param = [
            col.replace(suffix_list[suffix], "") for col in important_param
        ]
    return important_param


def impute_from_parameters(df: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Impute columns using dictionary defined in parameters.

    Args:
        df: Table to impute.
        parameters: parameters defined in parameters.yml.
    """
    default_values = parameters["feature_default_values"]
    default_values_to_apply = {
        col_name: default_values[col_name]
        for col_name in default_values
        if col_name in df.columns
    }
    return df.fillna(default_values_to_apply)


def return_none_if_missing(d: Dict, key: str) -> Any:
    """ Returns `d[key]` if applicable, None otherwise.

    Args:
        d: input dictionary.
        key: key queried.
    """
    if key in d:
        return d[key]
    else:
        return None


def get_today(parameters: Dict[str, Any], sample_type: str = "scoring") -> str:
    """ Returns scoring date if specified in parameters or today's date otherwise.

    Args:
        sample_type: can be "scoring" or "scoring_experiment", used to fetch chosen_date
            from parameters.
        parameters: parameters defined in parameters.yml.
    """
    chosen_date = parameters[sample_type]["chosen_date"]
    if chosen_date == "" or chosen_date is None:
        utc_now = pytz.utc.localize(datetime.utcnow())
        today = utc_now.astimezone(pytz.timezone("Asia/Bangkok")).strftime("%Y-%m-%d")
    else:
        today = chosen_date
    return today


def join_on(df1: DataFrame, df2: DataFrame, key_columns: List[str]) -> DataFrame:
    """ Joins given DataFrames with left join, drops duplicate columns before.

    Args:
        df1: left DataFrame.
        df2: right DataFrame.
        key_columns: column names of columns to join on.
    """
    cols_to_drop = [col_name for col_name in df1.columns if col_name in df2.columns]
    cols_to_drop = list(set(cols_to_drop) - set(key_columns))
    df2 = df2.drop(*cols_to_drop)
    return df1.join(df2, key_columns, "left")


def join_multiple(key_columns: List[str], *dfs: DataFrame) -> DataFrame:
    """ Left join multiple DataFrames dropping multiple columns.

    Args:
        key_columns: column names of columns to join on.
        *dfs: DataFrames to join.
    """
    return functools.reduce(lambda df1, df2: join_on(df1, df2, key_columns), [*dfs])


def refresh_parquet(df: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Allows reading and writing from the same DataFrame. Use before saving.

    Args:
        df: DataFrame to be saved.
        parameters: parameters defined in parameters.yml.
    """
    temp_path = parameters["refresh_temp_path"]
    df.write.mode("overwrite").parquet(temp_path)
    return get_spark_session().read.parquet(temp_path)


def pick_one_per_subscriber(
    df: DataFrame, col_name: str = "subscription_identifier"
) -> DataFrame:
    """ Some inputs have more then one row per `subscription_identifier`. This function
    picks only one row for such cases.

    Args:
        df: table to filter rows for.
        col_name: col_name to make unique.
    """
    order_col = [column_name for column_name in df.columns if column_name != col_name][
        0
    ]
    order_win = Window.partitionBy(col_name).orderBy(order_col)
    return (
        df.withColumn("row_no", func.row_number().over(order_win))
        .filter("row_no == 1")
        .drop("row_no")
    )
