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
from typing import Any, Dict, List, Tuple

from cvm.src.utils.classify_columns import classify_columns
from cvm.src.utils.list_operations import list_intersection, list_sub
from pyspark.ml.feature import Imputer, StringIndexer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when


def drop_blacklisted_columns(df: DataFrame, blacklisted_cols: List[Any]) -> DataFrame:
    """ Drop columns that are blacklisted in parameters.

    Args:
        blacklisted_cols: cols to be dropped.
        df: Table to perform dropping on.
    """
    log = logging.getLogger(__name__)
    cols_to_drop = list_intersection(blacklisted_cols, df.columns)
    df = df.drop(*cols_to_drop)
    log.info(f"Dropped {len(cols_to_drop)} columns")
    return df


def select_important_and_whitelisted_columns(
    df: DataFrame, parameters: Dict[str, Any], important_columns: List[Any]
) -> DataFrame:
    """ Selects features chosen by feature extraction and features necessary to run
    rest of the pipeline, eg key columns.

    Args:
        df: Table to perform selecting on.
        parameters: parameters defined in parameters.yml.
        important_columns: List of important columns.
    """
    columns_cats = classify_columns(df, parameters)
    if important_columns:
        cols_to_pick = set(
            columns_cats["target"]
            + columns_cats["key"]
            + columns_cats["segment"]
            + parameters["must_have_features"]
            + important_columns
        )
    else:
        cols_to_pick = df.columns
    cols_to_pick = list_intersection(list(cols_to_pick), df.columns)
    df = df.select(cols_to_pick)
    logging.getLogger(__name__).info(f"Selected {len(df.columns)} columns")
    return df


def numerical_to_floats(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Sets types of numerical columns.

    Args:
        df: Table to perform type changing on.
        parameters: parameters defined in parameters*.yml files.
    """
    columns_cats = classify_columns(df, parameters)
    num_cols = columns_cats["numerical"]
    non_num_cols = list_sub(df.columns, num_cols)
    df = df.select(
        [col(col_name).cast("float").alias(col_name) for col_name in num_cols]
        + non_num_cols
    )
    logging.getLogger(__name__).info(f"Types set")
    return df


def filter_out_nulls(df: DataFrame) -> Tuple[DataFrame, List]:
    """ Drops columns that have null values only.

    Args:
        df: Table to perform it on.

    Returns:
        Table without `null_columns`.
        List of null columns.
    """
    df_count = df.count()
    null_counts = df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    ).toPandas()
    null_columns = [
        colname for colname in df.columns if null_counts[colname][0] == df_count
    ]
    df = df.drop(*null_columns)
    logging.getLogger(__name__).info(f"{len(null_columns)} columns full of nulls")
    return df, null_columns


def get_string_indexers(df: DataFrame, parameters: Dict[str, Any]) -> List[Any]:
    """ Returns stages indexing strings.

    Args:
        df: Table to run it on.
        parameters: parameters defined in parameters*.yml files.
    """
    columns_cats = classify_columns(df, parameters)
    stages = []
    for col_name in columns_cats["categorical"]:
        indexer = StringIndexer(
            inputCol=col_name, outputCol=col_name + "_indexed"
        ).setHandleInvalid("keep")
        stages += [indexer]
    return stages


def get_imputer(df: DataFrame, parameters: Dict[str, Any]) -> List[Any]:
    """ Get imputer for given table.

    Args:
        df: Input table.
        parameters: parameters defined in parameters*.yml files.
    """
    columns_cats = classify_columns(df, parameters)
    imputer = Imputer(
        inputCols=columns_cats["numerical"],
        outputCols=[col + "_imputed" for col in columns_cats["numerical"]],
    )
    stages = [imputer]
    return stages
