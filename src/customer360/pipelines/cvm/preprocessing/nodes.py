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
from typing import Dict, Any, Tuple
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer
from pyspark.ml import Pipeline
from src.customer360.pipelines.cvm.src.list_categorical import list_categorical


def pick_columns(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """Simply pick chosen columns.

    Args:
        df: Table to pick columns from.
        parameters: parameters defined in parameters*.yml files.
    """

    cols_to_pick = parameters["prepro_cols_to_pick"]
    return df.select(cols_to_pick)


def string_indexer_fit(df: DataFrame,) -> DataFrame:
    """ Fits string indexer and runs it for given table.

    Args:
        df: Table to run string indexing for.
    Returns:
        String indexed table and OneHotEncoderEstimator object to use later.
    """
    categorical_cols = list_categorical(df)
    stages = []
    for col in categorical_cols:
        indexer = StringIndexer(
            inputCol=col, outputCol=col + "_indexed"
        ).setHandleInvalid("keep")
        stages += [indexer]
    indexer_pipeline = Pipeline(stages=stages)
    indexer_fitted = indexer_pipeline.fit(df)
    indexed = indexer_fitted.transform(df)
    indexed = indexed.drop(*categorical_cols)
    indexer_fitted.save("/mnt/customer360-cvm/string_indexer")
    return indexed


def string_indexer_transform(df: DataFrame) -> DataFrame:
    """ Transforms given table according to given indexer.

    Args:
        df: Table to run string indexing for.
        indexer: Saved string indexer pipeline.
    Returns:
        String indexed table object to use later.
    """

    indexer = Pipeline.load("/mnt/customer360-cvm/string_indexer")
    to_drop = indexer.getInputCol()
    indexed = indexer.transform(df)
    indexed = indexed.drop(*to_drop)
    return indexed


def one_hot_encoding_fit(df: DataFrame,) -> Tuple[DataFrame, OneHotEncoderEstimator]:
    """ Fits one hot encoder and runs it for given table.

    Args:
        df: Table to run one hot encoding for.
    Returns:
        One hot encoded table and OneHotEncoderEstimator object to use later.
    """
