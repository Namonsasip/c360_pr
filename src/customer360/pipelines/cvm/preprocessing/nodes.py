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
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from src.customer360.pipelines.cvm.src.list_categorical import list_categorical


def pipeline1_fit(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Fits string indexer and runs it for given table.

    Args:
        df: Table to run string indexing for.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table and OneHotEncoderEstimator object to use later.
    """

    # select columns
    cols_to_pick = parameters["prepro_cols_to_pick"]
    df = df.select(cols_to_pick)

    # string indexer
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
    indexer_fitted.write().overwrite().save("/mnt/customer360-cvm/string_indexer")

    return indexed


def pipeline1_transform(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Transforms given table according to given indexer.

    Args:
        df: Table to run string indexing for.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table object to use later.
    """

    # select columns
    cols_to_pick = parameters["prepro_cols_to_pick"]
    df = df.select(cols_to_pick)

    # string indexer
    indexer = PipelineModel.load("/mnt/customer360-cvm/string_indexer")
    indexed = indexer.transform(df)
    categorical_cols = list_categorical(df)
    indexed = indexed.drop(*categorical_cols)

    return indexed
