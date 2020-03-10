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
from typing import Dict, Any, List
from pyspark.ml.feature import StringIndexer, Imputer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col
from customer360.pipelines.cvm.src.utils.list_categorical import list_categorical
from customer360.pipelines.cvm.src.utils.list_operations import list_sub
from customer360.pipelines.cvm.src.utils.list_targets import list_targets
from customer360.pipelines.cvm.src.utils.setup_names import setup_names
from src.customer360.pipelines.cvm.src.feature_selection import feature_selection


def pipeline1_fit(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Fits preprocessing pipeline to given table and runs the pipeline on it.

    Args:
        df: Table to run string indexing for.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table and OneHotEncoderEstimator object to use later.
    """

    df = setup_names(df)

    # select columns
    target_cols = list_targets(parameters)
    key_columns = parameters["key_columns"]
    cols_to_pick = parameters["prepro_cols_to_pick"] + target_cols + key_columns
    df = df.select(cols_to_pick)

    categorical_cols = list_sub(list_categorical(df), target_cols + key_columns)
    numerical_cols = list_sub(df.columns, categorical_cols + target_cols + key_columns)

    # set types
    for col_name in numerical_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast("float"))

    # string indexer
    stages = []
    for col_name in categorical_cols:
        indexer = StringIndexer(
            inputCol=col_name, outputCol=col_name + "_indexed"
        ).setHandleInvalid("keep")
        stages += [indexer]
    # imputation
    imputer = Imputer(
        inputCols=numerical_cols,
        outputCols=[col + "_imputed" for col in numerical_cols],
    )
    stages += [imputer]

    pipeline = Pipeline(stages=stages)
    pipeline_fitted = pipeline.fit(df)
    data_transformed = pipeline_fitted.transform(df)
    data_transformed = data_transformed.drop(*categorical_cols)
    data_transformed = data_transformed.drop(*numerical_cols)

    pipeline_fitted.write().overwrite().save("/mnt/customer360-cvm/pipeline1")

    return data_transformed


def pipeline1_transform(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Preprocess given table.

    Args:
        df: Table to run string indexing for.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table object to use later.
    """

    df = setup_names(df)

    # select columns
    target_cols = list_targets(parameters)
    key_columns = parameters["key_columns"]
    cols_to_pick = parameters["prepro_cols_to_pick"] + target_cols + key_columns
    df = df.select(cols_to_pick)

    categorical_cols = list_sub(list_categorical(df), target_cols + key_columns)
    numerical_cols = list_sub(df.columns, categorical_cols + target_cols + key_columns)

    # set types
    for col_name in numerical_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast("float"))

    # string indexer
    pipeline_fitted = PipelineModel.load("/mnt/customer360-cvm/pipeline1")
    data_transformed = pipeline_fitted.transform(df)
    data_transformed = data_transformed.drop(*categorical_cols)
    data_transformed = data_transformed.drop(*numerical_cols)

    return data_transformed


def feature_selection_all_target(
    data: DataFrame, parameters: Dict[str, Any]
) -> List[Any]:
    """ Return list of selected features and plots for all target columns.
  Args:
      data: Spark dataframe contain all features and all target columns.
      parameters: parameters defined in target parameters*.yml files.
  Returns:
      List of selected feature column names for all target columns.
  """

    # Get target_type from target parameter dict
    target_class = {}
    for usecase in parameters["targets"]:
        for target in parameters["targets"][usecase]:
            target_class[target] = parameters["targets"][usecase][target]["target_type"]
    # Remove black list column
    data = data.drop(*parameters["feature_selection_parameter"]["exclude_col"])
    final_list = []
    for target in parameters["feature_selection_parameter"]["target_column"]:
        exclude_target = parameters["feature_selection_parameter"]["target_column"][:]
        exclude_target.remove(target)
        res_list, res_plot = feature_selection(
            data.drop(*exclude_target),
            target,
            parameters["feature_selection_parameter"]["step_size"],
            target_class[target],
        )
        final_list = list(set(final_list) | set(res_list))

    return final_list
