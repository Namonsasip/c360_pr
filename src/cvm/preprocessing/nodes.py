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

from cvm.src.preprocesssing.preprocessing import (
    drop_blacklisted_columns,
    filter_out_nulls,
    get_imputer,
    get_string_indexers,
    numerical_to_floats,
    select_important_and_whitelisted_columns,
)
from cvm.src.utils.classify_columns import classify_columns
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.utils import get_clean_important_variables, impute_from_parameters
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import DataFrame


def pipeline_fit(
    df: DataFrame, parameters: Dict[str, Any], important_param=None
) -> Tuple[DataFrame, Pipeline, List]:
    """ Fits preprocessing pipeline to given table and runs the pipeline on it.

    Args:
        df: Table to run string indexing for.
        important_param: List of important columns.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table and OneHotEncoderEstimator object to use later.
    """

    if important_param is None:
        important_param = []
    log = logging.getLogger(__name__)
    df = prepare_key_columns(df)
    important_param = get_clean_important_variables(important_param, parameters)
    log.info(f"Started with {len(df.columns)} columns")

    # drop columns
    df = drop_blacklisted_columns(df, parameters["drop_in_preprocessing"])

    # select columns
    df = select_important_and_whitelisted_columns(df, parameters, important_param)
    df = impute_from_parameters(df, parameters)
    df = numerical_to_floats(df, parameters)

    df, null_columns = filter_out_nulls(df)

    # setup string indexer
    stages = get_string_indexers(df, parameters)

    # imputation
    stages += get_imputer(df, parameters)

    pipeline = Pipeline(stages=stages)
    columns_cats = classify_columns(df, parameters)
    pipeline_fitted = pipeline.fit(df)
    data_transformed = pipeline_fitted.transform(df)
    data_transformed = data_transformed.drop(*columns_cats["categorical"])
    data_transformed = data_transformed.drop(*columns_cats["numerical"])

    return data_transformed, pipeline_fitted, null_columns


def pipeline_transform(
    df: DataFrame,
    important_param: List[Any],
    pipeline_fitted: PipelineModel,
    parameters: Dict[str, Any],
    null_columns: List,
) -> DataFrame:
    """ Preprocess given table.

    Args:
        df: Table to run string indexing for.
        important_param: List of important columns.
        pipeline_fitted: MLPipeline fitted to training data.
        parameters: parameters defined in parameters*.yml files.
        null_columns: Columns with all null values.
    Returns:
        String indexed table object to use later.
    """

    log = logging.getLogger(__name__)
    df = prepare_key_columns(df)

    important_param = get_clean_important_variables(important_param, parameters)
    log.info(f"Started with {len(df.columns)} columns")

    # drop columns
    df = drop_blacklisted_columns(
        df, parameters["drop_in_preprocessing"] + null_columns
    )

    # select columns
    df = select_important_and_whitelisted_columns(df, parameters, important_param)
    df = impute_from_parameters(df, parameters)
    df = numerical_to_floats(df, parameters)

    columns_cats = classify_columns(df, parameters)
    data_transformed = pipeline_fitted.transform(df)
    data_transformed = data_transformed.drop(*columns_cats["categorical"])
    data_transformed = data_transformed.drop(*columns_cats["numerical"])

    return data_transformed
