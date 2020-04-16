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
from typing import Dict, Any, List, Tuple
from pyspark.ml import Pipeline, PipelineModel

from cvm.src.preprocesssing.preprocessing import (
    Selector,
    TypeSetter,
    Dropper,
    NullDropper,
    MultiImputer,
    MultiStringIndexer,
)
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.classify_columns import classify_columns
from cvm.src.utils.list_operations import list_intersection, list_sub
from cvm.src.utils.utils import get_clean_important_variables, impute_from_parameters


def pipeline_fit(
    df: DataFrame, important_param: List[Any], parameters: Dict[str, Any]
) -> Tuple[DataFrame, Pipeline]:
    """ Fits preprocessing pipeline to given table and runs the pipeline on it.

    Args:
        df: Table to run string indexing for.
        important_param: List of important columns.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table and OneHotEncoderEstimator object to use later.
    """

    df = prepare_key_columns(df)
    df = impute_from_parameters(df, parameters)
    important_param = get_clean_important_variables(important_param, parameters)

    columns_cats = classify_columns(df, parameters)
    if not important_param:
        cols_to_pick = df.columns
    else:
        cols_to_pick = set(
            columns_cats["target"]
            + columns_cats["key"]
            + columns_cats["segment"]
            + parameters["must_have_features"]
            + important_param
        )
    cols_to_drop = list_intersection(parameters["drop_in_preprocessing"], df.columns)
    cols_to_pick = list_sub(cols_to_pick, cols_to_drop)

    stages = [
        NullDropper(),
        Selector(cols_to_pick),
        TypeSetter(parameters),
        MultiImputer(),
        MultiStringIndexer(),
        Dropper(columns_cats["numerical"] + columns_cats["categorical"]),
    ]

    pipeline = Pipeline(stages=stages)
    pipeline_fitted = pipeline.fit(df)
    data_transformed = pipeline_fitted.transform(df)

    return data_transformed, pipeline_fitted


def pipeline_transform(
    df: DataFrame, pipeline_fitted: PipelineModel, parameters: Dict[str, Any],
) -> DataFrame:
    """ Preprocess given table.

    Args:
        df: Table to run string indexing for.
        pipeline_fitted: MLPipeline fitted to training data.
        parameters: parameters defined in parameters*.yml files.
    Returns:
        String indexed table object to use later.
    """

    df = prepare_key_columns(df)
    df = impute_from_parameters(df, parameters)

    data_transformed = pipeline_fitted.transform(df)

    return data_transformed
