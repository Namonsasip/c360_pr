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
import logging
from typing import Any, Callable, Dict

import pandas as pd
from cvm.src.utils.list_operations import list_intersection, list_sub
from cvm.src.utils.list_targets import list_targets
from cvm.src.utils.utils import pick_one_per_subscriber
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType


def pyspark_predict_sklearn(
    df: DataFrame,
    sklearn_models: Dict[str, Any],
    parameters: Dict[str, Any],
    prediction_function: Callable,
) -> DataFrame:
    """ Runs predictions on given pyspark DataFrame using saved models. Assumes that
    sklearn is present on Spark cluster.

    Args:
        df: pyspark DataFrame to predict on.
        sklearn_models: sklearn models used for prediction, one per target.
        parameters: parameters defined in parameters.yml.
        prediction_function: function that takes model and pandas dataframe as input and
        returns predictions.
    Returns:
        Pyspark DataFrame of scores.
    """

    target_cols = list_targets(parameters)
    key_columns = parameters["key_columns"]
    segments_columns = parameters["segment_columns"]
    log = logging.getLogger(__name__)
    feature_cols = list_sub(
        df.columns,
        target_cols + key_columns + segments_columns + ["volatility_imputed"],
    )

    target_cols_use_case_split = list_targets(parameters, case_split=True)
    macrosegments = parameters["macrosegments"]
    pred_cols = [target + "_pred" for target in list_targets(parameters)]

    def _pred_for_macrosegment_target(
        df_target, use_case_chosen, macrosegment, target_chosen
    ):
        log.info(
            "Creating predictions for {} target, {} macrosegment.".format(
                target_chosen, macrosegment
            )
        )

        # spark prediction udf
        @func.pandas_udf(returnType=DoubleType())
        def _pandas_predict(*cols):
            pd_df = pd.concat(cols, axis=1)
            model_predictions = prediction_function(model_broadcasted.value, pd_df)
            return pd.Series(model_predictions)

        chosen_model = sklearn_models[use_case_chosen][macrosegment][target_chosen]
        model_broadcasted = SparkContext.getOrCreate().broadcast(chosen_model)
        return df_target.select(
            *df_target.columns,
            _pandas_predict(*feature_cols).alias(target_chosen + "_pred")
        )

    def _pred_for_macrosegments(df_for_macrosegment, use_case_chosen, macrosegment):
        df_macrosegment = df_for_macrosegment.filter(
            "{}_macrosegment == '{}'".format(use_case_chosen, macrosegment)
        )
        for target_chosen in target_cols_use_case_split[use_case_chosen]:
            df_macrosegment = _pred_for_macrosegment_target(
                df_macrosegment, use_case_chosen, macrosegment, target_chosen
            )
        return df_macrosegment

    def _pred_for_usecase(df_for_usecase, use_case_chosen):
        macrosegment_preds = [
            _pred_for_macrosegments(df_for_usecase, use_case_chosen, macrosegment)
            for macrosegment in macrosegments[use_case_chosen]
        ]
        return functools.reduce(lambda df1, df2: df1.union(df2), macrosegment_preds)

    use_case_preds = [
        _pred_for_usecase(df, use_case) for use_case in parameters["targets"]
    ]

    def join_on(df1, df2):
        cols_to_drop = [col_name for col_name in df1.columns if col_name in df2.columns]
        cols_to_drop = list(set(cols_to_drop) - set(key_columns))
        df2 = df2.drop(*cols_to_drop)
        return df1.join(df2, key_columns, "left")

    joined_tables = functools.reduce(join_on, use_case_preds)
    cols_to_pick = list_intersection(
        key_columns + segments_columns + pred_cols + target_cols, joined_tables.columns
    )
    return pick_one_per_subscriber(joined_tables.select(cols_to_pick))


def pyspark_predict_rf(
    df: DataFrame, rf_models: Dict[str, Any], parameters: Dict[str, Any],
) -> DataFrame:
    """ Runs predictions on given pyspark DataFrame using saved models. Assumes that
    sklearn is present on Spark cluster.

    Args:
        df: pyspark DataFrame to predict on.
        rf_models: Random Forest models used for prediction, one per target.
        parameters: parameters defined in parameters.yml.
    Returns:
        Pyspark DataFrame of scores.
    """

    def _rf_prediction_function(rf_model, pd_df):
        return rf_model.predict_proba(pd_df)[:, 1]

    return pyspark_predict_sklearn(df, rf_models, parameters, _rf_prediction_function)
