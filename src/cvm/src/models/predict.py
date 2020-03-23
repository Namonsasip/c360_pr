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
from sklearn.ensemble import RandomForestClassifier
from pyspark.sql import functions as func
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
import pandas as pd
from typing import Dict, Any
import xgboost

from cvm.src.utils import list_targets
from cvm.src.utils.list_operations import list_sub


def pandas_predict_xgb(
    pd_df: pd.DataFrame, xgb_models: Dict[str, xgboost.Booster], target_chosen: str
) -> pd.Series:
    """ Runs predictions on given pandas DataFrame using saved models.

    Args:
        pd_df: pandas DataFrame to predict on.
        xgb_models: xgb models used for prediction, one per target.
        target_chosen: name of target column.
    Returns:
        Pandas series of scores.
    """

    chosen_xgb_model = xgb_models[target_chosen]
    X_pred = xgboost.DMatrix(pd_df)
    predictions = chosen_xgb_model.predict(X_pred)

    return pd.Series(predictions)


def pyspark_predict_xgb(
    df: DataFrame, xgb_models: Dict[str, xgboost.Booster], parameters: Dict[str, Any]
) -> DataFrame:
    """ Runs predictions on given pyspark DataFrame using saved models. Assumes that
    xgboost is present on Spark cluster.

    Args:
        df: pyspark DataFrame to predict on.
        xgb_models: xgb models used for prediction, one per target.
        parameters: parameters defined in parameters.yml.
    Returns:
        Pyspark DataFrame of scores.
    """

    target_cols = list_targets(parameters)
    log = logging.getLogger(__name__)

    for target_chosen in target_cols:
        log.info("Creating {} predictions.".format(target_chosen))

        # spark prediction udf
        @func.pandas_udf(returnType=DoubleType())
        def _pandas_predict(*cols):
            chosen_xgb_model = xgb_models[target_chosen]
            pd_df = pd.concat(cols, axis=1)
            X_pred = xgboost.DMatrix(pd_df)
            predictions = chosen_xgb_model.predict(X_pred)
            return pd.Series(predictions)

        feature_cols = list_sub(df.columns, target_cols)
        df = df.withColumn(target_chosen + "_pred", _pandas_predict(*feature_cols))

    return df


def pyspark_predict_rf(
    df: DataFrame,
    rf_models: Dict[str, RandomForestClassifier],
    parameters: Dict[str, Any],
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

    def _pred_for_macrosegment_target(df, use_case, macrosegment, target_chosen):

        log.info(
            "Creating predictions for {} target, {} macrosegment.".format(
                target_chosen, macrosegment
            )
        )

        # spark prediction udf
        @func.pandas_udf(returnType=DoubleType())
        def _pandas_predict(*cols):
            chosen_model = rf_models[use_case][macrosegment][target_chosen]
            pd_df = pd.concat(cols, axis=1)
            predictions = chosen_model.predict_proba(pd_df)[:, 1]
            return pd.Series(predictions)

        df = df.select(
            *df.columns, _pandas_predict(*feature_cols).alias(target_chosen + "_pred")
        )
        return df

    def _pred_for_macrosegments(df, use_case, macrosegment):
        df = df.filter("{}_macrosegment == '{}'".format(use_case, macrosegment))
        for target_chosen in target_cols_use_case_split[use_case]:
            df = _pred_for_macrosegment_target(
                df, use_case, macrosegment, target_chosen
            )
        return df

    def _pred_for_usecase(df, use_case):
        macrosegment_preds = []
        for macrosegment in macrosegments[use_case]:
            macrosegment_preds.append(
                _pred_for_macrosegments(df, use_case, macrosegment)
            )
        return functools.reduce(lambda df1, df2: df1.union(df2), macrosegment_preds)

    use_case_preds = []
    for use_case in parameters["targets"]:
        use_case_preds.append(_pred_for_usecase(df, use_case))

    def join_on(df1, df2):
        cols_to_drop = [col_name for col_name in df1.columns if col_name in df2.columns]
        cols_to_drop = list(set(cols_to_drop) - set(key_columns))
        df2 = df2.drop(*cols_to_drop)
        return df1.join(df2, key_columns, "left")

    return functools.reduce(join_on, use_case_preds)


def predict_rf_pandas(
    df: DataFrame,
    rf_models: Dict[str, RandomForestClassifier],
    parameters: Dict[str, Any],
) -> pd.Series:
    """ Runs predictions on given DataFrame using saved models.

    Args:
        df: DataFrame to predict on.
        rf_models: Random Forest models used for prediction, one per target.
        parameters: parameters defined in parameters.yml.
    Returns:
        Pandas DataFrame of scores and targets.
    """

    target_cols = list_targets(parameters)
    key_columns = parameters["key_columns"]
    segments_columns = parameters["segment_columns"]
    log = logging.getLogger(__name__)
    feature_cols = list_sub(df.columns, target_cols + key_columns + segments_columns)
    pd_df = df.select(feature_cols).toPandas()
    predictions = df.select(target_cols + key_columns).toPandas()

    for target_chosen in target_cols:
        log.info("Creating {} predictions.".format(target_chosen))
        chosen_model = rf_models[target_chosen]
        preds_col_name = target_chosen + "_pred"
        predictions[preds_col_name] = chosen_model.predict_proba(pd_df)[:, 1]

    return predictions
