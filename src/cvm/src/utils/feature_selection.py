from pyspark.sql import DataFrame
from typing import Any, List
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import StratifiedKFold
from sklearn.feature_selection import RFECV


def feature_selection(
    data: DataFrame, target_col: str, step_size: int, target_type: str
) -> List[Any]:
    """ Return list of selected features given target column.
  Args:
      data: Spark dataframe contain all features and single target column.
      target_col: column name of the target.
      step_size: parameter step for RFECV function
      target_type: type of the target column only classification or regression.
  Returns:
      List of selected feature column names.
  """

    # Filter out the target column and convert to pandas dataframe
    data = data.filter(target_col + " IS NOT NULL").toPandas()

    # Remove correlated feature at correlation > 0.8
    correlated_features = set()
    correlation_matrix = data.drop(target_col, axis=1).corr()
    for i in range(len(correlation_matrix.columns)):
        for j in range(i):
            if abs(correlation_matrix.iloc[i, j]) > 0.8:
                colname = correlation_matrix.columns[i]
                correlated_features.add(colname)
    features = data.drop(target_col, axis=1)
    target = data[target_col]

    # Select the estimator for different target type
    assert (target_type == "class") | (
        target_type == "regression"
    ), "Target type incorrect."
    if target_type == "class":
        rfc = RandomForestClassifier(random_state=101)
        rfecv = RFECV(
            estimator=rfc, step=step_size, cv=StratifiedKFold(10), scoring="roc_auc"
        )
    else:
        lr = LinearRegression(normalize=True)
        rfecv = RFECV(
            estimator=lr, step=step_size, cv=StratifiedKFold(10), scoring="roc_auc"
        )
    rfecv.fit(features, target)

    # Remove least important variables
    features.drop(features.columns[np.where(~rfecv.support_)[0]], axis=1, inplace=True)

    return list(features.columns)


def data_filtering_feature(
    important_column: List[str], whitelist_column: List[str], *df_inputs: DataFrame
) -> DataFrame:
    """ Return DataFrame with only selected features and the white list columns.
    Args:
        important_column: List of column from the the feature selection process.
        whitelist_column: List of white list columns to be preserve in a DataFrame.
        df_inputs: List of DataFrame to filter the feature.
    Returns:
        DataFrame with only selected column and white list columns.
    """

    if len(df_inputs) < 1:
        raise Exception("df_inputs is missing.")
    df = df_inputs[0]
    if len(df_inputs) > 1:
        for df_input in df_inputs[1:]:
            df = df.join(df_input, whitelist_column, "left_outer")
    df = df.select(important_column + whitelist_column)

    return df
