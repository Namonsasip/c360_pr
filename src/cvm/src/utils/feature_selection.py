from typing import Any, List

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFECV
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import StratifiedKFold

from pyspark.sql import DataFrame


def feature_selection(
    data: DataFrame,
    target_col: str,
    step_size: int,
    target_type: str,
    correlation_threshold: float,
    n_estimators: float,
    n_folds: float,
    min_features_to_select: float,
) -> List[Any]:
    """ Return list of selected features given target column.
  Args:
      data: Spark dataframe contain all features and single target column.
      target_col: column name of the target.
      step_size: parameter step for RFECV function
      target_type: type of the target column only classification or regression.
      correlation_threshold: threshold used to filter out highly correlated features.
      n_estimators: number of estimators used for rfe.
      n_folds: number of cv folds used for rfe.
      min_features_to_select: minimum number of features to be selected per target.
  Returns:
      List of selected feature column names.
  """

    # Filter out the target column and convert to pandas dataframe
    data = data.filter(target_col + " IS NOT NULL").toPandas()

    # Remove highly correlated features
    correlated_features = set()
    correlation_matrix = data.drop(target_col, axis=1).corr()
    for i in range(len(correlation_matrix.columns)):
        for j in range(i):
            if abs(correlation_matrix.iloc[i, j]) > correlation_threshold:
                colname = correlation_matrix.columns[i]
                correlated_features.add(colname)
    features = data.drop(target_col, axis=1)
    target = data[target_col]

    # Select the estimator for different target type
    assert (target_type == "class") | (
        target_type == "regression"
    ), "Target type incorrect."
    if target_type == "class":
        rfc = RandomForestClassifier(random_state=101, n_estimators=n_estimators)
        rfecv = RFECV(
            estimator=rfc,
            step=step_size,
            cv=StratifiedKFold(n_folds),
            scoring="roc_auc",
            min_features_to_select=min_features_to_select,
        )
    else:
        lr = LinearRegression(normalize=True)
        rfecv = RFECV(
            estimator=lr,
            step=step_size,
            cv=StratifiedKFold(n_folds),
            scoring="roc_auc",
            min_features_to_select=min_features_to_select,
        )
    rfecv.fit(features, target)

    # Remove least important variables
    features.drop(features.columns[np.where(~rfecv.support_)[0]], axis=1, inplace=True)

    return list(features.columns)
