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
from typing import Any, Dict, List, Tuple

import pyspark.sql.functions as func
from cvm.src.features.parametrized_features import build_feature_from_parameters
from cvm.src.targets.ard_targets import get_ard_targets
from cvm.src.targets.churn_targets import filter_usage, get_churn_targets
from cvm.src.utils.feature_selection import feature_selection
from cvm.src.utils.list_targets import list_targets
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.utils import get_clean_important_variables, impute_from_parameters
from pyspark.sql import DataFrame


def add_ard_targets(
    users: DataFrame,
    reve: DataFrame,
    parameters: Dict[str, Any],
    sampling_parameters: Dict[str, Any],
) -> DataFrame:
    """ Create table with ARPU drop targets.

    Args:
        users: Table with users and dates to create targets for.
        reve: Table with revenue stats.
        parameters: parameters defined in parameters.yml.
        sampling_parameters: sampling parameters defined in parameters.yml.
    Returns:
        Table with ARD targets.
    """

    local_parameters = parameters["targets"]["ard"]
    chosen_date = sampling_parameters["chosen_date"]
    users = prepare_key_columns(users)
    ard_target_tables = [
        get_ard_targets(users, reve, local_parameters[targets], chosen_date)
        for targets in local_parameters
    ]

    def join_targets(df1, df2):
        return df1.join(df2, ["key_date", "subscription_identifier"], "full")

    return functools.reduce(join_targets, ard_target_tables)


def add_churn_targets(
    users: DataFrame,
    usage: DataFrame,
    parameters: Dict[str, Any],
    sampling_parameters: Dict[str, Any],
) -> DataFrame:
    """ Create table with churn targets.

    Args:
        users: Table with users and dates to create targets for.
        usage: Table with usage stats.
        parameters: parameters defined in parameters.yml.
        sampling_parameters: sampling parameters defined in parameters.yml.
    Returns:
        Table with churn targets.
    """

    local_parameters = parameters["targets"]["churn"]
    chosen_date = sampling_parameters["chosen_date"]

    users = prepare_key_columns(users)
    usage = prepare_key_columns(usage)
    usage = filter_usage(users, usage, parameters)
    churn_target_tables = [
        get_churn_targets(users, usage, local_parameters[targets], chosen_date)
        for targets in local_parameters
    ]

    def join_targets(df1, df2):
        return df1.join(df2, ["key_date", "subscription_identifier"], "full")

    return functools.reduce(join_targets, churn_target_tables)


def train_test_split(
    targets_features: DataFrame, parameters: Dict[str, Any],
) -> Tuple[DataFrame, DataFrame]:
    """Adds train-test column to features-targets table. Train share defined in
    parameters.

    Args:
        targets_features: Table with features and targets.
        parameters: parameters defined in parameters.yml.

    Returns:
        targets_features table with extra column with train / test flag.
    """

    train_share = parameters["training"]["train_share"]

    # add train test flag
    train_test = targets_features.withColumn(
        "train_test", func.when(func.rand() <= train_share, "train").otherwise("test")
    )
    train = train_test.filter("train_test == 'train'").drop("train_test")
    test = train_test.filter("train_test == 'test'").drop("train_test")

    return train, test


def subs_date_join_important_only(
    important_param: List[Any],
    parameters: Dict[str, Any],
    users: DataFrame,
    *args: DataFrame,
) -> DataFrame:
    """ Left join all tables with important variables by given keys. Join using
     `subscription_identifier` or `old_subscription_identifier`.

    Args:
        users: table with users, has to have both `old_subscription_identifier` and
            `subscription_identifier` columns.
        important_param: List of important columns.
        parameters: parameters defined in parameters.yml.
        *args: tables to join, each has to have either `old_subscription_identifier` or
            `subscription_identifier` column.
    Returns:
        Left joined and filtered tables.
    """

    keys = parameters["key_columns"] + ["old_subscription_identifier"]
    segments = parameters["segment_columns"]
    must_have_features = parameters["must_have_features"]
    targets = list_targets(parameters)
    tables = [prepare_key_columns(tab) for tab in args]
    important_param = get_clean_important_variables(important_param, parameters)

    def filter_column(df, filter_list):
        cols_to_drop = [
            col_name for col_name in df.columns if col_name not in filter_list
        ]
        return df.drop(*cols_to_drop)

    tables = [
        filter_column(
            tab, important_param + keys + segments + must_have_features + targets
        )
        for tab in tables
    ]

    return subs_date_join(parameters, users, *tables)


def subs_date_join(
    parameters: Dict[str, Any], users: DataFrame, *args: DataFrame,
) -> DataFrame:
    """ Left join all tables by given keys. Join using `subscription_identifier` or
    `old_subscription_identifier`.

    Args:
        users: table with users, has to have both `old_subscription_identifier` and
            `subscription_identifier` columns.
        parameters: parameters defined in parameters.yml.
        *args: tables to join, each has to have either `old_subscription_identifier` or
            `subscription_identifier` column.
    Returns:
        Left joined tables.
    """

    tables = [prepare_key_columns(tab) for tab in args]
    tables_without_sub_ids = [
        tab
        for tab in tables
        if ("old_subscription_identifier" not in tab.columns)
        and ("subscription_identifier" not in tab.columns)
    ]
    no_sub_id_present = len(tables_without_sub_ids) == 0
    if no_sub_id_present:
        raise Exception(
            "Not every table has `old_subscription_identifier`"
            + " or `subscription_identifier`"
        )
    old_sub_id_tables = [
        tab for tab in tables if "old_subscription_identifier" in tab.columns
    ]
    sub_id_tables = [tab for tab in tables if "subscription_identifier" in tab.columns]

    def join_on(df1, df2, keys):
        cols_to_drop = [col_name for col_name in df1.columns if col_name in df2.columns]
        cols_to_drop = list(set(cols_to_drop) - set(keys))
        df2 = df2.drop(*cols_to_drop)
        return df1.join(df2, keys, "left")

    old_sub_ids_joined = functools.reduce(
        functools.partial(join_on, keys=["old_subscription_identifier", "key_date"]),
        [users] + old_sub_id_tables,
    )
    sub_ids_joined = functools.reduce(
        functools.partial(join_on, keys=["subscription_identifier", "key_date"]),
        [users] + sub_id_tables,
    )

    return join_on(
        old_sub_ids_joined, sub_ids_joined, keys=["subscription_identifier", "key_date"]
    ).drop("old_subscription_identifier")


def add_macrosegments(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Add macrosegments columns.

    Args:
        df: DataFrame with all features.
        parameters: parameters defined in parameters.yml.
    Returns:
        Input DataFrame with extra column marking macrosegment.
    """

    logging.info("Defining macrosegments")
    df = impute_from_parameters(df, parameters)
    macrosegments_defs = parameters["macrosegments"]
    for use_case in macrosegments_defs:
        df = build_feature_from_parameters(
            df, use_case + "_macrosegment", macrosegments_defs[use_case]
        )

    return df


def feature_selection_all_target(
    data: DataFrame, parameters: Dict[str, Any]
) -> List[Any]:
    """ Return list of selected features and plots for all target columns.
  Args:
      data: Spark DataFrame contain all features and all target columns.
      parameters: parameters defined in target parameters*.yml files.
  Returns:
      List of selected feature column names for all target columns.
  """

    log = logging.getLogger(__name__)
    # Get target_type from target parameter dict
    target_class = {}
    for usecase in parameters["targets"]:
        for target in parameters["targets"][usecase]:
            target_class[target] = parameters["targets"][usecase][target]["target_type"]
    # Remove black list column
    data = data.drop(*parameters["feature_selection_parameter"]["exclude_col"])
    data = data.drop(*parameters["key_columns"])
    data = data.drop(*parameters["segment_columns"])

    final_list = []
    for target in parameters["feature_selection_parameter"]["target_column"]:
        log.info(f"Looking for important features for {target}")
        exclude_target = parameters["feature_selection_parameter"]["target_column"][:]
        exclude_target.remove(target)
        res_list = feature_selection(
            data.drop(*exclude_target),
            target,
            parameters["feature_selection_parameter"]["step_size"],
            target_class[target],
            parameters["feature_selection_parameter"]["correlation_threshold"],
            parameters["feature_selection_parameter"]["n_estimators"],
            parameters["feature_selection_parameter"]["n_folds"],
            parameters["feature_selection_parameter"]["min_features_to_select"],
        )
        final_list = list(set(final_list) | set(res_list))

    return final_list
