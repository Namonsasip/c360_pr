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
from typing import Any, Dict, List

from cvm.src.features.parametrized_features import build_feature_from_parameters
from cvm.src.utils.list_targets import list_targets
from cvm.src.utils.utils import get_clean_important_variables, impute_from_parameters
from pyspark.sql import DataFrame


def generate_macrosegments(
    raw_features: DataFrame, parameters: Dict[str, Any]
) -> DataFrame:
    """ Setup macrosegments table.

    Args:
        raw_features: DataFrame with all features.
        parameters: parameters defined in parameters.yml.
    Returns:
        Input DataFrame with extra column marking macrosegment.
    """

    logging.info("Defining macrosegments")
    raw_features = impute_from_parameters(raw_features, parameters)
    macrosegments_defs = parameters["macrosegments"]
    for use_case in macrosegments_defs:
        raw_features = build_feature_from_parameters(
            raw_features, use_case + "_macrosegment", macrosegments_defs[use_case]
        )
    macrosegment_cols = [use_case + "_macrosegment" for use_case in macrosegments_defs]
    cols_to_pick = macrosegment_cols + ["subscription_identifier"]

    return raw_features.select(cols_to_pick)


def filter_important_only(
    df: DataFrame,
    important_param: List[Any],
    parameters: Dict[str, Any],
    include_targets: bool,
) -> DataFrame:
    """ Filters out columns from a given table. Leaves only important columns and
    key columns.

    Args:
        include_targets: should targets be picked.
        df: table to be filtered.
        important_param: List of important columns.
        parameters: parameters defined in parameters.yml.
    """
    keys = parameters["key_columns"]
    segments = parameters["segment_columns"]
    must_have_features = parameters["must_have_features"]
    targets = list_targets(parameters)
    important_param = get_clean_important_variables(important_param, parameters)
    cols_to_leave = keys + segments + must_have_features + important_param
    if include_targets:
        cols_to_leave += targets
    return df.select(cols_to_leave)
