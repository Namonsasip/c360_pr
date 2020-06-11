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
from typing import Any, Dict

import pyspark.sql.functions as func
from cvm.src.features.parametrized_features import build_feature_from_parameters
from cvm.src.utils.utils import impute_from_parameters
from pandas import DataFrame


def generate_macrosegments(
    raw_features: DataFrame, parameters: Dict[str, Any], key_date: str
) -> DataFrame:
    """ Setup macrosegments table.

    Args:
        key_date: date to create macrosegments for.
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

    return raw_features.select(cols_to_pick).withColumn("key_date", func.lit(key_date))
