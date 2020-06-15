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

from pyspark.sql import DataFrame


def build_feature_from_parameters(
    df: DataFrame, new_col_name: str, feature_definition: Dict[Any, List[str]],
) -> DataFrame:
    """ Build new feature using parametrized definition. Parameterized definition is a
    dictionary. Its keys are values for column `new_col_name`. Each value is a list.
    Each element of the list is a SQL-compliant where statement. Values are assigned
    to `new_col_name` when all of the included in the list statements are true.

    Args:
        df: Table to get features from.
        new_col_name: Output column name.
        feature_definition: Parameterized definition of new column.
    Returns:
        Table `df` with new column called `new_col_name`.
    """

    def _get_one_when_then_for_value(conditions_list, value_to_fill):
        """Prepares when - then clause to fill up one value."""
        when_str = " and ".join([f"({cond})" for cond in conditions_list])
        return "when " + when_str + " then '" + value_to_fill + "'"

    def _get_when_then_clause_for_col(value_conditions_dict):
        """Prepare when - then clause to create microsegment column for one use case."""
        when_then_clauses = [
            _get_one_when_then_for_value(
                value_conditions_dict[value_to_fill], value_to_fill
            )
            for value_to_fill in value_conditions_dict
        ]
        return (
            "case " + "\n".join(when_then_clauses) + " else NULL end as " + new_col_name
        )

    logging.info("Creating {} from dictionary definition".format(new_col_name))
    if new_col_name in df.columns:
        raise Exception("{} already in provided table".format(new_col_name))
    case_when_clause = _get_when_then_clause_for_col(feature_definition)

    return df.selectExpr("*", case_when_clause)
