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
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as func


def mark_propensities_as_low_high(
    propensities: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Each propensity is mapped to 'H' if greater then given threshold and to 'L'
        otherwise.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
    """

    log = logging.getLogger(__name__)
    log.info("Mapping propensities to H / L")

    cutoffs = parameters["cutoffs"]

    def _map_usecase_macro_target(
        df, use_case_chosen, macrosegment_chosen, target_chosen
    ):
        cutoff_local = cutoffs[use_case_chosen][macrosegment_chosen][target_chosen]
        new_colname = target_chosen + "_lh"
        propensity_colname = target_chosen + "_pred"
        macrosegment_colname = use_case_chosen + "_macrosegment"

        macrosegment_cond = func.col(macrosegment_colname) == macrosegment_chosen
        H_cond = func.col(propensity_colname) >= cutoff_local
        L_cond = func.col(propensity_colname) < cutoff_local
        if new_colname not in df.columns:
            df = df.withColumn(new_colname, func.lit(None))
        when_clause = (
            func.when(macrosegment_cond & H_cond, "H")
            .when(macrosegment_cond & L_cond, "L")
            .otherwise(func.col(new_colname))
        )

        return df.withColumn(new_colname, when_clause)

    def _map_usecase_macro(df, use_case_chosen, macrosegment_chosen):
        for target_chosen in cutoffs[use_case_chosen][macrosegment_chosen]:
            df = _map_usecase_macro_target(
                df, use_case_chosen, macrosegment_chosen, target_chosen
            )
        return df

    def _map_usecase(df, use_case_chosen):
        for macrosegment_chosen in cutoffs[use_case_chosen]:
            df = _map_usecase_macro(df, use_case_chosen, macrosegment_chosen)
        return df

    for use_case in cutoffs:
        propensities = _map_usecase(propensities, use_case)

    return propensities


def filter_cutoffs(propensities: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Filters given propensities table according to cutoffs given.

    Args:
        propensities: table with propensities.
        parameters: parameters defined in parameters.yml.
    """

    log = logging.getLogger(__name__)
    log.info("Choosing users to target with treatment")

    cutoffs = parameters["cutoffs"]

    def _get_filter_for_macro(use_case_chosen, macrosegment):
        """ Return string that filters out users of specific macrosegment with all
        propensities higher then cutoffs."""
        cutoffs_local = cutoffs[use_case_chosen][macrosegment]
        filter_str = f"{use_case_chosen}_macrosegment == '{macrosegment}'"
        for target in cutoffs_local:
            filter_str += f" and ({target}_pred >= {cutoffs_local[target]})"
        return filter_str

    # propensity above threshold for all targets
    def _get_filter_for_use_case(use_case_chosen):
        """ Returns filter string that filter out users of all macrosegments with all
        propensities higher than cutoffs."""
        macrosegments = cutoffs[use_case_chosen]
        filter_str = ""
        for macrosegment in macrosegments:
            macrosegment_filter_str = _get_filter_for_macro(
                use_case_chosen, macrosegment
            )
            filter_str += f"({macrosegment_filter_str}) or "
        filter_str = filter_str[:-4]
        return filter_str

    dfs = []
    churn_filter_str = _get_filter_for_use_case("churn")
    dfs.append(
        propensities.filter(churn_filter_str)
        .select("subscription_identifier")
        .withColumn("use_case", func.lit("churn"))
    )
    ard_filter_str = _get_filter_for_use_case("ard")
    ard_filter_str = f"({ard_filter_str}) and not ({churn_filter_str})"
    dfs.append(
        propensities.filter(ard_filter_str)
        .select("subscription_identifier")
        .withColumn("use_case", func.lit("ard"))
    )

    def union_dfs(df1, df2):
        return df1.union(df2)

    return functools.reduce(union_dfs, dfs)
