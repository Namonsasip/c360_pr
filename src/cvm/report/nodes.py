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
from datetime import date
from typing import Any, Dict

from cvm.data_prep.nodes import add_macrosegments
from cvm.treatments.nodes import prepare_microsegments
from pyspark.sql import DataFrame
from pyspark.sql import functions as func


def prepare_users(customer_groups: DataFrame) -> DataFrame:
    """ Creates users table for reporting purposes.

    Args:
        customer_groups: Table with target, control and bau groups.
    """
    today = date.today().strftime("%Y-%m-%d")
    return (
        customer_groups.filter("target_group in ('TG', 'CG', 'BAU')")
        .select(["crm_sub_id", "target_group"])
        .distinct()
        .withColumn("key_date", func.lit(today))
        .withColumnRenamed("crm_sub_id", "subscription_identifier")
    )


def add_micro_macro(
    raw_features: DataFrame, reve: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Adds microsegment and macrosegment to C360 joined features table.

    Args:
        raw_features: Table with users to add microsegments to and pre - preprocessing
            features.
        reve: Table with monthly revenue. Assumes using l3 profile table.
        parameters: parameters defined in parameters.yml.
    """

    macro_added = add_macrosegments(raw_features, parameters)
    micro_macro_added = prepare_microsegments(
        macro_added, reve, parameters, reduce_cols=False
    )
    return micro_macro_added


def filter_out_micro_macro(all_features: DataFrame) -> DataFrame:
    """ Pick only microsegments and macrosegments from table with all features.

    Args:
        all_features: table with raw features, macrosegments and microsegments.
    """
    cols_to_pick = [
        "subscription_identifier",
        "ard_macrosegment",
        "churn_macrosegment",
        "ard_microsegment",
        "churn_microsegment",
        "target_group",
    ]
    return all_features.select(cols_to_pick)
