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
from typing import Any, Dict

import pyspark.sql.functions as F
from cvm.src.utils.utils import return_none_if_missing
from pyspark.sql import DataFrame


class order_policy:
    """Class prepares order policy to sort and serves top users according to it"""

    def __init__(self, order_str: str):
        self.order_str = order_str

    def get_top_users(self, df: DataFrame, n: int) -> DataFrame:
        """ Returns top `n` users from table `df` sorted in descending order defined
        by policy.

        Args:
            df: table to return top `n` rows from.
            n: number of rows to return from.
        """
        return (
            df.selectExpr("*", "{} as order_policy".format(self.order_str))
            .orderBy("order_policy", ascending=False)
            .limit(n)
            .drop("order_policy")
        )


def verify_rule(rule_dict: Dict[str, Any]):
    """ Look for erroneous input in rule.

    Args:
        rule_dict: dictionary as in parameters_treatment_rules.yml
    """
    if len(list(rule_dict.keys())) != 1:
        raise Exception("Campaign must have one code")
    campaign_code = list(rule_dict.keys())[0]
    rule_details = rule_dict[campaign_code]
    if "conditions" not in rule_details:
        raise Exception("Conditions are missing")


def verify_treatment(treatment_dict: Dict[str, Any]):
    """ Look for erroneous input in treatment.

    Args:
        treatment_dict: dictionary as in parameters_treatment_rules.yml
    """
    if len(list(treatment_dict.keys())) != 1:
        raise Exception("Single treatment must be supplied")
    treatment_name = list(treatment_dict.keys())[0]
    rules_details = treatment_dict[treatment_name]
    if len(list(rules_details.keys())) == 0:
        raise Exception("Treatment must contain rules")
    for rule_details in rules_details:
        verify_rule(rule_details)


class rule:
    """Create, assign, manipulate treatment rule"""

    def __init__(self, rule_dict: Dict[str, Any]):
        verify_rule(rule_dict)
        self.campaign_code = list(rule_dict.keys())[0]
        rule_details = rule_dict[self.campaign_code]
        self.limit_per_code = return_none_if_missing(rule_details, "limit_per_code")
        self.order_policy = return_none_if_missing(rule_details, "order_policy")
        self.variant = return_none_if_missing(rule_details, "variant")
        self.conditions = return_none_if_missing(rule_details, "conditions")

    def _filter_with_conditions(self, df: DataFrame) -> DataFrame:
        """ Filter given table according to conditions.

        Args:
            df: input DataFrame.
        """
        conditions_in_parenthesis = [
            "({})".format(condition) for condition in self.conditions
        ]
        filter_str = " and ".join(conditions_in_parenthesis)
        return df.filter(filter_str)

    def _get_top_users_by_order_policy(
        self, df: DataFrame, treatment_size_bound: int = None
    ) -> DataFrame:
        """ Returns top users by order policy to assign to campaign code.

        Args:
            df: DataFrame of applicable population with feature columns.
            treatment_size_bound: users number assigned to campaign code upper bound.
        """
        filtered_df = self._filter_with_conditions(df)
        addressable_users_number = filtered_df.count()
        campaign_code_group_size_bounds = [
            addressable_users_number,
            treatment_size_bound,
            self.limit_per_code,
        ]
        self.rule_users_group_size = min(
            [bound for bound in campaign_code_group_size_bounds if bound is not None]
        )
        policy = order_policy(self.order_policy)
        return policy.get_top_users(filtered_df, self.rule_users_group_size).select(
            "subscription_identifier"
        )

    def apply_rule(
        self,
        df: DataFrame,
        treatment_order_policy: str = None,
        treatment_size_bound: int = None,
    ) -> DataFrame:
        """ Create table with subscription identifiers and campaign codes.

        Args:
            treatment_order_policy: order policy used when there is no rule-specific
                order policy.
            df: DataFrame of applicable population with feature columns.
            treatment_size_bound: maximum size of users group that can be assigned to
                campaign.
        """
        if self.order_policy is None:
            self.order_policy = treatment_order_policy
        rule_applied = self._get_top_users_by_order_policy(df, treatment_size_bound)
        return rule_applied.withColumn("campaign_code", F.lit(self.campaign_code))
