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
from typing import Any, Dict, List

import pyspark.sql.functions as F
from cvm.src.utils.utils import return_none_if_missing
from pyspark.sql import DataFrame


class OrderPolicy:
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


class UsersBlacklist:
    """ Keep score of who was already picked for treatment / rule"""

    def __init__(self):
        self.blacklisted_users = None

    def add(self, df: DataFrame):
        """Add users to blacklist"""
        to_add = df.select("subscription_identifier").distinct()
        if self.blacklisted_users is None:
            self.blacklisted_users = to_add
        else:
            self.blacklisted_users = self.blacklisted_users.union(to_add).distinct()

    def drop_blacklisted(self, df: DataFrame):
        if self.blacklisted_users is None:
            return df
        else:
            return df.join(
                self.blacklisted_users, on="subscription_identifier", how="left_anti"
            )


class Rule:
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
        policy = OrderPolicy(self.order_policy)
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
        logging.info("Applying rule for campaign code: {}".format(self.campaign_code))
        if self.order_policy is None:
            self.order_policy = treatment_order_policy
        rule_applied = self._get_top_users_by_order_policy(df, treatment_size_bound)
        return rule_applied.withColumn("campaign_code", F.lit(self.campaign_code))


class Treatment:
    """ Create, assign, manipulate treatment in all variants"""

    def __init__(self, treatment_dict: Dict[str, Any]):
        verify_treatment(treatment_dict)
        self.treatment_name = list(treatment_dict.keys())[0]
        treatment_details = treatment_dict[self.treatment_name]
        self.treatment_size = return_none_if_missing(
            treatment_details, "treatment_size"
        )
        self.order_policy = return_none_if_missing(treatment_details, "order_policy")
        rules_dict = treatment_details["rules"]
        rules_list = [
            {campaign_code: rules_dict[campaign_code]}
            for campaign_code in rules_dict.keys()
        ]
        self.rules = [Rule(rule_dict) for rule_dict in rules_list]

    def _get_all_variants(self) -> List:
        """List all variants present in rules"""
        variants = [
            treatment_rule.variant
            for treatment_rule in self.rules
            if treatment_rule.variant is not None
        ]
        return list(set(variants))

    def _get_rules_for_variant(self, variant: str) -> List:
        """Filter rules for chosen variant"""
        if variant is None:
            return self.rules
        else:
            return [
                rule_chosen
                for rule_chosen in self.rules
                if not rule_chosen.variant != variant
            ]

    def _apply_rules(
        self, df: DataFrame, rules_to_apply: List[Rule], groups_size_bound: int = None
    ) -> DataFrame:
        """Apply treatment to given set of users and variables"""
        # derive limit from rules sizes if not supplied
        if groups_size_bound is None:
            groups_size_bound = sum(
                [
                    rule_chosen.limit_per_code
                    for rule_chosen in rules_to_apply
                    if rule_chosen.limit_per_code is not None
                ]
            )
        rules_applied = None
        for rule_chosen in rules_to_apply:
            # remove users with rules
            if rules_applied is not None:
                df = df.join(
                    rules_applied.select("subscription_identifier"),
                    on="subscription_identifier",
                    how="left_anti",
                )
            # apply rule
            rule_applied = rule_chosen.apply_rule(
                df, self.order_policy, groups_size_bound
            )
            # add to table with rest of rules applied
            if rules_applied is None:
                rules_applied = rule_applied
            else:
                rules_applied = rules_applied.union(rule_applied)
            # update size
            groups_size_bound -= rule_chosen.rule_users_group_size
            if groups_size_bound < 0:
                groups_size_bound = 0
        return rules_applied

    def _assign_users_to_variants(self, df: DataFrame):
        """ Randomly assigns users to variants from given rules, assumes there is more
        then one variant"""
        variants = self._get_all_variants()
        logging.info(
            "Randomizing variants, following variants found {}".format(
                ", ".join(variants)
            )
        )
        n = len(variants)
        df = df.withColumn("variant_id", F.floor(F.rand() * n))
        whens = [F.when(F.col("variant_id") == i, variants[i]) for i in range(0, n)]
        return df.withColumn("variant", F.coalesce(whens)).drop("variant_id")

    def _multiple_variants(self) -> bool:
        """Returns True if multiple variants found"""
        variants = self._get_all_variants()
        return len(variants) > 0

    def _apply_variant(self, df: DataFrame, variant_chosen: str = None) -> DataFrame:
        """Apply variant of rules, assumes df has column variant, ie assigning users
        to variants was performed"""
        if variant_chosen is not None:
            df = df.filter("variant == '{}'".format(variant_chosen))
        rules = self._get_rules_for_variant(variant_chosen)
        return self._apply_rules(df, rules, self.treatment_size).withColumn(
            "variant", F.lit(variant_chosen or "default")
        )

    def _apply_all_variants(self, df: DataFrame) -> DataFrame:
        """Apply all variants possible, assumes df has column variant, ie assigning
        users to variants was performed"""
        variants = self._get_all_variants()
        if len(variants) == 0:
            return self._apply_variant(df)
        else:
            variants_applied = [
                self._apply_variant(df, variant) for variant in variants
            ]
            return functools.reduce(lambda df1, df2: df1.union(df2), variants_applied)

    def apply_treatment(self, df: DataFrame) -> DataFrame:
        """Perform applying of treatment"""
        logging.info("Applying treatment {}".format(self.treatment_name))
        if self._multiple_variants():
            df = self._assign_users_to_variants(df)
        treatment_applied = self._apply_all_variants(df)
        return treatment_applied.withColumn(
            "treatment_name", F.lit(self.treatment_name)
        )


class MultipleTreatments:
    """Create, manipulate, assign, use multiple treatments"""

    def __init__(self, treatments_dict: Dict[str, Any]):
        treatments_list = [
            {treatment_name: treatments_dict[treatment_name]}
            for treatment_name in treatments_dict.keys()
        ]
        self.treatments = [
            Treatment(treatment_dict) for treatment_dict in treatments_list
        ]

    def apply_treatments(
        self, df: DataFrame, blacklisted_users: DataFrame = None
    ) -> DataFrame:
        """ Apply multiple treatments"""
        logging.info("Applying treatment {}".format(self.treatment_name))
