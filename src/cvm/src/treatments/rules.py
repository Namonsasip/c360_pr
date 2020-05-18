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

import pyspark.sql.functions as func
from cvm.src.utils.utils import return_none_if_missing
from pyspark.sql import DataFrame, Window


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
    rule_name = list(rule_dict.keys())[0]
    rule_details = rule_dict[rule_name]
    if "conditions" not in rule_details:
        raise Exception("Conditions are missing")
    if "campaign_code" not in rule_details:
        raise Exception("Campaign code is missing")


def verify_treatment(treatment_dict: Dict[str, Any]):
    """ Look for erroneous input in treatment.

    Args:
        treatment_dict: dictionary as in parameters_treatment_rules.yml
    """
    if len(list(treatment_dict.keys())) != 1:
        raise Exception("Single treatment must be supplied")
    treatment_name = list(treatment_dict.keys())[0]
    treatment_details = treatment_dict[treatment_name]
    if "rules" not in treatment_details:
        raise Exception("Treatment must contain rules")
    rules = [
        {rule_name: treatment_details["rules"][rule_name]}
        for rule_name in treatment_details["rules"]
    ]
    for rule in rules:
        verify_rule(rule)


class UsersBlacklist:
    """ Keep score of who was already picked for treatment / rule"""

    def __init__(self):
        self.blacklisted_users = None

    def add(self, df: DataFrame = None):
        """Add users to blacklist"""
        if df is not None:
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

    def __init__(self, rule_dict: Dict[str, Any], treatment_name: str):
        verify_rule(rule_dict)
        self.rule_name = list(rule_dict.keys())[0]
        rule_details = rule_dict[self.rule_name]
        self.campaign_code = return_none_if_missing(rule_details, "campaign_code")
        self.limit_per_code = return_none_if_missing(rule_details, "limit_per_code")
        self.order_policy = return_none_if_missing(rule_details, "order_policy")
        self.variant = return_none_if_missing(rule_details, "variant")
        self.conditions = return_none_if_missing(rule_details, "conditions")
        self.treatment_name = treatment_name

    def _add_user_applicable_column(self, df: DataFrame) -> DataFrame:
        """ Add column `user_applicable` marking who can be targeted.

        Args:
            df: input DataFrame.
        """
        conditions_in_parenthesis = [
            "({})".format(condition) for condition in self.conditions
        ]
        conditions_joined = " and ".join(conditions_in_parenthesis)
        applicable_str = conditions_joined + " and campaign_code is null"
        case_then = "case when " + applicable_str + " then 1 else 0 end"
        return df.selectExpr("*", "{} as user_applicable".format(case_then))

    def _add_row_number_on_order_policy(self, df: DataFrame) -> DataFrame:
        """ Adds row number column defined per `order_policy` descending order.

        Args:
            df: table to add order column to.
        """
        policy = OrderPolicy(self.order_policy)
        df = df.selectExpr("*", "{} as sort_on_col".format(policy))
        order_window = (
            Window.partitionBy("user_applicable").orderBy("sort_on_col").desc()
        )
        df = df.withColumn("policy_row_number", func.row_number().over(order_window))
        return df

    def _mark_campaign_for_top_users(self, df: DataFrame, users_num: int) -> DataFrame:
        """ Modify `campaign_code` column to assign campaign to top users according to
            `policy_row_number` column from applicable population.

        Args:
            df: table with both mentioned columns.
            users_num: number of users to pick.
        """
        assign_condition = (func.col("user_applicable") == 1) & (
            func.col("policy_row_number") <= users_num
        )
        self.rule_users_group_size = df.filter(assign_condition).count()
        return df.withColumn(
            "campaign_code",
            func.when(assign_condition, self.campaign_code).otherwise(
                func.col("campaign_code")
            ),
        )

    def apply_rule(
        self,
        df: DataFrame,
        treatment_order_policy: str = None,
        treatment_size_bound: int = None,
    ) -> DataFrame:
        """ Update `campaign_code` column to include campaigns from the rule.

        Args:
            treatment_order_policy: order policy used when there is no rule-specific
                order policy.
            df: DataFrame with users, features, campaigns.
            treatment_size_bound: maximum size of users group that can be assigned to
                campaign.
        """
        logging.info(
            "Applying rule for treatment: {}, campaign code: {}".format(
                self.treatment_name, self.campaign_code
            )
        )
        if self.order_policy is None:
            self.order_policy = treatment_order_policy
        if treatment_size_bound is not None:
            rule_group_size = min([self.limit_per_code, treatment_size_bound])
        else:
            rule_group_size = self.limit_per_code
        df = self._add_user_applicable_column(df)
        df = self._add_row_number_on_order_policy(df)
        df = self._mark_campaign_for_top_users(df, rule_group_size)
        return df.drop("policy_row_number", "user_applicable", "sort_on_col")


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
        self.rules = [Rule(rule_dict, self.treatment_name) for rule_dict in rules_list]

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
        for rule_chosen in rules_to_apply:
            if groups_size_bound > 0:
                df = rule_chosen.apply_rule(df, self.order_policy, groups_size_bound)
                groups_size_bound -= rule_chosen.rule_users_group_size
                if groups_size_bound < 0:
                    groups_size_bound = 0
        return df

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
        df = df.withColumn("variant_id", func.floor(func.rand() * n))
        whens = [
            func.when(func.col("variant_id") == i, func.lit(variants[i]))
            for i in range(0, n)
        ]
        return df.withColumn("variant", func.coalesce(*whens)).drop("variant_id")

    def _multiple_variants(self) -> bool:
        """Returns True if multiple variants found"""
        variants = self._get_all_variants()
        return len(variants) > 0

    def _apply_variant(self, df: DataFrame, variant_chosen: str = None) -> DataFrame:
        """Apply variant of rules, assumes df has column variant, ie assigning users
        to variants was performed"""
        if variant_chosen is not None:
            logging.info("Applying treatments for variant {}".format(variant_chosen))
            df = df.filter("variant == '{}'".format(variant_chosen))
        rules = self._get_rules_for_variant(variant_chosen)
        return self._apply_rules(df, rules, self.treatment_size).withColumn(
            "variant", func.lit(variant_chosen or "default")
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
            "treatment_name", func.lit(self.treatment_name)
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
        logging.info("Applying treatments")
        users_blacklist = UsersBlacklist()
        logging.info("Dropping recently contacted")
        users_blacklist.add(blacklisted_users)
        df = df.withColumn("treatment_name", func.lit(None)).withColumn(
            "campaign_code", func.lit(None)
        )
        for treatment in self.treatments:
            df = treatment.apply_treatment(df)
        return df
