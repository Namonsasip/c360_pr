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

import pyspark.sql.functions as func
from cvm.src.utils.list_operations import list_sub
from cvm.src.utils.utils import pick_one_per_subscriber, return_none_if_missing
from pyspark.sql import DataFrame, Window


def verify_rule(rule_dict: Dict[str, Any]):
    """ Look for erroneous input in rule.

    Args:
        rule_dict: dictionary as in parameters_treatment_rules.yml
    """
    rule_name = list(rule_dict.keys())[0]
    rule_details = rule_dict[rule_name]
    if "conditions" not in rule_details:
        raise Exception("Conditions are missing in rule {}".format(rule_name))
    if "campaign_code" not in rule_details:
        raise Exception("Campaign code is missing in rule {}".format(rule_name))


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
        raise Exception(
            "Treatment must contain rules, checkout treatment {}".format(treatment_name)
        )
    rules = [
        {rule_name: treatment_details["rules"][rule_name]}
        for rule_name in treatment_details["rules"]
    ]
    for rule in rules:
        verify_rule(rule)


class Rule:
    """Create, assign, manipulate treatment rule"""

    def __init__(self, rule_dict: Dict[str, Any]):
        verify_rule(rule_dict)
        self.rule_name = list(rule_dict.keys())[0]
        rule_details = rule_dict[self.rule_name]
        self.campaign_code = str(return_none_if_missing(rule_details, "campaign_code"))
        self.limit_per_code = str(
            return_none_if_missing(rule_details, "limit_per_code")
        )
        self.order_policy = return_none_if_missing(rule_details, "order_policy")
        self.variant = str(return_none_if_missing(rule_details, "variant"))
        self.conditions = return_none_if_missing(rule_details, "conditions")

    def _add_user_applicable_column(
        self, df: DataFrame, variant_chosen: str = None
    ) -> DataFrame:
        """ Add column `user_applicable` marking who can be targeted.

        Args:
            variant_chosen: chosen variant.
            df: input DataFrame.
        """
        conditions_in_parenthesis = [
            "({})".format(condition) for condition in self.conditions
        ]
        conditions_joined = " and ".join(conditions_in_parenthesis)
        applicable_str = conditions_joined + " and (campaign_code is null)"
        if variant_chosen is not None:
            applicable_str += " and (variant == '{}')".format(variant_chosen)
        case_then = "case when " + applicable_str + " then 1 else 0 end"
        return df.selectExpr("*", "{} as user_applicable".format(case_then))

    def _add_row_number_on_order_policy(self, df: DataFrame) -> DataFrame:
        """ Adds row number column defined per `order_policy` descending order.

        Args:
            df: table to add order column to.
        """
        df = df.selectExpr("*", "{} as sort_on_col".format(self.order_policy))
        order_window = Window.partitionBy("user_applicable").orderBy(
            func.col("sort_on_col").desc()
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
        return df.withColumn(
            "campaign_code",
            func.when(assign_condition, self.campaign_code).otherwise(
                func.col("campaign_code")
            ),
        )

    def apply_rule(
        self,
        df: DataFrame,
        variant_chosen: str = None,
        treatment_order_policy: str = None,
    ) -> DataFrame:
        """ Update `campaign_code` column to include campaigns from the rule.

        Args:
            variant_chosen: variant chosen, used to pick users.
            treatment_order_policy: order policy used when there is no rule-specific
                order policy.
            df: DataFrame with users, features, campaigns.
        """
        logging.info(
            "Applying rule for rule: {}, campaign code: {}".format(
                self.rule_name, self.campaign_code
            )
        )
        if self.order_policy is None:
            self.order_policy = treatment_order_policy
        df = self._add_user_applicable_column(df, variant_chosen)
        df = self._add_row_number_on_order_policy(df)
        df = self._mark_campaign_for_top_users(df, int(self.limit_per_code))
        df = df.drop("policy_row_number", "user_applicable", "sort_on_col")
        return df


class Treatment:
    """ Create, assign, manipulate treatment in all variants"""

    def __init__(self, treatment_dict: Dict[str, Any]):
        verify_treatment(treatment_dict)
        self.treatment_id = list(treatment_dict.keys())[0]
        treatment_details = treatment_dict[self.treatment_id]
        self.treatment_name = str(
            return_none_if_missing(treatment_details, "treatment_name")
        )
        self.treatment_size = str(
            return_none_if_missing(treatment_details, "treatment_size")
        )
        self.order_policy = treatment_details["order_policy"]
        self.use_case = str(return_none_if_missing(treatment_details, "use_case"))
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
                if (rule_chosen.variant is None) or (rule_chosen.variant == variant)
            ]

    def _apply_rules(
        self, df: DataFrame, rules_to_apply: List[Rule], variant_chosen: str = None,
    ) -> DataFrame:
        """Apply treatment to given set of users and variables disregarding treatment
        size"""
        for rule_chosen in rules_to_apply:
            df = rule_chosen.apply_rule(df, variant_chosen, self.order_policy)
        return df

    def _truncate_assigned_campaigns(self, df: DataFrame, variant_chosen: str = None):
        """Reduce number of people assigned to campaigns to fulfill treatment size
        condition. Top users according to order policy are picked"""
        current_treatment_variant = func.col("treatment_name") == self.treatment_name
        if variant_chosen is not None:
            current_treatment_variant &= func.col("variant") == variant_chosen
        df = df.selectExpr(
            "*", "{} as sort_on_col".format(self.order_policy),
        ).withColumn(
            "has_current_treatment",
            func.when(current_treatment_variant, 1).otherwise(0),
        )
        treatment_lp_window = Window.partitionBy("has_current_treatment").orderBy(
            func.col("sort_on_col").desc()
        )
        df = df.withColumn(
            "lp_in_treatment", func.row_number().over(treatment_lp_window)
        )
        to_truncate = (func.col("has_current_treatment") == 1) & (
            func.col("lp_in_treatment") >= self.treatment_size
        )
        cols_to_trunc = ["treatment_name", "use_case", "campaign_code"]
        for col_to_trunc in cols_to_trunc:
            df = df.withColumn(
                col_to_trunc,
                func.when(to_truncate, func.lit(None)).otherwise(
                    func.col(col_to_trunc)
                ),
            )
        to_drop = ["sort_on_col", "has_current_treatment", "lp_in_treatment"]
        return df.drop(*to_drop)

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
        rules = self._get_rules_for_variant(variant_chosen)
        df = self._apply_rules(df, rules, variant_chosen)
        df = Treatment._fill_for_users_with_campaign(
            df, "treatment_name", self.treatment_name
        )
        df = Treatment._fill_for_users_with_campaign(df, "use_case", self.use_case)
        return self._truncate_assigned_campaigns(df, variant_chosen)

    def _apply_all_variants(self, df: DataFrame) -> DataFrame:
        """Apply all variants possible, assumes df has column variant, ie assigning
        users to variants was performed"""
        variants = self._get_all_variants()
        if len(variants) == 0:
            return self._apply_variant(df)
        for variant_chosen in variants:
            df = self._apply_variant(df, variant_chosen)
        return df

    @staticmethod
    def _fill_for_users_with_campaign(
        df: DataFrame, column_to_fill: str, value_to_fill: str
    ) -> DataFrame:
        """ For users with current treatment set value of column `column_to_fill` to
        `value_to_fill`"""
        current_campaign_cond = (
            func.col(column_to_fill).isNull() & func.col("campaign_code").isNotNull()
        )
        return df.withColumn(
            column_to_fill,
            func.when(current_campaign_cond, func.lit(value_to_fill)).otherwise(
                func.col(column_to_fill)
            ),
        )

    def apply_treatment(self, df: DataFrame) -> DataFrame:
        """Perform applying of treatment"""
        logging.info("Applying treatment {}".format(self.treatment_id))
        must_have_cols = ["treatment_name", "campaign_code", "use_case"]
        for missing_col in list_sub(must_have_cols, df.columns):
            df = df.withColumn(missing_col, func.lit(None))
        if self._multiple_variants():
            df = self._assign_users_to_variants(df)
        df = self._apply_all_variants(df)
        return df


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
        if blacklisted_users is not None:
            logging.info("Dropping recently contacted")
            df = df.join(
                blacklisted_users.select("subscription_identifier"),
                on="subscription_identifier",
                how="left_anti",
            )
        logging.info("Applying treatments")
        for treatment in self.treatments:
            df = treatment.apply_treatment(df)
        return pick_one_per_subscriber(df.filter("campaign_code is not null"))
