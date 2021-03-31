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
from cvm.src.utils.utils import get_today
from pyspark.sql import DataFrame, Window


def add_call_center_features(df: DataFrame,) -> DataFrame:
    """ Add features related to users touchpoints with call center.
    Features added include `call_center_persona`.

    Args:
        df: table with users.
    """
    logging.getLogger(__name__).info("Adding call center features")
    cc_calls_last_4_weeks_col = (
        "sum_touchpoints_number_of_calls_on_cc_sum_weekly_last_four_week"
    )
    cc_calls_last_4_weeks = func.coalesce(
        func.col(cc_calls_last_4_weeks_col), func.lit(0)
    )
    cc_calls_last_12_weeks_col = (
        "sum_touchpoints_number_of_calls_on_cc_sum_weekly_last_twelve_week"
    )
    cc_calls_last_12_weeks = func.coalesce(
        func.col(cc_calls_last_12_weeks_col), func.lit(0)
    )
    cc_calls_last_4_12_weeks = cc_calls_last_12_weeks - cc_calls_last_4_weeks

    persona_mapping = (
        func.when(
            (cc_calls_last_4_weeks > 0) & (cc_calls_last_4_12_weeks > 0), "unsatisfied",
        )
        .when(
            (cc_calls_last_4_weeks > 0) & (cc_calls_last_4_12_weeks <= 0),
            "anomaly_detected",
        )
        .when(
            (cc_calls_last_4_weeks <= 0) & (cc_calls_last_4_12_weeks > 0),
            "problem_resolved_or_follow_up",
        )
        .otherwise(func.lit(None))
    )
    return df.withColumn("call_centre_persona", persona_mapping)


def add_other_sim_card_features(
    df: DataFrame,
    recent_profile: DataFrame,
    main_packs: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Add features for other sim card held by the same national id.
    Features added include youngest / oldest card revenue / tenure, number of simcards,
    internal churn flag.

    Args:
        df: table with users.
        recent_profile: table with users' national ids, only last date.
        main_packs: table with main packages details.
        parameters: parameters defined in parameters.yml.
    """
    normal_main_packs = (
        main_packs.filter(
            """promotion_group_tariff not in ('SIM 2 Fly',
             'Net SIM', 'Traveller SIM')"""
        )
        .select("package_id")
        .distinct()
        .withColumnRenamed("package_id", "current_package_id")
    )
    logging.getLogger(__name__).info(
        "{} applicable packages found".format(normal_main_packs.count())
    )
    number_of_simcards = (
        # keep only normal packages
        recent_profile.join(normal_main_packs, on="current_package_id")
        # calculate number of simcards per national id
        .groupBy("national_id_card")
        .agg(func.count("subscription_identifier").alias("number_of_simcards"))
        .select(["national_id_card", "number_of_simcards"])
    )
    # calculate statistics
    national_id_card_stats_youngest_card = (
        number_of_simcards.join(recent_profile, on="national_id_card")
        .withColumn(
            "card_age_rn",
            func.row_number().over(
                Window.partitionBy("national_id_card").orderBy(
                    func.col("subscriber_tenure")
                )
            ),
        )
        .filter("card_age_rn == 1")
        .selectExpr(
            "national_id_card",
            "norms_net_revenue as revenue_on_youngest_card",
            "subscriber_tenure as youngest_card_tenure",
            "number_of_simcards",
        )
    )
    national_id_card_stats_oldest_card = (
        number_of_simcards.join(recent_profile, on="national_id_card")
        .withColumn(
            "card_age_rn",
            func.row_number().over(
                Window.partitionBy("national_id_card").orderBy(
                    func.col("subscriber_tenure").desc()
                )
            ),
        )
        .filter("card_age_rn == 1")
        .selectExpr(
            "national_id_card",
            "norms_net_revenue as revenue_on_oldest_card",
            "subscriber_tenure as oldest_card_tenure",
        )
    )
    national_id_card_stats = national_id_card_stats_oldest_card.join(
        national_id_card_stats_youngest_card, on="national_id_card"
    )

    # join to given table
    df = df.join(recent_profile.select('subscription_identifier', 'national_id_card'),
                 ['subscription_identifier'],
                 'left_outer')
    df = df.join(national_id_card_stats, on="national_id_card", how="left")

    # add internal churn flag
    logging.getLogger(__name__).info("Adding internal churn flag")
    is_internal_churner = (
        (func.col("number_of_simcards") >= 2)
        & (func.col("number_of_simcards") <= 4)
        & (func.col("youngest_card_tenure") <= 12)
    )
    is_internal_churner_12 = (
            (func.col("number_of_simcards") >= 2)
            & (func.col("number_of_simcards") <= 4)
            & (func.col("youngest_card_tenure") > 12)
    )
    df = df.withColumn(
        "internal_churner_2_4_6", func.when(is_internal_churner, 1).otherwise(0)
    )
    df = df.withColumn(
        "internal_churner_2_4_12", func.when(is_internal_churner_12, 1).otherwise(0)
    )

    return df


def add_churn_ard_optimizer_features(
    df: DataFrame, propensities: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Adds estimated churn / ard campaign return.

    Args:
        df: table with users
        propensities: users propensities
        parameters: parameters defined in parameters.yml
    """
    logging.getLogger(__name__).info("Adding optimization")
    optimization_parameters = parameters["use_case_optimize"]
    ard_cost = optimization_parameters["ard_cost"]
    churn_cost = optimization_parameters["churn_cost"]
    costs = (
        propensities.withColumn(
            "optimizer_churn_cost", churn_cost * func.col("churn60_pred")
        )
        .withColumn("optimizer_ard_cost", ard_cost * func.col("dilution2_pred"))
        .select(
            ["subscription_identifier", "optimizer_churn_cost", "optimizer_ard_cost"]
        )
    )
    df = df.join(costs, on="subscription_identifier", how="left").fillna(
        {"optimizer_churn_cost": 0, "optimizer_ard_cost": 0}
    )
    return df


def add_inactivity_days_num(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Adds `inactivity_days_num` feature used to filter users.

    Args:
        parameters: parameters defined in parameters.yml
        df: treatment features, must contain `last_activity_date`.
    """
    logging.getLogger(__name__).info("Adding inactivity days number")
    today = get_today(parameters)
    value_cap = 90
    last_activity_col = func.col("last_activity_date")
    date_difference = func.datediff(func.lit(today), last_activity_col)
    date_difference_capped = func.when(
        date_difference >= value_cap, value_cap
    ).otherwise(date_difference)
    inactivity_col = func.when(last_activity_col.isNull(), value_cap).otherwise(
        date_difference_capped
    )
    return df.withColumn("inactivity_days_num", inactivity_col)

def add_remain_validity(df: DataFrame, remain_validity: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """ Adds `remain_validity` feature used to filter users.

    Args:
        parameters: parameters defined in parameters.yml
        df: treatment features.
    """
    logging.getLogger(__name__).info("Adding Remain Validity")
    date_filter = remain_validity.selectExpr("MAX(event_partition_date)").collect()[0][0]
    remain_validity = remain_validity.filter(f"event_partition_date == '{date_filter}'")
    remain_validity = (
        remain_validity.withColumn(
            "key_date", func.date_format(func.col("event_partition_date"), "yyyy-MM-dd")
        )
        .withColumn(
            "subscription_identifier",
            func.concat(
                func.col("access_method_num"),
                func.lit("-"),
                func.date_format(func.col("register_date"), "yyyyMMdd"),
            ),
        )
        .select(["remain_validity", "subscription_identifier"])
    )

    return df.join(remain_validity, on=["subscription_identifier"], how="left")