import os
import re
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window, functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
from customer360.utilities.spark_util import get_spark_session
from pyspark.sql import DataFrame, Window
import datetime

number_of_campaign_target = 2000
testgroup_name = "EXP_TG"


def create_rule_based_daily_upsell_new_experiment(
    l5_du_offer_blacklist: DataFrame,
    l5_du_offer_daily_eligible_list: DataFrame,
    data_upsell_usecase_control_group_2021,
    l4_data_ontop_package_preference: DataFrame,
    du_offer_score_with_package_preference: DataFrame,
    du_rule_based_offer_params,
    delta_table_schema,
):
    spark = get_spark_session()
    l5_du_offer_daily_eligible_list = l5_du_offer_daily_eligible_list.where(
        "scoring_day = date('"
        + datetime.datetime.strftime(
            datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
        )
        + "')"
    )
    all_blacklisted_sub = (
        l5_du_offer_blacklist.groupby("old_subscription_identifier")
        .agg(F.max("black_listed_end_date").alias("blacklisted_end_date"))
        .where(
            "blacklisted_end_date >= date('"
            + datetime.datetime.strftime(
                datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
            )
            + "')"
        )
        .select("old_subscription_identifier")
    )
    all_blacklisted_sub = all_blacklisted_sub.union(
        l5_du_offer_daily_eligible_list.select("old_subscription_identifier")
    )
    max_date_pref = (
        l4_data_ontop_package_preference.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("start_of_week"))
        .collect()
    )
    l4_data_ontop_package_preference = l4_data_ontop_package_preference.where(
        "start_of_week = date('" + max_date_pref[0][1].strftime("%Y-%m-%d") + "')"
    )
    eligible_upsell = l4_data_ontop_package_preference.join(
        all_blacklisted_sub, ["old_subscription_identifier"], "left_anti"
    )
    non_recuring = eligible_upsell.where(
        "package_name_report_30_days not like '%recurr%'"
    )
    non_recuring = non_recuring.join(
        data_upsell_usecase_control_group_2021.selectExpr(
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "usecase_control_group as group_name",
            "mckinsey_flag as group_flag",
        ).where("group_name LIKE '" + testgroup_name + "'"),
        ["old_subscription_identifier", "register_date"],
        "inner",
    )
    #
    iterate_n = 1
    all_manual_offering = (
        non_recuring.where(
            "duration_30_days >= "
            + str(
                du_rule_based_offer_params["850B_UL6Mbps_30D"]["offer_duration_mt_eq"]
            )
            + " AND duration_30_days <= "
            + str(
                du_rule_based_offer_params["850B_UL6Mbps_30D"]["offer_duration_lt_eq"]
            )
            + " AND data_quota_mb_30_days >= "
            + str(du_rule_based_offer_params["850B_UL6Mbps_30D"]["data_qouta_mt_eq"])
            + " AND data_quota_mb_30_days <= "
            + str(du_rule_based_offer_params["850B_UL6Mbps_30D"]["data_qouta_lt_eq"])
            + " AND mm_data_speed_30_days >= "
            + str(du_rule_based_offer_params["850B_UL6Mbps_30D"]["data_speed_lt_eq"])
            + " AND mm_data_speed_30_days <= "
            + str(du_rule_based_offer_params["850B_UL6Mbps_30D"]["data_speed_lt_eq"])
            + " AND price_inc_vat_30_days < "
            + str(du_rule_based_offer_params["850B_UL6Mbps_30D"]["offer_price_exc_vat"])
            + " * 1.07"
        )
        .withColumn("model_name", F.lit("850B_UL6Mbps_30D"))
        .withColumn(
            "campaign_child_code",
            F.lit(
                du_rule_based_offer_params["850B_UL6Mbps_30D"]["campaign_child_code"]
            ),
        )
        .limit(number_of_campaign_target)
    )
    for rule_name in du_rule_based_offer_params:
        if iterate_n == 1:
            continue
        else:
            all_manual_offering = all_manual_offering.union(
                non_recuring.where(
                    "duration_30_days >= "
                    + str(du_rule_based_offer_params[rule_name]["offer_duration_mt_eq"])
                    + " AND duration_30_days <= "
                    + str(du_rule_based_offer_params[rule_name]["offer_duration_lt_eq"])
                    + " AND data_quota_mb_30_days >= "
                    + str(du_rule_based_offer_params[rule_name]["data_qouta_mt_eq"])
                    + " AND data_quota_mb_30_days <= "
                    + str(du_rule_based_offer_params[rule_name]["data_qouta_lt_eq"])
                    + " AND mm_data_speed_30_days >= "
                    + str(du_rule_based_offer_params[rule_name]["data_speed_lt_eq"])
                    + " AND mm_data_speed_30_days <= "
                    + str(du_rule_based_offer_params[rule_name]["data_speed_lt_eq"])
                    + " AND price_inc_vat_30_days < "
                    + str(du_rule_based_offer_params[rule_name]["offer_price_exc_vat"])
                    + " * 1.07"
                )
                .withColumn("model_name", F.lit(rule_name))
                .withColumn(
                    "campaign_child_code",
                    F.lit(du_rule_based_offer_params[rule_name]["campaign_child_code"]),
                )
                .limit(number_of_campaign_target)
            )
        iterate_n += 1

    max_du_offer_date = (
        du_offer_score_with_package_preference.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    du_offer_score_with_package_preference = du_offer_score_with_package_preference.where(
        "scoring_day = date('" + max_du_offer_date[0][1].strftime("%Y-%m-%d") + "')"
    )

    final_daily_upsell_by_rule = (
        du_offer_score_with_package_preference.groupby(
            "subscription_identifier",
            "old_subscription_identifier",
            "register_date",
            "subscription_status",
            "sum_rev_arpu_total_revenue_monthly_last_month",
            "day_of_week",
            "day_of_month",
            "scoring_day",
        )
        .agg(F.count("*"))
        .join(
            all_manual_offering.select(
                "old_subscription_identifier",
                "package_name_report_30_days",
                "data_speed_30_days",
                "data_quota_mb_30_days",
                "duration_30_days",
                "price_inc_vat_30_days",
                "model_name",
                "campaign_child_code",
                "group_name",
                "group_flag",
            ),
            ["old_subscription_identifier"],
            "inner",
        )
    )
    final_daily_upsell_by_rule = final_daily_upsell_by_rule.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "register_date",
        "group_name",
        "group_flag",
        "subscription_status",
        "sum_rev_arpu_total_revenue_monthly_last_month",
        "CAST(-0.99 AS DOUBLE) AS propensity",
        "CAST(-9999 AS DOUBLE) AS arpu_uplift",
        "CAST(-9999 AS DOUBLE) AS expected_value",
        "CAST(0 AS INT) as downsell_speed",
        "CAST(0 AS INT) as downsell_duration",
        "model_name",
        "campaign_child_code",
        "day_of_week",
        "day_of_month",
        "CAST(0 AS INT) as offer_data_speed",
        "CAST(-9999 AS STRING) as offer_data_quota_mb",
        "CAST(-9999 AS STRING) as offer_duration",
        "CAST(-999.9999 AS decimal(8, 4)) as offer_price_inc_vat",
        "package_name_report_30_days",
        "data_speed_30_days",
        "data_quota_mb_30_days",
        "duration_30_days",
        "price_inc_vat_30_days",
        "scoring_day",
    ).dropDuplicates(["old_subscription_identifier"])

    final_daily_upsell_by_rule.write.format("delta").mode("append").partitionBy(
        "scoring_day"
    ).saveAsTable(f"{delta_table_schema}.du_offer_daily_eligible_list")

    return final_daily_upsell_by_rule
