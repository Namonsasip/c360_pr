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


def create_tg_cg_list(
    group_name,
    group_name_cg,
    all_offer,
    du_campaign_offer_target,
    du_control_campaign_child_code,
):
    res = []
    expr = """CASE """
    for ele in du_campaign_offer_target:
        if du_campaign_offer_target[ele] is not None:
            res.append(ele)
            expr = (
                expr
                + """WHEN model_name = '"""
                + ele
                + """' THEN '"""
                + du_campaign_offer_target[ele]
                + """'
                    """
            )
    expr = expr + "END AS campaign_child_code"
    ATL_TG = all_offer.where("group_name = '" + group_name + "'")
    ATL_TG = ATL_TG.where(F.col("model_name").isin(res))
    non_downsell_offer_ATL_TG = ATL_TG.where("downsell_speed = 0 ").drop("is_optimal")
    non_downsell_offer_ATL_TG = non_downsell_offer_ATL_TG.selectExpr(
        "*",
        """CASE WHEN data_speed_30_days < 51200 AND offer_data_speed == 51200 THEN 1
                 ELSE 0 END AS convert_fix_full""",
        """CASE WHEN offer_data_quota_mb < 999999999 AND data_quota_mb_30_days == 999999999 THEN 1
                ELSE 0 END AS convert_fix_to_cap""",
    )
    non_downsell_offer_ATL_TG = non_downsell_offer_ATL_TG.where(
        "convert_fix_full = 0 AND convert_fix_to_cap = 0"
    )

    window_score = Window.partitionBy(F.col("subscription_identifier")).orderBy(
        F.col("propensity").desc()
    )
    window = Window.partitionBy(F.col("model_name")).orderBy(F.col("propensity").desc())

    du_offer_score_optimal_offer_ATL = non_downsell_offer_ATL_TG.select(
        "*", F.rank().over(window_score).alias("rank_offer")
    )

    du_offer_score_optimal_offer_ATL = du_offer_score_optimal_offer_ATL.where(
        "rank_offer = 1"
    )
    ATL_contact = du_offer_score_optimal_offer_ATL.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(F.col("rank") <= 0.06)

    tg = ATL_contact.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "register_date",
        "group_name",
        "group_flag",
        "subscription_status",
        "sum_rev_arpu_total_revenue_monthly_last_month",
        "propensity",
        "arpu_uplift",
        "expected_value",
        "downsell_speed",
        "downsell_duration",
        "model_name",
        expr,
        "day_of_week",
        "day_of_month",
        "offer_data_speed",
        "offer_data_quota_mb",
        "offer_duration",
        "offer_price_inc_vat",
        "package_name_report_30_days",
        "data_speed_30_days",
        "data_quota_mb_30_days",
        "duration_30_days",
        "price_inc_vat_30_days",
        "scoring_day",
    )

    ATL_CG = all_offer.where("group_name = '" + group_name_cg + "'")
    ATL_CG = ATL_CG.where(F.col("model_name").isin(res))
    non_downsell_offer_ATL_CG = ATL_CG.where("downsell_speed = 0").drop("is_optimal")
    non_downsell_offer_ATL_CG = non_downsell_offer_ATL_CG.selectExpr(
        "*",
        """CASE WHEN data_speed_30_days < 51200 AND offer_data_speed == 51200 THEN 1
                 ELSE 0 END AS convert_fix_full""",
        """CASE WHEN offer_data_quota_mb < 999999999 AND data_quota_mb_30_days == 999999999 THEN 1
                ELSE 0 END AS convert_fix_to_cap""",
    )
    non_downsell_offer_ATL_CG = non_downsell_offer_ATL_CG.where(
        "convert_fix_full = 0 AND convert_fix_to_cap = 0"
    )

    du_offer_score_optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.select(
        "*", F.rank().over(window_score).alias("rank_offer")
    )
    du_offer_score_optimal_offer_ATL_CG = du_offer_score_optimal_offer_ATL_CG.where(
        "rank_offer = 1"
    )
    ATL_control = du_offer_score_optimal_offer_ATL_CG.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(F.col("rank") <= 0.06)

    cg = ATL_control.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "register_date",
        "group_name",
        "group_flag",
        "subscription_status",
        "sum_rev_arpu_total_revenue_monthly_last_month",
        "propensity",
        "arpu_uplift",
        "expected_value",
        "downsell_speed",
        "downsell_duration",
        "model_name",
        "'"
        + du_control_campaign_child_code[group_name_cg]
        + "' as campaign_child_code",
        "day_of_week",
        "day_of_month",
        "offer_data_speed",
        "offer_data_quota_mb",
        "offer_duration",
        "offer_price_inc_vat",
        "package_name_report_30_days",
        "data_speed_30_days",
        "data_quota_mb_30_days",
        "duration_30_days",
        "price_inc_vat_30_days",
        "scoring_day",
    )
    return tg, cg


# this function apply upsell rule using ATL concept where the propensity value is use for upsell strategy
def generate_daily_eligible_list_new_experiment(
    l5_du_offer_score_optimal_offer: DataFrame,
    data_upsell_usecase_control_group_2021: DataFrame,
    l5_du_offer_blacklist: DataFrame,
    du_campaign_offer_new_experiment,
    du_control_campaign_child_code,
    unused_optimal_upsell: DataFrame,
    schema_name,
    prod_schema_name,
    dev_schema_name,
):

    spark = get_spark_session()
    l5_du_offer_score_optimal_offer = l5_du_offer_score_optimal_offer.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(
            datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
        )
        + "')"
    )
    l5_du_offer_score_optimal_offer = l5_du_offer_score_optimal_offer.where(
        """old_subscription_identifier is not null 
        AND subscription_status = 'SA' 
        AND sum_rev_arpu_total_revenue_monthly_last_month > 0"""
    )
    # We change column name to make it available to append into old schema for data storing wise
    all_offer = (
        data_upsell_usecase_control_group_2021.selectExpr(
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "usecase_control_group",
            "mckinsey_flag",
        )
        .withColumnRenamed("usecase_control_group", "group_name")
        .withColumnRenamed("mckinsey_flag", "group_flag")
        .join(
            l5_du_offer_score_optimal_offer,
            ["old_subscription_identifier", "register_date"],
            "inner",
        )
    )
    # Get latest blacklist end date of individual subscriber
    date_from = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        + datetime.timedelta(days=-2)
    )
    l5_du_offer_blacklist = l5_du_offer_blacklist.where(
        "date(scoring_day) >= date('" + date_from.strftime("%Y-%m-%d") + "')"
    )
    all_blacklisted_sub = l5_du_offer_blacklist.groupby("subscription_identifier").agg(
        F.max("black_listed_end_date").alias("blacklisted_end_date")
    )
    # Create blacklisted flag for anyone who still within the period of campaign offer period
    all_offer = all_offer.join(
        all_blacklisted_sub, ["subscription_identifier"], "left"
    ).selectExpr(
        "*",
        """CASE WHEN blacklisted_end_date >= scoring_day THEN 1 ELSE 0 END AS blacklisted""",
    )
    # Filter only people who are not blacklisted
    all_offer = all_offer.where("blacklisted = 0")

    TG, CG = create_tg_cg_list(
        "EXP_TG",
        "EXP_CG",
        all_offer,
        du_campaign_offer_new_experiment,
        du_control_campaign_child_code,
    )

    daily_eligible_list = TG.union(CG)
    daily_eligible_list = daily_eligible_list.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "register_date",
        "group_name",
        "group_flag",
        "subscription_status",
        "sum_rev_arpu_total_revenue_monthly_last_month",
        "propensity",
        "arpu_uplift",
        "expected_value",
        "downsell_speed",
        "downsell_duration",
        "model_name",
        "campaign_child_code",
        "day_of_week",
        "day_of_month",
        "offer_data_speed",
        "offer_data_quota_mb",
        "offer_duration",
        "offer_price_inc_vat",
        "package_name_report_30_days",
        "data_speed_30_days",
        "data_quota_mb_30_days",
        "duration_30_days",
        "price_inc_vat_30_days",
        "scoring_day",
    )

    daily_eligible_list.write.format("delta").mode("append").partitionBy(
        "scoring_day"
    ).saveAsTable(schema_name + ".du_offer_daily_eligible_list")

    return daily_eligible_list
