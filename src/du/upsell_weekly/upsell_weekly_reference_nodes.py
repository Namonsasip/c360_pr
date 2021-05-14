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


def create_weekly_full_list(
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
        F.col("expected_value").desc()
    )
    window = Window.partitionBy(F.col("model_name")).orderBy(
        F.col("expected_value").desc()
    )

    du_offer_score_optimal_offer_ATL = non_downsell_offer_ATL_TG.select(
        "*", F.rank().over(window_score).alias("rank_offer")
    )
    du_offer_score_optimal_offer_ATL = du_offer_score_optimal_offer_ATL.where(
        "rank_offer = 1"
    )
    ATL_contact = du_offer_score_optimal_offer_ATL.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(F.col("rank") <= 1)
    # ATL_contact = (
    #     du_offer_score_optimal_offer_ATL.where("is_optimal = 'true'")
    #     .sort(F.desc("expected_value"))
    #     .limit(tg_size)
    # )
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

    ATL_CG = all_offer.where("group_flag = '" + group_name_cg + "'")
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
    ).filter(F.col("rank") <= 1)

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


def generate_weekly_eligible_list_reference(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l5_du_offer_score_optimal_offer: DataFrame,
    data_upsell_usecase_control_group_2021: DataFrame,
    du_campaign_child_code_low_score_reference,
    du_control_campaign_child_code_low_score_reference,
    unused_optimal_upsell: DataFrame,
    schema_name,
    prod_schema_name,
    dev_schema_name,
):
    spark = get_spark_session()
    today = datetime.datetime.now() + datetime.timedelta(hours=7)
    day_minus_nine = today + datetime.timedelta(days=-9)

    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        " date(contact_date) >= date('" + day_minus_nine.strftime("%Y-%m-%d") + "')"
    )
    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.where(
        """campaign_child_code in ('DataOTC.8.13','DataOTC.8.14','DataOTC.8.16',
        'DataOTC.8.1','DataOTC.8.2','DataOTC.8.3','DataOTC.8.4','DataOTC.8.17','DataOTC.8.18','DataOTC.8.5',
        'DataOTC.8.6','DataOTC.8.7','DataOTC.8.8','DataOTC.8.10','DataOTC.8.11','DataOTC.8.12','DataOTC.8.25',
        'DataOTC.8.26','DataOTC.8.29','DataOTC.8.51')
        OR campaign_child_code in ('DataOTC.9.1','DataOTC.9.2','DataOTC.9.3','DataOTC.9.6','DataOTC.9.7',
        'DataOTC.9.8','DataOTC.9.4','DataOTC.9.5','DataOTC.9.12')
        OR campaign_child_code in ('DataOTC.12.1','DataOTC.12.2','DataOTC.12.3','DataOTC.12.4','DataOTC.12.5',
        'DataOTC.12.6')
        OR campaign_child_code in ('DataOTC.28.1','DataOTC.28.2','DataOTC.28.3','DataOTC.28.6','DataOTC.8.29',
        'DataOTC.28.5','DataOTC.28.4','DataOTC.28.7','DataOTC.28.8','DataOTC.28.9','DataOTC.28.10','DataOTC.28.11',
        'DataOTC.28.12') """
    )

    contacted_subs = (
        dataupsell_contacted_campaign.groupby(
            "subscription_identifier", "register_date"
        )
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
    )

    max_day = (
        l5_du_offer_score_optimal_offer.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    l5_du_offer_score_optimal_offer = l5_du_offer_score_optimal_offer.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(max_day[0][1], "%Y-%m-%d")
        + "')"
    )
    l5_du_offer_score_optimal_offer = l5_du_offer_score_optimal_offer.where(
        """old_subscription_identifier is not null 
        AND subscription_status = 'SA' 
        AND sum_rev_arpu_total_revenue_monthly_last_month > 0"""
    )

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

    non_contacted_offers = all_offer.join(
        contacted_subs.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
        ),
        ["old_subscription_identifier", "register_date"],
        "leftanti",
    )

    TG, CG = create_weekly_full_list(
        "REF_TG",
        "REF_CG",
        non_contacted_offers,
        du_campaign_child_code_low_score_reference,
        du_control_campaign_child_code_low_score_reference,
    )

    weekly_low_score_list = TG.union(CG)
    weekly_low_score_list = weekly_low_score_list.selectExpr(
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
    if schema_name == dev_schema_name:
        spark.sql(
            """DROP TABLE IF EXISTS """
            + schema_name
            + """.weekly_low_score_list"""
        )
        weekly_low_score_list.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".weekly_low_score_list")
    else:
        spark.sql(
            "DELETE FROM "
            + schema_name
            + ".weekly_low_score_list WHERE scoring_day = date('"
            + datetime.datetime.strftime(
                datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
            )
            + "')"
        )
        weekly_low_score_list.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".weekly_low_score_list")
    return weekly_low_score_list
