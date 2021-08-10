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

number_of_campaign_target = 7000
testgroup_name = "BAU_TG"


def create_rule_based_daily_upsell_bau(
    l5_du_offer_blacklist: DataFrame,
    l5_du_offer_daily_eligible_list: DataFrame,
    data_upsell_usecase_control_group_2021,
    l4_data_ontop_package_preference: DataFrame,
    du_offer_score_with_package_preference: DataFrame,
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
    o_550B_UL6Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 4096 AND price_inc_vat_30_days < 550*1.07"
    ).limit(number_of_campaign_target)
    o_850B_UL6Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 4096"
    )

    o_450B_UL4Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 1024 AND price_inc_vat_30_days < 450*1.07"
    ).limit(number_of_campaign_target)
    o_300B_UL1Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 512 AND price_inc_vat_30_days < 300*1.07"
    ).limit(number_of_campaign_target)
    o_321B_UL512Kbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days <= 384"
    ).limit(number_of_campaign_target)
    o_32B_UL4Mbps1GB_1D = non_recuring.where(
        "duration_30_days = 1 AND data_quota_mb_30_days < 1024 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 1024 AND data_speed_30_days < 4096"
    ).limit(number_of_campaign_target)
    o_45B_UL6Mbps1_5GB_1D = non_recuring.where(
        "duration_30_days = 1 AND data_quota_mb_30_days < 1536 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 2048 AND data_speed_30_days < 6144"
    ).limit(number_of_campaign_target)
    o_120B_UL1Mbps2_5GB_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days < 2560 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 384 AND data_speed_30_days < 1024"
    ).limit(number_of_campaign_target)
    o_220B_UL4Mbps7GB_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days < 7168 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 512 AND data_speed_30_days < 4096"
    ).limit(number_of_campaign_target)
    o_270B_UL6Mbps9GB_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days < 9216 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 2048 AND data_speed_30_days < 6144"
    ).limit(number_of_campaign_target)
    o_300B_UL512Kbps7_5GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 7168 AND data_quota_mb_30_days > -1 AND data_speed_30_days < 512"
    ).limit(number_of_campaign_target)
    o_350B_UL1Mbps7_5GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 7680 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 384 AND data_speed_30_days < 1024"
    ).limit(number_of_campaign_target)
    o_650B_UL4Mbps20GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 20480 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 512 AND data_speed_30_days < 4096"
    ).limit(number_of_campaign_target)
    o_850B_UL6Mbps25GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 25600 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 1024 AND data_speed_30_days < 6144"
    ).limit(number_of_campaign_target)
    o_49B_3GB_1D = non_recuring.where(
        "duration_30_days = 1 AND data_quota_mb_30_days < 3072 AND data_quota_mb_30_days > -1 AND mm_data_speed_30_days = 'Full Speed'"
    ).limit(number_of_campaign_target)
    o_49B_6GB_1D = non_recuring.where(
        "duration_30_days = 1  AND data_quota_mb_30_days > -1 AND data_quota_mb_30_days < 6144 AND mm_data_speed_30_days = 'Full Speed' AND price_inc_vat_30_days > 35 AND price_inc_vat_30_days < 52.43"
    ).limit(number_of_campaign_target)
    o_199B_UL4Mbps_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 1024 AND price_inc_vat_30_days < 199*1.07"
    ).limit(number_of_campaign_target)
    o_189B_UL6Mbps_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 4096 AND price_inc_vat_30_days < 189*1.07"
    ).limit(number_of_campaign_target)
    o_550B_UL6Mbps_30D = o_550B_UL6Mbps_30D.withColumn(
        "model_name", F.lit("o_550B_UL6Mbps_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.28.11"))
    o_850B_UL6Mbps_30D = o_850B_UL6Mbps_30D.withColumn(
        "model_name", F.lit("o_850B_UL6Mbps_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.12"))
    o_450B_UL4Mbps_30D = o_450B_UL4Mbps_30D.withColumn(
        "model_name", F.lit("o_450B_UL4Mbps_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.28.10"))
    o_300B_UL1Mbps_30D = o_300B_UL1Mbps_30D.withColumn(
        "model_name", F.lit("o_300B_UL1Mbps_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.28.9"))
    o_321B_UL512Kbps_30D = o_321B_UL512Kbps_30D.withColumn(
        "model_name", F.lit("o_321B_UL512Kbps_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.9"))
    o_32B_UL4Mbps1GB_1D = o_32B_UL4Mbps1GB_1D.withColumn(
        "model_name", F.lit("o_32B_UL4Mbps1GB_1D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.15"))
    o_45B_UL6Mbps1_5GB_1D = o_45B_UL6Mbps1_5GB_1D.withColumn(
        "model_name", F.lit("o_45B_UL6Mbps1_5GB_1D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.16"))
    o_120B_UL1Mbps2_5GB_7D = o_120B_UL1Mbps2_5GB_7D.withColumn(
        "model_name", F.lit("o_120B_UL1Mbps2_5GB_7D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.18"))
    o_220B_UL4Mbps7GB_7D = o_220B_UL4Mbps7GB_7D.withColumn(
        "model_name", F.lit("o_220B_UL4Mbps7GB_7D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.19"))
    o_270B_UL6Mbps9GB_7D = o_270B_UL6Mbps9GB_7D.withColumn(
        "model_name", F.lit("o_270B_UL6Mbps9GB_7D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.20"))
    o_300B_UL512Kbps7_5GB_30D = o_300B_UL512Kbps7_5GB_30D.withColumn(
        "model_name", F.lit("o_300B_UL512Kbps7_5GB_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.21"))
    o_350B_UL1Mbps7_5GB_30D = o_350B_UL1Mbps7_5GB_30D.withColumn(
        "model_name", F.lit("o_350B_UL1Mbps7_5GB_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.22"))
    o_650B_UL4Mbps20GB_30D = o_650B_UL4Mbps20GB_30D.withColumn(
        "model_name", F.lit("o_650B_UL4Mbps20GB_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.23"))
    o_850B_UL6Mbps25GB_30D = o_850B_UL6Mbps25GB_30D.withColumn(
        "model_name", F.lit("o_850B_UL6Mbps25GB_30D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.24"))
    o_49B_3GB_1D = o_49B_3GB_1D.withColumn(
        "model_name", F.lit("o_49B_3GB_1D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.8.26"))
    o_49B_6GB_1D = o_49B_6GB_1D.withColumn(
        "model_name", F.lit("o_49B_6GB_1D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.28.5"))
    o_199B_UL4Mbps_7D = o_199B_UL4Mbps_7D.withColumn(
        "model_name", F.lit("o_199B_UL4Mbps_7D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.28.7"))
    o_189B_UL6Mbps_7D = o_189B_UL6Mbps_7D.withColumn(
        "model_name", F.lit("o_189B_UL6Mbps_7D")
    ).withColumn("campaign_child_code", F.lit("DataOTC.28.8"))
    all_manual_offering = (
        o_550B_UL6Mbps_30D.union(o_850B_UL6Mbps_30D)
        .union(o_450B_UL4Mbps_30D)
        .union(o_300B_UL1Mbps_30D)
        .union(o_321B_UL512Kbps_30D)
        .union(o_32B_UL4Mbps1GB_1D)
        .union(o_45B_UL6Mbps1_5GB_1D)
        .union(o_120B_UL1Mbps2_5GB_7D)
        .union(o_220B_UL4Mbps7GB_7D)
        .union(o_220B_UL4Mbps7GB_7D)
        .union(o_270B_UL6Mbps9GB_7D)
        .union(o_300B_UL512Kbps7_5GB_30D)
        .union(o_350B_UL1Mbps7_5GB_30D)
        .union(o_650B_UL4Mbps20GB_30D)
        .union(o_850B_UL6Mbps25GB_30D)
        .union(o_49B_3GB_1D)
        .union(o_49B_6GB_1D)
        .union(o_199B_UL4Mbps_7D)
        .union(o_189B_UL6Mbps_7D)
    )
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
