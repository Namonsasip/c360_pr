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


def apply_data_upsell_rules(
    du_campaign_offer_map_model,
    l5_du_offer_score_with_package_preference: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load,
    unused_memory_dataset_4: DataFrame,
    du_campaign_offer_atl_target,
    du_campaign_offer_btl1_target,
    du_campaign_offer_btl2_target,
    du_campaign_offer_btl3_target,
    du_control_campaign_child_code,
    schema_name,
    prod_schema_name,
    dev_schema_name,
):
    # l5_du_offer_score_with_package_preference = catalog.load(
    #     "l5_du_offer_score_with_package_preference"
    # )
    # du_campaign_offer_map_model = catalog.load("params:du_campaign_offer_map_model")
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
    #     "l0_campaign_tracking_contact_list_pre_full_load"
    # )
    # du_campaign_offer_atl_target = catalog.load("params:du_campaign_offer_atl_target")
    # du_campaign_offer_btl1_target = catalog.load("params:du_campaign_offer_btl1_target")
    # du_campaign_offer_btl2_target = catalog.load("params:du_campaign_offer_btl2_target")
    # du_campaign_offer_btl3_target = catalog.load("params:du_campaign_offer_btl3_target")
    # du_control_campaign_child_code = catalog.load(
    #     "params:du_control_campaign_child_code"
    # )
    spark = get_spark_session()
    res = []
    for ele in du_campaign_offer_map_model:
        if du_campaign_offer_map_model[ele] is not None:
            res.append(ele)
    max_day = (
        l5_du_offer_score_with_package_preference.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    l5_du_offer_score_with_package_preference = l5_du_offer_score_with_package_preference.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(max_day[0][1], "%Y-%m-%d")
        + "')"
    )
    l5_du_offer_score_with_package_preference = (
        l5_du_offer_score_with_package_preference.withColumn(
            "expected_value", F.col("propensity") * F.col("arpu_uplift")
        )
        .withColumn(
            "downsell_speed",
            F.when(
                F.col("data_speed_30_days") > F.col("offer_data_speed"), 1
            ).otherwise(0),
        )
        .withColumn(
            "downsell_duration",
            F.when(F.col("duration_30_days") > F.col("offer_duration"), 1).otherwise(0),
        )
    )
    l5_du_offer_score_with_package_preference_camp = l5_du_offer_score_with_package_preference.where(
        F.col("model_name").isin(res)
    )

    non_downsell_offer = l5_du_offer_score_with_package_preference_camp.where(
        "downsell_speed = 0 AND downsell_duration = 0"
    )

    # Smart next offer selection if campaign ignore
    all_campaign_child_codes = []
    expr = "CASE "
    for campaign in du_campaign_offer_atl_target:
        if (du_campaign_offer_atl_target[campaign] not in all_campaign_child_codes) & (
            du_campaign_offer_atl_target[campaign] is not None
        ):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_atl_target[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_atl_target[campaign])
    for campaign in du_campaign_offer_btl1_target:
        if (du_campaign_offer_btl1_target[campaign] not in all_campaign_child_codes) & (
            du_campaign_offer_btl1_target[campaign] is not None
        ):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_btl1_target[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_btl1_target[campaign])
    for campaign in du_campaign_offer_btl2_target:
        if (du_campaign_offer_btl2_target[campaign] not in all_campaign_child_codes) & (
            du_campaign_offer_btl2_target[campaign] is not None
        ):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_btl2_target[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_btl2_target[campaign])
    for campaign in du_campaign_offer_btl3_target:
        if (du_campaign_offer_btl3_target[campaign] not in all_campaign_child_codes) & (
            du_campaign_offer_btl3_target[campaign] is not None
        ):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_btl3_target[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_btl3_target[campaign])
    for campaign in du_control_campaign_child_code:
        if du_control_campaign_child_code[campaign] not in all_campaign_child_codes:
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_control_campaign_child_code[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_control_campaign_child_code[campaign])
    expr = expr + "END AS last_model_name"
    date_from = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        + datetime.timedelta(days=-45)
    )
    date_to = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        + datetime.timedelta(days=-1)
    )
    campaign_tracking_contact_list_pre = l0_campaign_tracking_contact_list_pre_full_load.filter(
        F.col("contact_date").between(date_from, date_to)
    )

    max_campaign_update = campaign_tracking_contact_list_pre.groupby(
        "subscription_identifier", "contact_date", "campaign_child_code"
    ).agg(F.max("update_date").alias("update_date"))
    campaign_updated = campaign_tracking_contact_list_pre.join(
        max_campaign_update,
        [
            "subscription_identifier",
            "contact_date",
            "campaign_child_code",
            "update_date",
        ],
        "inner",
    )
    upsell_campaigns = campaign_updated.where(
        campaign_updated.campaign_child_code.isin(all_campaign_child_codes)
    )

    max_offer_upsell_campaigns = upsell_campaigns.groupby(
        "subscription_identifier"
    ).agg(F.max("contact_date").alias("contact_date"))

    latest_upsell_campaigns = upsell_campaigns.join(
        max_offer_upsell_campaigns, ["subscription_identifier", "contact_date"], "inner"
    )
    today = datetime.datetime.now() + datetime.timedelta(hours=7)
    filtered_latest_upsell_campaigns = latest_upsell_campaigns.selectExpr(
        "*",
        """CASE WHEN campaign_child_code LIKE 'DataOTC.9%' THEN date_add(contact_date,15)
                WHEN campaign_child_code LIKE 'DataOTC.12%' THEN date_add(contact_date,30)
    ELSE date_add(contact_date,3) END as campaign_contact_end""",
    ).selectExpr(
        "subscription_identifier",
        "date(contact_date) as last_contact_date",
        "date(response_date) as last_response_date",
        "response",
        "campaign_child_code as last_campaign_child_code",
        expr,
        "CASE WHEN campaign_contact_end < date('"
        + today.strftime("%Y-%m-%d")
        + "') THEN 0 ELSE 1 END as contact_blacklisted",
        "CASE WHEN response = 'N' THEN 1 ELSE 0 END AS offer_blacklisted",
    )

    non_downsell_offer_bl = non_downsell_offer.join(
        filtered_latest_upsell_campaigns, ["subscription_identifier"], "left"
    )

    filtered_latest_upsell_campaigns.groupby("offer_blacklisted").agg(
        F.count("*")
    ).show()
    non_downsell_offer_non_bl = non_downsell_offer_bl.selectExpr(
        "*",
        "CASE WHEN last_model_name = model_name AND offer_blacklisted = 1 THEN 1 ELSE 0 end as drop_offer",
    ).where("drop_offer = 0 AND contact_blacklisted = 0")
    optimal_offer = non_downsell_offer_non_bl.groupby("subscription_identifier").agg(
        F.max("expected_value").alias("expected_value")
    )
    optimal_offer = optimal_offer.withColumn("is_optimal", F.lit(True))
    du_offer_score_optimal_offer = l5_du_offer_score_with_package_preference.join(
        optimal_offer, ["subscription_identifier", "expected_value"], "left"
    )
    if schema_name == dev_schema_name:
        spark.sql(
            """DROP TABLE IF EXISTS """
            + schema_name
            + """.du_offer_score_optimal_offer_rework"""
        )
        du_offer_score_optimal_offer.createOrReplaceTempView("tmp_tbl")
        spark.sql(
            """CREATE TABLE """
            + schema_name
            + """.du_offer_score_optimal_offer_rework
            AS
            SELECT * FROM tmp_tbl
        """
        )
    else:
        du_offer_score_optimal_offer.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_score_optimal_offer_rework")
    return du_offer_score_optimal_offer


def create_tg_cg_list(
    group_flag,
    group_flag_cg,
    all_offer,
    du_campaign_offer_target,
    du_control_campaign_child_code,
    tg_size,
    cg_size,
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
    ATL_TG = all_offer.where("group_flag = '" + group_flag + "'")
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
    # optimal_offer_ATL = non_downsell_offer_ATL_TG.groupby(
    #     "subscription_identifier"
    # ).agg(F.max("expected_value").alias("expected_value"))
    # optimal_offer_ATL = optimal_offer_ATL.withColumn("is_optimal", F.lit(True))
    # du_offer_score_optimal_offer_ATL = non_downsell_offer_ATL_TG.join(
    #     optimal_offer_ATL, ["subscription_identifier", "expected_value"], "left"
    # )
    if group_flag == "ATL_propensity_TG":
        window_score = Window.partitionBy(F.col("subscription_identifier")).orderBy(
            F.col("propensity").desc()
        )
        window = Window.partitionBy(F.col("model_name")).orderBy(
            F.col("propensity").desc()
        )
    else:
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
    ).filter(F.col("rank") <= 0.06)
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

    ATL_CG = all_offer.where("group_flag = '" + group_flag_cg + "'")
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
    # optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.groupby(
    #     "subscription_identifier"
    # ).agg(F.max("expected_value").alias("expected_value"))
    # optimal_offer_ATL_CG = optimal_offer_ATL_CG.withColumn("is_optimal", F.lit(True))
    # du_offer_score_optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.join(
    #     optimal_offer_ATL_CG, ["subscription_identifier", "expected_value"], "left"
    # )
    du_offer_score_optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.select(
        "*", F.rank().over(window_score).alias("rank_offer")
    )
    du_offer_score_optimal_offer_ATL_CG = du_offer_score_optimal_offer_ATL_CG.where(
        "rank_offer = 1"
    )
    ATL_control = du_offer_score_optimal_offer_ATL_CG.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(F.col("rank") <= 0.06)
    # ATL_control = (
    #     du_offer_score_optimal_offer_ATL_CG.where("is_optimal = 'true'")
    #     .sort(F.desc("expected_value"))
    #     .limit(cg_size)
    # )
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
        + du_control_campaign_child_code[group_flag_cg]
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


def generate_daily_eligible_list(
    l5_du_offer_score_optimal_offer: DataFrame,
    l0_du_pre_experiment3_groups: DataFrame,
    l5_du_offer_blacklist: DataFrame,
    du_campaign_offer_atl_target,
    du_campaign_offer_btl1_target,
    du_campaign_offer_btl2_target,
    du_campaign_offer_btl3_target,
    du_control_campaign_child_code,
    unused_optimal_upsell: DataFrame,
    schema_name,
    prod_schema_name,
    dev_schema_name,
):
    # l5_du_offer_blacklist = catalog.load("l5_du_offer_blacklist")
    # l5_du_offer_score_optimal_offer = catalog.load("l5_du_offer_score_optimal_offer")
    # l0_du_pre_experiment3_groups = catalog.load("l0_du_pre_experiment3_groups")
    # du_campaign_offer_atl_target = catalog.load("params:du_campaign_offer_atl_target")
    # du_campaign_offer_btl1_target = catalog.load("params:du_campaign_offer_btl1_target")
    # du_campaign_offer_btl2_target = catalog.load("params:du_campaign_offer_btl2_target")
    # du_campaign_offer_btl3_target = catalog.load("params:du_campaign_offer_btl3_target")
    # du_control_campaign_child_code = catalog.load(
    #     "params:du_control_campaign_child_code"
    # )
    spark = get_spark_session()
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

    all_offer = l0_du_pre_experiment3_groups.join(
        l5_du_offer_score_optimal_offer,
        ["old_subscription_identifier", "register_date"],
        "inner",
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

    ATL_uplift_TG, ATL_uplift_CG = create_tg_cg_list(
        "ATL_uplift_TG",
        "ATL_uplift_CG",
        all_offer,
        du_campaign_offer_atl_target,
        du_control_campaign_child_code,
        350000,
        22000,
    )

    ATL_contact, ATL_control = create_tg_cg_list(
        "ATL_propensity_TG",
        "ATL_propensity_CG",
        all_offer,
        du_campaign_offer_atl_target,
        du_control_campaign_child_code,
        350000,
        22000,
    )
    ATL_contact = ATL_contact.union(ATL_uplift_TG)
    ATL_control = ATL_control.union(ATL_uplift_CG)

    BTL1_contact, BTL1_control = create_tg_cg_list(
        "BTL1_TG",
        "BTL1_CG",
        all_offer,
        du_campaign_offer_btl1_target,
        du_control_campaign_child_code,
        172000,
        6000,
    )

    BTL2_contact, BTL2_control = create_tg_cg_list(
        "BTL2_TG",
        "BTL2_CG",
        all_offer,
        du_campaign_offer_btl2_target,
        du_control_campaign_child_code,
        172000,
        6000,
    )

    BTL3_contact, BTL3_control = create_tg_cg_list(
        "BTL3_TG",
        "BTL3_CG",
        all_offer,
        du_campaign_offer_btl3_target,
        du_control_campaign_child_code,
        700000,
        44000,
    )
    daily_eligible_list = (
        ATL_contact.union(ATL_control)
        .union(BTL1_contact)
        .union(BTL1_control)
        .union(BTL2_contact)
        .union(BTL2_control)
        .union(BTL3_contact)
        .union(BTL3_control)
    )

    if schema_name == dev_schema_name:
        spark.sql(
            """DROP TABLE IF EXISTS """
            + schema_name
            + """.du_offer_daily_eligible_list"""
        )
        daily_eligible_list.createOrReplaceTempView("tmp_tbl")
        spark.sql(
            """CREATE TABLE """
            + schema_name
            + """.du_offer_daily_eligible_list
            USING DELTA AS SELECT * FROM tmp_tbl"""
        )
    else:
        daily_eligible_list.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_daily_eligible_list")

    return daily_eligible_list


def create_target_list_file(
    l5_du_offer_daily_eligible_list: DataFrame,
    list_date,
    schema_name,
    prod_schema_name,
    dev_schema_name,
    target_list_path,
):
    # l5_du_offer_daily_eligible_list = catalog.load("l5_du_offer_daily_eligible_list")
    spark = get_spark_session()
    max_day = (
        l5_du_offer_daily_eligible_list.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    l5_du_offer_daily_eligible_list_latest = l5_du_offer_daily_eligible_list.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(max_day[0][1], "%Y-%m-%d")
        + "')"
    )
    if list_date is None:
        list_date = datetime.datetime.now() + datetime.timedelta(hours=7)
    follow_up_btl_campaign = l5_du_offer_daily_eligible_list_latest.where(
        "campaign_child_code LIKE 'DataOTC.12%' OR campaign_child_code LIKE 'DataOTC.9%' OR campaign_child_code LIKE 'DataOTC.28%'"
    ).dropDuplicates((["old_subscription_identifier"]))

    follow_up_btl_campaign_pdf = follow_up_btl_campaign.selectExpr(
        "date('" + list_date.strftime("%Y-%m-%d") + "') as data_date",
        "old_subscription_identifier",
        "campaign_child_code as dummy01",
        "'Upsell_Prepaid_BTL1' as project",
        "date_add(date('" + list_date.strftime("%Y-%m-%d") + "'),7) as expire_date",
    ).toPandas()
    follow_up_btl_campaign_pdf.to_csv(
        target_list_path
        + "DATA_UPSELL_PCM_BTL_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )

    ordinary_campaign = l5_du_offer_daily_eligible_list_latest.where(
        "campaign_child_code LIKE 'DataOTC.8%' "
    ).dropDuplicates((["old_subscription_identifier"]))
    ordinary_campaign_pdf = ordinary_campaign.selectExpr(
        "date('" + list_date.strftime("%Y-%m-%d") + "') as data_date",
        "old_subscription_identifier",
        "campaign_child_code as dummy01",
    ).toPandas()
    ordinary_campaign_pdf.to_csv(
        target_list_path
        + "DATA_UPSELL_PCM_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )
    to_blacklist = l5_du_offer_daily_eligible_list_latest.selectExpr(
        "*",
        (
            """CASE WHEN (campaign_child_code LIKE 'DataOTC.12%' OR campaign_child_code LIKE 'DataOTC.9%') 
            THEN date_add(date('"""
            + list_date.strftime("%Y-%m-%d")
            + """'),15) 
        WHEN (campaign_child_code LIKE 'DataOTC.8%' OR campaign_child_code LIKE 'DataOTC.28%') THEN  date_add(date('"""
            + list_date.strftime("%Y-%m-%d")
            + """'),4)  END as black_listed_end_date"""
        ),
    )
    if schema_name == dev_schema_name:
        spark.sql("""DROP TABLE IF EXISTS """ + schema_name + """.du_offer_blacklist""")
        to_blacklist.createOrReplaceTempView("tmp_tbl")
        spark.sql(
            """CREATE TABLE """
            + schema_name
            + """.du_offer_blacklist
        AS 
        SELECT * FROM tmp_tbl"""
        )
    else:
        to_blacklist.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_blacklist")
    return to_blacklist


def tmp_function():
    from pyspark.sql.functions import ntile
    from pyspark.sql.window import Window

    lift_df = du_offer_score_optimal_offer_ATL.selectExpr("*", "1 AS G")
    w = Window.partitionBy(lift_df.G).orderBy(lift_df.expected_value)
    decile_df = lift_df.select("*", ntile(100).over(w).alias("percentile"))
    decile_df.groupby("percentile").agg(
        F.min("propensity").alias("Min_propensity"),
        F.max("propensity").alias("Max_propensity"),
        F.mean("propensity").alias("Average_propensity"),
        F.min("arpu_uplift").alias("Min_arpu_uplift"),
        F.max("arpu_uplift").alias("Max_arpu_uplift"),
        F.mean("arpu_uplift").alias("Average_arpu_uplift"),
        F.min("expected_value").alias("Min_expected_value"),
        F.max("expected_value").alias("Max_expected_value"),
        F.mean("expected_value").alias("Average_expected_value"),
        F.countDistinct("old_subscription_identifier").alias("Distinct_sub"),
    ).toPandas().to_csv(
        "data/tmp/expected_value_distribution_3.csv",
        index=False,
        sep=",",
        header=True,
        encoding="utf-8-sig",
    )
    decile_df.groupby("percentile", "model_name").agg(
        F.min("propensity").alias("Min_propensity"),
        F.max("propensity").alias("Max_propensity"),
        F.mean("propensity").alias("Average_propensity"),
        F.min("arpu_uplift").alias("Min_arpu_uplift"),
        F.max("arpu_uplift").alias("Max_arpu_uplift"),
        F.mean("arpu_uplift").alias("Average_arpu_uplift"),
        F.min("expected_value").alias("Min_expected_value"),
        F.max("expected_value").alias("Max_expected_value"),
        F.mean("expected_value").alias("Average_expected_value"),
        F.countDistinct("old_subscription_identifier").alias("Distinct_sub"),
    ).toPandas().to_csv(
        "data/tmp/expected_value_distribution_over_offer_3.csv",
        index=False,
        sep=",",
        header=True,
        encoding="utf-8-sig",
    )


def create_weekly_full_list(
    group_flag,
    group_flag_cg,
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
    ATL_TG = all_offer.where("group_flag = '" + group_flag + "'")
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
    # optimal_offer_ATL = non_downsell_offer_ATL_TG.groupby(
    #     "subscription_identifier"
    # ).agg(F.max("expected_value").alias("expected_value"))
    # optimal_offer_ATL = optimal_offer_ATL.withColumn("is_optimal", F.lit(True))
    # du_offer_score_optimal_offer_ATL = non_downsell_offer_ATL_TG.join(
    #     optimal_offer_ATL, ["subscription_identifier", "expected_value"], "left"
    # )

    if group_flag == "ATL_propensity_TG":
        window_score = Window.partitionBy(F.col("subscription_identifier")).orderBy(
            F.col("propensity").desc()
        )
        window = Window.partitionBy(F.col("model_name")).orderBy(
            F.col("propensity").desc()
        )
    else:
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

    ATL_CG = all_offer.where("group_flag = '" + group_flag_cg + "'")
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
    # optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.groupby(
    #     "subscription_identifier"
    # ).agg(F.max("expected_value").alias("expected_value"))
    # optimal_offer_ATL_CG = optimal_offer_ATL_CG.withColumn("is_optimal", F.lit(True))
    # du_offer_score_optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.join(
    #     optimal_offer_ATL_CG, ["subscription_identifier", "expected_value"], "left"
    # )
    du_offer_score_optimal_offer_ATL_CG = non_downsell_offer_ATL_CG.select(
        "*", F.rank().over(window_score).alias("rank_offer")
    )
    du_offer_score_optimal_offer_ATL_CG = du_offer_score_optimal_offer_ATL_CG.where(
        "rank_offer = 1"
    )
    ATL_control = du_offer_score_optimal_offer_ATL_CG.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(F.col("rank") <= 1)
    # ATL_control = (
    #     du_offer_score_optimal_offer_ATL_CG.where("is_optimal = 'true'")
    #     .sort(F.desc("expected_value"))
    #     .limit(cg_size)
    # )
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
        + du_control_campaign_child_code[group_flag_cg]
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


def create_weekly_low_score_upsell_list(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l5_du_offer_score_optimal_offer: DataFrame,
    l0_du_pre_experiment3_groups: DataFrame,
    du_campaign_offer_atl_target_low_score,
    du_campaign_offer_btl3_target_low_score,
    du_control_campaign_child_code_low_score,
):
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

    all_offer = l0_du_pre_experiment3_groups.join(
        l5_du_offer_score_optimal_offer,
        ["old_subscription_identifier", "register_date"],
        "inner",
    )

    non_contacted_offers = all_offer.join(
        contacted_subs.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
        ),
        ["old_subscription_identifier", "register_date"],
        "leftanti",
    )

    ATL_contact, ATL_control = create_weekly_full_list(
        "ATL_propensity_TG",
        "ATL_propensity_CG",
        non_contacted_offers,
        du_campaign_offer_atl_target_low_score,
        du_control_campaign_child_code_low_score,
    )

    ATL_contact_up, ATL_control_up = create_weekly_full_list(
        "ATL_uplift_TG",
        "ATL_uplift_CG",
        non_contacted_offers,
        du_campaign_offer_atl_target_low_score,
        du_control_campaign_child_code_low_score,
    )
    ATL_contact = ATL_contact.union(ATL_contact_up)
    ATL_control = ATL_control.union(ATL_control_up)

    BTL1_contact, BTL1_control = create_weekly_full_list(
        "BTL1_TG",
        "BTL1_CG",
        non_contacted_offers,
        du_campaign_offer_atl_target_low_score,
        du_control_campaign_child_code_low_score,
    )

    BTL2_contact, BTL2_control = create_weekly_full_list(
        "BTL2_TG",
        "BTL2_CG",
        non_contacted_offers,
        du_campaign_offer_atl_target_low_score,
        du_control_campaign_child_code_low_score,
    )

    BTL3_contact, BTL3_control = create_weekly_full_list(
        "BTL3_TG",
        "BTL3_CG",
        non_contacted_offers,
        du_campaign_offer_btl3_target_low_score,
        du_control_campaign_child_code_low_score,
    )

    weekly_low_score_list = (
        ATL_contact.union(ATL_control)
        .union(BTL1_contact)
        .union(BTL1_control)
        .union(BTL2_contact)
        .union(BTL2_control)
        .union(BTL3_contact)
        .union(BTL3_control)
    )
    weekly_low_score_list.write.format("delta").mode("append").partitionBy(
        "scoring_day"
    ).saveAsTable("prod_dataupsell.weekly_low_score_list")
    return weekly_low_score_list


def create_weekly_low_score_target_list_file(
    l5_du_offer_weekly_low_score_list: DataFrame,
    unused_weekly_low_score_list: DataFrame,
    list_date,
):
    # l5_du_offer_daily_eligible_list = catalog.load("l5_du_offer_daily_eligible_list")
    max_day = (
        l5_du_offer_weekly_low_score_list.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    l5_du_offer_weekly_low_score_list_lastest = l5_du_offer_weekly_low_score_list.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(max_day[0][1], "%Y-%m-%d")
        + "')"
    )
    if list_date is None:
        list_date = datetime.datetime.now() + datetime.timedelta(hours=7)
    follow_up_btl_campaign = l5_du_offer_weekly_low_score_list_lastest.where(
        "campaign_child_code LIKE 'DataOTC.33%'"
    ).dropDuplicates((["old_subscription_identifier"]))

    follow_up_btl_campaign_pdf = follow_up_btl_campaign.selectExpr(
        "date('" + list_date.strftime("%Y-%m-%d") + "') as data_date",
        "old_subscription_identifier",
        "campaign_child_code as dummy01",
        "'Upsell_Prepaid_BTL1' as project",
        "date_add(date('" + list_date.strftime("%Y-%m-%d") + "'),7) as expire_date",
    ).toPandas()
    follow_up_btl_campaign_pdf.to_csv(
        "/dbfs/mnt/cvm02/cvm_output/MCK/DATAUP/PCM/DATA_UPSELL_PCM_BTL_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )

    ordinary_campaign = l5_du_offer_weekly_low_score_list_lastest.where(
        "campaign_child_code LIKE 'DataOTC.32%' "
    ).dropDuplicates((["old_subscription_identifier"]))
    ordinary_campaign_pdf = ordinary_campaign.selectExpr(
        "date('" + list_date.strftime("%Y-%m-%d") + "') as data_date",
        "old_subscription_identifier",
        "campaign_child_code as dummy01",
    ).toPandas()
    ordinary_campaign_pdf.to_csv(
        "/dbfs/mnt/cvm02/cvm_output/MCK/DATAUP/PCM/DATA_UPSELL_PCM_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )
    return l5_du_offer_weekly_low_score_list_lastest


def create_rule_based_daily_upsell(
    l5_du_offer_blacklist: DataFrame,
    l5_du_offer_daily_eligible_list: DataFrame,
    l4_data_ontop_package_preference: DataFrame,
    du_offer_score_with_package_preference: DataFrame,
    unused_optimal_upsell_2: DataFrame,
    schema_name,
    prod_schema_name,
    dev_schema_name,
):
    spark = get_spark_session()
    max_date = (
        l5_du_offer_daily_eligible_list.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    l5_du_offer_daily_eligible_list = l5_du_offer_daily_eligible_list.where(
        "scoring_day = date('" + max_date[0][1].strftime("%Y-%m-%d") + "')"
    )
    all_blacklisted_sub = (
        l5_du_offer_blacklist.groupby("old_subscription_identifier")
        .agg(F.max("black_listed_end_date").alias("blacklisted_end_date"))
        .where(
            "blacklisted_end_date >= date('"
            + max_date[0][1].strftime("%Y-%m-%d")
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
    o_550B_UL6Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 4096 AND price_inc_vat_30_days < 550*1.07"
    ).limit(10000)
    o_850B_UL6Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 4096"
    )
    o_850B_UL6Mbps_30D = o_850B_UL6Mbps_30D.join(
        o_550B_UL6Mbps_30D, ["access_method_num"], "left_anti"
    ).limit(10000)
    o_450B_UL4Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 1024 AND price_inc_vat_30_days < 450*1.07"
    ).limit(10000)
    o_300B_UL1Mbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 512 AND price_inc_vat_30_days < 300*1.07"
    ).limit(10000)
    o_321B_UL512Kbps_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days <= 384"
    ).limit(10000)
    o_32B_UL4Mbps1GB_1D = non_recuring.where(
        "duration_30_days = 1 AND data_quota_mb_30_days < 1024 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 1024 AND data_speed_30_days < 4096"
    ).limit(10000)
    o_45B_UL6Mbps1_5GB_1D = non_recuring.where(
        "duration_30_days = 1 AND data_quota_mb_30_days < 1536 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 2048 AND data_speed_30_days < 6144"
    ).limit(10000)
    o_120B_UL1Mbps2_5GB_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days < 2560 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 384 AND data_speed_30_days < 1024"
    ).limit(10000)
    o_220B_UL4Mbps7GB_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days < 7168 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 512 AND data_speed_30_days < 4096"
    ).limit(10000)
    o_270B_UL6Mbps9GB_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days < 9216 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 2048 AND data_speed_30_days < 6144"
    ).limit(10000)
    o_300B_UL512Kbps7_5GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 7168 AND data_quota_mb_30_days > -1 AND data_speed_30_days < 512"
    ).limit(10000)
    o_350B_UL1Mbps7_5GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 7680 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 384 AND data_speed_30_days < 1024"
    ).limit(10000)
    o_650B_UL4Mbps20GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 20480 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 512 AND data_speed_30_days < 4096"
    ).limit(10000)
    o_850B_UL6Mbps25GB_30D = non_recuring.where(
        "duration_30_days = 30 AND data_quota_mb_30_days < 25600 AND data_quota_mb_30_days > -1 AND data_speed_30_days > 1024 AND data_speed_30_days < 6144"
    ).limit(10000)
    o_49B_3GB_1D = non_recuring.where(
        "duration_30_days = 1 AND data_quota_mb_30_days < 3072 AND data_quota_mb_30_days > -1 AND mm_data_speed_30_days = 'Full Speed'"
    ).limit(10000)
    o_49B_6GB_1D = non_recuring.where(
        "duration_30_days = 1  AND data_quota_mb_30_days > -1 AND data_quota_mb_30_days < 6144 AND mm_data_speed_30_days = 'Full Speed' AND price_inc_vat_30_days > 35 AND price_inc_vat_30_days < 52.43"
    ).limit(10000)
    o_199B_UL4Mbps_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 1024 AND price_inc_vat_30_days < 199*1.07"
    ).limit(10000)
    o_189B_UL6Mbps_7D = non_recuring.where(
        "duration_30_days = 7 AND data_quota_mb_30_days = 999999999 AND data_speed_30_days = 4096 AND price_inc_vat_30_days < 189*1.07"
    ).limit(10000)
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
            ),
            ["old_subscription_identifier"],
            "inner",
        )
        .selectExpr(
            "subscription_identifier",
            "old_subscription_identifier",
            "register_date",
            "'Test-brief' as group_name",
            "'Test-brief' as group_flag",
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
        )
        .dropDuplicates(["old_subscription_identifier"])
    )

    if schema_name == dev_schema_name:
        final_daily_upsell_by_rule.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_daily_eligible_list")
    else:
        final_daily_upsell_by_rule.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_daily_eligible_list")

    return final_daily_upsell_by_rule
