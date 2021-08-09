import os
import re
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
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

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import to_date, date_format
import datetime
from customer360.utilities.spark_util import get_spark_session
import calendar


def getmonthlenght(date_format):
    return calendar.monthrange(date_format.year, date_format.month)[1]


def l5_du_weekly_revenue_uplift_report_overall_contacted(
    l4_revenue_prepaid_daily_features: DataFrame,
    l0_du_pre_experiment3_groups: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l4_revenue_prepaid_pru_f_usage_multi_features: DataFrame,
    mapping_for_model_training,
    control_group_initialize_profile_date,
    owner_name,
):
    spark = get_spark_session()
    mapping_for_model_training = mapping_for_model_training.where(
        "owners = '" + owner_name + "'"
    )
    upsell_campaign_child_code = mapping_for_model_training.select(
        "rework_macro_product", "campaign_child_code"
    )
    upsell_campaign_child_code = upsell_campaign_child_code.union(
        spark.createDataFrame(
            pd.DataFrame(
                list(
                    zip(
                        [
                            "ATL_low_score_CG",
                            "BTL_low_score_CG",
                            "ATL_CG",
                            "BTL1_CG",
                            "BTL2_CG",
                            "BTL3_CG",
                        ],
                        [
                            "DataOTC.32.51",
                            "DataOTC.33.12",
                            "DataOTC.8.51",
                            "DataOTC.9.12",
                            "DataOTC.12.6",
                            "DataOTC.28.12",
                        ],
                    )
                ),
                columns=["rework_macro_product", "campaign_child_code"],
            )
        )
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        " date(contact_date) >= date('" + control_group_initialize_profile_date + "')"
    )
    l4_revenue_prepaid_pru_f_usage_multi_features = l4_revenue_prepaid_pru_f_usage_multi_features.where(
        "date(start_of_week) >= date('" + control_group_initialize_profile_date + "')"
    )
    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.join(
        upsell_campaign_child_code, ["campaign_child_code"], "inner"
    )
    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "month(contact_date) as monthofyear",
            "year(contact_date) as contactyear",
        )
        .withColumn(
            "weekofmonth", date_format(to_date("contact_date", "yyyy-MM-dd"), "W")
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "monthofyear",
            "contactyear",
            "weekofmonth",
        )
        .agg(
            F.min("contact_date").alias("contact_date"),
            F.count("*").alias("Total_campaign_sent_within_sub"),
        )
    )
    dataupsell_contacted_sub_selected = dataupsell_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "monthofyear",
        "register_date",
        "contactyear",
        "weekofmonth",
        "Total_campaign_sent_within_sub",
    )
    l4_revenue_prepaid_daily_availability = l4_revenue_prepaid_daily_features.selectExpr(
        "event_partition_date as start_of_week",
        "dayofweek(event_partition_date) as dow",
    ).where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )
    start_of_week_df = (
        l4_revenue_prepaid_daily_availability.where("dow = 2")
        .groupby("start_of_week")
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
    )
    start_of_week_df.persist()
    dataupsell_contacted_sub_weekly = dataupsell_contacted_sub_selected.join(
        start_of_week_df.selectExpr(
            "start_of_week",
            "month(start_of_week) as monthofyear",
            "year(start_of_week) as contactyear",
        ).withColumn(
            "weekofmonth", date_format(to_date("start_of_week", "yyyy-MM-dd"), "W")
        ),
        ["monthofyear", "contactyear", "weekofmonth"],
        "inner",
    )
    dataupsell_contacted_sub_weekly = dataupsell_contacted_sub_weekly.dropDuplicates(
        ["old_subscription_identifier", "start_of_week"]
    )
    l0_du_pre_experiment3_groups = l0_du_pre_experiment3_groups.selectExpr(
        "*",
        "date(CONCAT(year(control_group_created_date),'-',month(control_group_created_date),'-','01')) as start_of_month",
    )
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.selectExpr(
        "old_subscription_identifier",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    control_group_fix_key = l0_du_pre_experiment3_groups.join(
        l3_customer_profile_union_monthly_feature_full_load,
        ["old_subscription_identifier", "register_date", "start_of_month"],
        "inner",
    )
    df_customer_date_period = control_group_fix_key.join(
        dataupsell_contacted_sub_weekly,
        ["old_subscription_identifier", "register_date"],
        "inner",
    )
    data_revenue_before = l4_revenue_prepaid_pru_f_usage_multi_features.selectExpr(
        "subscription_identifier",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_week
           as data_revenue_last_seven_day""",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_two_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_two_week
           as data_revenue_last_fourteen_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_last_seven_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_last_fourteen_day""",
        "start_of_week",
    )
    data_revenue_after_seven_day = l4_revenue_prepaid_pru_f_usage_multi_features.selectExpr(
        "subscription_identifier",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_week
           as data_revenue_after_seven_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_after_seven_day""",
        "date_add(start_of_week,-7) as start_of_week",
    )
    data_revenue_after_fourteen_day = l4_revenue_prepaid_pru_f_usage_multi_features.selectExpr(
        "subscription_identifier",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_two_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_two_week
           as data_revenue_after_fourteen_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_after_fourteen_day""",
        "date_add(start_of_week,-14) as start_of_week",
    )
    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "event_partition_date as start_of_week",
    )
    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(event_partition_date,-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(event_partition_date,-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(event_partition_date,-30) as start_of_week",
    )
    revenue_report_df = (
        df_customer_date_period.join(
            revenue_before, ["subscription_identifier", "start_of_week"], "left"
        )
        .join(
            revenue_after_seven_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            revenue_after_fourteen_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            revenue_after_thirty_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            data_revenue_before, ["subscription_identifier", "start_of_week"], "left",
        )
        .join(
            data_revenue_after_seven_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            data_revenue_after_fourteen_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN group_name LIKE '%TG' THEN 'TG' 
                WHEN group_name LIKE '%CG' THEN 'CG' END AS compare_group""",
    )

    revenue_uplift_report_df_by_group = (
        revenue_report_df.groupby("compare_group", "start_of_week")
        .agg(
            F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
            F.sum("Total_campaign_sent_within_sub").alias("Total_campaign_sent"),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "Total_arpu_last_seven_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
                "Total_arpu_last_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "Total_arpu_last_thirty_day"
            ),
            F.sum("data_revenue_last_seven_day").alias("data_revenue_last_seven_day"),
            F.sum("data_revenue_last_fourteen_day").alias(
                "data_revenue_last_fourteen_day"
            ),
            F.sum("ontop_data_revenue_last_seven_day").alias(
                "ontop_data_revenue_last_seven_day"
            ),
            F.sum("ontop_data_revenue_last_fourteen_day").alias(
                "ontop_data_revenue_last_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
                "Total_arpu_after_seven_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
                "Total_arpu_after_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_thirty_day").alias(
                "Total_arpu_after_thirty_day"
            ),
            F.sum("data_revenue_after_seven_day").alias("data_revenue_after_seven_day"),
            F.sum("data_revenue_after_fourteen_day").alias(
                "data_revenue_after_fourteen_day"
            ),
            F.sum("ontop_data_revenue_after_seven_day").alias(
                "ontop_data_revenue_after_seven_day"
            ),
            F.sum("ontop_data_revenue_after_fourteen_day").alias(
                "ontop_data_revenue_after_fourteen_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "Avg_arpu_per_sub_last_seven_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
                "Avg_arpu_per_sub_last_fourteen_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "Avg_arpu_per_sub_last_thirty_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
                "Avg_arpu_per_sub_after_seven_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
                "Avg_arpu_per_sub_after_fourteen_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_after_thirty_day").alias(
                "Avg_arpu_per_sub_after_thirty_day"
            ),
            F.avg("data_revenue_last_seven_day").alias(
                "Avg_data_revenue_per_sub_last_seven_day"
            ),
            F.avg("data_revenue_last_fourteen_day").alias(
                "Avg_data_revenue_per_sub_last_fourteen_day"
            ),
            F.avg("data_revenue_after_seven_day").alias(
                "Avg_data_revenue_per_sub_after_seven_day"
            ),
            F.avg("data_revenue_after_fourteen_day").alias(
                "Avg_data_revenue_per_sub_after_fourteen_day"
            ),
            F.avg("ontop_data_revenue_last_seven_day").alias(
                "Avg_ontop_data_revenue_per_sub_last_seven_day"
            ),
            F.avg("ontop_data_revenue_last_fourteen_day").alias(
                "Avg_ontop_data_revenue_per_sub_last_fourteen_day"
            ),
            F.avg("ontop_data_revenue_after_seven_day").alias(
                "Avg_ontop_data_revenue_per_sub_after_seven_day"
            ),
            F.avg("ontop_data_revenue_after_fourteen_day").alias(
                "Avg_ontop_data_revenue_per_sub_after_fourteen_day"
            ),
        )
        .withColumn(
            "Arpu_uplift_seven_day",
            (
                F.col("Avg_arpu_per_sub_after_seven_day")
                - F.col("Avg_arpu_per_sub_last_seven_day")
            )
            / F.col("Avg_arpu_per_sub_last_seven_day"),
        )
        .withColumn(
            "Arpu_uplift_fourteen_day",
            (
                F.col("Avg_arpu_per_sub_after_fourteen_day")
                - F.col("Avg_arpu_per_sub_last_fourteen_day")
            )
            / F.col("Avg_arpu_per_sub_last_fourteen_day"),
        )
        .withColumn(
            "Arpu_uplift_thirty_day",
            (
                F.col("Avg_arpu_per_sub_after_thirty_day")
                - F.col("Avg_arpu_per_sub_last_thirty_day")
            )
            / F.col("Avg_arpu_per_sub_last_thirty_day"),
        )
        .withColumn(
            "Data_revenue_uplift_seven_day",
            (
                F.col("Avg_data_revenue_per_sub_after_seven_day")
                - F.col("Avg_data_revenue_per_sub_last_seven_day")
            )
            / F.col("Avg_data_revenue_per_sub_last_seven_day"),
        )
        .withColumn(
            "Data_revenue_uplift_fourteen_day",
            (
                F.col("Avg_data_revenue_per_sub_after_fourteen_day")
                - F.col("Avg_data_revenue_per_sub_last_fourteen_day")
            )
            / F.col("Avg_data_revenue_per_sub_last_fourteen_day"),
        )
        .withColumn(
            "Ontop_data_revenue_uplift_seven_day",
            (
                F.col("Avg_ontop_data_revenue_per_sub_after_seven_day")
                - F.col("Avg_ontop_data_revenue_per_sub_last_seven_day")
            )
            / F.col("Avg_ontop_data_revenue_per_sub_last_seven_day"),
        )
        .withColumn(
            "Ontop_data_revenue_uplift_fourteen_day",
            (
                F.col("Avg_ontop_data_revenue_per_sub_after_fourteen_day")
                - F.col("Avg_ontop_data_revenue_per_sub_last_fourteen_day")
            )
            / F.col("Avg_ontop_data_revenue_per_sub_last_fourteen_day"),
        )
        .withColumn(
            "Campaign_sent_per_contacted_sub",
            (F.col("Total_campaign_sent")) / (F.col("Number_of_distinct_subs")),
        )
    )
    return revenue_uplift_report_df_by_group


def l5_du_monthly_revenue_uplift_report(
    l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly: DataFrame,
    control_group_tbl: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    dm42_promotion_prepaid: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_initialize_profile_date,
    owner_name,
):
    spark = get_spark_session()
    l0_product_pru_m_ontop_master_for_weekly_full_load = l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
        "partition_date_str", F.col("partition_date").cast(StringType())
    ).select(
        "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("ddate")
    )
    max_master_date = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("ddate").alias("ddate"))
        .collect()
    )
    product_pru_m_ontop_master = l0_product_pru_m_ontop_master_for_weekly_full_load.where(
        "ddate = date('" + max_master_date[0][1].strftime("%Y-%m-%d") + "')"
    )
    dm42_promotion_prepaid = dm42_promotion_prepaid.where(
        "date(ddate) > date('" + control_group_initialize_profile_date + "')"
    )
    reccuring_transaction = dm42_promotion_prepaid.join(
        product_pru_m_ontop_master.drop("ddate").where("recurring = 'Y'"),
        ["promotion_code"],
        "inner",
    )
    reccuring_transaction = reccuring_transaction.selectExpr(
        "*", "DATE(CONCAT(YEAR(ddate),'-',MONTH(ddate),'-01')) as start_of_month"
    )
    reccuring_transaction.createOrReplaceTempView("tmp_load")
    spark.sql("DROP TABLE IF EXISTS prod_dataupsell.recurring_sub_monthly")
    spark.sql(
        """CREATE TABLE prod_dataupsell.recurring_sub_monthly 
        AS SELECT start_of_month,recurring,crm_subscription_id as old_subscription_identifier 
        FROM tmp_load 
        GROUP BY start_of_month,recurring,crm_subscription_id"""
    )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    mapping_for_model_training = mapping_for_model_training.where(
        "owners = '" + owner_name + "'"
    )
    upsell_campaign_child_code = mapping_for_model_training.select(
        "rework_macro_product", "campaign_child_code"
    )
    upsell_campaign_child_code = upsell_campaign_child_code.union(
        spark.createDataFrame(
            pd.DataFrame(
                list(
                    zip(
                        [
                            "ATL_low_score_CG",
                            "BTL_low_score_CG",
                            "ATL_CG",
                            "BTL1_CG",
                            "BTL2_CG",
                            "BTL3_CG",
                        ],
                        [
                            "DataOTC.32.51",
                            "DataOTC.33.12",
                            "DataOTC.8.51",
                            "DataOTC.9.12",
                            "DataOTC.12.6",
                            "DataOTC.28.12",
                        ],
                    )
                ),
                columns=["rework_macro_product", "campaign_child_code"],
            )
        )
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        " date(contact_date) >= date('" + control_group_initialize_profile_date + "')"
    )
    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.join(
        upsell_campaign_child_code, ["campaign_child_code"], "inner"
    )
    dataupsell_contacted_campaign = dataupsell_contacted_campaign.selectExpr(
        "*",
        """CASE WHEN ( campaign_child_code LIKE 'DataOTC.8%') OR (campaign_child_code LIKE 'DataOTC.10%')
                        OR (campaign_child_code LIKE 'DataOTC.11%') OR (campaign_child_code LIKE 'DataOTC.12%')
                        OR (campaign_child_code LIKE 'DataOTC.28%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
    )
    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "DATE(CONCAT(year(contact_date),'-',month(contact_date),'-01')) as start_of_month",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_month",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_month"],
        "left_anti",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        low_score_contacted_sub
    )
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly = l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_revenue",
        "rev_arpu_total_gprs_net_revenue",
        "start_of_month",
    )
    arpu_before = l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_revenue as rev_arpu_total_revenue_before",
        "rev_arpu_total_gprs_net_revenue as rev_arpu_total_gprs_net_revenue_before",
        "add_months(start_of_month,1) as start_of_month",
    )
    arpu_after = l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_revenue as rev_arpu_total_revenue_after",
        "rev_arpu_total_gprs_net_revenue as rev_arpu_total_gprs_net_revenue_after",
        "add_months(start_of_month,-1) as start_of_month",
    )
    ontop_revenue_monthly = (
        dm42_promotion_prepaid.selectExpr(
            "crm_subscription_id as old_subscription_identifier",
            "register_date",
            "total_net_tariff",
            "date(concat(year(date_id),'-',month(date_id),'-01')) as start_of_month",
        )
        .groupby("old_subscription_identifier", "register_date", "start_of_month")
        .agg(F.sum("total_net_tariff").alias("ontop_revenue"))
    )
    ontop_revenue_monthly.persist()

    ontop_revenue_monthly_before = ontop_revenue_monthly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "ontop_revenue as ontop_revenue_before",
        "add_months(start_of_month,1) as start_of_month",
    )

    ontop_revenue_monthly_after = ontop_revenue_monthly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "ontop_revenue as ontop_revenue_after",
        "add_months(start_of_month,-1) as start_of_month",
    )
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected,
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(
            recurring_sub,
            ["old_subscription_identifier", "start_of_month"],
            "left",
        )
    )
    revenue_report_df = (
        df_customer_date_period.join(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly,
            ["subscription_identifier", "start_of_month"],
            "left",
        )
        .join(
            arpu_before,
            ["subscription_identifier", "start_of_month"],
            "left",
        )
        .join(
            arpu_after,
            ["subscription_identifier", "start_of_month"],
            "left",
        )
        .join(
            ontop_revenue_monthly,
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(
            ontop_revenue_monthly_before,
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(
            ontop_revenue_monthly_after,
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN score_priority is not null then score_priority
                ELSE 'Not-contact' END as upsell_coverage """,
        "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn",
    )

    revenue_uplift_report_df = revenue_report_df.groupby(
        "group_name", "start_of_month", "recurring_yn", "upsell_coverage"
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("rev_arpu_total_revenue_before").alias("rev_arpu_total_revenue_before"),
        F.sum("rev_arpu_total_gprs_net_revenue_before").alias(
            "rev_arpu_total_gprs_net_revenue_before"
        ),
        F.sum("ontop_revenue_before").alias("ontop_revenue_before"),
        F.sum("rev_arpu_total_revenue").alias("rev_arpu_total_revenue_present"),
        F.sum("rev_arpu_total_gprs_net_revenue").alias(
            "rev_arpu_total_gprs_net_revenue_present"
        ),
        F.sum("ontop_revenue").alias("ontop_revenue_present"),
        F.sum("rev_arpu_total_revenue_after").alias("rev_arpu_total_revenue_after"),
        F.sum("rev_arpu_total_gprs_net_revenue_after").alias(
            "rev_arpu_total_gprs_net_revenue_after"
        ),
        F.sum("ontop_revenue_after").alias("ontop_revenue_after"),
        F.avg("rev_arpu_total_revenue_before").alias(
            "rev_arpu_total_revenue_per_sub_before"
        ),
        F.avg("rev_arpu_total_gprs_net_revenue_before").alias(
            "rev_arpu_total_gprs_net_revenue_per_sub_before"
        ),
        F.avg("ontop_revenue_before").alias("ontop_revenue_per_sub_before"),
        F.avg("rev_arpu_total_revenue").alias("rev_arpu_total_revenue_per_sub_present"),
        F.avg("rev_arpu_total_gprs_net_revenue").alias(
            "rev_arpu_total_gprs_net_revenue_per_sub_present"
        ),
        F.avg("ontop_revenue").alias("ontop_revenue_per_sub_present"),
        F.avg("rev_arpu_total_revenue_after").alias(
            "rev_arpu_total_revenue_per_sub_after"
        ),
        F.avg("rev_arpu_total_gprs_net_revenue_after").alias(
            "rev_arpu_total_gprs_net_revenue_per_sub_after"
        ),
        F.avg("ontop_revenue_after").alias("ontop_revenue_per_sub_after"),
    )
    # udf_getmonthlenght = F.udf(getmonthlenght, IntegerType())
    # revenue_uplift_report_df = revenue_uplift_report_df.withColumn(
    #     "dayinmonth", udf_getmonthlenght(F.col("start_of_month"))
    # )
    # revenue_uplift_report_df = (
    #     revenue_uplift_report_df.withColumn(
    #         "rev_arpu_total_revenue_per_sub_per_day_before",
    #         F.col("rev_arpu_total_revenue_per_sub_before") / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "rev_arpu_total_gprs_net_revenue_per_sub_per_day_before",
    #         F.col("rev_arpu_total_gprs_net_revenue_per_sub_before")
    #         / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "ontop_revenue_per_sub_per_day_before",
    #         F.col("ontop_revenue_per_sub_before") / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "rev_arpu_total_revenue_per_sub_per_day_present",
    #         F.col("rev_arpu_total_revenue_per_sub_present") / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "rev_arpu_total_gprs_net_revenue_per_sub_per_day_present",
    #         F.col("rev_arpu_total_gprs_net_revenue_per_sub_present")
    #         / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "ontop_revenue_per_sub_per_day_present",
    #         F.col("ontop_revenue_per_sub_present") / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "rev_arpu_total_revenue_per_sub_per_day_after",
    #         F.col("rev_arpu_total_revenue_per_sub_after") / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "rev_arpu_total_gprs_net_revenue_per_sub_per_day_after",
    #         F.col("rev_arpu_total_gprs_net_revenue_per_sub_after")
    #         / F.col("dayinmonth"),
    #     )
    #     .withColumn(
    #         "ontop_revenue_per_sub_per_day_after",
    #         F.col("ontop_revenue_per_sub_after") / F.col("dayinmonth"),
    #     )
    # )
    return revenue_uplift_report_df


def l5_du_weekly_revenue_uplift_report_contacted_only(
    l4_revenue_prepaid_daily_features: DataFrame,
    l0_du_pre_experiment3_groups: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l4_revenue_prepaid_pru_f_usage_multi_features: DataFrame,
    dm42_promotion_prepaid: DataFrame,
    control_group_initialize_profile_date,
):
    # control_group_initialize_profile_date = "2020-08-01"
    # l0_du_pre_experiment3_groups = catalog.load("l0_du_pre_experiment3_groups")
    # l4_revenue_prepaid_daily_features = catalog.load(
    #     "l4_revenue_prepaid_daily_features"
    # )
    # l3_customer_profile_union_monthly_feature_full_load = catalog.load(
    #     "l3_customer_profile_union_monthly_feature_full_load"
    # )
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
    #     "l0_campaign_tracking_contact_list_pre_full_load"
    # )
    spark = get_spark_session()
    l0_product_pru_m_ontop_master_for_weekly_full_load = l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
        "partition_date_str", F.col("partition_date").cast(StringType())
    ).select(
        "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("ddate")
    )
    l4_revenue_prepaid_pru_f_usage_multi_features = l4_revenue_prepaid_pru_f_usage_multi_features.where(
        "date(start_of_week) >= date('" + control_group_initialize_profile_date + "')"
    )
    max_master_date = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("ddate").alias("ddate"))
        .collect()
    )
    product_pru_m_ontop_master = l0_product_pru_m_ontop_master_for_weekly_full_load.where(
        "ddate = date('" + max_master_date[0][1].strftime("%Y-%m-%d") + "')"
    )
    dm42_promotion_prepaid = dm42_promotion_prepaid.where(
        "date(ddate) > date('" + control_group_initialize_profile_date + "')"
    )
    reccuring_transaction = dm42_promotion_prepaid.join(
        product_pru_m_ontop_master.drop("ddate").where("recurring = 'Y'"),
        ["promotion_code"],
        "inner",
    )
    reccuring_transaction = reccuring_transaction.selectExpr(
        "*", "DATE(CONCAT(YEAR(ddate),'-',MONTH(ddate),'-01')) as start_of_month"
    )
    reccuring_transaction.createOrReplaceTempView("tmp_load")
    spark.sql("DROP TABLE IF EXISTS prod_dataupsell.recurring_sub_monthly")
    spark.sql(
        """CREATE TABLE prod_dataupsell.recurring_sub_monthly 
        AS SELECT start_of_month,recurring,crm_subscription_id as old_subscription_identifier 
        FROM tmp_load 
        GROUP BY start_of_month,recurring,crm_subscription_id"""
    )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        " date(contact_date) >= date('" + control_group_initialize_profile_date + "')"
    )
    l0_campaign_tracking_contact_list_pre_full_load.withColumnRenamed(
        "subscription_identifier", "old_subscription_identifier"
    ).where("campaign_child_code = 'DataOTC.9.12'").drop("register_date").join(
        l0_du_pre_experiment3_groups, ["old_subscription_identifier"], "inner"
    ).groupby(
        "contact_date"
    ).agg(
        F.count("*")
    ).orderBy(
        "contact_date"
    ).show(
        100
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
    l0_du_pre_experiment3_groups.show()
    dataupsell_contacted_campaign = (
        dataupsell_contacted_campaign.join(
            l0_du_pre_experiment3_groups.selectExpr(
                "old_subscription_identifier as subscription_identifier", "group_name"
            ),
            ["subscription_identifier"],
            "inner",
        )
        .selectExpr(
            "*",
            """CASE WHEN group_name IN ('ATL_propensity_CG','ATL_uplift_CG') AND campaign_child_code = 'DataOTC.8.51' THEN 1
                             WHEN group_name IN ('ATL_uplift_TG','ATL_propensity_TG')
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                             WHEN group_name = 'BTL1_CG' AND campaign_child_code = 'DataOTC.9.12' THEN 1
                             WHEN group_name = 'BTL1_TG'
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                             WHEN group_name = 'BTL2_CG' AND campaign_child_code = 'DataOTC.12.6' THEN 1
                             WHEN group_name = 'BTL2_TG'
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                             WHEN group_name = 'BTL3_CG' AND campaign_child_code = 'DataOTC.28.12' THEN 1
                             WHEN group_name = 'BTL3_TG'
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                    ELSE 0
                    END AS correct_flag""",
        )
        .where("correct_flag = 1")
        .drop("group_name")
    )
    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "month(contact_date) as monthofyear",
            "year(contact_date) as contactyear",
        )
        .withColumn(
            "weekofmonth", date_format(to_date("contact_date", "yyyy-MM-dd"), "W")
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "monthofyear",
            "contactyear",
            "weekofmonth",
        )
        .agg(
            F.min("contact_date").alias("contact_date"),
            F.count("*").alias("Total_campaign_sent_within_sub"),
        )
    )

    dataupsell_contacted_campaign.drop("register_date").withColumnRenamed(
        "subscription_identifier", "old_subscription_identifier"
    ).join(
        l0_du_pre_experiment3_groups, ["old_subscription_identifier"], "inner"
    ).where(
        "campaign_child_code = 'DataOTC.9.12' "
    ).groupby(
        "contact_month", "group_name"
    ).agg(
        F.count("*")
    ).show()

    dataupsell_contacted_sub_selected = dataupsell_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "monthofyear",
        "register_date",
        "contactyear",
        "weekofmonth",
        "Total_campaign_sent_within_sub",
    )

    l4_revenue_prepaid_daily_availability = l4_revenue_prepaid_daily_features.selectExpr(
        "event_partition_date as start_of_week",
        "dayofweek(event_partition_date) as dow",
    ).where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )
    start_of_week_df = (
        l4_revenue_prepaid_daily_availability.where("dow = 2")
        .groupby("start_of_week")
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
    )
    start_of_week_df.persist()

    dataupsell_contacted_sub_weekly = dataupsell_contacted_sub_selected.join(
        start_of_week_df.selectExpr(
            "start_of_week",
            "month(start_of_week) as monthofyear",
            "year(start_of_week) as contactyear",
        ).withColumn(
            "weekofmonth", date_format(to_date("start_of_week", "yyyy-MM-dd"), "W")
        ),
        ["monthofyear", "contactyear", "weekofmonth"],
        "inner",
    )
    dataupsell_contacted_sub_weekly = dataupsell_contacted_sub_weekly.dropDuplicates(
        ["old_subscription_identifier", "start_of_week"]
    )
    dataupsell_contacted_sub_weekly.groupby("start_of_week").agg(
        F.countDistinct("old_subscription_identifier"),
        F.sum("Total_campaign_sent_within_sub"),
    ).show()

    l0_du_pre_experiment3_groups = l0_du_pre_experiment3_groups.selectExpr(
        "*",
        "date(CONCAT(year(control_group_created_date),'-',month(control_group_created_date),'-','01')) as start_of_month",
    )

    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.selectExpr(
        "old_subscription_identifier",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    control_group_fix_key = l0_du_pre_experiment3_groups.join(
        l3_customer_profile_union_monthly_feature_full_load,
        ["old_subscription_identifier", "register_date", "start_of_month"],
        "inner",
    )
    df_customer_date_period = control_group_fix_key.join(
        dataupsell_contacted_sub_weekly,
        ["old_subscription_identifier", "register_date"],
        "inner",
    )

    data_revenue_before = l4_revenue_prepaid_pru_f_usage_multi_features.selectExpr(
        "subscription_identifier",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_week
           as data_revenue_last_seven_day""",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_two_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_two_week
           as data_revenue_last_fourteen_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_last_seven_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_last_fourteen_day""",
        "start_of_week",
    )
    data_revenue_after_seven_day = l4_revenue_prepaid_pru_f_usage_multi_features.selectExpr(
        "subscription_identifier",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_week
           as data_revenue_after_seven_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_after_seven_day""",
        "date_add(start_of_week,-7) as start_of_week",
    )
    data_revenue_after_fourteen_day = l4_revenue_prepaid_pru_f_usage_multi_features.selectExpr(
        "subscription_identifier",
        """sum_rev_arpu_data_rev_2g_3g_sum_weekly_last_two_week + sum_rev_arpu_data_rev_4g_sum_weekly_last_two_week
           as data_revenue_after_fourteen_day""",
        """sum_rev_arpu_data_rev_by_on_top_pkg_sum_weekly_last_week as ontop_data_revenue_after_fourteen_day""",
        "date_add(start_of_week,-14) as start_of_week",
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "event_partition_date as start_of_week",
    )
    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(event_partition_date,-7) as start_of_week",
    )

    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(event_partition_date,-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(event_partition_date,-30) as start_of_week",
    )
    revenue_report_df = (
        df_customer_date_period.selectExpr(
            "*",
            "DATE(CONCAT(YEAR(start_of_week),'-',MONTH(start_of_week),'-01')) as start_of_month",
        )
        .join(revenue_before, ["subscription_identifier", "start_of_week"], "left")
        .join(
            revenue_after_seven_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            revenue_after_fourteen_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            revenue_after_thirty_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            data_revenue_before, ["subscription_identifier", "start_of_week"], "left",
        )
        .join(
            data_revenue_after_seven_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(
            data_revenue_after_fourteen_day,
            ["subscription_identifier", "start_of_week"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left")
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*", "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn"
    )
    revenue_uplift_report_df = (
        revenue_report_df.groupby("group_name", "start_of_week", "recurring_yn")
        .agg(
            F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
            F.sum("Total_campaign_sent_within_sub").alias("Total_campaign_sent"),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "Total_arpu_last_seven_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
                "Total_arpu_last_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "Total_arpu_last_thirty_day"
            ),
            F.sum("data_revenue_last_seven_day").alias("data_revenue_last_seven_day"),
            F.sum("data_revenue_last_fourteen_day").alias(
                "data_revenue_last_fourteen_day"
            ),
            F.sum("ontop_data_revenue_last_seven_day").alias(
                "ontop_data_revenue_last_seven_day"
            ),
            F.sum("ontop_data_revenue_last_fourteen_day").alias(
                "ontop_data_revenue_last_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
                "Total_arpu_after_seven_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
                "Total_arpu_after_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_thirty_day").alias(
                "Total_arpu_after_thirty_day"
            ),
            F.sum("data_revenue_after_seven_day").alias("data_revenue_after_seven_day"),
            F.sum("data_revenue_after_fourteen_day").alias(
                "data_revenue_after_fourteen_day"
            ),
            F.sum("ontop_data_revenue_after_seven_day").alias(
                "ontop_data_revenue_after_seven_day"
            ),
            F.sum("ontop_data_revenue_after_fourteen_day").alias(
                "ontop_data_revenue_after_fourteen_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "Avg_arpu_per_sub_last_seven_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
                "Avg_arpu_per_sub_last_fourteen_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "Avg_arpu_per_sub_last_thirty_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
                "Avg_arpu_per_sub_after_seven_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
                "Avg_arpu_per_sub_after_fourteen_day"
            ),
            F.avg("sum_rev_arpu_total_net_rev_daily_after_thirty_day").alias(
                "Avg_arpu_per_sub_after_thirty_day"
            ),
            F.avg("data_revenue_last_seven_day").alias(
                "Avg_data_revenue_per_sub_last_seven_day"
            ),
            F.avg("data_revenue_last_fourteen_day").alias(
                "Avg_data_revenue_per_sub_last_fourteen_day"
            ),
            F.avg("data_revenue_after_seven_day").alias(
                "Avg_data_revenue_per_sub_after_seven_day"
            ),
            F.avg("data_revenue_after_fourteen_day").alias(
                "Avg_data_revenue_per_sub_after_fourteen_day"
            ),
            F.avg("ontop_data_revenue_last_seven_day").alias(
                "Avg_ontop_data_revenue_per_sub_last_seven_day"
            ),
            F.avg("ontop_data_revenue_last_fourteen_day").alias(
                "Avg_ontop_data_revenue_per_sub_last_fourteen_day"
            ),
            F.avg("ontop_data_revenue_after_seven_day").alias(
                "Avg_ontop_data_revenue_per_sub_after_seven_day"
            ),
            F.avg("ontop_data_revenue_after_fourteen_day").alias(
                "Avg_ontop_data_revenue_per_sub_after_fourteen_day"
            ),
        )
        .withColumn(
            "Arpu_uplift_seven_day",
            (
                F.col("Avg_arpu_per_sub_after_seven_day")
                - F.col("Avg_arpu_per_sub_last_seven_day")
            )
            / F.col("Avg_arpu_per_sub_last_seven_day"),
        )
        .withColumn(
            "Arpu_uplift_fourteen_day",
            (
                F.col("Avg_arpu_per_sub_after_fourteen_day")
                - F.col("Avg_arpu_per_sub_last_fourteen_day")
            )
            / F.col("Avg_arpu_per_sub_last_fourteen_day"),
        )
        .withColumn(
            "Arpu_uplift_thirty_day",
            (
                F.col("Avg_arpu_per_sub_after_thirty_day")
                - F.col("Avg_arpu_per_sub_last_thirty_day")
            )
            / F.col("Avg_arpu_per_sub_last_thirty_day"),
        )
        .withColumn(
            "Data_revenue_uplift_seven_day",
            (
                F.col("Avg_data_revenue_per_sub_after_seven_day")
                - F.col("Avg_data_revenue_per_sub_last_seven_day")
            )
            / F.col("Avg_data_revenue_per_sub_last_seven_day"),
        )
        .withColumn(
            "Data_revenue_uplift_fourteen_day",
            (
                F.col("Avg_data_revenue_per_sub_after_fourteen_day")
                - F.col("Avg_data_revenue_per_sub_last_fourteen_day")
            )
            / F.col("Avg_data_revenue_per_sub_last_fourteen_day"),
        )
        .withColumn(
            "Ontop_data_revenue_uplift_seven_day",
            (
                F.col("Avg_ontop_data_revenue_per_sub_after_seven_day")
                - F.col("Avg_ontop_data_revenue_per_sub_last_seven_day")
            )
            / F.col("Avg_ontop_data_revenue_per_sub_last_seven_day"),
        )
        .withColumn(
            "Ontop_data_revenue_uplift_fourteen_day",
            (
                F.col("Avg_ontop_data_revenue_per_sub_after_fourteen_day")
                - F.col("Avg_ontop_data_revenue_per_sub_last_fourteen_day")
            )
            / F.col("Avg_ontop_data_revenue_per_sub_last_fourteen_day"),
        )
        .withColumn(
            "Campaign_sent_per_contacted_sub",
            (F.col("Total_campaign_sent")) / (F.col("Number_of_distinct_subs")),
        )
    )
    # l5_du_weekly_revenue_uplift_report_contacted_only = catalog.load("l5_du_weekly_revenue_uplift_report_contacted_only")
    # l5_du_weekly_revenue_uplift_report_contacted_only.toPandas().to_csv('data/tmp/data_upsell_revenue_report_10012021_2.csv', index=False,header=True)
    return revenue_uplift_report_df


def data_upsell_distinct_contact_monthly():
    l0_du_pre_experiment5_groups = catalog.load("l0_du_pre_experiment5_groups")
    l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
        "l0_campaign_tracking_contact_list_pre_full_load"
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        " date(contact_date) >= date('" + "2020-08-01" + "')"
    )

    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.where(
        """campaign_child_code LIKE 'DataOTC.8%'
        OR campaign_child_code LIKE 'DataOTC.9%'
        OR campaign_child_code LIKE 'DataOTC.12%'
        OR campaign_child_code LIKE 'DataOTC.28%'"""
    )
    dataupsell_contacted_campaign_latest = dataupsell_contacted_campaign.groupby(
        "subscription_identifier", "campaign_child_code", "contact_date"
    ).agg(F.max("update_date").alias("update_date"))

    dataupsell_contacted_campaign = dataupsell_contacted_campaign.join(
        dataupsell_contacted_campaign_latest,
        [
            "subscription_identifier",
            "campaign_child_code",
            "contact_date",
            "update_date",
        ],
        "inner",
    )
    dataupsell_contacted_campaign = (
        dataupsell_contacted_campaign.join(
            l0_du_pre_experiment5_groups.selectExpr(
                "old_subscription_identifier as subscription_identifier", "group_name"
            ),
            ["subscription_identifier"],
            "inner",
        )
        .selectExpr(
            "*",
            """CASE WHEN group_name IN ('ATL_propensity_CG','ATL_uplift_CG') AND campaign_child_code = 'DataOTC.8.51' THEN 1
                             WHEN group_name IN ('ATL_uplift_TG','ATL_propensity_TG')
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                             WHEN group_name = 'BTL1_CG' AND campaign_child_code = 'DataOTC.9.12' THEN 1
                             WHEN group_name = 'BTL1_TG'
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                             WHEN group_name = 'BTL2_CG' AND campaign_child_code = 'DataOTC.12.6' THEN 1
                             WHEN group_name = 'BTL2_TG'
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                             WHEN group_name = 'BTL3_CG' AND campaign_child_code = 'DataOTC.28.12' THEN 1
                             WHEN group_name = 'BTL3_TG'
                    AND campaign_child_code NOT IN ('DataOTC.8.51','DataOTC.9.12','DataOTC.12.6','DataOTC.28.12') THEN 1
                    ELSE 0
                    END AS correct_flag""",
            "1 as contacted_flag",
        )
        .where("correct_flag = 1")
    )

    du_offer_blacklist = spark.sql("SELECT * FROM prod_dataupsell.du_offer_blacklist")
    du_offer_blacklist = du_offer_blacklist.withColumn("model_targeted_flag", F.lit(1))
    du_offer_blacklist.createOrReplaceTempView("du_offer_blacklist")
    dataupsell_contacted_campaign.createOrReplaceTempView(
        "dataupsell_contacted_campaign"
    )
    spine = spark.sql(
        """ SELECT offer.subscription_identifier,offer.group_name,offer.scoring_day,
        targeted.contact_date,offer.campaign_child_code,targeted.contacted_flag,targeted.response_int,
        targeted.response,offer.sum_rev_arpu_total_revenue_monthly_last_month,offer.propensity,offer.arpu_uplift,
        offer.offer_price_inc_vat,offer.price_inc_vat_30_days,
        CASE WHEN targeted.contact_date IS NOT NULL THEN 
        DATE(CONCAT(YEAR(targeted.contact_date),'-',MONTH(targeted.contact_date),'-01')) 
        ELSE DATE(CONCAT(YEAR(offer.scoring_day),'-',MONTH(offer.scoring_day),'-01')) 
        END AS start_of_month
        FROM
    (SELECT * FROM du_offer_blacklist) offer
    LEFT JOIN 
    (SELECT *,CASE WHEN response = 'Y' THEN 1 ELSE 0 END AS response_int 
    FROM dataupsell_contacted_campaign) targeted
    ON offer.old_subscription_identifier = targeted.subscription_identifier 
    AND offer.scoring_day <= targeted.contact_date 
    AND targeted.contact_date < offer.black_listed_end_date
    AND offer.campaign_child_code = targeted.campaign_child_code
    """
    )
    spine.createOrReplaceTempView("spine")
    spark.sql(
        "DROP TABLE IF EXISTS prod_dataupsell.data_upsell_distinct_contact_monthly"
    )
    spark.sql(
        """CREATE TABLE prod_dataupsell.data_upsell_distinct_contact_monthly
                USING DELTA
                AS
                SELECT * FROM spine"""
    )
    return spine


def dataupsell_lcg_contamination_monitoring():
    l0_du_pre_experiment5_groups = catalog.load("l0_du_pre_experiment5_groups")
    l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
        "l0_campaign_tracking_contact_list_pre_full_load"
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        " date(contact_date) >= date('" + "2020-08-01" + "')"
    )

    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.where(
        """campaign_child_code LIKE 'DataOTC.8%'
        OR campaign_child_code LIKE 'DataOTC.9%'
        OR campaign_child_code LIKE 'DataOTC.12%'
        OR campaign_child_code LIKE 'DataOTC.28%'"""
    )
    dataupsell_contacted_campaign_latest = dataupsell_contacted_campaign.groupby(
        "subscription_identifier", "campaign_child_code", "contact_date"
    ).agg(F.max("update_date").alias("update_date"))

    dataupsell_contacted_campaign = dataupsell_contacted_campaign.join(
        dataupsell_contacted_campaign_latest,
        [
            "subscription_identifier",
            "campaign_child_code",
            "contact_date",
            "update_date",
        ],
        "inner",
    )

    dataupsell_contacted_campaign_lcg = dataupsell_contacted_campaign.join(
        l0_du_pre_experiment5_groups.where(
            "group_name in ('ATL_propensity_CG','ATL_propensity_CG')"
        ).selectExpr(
            "old_subscription_identifier as subscription_identifier", "group_name"
        ),
        ["subscription_identifier"],
        "inner",
    )
    dataupsell_contacted_campaign_lcg.show()
    dataupsell_contacted_campaign_lcg.groupby("campaign_child_code", "group_name").agg(
        F.count("*")
    ).show(100)
