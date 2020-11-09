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


def create_weekly_revenue_uplift_report_freeze(
    l4_revenue_prepaid_daily_features: DataFrame,
    l0_du_pre_experiment3_groups: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    control_group_initialize_profile_date,
    csv_file_path,
):
    # control_group_initialize_profile_date = "2020-08-01"
    # l0_du_pre_experiment3_groups = catalog.load("l0_du_pre_experiment3_groups")
    # l4_revenue_prepaid_daily_features = catalog.load("l4_revenue_prepaid_daily_features")
    # l3_customer_profile_union_monthly_feature_full_load = catalog.load("l3_customer_profile_union_monthly_feature_full_load")
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
    freeze_control_group = l0_du_pre_experiment3_groups.where(
        "control_group_created_date = date('"
        + control_group_initialize_profile_date
        + "')"
    )

    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.selectExpr(
        "old_subscription_identifier",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month as control_group_created_date",
    )
    freeze_control_group_fix_key = freeze_control_group.join(
        l3_customer_profile_union_monthly_feature_full_load,
        ["old_subscription_identifier", "register_date", "control_group_created_date"],
        "inner",
    )
    df_customer_date_period = freeze_control_group_fix_key.crossJoin(
        F.broadcast(start_of_week_df)
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
        "date_add(event_partition_date,6) as start_of_week",
    )

    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(event_partition_date,13) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(event_partition_date,13) as start_of_week",
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
    )
    revenue_uplift_report_df = (
        revenue_report_df.groupby("group_name", "start_of_week")
        .agg(
            F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "Total_arpu_last_seven_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
                "Total_arpu_last_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "Total_arpu_last_thirty_day"
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
        )
        .withColumn(
            "Avg_arpu_per_sub_last_seven_day",
            F.col("Total_arpu_last_seven_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_last_fourteen_day",
            F.col("Total_arpu_last_fourteen_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_last_thirty_day",
            F.col("Total_arpu_last_thirty_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_after_seven_day",
            F.col("Total_arpu_after_seven_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_after_fourteen_day",
            F.col("Total_arpu_after_fourteen_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_after_thirty_day",
            F.col("Total_arpu_after_thirty_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Arpu_uplift_seven_day",
            (F.col("Total_arpu_after_seven_day") - F.col("Total_arpu_last_seven_day"))
            / F.col("Total_arpu_last_seven_day"),
        )
        .withColumn(
            "Arpu_uplift_fourteen_day",
            (
                F.col("Total_arpu_after_fourteen_day")
                - F.col("Total_arpu_last_fourteen_day")
            )
            / F.col("Total_arpu_last_fourteen_day"),
        )
        .withColumn(
            "Arpu_uplift_thirty_day",
            (F.col("Total_arpu_after_thirty_day") - F.col("Total_arpu_last_thirty_day"))
            / F.col("Total_arpu_last_thirty_day"),
        )
    )

    return revenue_uplift_report_df


def l5_du_weekly_revenue_uplift_report_contacted_only(
    l4_revenue_prepaid_daily_features: DataFrame,
    l0_du_pre_experiment3_groups: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
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
        """campaign_child_code LIKE 'DataOTC.8%' 
        OR campaign_child_code LIKE 'DataOTC.9%' 
        OR campaign_child_code LIKE 'DataOTC.12%'
        OR campaign_child_code LIKE 'DataOTC.28%'"""
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
        "date_add(event_partition_date,6) as start_of_week",
    )

    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(event_partition_date,13) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(event_partition_date,13) as start_of_week",
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
    )
    revenue_uplift_report_df = (
        revenue_report_df.groupby("group_name", "start_of_week")
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
            F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
                "Total_arpu_after_seven_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
                "Total_arpu_after_fourteen_day"
            ),
            F.sum("sum_rev_arpu_total_net_rev_daily_after_thirty_day").alias(
                "Total_arpu_after_thirty_day"
            ),
        )
        .withColumn(
            "Avg_arpu_per_sub_last_seven_day",
            F.col("Total_arpu_last_seven_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_last_fourteen_day",
            F.col("Total_arpu_last_fourteen_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_last_thirty_day",
            F.col("Total_arpu_last_thirty_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_after_seven_day",
            F.col("Total_arpu_after_seven_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_after_fourteen_day",
            F.col("Total_arpu_after_fourteen_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Avg_arpu_per_sub_after_thirty_day",
            F.col("Total_arpu_after_thirty_day") / F.col("Number_of_distinct_subs"),
        )
        .withColumn(
            "Arpu_uplift_seven_day",
            (F.col("Total_arpu_after_seven_day") - F.col("Total_arpu_last_seven_day"))
            / F.col("Total_arpu_last_seven_day"),
        )
        .withColumn(
            "Arpu_uplift_fourteen_day",
            (
                F.col("Total_arpu_after_fourteen_day")
                - F.col("Total_arpu_last_fourteen_day")
            )
            / F.col("Total_arpu_last_fourteen_day"),
        )
        .withColumn(
            "Arpu_uplift_thirty_day",
            (F.col("Total_arpu_after_thirty_day") - F.col("Total_arpu_last_thirty_day"))
            / F.col("Total_arpu_last_thirty_day"),
        )
        .withColumn(
            "Campaign_sent_per_contacted_sub",
            (F.col("Total_campaign_sent")) / (F.col("Number_of_distinct_subs")),
        )
    )
    # l5_du_weekly_revenue_uplift_report_contacted_only = catalog.load("l5_du_weekly_revenue_uplift_report_contacted_only")
    # l5_du_weekly_revenue_uplift_report_contacted_only.toPandas().to_csv('data/tmp/data_upsell_revenue_report_06112020.csv', index=False,header=True)
    return revenue_uplift_report_df
