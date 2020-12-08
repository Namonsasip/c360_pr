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
            """CASE WHEN group_name = 'ATL_CG' AND campaign_child_code = 'DataOTC.8.51' THEN 1
                             WHEN group_name = 'ATL_TG'
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
    # l5_du_weekly_revenue_uplift_report_contacted_only.toPandas().to_csv('data/tmp/data_upsell_revenue_report_26112020.csv', index=False,header=True)
    return revenue_uplift_report_df


def l5_du_weekly_revenue_uplift_report_contacted_only_old(
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
    l4_revenue_prepaid_daily_features.groupby("event_partition_date").agg(
        F.count("*")
    ).sort(F.desc("event_partition_date")).show(200)
    l3_customer_profile_union_monthly_feature_full_load.groupby("start_of_month").agg(
        F.count("*")
    ).sort(F.desc("start_of_month")).show(100)
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

    l0_du_pre_experiment3_groups.show()

    # >>> Start of old code
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

    # >>> End of old code

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
    # l5_du_weekly_revenue_uplift_report_contacted_only = catalog.load("l5_du_weekly_revenue_uplift_report_contacted_only_old")
    # l5_du_weekly_revenue_uplift_report_contacted_only.toPandas().to_csv('data/tmp/data_upsell_revenue_report_25112020_v1.csv', index=False,header=True)
    # l5_du_weekly_revenue_uplift_report_contacted_only = catalog.load("l5_du_weekly_revenue_uplift_report_contacted_only")
    # l5_du_weekly_revenue_uplift_report_contacted_only.toPandas().to_csv('data/tmp/data_upsell_revenue_report_25112020_v2.csv', index=False,header=True)
    return revenue_uplift_report_df
