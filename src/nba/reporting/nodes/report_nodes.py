import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, Any, List

import pandas as pd
import plotnine
from plotnine import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from customer360.utilities.spark_util import get_spark_session

import calendar


def add_months(input_date, months):
    month = input_date.month - 1 + months
    year = input_date.year + month // 12
    month = month % 12 + 1
    day = min(input_date.day, calendar.monthrange(year, month)[1])
    return datetime.strptime(str(year) + "-" + str(month) + "-" + str(day), "%Y-%m-%d")


def create_gcg_marketing_performance_pre_data(
    l4_campaign_postpaid_prepaid_features: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    l2_customer_profile_union_weekly_feature: DataFrame,
    l1_revenue_prepaid_pru_f_usage_multi_daily: DataFrame,
    prepaid_no_activity_daily: DataFrame,
    dm07_sub_clnt_info: DataFrame,
):
    # # Old cloud inactive dataset
    # prepaid_no_activity_daily = catalog.load("prepaid_no_activity_daily")
    #
    # # Old cloud profile dataset for key join
    # dm07_sub_clnt_info = catalog.load("dm07_sub_clnt_info")
    #
    # # profile data
    # l2_customer_profile_union_weekly_feature = catalog.load(
    #     "l2_customer_profile_union_weekly_feature"
    # )
    #
    # # Campaign Feature weekly
    # l4_campaign_postpaid_prepaid_features = catalog.load(
    #     "l4_campaign_postpaid_prepaid_features"
    # )
    # # Revenue Feature Daily
    # l4_revenue_prepaid_daily_features = catalog.load(
    #     "l4_revenue_prepaid_daily_features"
    # )
    # # l1 Revenue Daily ARPU
    # l1_revenue_prepaid_pru_f_usage_multi_daily = catalog.load(
    #     "l1_revenue_prepaid_pru_f_usage_multi_daily"
    # )
    spark = get_spark_session()
    rerun_from = "2020-08-01"
    # Prepare dormant Feature
    prepaid_no_activity_daily_selected = prepaid_no_activity_daily.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "no_activity_n_days",
        "date(ddate) as join_date",
        "date( CONCAT(YEAR(date(ddate)),'-',MONTH(date(ddate)),'-01') ) as join_month",
    ).where("join_month > date('2019-12-31')")

    dm07_sub_clnt_info = dm07_sub_clnt_info.where("date(ddate) >= date('2020-01-01')")
    dm07_sub_clnt_info = dm07_sub_clnt_info.selectExpr(
        "analytic_id",
        "date(activation_date) as register_date",
        "crm_sub_id as old_subscription_identifier",
        "date( CONCAT(YEAR(date(ddate)),'-',MONTH(date(ddate)),'-01') ) as join_month",
    )

    today_dt = datetime.now() + timedelta(hours=7)
    if today_dt.day < 20:
        cl = (
            dm07_sub_clnt_info.withColumn("G", F.lit(1))
            .groupby("G")
            .agg(F.max("join_month").alias("max_date"))
            .collect()
        )

        patch_key = dm07_sub_clnt_info.where(
            "join_month = date('" + cl[0][1].strftime("%Y-%m-%d") + "')"
        ).selectExpr(
            "analytic_id",
            "register_date",
            "old_subscription_identifier",
            "date('"
            + add_months(cl[0][1], 1).strftime("%Y-%m-%d")
            + "') as join_month",
        )
        dm07_sub_clnt_info = dm07_sub_clnt_info.union(patch_key)

    prepaid_no_activity_fix_key = prepaid_no_activity_daily_selected.join(
        dm07_sub_clnt_info, ["analytic_id", "register_date", "join_month"], "inner"
    ).drop("join_month")

    l2_customer_profile_union_weekly_feature_selected = l2_customer_profile_union_weekly_feature.where(
        "charge_type = 'Pre-paid'"
    )

    l2_customer_profile_union_weekly_feature_selected = l2_customer_profile_union_weekly_feature_selected.selectExpr(
        "subscription_identifier",
        "access_method_num",
        "old_subscription_identifier",
        """CASE WHEN global_control_group = 'Y' THEN 'GCG' ELSE 'Non GCG' END AS Global_Control_Group""",
        "date(register_date) as register_date",
        "date(start_of_week) as join_date",
    )

    inactivity_weekly = l2_customer_profile_union_weekly_feature_selected.join(
        prepaid_no_activity_fix_key,
        ["old_subscription_identifier", "register_date", "join_date"],
        "left",
    )

    inactivity_weekly_feature_today = inactivity_weekly.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "access_method_num",
        "analytic_id",
        "register_date",
        "Global_Control_Group",
        "CASE WHEN no_activity_n_days = 0 THEN 1 ELSE 0 END AS active_prepaid_subscribers_1_Day_Today",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0 END AS total_dormant_90_day",
        "join_date",
    )

    inactivity_weekly_feature_lastweek = inactivity_weekly.selectExpr(
        "subscription_identifier",
        "Global_Control_Group",
        "CASE WHEN no_activity_n_days = 0 THEN 1 ELSE 0 END AS active_prepaid_subscribers_1_Day_Last_week",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0 END AS total_dormant_90_day_Last_week",
        "date_sub(join_date,7) as join_date",
    )

    l4_campaign_postpaid_prepaid_features_selected = l4_campaign_postpaid_prepaid_features.selectExpr(
        "subscription_identifier",
        "sum_campaign_overall_count_sum_weekly_last_week as campaign_received_7_days",
        """sum_campaign_total_success_by_call_center_sum_weekly_last_week
        +
        sum_campaign_total_success_by_sms_sum_weekly_last_week
        as campaign_response_7_days""",
        "start_of_week as join_date",
    )
    l4_campaign_postpaid_prepaid_features_selected_today = l4_campaign_postpaid_prepaid_features_selected.selectExpr(
        "subscription_identifier",
        "campaign_received_7_days as campaign_received_7_days_Today",
        "campaign_response_7_days as campaign_response_7_days_Today",
        """CASE WHEN campaign_received_7_days > 0 THEN 1 ELSE 0 END AS campaign_received_yn_7_days_Today""",
        """CASE WHEN campaign_response_7_days > 0 THEN 1 ELSE 0 END AS campaign_response_yn_7_days_Today""",
        "join_date",
    )
    l4_campaign_postpaid_prepaid_features_selected_lastweek = l4_campaign_postpaid_prepaid_features_selected.selectExpr(
        "subscription_identifier",
        "campaign_received_7_days as campaign_received_7_days_Last_week",
        "campaign_response_7_days as campaign_response_7_days_Last_week",
        """CASE WHEN campaign_received_7_days > 0 THEN 1 ELSE 0 END AS campaign_received_yn_7_days_Last_week""",
        """CASE WHEN campaign_response_7_days > 0 THEN 1 ELSE 0 END AS campaign_response_yn_7_days_Last_week""",
        "date_sub(join_date,7) as join_date",
    )

    l1_revenue_prepaid_pru_f_usage_multi_daily_today = l1_revenue_prepaid_pru_f_usage_multi_daily.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_net_rev as Total_Revenue_1_day_Today",
        "date(event_partition_date) as join_date",
    )
    l1_revenue_prepaid_pru_f_usage_multi_daily_lastweek = l1_revenue_prepaid_pru_f_usage_multi_daily.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_net_rev as Total_Revenue_1_day_Last_week",
        "date_sub(date(event_partition_date),7) as join_date",
    )
    l4_revenue_prepaid_daily_features_today = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as ARPU_7_day_Today",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as ARPU_30_day_Today",
        "date(event_partition_date) as join_date",
    )

    l4_revenue_prepaid_daily_features_lastweek = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as ARPU_7_day_Last_week",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as ARPU_30_day_Last_week",
        "date_sub(date(event_partition_date),7) as join_date",
    )
    spine_report = inactivity_weekly_feature_today.join(
        inactivity_weekly_feature_lastweek.drop("Global_Control_Group"),
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l4_campaign_postpaid_prepaid_features_selected_today,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l4_campaign_postpaid_prepaid_features_selected_lastweek,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l1_revenue_prepaid_pru_f_usage_multi_daily_today,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l1_revenue_prepaid_pru_f_usage_multi_daily_lastweek,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l4_revenue_prepaid_daily_features_today,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l4_revenue_prepaid_daily_features_lastweek,
        ["subscription_identifier", "join_date"],
        "left",
    )
    Total_prepaid_subscribers_today = inactivity_weekly_feature_today.groupby(
        ["join_date", "Global_Control_Group"]
    ).agg(
        F.countDistinct("subscription_identifier").alias(
            "Total_prepaid_subscribers_1_Day_Today"
        )
    )
    Total_prepaid_subscribers_lastweek = inactivity_weekly_feature_lastweek.groupby(
        ["join_date", "Global_Control_Group"]
    ).agg(
        F.countDistinct("subscription_identifier").alias(
            "Total_prepaid_subscribers_1_Day_Last_week"
        )
    )

    gcg_report_df = spine_report.groupby(["join_date", "Global_Control_Group"]).agg(
        F.sum("Total_Revenue_1_day_Today").alias("Total_Revenue_1_day_Today"),
        F.sum("Total_Revenue_1_day_Last_week").alias("Total_Revenue_1_day_Last_week"),
        F.sum("ARPU_7_day_Today").alias("ARPU_7_day_Today"),
        F.sum("ARPU_30_day_Today").alias("ARPU_30_day_Today"),
        F.sum("ARPU_7_day_Last_week").alias("ARPU_7_day_Last_week"),
        F.sum("ARPU_30_day_Last_week").alias("ARPU_30_day_Last_week"),
        F.sum("total_dormant_90_day").alias("total_dormant_90_day"),
        F.sum("total_dormant_90_day_Last_week").alias("total_dormant_90_day_Last_week"),
        F.sum("campaign_received_7_days_Today").alias(
            "All_campaign_transactions_7_Day_Today"
        ),
        F.sum("campaign_response_7_days_Today").alias(
            "All_campaign_transactions_with_response_tracking_7_Day_Today"
        ),
        F.sum("campaign_received_7_days_Last_week").alias(
            "All_campaign_transactions_7_Day_Last_week"
        ),
        F.sum("campaign_response_7_days_Last_week").alias(
            "All_campaign_transactions_with_response_tracking_7_Day_Last_week"
        ),
        F.sum("campaign_response_yn_7_days_Today").alias(
            "Distinct_prepaid_sub_responders_7_day_Today"
        ),
        F.sum("campaign_response_yn_7_days_Last_week").alias(
            "Distinct_prepaid_sub_responders_7_day_Last_week"
        ),
        F.sum("campaign_received_yn_7_days_Today").alias(
            "Distinct_prepaid_sub_targeted_7_day_Today"
        ),
        F.sum("campaign_received_yn_7_days_Last_week").alias(
            "Distinct_prepaid_sub_targeted_7_day_Last_week"
        ),
        F.sum("active_prepaid_subscribers_1_Day_Today").alias(
            "active_prepaid_subscribers_1_Day_Today"
        ),
        F.sum("active_prepaid_subscribers_1_Day_Last_week").alias(
            "active_prepaid_subscribers_1_Day_Last_week"
        ),
    )
    gcg_report_df = gcg_report_df.join(
        Total_prepaid_subscribers_today, ["join_date", "Global_Control_Group"], "inner"
    )

    gcg_report_df = gcg_report_df.join(
        Total_prepaid_subscribers_lastweek,
        ["join_date", "Global_Control_Group"],
        "inner",
    )

    gcg_report_df.createOrReplaceTempView("temp_view_load")
    spark.sql("DROP TABLE IF EXISTS nba_dev.gcg_marketing_performance_report")
    spark.sql(
        """CREATE TABLE nba_dev.gcg_marketing_performance_report
        USING DELTA
        PARTITIONED BY (join_date)
        AS
        SELECT * FROM temp_view_load"""
    )
    # spark.sql(
    #     "SELECT * FROM nba_dev.gcg_marketing_performance_report"
    # ).toPandas().to_csv(
    #     "data/tmp/gcg_marketing_report_20201204.csv", index=False, header=True,
    # )
    return gcg_report_df
