from datetime import datetime
from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from customer360.utilities.spark_util import get_spark_session

import calendar

def add_months(input_date, months) :

    month = input_date.month - 1 + months
    year = input_date.year + month // 12
    month = month % 12 + 1
    day = min(input_date.day, calendar.monthrange(year, month)[1])

    return datetime.strptime(str(year) + "-" + str(month) + "-" + str(day), "%Y-%m-%d")

def create_gcg_marketing_performance_pre_data(
        l3_campaign_postpaid_prepaid_monthly : DataFrame,
        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly : DataFrame,
        l3_customer_profile_union_monthly_feature : DataFrame,
        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly : DataFrame,
        dm07_sub_clnt_info : DataFrame
        #TODO Checked
) :

    spark = get_spark_session()

    #Prepare dormant Feature
    #TODO prepaid_no_activity_daily_selected
    # Postpaid have no inactivity table so let's focus on customer profile only

    dm07_sub_clnt_info = dm07_sub_clnt_info.where("date(ddate) >= date('2020-01-01')")
    dm07_sub_clnt_info = dm07_sub_clnt_info.selectExpr(
        "analytic_id",
        "date(activation_date) AS register_date",
        "crm_sub_id AS old_subscription_identifier",
        "date( CONCAT(YEAR(date(ddate)), '-', MONTH(date(ddate)), '-01') ) AS join_date"
    )

    today_dt = datetime.now() + timedelta(hours=7)
    if today_dt.day < 20 :
        cl = (
            dm07_sub_clnt_info.withColumn("G", F.lit(1))
            .groupBy("G")
            .agg(F.max("join_date").alias("max_date"))
            .collect()
        )

        patch_key = dm07_sub_clnt_info.where(
            "join_date = date('" + cl[0][1].strftime("%Y-%m-%d") + "')"
        ).selectExpr(
            "analytic_id",
            "register_date",
            "old_subscription_identifier",
            "date('" + add_months(cl[0][1], 1).strftime("%Y-%m-%d") + "') AS join_date",
        ) #TODO change join_month to join_date Checked

        dm07_sub_clnt_info = dm07_sub_clnt_info.union(patch_key)

    #TODO prepaid_no_activity_fix_key 98
    # Postpaid have no inactivity table so let's focus on customer profile only

    l3_customer_profile_union_monthly_feature_selected = l3_customer_profile_union_monthly_feature.where(
        "charge_type = 'Post-paid'"
    )

    l3_customer_profile_union_monthly_feature_selected = l3_customer_profile_union_monthly_feature_selected.selectExpr(
        "subscription_identifier",
        "access_method_num",
        "old_subscription_identifier",
        """CASE WHEN global_control_group = 'Y' THEN 'GCG' ELSE 'Non GCG' END AS Global_Control_Group""",
        "date(register_date) AS register_date",
        "date(start_of_month) AS join_date",
    )

    #TODO inactivity_weekly 115
    #TODO inactivity_weekly_feature_today 121
    #TODO inactivity_weekly_feature_lastweek 133
    # Postpaid have no inactivity table so let's focus on customer profile only
    customer_profile_monthly = l3_customer_profile_union_monthly_feature_selected.join(
        dm07_sub_clnt_info,
        ["old_subscription_identifier", "register_date", "join_date"], #TODO find join_date of dm07 Checked
        "left",
    )

    customer_profile_monthly_today = customer_profile_monthly.selectExpr(
        "subscription_identifier",
        "Global_Control_Group",
        "join_date",
    )

    customer_profile_monthly_lastmonth = customer_profile_monthly.selectExpr(
        "subscription_identifier",
        "Global_Control_Group",
        "add_months(join_date, -1) AS join_date",
    )

    l3_campaign_postpaid_prepaid_monthly_selected = l3_campaign_postpaid_prepaid_monthly.selectExpr(
        "subscription_identifier",
        "campaign_overall_count_sum AS campaign_received_1_months",
        """campaign_total_success_by_call_center_sum
        +
        campaign_total_success_by_sms_sum 
        AS campaign_response_1_months""",
        "start_of_month AS join_date" #TODO change to start_of_month 148 Checked
    )

    l3_campaign_postpaid_prepaid_monthly_selected_today = l3_campaign_postpaid_prepaid_monthly_selected.selectExpr(
        "subscription_identifier",
        "campaign_received_1_months AS campaign_received_1_months_Today",
        "campaign_response_1_months AS campaign_response_1_months_Today",
        """CASE WHEN campaign_received_1_months > 0 THEN 1 ELSE 0 END AS campaign_received_yn_1_months_Today""",
        """CASE WHEN campaign_response_1_months > 0 THEN 1 ELSE 0 END AS campaign_response_yn_1_months_Today""",
        "join_date",
    )

    l3_campaign_postpaid_prepaid_monthly_selected_lastmonth = l3_campaign_postpaid_prepaid_monthly_selected.selectExpr(
        "subscription_identifier",
        "campaign_received_1_months AS campaign_received_1_months_Last_month",
        "campaign_response_1_months AS campaign_response_1_months_Last_month",
        """CASE WHEN campaign_received_1_months > 0 THEN 1 ELSE 0 END AS campaign_received_yn_1_month_Last_month""",
        """CASE WHEN campaign_response_1_months > 0 THEN 1 ELSE 0 END AS campaign_response_yn_1_month_Last_month""",
        "add_months(join_date, -1) AS join_date",
    )
    #TODO not sure about table and attribute 117-140 (this file)
    l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_today = l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_revenue AS Total_Revenue_1_month_Today",
        "start_of_month AS join_date",
    )

    l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_lastmonth = l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "rev_arpu_total_revenue AS Total_Revenue_1_month_Last_month",
        "add_months(start_of_month, -1) AS join_date"
    )

    l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_today = l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_revenue_monthly_last_three_month AS ARPU_3_months_Today",
        "date(start_of_month) AS join_date",
    )

    l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_lastmonth = l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_revenue_monthly_last_three_month AS ARPU_3_months_Last_month",
        "add_months(start_of_month, -1) AS join_date"
    )

    #TODO spine_report inactivity_weekly_feature_today 190
    # Postpaid have no inactivity table so let's focus on customer profile only

    spine_report = customer_profile_monthly.join(
        l3_campaign_postpaid_prepaid_monthly_selected_today,
        ["subscription_identifier", "join_date"], #TODO check join_date (monthly)
        "left",
    )

    spine_report = spine_report.join(
        l3_campaign_postpaid_prepaid_monthly_selected_lastmonth,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_today,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_lastmonth,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_today,
        ["subscription_identifier", "join_date"],
        "left",
    )

    spine_report = spine_report.join(
        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_lastmonth,
        ["subscription_identifier", "join_date"],
        "left",
    )

    #TODO Total_postpaid_subscribers_today inactivity_weekly_feature_today 231 Checked
    Total_postpaid_subscribers_today = customer_profile_monthly_today.groupBy(
        ["join_date", "Global_Control_Group"]
    ).agg(
        F.countDistinct("subscription_identifier").alias(
            "Total_postpaid_subscribers_1_month_Today"
        )
    )
    #TODO Total_postpaid_subscribers_lastmonth inactivity_weekly_feature_lastweek 238 Checked
    Total_postpaid_subscribers_lastmonth = customer_profile_monthly_lastmonth.groupBy(
        ["join_date", "Global_Control_Group"]
    ).agg(
        F.countDistinct("subscription_identifier").alias(
            "Total_postpaid_subscribers_1_month_Last_month"
        )
    )
    gcg_report_df = spine_report.groupBy(["join_date", "Global_Control_Group"]).agg(
        F.sum("Total_Revenue_1_month_Today").alias("Total_Revenue_1_month_Today"),
        F.sum("Total_Revenue_1_month_Last_month").alias("Total_Revenue_1_month_Last_month"),
        F.sum("ARPU_3_months_Today").alias("ARPU_3_months_Today"),
        F.sum("ARPU_3_months_Last_month").alias("ARPU_3_months_Last_month"),
        #F.sum("ARPU_7_day_Today").alias("ARPU_7_day_Today"), #TODO ot have
        #F.sum("ARPU_7_day_Last_week").alias("ARPU_7_day_Last_week"), #TODO Do not have
        #F.sum("total_dormant_90_day").alias("total_dormant_90_day"), #TODO Postpaid have no inactivity table so let's focus on customer profile only
        #F.sum("total_dormant_90_day_Last_week").alias("total_dormant_90_day_Last_week"), #TODO Postpaid have no inactivity table so let's focus on customer profile only
        F.sum("campaign_received_1_months_Today").alias(
            "All_campaign_transactions_1_month_Today"
        ),
        F.sum("campaign_response_1_months_Today").alias(
            "All_campaign_transactions_with_response_tracking_1_month_Today"
        ),
        F.sum("campaign_received_1_months_Last_month").alias(
            "All_campaign_transactions_1_month_Last_month"
        ),
        F.sum("campaign_response_1_months_Last_month").alias(
            "All_campaign_transactions_with_response_tracking_1_months_Last_month"
        ),
        F.sum("campaign_response_yn_1_months_Today").alias(
            "Distinct_postpaid_sub_responders_1_months_Today"
        ),
        F.sum("campaign_response_yn_1_month_Last_month").alias(
            "Distinct_postpaid_sub_responders_1_month_Last_month"
        ),
        F.sum("campaign_received_yn_1_months_Today").alias(
            "Distinct_postpaid_sub_targeted_1_months_Today"
        ),
        F.sum("campaign_received_yn_1_month_Last_month").alias(
            "Distinct_postpaid_sub_targeted_1_month_Last_month"
        ),
        #F.sum("active_prepaid_subscribers_1_Day_Today").alias(
        #    "active_prepaid_subscribers_1_Day_Today"
        #), #TODO Postpaid have no inactivity table so let's focus on customer profile only
        #F.sum("active_prepaid_subscribers_1_Day_Last_week").alias(
        #    "active_prepaid_subscribers_1_Day_Last_week"
        #), #TODO Postpaid have no inactivity table so let's focus on customer profile only
    )

    gcg_report_df = gcg_report_df.join(
        Total_postpaid_subscribers_today,
        ["join_date", "Global_Control_Group"],
        "inner"
    )

    gcg_report_df = gcg_report_df.join(
        Total_postpaid_subscribers_lastmonth,
        ["join_date", "Global_Control_Group"],
        "inner"
    )

    #Temp column for missing data
    gcg_report_df = gcg_report_df.withColumn(
        'total_dormant_90_day', F.lit(0)
    ).withColumn(
        'total_dormant_90_day_Last_week', F.lit(0)
    ).withColumn(
        'active_prepaid_subscribers_1_Day_Today', F.lit(0)
    ).withColumn(
        'active_prepaid_subscribers_1_Day_Last_week', F.lit(0)
    )

    gcg_report_df.createOrReplaceTempView("temp_view_load")
    spark.sql("""DROP TABLE IF EXISTS nba_dev.gcg_postpaid_marketing_performance_report""")
    spark.sql(
        """CREATE TABLE nba_dev.gcg_postpaid_marketing_performance_report
        USING DELTA
        PARTITIONED BY (join_date)
        AS
        SELECT * FROM temp_view_load"""
    )

    return gcg_report_df