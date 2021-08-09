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

def l5_data_upsell_churn_ontop_revenue_weekly_report_tg_cg(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l0_campaign_tracking_campaign_response_master: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
):
    spark = get_spark_session()
    l0_campaign_tracking_campaign_response_master = l0_campaign_tracking_campaign_response_master.withColumn(
        "partition_date_str", F.col("partition_date").cast(StringType())
    ).select(
        "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
    )

    max_master_date = (
        l0_campaign_tracking_campaign_response_master.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("date_id").alias("date_id"))
        .collect()
    )
    l0_campaign_tracking_campaign_response_master = l0_campaign_tracking_campaign_response_master.where(
        "date_id = date('" + max_master_date[0][1].strftime("%Y-%m-%d") + "')"
    )


    churn_campaign = l0_campaign_tracking_campaign_response_master.selectExpr(
        "campaign_type_cvm",
        "campaign_child_code",
    ).where("campaign_type_cvm = 'B) Churn Prevention' ")

    retention_campaign = l0_campaign_tracking_campaign_response_master.selectExpr(
        "campaign_type_cvm",
        "campaign_child_code",
    ).where("campaign_type_cvm = 'D) Retention' ")

    l0_product_pru_m_ontop_master_for_weekly_full_load = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
            "partition_date_str", F.col("partition_date").cast(StringType())
        )
        .select(
            "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
        )
        .selectExpr("promotion_code", "date_id", "price_exc_vat")
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
    churn_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*",
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    ).join(
        churn_campaign, ["campaign_child_code"], "inner"
    )
    retention_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*",
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    ).join(
        retention_campaign, ["campaign_child_code"], "inner"
    )

    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.join(
        upsell_campaign_child_code, ["campaign_child_code"], "inner"
    )

    dataupsell_contacted_campaign = dataupsell_contacted_campaign.selectExpr(
        "*",
        """CASE WHEN ( campaign_child_code LIKE 'DataOTC.8%') OR (campaign_child_code LIKE 'DataOTC.10%')
                        OR (campaign_child_code LIKE 'DataOTC.11%') OR (campaign_child_code LIKE 'DataOTC.12%')
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    churn_contacted_sub = (
        churn_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
        )
        .groupby("old_subscription_identifier", "register_date", "start_of_week",)
        .agg(F.min("contact_date").alias("contact_date"),)
    )

    retention_contacted_sub = (
        retention_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
        )
        .groupby("old_subscription_identifier", "register_date", "start_of_week",)
        .agg(F.min("contact_date").alias("contact_date"),)
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score - Daily' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score - Weekly' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)

    dataupsell_contacted_sub_selected = dataupsell_contacted_sub_selected.join(
        churn_contacted_sub.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "1 AS churn_campaign_contacted",
        ),
        ["old_subscription_identifier", "register_date", "start_of_week",],
        "left",
    ).join(
        retention_contacted_sub.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "1 AS retention_campaign_contacted",
        ),
        ["old_subscription_identifier", "register_date", "start_of_week",],
        "left",
    )
    dataupsell_contacted_sub_selected = dataupsell_contacted_sub_selected.selectExpr(
        "*",
        """CASE WHEN churn_campaign_contacted = 1 AND retention_campaign_contacted is null THEN 'DU-Churn' 
                     WHEN churn_campaign_contacted is null AND retention_campaign_contacted = 1 THEN 'DU-Retention'
                     WHEN churn_campaign_contacted = 1 AND retention_campaign_contacted = 1 THEN 'DU-Churn-Retention' 
                     ELSE 'DU-Only' END as campaign_treatment_combination """,
    )
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN score_priority is not null then score_priority
                ELSE 'Not-contact' END as upsell_coverage """,
        "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn",
        """CASE WHEN group_name LIKE '%TG' THEN 'TG' 
                WHEN group_name LIKE '%CG' THEN 'CG' END AS compare_group""",
    )

    revenue_uplift_report_df = revenue_report_df.groupby(
        "compare_group",
        "start_of_week",
        "upsell_coverage",
        "campaign_treatment_combination",
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df


def l5_data_upsell_churn_ontop_revenue_weekly_report(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l0_campaign_tracking_campaign_response_master: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
):
    spark = get_spark_session()
    l0_campaign_tracking_campaign_response_master = l0_campaign_tracking_campaign_response_master.withColumn(
        "partition_date_str", F.col("partition_date").cast(StringType())
    ).select(
        "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
    )

    max_master_date = (
        l0_campaign_tracking_campaign_response_master.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("date_id").alias("date_id"))
        .collect()
    )
    l0_campaign_tracking_campaign_response_master = l0_campaign_tracking_campaign_response_master.where(
        "date_id = date('" + max_master_date[0][1].strftime("%Y-%m-%d") + "')"
    )


    churn_campaign = l0_campaign_tracking_campaign_response_master.selectExpr(
        "campaign_type_cvm",
        "campaign_child_code",
    ).where("campaign_type_cvm = 'B) Churn Prevention' ")

    retention_campaign = l0_campaign_tracking_campaign_response_master.selectExpr(
        "campaign_type_cvm",
        "campaign_child_code",
    ).where("campaign_type_cvm = 'D) Retention' ")

    l0_product_pru_m_ontop_master_for_weekly_full_load = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
            "partition_date_str", F.col("partition_date").cast(StringType())
        )
        .select(
            "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
        )
        .selectExpr("promotion_code", "date_id", "price_exc_vat")
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
    churn_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*",
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    ).join(
        churn_campaign, ["campaign_child_code"], "inner"
    )
    retention_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*",
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    ).join(
        retention_campaign, ["campaign_child_code"], "inner"
    )

    dataupsell_contacted_campaign = l0_campaign_tracking_contact_list_pre_full_load.join(
        upsell_campaign_child_code, ["campaign_child_code"], "inner"
    )

    dataupsell_contacted_campaign = dataupsell_contacted_campaign.selectExpr(
        "*",
        """CASE WHEN ( campaign_child_code LIKE 'DataOTC.8%') OR (campaign_child_code LIKE 'DataOTC.10%')
                        OR (campaign_child_code LIKE 'DataOTC.11%') OR (campaign_child_code LIKE 'DataOTC.12%')
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    churn_contacted_sub = (
        churn_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
        )
        .groupby("old_subscription_identifier", "register_date", "start_of_week",)
        .agg(F.min("contact_date").alias("contact_date"),)
    )

    retention_contacted_sub = (
        retention_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
        )
        .groupby("old_subscription_identifier", "register_date", "start_of_week",)
        .agg(F.min("contact_date").alias("contact_date"),)
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score - Daily' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score - Weekly' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)

    dataupsell_contacted_sub_selected = dataupsell_contacted_sub_selected.join(
        churn_contacted_sub.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "1 AS churn_campaign_contacted",
        ),
        ["old_subscription_identifier", "register_date", "start_of_week",],
        "left",
    ).join(
        retention_contacted_sub.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "1 AS retention_campaign_contacted",
        ),
        ["old_subscription_identifier", "register_date", "start_of_week",],
        "left",
    )
    dataupsell_contacted_sub_selected = dataupsell_contacted_sub_selected.selectExpr(
        "*",
        """CASE WHEN churn_campaign_contacted = 1 AND retention_campaign_contacted is null THEN 'DU-Churn' 
                     WHEN churn_campaign_contacted is null AND retention_campaign_contacted = 1 THEN 'DU-Retention'
                     WHEN churn_campaign_contacted = 1 AND retention_campaign_contacted = 1 THEN 'DU-Churn-Retention' 
                     ELSE 'DU-Only' END as campaign_treatment_combination """,
    )
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
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
        "group_name",
        "start_of_week",
        "upsell_coverage",
        "campaign_treatment_combination",
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df


def l5_data_upsell_ontop_revenue_weekly_report(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
):
    spark = get_spark_session()
    l0_product_pru_m_ontop_master_for_weekly_full_load = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
            "partition_date_str", F.col("partition_date").cast(StringType())
        )
        .select(
            "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
        )
        .selectExpr("promotion_code", "date_id", "price_exc_vat")
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score - Daily' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score - Weekly' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
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
        "group_name", "start_of_week", "upsell_coverage"
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df


def l5_data_upsell_ontop_revenue_weekly_report_tg_cg(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
):
    spark = get_spark_session()
    l0_product_pru_m_ontop_master_for_weekly_full_load = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
            "partition_date_str", F.col("partition_date").cast(StringType())
        )
        .select(
            "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
        )
        .selectExpr("promotion_code", "date_id", "price_exc_vat")
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score - Daily' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score - Weekly' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN score_priority is not null then score_priority
                ELSE 'Not-contact' END as upsell_coverage """,
        "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn",
        """CASE WHEN group_name LIKE '%TG' THEN 'TG' 
                WHEN group_name LIKE '%CG' THEN 'CG' END AS compare_group""",
    )

    revenue_uplift_report_df = revenue_report_df.groupby(
        "compare_group", "start_of_week", "upsell_coverage"
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df

def l5_data_upsell_ontop_revenue_weekly_report_tg_cg_combine_hs(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
):
    spark = get_spark_session()
    l0_product_pru_m_ontop_master_for_weekly_full_load = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
            "partition_date_str", F.col("partition_date").cast(StringType())
        )
        .select(
            "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
        )
        .selectExpr("promotion_code", "date_id", "price_exc_vat")
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN score_priority is not null then score_priority
                ELSE 'Not-contact' END as upsell_coverage """,
        "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn",
        """CASE WHEN group_name LIKE '%TG' THEN 'TG' 
                WHEN group_name LIKE '%CG' THEN 'CG' END AS compare_group""",
    )

    revenue_uplift_report_df = revenue_report_df.groupby(
        "compare_group", "start_of_week", "upsell_coverage"
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df


def l5_data_upsell_ontop_revenue_weekly_report_group_combine_hs(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
):
    spark = get_spark_session()
    l0_product_pru_m_ontop_master_for_weekly_full_load = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
            "partition_date_str", F.col("partition_date").cast(StringType())
        )
        .select(
            "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("date_id")
        )
        .selectExpr("promotion_code", "date_id", "price_exc_vat")
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )
    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN score_priority is not null then score_priority
                ELSE 'Not-contact' END as upsell_coverage """,
        "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn",
        """CASE WHEN group_name LIKE '%TG' THEN 'TG' 
                WHEN group_name LIKE '%CG' THEN 'CG' END AS compare_group""",
    )

    revenue_uplift_report_df = revenue_report_df.groupby(
        "group_name", "start_of_week", "upsell_coverage"
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df

def l5_data_upsell_ontop_revenue_weekly_report_group_sandbox_combine_hs(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l0_revenue_pru_f_ontop_pospre_daily_full_load: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l3_customer_profile_union_monthly_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    dm42_promotion_prepaid: DataFrame,
    mapping_for_model_training: DataFrame,
    control_group_tbl: DataFrame,
    l5_du_offer_weekly_low_score_list: DataFrame,
    owner_name: str,
    control_group_initialize_profile_date,
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

    l0_product_pru_m_ontop_master_for_weekly_full_load = l0_product_pru_m_ontop_master_for_weekly_full_load.selectExpr(
        "promotion_code", "ddate as date_id", "price_exc_vat"
    )
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.where(
        "date(event_partition_date) > date('"
        + control_group_initialize_profile_date
        + "')"
    )

    revenue_before = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day",
        "date(event_partition_date) as start_of_week",
    )

    revenue_after_seven_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_seven_day as sum_rev_arpu_total_net_rev_daily_after_seven_day",
        "date_add(date(event_partition_date),-7) as start_of_week",
    )
    revenue_after_fourteen_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_fourteen_day as sum_rev_arpu_total_net_rev_daily_after_fourteen_day",
        "date_add(date(event_partition_date),-14) as start_of_week",
    )
    revenue_after_thirty_day = l4_revenue_prepaid_daily_features.selectExpr(
        "subscription_identifier",
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day as sum_rev_arpu_total_net_rev_daily_after_thirty_day",
        "date_add(date(event_partition_date),-30) as start_of_week",
    )

    l0_revenue_pru_f_ontop_pospre_daily_full_load = l0_revenue_pru_f_ontop_pospre_daily_full_load.selectExpr(
        "date(date_id) as date_id",
        "access_method_num",
        "number_of_call as number_of_transaction",
        "date(register_date) as register_date",
        "package_id as promotion_code",
        "net_revenue",
        """CASE WHEN dayofweek(date_id) == 0 THEN date(date_add(date_id,2))
                WHEN dayofweek(date_id) == 1 THEN date(date_add(date_id,1))
                WHEN dayofweek(date_id) == 2 THEN date(date_id) 
                WHEN dayofweek(date_id) > 2 THEN date(date_sub(date_id,dayofweek(date_id)-2) )
                END AS start_of_week""",
    ).where(
        " date(date_id) >= date('2020-07-01')"
    )

    revenue_ontop_daily = l0_revenue_pru_f_ontop_pospre_daily_full_load.join(
        l0_product_pru_m_ontop_master_for_weekly_full_load,
        ["promotion_code", "date_id"],
        "inner",
    ).selectExpr(
        "start_of_week",
        "number_of_transaction * price_exc_vat as net_revenue_exc_vat",
        "access_method_num",
        "register_date",
    )

    revenue_ontop_before = revenue_ontop_daily.groupby(
        "start_of_week", "access_method_num", "register_date"
    ).agg(F.sum("net_revenue_exc_vat").alias("ontop_revenue_last_seven_day"))

    revenue_ontop_before.persist()

    revenue_ontop_after = revenue_ontop_before.selectExpr(
        "date_sub(start_of_week,7) as start_of_week",
        "access_method_num",
        "register_date",
        "ontop_revenue_last_seven_day as ontop_revenue_after_seven_day",
    )

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
                        OR (campaign_child_code LIKE 'DataOTC.28%') OR (campaign_child_code LIKE 'DataOTC.9%') THEN 'HS' 
                     WHEN (campaign_child_code LIKE 'DataOTC.30%') OR (campaign_child_code LIKE 'DataOTC.32%')
                        OR (campaign_child_code LIKE 'DataOTC.33%') THEN 'LS' 
                     ELSE 'LS' END AS score_priority """,
        """CASE WHEN dayofweek(contact_date) == 0 THEN date(date_add(contact_date,2))
                WHEN dayofweek(contact_date) == 1 THEN date(date_add(contact_date,1))
                WHEN dayofweek(contact_date) == 2 THEN date(contact_date) 
                WHEN dayofweek(contact_date) > 2 THEN date(date_sub(contact_date,dayofweek(contact_date)-2) )
                END AS start_of_week""",
    )

    dataupsell_contacted_sub = (
        dataupsell_contacted_campaign.selectExpr(
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            "date(contact_date) as contact_date",
            "campaign_child_code",
            "start_of_week",
            "score_priority",
        )
        .groupby(
            "old_subscription_identifier",
            "register_date",
            "start_of_week",
            "score_priority",
        )
        .agg(F.min("contact_date").alias("contact_date"),)
    )
    high_score_contacted_sub = dataupsell_contacted_sub.where("score_priority = 'HS'")
    low_score_contacted_sub = dataupsell_contacted_sub.join(
        high_score_contacted_sub,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    l5_du_offer_weekly_low_score_list = l5_du_offer_weekly_low_score_list.selectExpr(
        "register_date",
        "old_subscription_identifier",
        """CASE WHEN dayofweek(scoring_day) == 0 THEN date(date_add(scoring_day,2))
                WHEN dayofweek(scoring_day) == 1 THEN date(date_add(scoring_day,1))
                WHEN dayofweek(scoring_day) == 2 THEN date(scoring_day) 
                WHEN dayofweek(scoring_day) > 2 THEN date(date_sub(scoring_day,dayofweek(scoring_day)-2) )
                END AS start_of_week""",
    ).where("propensity > 0.1")
    high_score_weekly = low_score_contacted_sub.join(
        l5_du_offer_weekly_low_score_list,
        ["register_date", "old_subscription_identifier", "start_of_week"],
        "inner",
    )
    low_score = low_score_contacted_sub.join(
        high_score_weekly,
        ["old_subscription_identifier", "register_date", "start_of_week"],
        "left_anti",
    )
    high_score_contacted_sub = high_score_contacted_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "start_of_week",
        "'High Score' AS score_priority",
    )
    high_score_weekly = high_score_weekly.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'High Score' AS score_priority",
    )
    low_score = low_score.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "date_add(start_of_week,7) as start_of_week",
        "'Low Score - Weekly' AS score_priority",
    )
    dataupsell_contacted_sub_selected = high_score_contacted_sub.union(
        high_score_weekly
    ).union(low_score)
    l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.where(
        "charge_type = 'Pre-paid'"
    ).selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "charge_type",
        "subscription_identifier",
        "date(register_date) as register_date",
        "start_of_month",
    )
    max_profile_date = (
        l3_customer_profile_union_monthly_feature_full_load.groupby("charge_type")
        .agg(F.max("start_of_month"))
        .collect()
    )
    max_campaign_date = (
        dataupsell_contacted_sub.selectExpr(
            "1 as G",
            "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
        )
        .groupby("G")
        .agg(F.max("start_of_month"))
        .collect()
    )
    if max_campaign_date[0][1] > max_profile_date[0][1]:
        latest_month_profile = l3_customer_profile_union_monthly_feature_full_load.where(
            "start_of_month = date('"
            + max_profile_date[0][1].strftime("%Y-%m-%d")
            + "')"
        )
        l3_customer_profile_union_monthly_feature_full_load = l3_customer_profile_union_monthly_feature_full_load.union(
            latest_month_profile.selectExpr(
                "old_subscription_identifier",
                "access_method_num",
                "charge_type",
                "subscription_identifier",
                "register_date",
                "add_months(start_of_month,1) as start_of_month",
            )
        )

    recurring_sub = spark.sql("SELECT * FROM prod_dataupsell.recurring_sub_monthly")
    control_group_tbl = control_group_tbl.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "usecase_control_group as group_name",
        "mckinsey_flag",
    ).where("mckinsey_flag is null")
    df_customer_date_period = (
        l3_customer_profile_union_monthly_feature_full_load.join(
            control_group_tbl, ["old_subscription_identifier", "register_date"], "left"
        )
        .join(
            dataupsell_contacted_sub_selected.selectExpr(
                "*",
                "DATE(CONCAT(year(start_of_week),'-',month(start_of_week),'-01')) as start_of_month",
            ),
            ["old_subscription_identifier", "register_date", "start_of_month"],
            "left",
        )
        .join(recurring_sub, ["old_subscription_identifier", "start_of_month"], "left",)
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
            revenue_ontop_before,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
        .join(
            revenue_ontop_after,
            ["access_method_num", "register_date", "start_of_week"],
            "left",
        )
    )
    revenue_report_df = revenue_report_df.selectExpr(
        "*",
        """CASE WHEN score_priority is not null then score_priority
                ELSE 'Not-contact' END as upsell_coverage """,
        "CASE WHEN recurring = 'Y' THEN 'Y' ELSE 'N' END AS recurring_yn",
        """CASE WHEN group_name LIKE '%TG' THEN 'TG' 
                WHEN group_name LIKE '%CG' THEN 'CG' END AS compare_group""",
    )

    revenue_uplift_report_df = revenue_report_df.groupby(
        "group_name", "start_of_week", "upsell_coverage"
    ).agg(
        F.countDistinct("subscription_identifier").alias("Number_of_distinct_subs"),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_feature_last_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_feature_last_fourteen_day"
        ),
        F.sum("ontop_revenue_last_seven_day").alias("L0_ontop_revenue_last_seven_day"),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_feature_after_seven_day"
        ),
        F.sum("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_feature_after_fourteen_day"
        ),
        F.sum("ontop_revenue_after_seven_day").alias(
            "L0_ontop_revenue_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
            "C360_ARPU_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_last_fourteen_day").alias(
            "C360_ARPU_per_sub_last_fourteen_day"
        ),
        F.avg("ontop_revenue_last_seven_day").alias(
            "L0_Ontop_revenue_per_sub_last_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_seven_day").alias(
            "C360_ARPU_per_sub_after_seven_day"
        ),
        F.avg("sum_rev_arpu_total_net_rev_daily_after_fourteen_day").alias(
            "C360_ARPU_per_sub_after_fourteen_day"
        ),
        F.avg("ontop_revenue_after_seven_day").alias(
            "L0_Ontop_revenue_per_sub_after_seven_day"
        ),
    )
    return revenue_uplift_report_df