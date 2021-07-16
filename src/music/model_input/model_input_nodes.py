import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, List, Tuple, Union

import pandas as pd
import plotnine
from plotnine import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from customer360.utilities.spark_util import get_spark_session
from nba.model_input.model_input_nodes import add_c360_dates_columns


def node_l0_calling_melody_campaign_target_variable_table(
    daily_response_music_campaign: DataFrame,
    dm07_sub_clnt_info: DataFrame,  # old client info
    start_date,
    end_date,
) -> DataFrame:
    spark = get_spark_session()
    # start_date = 'start_date'
    # end_date = 'end_date'
    dm07_sub_clnt_info = dm07_sub_clnt_info.selectExpr(
        "date(activation_date) as register_date",
        "analytic_id",
        "crm_sub_id as subscription_identifier",
        "date(ddate) as ddate",
    )
    music_campaign_type = (
        daily_response_music_campaign.where("campaign_name LIKE '%Calling%'")
        .withColumn("music_campaign_type", F.lit("Calling_Melody"))
        .union(
            daily_response_music_campaign.where(
                "campaign_name LIKE '%Spotify%'"
            ).withColumn("music_campaign_type", F.lit("Spotify"))
        )
        .union(
            daily_response_music_campaign.where(
                "campaign_name LIKE '%JOOX%'"
            ).withColumn("music_campaign_type", F.lit("JOOX"))
        )
        .union(
            daily_response_music_campaign.where(
                "campaign_name LIKE '%Karaoke%' OR campaign_name LIKE '%KARAOKE%' "
            ).withColumn("music_campaign_type", F.lit("Karaoke"))
        )
    )
    calling_melody_campaign = music_campaign_type.where(
        "music_campaign_type = 'Calling_Melody' "
    )
    calling_melody_response_df = calling_melody_campaign.selectExpr(
        "campaign_child_code",
        "response_type",
        "analytic_id",
        "date(register_date) as register_date",
        """CASE WHEN response_yn = 'N' THEN 0
                WHEN response_yn = 'Y' THEN 1
                END as target_response""",
        "date(contact_date) as contact_date",
        "music_campaign_type",
    ).where(
        """charge_type = 'Prepaid' AND date(contact_date) >= date('"""
        + start_date
        + """')
    AND date(contact_date) < date('"""
        + end_date
        + """')"""
    )
    calling_melody_response_df.withColumn("G", F.lit(1)).groupby("G").agg(
        F.sum("target_response") / F.count("*").alias("Response %"),
        F.count("*").alias("Total Campaign Sent"),
        F.sum("target_response").alias("Total Response True"),
    ).show()
    Total_positive_response = (
        calling_melody_response_df.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.sum("target_response").alias("Total_positive_response"))
        .collect()[0]["Total_positive_response"]
    )
    Total_campaign = (
        calling_melody_response_df.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.count("*").alias("Total_campaign"))
        .collect()[0]["Total_campaign"]
    )
    Total_negative_response = Total_campaign - Total_positive_response
    random_neg_size = (Total_positive_response * 4) / Total_negative_response
    non_responder, others = calling_melody_response_df.where(
        "target_response = 0"
    ).randomSplit([random_neg_size, 1 - random_neg_size])
    train_test_df = non_responder.union(
        calling_melody_response_df.where("target_response = 1")
    )
    train_test_df_crmsub = train_test_df.selectExpr(
        "*", "month(contact_date) as month_num", "year(contact_date) as year_num"
    ).join(
        dm07_sub_clnt_info.selectExpr(
            "*", "month(ddate) as month_num", "year(ddate) as year_num"
        ),
        ["analytic_id", "register_date", "year_num", "month_num"],
        "inner",
    )
    train_test_df_crmsub.groupby("target_response").agg(F.count("*")).show()
    train_test_df_crmsub = train_test_df_crmsub.drop("year_num", "month_num", "ddate")
    return train_test_df_crmsub


def node_l0_calling_melody_campaign_lift_table(
    daily_response_music_campaign: DataFrame,
    dm07_sub_clnt_info: DataFrame,
    start_date,
    end_date,
) -> DataFrame:
    spark = get_spark_session()
    # start_date = 'start_date'
    # end_date = 'end_date'
    dm07_sub_clnt_info = dm07_sub_clnt_info.selectExpr(
        "date(activation_date) as register_date",
        "analytic_id",
        "crm_sub_id as subscription_identifier",
        "date(ddate) as ddate",
    )
    music_campaign_type = (
        daily_response_music_campaign.where("campaign_name LIKE '%Calling%'")
        .withColumn("music_campaign_type", F.lit("Calling_Melody"))
        .union(
            daily_response_music_campaign.where(
                "campaign_name LIKE '%Spotify%'"
            ).withColumn("music_campaign_type", F.lit("Spotify"))
        )
        .union(
            daily_response_music_campaign.where(
                "campaign_name LIKE '%JOOX%'"
            ).withColumn("music_campaign_type", F.lit("JOOX"))
        )
        .union(
            daily_response_music_campaign.where(
                "campaign_name LIKE '%Karaoke%' OR campaign_name LIKE '%KARAOKE%' "
            ).withColumn("music_campaign_type", F.lit("Karaoke"))
        )
    )
    calling_melody_campaign = music_campaign_type.where(
        "music_campaign_type = 'Calling_Melody' "
    )
    calling_melody_response_df = calling_melody_campaign.selectExpr(
        "campaign_child_code",
        "response_type",
        "analytic_id",
        "date(register_date) as register_date",
        """CASE WHEN response_yn = 'N' THEN 0
                WHEN response_yn = 'Y' THEN 1
                END as target_response""",
        "date(contact_date) as contact_date",
        "music_campaign_type",
    ).where(
        """charge_type = 'Prepaid' AND date(contact_date) >= date('"""
        + start_date
        + """')
    AND date(contact_date) < date('"""
        + end_date
        + """')"""
    )
    calling_melody_response_df.withColumn("G", F.lit(1)).groupby("G").agg(
        F.sum("target_response") / F.count("*").alias("Response %"),
        F.count("*").alias("Total Campaign Sent"),
        F.sum("target_response").alias("Total Response True"),
    ).show()
    Total_positive_response = (
        calling_melody_response_df.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.sum("target_response").alias("Total_positive_response"))
        .collect()[0]["Total_positive_response"]
    )
    Total_campaign = (
        calling_melody_response_df.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.count("*").alias("Total_campaign"))
        .collect()[0]["Total_campaign"]
    )
    # Total_negative_response = Total_campaign - Total_positive_response
    # random_neg_size = (Total_positive_response * 4) / Total_negative_response
    # non_responder, others = calling_melody_response_df.where(
    #     "target_response = 0"
    # ).randomSplit([random_neg_size, 1 - random_neg_size])
    # train_test_df = non_responder.union(
    #     calling_melody_response_df.where("target_response = 1")
    # )
    train_test_df_crmsub = calling_melody_response_df.selectExpr(
        "*", "month(contact_date) as month_num", "year(contact_date) as year_num"
    ).join(
        dm07_sub_clnt_info.selectExpr(
            "*", "month(ddate) as month_num", "year(ddate) as year_num"
        ),
        ["analytic_id", "register_date", "year_num", "month_num"],
        "inner",
    )
    train_test_df_crmsub.groupby("target_response").agg(F.count("*")).show()
    train_test_df_crmsub = train_test_df_crmsub.drop("year_num", "month_num", "ddate")
    return train_test_df_crmsub


def node_l5_music_master_spine_table_scoring(
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    min_feature_days_lag: int,
) -> DataFrame:
    # NBA Function
    df_spine = l1_customer_profile_union_daily_feature_full_load.selectExpr(
        "subscription_identifier",
        "access_method_num",
        "old_subscription_identifier",
        "date(register_date) as register_date",
        "event_partition_date",
    )
    df_spine = df_spine.withColumn("music_campaign_type", F.lit("Calling_Melody"))

    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.fillna(
        0,
        subset=list(
            set(l4_revenue_prepaid_daily_features.columns)
            - set(["subscription_identifier", "event_partition_date"])
        ),
    )
    # Add ARPU uplift
    for n_days, feature_name in [
        (30, "sum_rev_arpu_total_net_rev_daily_last_thirty_day"),
        (7, "sum_rev_arpu_total_net_rev_daily_last_seven_day"),
    ]:
        df_arpu_before = l4_revenue_prepaid_daily_features.select(
            "subscription_identifier", "event_partition_date", feature_name,
        )
        df_arpu_after = l4_revenue_prepaid_daily_features.select(
            "subscription_identifier",
            F.date_sub(F.col("event_partition_date"), n_days).alias(
                "event_partition_date"
            ),
            F.col(feature_name).alias(f"{feature_name}_after"),
        )
        df_arpu_uplift = df_arpu_before.join(
            df_arpu_after,
            how="inner",
            on=["subscription_identifier", "event_partition_date"],
        ).withColumn(
            f"target_relative_arpu_increase_{n_days}d",
            (F.col(f"{feature_name}_after") - F.col(feature_name)),
        )

        # Add the average ARPU on each day for all subscribers in case we want to
        # normalize the ARPU target later
        df_arpu_uplift = (
            df_arpu_uplift.withColumn(
                f"{feature_name}_avg_all_subs",
                F.mean(feature_name).over(Window.partitionBy("event_partition_date")),
            )
            .withColumn(
                f"{feature_name}_after_avg_all_subs",
                F.mean(f"{feature_name}_after").over(
                    Window.partitionBy("event_partition_date")
                ),
            )
            .withColumn(
                f"target_relative_arpu_increase_{n_days}d_avg_all_subs",
                F.mean(f"target_relative_arpu_increase_{n_days}d").over(
                    Window.partitionBy("event_partition_date")
                ),
            )
        )

        df_spine = df_spine.join(
            df_arpu_uplift,
            on=["subscription_identifier", "event_partition_date"],
            how="left",
        )

    df_spine = df_spine.withColumn(
        "music_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("event_partition_date"),
            F.lit("_"),
            F.col("music_campaign_type"),
        ),
    )
    return df_spine


def node_l5_music_master_spine_table(
    l0_calling_melody_campaign_target_variable_table: DataFrame,
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    min_feature_days_lag: int,
) -> DataFrame:
    ######## For testing Purpose
    # l0_calling_melody_campaign_target_variable_table = catalog.load("l0_calling_melody_campaign_target_variable_table")
    # l1_customer_profile_union_daily_feature_full_load = catalog.load("l1_customer_profile_union_daily_feature_full_load")
    # l4_revenue_prepaid_daily_features = catalog.load("l4_revenue_prepaid_daily_features")
    # min_feature_days_lag = 5
    ########

    # NBA Function
    df_spine = add_c360_dates_columns(
        l0_calling_melody_campaign_target_variable_table,
        date_column="contact_date",
        min_feature_days_lag=min_feature_days_lag,
    )
    # subscription_identifier is different in L0 and all other C360 levels, so we need to add
    # both of them to the spine, for which we use l1 customer profile as an auxiliary table
    df_spine = df_spine.withColumnRenamed(
        "subscription_identifier", "old_subscription_identifier"
    )
    df_spine = df_spine.join(
        l1_customer_profile_union_daily_feature_full_load.selectExpr(
            "subscription_identifier",
            "access_method_num",
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "event_partition_date",
        ),
        on=["old_subscription_identifier", "register_date", "event_partition_date"],
        how="left",
    )

    # Impute ARPU uplift columns as NA means that subscriber had 0 ARPU
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.fillna(
        0,
        subset=list(
            set(l4_revenue_prepaid_daily_features.columns)
            - set(["subscription_identifier", "event_partition_date"])
        ),
    )
    # Add ARPU uplift
    for n_days, feature_name in [
        (30, "sum_rev_arpu_total_net_rev_daily_last_thirty_day"),
        (7, "sum_rev_arpu_total_net_rev_daily_last_seven_day"),
    ]:
        df_arpu_before = l4_revenue_prepaid_daily_features.select(
            "subscription_identifier", "event_partition_date", feature_name,
        )
        df_arpu_after = l4_revenue_prepaid_daily_features.select(
            "subscription_identifier",
            F.date_sub(F.col("event_partition_date"), n_days).alias(
                "event_partition_date"
            ),
            F.col(feature_name).alias(f"{feature_name}_after"),
        )
        df_arpu_uplift = df_arpu_before.join(
            df_arpu_after,
            how="inner",
            on=["subscription_identifier", "event_partition_date"],
        ).withColumn(
            f"target_relative_arpu_increase_{n_days}d",
            (F.col(f"{feature_name}_after") - F.col(feature_name)),
        )

        # Add the average ARPU on each day for all subscribers in case we want to
        # normalize the ARPU target later
        df_arpu_uplift = (
            df_arpu_uplift.withColumn(
                f"{feature_name}_avg_all_subs",
                F.mean(feature_name).over(Window.partitionBy("event_partition_date")),
            )
            .withColumn(
                f"{feature_name}_after_avg_all_subs",
                F.mean(f"{feature_name}_after").over(
                    Window.partitionBy("event_partition_date")
                ),
            )
            .withColumn(
                f"target_relative_arpu_increase_{n_days}d_avg_all_subs",
                F.mean(f"target_relative_arpu_increase_{n_days}d").over(
                    Window.partitionBy("event_partition_date")
                ),
            )
        )

        df_spine = df_spine.join(
            df_arpu_uplift,
            on=["subscription_identifier", "event_partition_date"],
            how="left",
        )

    # Remove duplicates to make sure the tuple (subscriber, date, child code, is unique)
    # We order by the target to prioritize tracked responses with a positive response
    df_spine = df_spine.withColumn(
        "aux_row_number",
        F.row_number().over(
            Window.partitionBy(
                "subscription_identifier", "contact_date", "music_campaign_type"
            ).orderBy(F.col("target_response").desc_nulls_last())
        ),
    )
    df_spine = df_spine.filter(F.col("aux_row_number") == 1).drop("aux_row_number")
    df_spine = df_spine.withColumn(
        "music_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("contact_date"),
            F.lit("_"),
            F.col("music_campaign_type"),
        ),
    )
    return df_spine


def node_calling_melody():
    l0_product_ru_a_callingmelody_daily = catalog.load(
        "l0_product_ru_a_callingmelody_daily"
    )
    l0_product_ru_a_callingmelody_daily.where(
        "rbt_sub_group = 'ACTIVATE FREE-TRIAL' "
    ).selectExpr(
        "*",
        " date(CONCAT( year(date(day_id)),'-',month(date(day_id)),'-01') ) as month_id",
    ).groupby(
        "day_id"
    ).agg(
        F.count("*").alias("Total_transaction_per_day"),
        F.countDistinct("access_method_num").alias("Distinct_sub"),
    ).orderBy(
        "day_id"
    ).show(
        100
    )

    l0_product_ru_a_callingmelody_daily.where(
        "rbt_sub_group LIKE 'RECURRING%' "
    ).selectExpr(
        "*",
        " date(CONCAT( year(date(day_id)),'-',month(date(day_id)),'-01') ) as month_id",
    ).groupby(
        "month_id", "rbt_sub_group"
    ).agg(
        F.count("*").alias("Total_transaction_per_month"),
        F.countDistinct("access_method_num").alias("Distinct_sub"),
    ).orderBy(
        "month_id"
    ).show(
        100
    )

    l0_product_ru_a_callingmelody_daily.selectExpr(
        "*",
        " date(CONCAT( year(date(day_id)),'-',month(date(day_id)),'-01') ) as month_id",
    ).where("month_id = date('2020-10-01') ").groupby("month_id", "rbt_sub_group").agg(
        F.count("*").alias("Total_transaction"),
        F.countDistinct("access_method_num").alias("Distinct_sub"),
    ).sort(
        F.desc("Total_transaction")
    ).show(
        100
    )
    l0_product_ru_a_callingmelody_daily.where("network_type = '3GPost-paid'").groupby(
        "rbt_group"
    ).agg(F.count("*")).show()

    l0_product_ru_a_callingmelody_daily.where(
        "network_type = '3GPre-paid' "
    ).selectExpr(
        "*",
        " date(CONCAT( year(date(day_id)),'-',month(date(day_id)),'-01') ) as month_id",
    ).groupby(
        "month_id"
    ).agg(
        F.count("*").alias("Total_transaction_per_month"),
        F.countDistinct("access_method_num").alias("Distinct_sub"),
    ).orderBy(
        "month_id"
    ).show(
        100
    )

    l0_product_ru_a_callingmelody_daily.where(
        "rbt_sub_group LIKE 'RECURRING NORMAL' AND network_type = '3GPre-paid' "
    ).selectExpr(
        "*",
        " date(CONCAT( year(date(day_id)),'-',month(date(day_id)),'-01') ) as month_id",
    ).groupby(
        "month_id"
    ).agg(
        F.count("*").alias("Total_transaction_per_month"),
        F.countDistinct("access_method_num").alias("Distinct_sub"),
    ).orderBy(
        "month_id"
    ).show(
        100
    )

    l0_product_ru_a_callingmelody_daily.where(
        "rbt_sub_group NOT LIKE 'RECURRING NORMAL' AND rbt_sub_group LIKE 'RECURRING%'  "
    ).selectExpr(
        "*",
        " date(CONCAT( year(date(day_id)),'-',month(date(day_id)),'-01') ) as month_id",
    ).groupby(
        "month_id"
    ).agg(
        F.avg("net_revenue")
    ).show()


def fix_input_table(l5_music_lift_tbl):
    l5_music_lift_tbl.count()
    l5_music_lift_tbl.dropDuplicates(
        ["subscription_identifier", "start_of_week", "music_campaign_type"]
    ).count()


def node_l0_calling_melody_target_variable(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l0_product_ru_a_callingmelody_daily: DataFrame,
    l3_customer_profile_include_1mo_non_active: DataFrame,
    l0_calling_melody_campaign_lift_table: DataFrame,
    start_date,
    end_date,
) -> DataFrame:
    spark = get_spark_session()
    # start_date = 'start_date'
    # end_date = 'end_date'
    l0_campaign_tracking_contact_list_pre_full_load_limited_date = l0_campaign_tracking_contact_list_pre_full_load.where(
        """date(contact_date) >= date('"""
        + start_date
        + """')
            AND date(contact_date) <= date('"""
        + end_date
        + """')"""
    )  # with limited date

    max_update = l0_campaign_tracking_contact_list_pre_full_load_limited_date.groupby(
        "subscription_identifier", "contact_date", "campaign_child_code",
    ).agg(F.max("update_date").alias("update_date"))

    l0_campaign_tracking_contact_list_pre_updated = l0_campaign_tracking_contact_list_pre_full_load_limited_date.join(
        max_update,
        [
            "subscription_identifier",
            "contact_date",
            "campaign_child_code",
            "update_date",
        ],
        "inner",
    )

    calling_melody_response_df_new = (
        l0_campaign_tracking_contact_list_pre_updated.where(
            "campaign_child_code = 'CallingML.2.*' "
        )
        .withColumn("music_campaign_type", F.lit("Calling_Melody_New_Acquire"))
        .selectExpr(
            "campaign_child_code",
            "subscription_identifier as old_subscription_identifier",
            "date(register_date) as register_date",
            """CASE WHEN response = 'N' THEN 0
                WHEN response = 'Y' THEN 1
                END as target_response""",
            "date(contact_date) as contact_date",
            "music_campaign_type",
        )
    )

    # calling_melody_response_df_existing = (
    #     l0_campaign_tracking_contact_list_pre_updated.where(
    #         """lower(campaign_name) NOT LIKE '%free%'
    #         AND lower(campaign_name) NOT LIKE '%nonuser%'
    #         AND lower(campaign_name) NOT LIKE '%non user%'
    #         AND lower(campaign_name) LIKE '%melody%'"""
    #     )
    #     .withColumn("music_campaign_type", F.lit("Calling_Melody_Existing_Upsell"))
    #     .selectExpr(
    #         "campaign_child_code",
    #         "subscription_identifier as old_subscription_identifier",
    #         "date(register_date) as register_date",
    #         """CASE WHEN response = 'N' THEN 0
    #             WHEN response = 'Y' THEN 1
    #             END as target_response""",
    #         "date(contact_date) as contact_date",
    #         "music_campaign_type",
    #     )
    # )

    l0_product_ru_a_callingmelody_daily_limited_date = l0_product_ru_a_callingmelody_daily.where(
        """date(day_id) >= date('"""
        + start_date
        + """')
            AND date(day_id) <= date('"""
        + end_date
        + """')"""
    )
    l0_product_ru_a_callingmelody_daily_distinct = l0_product_ru_a_callingmelody_daily_limited_date.groupby(
        'access_method_num').agg(F.count('*').alias("CNT")).drop('CNT')

    month = '2021-06'
    df = pd.DataFrame({
        'all_dates': pd.date_range(
            start=pd.Timestamp(month),
            end=pd.Timestamp(month) + pd.offsets.MonthEnd(0),
            freq='D'
        )
    })

    l0_product_ru_a_callingmelody_daily_with_dates = l0_product_ru_a_callingmelody_daily_distinct.crossJoin(df)

    l0_product_ru_a_callingmelody_daily_target_response = l0_product_ru_a_callingmelody_daily_limited_date.selectExpr(
        "access_method_num", "date(day_id) as contact_date",
        "CASE WHEN song_id IS NOT NULL AND net_revenue > 0 THEN 1 ELSE  0 END AS target_response", "rbt_sub_group",
        "song_id", "net_revenue", "content_name", "content_type", "content_style", "content_mood")

    l0_product_ru_a_callingmelody_daily_with_dates = l0_product_ru_a_callingmelody_daily_with_dates.selectExpr(
        "access_method_num", "date(all_dates) as contact_date")

    all_records = l0_product_ru_a_callingmelody_daily_with_dates.join(l0_product_ru_a_callingmelody_daily_target_response,
                                                                   ["access_method_num", "contact_date"], "left")

    distinct_purchaser = all_records.where("target_response = 1").groupby("access_method_num").agg(
        F.count("*").alias("CNT")).drop("CNT")

    negative_response = all_records.join(distinct_purchaser, ["access_method_num"], "left_anti")

    pre_final_df = all_records.where("target_response = 1").selectExpr("access_method_num", "contact_date",
                                                                       "target_response").union(
        negative_response.selectExpr("access_method_num", "contact_date", "0 as target_response"))

    final_df = pre_final_df.join(
        l3_customer_profile_include_1mo_non_active
        , ["access_method_num"]
        , "left"
    ).join(
        l0_calling_melody_campaign_lift_table.selectExpr("campaign_child_code", "old_subscription_identifier")
        , ["old_subscription_identifier"]
        , "left"
    ).withColumn("music_campaign_type", F.lit("Calling_Melody_Existing_Upsell"))

    calling_melody_response_df_existing = final_df.selectExpr(
        "campaign_child_code",
        "old_subscription_identifier",
        "date(register_date) as register_date",
        "target_response",
        "date(contact_date) as contact_date",
        "music_campaign_type",
    )

    calling_melody_response_df_final = calling_melody_response_df_new.union(
        calling_melody_response_df_existing
    )

    # Total_negative_response = Total_campaign - Total_positive_response
    # random_neg_size = (Total_positive_response * 4) / Total_negative_response
    # non_responder, others = calling_melody_response_df.where(
    #     "target_response = 0"
    # ).randomSplit([random_neg_size, 1 - random_neg_size])
    # train_test_df = non_responder.union(
    #     calling_melody_response_df.where("target_response = 1")
    # )

    return calling_melody_response_df_final
