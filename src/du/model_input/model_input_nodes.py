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
from du.models.models_nodes import calculate_extra_pai_metrics


def node_l5_du_target_variable_table_new(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    mapping_for_model_training: DataFrame,
    starting_date,
) -> DataFrame:
    spark = get_spark_session()

    # l0_campaign_tracking_contact_list_pre_full_load = spark.sql(
    #     "SELECT * FROM c360_l0.campaign_tracking_contact_list_pre WHERE date(contact_date) >= date('2020-10-01') AND date(contact_date) <= date('2021-01-20')"
    # )

    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(f"date(contact_date) >= date('{starting_date}')")
    latest_campaign_update = l0_campaign_tracking_contact_list_pre_full_load.groupby(
        "subscription_identifier", "campaign_child_code", "contact_date"
    ).agg(F.max("update_date").alias("update_date"))
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.join(
        latest_campaign_update,
        [
            "subscription_identifier",
            "campaign_child_code",
            "contact_date",
            "update_date",
        ],
        "inner",
    )
    latest_campaign_update = l0_campaign_tracking_contact_list_pre_full_load.groupby(
        "subscription_identifier", "campaign_child_code", "contact_date"
    ).agg(F.max("update_date").alias("update_date"))
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.join(
        latest_campaign_update,
        [
            "subscription_identifier",
            "campaign_child_code",
            "contact_date",
            "update_date",
        ],
        "inner",
    )
    upsell_model_campaign_tracking = l0_campaign_tracking_contact_list_pre_full_load.join(
        mapping_for_model_training.drop("partition_date").drop("campaign_category"), ["campaign_child_code"], "inner"
    )
    upsell_model_campaign_tracking = upsell_model_campaign_tracking.withColumn(
        "target_response", F.expr("""CASE WHEN response = 'Y' THEN 1 ELSE 0 END"""),
    )
    upsell_model_campaign_tracking = upsell_model_campaign_tracking.where(
        "rework_macro_product is not null"
    )
    return upsell_model_campaign_tracking


def node_l5_du_target_variable_table(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    mapping_for_model_training: DataFrame,
    running_day,
) -> DataFrame:
    ############ Loading Data & Var assigned for Testing Purpose
    #
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
    #     "l0_campaign_tracking_contact_list_pre_full_load"
    # )
    # mapping_for_model_training = catalog.load("mapping_for_model_training")
    # running_day = "2020-04-01"
    ############
    atl_campaign_mapping = mapping_for_model_training.where(
        "to_model = 1 AND COUNT_PRODUCT_SELL_IN_CMP = 1 AND Macro_product_Offer_type = 'ATL'"
    )
    btl_campaign_mapping = (
        mapping_for_model_training.where(
            "to_model = 1 AND COUNT_PRODUCT_SELL_IN_CMP = 1 AND Macro_product_Offer_type = 'BTL'"
        )
        .drop("Discount_percent")
        .withColumn(
            "Discount_percent",
            (F.col("highest_price") - F.col("price_inc_vat")) / F.col("highest_price"),
        )
    )

    btl_campaign_mapping = (
        btl_campaign_mapping.where("Discount_percent <= 0.50")
        .drop("Discount_predefine_range")
        .withColumn(
            "Discount_predefine_range",
            F.expr(
                """CASE WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price >= 0.05 AND (highest_price-price_inc_vat)/highest_price <= 0.10 THEN 1
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.10 AND (highest_price-price_inc_vat)/highest_price <= 0.20 THEN 2
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.20 AND (highest_price-price_inc_vat)/highest_price <= 0.30 THEN 3
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.30 AND (highest_price-price_inc_vat)/highest_price <= 0.40 THEN 4
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.40 AND (highest_price-price_inc_vat)/highest_price <= 0.50 THEN 5
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.50 THEN 6 ELSE 0 END"""
            ),
        )
    )
    atl_campaign_mapping = atl_campaign_mapping.withColumn(
        "rework_macro_product",
        F.concat(F.col("Macro_product"), F.lit("_"), F.col("Macro_product_Offer_type")),
    )
    btl_campaign_mapping = btl_campaign_mapping.withColumn(
        "rework_macro_product",
        F.concat(
            F.col("Macro_product"),
            F.lit("_"),
            F.col("Macro_product_Offer_type"),
            F.lit("_"),
            F.col("Discount_predefine_range"),
        ),
    )

    start_day_data = datetime.date(
        datetime.strptime(running_day, "%Y-%m-%d")
    ) - timedelta(120)

    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*",
        "date(contact_date) as contact_date_dt",
        "response_date as response_date_dt",
    ).filter(
        F.col("contact_date_dt").between(
            start_day_data, datetime.date(datetime.strptime(running_day, "%Y-%m-%d"))
        )
    )

    upsell_model_campaign_mapping = atl_campaign_mapping.select(
        "campaign_child_code",
        "Package_name",
        "macro_product",
        "Macro_product_Offer_type",
        "Discount_percent",
        "COUNT_PRODUCT_SELL_IN_CMP",
        "Discount_predefine_range",
        "rework_macro_product",
    ).union(
        btl_campaign_mapping.select(
            "campaign_child_code",
            "Package_name",
            "macro_product",
            "Macro_product_Offer_type",
            "Discount_percent",
            "COUNT_PRODUCT_SELL_IN_CMP",
            "Discount_predefine_range",
            "rework_macro_product",
        )
    )
    upsell_model_campaign_tracking = l0_campaign_tracking_contact_list_pre_full_load.join(
        upsell_model_campaign_mapping, ["campaign_child_code"], "inner"
    ).selectExpr(
        "campaign_child_code",
        "subscription_identifier",
        "mobile_no",
        "register_date",
        "contact_control_group",
        "response_date",
        "date(update_date) as update_date_dt",
        "contact_date_dt",
        "response_date_dt",
        "macro_product",
        "Package_name",
        "Macro_product_Offer_type",
        "Discount_percent",
        "COUNT_PRODUCT_SELL_IN_CMP",
        "Discount_predefine_range",
        "rework_macro_product",
    )
    upsell_model_campaign_tracking_latest_update = upsell_model_campaign_tracking.groupBy(
        [
            "campaign_child_code",
            "subscription_identifier",
            "register_date",
            "contact_date_dt",
        ]
    ).agg(
        F.max("update_date_dt").alias("update_date_dt")
    )
    upsell_model_campaign_tracking = upsell_model_campaign_tracking.join(
        upsell_model_campaign_tracking_latest_update,
        [
            "campaign_child_code",
            "subscription_identifier",
            "register_date",
            "contact_date_dt",
            "update_date_dt",
        ],
        "inner",
    )

    # Customer Must response to the campaign within 4 days to be identify as Response = True
    upsell_model_campaign_tracking = upsell_model_campaign_tracking.withColumn(
        "target_response",
        F.expr(
            """CASE WHEN contact_date_dt >= date_sub(response_date_dt,4) THEN 1 ELSE 0 END"""
        ),
    )
    upsell_model_campaign_tracking = upsell_model_campaign_tracking.withColumnRenamed(
        "contact_date_dt", "contact_date"
    )
    upsell_model_campaign_tracking.where("target_response = 1").show()
    return upsell_model_campaign_tracking


def node_l5_du_master_spine_table(
    l5_du_target_variable_tbl: DataFrame,
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    min_feature_days_lag: int,
) -> DataFrame:

    ######## For testing Purpose
    # l5_du_target_variable_tbl = catalog.load("l5_du_target_variable_tbl")
    # l1_customer_profile_union_daily_feature_full_load = catalog.load("l1_customer_profile_union_daily_feature_full_load")
    # l4_revenue_prepaid_daily_features = catalog.load("l4_revenue_prepaid_daily_features")
    # min_feature_days_lag = 5
    ########

    # NBA Function
    df_spine = add_c360_dates_columns(
        l5_du_target_variable_tbl,
        date_column="contact_date",
        min_feature_days_lag=min_feature_days_lag,
    )
    # subscription_identifier is different in L0 and all other C360 levels, so we need to add
    # both of them to the spine, for which we use l1 customer profile as an auxiliary table
    df_spine = df_spine.withColumnRenamed(
        "subscription_identifier", "old_subscription_identifier"
    ).withColumnRenamed("mobile_no", "access_method_num")
    df_spine = df_spine.join(
        l1_customer_profile_union_daily_feature_full_load.select(
            "subscription_identifier", "access_method_num", "event_partition_date",
        ),
        on=["access_method_num", "event_partition_date"],
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
                "subscription_identifier", "contact_date", "campaign_child_code"
            ).orderBy(F.col("target_response").desc_nulls_last())
        ),
    )
    df_spine = df_spine.filter(F.col("aux_row_number") == 1).drop("aux_row_number")
    df_spine = df_spine.withColumn(
        "du_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("contact_date"),
            F.lit("_"),
            F.col("rework_macro_product"),
        ),
    )
    return df_spine


def node_l5_du_master_table_only_accepted(l5_du_master_table: DataFrame,) -> DataFrame:
    return l5_du_master_table.filter(F.col("target_response") == 1)


def node_l5_du_master_table_chunk_debug_acceptance(
    l5_du_master_table: DataFrame, group_target: str, sampling_rate: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_chunk = l5_du_master_table.filter(F.col("rework_macro_product") == group_target)

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        l5_du_master_table, target_column="target_response", by="rework_macro_product"
    )
    l5_du_master_table_chunk_debug = (
        df_chunk.filter(~F.isnull(F.col("target_response")))
        .sample(sampling_rate)
        .toPandas()
    )
    return l5_du_master_table_chunk_debug, pdf_extra_pai_metrics


def fix_analytic_id_key(
    l4_macro_product_purchase_feature_weekly,
    l3_customer_profile_include_1mo_non_active,
    dm07_sub_clnt_info,
):
    # l3_customer_profile_include_1mo_non_active = catalog.load(
    #     "l3_customer_profile_include_1mo_non_active"
    # )
    # target_table = catalog.load("l4_macro_product_purchase_feature_weekly")
    # dm07_sub_clnt_info = catalog.load("dm07_sub_clnt_info")
    spark = get_spark_session()
    dm07_sub_clnt_info = dm07_sub_clnt_info.selectExpr(
        "crm_sub_id as old_subscription_identifier",
        "activation_date as register_date",
        "analytic_id",
    )
    l3_customer_profile_include_1mo_non_active = l3_customer_profile_include_1mo_non_active.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "date(register_date) as register_date",
    )
    analytic_sub = (
        dm07_sub_clnt_info.groupby(
            "old_subscription_identifier", "register_date", "analytic_id"
        )
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
    )
    mck_sub = (
        l3_customer_profile_include_1mo_non_active.groupby(
            "subscription_identifier", "old_subscription_identifier", "register_date"
        )
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
    )
    fixed_key = analytic_sub.join(
        mck_sub, ["old_subscription_identifier", "register_date"], "inner"
    )
    spark.sql(
        "DROP TABLE IF EXISTS prod_dataupsell.l4_macro_product_purchase_feature_weekly_key_fixed"
    )
    l4_macro_product_purchase_feature_weekly.join(
        fixed_key, ["analytic_id", "register_date"], "inner"
    ).write.format("delta").mode("overwrite").partitionBy("start_of_week").saveAsTable(
        "prod_dataupsell.l4_macro_product_purchase_feature_weekly_key_fixed"
    )
    return l4_macro_product_purchase_feature_weekly
