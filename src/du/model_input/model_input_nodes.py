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
from nba.model_input.model_input_nodes import add_c360_dates_columns


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
            (F.col("highest_price") - F.col("price_exc_vat")) / F.col("highest_price"),
        )
    )

    btl_campaign_mapping = (
        btl_campaign_mapping.where("Discount_percent <= 0.50")
        .drop("Discount_predefine_range")
        .withColumn(
            "Discount_predefine_range",
            F.expr(
                """CASE WHEN highest_price != price_exc_vat AND (highest_price-price_exc_vat)/highest_price >= 0.05 AND (highest_price-price_exc_vat)/highest_price <= 0.10 THEN 1
     WHEN highest_price != price_exc_vat AND (highest_price-price_exc_vat)/highest_price > 0.10 AND (highest_price-price_exc_vat)/highest_price <= 0.20 THEN 2
     WHEN highest_price != price_exc_vat AND (highest_price-price_exc_vat)/highest_price > 0.20 AND (highest_price-price_exc_vat)/highest_price <= 0.30 THEN 3
     WHEN highest_price != price_exc_vat AND (highest_price-price_exc_vat)/highest_price > 0.30 AND (highest_price-price_exc_vat)/highest_price <= 0.40 THEN 4
     WHEN highest_price != price_exc_vat AND (highest_price-price_exc_vat)/highest_price > 0.40 AND (highest_price-price_exc_vat)/highest_price <= 0.50 THEN 5
     WHEN highest_price != price_exc_vat AND (highest_price-price_exc_vat)/highest_price > 0.50 THEN 6 ELSE 0 END"""
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
            F.lit("_") + F.col("Macro_product_Offer_type"),
            F.lit("_"),
            F.col("Discount_predefine_range"),
        ),
    )

    start_day_data = datetime.date(
        datetime.strptime(running_day, "%Y-%m-%d")
    ) - timedelta(60)

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
    ).union(btl_campaign_mapping.select(
        "campaign_child_code",
        "Package_name",
        "macro_product",
        "Macro_product_Offer_type",
        "Discount_percent",
        "COUNT_PRODUCT_SELL_IN_CMP",
        "Discount_predefine_range",
        "rework_macro_product",
    ))
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
    ).agg(F.max("update_date_dt").alias("update_date_dt"))
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
        "response",
        F.expr(
            """CASE WHEN contact_date_dt >= date_sub(response_date_dt,4) THEN 1 ELSE 0 END"""
        ),
    )
    upsell_model_campaign_tracking.where("response = 1").show()
    return upsell_model_campaign_tracking

def create_data_upsell_master_spine_table(l5_du_target_variable_tbl: DataFrame,
                                          l1_customer_profile_union_daily_feature_full_load: DataFrame,
                                          l4_revenue_prepaid_daily_features: DataFrame,
                                          min_feature_days_lag: int,
                                          ) -> DataFrame:


    ######## For testing Purpose
    min_feature_days_lag = 5
    # NBA Function
    df_spine = add_c360_dates_columns(
        l5_du_target_variable_tbl, date_column="contact_date_dt", min_feature_days_lag=min_feature_days_lag
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