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


def create_dataupsell_optimal_offer(
    l5_du_offer_score_with_package_preference: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load,
    du_campaign_offer_bau,
    du_campaign_offer_new_experiment,
    du_campaign_offer_reference,
    du_control_campaign_child_code,
    schema_name,
    prod_schema_name,
    dev_schema_name,
):
    spark = get_spark_session()
    res = []

    # For every product for new_exp
    for ele in du_campaign_offer_new_experiment:
        if du_campaign_offer_new_experiment[ele] is not None:
            res.append(ele)

    l5_du_offer_score_with_package_preference = l5_du_offer_score_with_package_preference.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(
            datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
        )
        + "')"
    )

    ##### Calculate expected value and flagg #####
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

    # Scope only the product that will be used for new_exp
    l5_du_offer_score_with_package_preference_camp = l5_du_offer_score_with_package_preference.where(
        F.col("model_name").isin(res)
    )

    non_downsell_offer = l5_du_offer_score_with_package_preference_camp.where(
        "downsell_speed = 0 AND downsell_duration = 0"
    )

    # Smart next offer selection if campaign ignore
    all_campaign_child_codes = []
    expr = "CASE "
    for campaign in du_campaign_offer_bau:
        if (du_campaign_offer_bau[campaign] not in all_campaign_child_codes) & (
            du_campaign_offer_bau[campaign] is not None
        ):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_bau[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_bau[campaign])

    for campaign in du_campaign_offer_new_experiment:
        if (
            du_campaign_offer_new_experiment[campaign] not in all_campaign_child_codes
        ) & (du_campaign_offer_new_experiment[campaign] is not None):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_new_experiment[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_new_experiment[campaign])
    for campaign in du_campaign_offer_reference:
        if (du_campaign_offer_reference[campaign] not in all_campaign_child_codes) & (
            du_campaign_offer_reference[campaign] is not None
        ):
            expr = (
                expr
                + "WHEN campaign_child_code = '"
                + du_campaign_offer_reference[campaign]
                + "' THEN '"
                + campaign
                + "' "
            )
            all_campaign_child_codes.append(du_campaign_offer_reference[campaign])
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
        spark.sql(
            "DELETE FROM "
            + schema_name
            + ".du_offer_score_optimal_offer_rework WHERE scoring_day = date('"
            + datetime.datetime.strftime(
                datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
            )
            + "')"
        )
        du_offer_score_optimal_offer.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_score_optimal_offer_rework")
    return du_offer_score_optimal_offer
