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


def create_target_list_file(
        l5_du_offer_daily_eligible_list: DataFrame,
        list_date,
        mode,
        delta_table_schema,
        target_list_path,
):
    # l5_du_offer_daily_eligible_list = catalog.load("l5_du_offer_daily_eligible_list")
    spark = get_spark_session()

    l5_du_offer_daily_eligible_list_latest = l5_du_offer_daily_eligible_list.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(
            datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
        )
        + "')"
    )
    if list_date is None:
        list_date = datetime.datetime.now() + datetime.timedelta(hours=7)
    follow_up_btl_campaign = l5_du_offer_daily_eligible_list_latest.where(
        "campaign_child_code LIKE 'DataOTC.12%' OR campaign_child_code LIKE 'DataOTC.9%' OR campaign_child_code LIKE 'DataOTC.28%'"
    ).dropDuplicates((["old_subscription_identifier"]))

    follow_up_btl_campaign_pdf = follow_up_btl_campaign.selectExpr(
        "date('" + list_date.strftime("%Y-%m-%d") + "') as data_date",
        "old_subscription_identifier",
        "campaign_child_code as dummy01",
        "'Upsell_Prepaid_BTL1' as project",
        "date_add(date('" + list_date.strftime("%Y-%m-%d") + "'),7) as expire_date",
    ).toPandas()
    follow_up_btl_campaign_pdf.to_csv(
        target_list_path
        + "DATA_UPSELL_PCM_BTL_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )

    ordinary_campaign = l5_du_offer_daily_eligible_list_latest.where(
        "campaign_child_code LIKE 'DataOTC.8%' "
    ).dropDuplicates((["old_subscription_identifier"]))
    ordinary_campaign_pdf = ordinary_campaign.selectExpr(
        "date('" + list_date.strftime("%Y-%m-%d") + "') as data_date",
        "old_subscription_identifier",
        "campaign_child_code as dummy01",
    ).toPandas()
    ordinary_campaign_pdf.to_csv(
        target_list_path
        + "DATA_UPSELL_PCM_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )
    to_blacklist = l5_du_offer_daily_eligible_list_latest.selectExpr(
        "*",
        (
                """CASE WHEN (campaign_child_code LIKE 'DataOTC.12%' OR campaign_child_code LIKE 'DataOTC.9%') 
                THEN date_add(date('"""
                + list_date.strftime("%Y-%m-%d")
                + """'),15) 
        WHEN (campaign_child_code LIKE 'DataOTC.8%' OR campaign_child_code LIKE 'DataOTC.28%') THEN  date_add(date('"""
                + list_date.strftime("%Y-%m-%d")
                + """'),4)  END as black_listed_end_date"""
        ),
    )
    if mode == "Development":
        spark.sql(f"""DROP TABLE IF EXISTS {delta_table_schema}.du_offer_blacklist""")
        to_blacklist.createOrReplaceTempView("tmp_tbl")
        spark.sql(
            f"""CREATE TABLE {delta_table_schema}.du_offer_blacklist
        AS 
        SELECT * FROM tmp_tbl"""
        )
    else:  # Production
        to_blacklist.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(f"{delta_table_schema}.du_offer_blacklist")
    return to_blacklist


def create_disney_target_list_file(
        disney_tg_prediction: DataFrame,
        disney_usecase_control_group_table: DataFrame,
        disney_blacklist: DataFrame,
        list_date,
        mode,
        delta_table_schema,
        target_list_path,
):
    """
    Args:
        disney_tg_prediction: A pyspark dataframe with the prediction of the TG list
        disney_usecase_control_group_table: A table that stores the target list of both CG & TG for the disney
        disney_blacklist: A table that stores the blacklist date to each user
        list_date: Date that generates the list (set to be the next day from the current)
        mode: prod or dev
        delta_table_schema: prod or dev table
        target_list_path: Path to save the file for the DE & NGCM

    Returns:
    """

    # Select top 3 deciles propensity score from the TG prediction
    window = Window.partitionBy().orderBy(F.col("propensity").desc())
    tg_top_3_deciles = disney_tg_prediction.select(
        "*", F.percent_rank().over(window).alias("rank")).filter(F.col("rank") <= 0.3).withColumnRenamed(
        'subscription_identifier', 'old_subscription_identifier')

    disney_usecase_cg = disney_usecase_control_group_table.where("usecase_control_group LIKE '%CG' AND "
                                                                 "usecase_control_group != 'GCG'")
    disney_usecase_tg = disney_usecase_control_group_table.where("usecase_control_group LIKE '%TG'")

    disney_usecase_tg_only_top3_deciles = \
        disney_usecase_tg.join(tg_top_3_deciles,
                               on=['old_subscription_identifier'],
                               how='inner'
                               ).select('old_subscription_identifier',
                                        'subscription_identifier',
                                        'access_method_num',
                                        'usecase_control_group',
                                        'global_control_group')

    # Union CG and TG top 3 deciles together before further processing.
    disney_weekly_eligible_list = disney_usecase_cg.unionByName(disney_usecase_tg_only_top3_deciles)

    ######################
    ###### BLACKLIST #####
    ######################

    # Create SCORING_DAY column to be the current day (the day that make the prediction)
    disney_weekly_eligible_list = disney_weekly_eligible_list.withColumn(
        "scoring_day",
        F.lit(
            datetime.datetime.date(
                datetime.datetime.now() + datetime.timedelta(hours=7)
            )
        )
    )

    # Get the latest blacklist date for each customer
    all_blacklisted_sub = disney_blacklist.groupby("old_subscription_identifier").agg(
        F.max("blacklisted_end_date").alias("blacklisted_end_date")
    )

    # Create blacklisted flag for anyone who still within the period of campaign offer period
    disney_weekly_eligible_list = disney_weekly_eligible_list.join(
        all_blacklisted_sub, ["old_subscription_identifier"], "left"
    ).selectExpr(
        "*",
        """CASE WHEN blacklisted_end_date >= scoring_day THEN 1 ELSE 0 END AS blacklisted""",
    )

    # Filter only people who are not blacklisted
    disney_weekly_eligible_list_exclude_blacklist = disney_weekly_eligible_list.where("blacklisted = 0")

    ######################
    ## Gen list to BLOB ##
    ######################

    cg = disney_weekly_eligible_list_exclude_blacklist.where("usecase_control_group LIKE '%CG' AND "
                                                             "usecase_control_group != 'GCG'").select(
        'old_subscription_identifier', 'usecase_control_group')

    # list_date = datetime.datetime.now() + datetime.timedelta(hours=7) + datetime.timedelta(days=1)
    cg = cg.withColumn('data_date', F.lit(list_date.strftime("%Y-%m-%d")))
    cg = cg.withColumn('MA_ID', F.lit('DisneyPre.1.4'))
    cg = cg.withColumn('MA_NAME', F.lit('DisneyPre.1.4_30D_49B_Disney_BAU_SMS'))
    cg = cg.withColumn('expired_date', F.date_add(cg['data_date'], 15))

    tg = disney_weekly_eligible_list_exclude_blacklist.where("usecase_control_group LIKE '%TG'").selectExpr(
        'subscription_identifier AS old_subscription_identifier',
        'usecase_control_group')

    # list_date = datetime.datetime.now() + datetime.timedelta(hours=7) + datetime.timedelta(days=1)
    tg = tg.withColumn('data_date', F.lit(list_date.strftime("%Y-%m-%d")))
    tg = tg.withColumn('MA_ID', F.lit('DisneyPre.1.3'))
    tg = tg.withColumn('MA_NAME', F.lit('DisneyPre.1.3_30D_49B_Disney_Model_SMS'))
    tg = tg.withColumn('expired_date', F.date_add(tg['data_date'], 15))

    eligible_list = cg.union(tg)
    eligible_list = eligible_list.dropDuplicates(['old_subscription_identifier'])

    print("Convert pyspark df to pandas df")
    pdf = eligible_list[['old_subscription_identifier', 'data_date', 'MA_ID', 'MA_NAME', 'expired_date']].toPandas()

    # Export
    print("Export csv to BLOB")

    pdf.to_csv(
        target_list_path
        + "DISNEYPLUS_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )

    ############################
    ## Update blacklist table ##
    ############################
    eligible_list = eligible_list.withColumn("data_date", F.to_date(F.col("data_date")))
    eligible_list = eligible_list.withColumn("expired_date", F.to_date(F.col("expired_date")))

    eligible_list.withColumnRenamed("data_date", "scoring_day").withColumnRenamed(
        "expired_date", "blacklisted_end_date"
    ).select(
        "old_subscription_identifier",
        "scoring_day",
        "MA_ID",
        "MA_NAME",
        "blacklisted_end_date",
        "usecase_control_group",
    ).write.format("delta").mode("append").partitionBy("scoring_day").saveAsTable(
        f"{delta_table_schema}.disney_offer_blacklist"
    )

    return eligible_list
