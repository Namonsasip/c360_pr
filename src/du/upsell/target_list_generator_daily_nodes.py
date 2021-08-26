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
        list_date,
        mode,
        delta_table_schema,
        target_list_path,
):
    """
    Args:
        disney_tg_prediction: A pyspark dataframe with the prediction of the TG list
        disney_usecase_control_group_table: A table that stores the target list of both CG & TG for the disney
        list_date: Date that generates the list (set to be the next day from the current)
        mode: prod or dev
        delta_table_schema: prod or dev table
        target_list_path: Path to save the file for the DE & NGCM

    Returns:
    """

    # Select top 3 deciles propensity score
    window = Window.partitionBy().orderBy(F.col("propensity").desc())
    tg_top_3_deciles = disney_tg_prediction.select(
        "*", F.percent_rank().over(window).alias("rank")).filter(F.col("rank") <= 0.3)

    # Split CG and TG -> We union CG with the eligible TG group
    # Inner join tg_target with the TG in disney_cg_tg_group (a table that store the target list of both CG & TG)
    # From the inner join, all of the TG prediction that has propensity < top 30% will be excluded

    disney_usecase_cg = disney_usecase_control_group_table.where("usecase_control_group LIKE '%CG' AND "
                                                                 "usecase_control_group != 'GCG'")

    ######################
    ## Gen list to BLOB ##
    ######################

    cg = disney_usecase_cg.select('old_subscription_identifier')

    # list_date = datetime.datetime.now() + datetime.timedelta(hours=7) + datetime.timedelta(days=1)
    cg = cg.withColumn('data_date', F.lit(list_date.strftime("%Y-%m-%d")))
    cg = cg.withColumn('MA_ID', F.lit('DisneyPre.1.2'))
    cg = cg.withColumn('MA_NAME', F.lit('DisneyPre.1.2_30D_49B_Disney_BAU_CG'))
    cg = cg.withColumn('expired_date', F.date_add(cg['data_date'], 15))

    tg = tg_top_3_deciles.selectExpr('subscription_identifier AS old_subscription_identifier')

    # list_date = datetime.datetime.now() + datetime.timedelta(hours=7) + datetime.timedelta(days=1)
    tg = tg.withColumn('data_date', F.lit(list_date.strftime("%Y-%m-%d")))
    tg = tg.withColumn('MA_ID', F.lit('DisneyPre.1.1'))
    tg = tg.withColumn('MA_NAME', F.lit('DisneyPre.1.1_30D_49B_Disney_Model_TG'))
    tg = tg.withColumn('expired_date', F.date_add(tg['data_date'], 15))

    eligible_list = cg.union(tg)
    eligible_list = eligible_list.dropDuplicates(['old_subscription_identifier'])

    print("Convert pyspark df to pandas df")
    pdf = eligible_list.toPandas()

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

    return pdf
