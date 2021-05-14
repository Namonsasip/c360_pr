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

def create_weekly_low_score_target_list_file(
    l5_du_offer_weekly_low_score_list: DataFrame,
    unused_weekly_low_score_list: DataFrame,
    target_list_path,
    list_date,
):
    # l5_du_offer_daily_eligible_list = catalog.load("l5_du_offer_daily_eligible_list")
    max_day = (
        l5_du_offer_weekly_low_score_list.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("scoring_day"))
        .collect()
    )
    l5_du_offer_weekly_low_score_list_lastest = l5_du_offer_weekly_low_score_list.where(
        "date(scoring_day) = date('"
        + datetime.datetime.strftime(max_day[0][1], "%Y-%m-%d")
        + "')"
    )
    if list_date is None:
        list_date = datetime.datetime.now() + datetime.timedelta(hours=7)
    follow_up_btl_campaign = l5_du_offer_weekly_low_score_list_lastest.where(
        "campaign_child_code LIKE 'DataOTC.33%'"
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

    ordinary_campaign = l5_du_offer_weekly_low_score_list_lastest.where(
        "campaign_child_code LIKE 'DataOTC.32%' "
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
    return l5_du_offer_weekly_low_score_list_lastest