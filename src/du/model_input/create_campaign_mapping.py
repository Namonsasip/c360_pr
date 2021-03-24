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


def l4_campaign_mapping_response_product(
    l0_campaign_tracking_campaign_response_master: DataFrame,
):
    max_date = (
        l0_campaign_tracking_campaign_response_master.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("partition_date"))
        .collect()
    )
    l0_campaign_tracking_campaign_response_master = l0_campaign_tracking_campaign_response_master.where(
        "partition_date = '" + max_date[0][1] + "'"
    )
    response_by_child_code = l0_campaign_tracking_campaign_response_master.selectExpr(
        "campaign_type_cvm",
        "campaign_child_code",
        "campaign_name",
        "response_type",
        "offer_period",
        "ussd AS USSD_CODE",
        "SPLIT(ontop_pack_id,',')[0] as promotion_code",
        "campaign_owner",
        "partition_date",
    )
    cnt_num_product = (
        l0_campaign_tracking_campaign_response_master.groupby("campaign_child_code")
        .agg(F.count("*").alias("CNT"))
        .where("CNT = 1")
    )
    campaign_mapping_response_product = response_by_child_code.join(
        cnt_num_product, ["campaign_child_code"], "inner"
    ).where("promotion_code is not null")

    return campaign_mapping_response_product


def l4_campaign_mapping(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    l4_campaign_mapping_response_product: DataFrame,
):
    spark = get_spark_session()

    # This csv file is manually upload to blob storage
    # DS receive this file from marketing owner to map campaign child code with product id
    # Product id should be available in the campaign history in the near future
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # Select latest ontop product master
    product_pru_m_ontop_master = l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
        "partition_date_str", F.col("partition_date").cast(StringType())
    ).select(
        "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("ddate")
    )
    max_master_date = (
        product_pru_m_ontop_master.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("ddate").alias("ddate"))
        .collect()
    )
    product_pru_m_ontop_master = product_pru_m_ontop_master.where(
        "ddate = date('" + max_master_date[0][1].strftime("%Y-%m-%d") + "')"
    )

    # Join ontop product master with campaign child code mapping
    campaign_mapping = product_pru_m_ontop_master.join(
        l4_campaign_mapping_response_product, ["promotion_code"], "inner",
    )

    # Use regex to remove special symbol that could potentially break code in the future
    # rework_macro_product will be used as reference to data-upsell train model
    rework_macro_product = campaign_mapping.withColumn(
        "rework_macro_product",
        F.regexp_replace("package_name_report", "(\.\/|\/|\.|\+|\-|\(|\)|\ )", "_"),
    )
    return rework_macro_product
