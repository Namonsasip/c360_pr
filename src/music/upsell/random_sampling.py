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

def create_prepaid_test_groups(
    l0_customer_profile_profile_customer_profile_pre_current_full_load: DataFrame,
    sampling_rate,
    test_group_name,
    test_group_flag,
) -> DataFrame:
    spark = get_spark_session()
    l0_customer_profile_profile_customer_profile_pre_current_full_load =catalog.load("l0_customer_profile_profile_customer_profile_pre_current_full_load")
    sampling_rate=[
                0.50,
                0.47,
                0.03,
            ]
    test_group_name=[
                "Default",
                "TG",
                "CG",
            ]
    test_group_flag=[
                "Default",
                "TG",
                "CG",
                ]
    max_date = (
        l0_customer_profile_profile_customer_profile_pre_current_full_load.withColumn(
            "G", F.lit(1)
        )
        .groupby("G")
        .agg(F.max("partition_date").alias("max_partition_date"))
        .collect()
    )
    prepaid_customer_profile_latest = l0_customer_profile_profile_customer_profile_pre_current_full_load.where(
        "partition_date = " + str(max_date[0][1])
    )
    prepaid_customer_profile_latest = prepaid_customer_profile_latest.selectExpr(
        "subscription_identifier as old_subscription_identifier",
        "mobile_status",
        "date(register_date) as register_date",
        "service_month",
        "age",
        "gender",
        "mobile_segment",
        "promotion_name",
        "product_type",
        "promotion_group_tariff",
        "vip_flag",
        "royal_family_flag",
        "staff_promotion_flag",
        "smartphone_flag",
    )
    gomo = prepaid_customer_profile_latest.where(
        "promotion_group_tariff = 'GOMO' OR promotion_group_tariff = 'NU Mobile' "
    )
    gomo = gomo.select("old_subscription_identifier", "register_date")
    simtofly = prepaid_customer_profile_latest.where("promotion_group_tariff = 'SIM 2 Fly'")
    simtofly = simtofly.select("old_subscription_identifier", "register_date")
    vip_rf = prepaid_customer_profile_latest.where("vip_flag = 'Y' OR royal_family_flag = 'Y'")
    vip_rf = vip_rf.select("old_subscription_identifier", "register_date")
    vip_rf_simtofly_gomo = (
        gomo.union(simtofly)
        .union(vip_rf)
        .dropDuplicates(["old_subscription_identifier", "register_date"])
    )
    default_customer = prepaid_customer_profile_latest.join(
        vip_rf_simtofly_gomo,
        ["old_subscription_identifier", "register_date"],
        "leftanti",
    )
    default_customer = default_customer.select(
        "old_subscription_identifier", "register_date"
    )
    groups = default_customer.randomSplit(sampling_rate)
    test_groups = vip_rf_simtofly_gomo.withColumn(
        "group_name", F.lit("vip_rf_simtofly_gomo")
    ).withColumn("group_flag", F.lit("V"))
    iterate_n = 0
    for g in groups:
        test_groups = test_groups.union(
            g.withColumn("group_name", F.lit(test_group_name[iterate_n])).withColumn(
                "group_flag", F.lit(test_group_flag[iterate_n])
            )
        )
        iterate_n += 1
    test_groups = test_groups.join(
        prepaid_customer_profile_latest, ["old_subscription_identifier", "register_date"], "inner",
    )
    test_groups.createOrReplaceTempView("tmp_load_view")
    spark.sql("""CREATE TABLE prod_musicupsell.calling_melody_control_group_1
                USING DELTA
                AS
                SELECT old_subscription_identifier,register_date,
                group_name,group_flag,DATE('2020-03-30') as control_group_created_date FROM tmp_load_view
    """)
    return df