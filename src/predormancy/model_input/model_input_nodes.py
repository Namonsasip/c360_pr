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
from dateutil.relativedelta import *


def create_predormancy_target_variable(
    prepaid_no_activity_daily: DataFrame, dm07_sub_clnt_info: DataFrame
):
    spark = get_spark_session()
    # Get current datetime
    today = datetime.datetime.now() + relativedelta(hours=+7)
    # Create selecting period for inactivity data
    start_period_dt = today + relativedelta(months=-4)
    end_period_dt = today + relativedelta(months=-1)
    print(start_period_dt)
    print(end_period_dt)
    # Select Only Required Data from Prepaid no activity
    inactive_df = prepaid_no_activity_daily.selectExpr(
        "analytic_id",
        "register_date",
        "total_call",
        "recharge_amount",
        "income_call",
        "sgsn_volume_mb",
        "total_net_tariff_revenue",
        "total_trans_ussd",
        "no_activity_n_days",
        "ddate",
    ).where(
        "ddate >= date('"
        + start_period_dt.strftime("%Y-%m-%d")
        + "')"
        + " AND ddate <= date('"
        + end_period_dt.strftime("%Y-%m-%d")
        + "')"
    )

    # Selecting period conditioning for accumulate monthly dataset
    if today.day >= 15:
        demo_t_minus_1_dt = today + relativedelta(months=-1)
        demo_t_minus_2_dt = today + relativedelta(months=-2)
        demo_t_minus_3_dt = today + relativedelta(months=-3)
    else:
        demo_t_minus_1_dt = today + relativedelta(months=-2)
        demo_t_minus_2_dt = today + relativedelta(months=-3)
        demo_t_minus_3_dt = today + relativedelta(months=-4)

    # Select demographic data from hive table to spark dataframe
    demo_t_minus_1_df = dm07_sub_clnt_info.where(
        " YEAR(ddate) = YEAR(date('"
        + demo_t_minus_1_dt.strftime("%Y-%m-%d")
        + "')) AND MONTH(ddate) = MONTH(date('"
        + demo_t_minus_1_dt.strftime("%Y-%m-%d")
        + "'))"
    )
    demo_t_minus_2_df = dm07_sub_clnt_info.where(
        "YEAR(ddate) = YEAR(date('"
        + demo_t_minus_2_dt.strftime("%Y-%m-%d")
        + "')) AND MONTH(ddate) = MONTH(date('"
        + demo_t_minus_2_dt.strftime("%Y-%m-%d")
        + "'))"
    )
    demo_t_minus_3_df = dm07_sub_clnt_info.where(
        "YEAR(ddate) = YEAR(date('"
        + demo_t_minus_3_dt.strftime("%Y-%m-%d")
        + "')) AND MONTH(ddate) = MONTH(date('"
        + demo_t_minus_3_dt.strftime("%Y-%m-%d")
        + "'))"
    )

    # Business criteria filter customer with service month more than or equal to 6
    inactive_filter_servicemonth = (
        inactive_df.join(
            demo_t_minus_1_df.where("service_months >= 6").select(
                "analytic_id", "register_date"
            ),
            ["analytic_id", "register_date"],
            "inner",
        )
        .withColumn(
            "date_no",
            F.datediff(
                F.to_date("ddate"), F.to_date(F.lit(str(today.year) + "-01-01"))
            ),
        )
        .select(
            "ddate",
            "date_no",
            "analytic_id",
            "register_date",
            "total_call",
            "recharge_amount",
            "income_call",
            "sgsn_volume_mb",
            "total_net_tariff_revenue",
            "total_trans_ussd",
            "no_activity_n_days",
            "ddate",
        )
    )
    # Create binary feature of inactivity, if there are any value in each dimension. Infer that user is active today
    inactive_filter_servicemonth = (
        inactive_filter_servicemonth.withColumn(
            "outgoing",
            F.when(inactive_filter_servicemonth.total_call > 0, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "recharge",
            F.when(
                inactive_filter_servicemonth.recharge_amount > 0, F.lit(1)
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "incoming_call",
            F.when(inactive_filter_servicemonth.income_call > 0, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "data_usage",
            F.when(inactive_filter_servicemonth.sgsn_volume_mb > 0, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "revenue",
            F.when(
                inactive_filter_servicemonth.total_net_tariff_revenue > 0, F.lit(1)
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "ussd",
            F.when(
                inactive_filter_servicemonth.total_trans_ussd > 0, F.lit(1)
            ).otherwise(F.lit(0)),
        )
        .drop(
            "total_call",
            "recharge_amount",
            "income_call",
            "sgsn_volume_mb",
            "total_net_tariff_revenue",
            "total_trans_ussd",
        )
    )
    inactive_filter_servicemonth.createOrReplaceTempView("inactivedays_trans")
    # Select max and min day of year within dataset and set 3 weeks prediction gap
    inactive_filter_servicemonth.agg({"date_no": "max"}).collect()[0]["max(date_no)"]
    max = int(
        str(
            inactive_filter_servicemonth.agg({"date_no": "max"}).collect()[0][
                "max(date_no)"
            ]
        )
    )
    inactive_filter_servicemonth.agg({"date_no": "min"}).collect()[0]
    min = int(
        str(
            inactive_filter_servicemonth.agg({"date_no": "min"}).collect()[0][
                "min(date_no)"
            ]
        )
    )
    gap = 21

    # Select no_activity_n_days at specific time period to be target list
    joinedData = spark.sql(
        "SELECT analytic_id,register_date,no_activity_n_days AS no_activity_n_days_"
        + str(max)
        + " FROM inactivedays_trans where date_no = "
        + str(max)
    )
    joinedData = joinedData.join(
        spark.sql(
            "SELECT analytic_id,register_date,no_activity_n_days AS no_activity_n_days_"
            + str(max - 7)
            + " FROM inactivedays_trans where date_no = "
            + str(max - 7)
        ),
        ["analytic_id", "register_date"],
        "inner",
    )
    joinedData = joinedData.join(
        spark.sql(
            "SELECT analytic_id,register_date,no_activity_n_days AS no_activity_n_days_"
            + str(max - 7 - gap)
            + ", ddate as scoring_day FROM inactivedays_trans where date_no = "
            + str(max - 7 - gap)
        ),
        ["analytic_id", "register_date"],
        "inner",
    )

    # Business Selecting criteria, select user who are inactive for consecutive 8 days are target customer
    # Prediction will base on the person who active 2 weeks before that
    target = joinedData.where(
        "no_activity_n_days_"
        + str(max)
        + " = 8 AND no_activity_n_days_"
        + str(max - 7)
        + " = 1"
    ).select("analytic_id", "register_date", "scoring_day")
    active = (
        joinedData.where(
            "no_activity_n_days_"
            + str(max - 7 - gap)
            + " = 0 AND no_activity_n_days_"
            + str(max)
            + " < 8"
        )
        .select("analytic_id", "register_date", "scoring_day")
        .join(target, ["analytic_id", "register_date"], "leftanti")
    )
    # Create first iteration of aggegrating inactivity features
    features = spark.sql(
        """SELECT analytic_id, register_date,SUM(outgoing) as inac_outgoing_w8,
        SUM(recharge) as inac_recharge_w8,SUM(incoming_call) as inac_incoming_call_w8,
        SUM(data_usage) as inac_data_usage_w8,SUM(revenue) as inac_revenue_w8,SUM(ussd) as inac_ussd_w8 
        FROM inactivedays_trans WHERE date_no >"""
        + str(max - 7 - gap - 7)
        + " AND date_no <= "
        + str(max - 7 - gap)
        + " GROUP BY analytic_id,register_date"
    )
    # Join Features with labeled target&Non-target dataset
    features = features.join(
        target.withColumn("label", F.lit(1)).union(
            active.withColumn("label", F.lit(0))
        ),
        ["analytic_id", "register_date"],
        "inner",
    )

    # Loop for aggegrating weekly features
    for i in range(1, 8):
        tmp = spark.sql(
            "SELECT analytic_id, register_date,SUM(outgoing) as inac_outgoing_w"
            + str(8 - i)
            + ",SUM(recharge) as inac_recharge_w"
            + str(8 - i)
            + ",SUM(incoming_call) as inac_incoming_call_w"
            + str(8 - i)
            + ",SUM(data_usage) as inac_data_usage_w"
            + str(8 - i)
            + ",SUM(revenue) as inac_revenue_w"
            + str(8 - i)
            + ",SUM(ussd) as inac_ussd_w"
            + str(8 - i)
            + " FROM inactivedays_trans WHERE date_no >"
            + str(max - 7 - gap - (7 * (i + 1)))
            + " AND date_no <="
            + str(max - 7 - gap - (7 * i))
            + " GROUP BY analytic_id,register_date"
        )
        features = features.join(tmp, ["analytic_id", "register_date"], "inner")
        features.persist()

    # Adding monthly features to dataset
    month = 1
    features = features.join(
        demo_t_minus_1_df.selectExpr(
            "analytic_id",
            "register_date",
            "crm_sub_id as old_subscription_identifier",
            "service_months",
            """CASE WHEN norms_net_revenue_voice+norms_net_revenue_vas+norms_net_revenue_gprs > 0 THEN 'N' 
                ELSE 'Y' END as zero_arpu_minus{}""".format(
                str(month)
            ),
            "crm_most_usage_region as most_usage_region_minus{}".format(str(month)),
            "master_segment_id as customer_value_segment_minus{}".format(str(month)),
            "handset_os as handset_os_minus{}".format(str(month)),
            "norms_net_revenue_vas as norms_net_revenue_vas_minus{}".format(str(month)),
            "norms_net_revenue_gprs as norms_net_revenue_gprs_minus{}".format(
                str(month)
            ),
            "incoming_usage_voice_min as incoming_usage_voice_min_minus{}".format(
                str(month)
            ),
            "norms_net_revenue_voice as norms_net_revenue_voice_minus{}".format(
                str(month)
            ),
            "norms_net_revenue_voice+norms_net_revenue_vas+norms_net_revenue_gprs as arpu_minus{}".format(
                str(month)
            ),
        ),
        ["analytic_id", "register_date"],
        "inner",
    )
    # Adding monthly features to dataset
    month = 2
    features = features.join(
        demo_t_minus_2_df.selectExpr(
            "analytic_id",
            "register_date",
            "crm_sub_id as old_subscription_identifier",
            "service_months",
            """CASE WHEN norms_net_revenue_voice+norms_net_revenue_vas+norms_net_revenue_gprs > 0 THEN 'N' 
                ELSE 'Y' END as zero_arpu_minus{}""".format(
                str(month)
            ),
            "crm_most_usage_region as most_usage_region_minus{}".format(str(month)),
            "master_segment_id as customer_value_segment_minus{}".format(str(month)),
            "handset_os as handset_os_minus{}".format(str(month)),
            "norms_net_revenue_vas as norms_net_revenue_vas_minus{}".format(str(month)),
            "norms_net_revenue_gprs as norms_net_revenue_gprs_minus{}".format(
                str(month)
            ),
            "incoming_usage_voice_min as incoming_usage_voice_min_minus{}".format(
                str(month)
            ),
            "norms_net_revenue_voice as norms_net_revenue_voice_minus{}".format(
                str(month)
            ),
            "norms_net_revenue_voice+norms_net_revenue_vas+norms_net_revenue_gprs as arpu_minus{}".format(
                str(month)
            ),
        ),
        ["analytic_id", "register_date", "old_subscription_identifier"],
        "inner",
    )
    month = 3
    features = features.join(
        demo_t_minus_3_df.selectExpr(
            "analytic_id",
            "register_date",
            "crm_sub_id as old_subscription_identifier",
            "service_months",
            """CASE WHEN norms_net_revenue_voice+norms_net_revenue_vas+norms_net_revenue_gprs > 0 THEN 'N' 
                ELSE 'Y' END as zero_arpu_minus{}""".format(
                str(month)
            ),
            "crm_most_usage_region as most_usage_region_minus{}".format(str(month)),
            "master_segment_id as customer_value_segment_minus{}".format(str(month)),
            "handset_os as handset_os_minus{}".format(str(month)),
            "norms_net_revenue_vas as norms_net_revenue_vas_minus{}".format(str(month)),
            "norms_net_revenue_gprs as norms_net_revenue_gprs_minus{}".format(
                str(month)
            ),
            "incoming_usage_voice_min as incoming_usage_voice_min_minus{}".format(
                str(month)
            ),
            "norms_net_revenue_voice as norms_net_revenue_voice_minus{}".format(
                str(month)
            ),
            "norms_net_revenue_voice+norms_net_revenue_vas+norms_net_revenue_gprs as arpu_minus{}".format(
                str(month)
            ),
        ),
        ["analytic_id", "register_date", "old_subscription_identifier"],
        "inner",
    )

    return features
