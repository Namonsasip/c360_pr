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


def gcg_contamination_checking_report():
    l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
        "l0_campaign_tracking_contact_list_pre_full_load"
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        "date(contact_date) > date('2020-10-05')"
    )

    l3_customer_profile_union_monthly_feature = catalog.load(
        "l3_customer_profile_union_monthly_feature"
    )
    max_month = (
        l3_customer_profile_union_monthly_feature.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("start_of_month").alias("start_of_month"))
        .drop("G")
        .collect()
    )

    l3_gcg_latest = (
        l3_customer_profile_union_monthly_feature.where(
            """date(start_of_month) = date('"""
            + datetime.datetime.strftime(max_month[0][0], "%Y-%m-%d")
            + """')"""
        )
        .selectExpr(
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "global_control_group",
            "start_of_month",
        )
        .where("global_control_group = 'Y'")
    )
    l0_selected_campaign_columns = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "campaign_system",
        "subscription_identifier as old_subscription_identifier",
        "date(register_date) as register_date",
        "campaign_type",
        "campaign_group",
        "campaign_child_code",
        "campaign_name",
        "date(contact_date) as contact_date",
        "date(update_date) as update_date",
    )

    l0_selected_campaign_columns = l0_selected_campaign_columns.join(
        l3_gcg_latest, ["old_subscription_identifier", "register_date"], "inner"
    )

    l0_updated_campaign = l0_selected_campaign_columns.groupby(
        "old_subscription_identifier",
        "register_date",
        "campaign_child_code",
        "contact_date",
    ).agg(F.max("update_date").alias("update_date"))

    l0_latest_campaign_updated = l0_selected_campaign_columns.join(
        l0_updated_campaign,
        [
            "old_subscription_identifier",
            "register_date",
            "campaign_child_code",
            "contact_date",
            "update_date",
        ],
        "inner",
    )

    l0_latest_campaign_updated.selectExpr(
        "*", "CONCAT(YEAR(contact_date),'-',MONTH(contact_date),'-01') AS contact_month"
    ).groupby(
        "contact_date",
        "campaign_child_code",
        "campaign_system",
        "campaign_type",
        "campaign_group",
        "campaign_name",
        "global_control_group",
    ).agg(
        F.count("*").alias("Total_contacts_daily"),
        F.countDistinct("old_subscription_identifier").alias("Distinct_subs"),
    ).toPandas().to_csv(
        "data/tmp/gcg_campaign_contact_contamination_20201125.csv",
        index=False,
        header=True,
    )

    l0_latest_campaign_updated.selectExpr(
        "*", "CONCAT(YEAR(contact_date),'-',MONTH(contact_date),'-01') AS contact_month"
    ).groupby(
        "contact_month",
        "campaign_child_code",
        "campaign_system",
        "campaign_type",
        "campaign_group",
        "campaign_name",
        "global_control_group",
    ).agg(
        F.count("*").alias("Total_contacts"),
        F.countDistinct("old_subscription_identifier").alias("Distinct_subs"),
    ).toPandas().to_csv(
        "data/tmp/gcg_campaign_contact_contamination_20201125_monthly.csv",
        index=False,
        header=True,
    )

    return df
