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


def update_sandbox_control_group(
    sandbox_framework_2021,
    l0_customer_profile_profile_customer_profile_pre_current_full_load,
    sampling_rate,
    test_group_name,
    test_group_flag,
):
    spark = get_spark_session()
    prepaid_customer_profile_latest = (
        l0_customer_profile_profile_customer_profile_pre_current_full_load.withColumn(
            "G", F.lit(1)
        )
        .groupby("G")
        .agg(F.max("partition_date"))
        .collect()
    )
    profile_customer_profile_pre_current = l0_customer_profile_profile_customer_profile_pre_current_full_load.selectExpr(
        "subscription_identifier as old_subscription_identifier",
        "mobile_status",
        "date(register_date) as register_date",
        "global_control_group",
    ).where(
        "partition_date = '" + str(prepaid_customer_profile_latest[0][1]) + "'"
    )

    # updating customer profile and status
    update_existing_active_sub_status = profile_customer_profile_pre_current.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
    ).join(
        sandbox_framework_2021.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "mckinsey_flag",
            "sandbox_flag",
        ),
        ["old_subscription_identifier", "register_date"],
        "inner",
    )
    updated_existing_sub = update_existing_active_sub_status.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
        "mckinsey_flag",
        "CASE WHEN sandbox_flag != 'gcg' AND global_control_group = 'Y' THEN 'gcg' ELSE sandbox_flag END AS sandbox_flag",
    )
    # updating customer that already churn
    churned_sub = sandbox_framework_2021.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mckinsey_flag",
        "sandbox_flag",
        "global_control_group",
    ).join(
        profile_customer_profile_pre_current.selectExpr(
            "old_subscription_identifier", "register_date"
        ),
        ["old_subscription_identifier"],
        "left_anti",
    )
    updated_churned_sub = churned_sub.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "'CHURN' as mobile_status",
        "global_control_group",
        "mckinsey_flag",
        "sandbox_flag",
    )
    # assigning new sub according to random sampling size
    new_sub = profile_customer_profile_pre_current.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
    ).join(
        sandbox_framework_2021.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "mckinsey_flag",
            "sandbox_flag",
        ),
        ["old_subscription_identifier", "register_date"],
        "left_anti",
    )

    new_sub_non_gcg = new_sub.where("global_control_group != 'Y'")
    new_sub_gcg = new_sub.where("global_control_group = 'Y'")

    new_experiment, bau, bau_freeze = new_sub_non_gcg.randomSplit([0.20, 0.72, 0.08])
    new_sub_gcg = new_sub_gcg.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
        "'gcg' as mckinsey_flag",
        "'gcg' as sandbox_flag",
    )
    new_experiment = new_experiment.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
        "'' as mckinsey_flag",
        "'new_experiment' as sandbox_flag",
    )
    bau = bau.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
        "'' as mckinsey_flag",
        "'bau_2021' as sandbox_flag",
    )
    bau_freeze = bau_freeze.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
        "'' as mckinsey_flag",
        "'bau_2020' as sandbox_flag",
    )

    assigned_new_sub = new_sub_gcg.union(new_experiment).union(bau).union(bau_freeze)

    # union all updated sandbox subs
    updated_sub = updated_existing_sub.union(updated_churned_sub).union(
        assigned_new_sub
    )

    # flag mckinsey sandbox according to mckinsey churn/ard sandbox monthly update
    cvm_sandbox = spark.sql(
        """SELECT crm_sub_id as old_subscription_identifier,date(register_date) as register_date,target_group as mckinsey_flag FROM prod_delta.cvm_prepaid_customer_groups
                 WHERE target_group in ('TG_2020_CVM_V3','CG_2020_CVM_V3') """
    )
    updated_sandbox = updated_sub.select(
        "old_subscription_identifier",
        "register_date",
        "mobile_status",
        "global_control_group",
        "sandbox_flag",
    ).join(cvm_sandbox, ["old_subscription_identifier", "register_date"], "inner")

    updated_sandbox = updated_sandbox.dropDuplicates(["old_subscription_identifier","register_date"])

    updated_sandbox.createOrReplaceTempView("updated_sandbox")
    spark.sql("""DROP TABLE IF EXISTS prod_dataupsell.sandbox_framework_2021_tmp""")
    spark.sql(
        """ CREATE TABLE prod_dataupsell.sandbox_framework_2021_tmp
                  AS 
                  SELECT * FROM updated_sandbox"""
    )
    spark.sql("""DROP TABLE IF EXISTS prod_dataupsell.sandbox_framework_2021""")
    spark.sql(
        """ CREATE TABLE prod_dataupsell.sandbox_framework_2021
                  AS 
                  SELECT * FROM prod_dataupsell.sandbox_framework_2021_tmp"""
    )

    return updated_sandbox
