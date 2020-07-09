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

from pyspark.sql import DataFrame, Window


def create_btl_experiment_score_distribution(
    l5_du_scored: DataFrame,
    l4_revenue_prepaid_pru_f_usage_multi_features_sum: DataFrame,
):
    # l5_du_scored = catalog.load("l5_du_scored")
    # l4_revenue_prepaid_pru_f_usage_multi_features_sum = catalog.load(
    #     "l4_revenue_prepaid_pru_f_usage_multi_features_sum"
    # )
    max_data_date = (
        l4_revenue_prepaid_pru_f_usage_multi_features_sum.withColumn("G", F.lit(1))
        .groupBy("G")
        .agg(F.max("start_of_week"))
        .collect()
    )
    revenue_features = l4_revenue_prepaid_pru_f_usage_multi_features_sum.where(
        "start_of_week =date('" + str(max_data_date[0][1]) + "')"
    ).selectExpr(
        "subscription_identifier",
        "access_method_num",
        "sum_rev_arpu_net_tariff_rev_exc_reward_sum_weekly_last_four_week as arpu_last_month",
        "sum_rev_arpu_net_tariff_rev_exc_reward_sum_weekly_last_twelve_week/3 as average_arpu_last_three_month",
    )
    selected_model = l5_du_scored.where("model_name = 'Data_NonStop_4Mbps_30_ATL'")
    selected_model = selected_model.join(
        revenue_features, ["subscription_identifier", "access_method_num",], "left"
    )
    count_eligible_sub = selected_model.where(
        "arpu_last_month > 0 AND arpu_last_month <= 120 AND average_arpu_last_three_month <= 120 AND arpu_uplift > 0"
    ).count()
    ranked_model = selected_model.where(
        "arpu_last_month > 0 AND arpu_last_month <= 120 AND average_arpu_last_three_month <= 120 AND arpu_uplift > 0"
    ).withColumn("ranking", F.row_number().over(Window.orderBy("propensity")))
    ranked_model.sort(F.desc("ranking")).show()
    return ranked_model.sort(F.desc("ranking"))


def create_target_list(
    l5_experiment4_eligible_upsell: DataFrame, date: str, size: int
) -> DataFrame:
    l5_experiment4_eligible_upsell = catalog.load("l5_experiment4_eligible_upsell")
    l5_experiment4_eligible_upsell = l5_experiment4_eligible_upsell.dropDuplicates(
        ["old_subscription_identifier"]
    )
    l0_test_groups_experiment4_30062020 = catalog.load(
        "l0_test_groups_experiment4_30062020"
    )
    l0_test_groups_experiment4_30062020 = l0_test_groups_experiment4_30062020.where(
        "smartphone_flag = 'Y'"
    ).selectExpr(
        "subscription_identifier as old_subscription_identifier",
        "register_date",
        "group_flag",
    )
    l5_experiment4_eligible_upsell = l5_experiment4_eligible_upsell.join(
        l0_test_groups_experiment4_30062020,
        ["old_subscription_identifier", "register_date"],
        "inner",
    )
    join_keys = ["old_subscription_identifier"]
    batch1 = l5_experiment4_eligible_upsell.sort(F.desc("propensity")).limit(100000)
    eligible2 = l5_experiment4_eligible_upsell.join(batch1, join_keys, "leftanti")
    batch2 = eligible2.sort(F.desc("propensity")).limit(100000)
    eligible3 = eligible2.join(batch2, join_keys, "leftanti")
    batch3 = eligible3.sort(F.desc("propensity")).limit(100000)
    eligible4 = eligible3.join(batch3, join_keys, "leftanti")
    batch4 = eligible4.sort(F.desc("propensity")).limit(100000)
    eligible5 = eligible4.join(batch4, join_keys, "leftanti")
    batch5 = eligible5.sort(F.desc("propensity")).limit(100000)
    test = (
        batch1.withColumn("contact_date", F.lit("2020-07-08"))
        .union(batch2.withColumn("contact_date", F.lit("2020-07-09")))
        .union(batch3.withColumn("contact_date", F.lit("2020-07-10")))
        .union(batch4.withColumn("contact_date", F.lit("2020-07-11")))
        .union(batch5.withColumn("contact_date", F.lit("2020-07-12")))
    )

    contact_list = test.selectExpr(
        "old_subscription_identifier as crm_sub_id",
        "CASE WHEN group_flag = 'TG' THEN '200623-11' ELSE '200623-12' END AS reference_code",
        "contact_date",
    )
    test.selectExpr(
        "old_subscription_identifier as crm_sub_id",
        "CASE WHEN group_flag = 'TG' THEN '200623-11' ELSE '200623-12' END AS reference_code",
        "contact_date","model_name","propensity","arpu_uplift","arpu_last_month","average_arpu_last_three_month","group_flag"
    ).toPandas().to_csv("data/tmp/DATA_UPSELL.csv",index=False,header=True)
    contact_list.where("contact_date = '2020-07-08'").select(
        "crm_sub_id", "reference_code"
    ).toPandas().to_csv("data/tmp/DATA_UPSELL_20200708.csv",index=False,header=False)

    contact_list.where("contact_date = '2020-07-09'").select(
        "crm_sub_id", "reference_code"
    ).toPandas().to_csv("data/tmp/DATA_UPSELL_20200709.csv",index=False,header=False)

    contact_list.where("contact_date = '2020-07-10'").select(
        "crm_sub_id", "reference_code"
    ).toPandas().to_csv("data/tmp/DATA_UPSELL_20200710.csv",index=False,header=False)

    contact_list.where("contact_date = '2020-07-11'").select(
        "crm_sub_id", "reference_code"
    ).toPandas().to_csv("data/tmp/DATA_UPSELL_20200711.csv",index=False,header=False)

    contact_list.where("contact_date = '2020-07-12'").select(
        "crm_sub_id", "reference_code"
    ).toPandas().to_csv("data/tmp/DATA_UPSELL_20200712.csv",index=False,header=False)


    test.withColumn("G", F.lit(1)).groupBy("G").agg(
        F.countDistinct("old_subscription_identifier")
    ).show()

    campaign_history_master_active = catalog.load("campaign_history_master_active")
    campaign_history_master_active.limit(100).toPandas().to_csv("data/tmp/campaign_master.csv",index=False,header=True)
    campaign_response_input_table.limit(100).toPandas().to_csv("data/tmp/campaign_response.csv",index=False,header=True)
    reporting_kpis.limit(100).toPandas().to_csv("data/tmp/usage_features.csv",index=False,header=True)

    historical_use_case_view_report_table.toPandas().to_csv("data/tmp/final_usecase_view.csv",index=False,header=True)

    return test
