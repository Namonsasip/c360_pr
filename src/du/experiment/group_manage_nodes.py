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
    cvm_sandbox_gcg: DataFrame,
    sampling_rate,
    test_group_name,
    test_group_flag,
    partition_date_str,
) -> DataFrame:
    # l0_customer_profile_profile_customer_profile_pre_current_full_load = catalog.load(
    #     "l0_customer_profile_profile_customer_profile_pre_current_full_load"
    # )
    # sampling_rate = [0.9, 0.1]
    # test_group_name = ["target_group", "control_group"]
    # test_group_flag = ["TG", "CG"]
    # max_date = (
    #     l0_customer_profile_profile_customer_profile_pre_current_full_load.withColumn(
    #         "G", F.lit(1)
    #     )
    #     .groupby("G")
    #     .agg(F.max("partition_date").alias("max_partition_date"))
    #     .collect()
    # )
    # prepaid_customer_profile_latest = l0_customer_profile_profile_customer_profile_pre_current_full_load.where(
    #     "partition_date = " + str(max_date[0][1]) + ""
    # )

    spark = get_spark_session()
    cvm_sandbox_gcg_df = cvm_sandbox_gcg.selectExpr(
        "crm_sub_id as subscription_identifier",
        "date(register_date) as register_date",
        "target_group",
    )
    cvm_sandbox = spark.sql(
        """SELECT crm_sub_id as subscription_identifier,date(register_date) as register_date,target_group
                 FROM prod_delta.cvm_prepaid_customer_groups
                 WHERE target_group LIKE '%2020_CVM_V3' """
    )
    cvm_sandbox_excl_gcg = cvm_sandbox.join(
        cvm_sandbox_gcg_df, ["subscription_identifier", "register_date"], "left_anti"
    )
    corrected_cvm_sandbox = cvm_sandbox_excl_gcg.union(cvm_sandbox_gcg_df)
    # partition_date_str= "20200918"
    prepaid_customer_profile_latest = l0_customer_profile_profile_customer_profile_pre_current_full_load.where(
        "partition_date = " + partition_date_str
    )
    prepaid_customer_profile_latest = prepaid_customer_profile_latest.select(
        "subscription_identifier",
        "mobile_status",
        "register_date",
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
        "cust_type",
        "partition_date",
    ).where("cust_type = 'R' AND mobile_status LIKE 'S%'")
    # gomo = prepaid_customer_profile_latest.where(
    #     "promotion_name like 'INS_GM%'  "
    # )
    # gomo = gomo.select("subscription_identifier", "register_date")
    # Sim2fly and Traveller SIM, INS_GM% is GOMO, NU Mobile already migrated to GOMO
    simtofly = prepaid_customer_profile_latest.where(
        """ promotion_group_tariff = 'SIM 2 Fly' 
        OR promotion_group_tariff = 'Traveller SIM' 
        OR promotion_name like 'INS_GM%' """
    )
    simtofly = simtofly.select("subscription_identifier", "register_date")
    vip_rf = prepaid_customer_profile_latest.where(
        "vip_flag = 'Y' OR royal_family_flag = 'Y'"
    )
    vip_rf = vip_rf.select("subscription_identifier", "register_date")
    vip_rf = vip_rf.join(
        simtofly, ["subscription_identifier", "register_date"], "leftanti"
    ).dropDuplicates(["subscription_identifier", "register_date"])
    vip_rf_simtofly = simtofly.union(vip_rf).dropDuplicates(
        ["subscription_identifier", "register_date"]
    )

    # Re-check & exclude Sim2fly VIP RF from sandbox subs
    cvm_sandbox_excl_viprf_simfly = corrected_cvm_sandbox.join(
        vip_rf_simtofly, ["subscription_identifier", "register_date"], "leftanti"
    )

    # Exclude Sim2Fly VIP RF From All sub base
    default_customer = prepaid_customer_profile_latest.join(
        vip_rf_simtofly, ["subscription_identifier", "register_date"], "leftanti"
    )
    default_customer = default_customer.select(
        "subscription_identifier", "register_date"
    )

    # Exclude CVM sandbox from GCG randomization
    default_customer = default_customer.join(
        cvm_sandbox_excl_viprf_simfly,
        ["subscription_identifier", "register_date"],
        "leftanti",
    )

    groups = default_customer.randomSplit(sampling_rate)
    test_groups = vip_rf.withColumn("group_name", F.lit("vip_rf")).withColumn(
        "group_flag", F.lit("V")
    )
    test_groups = test_groups.union(
        simtofly.withColumn("group_name", F.lit("simtofly_gomo")).withColumn(
            "group_flag", F.lit("O")
        )
    )
    # Starting to randomize Pre-paid GCG
    iterate_n = 0
    for g in groups:
        test_groups = test_groups.union(
            g.withColumn("group_name", F.lit(test_group_name[iterate_n])).withColumn(
                "group_flag", F.lit(test_group_flag[iterate_n])
            )
        )
        iterate_n += 1

    # Add CVM Sandbox back to default customer
    test_groups = test_groups.union(
        cvm_sandbox_excl_viprf_simfly.drop("target_group")
        .where("target_group LIKE '%2020_CVM_V3'")
        .withColumn("group_name", F.lit("Default"))
        .withColumn("group_flag", F.lit("N"))
    )

    test_groups = test_groups.union(
        cvm_sandbox_excl_viprf_simfly.drop("target_group")
        .where("target_group LIKE 'GCG'")
        .withColumn("group_name", F.lit("GCG"))
        .withColumn("group_flag", F.lit("Y"))
    )

    test_groups = test_groups.join(
        prepaid_customer_profile_latest,
        ["subscription_identifier", "register_date"],
        "inner",
    )
    return test_groups


def create_postpaid_test_groups(
    l0_customer_profile_profile_customer_profile_post_current_full_load: DataFrame,
    l3_customer_profile_include_1mo_non_active: DataFrame,
    sampling_rate,
    test_group_name,
    test_group_flag,
    partition_date_str,
) -> DataFrame:
    # l0_customer_profile_profile_customer_profile_post_current_full_load = catalog.load(
    #     "l0_customer_profile_profile_customer_profile_post_current_full_load"
    # )
    # l3_customer_profile_include_1mo_non_active = catalog.load(
    #     "l3_customer_profile_include_1mo_non_active"
    # )
    # sampling_rate = [0.9, 0.1]
    # test_group_name = ["target_group", "control_group"]
    # test_group_flag = ["TG", "CG"]
    # max_date = (
    #     l0_customer_profile_profile_customer_profile_post_current_full_load.withColumn(
    #         "G", F.lit(1)
    #     )
    #     .groupby("G")
    #     .agg(F.max("partition_date").alias("max_partition_date"))
    #     .collect()
    # )
    # postpaid_customer_profile_latest = l0_customer_profile_profile_customer_profile_post_current_full_load.where(
    #     "partition_date = " + str(max_date[0][1]) + ""
    # )

    max_date = (
        l3_customer_profile_include_1mo_non_active.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("partition_month").alias("max_partition_month"))
        .collect()
    )

    monthly_customer_profile_latest = l3_customer_profile_include_1mo_non_active.where(
        "partition_month = date('" + str(max_date[0][1]) + "')"
    )
    # Filter Only Residential Customer Profile
    residential_profile = monthly_customer_profile_latest.selectExpr(
        "old_subscription_identifier as subscription_identifier",
        # "norms_net_revenue",
        "register_date",
        # "current_package_name",
        # "charge_type",
    ).where("charge_type = 'Post-paid' AND moc_cust_type = 'RESIDENTIAL_MOC'")

    postpaid_monthly_profile = monthly_customer_profile_latest.selectExpr(
        "old_subscription_identifier as subscription_identifier",
        # "norms_net_revenue",
        "moc_cust_type",
        "register_date",
        # "current_package_name",
        # "charge_type",
    ).where("charge_type = 'Post-paid'")

    monthly_customer_profile_latest.where("charge_type = 'Post-paid' ").groupby(
        "moc_cust_type"
    ).agg(F.count("*")).show()

    # residential_profile.groupby("charge_type").agg(
    #     F.avg("norms_net_revenue"),
    #     F.stddev("norms_net_revenue"),
    #     F.min("norms_net_revenue"),
    #     F.max("norms_net_revenue"),
    # ).show()

    postpaid_customer_profile_latest = l0_customer_profile_profile_customer_profile_post_current_full_load.where(
        "partition_date = " + partition_date_str
    )
    postpaid_customer_profile_latest = postpaid_customer_profile_latest.selectExpr(
        "subscription_identifier",
        "mobile_status",
        "register_date",
        "service_month",
        "ma_age as age",
        "register_gender_code as gender",
        "mobile_segment",
        "current_promotion_title_ma as promotion_name",
        "vip_flag",
        "royal_family_flag",
        "staff_promotion_flag",
        "smartphone_flag",
        "cust_type",
        "partition_date",
    ).where(
        "cust_type = 'R' AND mobile_status in ('Active', 'Suspend', 'Suspend - Credit Limit', 'Suspend - Debt', 'Suspend - Fraud')"
    )

    postpaid_customer_profile_latest.groupby("cust_type").agg(F.count("*")).show()

    postpaid_customer_profile_latest.join(
        postpaid_monthly_profile, ["subscription_identifier", "register_date"], "inner"
    ).groupby("cust_type").agg(F.count("*")).show()
    # postpaid_customer_profile_latest = residential_profile.join(
    #     postpaid_customer_profile_latest, ["subscription_identifier", "register_date"],"inner"
    # )
    vip = postpaid_customer_profile_latest.where("vip_flag NOT IN ('NNN')")
    rf = postpaid_customer_profile_latest.where("royal_family_flag NOT IN ('NNN')")
    vip_rf = vip.union(rf)
    vip_rf = vip_rf.select("subscription_identifier", "register_date").dropDuplicates(
        ["subscription_identifier", "register_date"]
    )
    default_customer = postpaid_customer_profile_latest.join(
        vip_rf, ["subscription_identifier", "register_date"], "leftanti"
    )
    default_customer = default_customer.select(
        "subscription_identifier", "register_date"
    )
    groups = default_customer.randomSplit(sampling_rate)
    test_groups = vip_rf.withColumn("group_name", F.lit("vip_rf")).withColumn(
        "group_flag", F.lit("V")
    )
    iterate_n = 0
    for g in groups:
        test_groups = test_groups.union(
            g.withColumn("group_name", F.lit(test_group_name[iterate_n])).withColumn(
                "group_flag", F.lit(test_group_flag[iterate_n])
            )
        )
        iterate_n += 1
    test_groups = test_groups.join(
        postpaid_customer_profile_latest,
        ["subscription_identifier", "register_date"],
        "inner",
    )
    return test_groups


def create_sanity_check_for_random_test_group(
    df_test_group: DataFrame,
    l3_customer_profile_include_1mo_non_active: DataFrame,
    l3_usage_postpaid_prepaid_monthly: DataFrame,
    group_name_column: str,
    group_flag_column: str,
    csv_file_path: str,
) -> DataFrame:
    # l3_customer_profile_include_1mo_non_active = catalog.load(
    #     "l3_customer_profile_include_1mo_non_active"
    # )
    # l3_usage_postpaid_prepaid_monthly = catalog.load(
    #     "l3_usage_postpaid_prepaid_monthly"
    # )
    #
    # df_test_group = catalog.load("l0_gcg_pre_20200725")
    # group_name_column = "group_name"
    # group_flag_column = "group_flag"
    df_test_group = df_test_group.drop("age").withColumnRenamed(
        "subscription_identifier", "old_subscription_identifier"
    )
    l3_customer_profile_include_1mo_non_active = l3_customer_profile_include_1mo_non_active.selectExpr(
        "old_subscription_identifier",
        "subscription_identifier",
        "access_method_num",
        "register_date",
        "charge_type",
        "age",
        "activation_region",
        "cust_active_this_month",
        "norms_net_revenue",
        "date(partition_month) as partition_month",
    )
    max_date = (
        l3_customer_profile_include_1mo_non_active.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("partition_month").alias("max_partition_month"))
        .collect()
    )

    monthly_customer_profile_latest = l3_customer_profile_include_1mo_non_active.where(
        "partition_month = date('" + str(max_date[0][1]) + "')"
    )

    max_date_usage = (
        l3_usage_postpaid_prepaid_monthly.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("start_of_month").alias("max_start_of_month"))
        .collect()
    )

    l3_usage_postpaid_prepaid_monthly = l3_usage_postpaid_prepaid_monthly.where(
        "start_of_month = date('" + str(max_date_usage[0][1]) + "')"
    )
    sanity_checking_features = df_test_group.join(
        monthly_customer_profile_latest,
        ["old_subscription_identifier", "register_date"],
        "left",
    )

    sanity_checking_features = (
        sanity_checking_features.withColumn(
            "age_less_than_25", F.when(F.col("age") < 25, 1).otherwise(0)
        )
        .withColumn(
            "age_25_45",
            F.when((F.col("age") >= 25) & (F.col("age") <= 45), 1).otherwise(0),
        )
        .withColumn("age_more_than_45", F.when(F.col("age") > 45, 1).otherwise(0))
        .withColumn("age_is_null", F.when(F.col("age").isNull(), 1).otherwise(0))
    )

    sanity_checking_features = (
        sanity_checking_features.withColumn(
            "city_of_residence_XU",
            F.when(F.col("activation_region") == "XU", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_SL",
            F.when(F.col("activation_region") == "SL", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_NL",
            F.when(F.col("activation_region") == "NL", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_XL",
            F.when(F.col("activation_region") == "XL", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_CN",
            F.when(F.col("activation_region") == "CN", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_SU",
            F.when(F.col("activation_region") == "SU", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_NU",
            F.when(F.col("activation_region") == "NU", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_CW",
            F.when(F.col("activation_region") == "CW", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_CE",
            F.when(F.col("activation_region") == "CE", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_CB",
            F.when(F.col("activation_region") == "CB", 1).otherwise(0),
        )
        .withColumn(
            "city_of_residence_null",
            F.when(F.col("activation_region").isNull(), 1).otherwise(0),
        )
        .withColumn("cust_type_R", F.when(F.col("cust_type") == "R", 1).otherwise(0),)
        .withColumn("cust_type_B", F.when(F.col("cust_type") == "B", 1).otherwise(0),)
        .withColumn("cust_type_E", F.when(F.col("cust_type") == "E", 1).otherwise(0),)
        .withColumn("cust_type_G", F.when(F.col("cust_type") == "G", 1).otherwise(0),)
        .withColumn("cust_type_I", F.when(F.col("cust_type") == "I", 1).otherwise(0),)
        .withColumn(
            "non_residential_flag", F.when(F.col("cust_type") != "R", 1).otherwise(0),
        )
    )

    sanity_checking_features = sanity_checking_features.withColumn(
        "smartphone_integer", F.when(F.col("smartphone_flag") == "Y", 1).otherwise(0)
    )

    sanity_checking_features = sanity_checking_features.join(
        l3_usage_postpaid_prepaid_monthly.selectExpr(
            "subscription_identifier",
            "usg_total_data_volume_sum/1000000 as usg_total_data_volume_sum_mb",
            "usg_outgoing_total_call_duration_sum",
            "usg_incoming_total_call_duration_sum",
        ),
        ["subscription_identifier"],
        "left",
    )
    sanity_checking_features = sanity_checking_features.withColumn(
        "data_user", F.when(F.col("usg_total_data_volume_sum_mb") < 10, 0).otherwise(1)
    )

    sanity_checking_features = (
        sanity_checking_features.withColumn(
            "ARPU_range_0", F.when(F.col("norms_net_revenue") == 0, 1).otherwise(0),
        )
        .withColumn(
            "ARPU_range_1_100",
            F.when(
                (F.col("norms_net_revenue") > 0) & (F.col("norms_net_revenue") <= 100),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "ARPU_range_101_200",
            F.when(
                (F.col("norms_net_revenue") > 100)
                & (F.col("norms_net_revenue") <= 200),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "ARPU_range_201_300",
            F.when(
                (F.col("norms_net_revenue") > 200)
                & (F.col("norms_net_revenue") <= 300),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "ARPU_range_301_400",
            F.when(
                (F.col("norms_net_revenue") > 300)
                & (F.col("norms_net_revenue") <= 400),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "ARPU_range_401", F.when(F.col("norms_net_revenue") > 400, 1).otherwise(0),
        )
    )

    sanity_checking_features = (
        sanity_checking_features.withColumn(
            "Service_month_range_0_3",
            F.when(F.col("service_month") <= 3, 1).otherwise(0),
        )
        .withColumn(
            "Service_month_range_3_18",
            F.when(
                (F.col("service_month") > 3) & (F.col("service_month") <= 18), 1,
            ).otherwise(0),
        )
        .withColumn(
            "Service_month_range_18_up",
            F.when((F.col("service_month") > 18), 1,).otherwise(0),
        )
    )

    pdf_sanity_check = (
        sanity_checking_features.groupby(group_name_column, group_flag_column)
        .agg(
            F.count("*").alias("TOTAL_SUB"),
            F.sum("cust_type_R").alias("Count_residential_customers_R"),
            F.sum("non_residential_flag").alias(
                "Count_non_residential_customers_Non_R"
            ),
            F.sum("cust_type_E").alias("Count_cust_type_E"),
            F.sum("cust_type_B").alias("Count_cust_type_B"),
            F.sum("cust_type_G").alias("Count_cust_type_G"),
            F.sum("cust_type_I").alias("Count_cust_type_I"),
            F.avg("service_month").alias("Avg_service_month"),
            F.stddev("service_month").alias("Stddev_service_month"),
            F.avg("norms_net_revenue").alias("Avg_Arpu_monthly"),
            F.stddev("norms_net_revenue").alias("Stddev_Arpu_monthly"),
            F.sum("Service_month_range_0_3").alias("sum_service_month_0_3"),
            F.sum("Service_month_range_3_18").alias("sum_service_month_3_18"),
            F.sum("Service_month_range_18_up").alias("sum_service_month_18_up"),
            F.sum("ARPU_range_0").alias("sum_zero_arpu"),
            F.sum("ARPU_range_1_100").alias("sum_ARPU_range_1_100"),
            F.sum("ARPU_range_101_200").alias("sum_ARPU_range_101_200"),
            F.sum("ARPU_range_201_300").alias("sum_ARPU_range_201_300"),
            F.sum("ARPU_range_301_400").alias("sum_ARPU_range_301_400"),
            F.sum("ARPU_range_401").alias("sum_ARPU_range_401_up"),
            F.sum("city_of_residence_XU").alias("sum_city_of_residence_XU"),
            F.sum("city_of_residence_SL").alias("sum_city_of_residence_SL"),
            F.sum("city_of_residence_NL").alias("sum_city_of_residence_NL"),
            F.sum("city_of_residence_XL").alias("sum_city_of_residence_XL"),
            F.sum("city_of_residence_CB").alias("sum_city_of_residence_CB"),
            F.sum("city_of_residence_CN").alias("sum_city_of_residence_CN"),
            F.sum("city_of_residence_SU").alias("sum_city_of_residence_SU"),
            F.sum("city_of_residence_NU").alias("sum_city_of_residence_NU"),
            F.sum("city_of_residence_CW").alias("sum_city_of_residence_CW"),
            F.sum("city_of_residence_CE").alias("sum_city_of_residence_CE"),
            F.sum("city_of_residence_null").alias("sum_city_of_residence_null"),
            F.sum("age_less_than_25").alias("sum_age_less_than_25"),
            F.sum("age_25_45").alias("sum_age_25_45"),
            F.sum("age_more_than_45").alias("sum_age_more_than_45"),
            F.sum("age_is_null").alias("sum_age_is_null"),
            F.sum("smartphone_integer").alias("sum_smartphone_user"),
            F.sum("data_user").alias("sum_data_user"),
        )
        .selectExpr(
            group_name_column,
            group_flag_column,
            "TOTAL_SUB",
            "Avg_service_month",
            "Stddev_service_month",
            "Count_residential_customers_R",
            "Count_non_residential_customers_Non_R",
            "Count_cust_type_E",
            "Count_cust_type_B",
            "Count_cust_type_G",
            "Count_cust_type_I",
            "sum_service_month_0_3/TOTAL_SUB AS percent_sub_service_month_0_3",
            "sum_service_month_3_18/TOTAL_SUB AS percent_sub_service_month_3_18",
            "sum_service_month_18_up/TOTAL_SUB AS percent_sub_service_month_18_up",
            "Avg_Arpu_monthly",
            "Stddev_Arpu_monthly",
            "sum_zero_arpu/TOTAL_SUB AS percent_sub_zero_arpu",
            "sum_ARPU_range_1_100/TOTAL_SUB AS percent_sub_arpu_range_1_100",
            "sum_ARPU_range_101_200/TOTAL_SUB AS percent_sub_arpu_range_101_200",
            "sum_ARPU_range_201_300/TOTAL_SUB AS percent_sub_arpu_range_201_300",
            "sum_ARPU_range_301_400/TOTAL_SUB AS percent_sub_arpu_range_301_400",
            "sum_ARPU_range_401_up/TOTAL_SUB AS percent_sub_arpu_range_401_up",
            "(sum_city_of_residence_XU/TOTAL_SUB) AS percent_city_of_residence_XU",
            "(sum_city_of_residence_SL/TOTAL_SUB) AS percent_city_of_residence_SL",
            "(sum_city_of_residence_NL/TOTAL_SUB) AS percent_city_of_residence_NL",
            "(sum_city_of_residence_XL/TOTAL_SUB) AS percent_city_of_residence_XL",
            "(sum_city_of_residence_CB/TOTAL_SUB) AS percent_city_of_residence_CB",
            "(sum_city_of_residence_CN/TOTAL_SUB) AS percent_city_of_residence_CN",
            "(sum_city_of_residence_SU/TOTAL_SUB) AS percent_city_of_residence_SU",
            "(sum_city_of_residence_NU/TOTAL_SUB) AS percent_city_of_residence_NU",
            "(sum_city_of_residence_CW/TOTAL_SUB) AS percent_city_of_residence_CW",
            "(sum_city_of_residence_CE/TOTAL_SUB) AS percent_city_of_residence_CE",
            "(sum_city_of_residence_null/TOTAL_SUB) AS percent_city_of_residence_null",
            "(sum_age_less_than_25/TOTAL_SUB) as percent_age_less_than_25",
            "(sum_age_25_45/TOTAL_SUB) as percent_age_25_45",
            "(sum_age_more_than_45/TOTAL_SUB) as percent_age_more_than_45",
            "(sum_age_is_null/TOTAL_SUB) as percent_age_is_null",
            "(sum_smartphone_user/TOTAL_SUB) AS percent_smartphone_penetration",
            "(sum_data_user/TOTAL_SUB) AS percent_data_user",
        )
        .toPandas()
    )
    pdf_sanity_check.to_csv(csv_file_path, index=False)
    return sanity_checking_features


def update_du_control_group(
    l0_du_pre_experiment3_groups,
    l0_customer_profile_profile_customer_profile_pre_current_full_load,
    sampling_rate,
    test_group_name,
    test_group_flag,
):
    # spark = get_spark_session()
    # l0_du_pre_experiment3_20200801= catalog.load("l0_du_pre_experiment3_20200801")
    # l0_du_pre_experiment3_20200801.selectExpr(
    #     "subscription_identifier as old_subscription_identifier",
    #     "date(register_date) as register_date",
    #     "group_name",
    #     "group_flag",
    #     "date('2020-08-01') as control_group_created_date",
    # ).createOrReplaceTempView("temp_view_load")
    # spark.sql("DROP TABLE IF EXISTS prod_dataupsell.l0_du_pre_experiment3_groups")
    # spark.sql(
    #     """CREATE TABLE prod_dataupsell.l0_du_pre_experiment3_groups
    #     USING DELTA
    #     PARTITIONED BY (control_group_created_date)
    #     AS
    #     SELECT * FROM temp_view_load"""
    # )
    # l0_du_pre_experiment3_groups = catalog.load("l0_du_pre_experiment3_groups")
    #
    # sampling_rate = [0.8, 0.022, 0.086, 0.003, 0.086, 0.003]
    # test_group_name = [
    #     "ATL_TG",
    #     "ATL_CG",
    #     "BTL1_TG",
    #     "BTL1_CG",
    #     "BTL2_TG",
    #     "BTL2_CG",
    # ]
    # test_group_flag = [
    #     "ATL_TG",
    #     "ATL_CG",
    #     "BTL1_TG",
    #     "BTL1_CG",
    #     "BTL2_TG",
    #     "BTL2_CG",
    # ]
    # l0_customer_profile_profile_customer_profile_pre_current_full_load = catalog.load(
    #     "l0_customer_profile_profile_customer_profile_pre_current_full_load"
    # )

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
    old_prepaid_sub = l0_du_pre_experiment3_groups.select(
        "old_subscription_identifier", "register_date"
    )
    new_prepaid_sub = prepaid_customer_profile_latest.join(
        old_prepaid_sub, ["old_subscription_identifier", "register_date"], "leftanti"
    )

    gomo = new_prepaid_sub.where(
        "promotion_group_tariff = 'GOMO' OR promotion_group_tariff = 'NU Mobile' "
    )
    gomo = gomo.select("old_subscription_identifier", "register_date")
    simtofly = new_prepaid_sub.where("promotion_group_tariff = 'SIM 2 Fly'")
    simtofly = simtofly.select("old_subscription_identifier", "register_date")
    vip_rf = new_prepaid_sub.where("vip_flag = 'Y' OR royal_family_flag = 'Y'")
    vip_rf = vip_rf.select("old_subscription_identifier", "register_date")
    vip_rf_simtofly_gomo = (
        gomo.union(simtofly)
        .union(vip_rf)
        .dropDuplicates(["old_subscription_identifier", "register_date"])
    )
    default_customer = new_prepaid_sub.join(
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
        new_prepaid_sub, ["old_subscription_identifier", "register_date"], "inner",
    )
    max_control_group_created_date = (
        l0_du_pre_experiment3_groups.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("control_group_created_date"))
        .collect()
    )

    if (
        datetime.date(datetime.strptime(str(max_date[0][1]), "%Y%m%d"))
        > max_control_group_created_date[0][1]
    ):
        test_groups.selectExpr(
            "old_subscription_identifier",
            "register_date",
            "group_name",
            "group_flag",
            "'None' as old_group_name",
            "'None' as old_group_flag",
            "date('"
            + datetime.strptime(str(max_date[0][1]), "%Y%m%d").strftime("%Y-%m-%d")
            + "') as control_group_created_date",
        ).write.format("delta").mode("append").saveAsTable(
            "prod_dataupsell.l0_du_pre_experiment5_groups"
        )
    return test_groups


def split_atl_test_group():
    l0_du_pre_experiment3_groups = catalog.load("l0_du_pre_experiment3_groups")
    l0_du_pre_experiment3_groups.show()
    atl_tg = l0_du_pre_experiment3_groups.where("group_name = 'ATL_TG'")
    atl_cg = l0_du_pre_experiment3_groups.where("group_name = 'ATL_CG'")
    the_rest = l0_du_pre_experiment3_groups.where(
        "group_name NOT IN ('ATL_TG','ATL_CG')"
    )
    a_tg, b_tg = atl_tg.randomSplit([0.5, 0.5])
    a_cg, b_cg = atl_cg.randomSplit([0.5, 0.5])
    a_tg = a_tg.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "'ATL_propensity_TG' as group_name",
        "'ATL_propensity_TG' as group_flag",
        "group_name as old_group_name",
        "group_flag as old_group_flag",
        "control_group_created_date",
    )
    b_tg = b_tg.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "'ATL_uplift_TG' as group_name",
        "'ATL_uplift_TG' as group_flag",
        "group_name as old_group_name",
        "group_flag as old_group_flag",
        "control_group_created_date",
    )

    a_cg = a_cg.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "'ATL_propensity_CG' as group_name",
        "'ATL_propensity_CG' as group_flag",
        "group_name as old_group_name",
        "group_flag as old_group_flag",
        "control_group_created_date",
    )
    b_cg = b_cg.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "'ATL_uplift_CG' as group_name",
        "'ATL_uplift_CG' as group_flag",
        "group_name as old_group_name",
        "group_flag as old_group_flag",
        "control_group_created_date",
    )
    the_rest = the_rest.selectExpr(
        "old_subscription_identifier",
        "register_date",
        "group_name",
        "group_flag",
        "group_name as old_group_name",
        "group_flag as old_group_flag",
        "control_group_created_date",
    )

    new_random_group_df = a_tg.union(b_tg).union(a_cg).union(b_cg).union(the_rest)
    new_random_group_df = new_random_group_df.dropDuplicates([
        "old_subscription_identifier", "register_date"
    ])
    new_random_group_df.createOrReplaceTempView("tmp_table")
    spark.sql("""CREATE TABLE prod_dataupsell.l0_du_pre_experiment5_groups 
                USING DELTA
                AS 
                SELECT * FROM tmp_table""")
    return new_random_group_df
