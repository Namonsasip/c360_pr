from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols, get_max_date_from_master_data
from customer360.utilities.spark_util import get_spark_empty_df, get_spark_session


def l3_monthly_product_last_most_popular_promotion(inputDF, inputEF, profileDF):
    if check_empty_dfs([inputDF, inputEF, profileDF]):
        return get_spark_empty_df()

    inputEF = inputEF.withColumn("start_of_month",
                                 F.to_date(F.date_trunc('month', F.to_date((F.col('date_id'))))))

    profileDF = data_non_availability_and_missing_check(df=profileDF, grouping="monthly",
                                                        par_col="start_of_month",
                                                        target_table_name="l3_monthly_product_last_most_popular_promotion")

    inputDF = data_non_availability_and_missing_check(df=inputDF, grouping="monthly",
                                                      par_col="partition_date",
                                                      target_table_name="l3_monthly_product_last_most_popular_promotion"
                                                      )

    inputEF = data_non_availability_and_missing_check(df=inputEF, grouping="monthly",
                                                      par_col="partition_date",
                                                      target_table_name="l3_monthly_product_last_most_popular_promotion"
                                                      )

    min_value = union_dataframes_with_missing_cols(
        [
            inputEF.select(F.max(F.col("start_of_month")).alias("max_date")),
            profileDF.select(F.max(F.col("start_of_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    inputEF = inputDF.filter(F.col("start_of_month") <= min_value)
    profileDF = profileDF.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([inputDF, inputEF, profileDF]):
        return get_spark_empty_df()



    pymtSelectedDF = inputDF.join(inputEF, (inputDF.promotion_code == inputEF.package_id)) \
        .select("promotion_code", "siebel_name", "price", "package_type", "mm_data_speed", "data_quota", "duration",
                "recurring", "date_id", "access_method_num", "start_of_month")
    pymtGroupDF = pymtSelectedDF.join(profileDF, (pymtSelectedDF.access_method_num == profileDF.access_method_num
                                                  and pymtSelectedDF.start_of_month == profileDF.start_of_month)) \
        .select("promotion_code", "siebel_name", "price", "package_type", "mm_data_speed", "data_quota", "duration",
                "recurring", "date_id", "subscription_identifier", "start_of_month")

    pymtLastDF = pymtGroupDF.withColumn("rn", F.expr(
        "row_number() over (partition by subscription_identifier, promotion_code, siebel_name, price,package_type, mm_data_speed, data_quota, duration, recurring, date_id ,start_of_month"
        " order by date_id desc, subscription_identifier desc , promotion_code desc, siebel_name desc, price desc, package_type desc, mm_data_speed desc, data_quota desc, duration desc , recurring desc , start_of_month desc)")).where(
        "rn = 1").drop("rn")

    pymtLastDF = pymtLastDF.select("start_of_month", "subscription_identifier", "promotion_code", "siebel_name",
                                   "price", "package_type",
                                   "mm_data_speed", "data_quota", "duration", "recurring")

    pymtLastDF = pymtLastDF.groupBy(
        ["start_of_month", "subscription_identifier"]).agg(
        F.max("promotion_code").alias("last_promotion_code"),
        F.max("siebel_name").alias("last_siebel_name"),
        F.max("price").alias("last_price"),
        F.max("package_type").alias("last_package_type"),
        F.max("mm_data_speed").alias("last_mm_data_speed"),
        F.max("data_quata").alias("last_data_quata"),
        F.max("duration").alias("last_duration"),
        F.max("recurring").alias("last_recurring"))

    mostProDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "promotion_code"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostSieDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "siebel_name"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostPriDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "price"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostPacDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "package_type"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostMmdDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "mm_data_speed"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostDataDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "data_quata"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostDuraDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "duration"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
        "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostRecDF = pymtGroupDF.groupBy(
        ["start_of_month", "subscription_identifier", "recurring"]).agg(F.max("date_id").alias("date_id"),
                           F.count("*").alias("no_of_pay")).withColumn("rn", F.expr(
         "row_number() over (partition by start_of_month, subscription_identifier order by no_of_pay desc, date_id desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "date_id")

    mostDF = union_dataframes_with_missing_cols(
        [mostProDF, mostSieDF, mostPriDF, mostPacDF, mostMmdDF, mostDataDF, mostDuraDF, mostRecDF])

    mostDF = mostDF.groupBy(
        ["start_of_month", "subscription_identifier"]).agg(
        F.max("promotion_code").alias("most_promotion_code"),
        F.max("siebel_name").alias("most_siebel_name"),
        F.max("price").alias("most_price"),
        F.max("package_type").alias("most_package_type"),
        F.max("mm_data_speed").alias("most_mm_data_speed"),
        F.max("data_quata").alias("most_data_quata"),
        F.max("duration").alias("most_duration"),
        F.max("recurring").alias("most_recurring"))

    resultDF = union_dataframes_with_missing_cols([pymtLastDF, mostDF])

    resultDF = resultDF.groupBy(
        ["start_of_month", "subscription_identifier"]).agg(
        F.max("last_promotion_code").alias("last_promotion_code"),
        F.max("last_siebel_name").alias("last_siebel_name"),
        F.max("last_price").alias("last_price"),
        F.max("last_package_type").alias("last_package_type"),
        F.max("last_mm_data_speed").alias("last_mm_data_speed"),
        F.max("last_data_quata").alias("last_data_quata"),
        F.max("last_duration").alias("last_duration"),
        F.max("last_recurring").alias("last_recurring"),
        F.max("most_promotion_code").alias("most_promotion_code"),
        F.max("most_siebel_name").alias("most_siebel_name"),
        F.max("most_price").alias("most_price"),
        F.max("most_package_type").alias("most_package_type"),
        F.max("most_mm_data_speed").alias("most_mm_data_speed"),
        F.max("most_data_quata").alias("most_data_quata"),
        F.max("most_duration").alias("most_duration"),
        F.max("most_recurring").alias("most_recurring"))

    return resultDF


def merge_with_customer_prepaid_df(source_df: DataFrame,
                                   cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_df, cust_df]):
        return get_spark_empty_df()

    source_df = data_non_availability_and_missing_check(
        df=source_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            source_df.select(
                F.max(F.col("start_of_month")).alias("max_date")),
            cust_df.select(
                F.max(F.col("start_of_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    source_df = source_df.filter(F.col("start_of_month") <= min_value)
    cust_df = cust_df.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([source_df, cust_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['start_of_month', 'subscription_identifier']
    join_key = ['subscription_identifier', 'start_of_month']

    source_df = (source_df
                 .withColumn("subscription_identifier",
                             F.expr("concat(access_method_num, '-', date_format(register_date, 'yyyyMMdd')) ")))

    cust_df = cust_df.select(cust_df_cols)

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.drop("access_method_num", "register_date", "start_of_week", "event_partition_date")

    return final_df


def merge_with_customer_postpaid_df(source_df: DataFrame,
                                    cust_df: DataFrame) -> DataFrame:
    """
    :param source_df:
    :param cust_df:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_df, cust_df]):
        return get_spark_empty_df()

    source_df = data_non_availability_and_missing_check(
        df=source_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            source_df.select(
                F.max(F.col("start_of_month")).alias("max_date")),
            cust_df.select(
                F.max(F.col("start_of_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    source_df = source_df.filter(F.col("start_of_month") <= min_value)
    cust_df = cust_df.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([source_df, cust_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # This code will populate a subscriber id to the data set.
    source_df = source_df.withColumnRenamed("sub_id", "subscription_identifier")

    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['start_of_month', 'subscription_identifier']

    join_key = ['subscription_identifier', 'start_of_month']

    cust_df = cust_df.select(cust_df_cols)

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.drop("start_of_week", "event_partition_date")

    return final_df


def revenue_postpaid_ru_f_sum(input_df, sql):
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = node_from_config(input_df, sql)
    input_df = input_df.withColumnRenamed("sub_id", "subscription_identifier")
    output_df = input_df.drop("start_of_week", "event_partition_date")
    return output_df


def revenue_prepaid_ru_f_sum(input_df, sql):
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = node_from_config(input_df, sql)
    input_df = input_df.withColumnRenamed("c360_subscription_identifier", "subscription_identifier")
    output_df = input_df.drop("start_of_week", "event_partition_date")
    return output_df


################################ feature add norm ################################ 2021-05-17
def l3_merge_vat_with_revenue_prepaid_pru_f_revenue_allocate_usage(source_df: DataFrame, config):
    if check_empty_dfs([source_df]):
        return get_spark_empty_df()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    source_df = source_df.withColumn("vat_id", F.lit("1"))
    source_df = source_df.withColumnRenamed("c360_subscription_identifier", "subscription_identifier")
    vat = spark.createDataFrame([(1.07, "Pre-paid"), (1.0, "Post-paid")], ["vat", "service"])
    vat = vat.withColumn("vat_id", F.lit("1")).filter(F.col("service") == "Pre-paid")
    final_df = source_df.join(vat, (["vat_id"]))
    return final_df


def l3_merge_vat_with_revenue_prepaid_pru_f_active_sub_cross_mao_mao(source_df: DataFrame, config):
    if check_empty_dfs([source_df]):
        return get_spark_empty_df()

    df_cols = ['month_id', 'register_date', 'access_method_num', 'total_amount_mao_mao_voice',
               'c360_subscription_identifier']
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    source_df = source_df.select(df_cols)
    source_df = source_df.withColumnRenamed("c360_subscription_identifier", "subscription_identifier")
    source_df = source_df.withColumn("vat_id", F.lit("1"))
    vat = spark.createDataFrame([(1.07, "Pre-paid"), (1.0, "Post-paid")], ["vat", "service"])
    vat = vat.withColumn("vat_id", F.lit("1")).filter(F.col("service") == "Pre-paid")
    final_df = source_df.join(vat, (["vat_id"]))
    return final_df


def l3_merge_vat_with_revenue_pre_pru_f_active_mao_mao_m_pre_rev_allocate_usg(source_mao_mao_df: DataFrame,
                                                                              source_rev_usg_df: DataFrame, config):
    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_mao_mao_df, source_rev_usg_df]):
        return get_spark_empty_df()

    source_mao_mao_df = data_non_availability_and_missing_check(
        df=source_mao_mao_df, grouping="monthly",
        par_col="partition_month",
        target_table_name="l3_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly",
        missing_data_check_flg='N')

    source_rev_usg_df = data_non_availability_and_missing_check(
        df=source_rev_usg_df, grouping="monthly",
        par_col="partition_month",
        target_table_name="l3_features_prepaid_pru_f_active_sub_cross_mao_mao_cross_rev_allocate_usg_monthly",
        missing_data_check_flg='N')

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            source_mao_mao_df.select(
                F.max(F.col("partition_month")).alias("max_date")),
            source_rev_usg_df.select(
                F.max(F.col("partition_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    source_mao_mao_df = source_mao_mao_df.filter(F.col("partition_month") <= min_value)
    source_rev_usg_df = source_rev_usg_df.filter(F.col("partition_month") <= min_value)

    if check_empty_dfs([source_mao_mao_df, source_rev_usg_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    df_rev_usg_cols = ['month_id', 'register_date', 'access_method_num', 'total_voice_net_tariff_rev_mth_af']
    df_mao_mao_cols = ['month_id', 'register_date', 'access_method_num', 'total_amount_mao_mao_voice',
                       'c360_subscription_identifier']
    join_vat_key = ['vat_id']
    join_key = ['month_id', 'register_date', 'access_method_num']
    # spark = SparkSession.builder.getOrCreate()
    source_mao_mao_df = source_mao_mao_df.select(df_mao_mao_cols)
    source_mao_mao_df = source_mao_mao_df.withColumnRenamed("c360_subscription_identifier", "subscription_identifier")
    source_rev_usg_df = source_rev_usg_df.select(df_rev_usg_cols)
    final_df = source_mao_mao_df.join(source_rev_usg_df, join_key)
    final_df = final_df.drop(source_rev_usg_df["month_id"])
    final_df = final_df.drop(source_rev_usg_df["register_date"])
    final_df = final_df.drop(source_rev_usg_df["access_method_num"])
    # df_usg = df_usg_stg.withColumn("vat_id", k.lit("1"))
    # vat = spark.createDataFrame([(1.07, "Pre-paid"), (1.0, "Post-paid")], ["vat", "service"])
    # vat = vat.withColumn("vat_id", k.lit("1")).filter(F.col("service") == "Pre-paid")
    # final_df = df_usg.join(vat, join_vat_key)
    return final_df


def l3_rename_sub_id_to_subscription_identifier(source_df: DataFrame, config):
    if check_empty_dfs([source_df]):
        return get_spark_empty_df()
    final_df = source_df.withColumnRenamed("sub_id", "subscription_identifier")
    return final_df
