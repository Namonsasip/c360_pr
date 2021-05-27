import pyspark.sql.functions as f, logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , add_event_week_and_month_from_yyyymmdd, union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df


def build_digital_l1_daily_features(cxense_site_traffic: DataFrame,
                                    cust_df: DataFrame,
                                    exception_partition_list_for_l0_digital_cxenxse_site_traffic: dict,
                                    daily_dict: dict,
                                    popular_url_dict: dict,
                                    popular_postal_code_dict: dict,
                                    popular_referrer_query_dict: dict,
                                    popular_referrer_host_dict: dict,
                                    ) -> [DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    :param cxense_site_traffic:
    :param cust_df:
    :param exception_partition_list_for_l0_digital_cxenxse_site_traffic:
    :param daily_dict:
    :param popular_url_dict:
    :param popular_postal_code_dict:
    :param popular_referrer_query_dict:
    :param popular_referrer_host_dict:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cxense_site_traffic, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()
            , get_spark_empty_df()]

    cxense_site_traffic = data_non_availability_and_missing_check(
        df=cxense_site_traffic, grouping="daily",
        par_col="partition_date",
        target_table_name="l1_digital_cxenxse_site_traffic_daily",
        exception_partitions=exception_partition_list_for_l0_digital_cxenxse_site_traffic)

    cxense_site_traffic = add_event_week_and_month_from_yyyymmdd(cxense_site_traffic, column='partition_date')

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_digital_cxenxse_site_traffic_daily")

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            cxense_site_traffic.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
            cust_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    cxense_site_traffic = cxense_site_traffic.filter(f.col("event_partition_date") <= min_value)
    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    if check_empty_dfs([cxense_site_traffic, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()
            , get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    cust_df_cols = ['access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month',
                    'subscription_identifier']
    join_cols = ['access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month']

    cxense_site_traffic = cxense_site_traffic \
        .withColumnRenamed("mobile_no", "access_method_num") \
        .withColumn("digital_is_explorer", f.lit(1)) \
        .withColumn("digital_is_serenade", f.when(f.col("url").contains("serenade"), f.lit(1)).otherwise(f.lit(0)))

    cust_df = cust_df.where("charge_type IN ('Pre-paid', 'Post-paid')").select(cust_df_cols)

    cxense_site_traffic = cxense_site_traffic.join(cust_df, join_cols, how="left")

    daily_features = node_from_config(cxense_site_traffic, daily_dict)
    popular_url = node_from_config(cxense_site_traffic, popular_url_dict)
    popular_postal_code = node_from_config(cxense_site_traffic, popular_postal_code_dict)
    popular_referrer_query = node_from_config(cxense_site_traffic, popular_referrer_query_dict)
    popular_referrer_host = node_from_config(cxense_site_traffic, popular_referrer_host_dict)

    return [daily_features, popular_url, popular_postal_code, popular_referrer_query, popular_referrer_host]

    ############################### Mobile_app_daily ##############################


def digital_mobile_app_category_agg_daily(mobile_app_daily: DataFrame, mobile_app_daily_sql: dict):
    ##check missing data##
    if check_empty_dfs([mobile_app_daily]):
        return get_spark_empty_df()

    # where this column more than 0
    mobile_app_daily = mobile_app_daily.where(f.col("count_trans") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("duration") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("total_byte") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("download_byte") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("upload_byte") > 0)

    mobile_app_daily = mobile_app_daily.withColumnRenamed('category_level_1', 'category_name')
    mobile_app_daily = mobile_app_daily.withColumn("priority", f.lit(None).cast(StringType()))
    mobile_app_daily = mobile_app_daily.withColumnRenamed('partition_date', 'event_partition_date')

    df_return = node_from_config(mobile_app_daily, mobile_app_daily_sql)
    return df_return

    ############################### Mobile_app_timeband ##############################

# def digital_mobile_app_category_agg_timeband(Mobile_app_timeband: DataFrame, mobile_app_timeband_sql: dict):
#     ##check missing data##
#     if check_empty_dfs([mobile_app_daily]):
#         return get_spark_empty_df()
#
#     # where this column more than 0
#     Mobile_app_timeband = Mobile_app_timeband.where(f.col("count_trans") > 0)
#     Mobile_app_timeband = Mobile_app_timeband.where(f.col("duration") > 0)
#     Mobile_app_timeband = Mobile_app_timeband.where(f.col("total_byte") > 0)
#     Mobile_app_timeband = Mobile_app_timeband.where(f.col("download_byte") > 0)
#     Mobile_app_timeband = Mobile_app_timeband.where(f.col("upload_byte") > 0)
#
#     Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('category_level_1', 'category_name')
#     Mobile_app_timeband = Mobile_app_timeband.withColumn("priority", f.lit(None).cast(StringType()))
#     Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('partition_date', 'event_partition_date')
#
#     df_return = node_from_config(Mobile_app_timeband, mobile_app_timeband_sql)
#     return df_return

    ############################### category_daily ##############################


def build_l1_digital_iab_category_table(aib_raw: DataFrame, aib_priority_mapping: DataFrame):

    if check_empty_dfs([aib_raw]):
        return get_spark_empty_df()

    aib_clean = (
        aib_raw.withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
            .filter(f.col("argument").isNotNull())
            .filter(f.col("argument") != "")
    ).drop_duplicates()

    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_1"
    ).withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=["level_1"], how="inner"
    ).withColumnRenamed("level_1", "category_name").drop("level_1", "level_2", "level_3", "level_4")

    return iab_category_table

def build_l1_digital_iab_category_table_catlv_2(
        aib_raw: DataFrame, aib_priority_mapping: DataFrame
) -> DataFrame:
    aib_clean = (
        aib_raw.withColumn("level_2", f.trim(f.lower(f.col("level_2"))))
            .filter(f.col("argument").isNotNull())
            .filter(f.col("argument") != "")
    ).drop_duplicates()

    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_2"
    ).withColumn("level_2", f.trim(f.lower(f.col("level_2"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=["level_2"], how="inner"
    ).withColumnRenamed("level_2", "category_name").drop("level_1", "level_2", "level_3", "level_4")

    return iab_category_table

def build_l1_digital_iab_category_table_catlv_3(
        aib_raw: DataFrame, aib_priority_mapping: DataFrame
) -> DataFrame:
    aib_clean = (
        aib_raw.withColumn("level_3", f.trim(f.lower(f.col("level_3"))))
            .filter(f.col("argument").isNotNull())
            .filter(f.col("argument") != "")
    ).drop_duplicates()

    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_3"
    ).withColumn("level_3", f.trim(f.lower(f.col("level_3"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=["level_3"], how="inner"
    ).withColumnRenamed("level_3", "category_name").drop("level_1", "level_2", "level_3", "level_4")

    return iab_category_table

def build_l1_digital_iab_category_table_catlv_4(
        aib_raw: DataFrame, aib_priority_mapping: DataFrame
) -> DataFrame:
    #### Clear Dup and clear condition ########
    aib_clean = (
        aib_raw.withColumn("level_4", f.trim(f.lower(f.col("level_4"))))
            .filter(f.col("argument").isNotNull())
            .filter(f.col("argument") != "")
    ).drop_duplicates()

    #### Join Category level #######
    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_4"
    ).withColumn("level_4", f.trim(f.lower(f.col("level_4"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=["level_4"], how="inner"
    ).withColumnRenamed("level_4", "category_name").drop("level_1", "level_2", "level_3", "level_4")

    return iab_category_table

def l1_digital_mobile_web_category_agg_daily(mobile_web_daily_raw: DataFrame, aib_categories_clean: DataFrame) -> DataFrame:
    ##check missing data##
    if check_empty_dfs([mobile_web_daily_raw]):
        return get_spark_empty_df()

    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_type"))) == "url")
    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")

    df_mobile_web_daily = mobile_web_daily_raw.join(
        f.broadcast(aib_categories_clean)
        , on=[aib_categories_clean.argument == mobile_web_daily_raw.domain]
        , how="inner",
    ).select("mobile_no", "subscription_identifier", "category_name", "priority", "download_kb", "duration")

    df_mobile_web_daily_category_agg = df_mobile_web_daily.groupBy("mobile_no", "subscription_identifier",
                                                                   "category_name", "priority").agg(
        f.sum("duration").alias("total_visit_duration"),
        f.sum("download_kb").alias("total_traffic_byte"),
        f.count("*").alias("total_visit_counts"), )

    return df_mobile_web_daily_category_agg
