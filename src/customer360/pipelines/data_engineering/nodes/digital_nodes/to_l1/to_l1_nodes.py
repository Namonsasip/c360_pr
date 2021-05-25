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

def digital_mobile_app_category_agg_daily(mobile_app_daily: DataFrame,mobile_app_daily_sql: dict):

    ##check missing data##
    if check_empty_dfs([mobile_app_daily]):
        return get_spark_empty_df()

    #where this column more than 0
    mobile_app_daily = mobile_app_daily.where(f.col("count_trans") > 1)
    mobile_app_daily = mobile_app_daily.where(f.col("duration") > 1)
    mobile_app_daily = mobile_app_daily.where(f.col("total_byte") > 1)
    mobile_app_daily = mobile_app_daily.where(f.col("download_byte") > 1)
    mobile_app_daily = mobile_app_daily.where(f.col("upload_byte") > 1)
    
    mobile_app_daily = mobile_app_daily.withColumnRenamed('category_level_1', 'category_name')
    
    # mobile_app_daily.show(10)
    df_return = node_from_config(mobile_app_daily, mobile_app_daily_sql)
    df_return = df_return.withColumnRenamed('partition_date', 'even_partition_date')
    df_return = df_return.withColumn('priority', lit(None).cast(StringType()))
    df_return.show(10)
    return df_return

    ############################### category_daily ##############################
def build_l1_digital_iab_category_table(
    aib_raw: DataFrame, aib_priority_mapping: DataFrame
) -> DataFrame:

    aib_clean = (
        aib_raw.withColumn("level_2", f.trim(f.lower(f.col("level_2"))))
        .filter(f.col("argument").isNotNull())
        .filter(f.col("argument") != "")
    ).drop_duplicates()
    total_rows_in_aib = aib_clean.count()
    unique_rows_in_aib = aib_clean.dropDuplicates(["argument"]).count()
    if total_rows_in_aib != unique_rows_in_aib:
        raise Exception(
            "IAB has duplicates!!! Please make sure to have unique rows at argument level."
        )

    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_2"
    ).withColumn("level_2", f.trim(f.lower(f.col("level_2"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=["level_2"], how="inner"
    ).withColumnRenamed("level_2", "category_name").drop("level_2")

    return iab_category_table