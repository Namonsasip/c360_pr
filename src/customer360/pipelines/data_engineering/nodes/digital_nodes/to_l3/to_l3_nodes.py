import pyspark.sql.functions as f ,logging
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
import pyspark as pyspark
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from typing import Dict, Any
from functools import reduce
def build_digital_l3_monthly_features(cxense_user_profile: DataFrame,
                                      cust_df: DataFrame,
                                      node_config_dict: dict,
                                      except_partition) -> DataFrame:
    """
    :param cxense_user_profile:
    :param cust_df:
    :param node_config_dict:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cxense_user_profile, cust_df]):
        return get_spark_empty_df()

    cxense_user_profile = data_non_availability_and_missing_check(
        df=cxense_user_profile, grouping="monthly",
        par_col="partition_month",
        target_table_name="l3_digital_cxenxse_user_profile_monthly",
        exception_partitions=except_partition)

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_digital_cxenxse_user_profile_monthly")

    # new section to handle data latency
    cxense_user_profile = cxense_user_profile \
        .withColumn("partition_month", f.col("partition_month").cast(StringType())) \
        .withColumn("start_of_month", f.to_date(f.date_trunc('month', f.to_date(f.col("partition_month"), 'yyyyMM'))))

    min_value = union_dataframes_with_missing_cols(
        [
            cxense_user_profile.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            cust_df.select(
                f.max(f.col("start_of_month")).alias("max_date"))
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    cxense_user_profile = cxense_user_profile.filter(f.col("start_of_month") <= min_value)
    cust_df = cust_df.filter(f.col("start_of_month") <= min_value)

    if check_empty_dfs([cxense_user_profile, cust_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    cxense_user_profile = cxense_user_profile.withColumnRenamed("mobile_no", "access_method_num") \
        .withColumn("device_type", f.when(f.col("groups") == "device-type", f.col("item")).otherwise(f.lit(None))) \
        .withColumn("device_brand", f.when(f.col("groups") == "device-brand", f.col("item")).otherwise(f.lit(None)))

    # This code will populate a subscriber id to the data set.
    join_key = ['access_method_num', 'start_of_month']
    cust_df = cust_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_month,access_method_num order by "
        "start_of_month desc, mobile_status_date desc)")) \
        .where("rn = 1")\
        .select("subscription_identifier", "access_method_num", "start_of_month")

    final_df = cust_df.join(cxense_user_profile, join_key)

    return_df = node_from_config(final_df, node_config_dict)

    return_df = return_df.where("subscription_identifier is not null and start_of_month is not null")

    return return_df

################## Web agg category monthly by category ###########################
def l3_digital_mobile_web_category_agg_monthly (mobile_web_daily_agg: DataFrame) -> DataFrame :

    if check_empty_dfs([mobile_web_daily_agg]):
        return get_spark_empty_df()

    df_mobile_web_monthly = mobile_web_daily_agg.withColumn("start_of_month", f.to_date(f.date_trunc('month', "event_partition_date")))
    df_mobile_web_monthly_category_agg = df_mobile_web_monthly.groupBy("subscription_identifier","mobile_no","category_name","priority" ,"start_of_month").agg(
        f.sum("total_visit_count").alias("total_visit_count"),
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_volume_byte").alias("total_volume_byte"),
        f.sum("total_download_byte").alias("total_download_byte"),
        f.sum("total_upload_byte").alias("total_upload_byte")
        )

    return df_mobile_web_monthly_category_agg

#=============== Web agg monthly by domain ================#
def digital_mobile_web_agg_monthly(web_category_agg_daily: pyspark.sql.DataFrame, aib_clean: pyspark.sql.DataFrame,web_sql: Dict[str, Any]):
    web_category_agg_daily = web_category_agg_daily.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 7), f.lit("-01")),).drop(*["partition_date"])

    if (web_category_agg_daily == "upload_kb"):
        web_category_agg_daily = web_category_agg_daily.withColumnRenamed("upload_kb", "upload_byte")
    elif (web_category_agg_daily == "download_kb"):
        web_category_agg_daily = web_category_agg_daily.withColumnRenamed("download_kb", "download_byte")
    else:
        web_category_agg_daily = web_category_agg_daily.withColumn("upload_kb", f.col("upload_kb").cast("decimal(35,4)")).withColumnRenamed("upload_kb", "upload_byte")
        web_category_agg_daily = web_category_agg_daily.withColumn("download_kb", f.col("download_kb").cast("decimal(35,4)")).withColumnRenamed("download_kb", "download_byte")
        web_category_agg_daily = web_category_agg_daily.withColumn("total_kb", f.col("total_kb").cast("decimal(35,4)")).withColumnRenamed("total_kb", "total_byte")

    web_category_agg_daily = web_category_agg_daily.where(f.col("upload_byte") > 0)
    web_category_agg_daily = web_category_agg_daily.where(f.col("download_byte") > 0)
    web_category_agg_daily = web_category_agg_daily.where(f.col("total_byte") > 0)
    web_category_agg_daily = web_category_agg_daily.where(f.col("duration") > 0)
    web_category_agg_daily = web_category_agg_daily.where(f.col("count_trans") > 0)

    web_category_agg_daily = web_category_agg_daily.join(f.broadcast(aib_clean), on=[aib_clean.argument == web_category_agg_daily.domain], how="inner")

    web_category_agg_daily = web_category_agg_daily.select("subscription_identifier",
                                                           "mobile_no",
                                                           "domain",
                                                           "category_name",
                                                           "level_2",
                                                           "level_3",
                                                           "level_4",
                                                           "priority",
                                                           "upload_byte",
                                                           "download_byte",
                                                           "duration",
                                                           "total_byte",
                                                           "count_trans",
                                                           "start_of_month")

    web_category_agg_daily = web_category_agg_daily.withColumnRenamed("category_name", "category_level_1")
    web_category_agg_daily = web_category_agg_daily.withColumnRenamed("level_2", "category_level_2")
    web_category_agg_daily = web_category_agg_daily.withColumnRenamed("level_3", "category_level_3")
    web_category_agg_daily = web_category_agg_daily.withColumnRenamed("level_4", "category_level_4")

    web_category_agg_daily = node_from_config(web_category_agg_daily, web_sql)

    return web_category_agg_daily

############## Web agg monthly Score by category ################
def l3_digital_mobile_web_category_score_monthly(web_category_fav_monthly: pyspark.sql.DataFrame,web_sql: Dict[str, Any], web_sql_sum: Dict[str, Any]):

    web_category_fav_monthly_transaction = web_category_fav_monthly.filter(web_category_fav_monthly["favorite_by"] == 'Transaction')
    web_category_fav_monthly_duration = web_category_fav_monthly.filter(web_category_fav_monthly["favorite_by"] == 'Duration')
    web_category_fav_monthly_volume = web_category_fav_monthly.filter(web_category_fav_monthly["favorite_by"] == 'Volume')

    web_category_fav_monthly_transaction = web_category_fav_monthly_transaction.withColumnRenamed("sharing_score", 'score_transaction')
    web_category_fav_monthly_duration = web_category_fav_monthly_duration.withColumnRenamed("sharing_score", 'score_duration')
    web_category_fav_monthly_volume = web_category_fav_monthly_volume.withColumnRenamed("sharing_score", 'score_volume')

    web_category_fav_monthly_transaction = web_category_fav_monthly_transaction.withColumn("score_duration", lit(0)).withColumn("score_volume", lit(0))
    web_category_fav_monthly_duration = web_category_fav_monthly_duration.withColumn("score_transaction", lit(0)).withColumn("score_volume", lit(0))
    web_category_fav_monthly_volume = web_category_fav_monthly_volume.withColumn("score_transaction", lit(0)).withColumn("score_duration", lit(0))

    web_category_fav_monthly_transaction = web_category_fav_monthly_transaction.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    web_category_fav_monthly_duration = web_category_fav_monthly_duration.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    web_category_fav_monthly_volume = web_category_fav_monthly_volume.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")

    df_return = web_category_fav_monthly_transaction.union(web_category_fav_monthly_duration)
    df_return = df_return.union(web_category_fav_monthly_volume)

    df_return = node_from_config(df_return, web_sql_sum)
    df_return = node_from_config(df_return, web_sql)

    return df_return

############## Web agg monthly timeband by category ################
def l3_digital_mobile_web_category_agg_timeband (mobile_web_daily_agg_timeband: pyspark.sql.DataFrame,
                                                 mobile_web_agg_monthly: pyspark.sql.DataFrame,
                                                 mobile_web_timeband_monthly_share_sql: Dict[str, Any]):

    if check_empty_dfs([mobile_web_daily_agg_timeband]):
        return get_spark_empty_df()
    if check_empty_dfs([mobile_web_agg_monthly]):
        return get_spark_empty_df()

    mobile_web_daily_agg_timeband = mobile_web_daily_agg_timeband.withColumn("start_of_month", f.to_date(f.date_trunc('month', "event_partition_date")))
    mobile_web_timeband_monthly = mobile_web_daily_agg_timeband.groupBy("subscription_identifier","mobile_no","category_name","priority"
                                                                       ,"start_of_month").agg(
        f.sum("total_visit_count").alias("total_visit_count"),
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_volume_byte").alias("total_volume_byte"),
        f.sum("total_download_byte").alias("total_download_byte"),
        f.sum("total_upload_byte").alias("total_upload_byte")
        )

    mobile_web_agg_monthly = mobile_web_agg_monthly.withColumnRenamed("total_visit_count", 'total_visit_count_monthly')
    mobile_web_agg_monthly = mobile_web_agg_monthly.withColumnRenamed("total_visit_duration", 'total_visit_duration_monthly')
    mobile_web_agg_monthly = mobile_web_agg_monthly.withColumnRenamed("total_volume_byte", 'total_volume_byte_monthly')
    mobile_web_agg_monthly = mobile_web_agg_monthly.withColumnRenamed("total_download_byte", 'total_download_byte_monthly')
    mobile_web_agg_monthly = mobile_web_agg_monthly.withColumnRenamed("total_upload_byte", 'total_upload_byte_monthly')
    mobile_web_agg_monthly = mobile_web_agg_monthly.withColumnRenamed("priority", 'priorityt_monthly')

    mobile_web_timeband_monthly = mobile_web_timeband_monthly.join(mobile_web_agg_monthly,
                                                   on=[mobile_web_timeband_monthly.mobile_no == mobile_web_agg_monthly.mobile_no,
                                                       mobile_web_timeband_monthly.category_name == mobile_web_agg_monthly.category_name],
                                                   how="inner",)

    mobile_web_timeband_monthly = mobile_web_timeband_monthly.select(mobile_web_agg_monthly["subscription_identifier"],
                                                     mobile_web_agg_monthly["mobile_no"],
                                                     mobile_web_agg_monthly["category_name"],
                                                     mobile_web_timeband_monthly["priority"],
                                                     "total_visit_count",
                                                     "total_visit_duration",
                                                     "total_volume_byte",
                                                     "total_download_byte",
                                                     "total_upload_byte",
                                                     "total_visit_count_monthly",
                                                     "total_visit_duration_monthly",
                                                     "total_volume_byte_monthly",
                                                     "total_download_byte_monthly",
                                                     "total_upload_byte_monthly",
                                                     mobile_web_agg_monthly["start_of_month"])

    df_return = node_from_config(mobile_web_timeband_monthly, mobile_web_timeband_monthly_share_sql)
    return df_return

############################## favorite_web_monthly #############################
def digital_mobile_web_category_favorite_monthly(web_category_agg_daily: pyspark.sql.DataFrame,
                                                 web_sql_total: Dict[str, Any],
                                                 web_sql_transaction: Dict[str, Any],
                                                 web_sql_duration: Dict[str, Any],
                                                 web_sql_volume: Dict[str, Any]):
        # ---------------  sum traffic ------------------
        web_category_agg_daily_sql_total = node_from_config(web_category_agg_daily, web_sql_total)

        web_category_agg_daily = web_category_agg_daily.alias("web_category_agg_daily").join(web_category_agg_daily_sql_total.alias("web_category_agg_daily_sql_total"), on=["subscription_identifier", "mobile_no","start_of_month"], how="inner",)

        web_category_agg_daily = web_category_agg_daily.select(
            "web_category_agg_daily.subscription_identifier",
            "web_category_agg_daily.mobile_no",
            "web_category_agg_daily.priority",
            "web_category_agg_daily.start_of_month",
            "web_category_agg_daily.category_name",
            "web_category_agg_daily.total_visit_count",
            "web_category_agg_daily.total_visit_duration",
            "web_category_agg_daily.total_volume_byte",
            "web_category_agg_daily_sql_total.sum_total_visit_count",
            "web_category_agg_daily_sql_total.sum_total_visit_duration",
            "web_category_agg_daily_sql_total.sum_total_volume_byte"
        )
        # ---------------  sum cal fav ------------------
        web_category_agg_daily_transaction = node_from_config(web_category_agg_daily, web_sql_transaction)
        web_category_agg_daily_duration = node_from_config(web_category_agg_daily, web_sql_duration)
        web_category_agg_daily_volume = node_from_config(web_category_agg_daily, web_sql_volume)

        # ---------------  union ------------------
        df_return = web_category_agg_daily_transaction.union(web_category_agg_daily_duration)
        df_return = df_return.union(web_category_agg_daily_volume)
        return df_return

#=============== Web agg monthly by domain Fav ================#
def digital_mobile_web_favorite_by_category_monthly(web_category_agg_monthly: pyspark.sql.DataFrame,
                                                        web_sql_total: Dict[str, Any], web_sql_transaction: Dict[str, Any],
                                                        web_sql_duration: Dict[str, Any], web_sql_volume: Dict[str, Any],category_level: Dict[str, Any]):
    # ---------------  sum traffic -------------- #

    web_category_agg_monthly = web_category_agg_monthly.withColumnRenamed(category_level, 'category_name')
    web_category_agg_monthly_sql_total = node_from_config(web_category_agg_monthly, web_sql_total)

    web_category_agg_monthly = web_category_agg_monthly.alias('web_category_agg_monthly').join(
        web_category_agg_monthly_sql_total.alias('web_category_agg_monthly_sql_total'),
        on=["subscription_identifier", "mobile_no" ,"start_of_month", "category_name"], how="inner")

    web_category_agg_monthly = web_category_agg_monthly.select(
        "web_category_agg_monthly.subscription_identifier",
        "web_category_agg_monthly.mobile_no",
        "web_category_agg_monthly.category_name",
        "web_category_agg_monthly.domain",
        "web_category_agg_monthly.start_of_month",
        "web_category_agg_monthly.total_visit_count",
        "web_category_agg_monthly.total_visit_duration",
        "web_category_agg_monthly.total_volume_byte",
        "web_category_agg_monthly_sql_total.sum_total_visit_count",
        "web_category_agg_monthly_sql_total.sum_total_visit_duration",
        "web_category_agg_monthly_sql_total.sum_total_volume_byte"
    )
    # ---------------  sum cal fav ------------------
    web_category_agg_monthly_transaction = node_from_config(web_category_agg_monthly, web_sql_transaction)
    web_category_agg_monthly_duration = node_from_config(web_category_agg_monthly, web_sql_duration)
    web_category_agg_monthly_volume = node_from_config(web_category_agg_monthly, web_sql_volume)

    # ---------------  union ------------------
    df_return = web_category_agg_monthly_transaction.union(web_category_agg_monthly_duration)
    df_return = df_return.union(web_category_agg_monthly_volume)
    return df_return

#============ Weg agg monthly by category Fav timeband ==================#
def l3_digital_mobile_web_category_favorite_monthly_timeband(web_category_agg_timeband: pyspark.sql.DataFrame,
                                                             sql_total: Dict[str, Any], sql_transaction: Dict[str, Any],
                                                             sql_duration: Dict[str, Any], sql_volume: Dict[str, Any]):
    # ---------------  sum traffic ------------------
    web_category_agg_timeband_sql_total = node_from_config(web_category_agg_timeband, sql_total)

    web_category_agg_timeband = web_category_agg_timeband.alias('web_category_agg_timeband').join(
        web_category_agg_timeband_sql_total.alias('web_category_agg_timeband_sql_total'),
        on=["subscription_identifier", "mobile_no","start_of_month" ], how="inner", )

    web_category_agg_timeband = web_category_agg_timeband.select(
        "web_category_agg_timeband.subscription_identifier",
        "web_category_agg_timeband.mobile_no",
        "web_category_agg_timeband.category_name",
        "web_category_agg_timeband.priority",
        "web_category_agg_timeband.start_of_month",
        "web_category_agg_timeband.total_visit_count",
        "web_category_agg_timeband.total_visit_duration",
        "web_category_agg_timeband.total_volume_byte",
        "web_category_agg_timeband_sql_total.sum_total_visit_count",
        "web_category_agg_timeband_sql_total.sum_total_visit_duration",
        "web_category_agg_timeband_sql_total.sum_total_volume_byte"
    )
    # ---------------  sum cal fav ------------------
    pp_category_agg_timeband_transection = node_from_config(web_category_agg_timeband, sql_transaction)
    pp_category_agg_timeband_duration = node_from_config(web_category_agg_timeband, sql_duration)
    pp_category_agg_timeband_volume = node_from_config(web_category_agg_timeband, sql_volume)

    # ---------------  union ------------------
    df_return = pp_category_agg_timeband_transection.union(pp_category_agg_timeband_duration)
    df_return = df_return.union(pp_category_agg_timeband_volume)
    return df_return

############################## Web agg monthly by category Fav #############################
# def digital_mobile_web_category_favorite_monthly(web_category_agg_daily: pyspark.sql.DataFrame,
#                                                 web_sql_total: Dict[str, Any],
#                                                 web_sql_transaction: Dict[str, Any],
#                                                 web_sql_duration: Dict[str, Any],
#                                                 web_sql_volume: Dict[str, Any]):
#         # ---------------  sum traffic ------------------
#     web_category_agg_daily_sql_total = node_from_config(web_category_agg_daily, web_sql_total)
#
#     web_category_agg_daily = web_category_agg_daily.alias("web_category_agg_daily").join(
#         web_category_agg_daily_sql_total.alias("web_category_agg_daily_sql_total"),
#         on=["subscription_identifier", "mobile_no", "start_of_month"], how="inner", )
#
#     web_category_agg_daily = web_category_agg_daily.select(
#         "web_category_agg_daily.subscription_identifier",
#         "web_category_agg_daily.mobile_no"
#         "web_category_agg_daily.priority",
#         "web_category_agg_daily.start_of_month",
#         "web_category_agg_daily.category_name",
#         "web_category_agg_daily.total_visit_count",
#         "web_category_agg_daily.total_visit_duration",
#         "web_category_agg_daily.total_volume_byte",
#         "web_category_agg_daily_sql_total.sum_total_visit_count",
#         "web_category_agg_daily_sql_total.sum_total_visit_duration",
#         "web_category_agg_daily_sql_total.sum_total_volume_byte"
#     )
#
#     # ---------------  sum cal fav ------------------
#     web_category_agg_daily_transaction = node_from_config(web_category_agg_daily, web_sql_transaction)
#     web_category_agg_daily_duration = node_from_config(web_category_agg_daily, web_sql_duration)
#     web_category_agg_daily_volume = node_from_config(web_category_agg_daily, web_sql_volume)
#
#     # ---------------  union ------------------
#     df_return = web_category_agg_daily_transaction.union(web_category_agg_daily_duration)
#     df_return = df_return.union(web_category_agg_daily_volume)
#     return df_return

#============ App agg monthly timeband ==================#

def digital_customer_app_category_agg_timeband_monthly(customer_app_agg_timeband: pyspark.sql.DataFrame,
                                                       customer_app_agg: pyspark.sql.DataFrame,
                                                       customer_app_timeband_monthly_share_sql: Dict[str, Any]):
    if check_empty_dfs([customer_app_agg_timeband]):
        return get_spark_empty_df()
    if check_empty_dfs([customer_app_agg]):
        return get_spark_empty_df()


    customer_app_agg_timeband = customer_app_agg_timeband.withColumn("start_of_month", f.to_date(f.date_trunc('month', "event_partition_date")))
    customer_app_agg_timeband_monthly = customer_app_agg_timeband.groupBy("subscription_identifier", "mobile_no",
                                                                        "category_name", "priority"
                                                                        ,"start_of_month").agg(
        f.sum("total_visit_count").alias("total_visit_count"),
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_volume_byte").alias("total_volume_byte"),
        f.sum("total_download_byte").alias("total_download_byte"),
        f.sum("total_upload_byte").alias("total_upload_byte")
    )

    customer_app_agg = customer_app_agg.withColumnRenamed("total_visit_count", 'total_visit_count_monthly')
    customer_app_agg = customer_app_agg.withColumnRenamed("total_visit_duration",'total_visit_duration_monthly')
    customer_app_agg = customer_app_agg.withColumnRenamed("total_volume_byte", 'total_volume_byte_monthly')
    customer_app_agg = customer_app_agg.withColumnRenamed("total_download_byte",'total_download_byte_monthly')
    customer_app_agg = customer_app_agg.withColumnRenamed("total_upload_byte", 'total_upload_byte_monthly')
    customer_app_agg = customer_app_agg.withColumnRenamed("priority", 'priorityt_monthly')

    customer_app_agg_timeband_monthly = customer_app_agg_timeband_monthly.join(customer_app_agg,
                                                                   on=[
                                                                       customer_app_agg_timeband_monthly.subscription_identifier == customer_app_agg.subscription_identifier,
                                                                       customer_app_agg_timeband_monthly.category_name == customer_app_agg.category_name,
                                                                       customer_app_agg_timeband_monthly.start_of_month == customer_app_agg.start_of_month
                                                                   ],how="inner",)

    customer_app_agg_timeband_monthly = customer_app_agg_timeband_monthly.select(customer_app_agg["subscription_identifier"],
                                                                     customer_app_agg["mobile_no"],
                                                                     customer_app_agg["category_name"],
                                                                     customer_app_agg_timeband_monthly["priority"],
                                                                     "total_visit_count",
                                                                     "total_visit_duration",
                                                                     "total_volume_byte",
                                                                     "total_download_byte",
                                                                     "total_upload_byte",
                                                                     "total_visit_count_monthly",
                                                                     "total_visit_duration_monthly",
                                                                     "total_volume_byte_monthly",
                                                                     "total_download_byte_monthly",
                                                                     "total_upload_byte_monthly",
                                                                     customer_app_agg["start_of_month"])

    df_return = node_from_config(customer_app_agg_timeband_monthly, customer_app_timeband_monthly_share_sql)
    return df_return

def relay_drop_nulls(df_relay: pyspark.sql.DataFrame):
    df_relay_cleaned = df_relay.filter(
        (f.col("mobile_no").isNotNull())
        & (f.col("mobile_no") != "")
        & (f.col("subscription_identifier") != "")
        & (f.col("subscription_identifier").isNotNull())
    )
    return df_relay_cleaned

def join_all(dfs, on, how="inner"):
    """
    Merge all the dataframes
    """
    return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)

def digital_customer_relay_conversion_agg_monthly(
    df_conversion: pyspark.sql.DataFrame,df_conversion_package: pyspark.sql.DataFrame,conversion_count_visit_by_cid: Dict[str, Any],conversion_package_count_visit_by_cid: Dict[str, Any],
):
    if check_empty_dfs([df_conversion]):
        return get_spark_empty_df()
    if check_empty_dfs([df_conversion_package]):
        return get_spark_empty_df()

    df_engagement_conversion_clean = relay_drop_nulls(df_conversion)
    df_engagement_conversion = df_engagement_conversion_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != "") & (f.col("R42paymentStatus") == "successful"))
    df_engagement_conversion = df_engagement_conversion.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion = df_engagement_conversion.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_conversion_package_clean = relay_drop_nulls(df_conversion_package)
    df_engagement_conversion_package = df_engagement_conversion_package_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != "") & (f.col("R42Product_status") == "successful"))
    df_engagement_conversion_package = df_engagement_conversion_package.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion_package = df_engagement_conversion_package.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_conversion_visits = node_from_config(df_engagement_conversion, conversion_count_visit_by_cid)
    df_engagement_conversion_package_visits = node_from_config(df_engagement_conversion_package, conversion_package_count_visit_by_cid)

    # df_engagement_conversion_visits.createOrReplaceTempView("df_engagement_conversion_visits")
    # df_engagement_conversion_package_visits.createOrReplaceTempView("df_engagement_conversion_package_visits")
    #
    # spark = get_spark_session()
    # df_conversion_and_package_visits = spark.sql("""
    # select
    # COALESCE(a.subscription_identifier,b.subscription_identifier) as subscription_identifier,
    # COALESCE(a.mobile_no,b.mobile_no) as mobile_no,
    # COALESCE(a.campaign_id,b.campaign_id) as campaign_id,
    # a.total_conversion_product_count as total_conversion_product_count,
    # b.total_conversion_package_count as total_conversion_package_count,
    # COALESCE(a.start_of_month,b.start_of_month) as start_of_month
    # from df_engagement_conversion_visits a
    # FULL JOIN df_engagement_conversion_package_visits b
    # ON a.subscription_identifier = b.subscription_identifier
    # and a.mobile_no = b.mobile_no
    # and a.campaign_id = b.campaign_id
    # and a.start_of_month = b.start_of_month
    # """)
    df_conversion_and_package_visits = join_all(
    [
        df_engagement_conversion_visits,
        df_engagement_conversion_package_visits
    ],
    on=["subscription_identifier", "start_of_month", "mobile_no","campaign_id"],
    how="outer",
    )
    return df_conversion_and_package_visits

def digital_customer_relay_pageview_fav_monthly(
    df_pageviews: pyspark.sql.DataFrame,
    df_productinfo: pyspark.sql.DataFrame,
    count_visit: Dict[str, Any],
    popular_url: Dict[str, Any],
    popular_subcategory1: Dict[str, Any],
    popular_subcategory2: Dict[str, Any],
    popular_cid: Dict[str, Any],
    popular_productname: Dict[str, Any],
    most_popular_url: Dict[str, Any],
    most_popular_subcategory1: Dict[str, Any],
    most_popular_subcategory2: Dict[str, Any],
    most_popular_cid: Dict[str, Any],
    most_popular_productname: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_pageviews]):
        return get_spark_empty_df()
    if check_empty_dfs([df_productinfo]):
        return get_spark_empty_df()
    df_engagement_pageview_clean = relay_drop_nulls(df_pageviews)
    df_engagement_pageview = df_engagement_pageview_clean.withColumnRenamed("cid", "campaign_id")
    df_engagement_pageview = df_engagement_pageview.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    df_engagement_pageview_visits = node_from_config(df_engagement_pageview, count_visit)

    df_engagement_productinfo_clean = relay_drop_nulls(df_productinfo)
    df_engagement_productinfo = df_engagement_productinfo_clean.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    # most_popular_subcategory1
    df_pageviews_subcat1 = df_engagement_pageview.filter((f.col("subCategory1").isNotNull()) & (f.col("subCategory1") != ""))
    popular_subcategory1_df = node_from_config(df_pageviews_subcat1, popular_subcategory1)

    df_most_popular_subcategory1 = node_from_config(popular_subcategory1_df, most_popular_subcategory1)

    # most_popular_subcategory2
    df_pageviews_subcat2 = df_engagement_pageview.filter((f.col("subCategory2").isNotNull()) & (f.col("subCategory2") != ""))
    popular_subcategory2_df = node_from_config(df_pageviews_subcat2, popular_subcategory2)

    df_most_popular_subcategory2 = node_from_config(popular_subcategory2_df, most_popular_subcategory2)

    # most_popular_url
    df_pageviews_url = df_engagement_pageview.filter((f.col("url").isNotNull()) & (f.col("url") != ""))
    popular_url_df = node_from_config(df_pageviews_url, popular_url)

    df_most_popular_url = node_from_config(popular_url_df, most_popular_url)

    # most_popular_cid
    df_pageviews_cid = df_engagement_pageview.filter((f.col("campaign_id").isNotNull()) & (f.col("campaign_id") != ""))
    df_popular_cid = node_from_config(df_pageviews_cid, popular_cid)

    df_most_popular_cid = node_from_config(df_popular_cid, most_popular_cid)

    # most_popular_productname
    df_engagement_productinfo = df_engagement_productinfo.filter((f.col("R42productName").isNotNull()) & (f.col("R42productName") != ""))
    popular_productname_df = node_from_config(df_engagement_productinfo, popular_productname)

    df_most_popular_productname = node_from_config(popular_productname_df, most_popular_productname)

    pageviews_monthly_features = join_all(
        [
            df_engagement_pageview_visits,
            df_most_popular_subcategory1,
            df_most_popular_subcategory2,
            df_most_popular_url,
            df_most_popular_productname,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "start_of_month", "mobile_no"],
        how="outer",
    )
    return pageviews_monthly_features

def digital_customer_relay_conversion_fav_monthly(
        df_conversion: pyspark.sql.DataFrame,
        popular_product: Dict[str, Any],
        popular_cid: Dict[str, Any],
        most_popular_product: Dict[str, Any],
        most_popular_cid: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_conversion]):
        return get_spark_empty_df()
    df_engagement_conversion_clean = relay_drop_nulls(df_conversion)
    df_engagement_conversion_clean = df_engagement_conversion_clean.filter(
        f.col("R42paymentStatus") == "successful")
    df_engagement_conversion = df_engagement_conversion_clean.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion = df_engagement_conversion.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    # favourite product
    df_engagement_conversion_product = df_engagement_conversion.withColumn(
        "R42productLists", f.split("R42productLists", ",")
    ).withColumn("product", f.explode("R42productLists"))
    df_engagement_conversion_product_clean = df_engagement_conversion_product.filter(
        (f.col("product").isNotNull()) & (f.col("product") != ""))
    df_popular_product = node_from_config(df_engagement_conversion_product_clean, popular_product)
    df_most_popular_product = node_from_config(df_popular_product, most_popular_product)

    # favourite cid
    df_conversion_cid = df_engagement_conversion.filter(
        (f.col("campaign_id").isNotNull()) & (f.col("campaign_id") != ""))
    df_popular_cid = node_from_config(df_conversion_cid, popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, most_popular_cid)

    engagement_conversion_monthly_features = join_all(
        [
            df_most_popular_product,
            df_most_popular_cid
        ],
        on=["subscription_identifier", "start_of_month", "mobile_no"],
        how="outer",
    )

    return engagement_conversion_monthly_features

def digital_customer_relay_conversion_package_fav_monthly(
        df_conversion_package: pyspark.sql.DataFrame,
        popular_product: Dict[str, Any],
        popular_cid: Dict[str, Any],
        most_popular_product: Dict[str, Any],
        most_popular_cid: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_conversion_package]):
        return get_spark_empty_df()
    df_engagement_conversion_package_clean = relay_drop_nulls(df_conversion_package)
    df_engagement_conversion_package_clean = df_engagement_conversion_package_clean.filter(
        f.col("R42Product_status") == "successful")
    df_engagement_conversion_package = df_engagement_conversion_package_clean.withColumnRenamed("cid",
                                                                                                "campaign_id")
    df_engagement_conversion_package = df_engagement_conversion_package.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    # favourite product
    df_engagement_conversion_package_product = df_engagement_conversion_package.withColumnRenamed("R42Product_name",
                                                                                                  "product")

    df_engagement_conversion_package_product_clean = df_engagement_conversion_package_product.filter(
        (f.col("product").isNotNull()) & (f.col("product") != ""))
    df_popular_product = node_from_config(
        df_engagement_conversion_package_product_clean, popular_product
    )
    df_most_popular_product = node_from_config(
        df_popular_product, most_popular_product
    )

    # favourite cid
    df_engagement_cid = df_engagement_conversion_package.filter(
        (f.col("campaign_id").isNotNull()) & (f.col("campaign_id") != ""))
    df_popular_cid = node_from_config(df_engagement_cid, popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, most_popular_cid)

    engagement_conversion_package_monthly_features = join_all(
        [
            df_most_popular_product,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "start_of_month", "mobile_no"],
        how="outer",
    )

    return engagement_conversion_package_monthly_features


    ################################# mobile_app_monthly ###############################
def digital_mobile_app_category_agg_monthly(app_category_agg_daily: pyspark.sql.DataFrame,sql: Dict[str, Any]):
    app_category_agg_daily = app_category_agg_daily.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("event_partition_date").cast("string"), 1, 7), f.lit("-01")
        ),
    ).drop(*["event_partition_date"])
    app_category_agg_daily = node_from_config(app_category_agg_daily,sql)
    return app_category_agg_daily
    
    #-------------------------------- mobile_app_agg_daily ------------------------------

def digital_mobile_app_agg_monthly(app_category_agg_daily: pyspark.sql.DataFrame,sql: Dict[str, Any]):
    app_category_agg_daily = app_category_agg_daily.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 7), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    app_category_agg_daily = app_category_agg_daily.withColumnRenamed('upload_kb', 'upload_byte')
    app_category_agg_daily = app_category_agg_daily.withColumnRenamed('download_kb', 'download_byte')
    app_category_agg_daily = app_category_agg_daily.withColumnRenamed('total_kb', 'total_byte')

    app_category_agg_daily = app_category_agg_daily.where(f.col("upload_byte") > 0)
    app_category_agg_daily = app_category_agg_daily.where(f.col("download_byte") > 0)
    app_category_agg_daily = app_category_agg_daily.where(f.col("total_byte") > 0)
    app_category_agg_daily = app_category_agg_daily.where(f.col("duration") > 0)
    app_category_agg_daily = app_category_agg_daily.where(f.col("count_trans") > 0)

    app_category_agg_daily = node_from_config(app_category_agg_daily,sql)
    return app_category_agg_daily

############################## favorite_app_monthly #############################
def digital_mobile_app_category_favorite_monthly(app_category_agg_daily: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_transection: Dict[str, Any],sql_duration: Dict[str, Any],sql_volume: Dict[str, Any]):
    #---------------  sum traffic ------------------
    logging.info("favorite ------- > sum traffic")
    app_category_agg_daily_sql_total = node_from_config(app_category_agg_daily, sql_total)

    app_category_agg_daily = app_category_agg_daily.alias('app_category_agg_daily').join(app_category_agg_daily_sql_total.alias('app_category_agg_daily_sql_total'),on=["subscription_identifier","mobile_no","start_of_month",],how="inner",)
    
    app_category_agg_daily = app_category_agg_daily.select(
        "app_category_agg_daily.subscription_identifier",
        "app_category_agg_daily.mobile_no",
        "app_category_agg_daily.category_name",
        "app_category_agg_daily.priority",
        "app_category_agg_daily.start_of_month",
        "app_category_agg_daily.total_visit_count",
        "app_category_agg_daily.total_visit_duration",
        "app_category_agg_daily.total_volume_byte",
        "app_category_agg_daily_sql_total.sum_total_visit_count",
        "app_category_agg_daily_sql_total.sum_total_visit_duration",
        "app_category_agg_daily_sql_total.sum_total_volume_byte"
        )
    #---------------  sum cal fav ------------------
    logging.info("favorite ------- > cal")
    app_category_agg_daily_transection = node_from_config(app_category_agg_daily,sql_transection)
    logging.info("favorite ------- > transection complete")
    app_category_agg_daily_duration = node_from_config(app_category_agg_daily,sql_duration)
    logging.info("favorite ------- > duration complete")
    app_category_agg_daily_volume = node_from_config(app_category_agg_daily,sql_volume)
    logging.info("favorite ------- > volume complete")
    #---------------  union ------------------
    logging.info("favorite ------- > union")
    df_return = app_category_agg_daily_transection.union(app_category_agg_daily_duration)
    df_return = df_return.union(app_category_agg_daily_volume)

    return df_return

    ############################## favorite_by_category_app_monthly #############################
def digital_mobile_app_favorite_by_category_monthly(app_category_agg_monthly: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_transection: Dict[str, Any],sql_duration: Dict[str, Any],sql_volume: Dict[str, Any],category_level: dict):
    #---------------  sum traffic ------------------
    logging.info("favorite ------- > sum traffic")

    app_category_agg_monthly = app_category_agg_monthly.withColumnRenamed(category_level, 'category_name')
    app_category_agg_monthly_sql_total = node_from_config(app_category_agg_monthly, sql_total)

    app_category_agg_monthly = app_category_agg_monthly.alias('app_category_agg_monthly').join(app_category_agg_monthly_sql_total.alias('app_category_agg_monthly_sql_total'),on=["subscription_identifier","mobile_no","start_of_month","category_name"],how="inner",)
    
    app_category_agg_monthly = app_category_agg_monthly.select(
        "app_category_agg_monthly.subscription_identifier",
        "app_category_agg_monthly.mobile_no",
        "app_category_agg_monthly.category_name",
        "app_category_agg_monthly.application",
        # "app_category_agg_monthly.priority",
        "app_category_agg_monthly.start_of_month",
        "app_category_agg_monthly.total_visit_count",
        "app_category_agg_monthly.total_visit_duration",
        "app_category_agg_monthly.total_volume_byte",
        "app_category_agg_monthly_sql_total.sum_total_visit_count",
        "app_category_agg_monthly_sql_total.sum_total_visit_duration",
        "app_category_agg_monthly_sql_total.sum_total_volume_byte"
        )
    #---------------  sum cal fav ------------------
    logging.info("favorite ------- > cal")
    pp_category_agg_monthly_transection = node_from_config(app_category_agg_monthly,sql_transection)
    logging.info("favorite ------- > transection complete")
    pp_category_agg_monthly_duration = node_from_config(app_category_agg_monthly,sql_duration)
    logging.info("favorite ------- > duration complete")
    pp_category_agg_monthly_volume = node_from_config(app_category_agg_monthly,sql_volume)
    logging.info("favorite ------- > volume complete")
    #---------------  union ------------------
    logging.info("favorite ------- > union")
    df_return = pp_category_agg_monthly_transection.union(pp_category_agg_monthly_duration)
    df_return = df_return.union(pp_category_agg_monthly_volume)
    return df_return

    ############################## score_app_monthly#############################
def l3_digital_mobile_app_category_score_monthly(app_category_fav_monthly: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_sum: Dict[str, Any]):

    app_category_fav_monthly_transaction = app_category_fav_monthly.filter(app_category_fav_monthly["favorite_by"] == 'Transaction')
    app_category_fav_monthly_duration = app_category_fav_monthly.filter(app_category_fav_monthly["favorite_by"] == 'Duration')
    app_category_fav_monthly_volume = app_category_fav_monthly.filter(app_category_fav_monthly["favorite_by"] == 'Volume')

    app_category_fav_monthly_transaction = app_category_fav_monthly_transaction.withColumnRenamed("sharing_score", 'score_transaction')
    app_category_fav_monthly_duration = app_category_fav_monthly_duration.withColumnRenamed("sharing_score", 'score_duration')
    app_category_fav_monthly_volume = app_category_fav_monthly_volume.withColumnRenamed("sharing_score", 'score_volume')

    app_category_fav_monthly_transaction = app_category_fav_monthly_transaction.withColumn("score_duration", lit(0)).withColumn("score_volume", lit(0))
    app_category_fav_monthly_duration = app_category_fav_monthly_duration.withColumn("score_transaction", lit(0)).withColumn("score_volume", lit(0))
    app_category_fav_monthly_volume = app_category_fav_monthly_volume.withColumn("score_transaction", lit(0)).withColumn("score_duration", lit(0))

    app_category_fav_monthly_transaction = app_category_fav_monthly_transaction.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    app_category_fav_monthly_duration = app_category_fav_monthly_duration.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    app_category_fav_monthly_volume = app_category_fav_monthly_volume.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")

    df_return = app_category_fav_monthly_transaction.union(app_category_fav_monthly_duration)
    df_return = df_return.union(app_category_fav_monthly_volume)

    df_return = node_from_config(df_return, sql_sum)
    df_return = node_from_config(df_return, sql_total)

    return df_return

    ############################## favorite_app_monthly_timeband #############################
def l3_digital_mobile_app_category_favorite_monthly_timeband(app_category_agg_timeband: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_transection: Dict[str, Any],sql_duration: Dict[str, Any],sql_volume: Dict[str, Any]):
    #---------------  sum traffic ------------------
    logging.info("favorite ------- > sum traffic")
    app_category_agg_timeband_sql_total = node_from_config(app_category_agg_timeband, sql_total)

    app_category_agg_timeband = app_category_agg_timeband.alias('app_category_agg_timeband').join(app_category_agg_timeband_sql_total.alias('app_category_agg_timeband_sql_total'),on=["subscription_identifier","mobile_no","start_of_month"],how="inner",)
    
    app_category_agg_timeband = app_category_agg_timeband.select(
        "app_category_agg_timeband.subscription_identifier",
        "app_category_agg_timeband.mobile_no",
        "app_category_agg_timeband.category_name",
        "app_category_agg_timeband.priority",
        "app_category_agg_timeband.start_of_month",
        "app_category_agg_timeband.total_visit_count",
        "app_category_agg_timeband.total_visit_duration",
        "app_category_agg_timeband.total_volume_byte",
        "app_category_agg_timeband_sql_total.sum_total_visit_count",
        "app_category_agg_timeband_sql_total.sum_total_visit_duration",
        "app_category_agg_timeband_sql_total.sum_total_volume_byte"
        )
    #---------------  sum cal fav ------------------
    logging.info("favorite ------- > cal")
    pp_category_agg_timeband_transection = node_from_config(app_category_agg_timeband,sql_transection)
    logging.info("favorite ------- > transection complete")
    pp_category_agg_timeband_duration = node_from_config(app_category_agg_timeband,sql_duration)
    logging.info("favorite ------- > duration complete")
    pp_category_agg_timeband_volume = node_from_config(app_category_agg_timeband,sql_volume)
    logging.info("favorite ------- > volume complete")
    #---------------  union ------------------
    logging.info("favorite ------- > union")
    df_return = pp_category_agg_timeband_transection.union(pp_category_agg_timeband_duration)
    df_return = df_return.union(pp_category_agg_timeband_volume)
    return df_return

    ################################# combine_monthly ###############################

def digital_to_l3_digital_combine_agg_monthly(combine_category_agg_daily: pyspark.sql.DataFrame,sql: Dict[str, Any]):
    combine_category_agg_daily = combine_category_agg_daily.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("event_partition_date").cast("string"), 1, 7), f.lit("-01")
        ),
    ).drop(*["event_partition_date"])
    combine_category_agg_daily = node_from_config(combine_category_agg_daily,sql)
    return combine_category_agg_daily

    ############################## favorite_combine_monthly #############################
def digital_mobile_combine_category_favorite_monthly(combine_monthly: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_transection: Dict[str, Any],sql_duration: Dict[str, Any],sql_volume: Dict[str, Any]):
    #---------------  sum traffic ------------------
    logging.info("favorite ------- > sum traffic")
    combine_monthly_sql_total = node_from_config(combine_monthly, sql_total)

    combine_monthly = combine_monthly.alias('combine_monthly').join(combine_monthly_sql_total.alias('combine_monthly_sql_total'),on=["subscription_identifier","mobile_no","start_of_month",],how="inner",)
    
    combine_monthly = combine_monthly.select(
        "combine_monthly.subscription_identifier",
        "combine_monthly.mobile_no",
        "combine_monthly.category_name",
        # "combine_monthly.priority",
        "combine_monthly.start_of_month",
        "combine_monthly.total_visit_count",
        "combine_monthly.total_visit_duration",
        "combine_monthly.total_volume_byte",
        "combine_monthly_sql_total.sum_total_visit_count",
        "combine_monthly_sql_total.sum_total_visit_duration",
        "combine_monthly_sql_total.sum_total_volume_byte"
        )
    #---------------  sum cal fav ------------------
    logging.info("favorite ------- > cal")
    combine_monthly_transection = node_from_config(combine_monthly,sql_transection)
    logging.info("favorite ------- > transection complete")
    combine_monthly_duration = node_from_config(combine_monthly,sql_duration)
    logging.info("favorite ------- > duration complete")
    app_category_agg_daily_volume = node_from_config(combine_monthly,sql_volume)
    logging.info("favorite ------- > volume complete")
    #---------------  union ------------------
    logging.info("favorite ------- > union")
    df_return = combine_monthly_transection.union(combine_monthly_duration)
    df_return = df_return.union(app_category_agg_daily_volume)
    return df_return
    ################################## timeband_monthly ################################

def digital_mobile_app_category_agg_timeband_monthly(Mobile_app_timeband_monthly: pyspark.sql.DataFrame,
                                                     mobile_app_timeband_monthly_share_sql: Dict[str, Any]):
    # import os, subprocess
    ##check missing data##
    if check_empty_dfs([Mobile_app_timeband_monthly]):
        return get_spark_empty_df()
    # where data timeband
    # p_partition = str(os.getenv("RUN_PARTITION", "no_input"))
    # if (p_partition != 'no_input'):
    #     Mobile_app_timeband_monthly = Mobile_app_timeband_monthly.filter(Mobile_app_timeband_monthly["starttime"][0:8] == p_partition)
    #
    # # where timeband
    # if (timeband == "Morning"):
    #     Mobile_app_timeband_monthly = Mobile_app_timeband_monthly.filter(Mobile_app_timeband_monthly["ld_hour"] >= 6).filter(
    #         Mobile_app_timeband_monthly["ld_hour"] <= 11)
    # elif (timeband == "Afternoon"):
    #     Mobile_app_timeband_monthly = Mobile_app_timeband_monthly.filter(Mobile_app_timeband_monthly["ld_hour"] >= 12).filter(
    #         Mobile_app_timeband_monthly["ld_hour"] <= 17)
    # elif (timeband == "Evening"):
    #     Mobile_app_timeband_monthly = Mobile_app_timeband_monthly.filter(Mobile_app_timeband_monthly["ld_hour"] >= 18).filter(
    #         Mobile_app_timeband_monthly["ld_hour"] <= 23)
    # else:
    #     Mobile_app_timeband_monthly = Mobile_app_timeband_monthly.filter(Mobile_app_timeband_monthly["ld_hour"] >= 0).filter(
    #         Mobile_app_timeband_monthly["ld_hour"] <= 5)

    Mobile_app_timeband_monthly = Mobile_app_timeband_monthly.withColumn("start_of_month",f.to_date(f.date_trunc('month',"event_partition_date")))

    df_return = node_from_config(Mobile_app_timeband_monthly, mobile_app_timeband_monthly_share_sql)
    return df_return
    
    ################################## combine_score_monthly ################################
def l3_digital_mobile_combine_category_score_monthly(app_category_fav_monthly: pyspark.sql.DataFrame,web_category_fav_monthly: pyspark.sql.DataFrame,sql_combine: Dict[str, Any],sql_sum_total: Dict[str, Any],sql_sum_share: Dict[str, Any],sql_total: Dict[str, Any],sql_sum: Dict[str, Any]):
    #union app & web 
    app_category_fav_monthly = app_category_fav_monthly.union(web_category_fav_monthly)
    app_category_fav_monthly = node_from_config(app_category_fav_monthly, sql_combine)
    #cal share
    app_category_fav_monthly_total = node_from_config(app_category_fav_monthly, sql_sum_total)
    app_category_fav_monthly = app_category_fav_monthly.alias('app_category_fav_monthly').join(app_category_fav_monthly_total.alias('app_category_fav_monthly_total'),on=["subscription_identifier","mobile_no","favorite_by","start_of_month",],how="inner",)
    app_category_fav_monthly = node_from_config(app_category_fav_monthly, sql_sum_share)
    #sprit by favorite by
    app_category_fav_monthly_transaction = app_category_fav_monthly.filter(app_category_fav_monthly["favorite_by"] == 'Transaction')
    app_category_fav_monthly_duration = app_category_fav_monthly.filter(app_category_fav_monthly["favorite_by"] == 'Duration')
    app_category_fav_monthly_volume = app_category_fav_monthly.filter(app_category_fav_monthly["favorite_by"] == 'Volume')
    #add column
    app_category_fav_monthly_transaction = app_category_fav_monthly_transaction.withColumnRenamed("sharing_score", 'score_transaction')
    app_category_fav_monthly_duration = app_category_fav_monthly_duration.withColumnRenamed("sharing_score", 'score_duration')
    app_category_fav_monthly_volume = app_category_fav_monthly_volume.withColumnRenamed("sharing_score", 'score_volume')

    app_category_fav_monthly_transaction = app_category_fav_monthly_transaction.withColumn("score_duration", lit(0)).withColumn("score_volume", lit(0))
    app_category_fav_monthly_duration = app_category_fav_monthly_duration.withColumn("score_transaction", lit(0)).withColumn("score_volume", lit(0))
    app_category_fav_monthly_volume = app_category_fav_monthly_volume.withColumn("score_transaction", lit(0)).withColumn("score_duration", lit(0))

    app_category_fav_monthly_transaction = app_category_fav_monthly_transaction.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    app_category_fav_monthly_duration = app_category_fav_monthly_duration.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    app_category_fav_monthly_volume = app_category_fav_monthly_volume.select("subscription_identifier","mobile_no","category_name","score_transaction","score_duration","score_volume","start_of_month")
    #union
    df_return = app_category_fav_monthly_transaction.union(app_category_fav_monthly_duration)
    df_return = df_return.union(app_category_fav_monthly_volume)

    df_return = node_from_config(df_return, sql_sum)
    df_return = node_from_config(df_return, sql_total)

    return df_return

    ################################## combine_score_monthly ################################
def l3_digital_mobile_combine_favorite_by_category_monthly(app_monthly: pyspark.sql.DataFrame,web_monthly: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_transection: Dict[str, Any],sql_duration: Dict[str, Any],sql_volume: Dict[str, Any],category_level: Dict[str, Any]):
    logging.info("combine ------- > union all App & Web")   
    app_monthly = app_monthly.withColumnRenamed("application", 'argument')
    web_monthly = web_monthly.withColumnRenamed("domain", 'argument')  
    combine_monthly = app_monthly.unionAll(web_monthly)
    combine_monthly = combine_monthly.withColumnRenamed(category_level, 'category_name')
    logging.info("favorite ------- > sum traffic")
    combine_category_agg_monthly_sql_total = node_from_config(combine_monthly, sql_total)
    combine_category_agg_monthly = combine_monthly.alias('combine_category_agg_monthly').join(combine_category_agg_monthly_sql_total.alias('combine_category_agg_monthly_sql_total'),on=["subscription_identifier","mobile_no","start_of_month","category_name"],how="inner",)
    

    combine_category_agg_monthly = combine_category_agg_monthly.select(
        "combine_category_agg_monthly.subscription_identifier",
        "combine_category_agg_monthly.mobile_no",
        "combine_category_agg_monthly.category_name",
        "combine_category_agg_monthly.argument",
        # "combine_category_agg_monthly.priority",
        "combine_category_agg_monthly.start_of_month",
        "combine_category_agg_monthly.total_visit_count",
        "combine_category_agg_monthly.total_visit_duration",
        "combine_category_agg_monthly.total_volume_byte",
        "combine_category_agg_monthly_sql_total.sum_total_visit_count",
        "combine_category_agg_monthly_sql_total.sum_total_visit_duration",
        "combine_category_agg_monthly_sql_total.sum_total_volume_byte"
        )
    #---------------  sum cal fav ------------------
    logging.info("favorite ------- > cal")
    combine_category_agg_monthly_transection = node_from_config(combine_category_agg_monthly,sql_transection)
    logging.info("favorite ------- > transection complete")
    combine_category_agg_monthly_duration = node_from_config(combine_category_agg_monthly,sql_duration)
    logging.info("favorite ------- > duration complete")
    combine_category_agg_monthly_volume = node_from_config(combine_category_agg_monthly,sql_volume)
    logging.info("favorite ------- > volume complete")
    #---------------  union ------------------
    logging.info("favorite ------- > union")
    df_return = combine_category_agg_monthly_transection.unionAll(combine_category_agg_monthly_duration)
    df_return = df_return.unionAll(combine_category_agg_monthly_volume)

    return df_return


    ################################# combine_timeband_monthly ###############################

def digital_to_l3_digital_combine_timeband_monthly(combine_category_agg_timeband_monthly: pyspark.sql.DataFrame,combine_category_agg_monthly: pyspark.sql.DataFrame,sql: Dict[str, Any],sql_share: Dict[str, Any]):
    
    combine_category_agg_timeband_monthly = combine_category_agg_timeband_monthly.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("event_partition_date").cast("string"), 1, 7), f.lit("-01")
        ),
    ).drop(*["event_partition_date"])
    logging.info("timeband ---------------> sum timeband monthly")
    combine_category_agg_timeband_monthly = node_from_config(combine_category_agg_timeband_monthly,sql)
    
    logging.info("timeband ---------------> cal monthly")
    combine_category_agg_monthly = combine_category_agg_monthly.withColumnRenamed("total_visit_count", 'total_visit_count_monthly')
    combine_category_agg_monthly = combine_category_agg_monthly.withColumnRenamed("total_visit_duration", 'total_visit_duration_monthly')
    combine_category_agg_monthly = combine_category_agg_monthly.withColumnRenamed("total_volume_byte", 'total_volume_byte_monthly')
    combine_category_agg_monthly = combine_category_agg_monthly.withColumnRenamed("total_download_byte", 'total_download_byte_monthly')
    combine_category_agg_monthly = combine_category_agg_monthly.withColumnRenamed("total_upload_byte", 'total_upload_byte_monthly')
    # combine_category_agg_monthly = combine_category_agg_monthly.withColumnRenamed("priority", 'priority_daily')
    logging.info("Dates to run for join time band and monthly")

    combine_category_agg_timeband_monthly = combine_category_agg_timeband_monthly.alias('combine_category_agg_timeband_monthly').join(combine_category_agg_monthly.alias('combine_category_agg_monthly'),
        on=[
            combine_category_agg_timeband_monthly.subscription_identifier == combine_category_agg_monthly.subscription_identifier,
            combine_category_agg_timeband_monthly.mobile_no == combine_category_agg_monthly.mobile_no ,
            combine_category_agg_timeband_monthly.category_name == combine_category_agg_monthly.category_name,
            combine_category_agg_timeband_monthly.start_of_month == combine_category_agg_monthly.start_of_month 
        ],
        how="inner",
    )
    combine_category_agg_timeband_monthly = combine_category_agg_timeband_monthly.select(
        "combine_category_agg_timeband_monthly.subscription_identifier",
        "combine_category_agg_timeband_monthly.mobile_no",
        "combine_category_agg_timeband_monthly.category_name",
        # "combine_category_agg_timeband_monthly.priority",
        "combine_category_agg_timeband_monthly.start_of_month",
        "combine_category_agg_timeband_monthly.total_visit_count",
        "combine_category_agg_timeband_monthly.total_visit_duration",
        "combine_category_agg_timeband_monthly.total_volume_byte",
        "combine_category_agg_timeband_monthly.total_download_byte",
        "combine_category_agg_timeband_monthly.total_upload_byte",
        "combine_category_agg_monthly.total_visit_count_monthly",
        "combine_category_agg_monthly.total_visit_duration_monthly",
        "combine_category_agg_monthly.total_volume_byte_monthly",
        "combine_category_agg_monthly.total_download_byte_monthly",
        "combine_category_agg_monthly.total_upload_byte_monthly",
        )
    #----------
    logging.info("timeband ---------------> share")
    combine_category_agg_timeband_monthly = node_from_config(combine_category_agg_timeband_monthly,sql_share)

    return combine_category_agg_timeband_monthly

################## Cxense agg category monthly by category ###########################
def l3_digital_cxense_category_agg_monthly (cxense_agg_daily: DataFrame, cxense_agg_sql: Dict[str, Any]) -> DataFrame :

    if check_empty_dfs([cxense_agg_daily]):
        return get_spark_empty_df()

    cxense_agg_daily = cxense_agg_daily.withColumn("start_of_month", f.to_date(f.date_trunc('month', "event_partition_date")))

    df_cxense_agg_monthly_category_agg = cxense_agg_daily.groupBy("subscription_identifier","mobile_no","url" ,"category_name","priority" ,"start_of_month").agg(
        f.sum("total_visit_count").alias("total_visit_count"),
        f.sum("total_visit_duration").alias("total_visit_duration")
        )

    df_cxense_agg_monthly_category_agg = node_from_config(df_cxense_agg_monthly_category_agg,cxense_agg_sql)
    return df_cxense_agg_monthly_category_agg