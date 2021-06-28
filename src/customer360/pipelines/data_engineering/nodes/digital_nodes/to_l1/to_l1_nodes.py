import pyspark.sql.functions as f, logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame, Window
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , add_event_week_and_month_from_yyyymmdd, union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
import pyspark as pyspark
from functools import reduce
from typing import Dict, Any
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

    ############################### category_daily ##############################

#####################  Category master aib ###########################
def build_l1_digital_iab_category_table(aib_raw: DataFrame, aib_priority_mapping: DataFrame):

    # if check_empty_dfs([aib_raw]):
    #     return get_spark_empty_df()
    # if check_empty_dfs([aib_priority_mapping]):
    #     return get_spark_empty_df()

    aib_clean = (
        aib_raw.withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
            .filter(f.col("argument").isNotNull())
            .filter(f.col("argument") != "")
    )

    aib_priority_mapping_clean = aib_priority_mapping.withColumnRenamed(
        "category", "level_1"
    ).withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping_clean, on=["level_1"], how="inner"
    ).withColumnRenamed("level_1" , "category_name")

    return iab_category_table

    ############################### Mobile_app_master##############################
def digital_mobile_app_category_master(app_categories_master: DataFrame,iab_category_master: DataFrame,iab_category_priority: DataFrame):

    iab_category_master = iab_category_master.filter(f.lower(f.trim(f.col("source_type"))) == "application")
    iab_category_master = iab_category_master.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")

    app_categories_master = app_categories_master.join(
        f.broadcast(iab_category_master),
        on=[app_categories_master.application_name == iab_category_master.argument],
        how="inner",
    )

    app_categories_master = app_categories_master.select(
                                                     app_categories_master["application_id"],
                                                     iab_category_master["argument"],
                                                     iab_category_master["level_1"],
                                                     iab_category_master["level_2"],
                                                     iab_category_master["level_3"],
                                                     iab_category_master["level_4"]
                                                    )

    app_categories_master_map_priority = app_categories_master.join(
        f.broadcast(iab_category_priority),
        on=[app_categories_master.level_1 == iab_category_priority.category],
        how="inner",
    )

    df_return = app_categories_master_map_priority.select(app_categories_master["application_id"],
                                app_categories_master["argument"],
                                app_categories_master["level_1"],
                                app_categories_master["level_2"],
                                app_categories_master["level_3"],
                                app_categories_master["level_4"],
                                iab_category_priority["priority"])

    return df_return

    ############################### Mobile_app_daily ##############################
def digital_mobile_app_category_agg_daily(mobile_app_daily: DataFrame, mobile_app_daily_sql: dict,category_level: dict):
    ##check missing data##
    if check_empty_dfs([mobile_app_daily]):
        return get_spark_empty_df()

    # where this column more than 0
    mobile_app_daily = mobile_app_daily.where(f.col("count_trans") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("duration") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("total_byte") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("download_byte") > 0)
    mobile_app_daily = mobile_app_daily.where(f.col("upload_byte") > 0)

    mobile_app_daily = mobile_app_daily.withColumnRenamed(category_level, 'category_name')
    mobile_app_daily = mobile_app_daily.withColumn("priority", f.lit(None).cast(StringType()))
    mobile_app_daily = mobile_app_daily.withColumnRenamed('partition_date', 'event_partition_date')

    df_return = node_from_config(mobile_app_daily, mobile_app_daily_sql)
    return df_return

    ############################### Mobile_app_timeband ##############################

def digital_mobile_app_category_agg_timeband(Mobile_app_timeband: DataFrame,
                                             Mobile_app_daily: DataFrame,
                                             app_categories_master: DataFrame,
                                             category_level: dict,
                                             timeband: dict,
                                             mobile_app_timeband_sql: dict,
                                             mobile_app_timeband_sql_share: dict):
    import os,subprocess

    ##check missing data##
    # if check_empty_dfs([Mobile_app_timeband]):
    #     return get_spark_empty_df()
    #where data timeband

    p_partition = str(os.getenv("RUN_PARTITION", "no_input"))
    if  (p_partition != 'no_input'):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["starttime"][0:8] == p_partition )

    #where timeband
    if (timeband == "Morning"):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 6 ).filter(Mobile_app_timeband["ld_hour"] <= 11 )
    elif (timeband == "Afternoon"):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 12 ).filter(Mobile_app_timeband["ld_hour"] <= 17 )
    elif (timeband == "Evening"):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 18 ).filter(Mobile_app_timeband["ld_hour"] <= 23 )
    else:
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 0 ).filter(Mobile_app_timeband["ld_hour"] <= 5 )

    # where this column more than 0
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('ul_kbyte', 'ul_byte')

    Mobile_app_timeband = Mobile_app_timeband.where(f.col("dw_byte") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("ul_byte") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("time_cnt") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("duration_sec") > 0)
    #join master
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed("msisdn", "mobile_no").join(f.broadcast(app_categories_master),
        on=[app_categories_master.application_id == Mobile_app_timeband.application],
        how="inner",
    )

    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed(category_level, 'category_name')
    Mobile_app_timeband = Mobile_app_timeband.withColumn('event_partition_date',concat(col("starttime")[0:4],f.lit('-'),concat(col("starttime")[5:2]),f.lit('-'),concat(col("starttime")[7:2])))
    logging.info("Dates to run for timebamd")
    Mobile_app_timeband = node_from_config(Mobile_app_timeband, mobile_app_timeband_sql)
    logging.info("Dates to run for timebamd Complete")
    #-------------------------------- share ----------------------------

    Mobile_app_daily = Mobile_app_daily.withColumnRenamed("total_visit_count", 'total_visit_count_daily')
    Mobile_app_daily = Mobile_app_daily.withColumnRenamed("total_visit_duration", 'total_visit_duration_daily')
    Mobile_app_daily = Mobile_app_daily.withColumnRenamed("total_volume_byte", 'total_volume_byte_daily')
    Mobile_app_daily = Mobile_app_daily.withColumnRenamed("total_download_byte", 'total_download_byte_daily')
    Mobile_app_daily = Mobile_app_daily.withColumnRenamed("total_upload_byte", 'total_upload_byte_daily')
    Mobile_app_daily = Mobile_app_daily.withColumnRenamed("priority", 'priority_daily')
    logging.info("Dates to run for join time band and daily")
    Mobile_app_timeband = Mobile_app_timeband.join(Mobile_app_daily,
        on=[Mobile_app_timeband.mobile_no == Mobile_app_daily.mobile_no ,Mobile_app_timeband.category_name == Mobile_app_daily.category_name,Mobile_app_timeband.event_partition_date == Mobile_app_daily.event_partition_date ],
        how="inner",
    )
    logging.info("select column")
    Mobile_app_timeband = Mobile_app_timeband.select(Mobile_app_daily["subscription_identifier"],Mobile_app_daily["mobile_no"],Mobile_app_daily["category_name"],Mobile_app_timeband["priority"],"total_visit_count","total_visit_duration","total_volume_byte","total_download_byte","total_upload_byte","total_visit_count_daily","total_visit_duration_daily","total_volume_byte_daily","total_download_byte_daily","total_upload_byte_daily",Mobile_app_daily["event_partition_date"])
    logging.info("Dates to run for share")
    df_return = node_from_config(Mobile_app_timeband, mobile_app_timeband_sql_share)
    return df_return

    ################### timeband join sub ################################

def digital_mobile_app_category_agg_timeband_feature(Mobile_app_timeband: DataFrame,customer_profile_key: DataFrame):

    customer_profile_key = customer_profile_key.select(customer_profile_key["access_method_num"],customer_profile_key["subscription_identifier"])
    #clear dup
    customer_profile_key =  customer_profile_key.groupby("access_method_num", "subscription_identifier").count()
    customer_profile_key = customer_profile_key.drop('count')
    Mobile_app_timeband = Mobile_app_timeband.join(customer_profile_key,
        on=[Mobile_app_timeband.mobile_no == customer_profile_key.access_method_num],
        how="inner",
    )

    Mobile_app_timeband = Mobile_app_timeband.select("subscription_identifier","mobile_no","category_name","priority","total_visit_count","total_visit_duration","total_volume_byte","total_upload_byte","event_partition_date")
    return Mobile_app_timeband

################## mobile web daily agg category ###########################
def l1_digital_customer_web_category_agg_daily(
        mobile_web_daily_raw: DataFrame,
        aib_categories_clean: DataFrame,
        cxense_daily: DataFrame,
        web_sql_sum: dict
) -> DataFrame:
    ##check missing data##
    if check_empty_dfs([mobile_web_daily_raw]):
        return get_spark_empty_df()

    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_type"))) == "url")
    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")

    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("count_trans") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("duration") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("total_kb") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("download_kb") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("upload_kb") > 0)

    df_mobile_web_daily = mobile_web_daily_raw.join(
        f.broadcast(aib_categories_clean)
        , on=[aib_categories_clean.argument == mobile_web_daily_raw.domain]
        , how="inner"
    ).select("subscription_identifier", "mobile_no", "category_name", "priority", "upload_byte", "download_byte", "duration" , "total_byte", "count_trans", "partition_date")

    df_mobile_web_daily_category_agg = df_mobile_web_daily.groupBy("subscription_identifier", "mobile_no",
                                                                   "category_name", "priority", "partition_date").agg(

        f.sum("count_trans").cast(LongType()).alias("total_visit_count"),
        f.sum("duration").cast(LongType()).alias("total_visit_duration"),
        f.sum("total_byte").cast("decimal(35,4)").alias("total_volume_byte"),
        f.sum("download_byte").cast("decimal(35,4)").alias("total_download_byte"),
        f.sum("upload_byte").cast("decimal(35,4)").alias("total_upload_byte"),
        )

    df_mobile_web_daily_category_agg = df_mobile_web_daily_category_agg.withColumnRenamed("partition_date", "event_partition_date")

    df_mobile_web_daily_category_agg = df_mobile_web_daily_category_agg.select("subscription_identifier",
                                                                               "mobile_no",
                                                                               "category_name",
                                                                               "priority",
                                                                               "total_visit_count",
                                                                               "total_visit_duration",
                                                                               "total_volume_byte",
                                                                               "total_download_byte",
                                                                               "total_upload_byte",
                                                                               "event_partition_date")

    cxense_daily = cxense_daily.withColumn("total_volume_byte", f.lit(0).cast("decimal(35,4)"))\
        .withColumn("total_download_byte", f.lit(0).cast("decimal(35,4)"))\
        .withColumn("total_upload_byte",f.lit(0).cast("decimal(35,4)"))

    cxense_daily = cxense_daily.select("subscription_identifier",
                                       "mobile_no",
                                       "category_name",
                                       "priority",
                                       "total_visit_count",
                                       "total_visit_duration",
                                       "total_volume_byte",
                                       "total_download_byte",
                                       "total_upload_byte",
                                       "event_partition_date")

    df_return = df_mobile_web_daily_category_agg.unionAll(cxense_daily).distinct()
    df_return = node_from_config(df_return, web_sql_sum)

    return df_return

################## mobile web daily agg category level_2-4 ###########################
def l1_digital_customer_web_category_agg_daily_cat_level(
        mobile_web_daily_raw: DataFrame,
        aib_categories_clean: DataFrame,
        cxense_daily: DataFrame,
        web_sql_sum: dict,
        cat_level: dict
) -> DataFrame:
    ##check missing data##
    if check_empty_dfs([mobile_web_daily_raw]):
        return get_spark_empty_df()

    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_type"))) == "url")
    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")

    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("count_trans") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("duration") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("total_byte") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("download_byte") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("upload_byte") > 0)

    df_mobile_web_daily = mobile_web_daily_raw.join(
        f.broadcast(aib_categories_clean)
        , on=[aib_categories_clean.argument == mobile_web_daily_raw.domain]
        , how="inner"
    ).select("subscription_identifier", "mobile_no", cat_level, "priority", "upload_byte", "download_byte", "duration" , "total_byte", "count_trans", "partition_date")

    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed(cat_level, "category_name")

    df_mobile_web_daily_category_agg = df_mobile_web_daily.groupBy("subscription_identifier", "mobile_no",
                                                                   "category_name", "priority", "partition_date").agg(

        f.sum("count_trans").cast(LongType()).alias("total_visit_count"),
        f.sum("duration").cast(LongType()).alias("total_visit_duration"),
        f.sum("total_byte").cast("decimal(35,4)").alias("total_volume_byte"),
        f.sum("download_byte").cast("decimal(35,4)").alias("total_download_byte"),
        f.sum("upload_byte").cast("decimal(35,4)").alias("total_upload_byte"),
        )

    df_mobile_web_daily_category_agg = df_mobile_web_daily_category_agg.withColumnRenamed("partition_date", "event_partition_date")

    df_mobile_web_daily_category_agg = df_mobile_web_daily_category_agg.select("subscription_identifier",
                                                                               "mobile_no",
                                                                               "category_name",
                                                                               "priority",
                                                                               "total_visit_count",
                                                                               "total_visit_duration",
                                                                               "total_volume_byte",
                                                                               "total_download_byte",
                                                                               "total_upload_byte",
                                                                               "event_partition_date")

    cxense_daily = cxense_daily.withColumn("total_volume_byte", f.lit(0).cast("decimal(35,4)"))\
        .withColumn("total_download_byte", f.lit(0).cast("decimal(35,4)"))\
        .withColumn("total_upload_byte",f.lit(0).cast("decimal(35,4)"))

    cxense_daily = cxense_daily.select("subscription_identifier",
                                       "mobile_no",
                                       "category_name",
                                       "priority",
                                       "total_visit_count",
                                       "total_visit_duration",
                                       "total_volume_byte",
                                       "total_download_byte",
                                       "total_upload_byte",
                                       "event_partition_date")

    df_return = df_mobile_web_daily_category_agg.unionAll(cxense_daily).distinct()
    df_return = node_from_config(df_return, web_sql_sum)

    return df_return

################## mobile web agg level category ###########################
def l1_digital_mobile_web_level_category(mobile_web_daily_category_agg: DataFrame):

    if check_empty_dfs([mobile_web_daily_category_agg]):
        return get_spark_empty_df()

    key = ["mobile_no", "event_partition_date"]
    df_soc_web_day_level_stats = mobile_web_daily_category_agg.groupBy(key).agg(
        f.sum("total_download_byte").alias("total_download_byte"),
        f.sum("total_upload_byte").alias("total_upload_byte"),
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_volume_byte").alias("total_volume_byte"),
        f.sum("total_visit_counts").alias("total_visit_count"),
    )
    return df_soc_web_day_level_stats

################## mobile web timebrand agg category ###########################
def l1_digital_customer_web_category_agg_timeband(mobile_web_hourly_raw: DataFrame,
                                                 union_profile: DataFrame,
                                                 mobile_web_daily_raw: DataFrame,
                                                 aib_categories_clean: DataFrame,
                                                 df_mobile_web_hourly_agg_sql: dict,
                                                 df_timeband_web: dict,
                                                 df_timeband_sql: dict) -> DataFrame:

    if check_empty_dfs([mobile_web_hourly_raw]):
        return get_spark_empty_df()
    if check_empty_dfs([aib_categories_clean]):
        return get_spark_empty_df()

################## Filter Hour ###########################
    if (df_timeband_web == "Morning"):
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 6).filter(
            mobile_web_hourly_raw["ld_hour"] <= 11)
    elif (df_timeband_web == "Afternoon"):
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 12).filter(
            mobile_web_hourly_raw["ld_hour"] <= 17)
    elif (df_timeband_web == "Evening"):
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 18).filter(
            mobile_web_hourly_raw["ld_hour"] <= 23)
    else:
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 0).filter(
            mobile_web_hourly_raw["ld_hour"] <= 5)

    mobile_web_hourly_raw = mobile_web_hourly_raw.where(f.col("dw_kbyte") > 0)
    mobile_web_hourly_raw = mobile_web_hourly_raw.where(f.col("ul_kbyte") > 0)

################## Join url and argument ###########################
    mobile_web_hourly_raw = (
        mobile_web_hourly_raw.withColumnRenamed("msisdn", "mobile_no").join(f.broadcast(aib_categories_clean), on=[
            aib_categories_clean.argument == mobile_web_hourly_raw.host], how="inner", )).select("batchno", "mobile_no",
                                                                                                    "category_name",
                                                                                                    "priority",
                                                                                                    "dw_kbyte",
                                                                                                    "ul_kbyte",
                                                                                                    "air_port_duration",
                                                                                                    "count_transaction",
                                                                                                    "ld_hour")

################## Rename Columns Event Partition Date ###########################
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumnRenamed("dw_kbyte", "dw_byte")
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumnRenamed("ul_kbyte", "ul_byte")
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumn('event_partition_date',
                                                             concat(f.col("batchno")[0:4], f.lit('-'),
                                                                    concat(f.col("batchno")[5:2]), f.lit('-'),
                                                                    concat(f.col("batchno")[7:2]))).drop("batchno", "ld_hour")

    mobile_web_hourly_raw = node_from_config(mobile_web_hourly_raw, df_mobile_web_hourly_agg_sql)

############################# share ####################
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_visit_count", 'total_visit_count_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_visit_duration", 'total_visit_duration_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_volume_byte", 'total_volume_byte_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_download_byte", 'total_download_byte_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_upload_byte", 'total_upload_byte_daily')

############################# timeband join mobile web daily for sub_id ####################
    mobile_web_hourly_raw = mobile_web_hourly_raw.join(mobile_web_daily_raw,
                                                       on=[mobile_web_hourly_raw.mobile_no == mobile_web_daily_raw.mobile_no ,
                                                           mobile_web_hourly_raw.category_name == mobile_web_daily_raw.category_name],
                                                       how="inner").select(
                                                       mobile_web_daily_raw.subscription_identifier,
                                                       mobile_web_daily_raw.mobile_no,
                                                       mobile_web_daily_raw.category_name,
                                                       mobile_web_hourly_raw.priority,
                                                       mobile_web_hourly_raw.total_visit_count,
                                                       mobile_web_hourly_raw.total_visit_duration,
                                                       mobile_web_hourly_raw.total_volume_byte,
                                                       mobile_web_hourly_raw.total_download_byte,
                                                       mobile_web_hourly_raw.total_upload_byte,
                                                       mobile_web_daily_raw.total_visit_count_daily,
                                                       mobile_web_daily_raw.total_visit_duration_daily,
                                                       mobile_web_daily_raw.total_volume_byte_daily,
                                                       mobile_web_daily_raw.total_download_byte_daily,
                                                       mobile_web_daily_raw.total_upload_byte_daily,
                                                       mobile_web_daily_raw.event_partition_date)

    mobile_web_hourly_raw = node_from_config(mobile_web_hourly_raw, df_timeband_sql)

############################# timeband join union daily for sub_id ####################
    mobile_web_hourly_raw = mobile_web_hourly_raw.join(union_profile,
                                                       on=[
                                                           mobile_web_hourly_raw.mobile_no == union_profile.access_method_num],
                                                       how="inner").select(
                                                       union_profile.subscription_identifier,
                                                       mobile_web_hourly_raw.mobile_no,
                                                       mobile_web_hourly_raw.category_name,
                                                       mobile_web_hourly_raw.priority,
                                                       mobile_web_hourly_raw.total_visit_count,
                                                       mobile_web_hourly_raw.total_visit_duration,
                                                       mobile_web_hourly_raw.total_volume_byte,
                                                       mobile_web_hourly_raw.total_download_byte,
                                                       mobile_web_hourly_raw.total_upload_byte,
                                                       mobile_web_hourly_raw.share_total_visit_count,
                                                       mobile_web_hourly_raw.share_total_visit_duration,
                                                       mobile_web_hourly_raw.share_total_volume_byte,
                                                       mobile_web_hourly_raw.share_total_download_byte,
                                                       mobile_web_hourly_raw.share_total_upload_byte,
                                                       mobile_web_hourly_raw.event_partition_date)

    return mobile_web_hourly_raw

################## mobile web timebrand agg category ###########################
def l1_digital_customer_web_category_agg_timeband_cat_level(mobile_web_hourly_raw: DataFrame,
                                                 union_profile: DataFrame,
                                                 mobile_web_daily_raw: DataFrame,
                                                 aib_categories_clean: DataFrame,
                                                 df_mobile_web_hourly_agg_sql: dict,
                                                 df_timeband_web: dict,
                                                 df_timeband_sql: dict,
                                                 cat_level: dict) -> DataFrame:

    if check_empty_dfs([mobile_web_hourly_raw]):
        return get_spark_empty_df()
    if check_empty_dfs([aib_categories_clean]):
        return get_spark_empty_df()

################## Filter Hour ###########################
    if (df_timeband_web == "Morning"):
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 6).filter(
            mobile_web_hourly_raw["ld_hour"] <= 11)
    elif (df_timeband_web == "Afternoon"):
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 12).filter(
            mobile_web_hourly_raw["ld_hour"] <= 17)
    elif (df_timeband_web == "Evening"):
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 18).filter(
            mobile_web_hourly_raw["ld_hour"] <= 23)
    else:
        mobile_web_hourly_raw = mobile_web_hourly_raw.filter(mobile_web_hourly_raw["ld_hour"] >= 0).filter(
            mobile_web_hourly_raw["ld_hour"] <= 5)

    mobile_web_hourly_raw = mobile_web_hourly_raw.where(f.col("dw_kbyte") > 0)
    mobile_web_hourly_raw = mobile_web_hourly_raw.where(f.col("ul_kbyte") > 0)

################## Join url and argument ###########################
    mobile_web_hourly_raw = (
        mobile_web_hourly_raw.withColumnRenamed("msisdn", "mobile_no").join(f.broadcast(aib_categories_clean), on=[
            aib_categories_clean.argument == mobile_web_hourly_raw.host], how="inner", )).select("batchno", "mobile_no",
                                                                                                    cat_level,
                                                                                                    "priority",
                                                                                                    "dw_kbyte",
                                                                                                    "ul_kbyte",
                                                                                                    "air_port_duration",
                                                                                                    "count_transaction",
                                                                                                    "ld_hour")

################## Rename Columns Event Partition Date ###########################
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumnRenamed("dw_kbyte", "dw_byte")
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumnRenamed("ul_kbyte", "ul_byte")
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumnRenamed(cat_level, "category_name")
    mobile_web_hourly_raw = mobile_web_hourly_raw.withColumn('event_partition_date',
                                                             concat(f.col("batchno")[0:4], f.lit('-'),
                                                                    concat(f.col("batchno")[5:2]), f.lit('-'),
                                                                    concat(f.col("batchno")[7:2]))).drop("batchno", "ld_hour")

    mobile_web_hourly_raw = node_from_config(mobile_web_hourly_raw, df_mobile_web_hourly_agg_sql)

############################# share ####################
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_visit_count", 'total_visit_count_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_visit_duration", 'total_visit_duration_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_volume_byte", 'total_volume_byte_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_download_byte", 'total_download_byte_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_upload_byte", 'total_upload_byte_daily')

############################# timeband join mobile web daily for sub_id ####################
    mobile_web_hourly_raw = mobile_web_hourly_raw.join(mobile_web_daily_raw,
                                                       on=[mobile_web_hourly_raw.mobile_no == mobile_web_daily_raw.mobile_no ,
                                                           mobile_web_hourly_raw.category_name == mobile_web_daily_raw.category_name],
                                                       how="inner").select(
                                                       mobile_web_daily_raw.subscription_identifier,
                                                       mobile_web_daily_raw.mobile_no,
                                                       mobile_web_daily_raw.category_name,
                                                       mobile_web_hourly_raw.priority,
                                                       mobile_web_hourly_raw.total_visit_count,
                                                       mobile_web_hourly_raw.total_visit_duration,
                                                       mobile_web_hourly_raw.total_volume_byte,
                                                       mobile_web_hourly_raw.total_download_byte,
                                                       mobile_web_hourly_raw.total_upload_byte,
                                                       mobile_web_daily_raw.total_visit_count_daily,
                                                       mobile_web_daily_raw.total_visit_duration_daily,
                                                       mobile_web_daily_raw.total_volume_byte_daily,
                                                       mobile_web_daily_raw.total_download_byte_daily,
                                                       mobile_web_daily_raw.total_upload_byte_daily,
                                                       mobile_web_daily_raw.event_partition_date)

    mobile_web_hourly_raw = node_from_config(mobile_web_hourly_raw, df_timeband_sql)

############################# timeband join union daily for sub_id ####################
    mobile_web_hourly_raw = mobile_web_hourly_raw.join(union_profile,
                                                       on=[
                                                           mobile_web_hourly_raw.mobile_no == union_profile.access_method_num],
                                                       how="inner").select(
                                                       union_profile.subscription_identifier,
                                                       mobile_web_hourly_raw.mobile_no,
                                                       mobile_web_hourly_raw.category_name,
                                                       mobile_web_hourly_raw.priority,
                                                       mobile_web_hourly_raw.total_visit_count,
                                                       mobile_web_hourly_raw.total_visit_duration,
                                                       mobile_web_hourly_raw.total_volume_byte,
                                                       mobile_web_hourly_raw.total_download_byte,
                                                       mobile_web_hourly_raw.total_upload_byte,
                                                       mobile_web_hourly_raw.share_total_visit_count,
                                                       mobile_web_hourly_raw.share_total_visit_duration,
                                                       mobile_web_hourly_raw.share_total_volume_byte,
                                                       mobile_web_hourly_raw.share_total_download_byte,
                                                       mobile_web_hourly_raw.share_total_upload_byte,
                                                       mobile_web_hourly_raw.event_partition_date)

    return mobile_web_hourly_raw


################## Timebrand join subscription identifier ###########################
def l1_digital_mobile_web_category_agg_timeband_features(union_profile_daily: DataFrame,

                                                 mobile_web_hourly_agg: DataFrame,) -> DataFrame:

    if check_empty_dfs([union_profile_daily]):
        return get_spark_empty_df()
    if check_empty_dfs([mobile_web_hourly_agg]):
        return get_spark_empty_df()

################## Max date  ###########################
    df_mobile_web_hourly_agg_max_date = union_profile_daily.withColumn("max_date", f.col("event_partition_date").cast("string")).groupBy(
        "access_method_num", "subscription_identifier").agg(max("event_partition_date").alias("max_date"))

################## Join subscription identifier  ###########################
    df_mobile_web_hourly_agg = (
        mobile_web_hourly_agg.join(union_profile_daily,
                                   on=[union_profile_daily.access_method_num == mobile_web_hourly_agg.mobile_no],
                                   how="inner").select("subscription_identifier",
                                                          "mobile_no" ,
                                                          "category_name",
                                                          "priority",
                                                          "total_download_byte",
                                                          "total_upload_byte",
                                                          "total_visit_count",
                                                          "total_visit_duration",
                                                          "total_volume_byte"))

    return df_mobile_web_hourly_agg

################## relay agg ###########################
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

def digital_customer_relay_pageview_agg_daily(
    df_pageview: pyspark.sql.DataFrame, pageview_count_visit: Dict[str, Any],
):
    if check_empty_dfs([df_pageview]):
        return get_spark_empty_df()

    df_engagement_pageview_clean = relay_drop_nulls(df_pageview)
    df_engagement_pageview = df_engagement_pageview_clean.withColumn(
        "event_partition_date",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 7, 2)
                 ),
    ).drop(*["partition_date"])

    df_engagement_pageview_visits = node_from_config(df_engagement_pageview, pageview_count_visit)
    return df_engagement_pageview_visits

def digital_customer_relay_conversion_agg_daily(
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
        "event_partition_date",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 7, 2)
                 ),
    ).drop(*["partition_date"])

    df_engagement_conversion_package_clean = relay_drop_nulls(df_conversion_package)
    df_engagement_conversion_package = df_engagement_conversion_package_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != "") & (f.col("R42Product_status") == "successful"))
    df_engagement_conversion_package = df_engagement_conversion_package.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion_package = df_engagement_conversion_package.withColumn(
        "event_partition_date",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 7, 2)
                 ),
    ).drop(*["partition_date"])

    df_engagement_conversion_visits = node_from_config(df_engagement_conversion, conversion_count_visit_by_cid)
    df_engagement_conversion_package_visits = node_from_config(df_engagement_conversion_package, conversion_package_count_visit_by_cid)

    df_engagement_conversion_visits.createOrReplaceTempView("df_engagement_conversion_visits")
    df_engagement_conversion_package_visits.createOrReplaceTempView("df_engagement_conversion_package_visits")

    # spark = get_spark_session()
    # df_conversion_and_package_visits = spark.sql("""
    # select
    # COALESCE(a.subscription_identifier,b.subscription_identifier) as subscription_identifier,
    # COALESCE(a.mobile_no,b.mobile_no) as mobile_no,
    # COALESCE(a.campaign_id,b.campaign_id) as campaign_id,
    # a.total_conversion_product_count as total_conversion_product_count,
    # b.total_conversion_package_count as total_conversion_package_count,
    # COALESCE(a.event_partition_date,b.event_partition_date) as event_partition_date
    # from df_engagement_conversion_visits a
    # FULL JOIN df_engagement_conversion_package_visits b
    # ON a.subscription_identifier = b.subscription_identifier
    # and a.mobile_no = b.mobile_no
    # and a.campaign_id = b.campaign_id
    # and a.event_partition_date = b.event_partition_date
    # """)
    df_conversion_and_package_visits = join_all(
    [
        df_engagement_conversion_visits,
        df_engagement_conversion_package_visits
    ],
    on=["subscription_identifier", "event_partition_date", "mobile_no","campaign_id"],
    how="outer",
    )
    return df_conversion_and_package_visits


################## combine web agg category ###########################
def digital_to_l1_combine_app_web_agg_daily(app_category_agg_daily: pyspark.sql.DataFrame,app_category_web_daily: pyspark.sql.DataFrame,combine_app_web_agg_daily: dict):

    if check_empty_dfs([app_category_agg_daily]):
        return get_spark_empty_df()

    # if check_empty_dfs([app_category_web_daily]):
    #     return get_spark_empty_df()


    combine = app_category_agg_daily.union(app_category_web_daily)
    logging.info("Union App & Web Complete")

    combine = combine.withColumnRenamed("category_name", "category_name_old")
    combine = combine.withColumn('category_name', f.lower(f.col("category_name_old")))
    combine = combine.drop('category_name_old')
    df_return = node_from_config(combine,combine_app_web_agg_daily)

    return df_return

    ################## combine agg category timeband ###########################
def l1_digital_customer_combine_category_agg_timeband(app_timeband: pyspark.sql.DataFrame,web_timeband: pyspark.sql.DataFrame,combine_daily: pyspark.sql.DataFrame,sql_agg_timeband: dict,sql_share_timeband: dict):

    # if check_empty_dfs([app_category_agg_daily]):
    #     return get_spark_empty_df()

    # if check_empty_dfs([app_category_web_daily]):
    #     return get_spark_empty_df()


    combine = app_timeband.union(web_timeband)
    logging.info("Union App & Web Complete")

    combine = combine.withColumnRenamed("category_name", "category_name_old")
    combine = combine.withColumn('category_name', f.lower(f.col("category_name_old")))
    combine = combine.drop('category_name_old')

    combine = node_from_config(combine,sql_agg_timeband)

    #-------------------------------- share ----------------------------

    combine_daily = combine_daily.withColumnRenamed("total_visit_count", 'total_visit_count_daily')
    combine_daily = combine_daily.withColumnRenamed("total_visit_duration", 'total_visit_duration_daily')
    combine_daily = combine_daily.withColumnRenamed("total_volume_byte", 'total_volume_byte_daily')
    combine_daily = combine_daily.withColumnRenamed("total_download_byte", 'total_download_byte_daily')
    combine_daily = combine_daily.withColumnRenamed("total_upload_byte", 'total_upload_byte_daily')
    # combine_daily = combine_daily.withColumnRenamed("priority", 'priority_daily')

    logging.info("Dates to run for join time band and daily")
    combine = combine.alias('combine').join(combine_daily.alias('combine_daily'),
        on=[
            combine.subscription_identifier == combine_daily.subscription_identifier ,
            combine.mobile_no == combine_daily.mobile_no  ,
            combine.category_name == combine_daily.category_name,
            combine.event_partition_date == combine_daily.event_partition_date ],
        how="inner",
    )
    logging.info("select column")
    combine = combine.select(
        "combine.subscription_identifier",
        "combine.mobile_no",
        "combine.category_name",
        "combine.total_visit_count",
        "combine.total_visit_duration",
        "combine.total_volume_byte",
        "combine.total_download_byte",
        "combine.total_upload_byte",
        "combine_daily.total_visit_count_daily",
        "combine_daily.total_visit_duration_daily",
        "combine_daily.total_volume_byte_daily",
        "combine_daily.total_download_byte_daily",
        "combine_daily.total_upload_byte_daily",
        "combine.event_partition_date")
    logging.info("Dates to run for share")

    df_return = node_from_config(combine, sql_share_timeband)

    return df_return
    ######################################################################

def _remove_time_dupe_cxense_traffic(df_traffic: pyspark.sql.DataFrame):
    # first grouping by traffic_name, traffic value because they are
    # repeated at identical times with different activetime
    # getting max for the same traffic name and traffic value

    df_traffic = (
        df_traffic.withColumn("activetime", f.col("activetime").cast(IntegerType()))
        .groupBy(
            "mobile_no",
            "hash_id",
            "cx_id",
            "site_id",
            "url",
            "partition_date",
            "time",
            "traffic_name",
            "traffic_value",
        )
        .agg(f.max("activetime").alias("activetime"))
        .withColumn("time_fmtd", f.to_timestamp("time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hour", f.hour("time_fmtd"))
    )
    return df_traffic


def _basic_clean_cxense_traffic(df_traffic_raw: pyspark.sql.DataFrame):
    df_traffic = (
        df_traffic_raw.filter(f.col("url").isNotNull())
        .filter(f.col("site_id").isNotNull())
        .filter(f.col("url") != "")
        .filter(f.col("site_id") != "")
        .filter(f.col("activetime").isNotNull())
        .withColumn("url", f.lower("url"))
        .dropDuplicates()
    )
    return df_traffic


def clean_cxense_traffic(df_traffic_raw: pyspark.sql.DataFrame):
    df_traffic = _basic_clean_cxense_traffic(df_traffic_raw)
    df_traffic = _remove_time_dupe_cxense_traffic(df_traffic)
    return df_traffic


def clean_cxense_content_profile(df_cxense_cp_raw: pyspark.sql.DataFrame):
    df_cp = (
        df_cxense_cp_raw.filter(f.col("url0").isNotNull())
        .filter(f.col("siteid").isNotNull())
        .filter(f.col("content_name").isNotNull())
        .filter(f.col("content_value").isNotNull())
        .filter(f.col("weight").isNotNull())
        .filter(f.col("url0") != "")
        .filter(f.col("siteid") != "")
        .filter(f.col("content_name") != "")
        .filter(f.col("content_value") != "")
        .withColumn("content_value", f.lower("content_value"))
        .withColumn("url0", f.lower("url0")).withColumnRenamed("partition_month", "start_of_month")
        .dropDuplicates()
    )
    return df_cp

def l1_digital_cxense_traffic_clean(
        df_traffic_raw: pyspark.sql.DataFrame,
        # df_cxense_cp_raw: pyspark.sql.DataFrame,
        df_timeband_web: dict
):

    df_traffic = clean_cxense_traffic(df_traffic_raw)

    # Filter Hour
    if (df_timeband_web == "Morning"):
        df_traffic = df_traffic.filter(df_traffic["hour"] >= 6).filter(
            df_traffic["hour"] <= 11)
    elif (df_timeband_web == "Afternoon"):
        df_traffic = df_traffic.filter(df_traffic["hour"] >= 12).filter(
            df_traffic["hour"] <= 17)
    elif (df_timeband_web == "Evening"):
        df_traffic = df_traffic.filter(df_traffic["hour"] >= 18).filter(
            df_traffic["hour"] <= 23)
    else:
        df_traffic = df_traffic.filter(df_traffic["hour"] >= 0).filter(
            df_traffic["hour"] <= 5)

    df_cxense_traffic = df_traffic.withColumn(
        "event_partition_date",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 7, 2)
                 ),
    ).drop(*["partition_date"])

    return df_cxense_traffic


def create_content_profile_mapping(
    df_cp: pyspark.sql.DataFrame, df_cat: pyspark.sql.DataFrame
):
    df_cat = df_cat.filter(f.lower(f.trim(f.col("source_platform"))) == "than")
    df_cp_rank_by_wt = (
        df_cp.filter("content_name = 'ais-categories'")
        .withColumn("category_length", f.size(f.split("content_value", "/")))
        .withColumn(
            "rn",
            f.rank().over(
                Window.partitionBy("siteid", "url0").orderBy(
                    f.desc("weight"),
                    f.desc("category_length"),
                    f.desc("start_of_month"),
                    f.desc("lastfetched"),
                )
            ),
        )
        .filter("rn = 1")
    )

    df_cp_urls_with_multiple_weights = (
        df_cp_rank_by_wt.groupBy("siteid", "url0", "rn")
        .count()
        .filter("count > 1")
        .select("siteid", "url0")
        .distinct()
    )

    df_cp_cleaned = df_cp_rank_by_wt.join(
        df_cp_urls_with_multiple_weights, on=["siteid", "url0"], how="inner"
    )

    df_cp_join_iab = df_cp_cleaned.join(
        df_cat, on=[df_cp_cleaned.content_value == df_cat.argument], how="inner"
    )
    return df_cp_join_iab

def l1_digital_content_profile_mapping(
        df_cp: pyspark.sql.DataFrame, df_cat: pyspark.sql.DataFrame
):
    df_cp_cleaned = create_content_profile_mapping(df_cp, df_cat)
    return df_cp_cleaned

def l1_digital_agg_cxense_traffic(df_traffic_cleaned: pyspark.sql.DataFrame):
    # aggregating url visits activetime, visit counts
    if check_empty_dfs([df_traffic_cleaned]):
        return get_spark_empty_df()

    df_traffic_agg = df_traffic_cleaned.groupBy(
        "mobile_no", "site_id", "url", "event_partition_date"
    ).agg(
        f.sum("activetime").alias("total_visit_duration"),
        f.count("*").alias("total_visit_count"),
    )
    return df_traffic_agg


def get_matched_urls(df_traffic_join_cp_join_iab: pyspark.sql.DataFrame):

    if check_empty_dfs([df_traffic_join_cp_join_iab]):
        return get_spark_empty_df()

    df_traffic_join_cp_matched = df_traffic_join_cp_join_iab.filter(
        (f.col("siteid").isNotNull()) & (f.col("url0").isNotNull())
    ).select("mobile_no",
             "event_partition_date",
             "url",
             "category_name",
             "level_2",
             "level_3",
             "level_4",
             "priority",
             "total_visit_duration",
             "total_visit_count")

    return df_traffic_join_cp_matched


def get_unmatched_urls(df_traffic_join_cp_join_iab: pyspark.sql.DataFrame):

    if check_empty_dfs([df_traffic_join_cp_join_iab]):
        return get_spark_empty_df()

    df_traffic_join_cp_missing = df_traffic_join_cp_join_iab.filter(
        (f.col("siteid").isNull()) | (f.col("url0").isNull())
    )
    return df_traffic_join_cp_missing

def l1_digital_get_matched_and_unmatched_urls(
    df_traffic_agg: pyspark.sql.DataFrame, df_cp_join_iab: pyspark.sql.DataFrame
):
    if check_empty_dfs([df_traffic_agg, df_cp_join_iab]):
        return get_spark_empty_df()

    df_traffic_join_cp_join_iab = df_traffic_agg.join(
        df_cp_join_iab,
        on=[
            (df_traffic_agg.site_id == df_cp_join_iab.siteid)
            & (df_traffic_agg.url == df_cp_join_iab.url0)
        ],
        how="left",
    )
    matched_urls = get_matched_urls(df_traffic_join_cp_join_iab)
    unmatched_urls = get_unmatched_urls(df_traffic_join_cp_join_iab)

    return [matched_urls, unmatched_urls]

def get_cp_category_ais_priorities(df_cp_join_iab: pyspark.sql.DataFrame):
    df_cp_join_iab_join_ais_priority = df_cp_join_iab.withColumn(
        "cat_rank",
        f.rank().over(
            Window.partitionBy("siteid").orderBy(
                f.desc("weight"),
                f.desc("category_length"),
                f.desc("start_of_month"),
                f.desc("lastfetched"),
                f.asc("priority"),
            )
        ),
    ).filter("cat_rank = 1")
    return df_cp_join_iab_join_ais_priority

def l1_digital_get_best_match_for_unmatched_urls(
    df_traffic_join_cp_missing: pyspark.sql.DataFrame,
    df_cp_join_iab: pyspark.sql.DataFrame,
):
    df_cp_join_iab_join_ais_priority = get_cp_category_ais_priorities(df_cp_join_iab)
    df_traffic_get_missing_urls = (
        df_traffic_join_cp_missing.drop(*df_cp_join_iab.columns)
        .join(
            df_cp_join_iab_join_ais_priority,
            on=[
                df_traffic_join_cp_missing.site_id
                == df_cp_join_iab_join_ais_priority.siteid
            ],
            how="inner",
        )
        .drop("siteid").select("mobile_no",
                               "event_partition_date",
                               "url",
                               "category_name",
                               "level_2",
                               "level_3",
                               "level_4",
                               "priority",
                               "total_visit_duration",
                               "total_visit_count")
    )
    return df_traffic_get_missing_urls

def l1_digital_union_matched_and_unmatched_urls(
    customer_profile: pyspark.sql.DataFrame,
    df_traffic_join_cp_matched: pyspark.sql.DataFrame,
    df_traffic_get_missing_urls: pyspark.sql.DataFrame,
):
    df_traffic_join_cp_matched = (
        df_traffic_join_cp_matched.union(df_traffic_get_missing_urls).groupBy("mobile_no",
                                                                              "event_partition_date",
                                                                              "url", "category_name",
                                                                              "priority").agg(
            f.sum("total_visit_duration").alias("total_visit_duration"),
            f.sum("total_visit_count").alias("total_visit_count")
        )
    )

    df_traffic_join_cp_matched = df_traffic_join_cp_matched.join(customer_profile,
                                   on=[df_traffic_join_cp_matched.mobile_no == customer_profile.access_method_num],
                                   how="inner").select(customer_profile.subscription_identifier,
                                                       df_traffic_join_cp_matched.mobile_no,
                                                       df_traffic_join_cp_matched.event_partition_date,
                                                       df_traffic_join_cp_matched.url,
                                                       df_traffic_join_cp_matched.category_name,
                                                       df_traffic_join_cp_matched.priority,
                                                       df_traffic_join_cp_matched.total_visit_duration,
                                                       df_traffic_join_cp_matched.total_visit_count)
    return df_traffic_join_cp_matched

def l1_digital_union_matched_and_unmatched_urls_cat_level(
    customer_profile: pyspark.sql.DataFrame,
    df_traffic_join_cp_matched: pyspark.sql.DataFrame,
    df_traffic_get_missing_urls: pyspark.sql.DataFrame,
    cat_level: dict
):
    df_traffic_join_cp_matched = (
        df_traffic_join_cp_matched.union(df_traffic_get_missing_urls).groupBy("mobile_no",
                                                                              "event_partition_date",
                                                                              "url",
                                                                              cat_level,
                                                                              "priority").agg(
            f.sum("total_visit_duration").alias("total_visit_duration"),
            f.sum("total_visit_count").alias("total_visit_count")
        )
    )

    df_traffic_join_cp_matched = df_traffic_join_cp_matched.withColumnRenamed(cat_level, "category_name")

    df_traffic_join_cp_matched = df_traffic_join_cp_matched.join(customer_profile,
                                   on=[df_traffic_join_cp_matched.mobile_no == customer_profile.access_method_num],
                                   how="inner").select(customer_profile.subscription_identifier,
                                                       df_traffic_join_cp_matched.mobile_no,
                                                       df_traffic_join_cp_matched.event_partition_date,
                                                       df_traffic_join_cp_matched.url,
                                                       df_traffic_join_cp_matched.category_name,
                                                       df_traffic_join_cp_matched.priority,
                                                       df_traffic_join_cp_matched.total_visit_duration,
                                                       df_traffic_join_cp_matched.total_visit_count)

    return df_traffic_join_cp_matched