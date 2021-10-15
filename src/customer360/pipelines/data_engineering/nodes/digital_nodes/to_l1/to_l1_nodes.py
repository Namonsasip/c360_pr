import pyspark.sql.functions as f, logging
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit,monotonically_increasing_id,explode,udf
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
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id,explode,udf
from pyspark.sql.types import StringType
from pyspark import SQLContext
from pyspark.sql import SQLContext
from datetime import date, timedelta
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

    if check_empty_dfs([aib_raw]):
        return get_spark_empty_df()

    if check_empty_dfs([aib_priority_mapping]):
        return get_spark_empty_df()

    aib_clean = aib_raw.filter(f.col("argument").isNotNull()).filter(f.col("argument") != "")
    # P_MAX_DATE = aib_clean.agg({'partition_date': 'max'})
    # aib_clean = aib_clean.filter(aib_clean["partition_date"] == P_MAX_DATE)

    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=[aib_clean.level_1 == aib_priority_mapping.category], how="left"
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

    p_partition = str(os.getenv("RUN_PARTITION", "no_input"))
    if  (p_partition != 'no_input'):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["starttime"][0:8] == p_partition)

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
    #check missing data##
    if check_empty_dfs([Mobile_app_timeband]):
        return get_spark_empty_df()
        
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
    # Mobile_app_daily = Mobile_app_daily.withColumnRenamed("priority", 'priority_daily')
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
        aib_categories_clean: DataFrame
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
        , how="left"
    ).select("subscription_identifier", "mobile_no", "category_name","level_2","level_3","level_4", "priority", "upload_byte", "download_byte", "duration" , "total_byte", "count_trans", mobile_web_daily_raw.partition_date)
    df_mobile_web_daily = df_mobile_web_daily.withColumn("event_partition_date", f.to_date(f.col("partition_date").cast(StringType()), 'yyyy-MM-dd'))

    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("duration", "total_visit_duration")
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("count_trans", "total_visit_count")
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("upload_byte", "total_upload_byte")
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("download_byte", "total_download_byte")
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("total_byte", "total_volume_byte")

    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("category_name", 'category_level_1')
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("level_2", 'category_level_2')
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("level_3", 'category_level_3')
    df_mobile_web_daily = df_mobile_web_daily.withColumnRenamed("level_4", 'category_level_4')

    df_mobile_web_daily = df_mobile_web_daily.select("subscription_identifier","mobile_no", "category_level_1", "category_level_2", "category_level_3", "category_level_4","total_visit_count","total_visit_duration","total_volume_byte","total_download_byte","total_upload_byte","event_partition_date")
    return df_mobile_web_daily

def l1_digital_customer_web_category_agg_union_daily(mobile_web_daily_agg: DataFrame,cxense_daily: DataFrame,cat_level: dict,mobile_web_daily_agg_sql: dict) -> DataFrame:

    if check_empty_dfs([mobile_web_daily_agg,cxense_daily]):
        return get_spark_empty_df()
    logging.info("select category level")
    mobile_web_daily_agg = mobile_web_daily_agg.withColumnRenamed(cat_level, "category_name")
    #---------- select data --------------#
    mobile_web_daily_agg = mobile_web_daily_agg.select("subscription_identifier","mobile_no","category_name","count_trans","duration","total_byte","download_byte","upload_byte","event_partition_date")
    logging.info("select select column")
    cxense_daily = cxense_daily.withColumnRenamed(cat_level, "category_name")
    cxense_daily = cxense_daily.select("subscription_identifier","mobile_no","category_name","count_trans","duration","total_byte","download_byte","upload_byte",cxense_daily.event_partition_date)
    logging.info("union data")
    mobile_web_daily_agg = mobile_web_daily_agg.unionAll(cxense_daily)
    logging.info("sum data")
    mobile_web_daily_agg = mobile_web_daily_agg.withColumnRenamed("duration", "total_visit_duration")
    mobile_web_daily_agg = mobile_web_daily_agg.withColumnRenamed("count_trans", "total_visit_count")
    mobile_web_daily_agg = mobile_web_daily_agg.withColumnRenamed("upload_byte", "total_upload_byte")
    mobile_web_daily_agg = mobile_web_daily_agg.withColumnRenamed("download_byte", "total_download_byte")
    mobile_web_daily_agg = mobile_web_daily_agg.withColumnRenamed("total_byte", "total_volume_byte")
    df_return = node_from_config(mobile_web_daily_agg, mobile_web_daily_agg_sql)
    
    return df_return

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
        mobile_web_hourly_raw.withColumnRenamed("msisdn", "mobile_no").join(aib_categories_clean, on=[
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
    # mobile_web_hourly_raw = mobile_web_hourly_raw.join(union_profile,
    #                                                    on=[
    #                                                        mobile_web_hourly_raw.mobile_no == union_profile.access_method_num],
    #                                                    how="inner").select(
    #                                                    union_profile.subscription_identifier,
    #                                                    mobile_web_hourly_raw.mobile_no,
    #                                                    mobile_web_hourly_raw.category_name,
    #                                                    mobile_web_hourly_raw.priority,
    #                                                    mobile_web_hourly_raw.total_visit_count,
    #                                                    mobile_web_hourly_raw.total_visit_duration,
    #                                                    mobile_web_hourly_raw.total_volume_byte,
    #                                                    mobile_web_hourly_raw.total_download_byte,
    #                                                    mobile_web_hourly_raw.total_upload_byte,
    #                                                    mobile_web_hourly_raw.share_total_visit_count,
    #                                                    mobile_web_hourly_raw.share_total_visit_duration,
    #                                                    mobile_web_hourly_raw.share_total_volume_byte,
    #                                                    mobile_web_hourly_raw.share_total_download_byte,
    #                                                    mobile_web_hourly_raw.share_total_upload_byte,
    #                                                    mobile_web_hourly_raw.event_partition_date)

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
        mobile_web_hourly_raw.withColumnRenamed("msisdn", "mobile_no").join(aib_categories_clean, on=[
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
    # mobile_web_hourly_raw = mobile_web_hourly_raw.join(union_profile,
    #                                                    on=[
    #                                                        mobile_web_hourly_raw.mobile_no == union_profile.access_method_num],
    #                                                    how="inner").select(
    #                                                    union_profile.subscription_identifier,
    #                                                    mobile_web_hourly_raw.mobile_no,
    #                                                    mobile_web_hourly_raw.category_name,
    #                                                    mobile_web_hourly_raw.priority,
    #                                                    mobile_web_hourly_raw.total_visit_count,
    #                                                    mobile_web_hourly_raw.total_visit_duration,
    #                                                    mobile_web_hourly_raw.total_volume_byte,
    #                                                    mobile_web_hourly_raw.total_download_byte,
    #                                                    mobile_web_hourly_raw.total_upload_byte,
    #                                                    mobile_web_hourly_raw.share_total_visit_count,
    #                                                    mobile_web_hourly_raw.share_total_visit_duration,
    #                                                    mobile_web_hourly_raw.share_total_volume_byte,
    #                                                    mobile_web_hourly_raw.share_total_download_byte,
    #                                                    mobile_web_hourly_raw.share_total_upload_byte,
    #                                                    mobile_web_hourly_raw.event_partition_date)

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

    if check_empty_dfs([app_category_web_daily]):
        return get_spark_empty_df()

    app_category_agg_daily = app_category_agg_daily.select("subscription_identifier","mobile_no","category_name","total_visit_count","total_visit_duration","total_volume_byte","total_download_byte","total_upload_byte","event_partition_date")
    app_category_web_daily = app_category_web_daily.select("subscription_identifier","mobile_no","category_name","total_visit_count","total_visit_duration","total_volume_byte","total_download_byte","total_upload_byte","event_partition_date")
    combine = app_category_agg_daily.unionAll(app_category_web_daily)
    logging.info("Union App & Web Complete")

    combine = combine.withColumnRenamed("category_name", "category_name_old")
    combine = combine.withColumn('category_name', f.lower(f.col("category_name_old")))
    combine = combine.drop('category_name_old')
    df_return = node_from_config(combine,combine_app_web_agg_daily)

    return df_return

    ################## combine agg category timeband ###########################
def l1_digital_customer_combine_category_agg_timeband(app_timeband: pyspark.sql.DataFrame,web_timeband: pyspark.sql.DataFrame,combine_daily: pyspark.sql.DataFrame,sql_agg_timeband: dict,sql_share_timeband: dict):

    if check_empty_dfs([app_category_agg_daily]):
        return get_spark_empty_df()

    if check_empty_dfs([app_category_web_daily]):
        return get_spark_empty_df()


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
    )
    return df_traffic


def _basic_clean_cxense_traffic(df_traffic_raw: pyspark.sql.DataFrame):
    df_traffic = (
        df_traffic_raw.filter(f.col("url").isNotNull())
        .filter(f.col("site_id").isNotNull())
        .filter(f.col("url") != "")
        .filter(f.col("site_id") != "")
        .filter(f.col("activetime").isNotNull())
        .withColumn("url", f.col("url"))
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
        .withColumn("content_value", f.col("content_value"))
        .withColumn("url", f.col("url0")).withColumnRenamed("partition_month", "start_of_month")
        .dropDuplicates()
    )
    return df_cp

def l1_digital_cxense_content_profile_int(
df_cxense_cp_raw: pyspark.sql.DataFrame
):
    df_cp = clean_cxense_content_profile(df_cxense_cp_raw)
    return df_cp

def l1_digital_cxense_traffic_clean(
        df_traffic_raw: pyspark.sql.DataFrame,
):

    df_traffic = clean_cxense_traffic(df_traffic_raw)

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
        df_cp_urls_with_multiple_weights, on=["siteid", "url0"], how="left_anti"
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
    ).select("mobile_no",
         "event_partition_date",
         "site_id",
         "url",
         "category_name",
         "level_2",
         "level_3",
         "level_4",
         "priority",
         "total_visit_duration",
         "total_visit_count")

    return df_traffic_join_cp_missing

# def create_content_profile_mapping_unmatch (df_cp: pyspark.sql.DataFrame, df_cat: pyspark.sql.DataFrame):
#     df_cat = df_cat.filter(f.lower(f.trim(f.col("source_platform"))) == "than")
#     df_cp_join_iab = df_cp.join(
#         df_cat, on=[df_cp.content_value == df_cat.argument], how="inner"
#     )
#
#     df_cp_rank_by_wt = (
#     df_cp_join_iab.filter("content_name = 'ais-categories'")
#     .withColumn("category_length", f.size(f.split("content_value", "/")))
#     .withColumn(
#         "cat_rank",
#         f.rank().over(
#             Window.partitionBy("siteid").orderBy(
#                 f.desc("weight"),
#                 f.desc("category_length"),
#                 f.desc("start_of_month"),
#                 f.desc("lastfetched"),
#                 f.asc("priority"),
#             )
#         ),
#     ).filter("cat_rank = 1"))
#
#     return df_cp_rank_by_wt
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

def l1_digital_get_matched_and_unmatched_urls(
    cxense_agg_daily: pyspark.sql.DataFrame, iab_content: pyspark.sql.DataFrame
):
    if check_empty_dfs([cxense_agg_daily, iab_content]):
        return get_spark_empty_df(2)

    spark = get_spark_session()
    cxense_agg_daily.createOrReplaceTempView("df_traffic_agg")
    iab_content.createOrReplaceTempView("df_cp_join_iab")

    df_traffic_join_cp_join_iab = spark.sql("""
    select 
    mobile_no,
    event_partition_date,
    b.url0,
    b.siteid,
    a.site_id,
    a.url,
    category_name,
    level_2,
    level_3,
    level_4,
    priority,
    total_visit_duration,
    total_visit_count
    from df_traffic_agg a
    left join df_cp_join_iab b
    on a.site_id = b.siteid
    and a.url = b.url0""")

    df_traffic_join_cp_join_iab.createOrReplaceTempView("df_traffic_join_cp_join_iab")
    # df_traffic_join_cp_join_iab = df_traffic_agg.join(
    #     df_cp_join_iab,
    #     on=[
    #         (df_traffic_agg.site_id == df_cp_join_iab.siteid)
    #         & (df_traffic_agg.url == df_cp_join_iab.url0)
    #     ],
    #     how="left",
    # )

    #
    # matched_urls = get_matched_urls(df_traffic_join_cp_join_iab)
    # unmatched_urls = get_unmatched_urls(df_traffic_join_cp_join_iab)

    matched_urls = spark.sql("""select * from df_traffic_join_cp_join_iab where siteid is not null and url0 is not null""")
    # matched_urls = df_traffic_join_cp_join_iab.filter(
    #     (f.col("siteid").isNotNull()) & (f.col("url0").isNotNull())
    # )


    # if check_empty_dfs([matched_urls]):
    #     return get_spark_empty_df()

    unmatched_urls = spark.sql("""select * from df_traffic_join_cp_join_iab where siteid is null and url0 is null""")
    # unmatched_urls = df_traffic_join_cp_join_iab.filter(
    #     (f.col("siteid").isNull()) | (f.col("url0").isNull())
    # )

    unmatched_urls.createOrReplaceTempView("unmatched_urls")
    df_cp_join_iab_join_ais_priority = get_cp_category_ais_priorities(iab_content)
    df_cp_join_iab_join_ais_priority.createOrReplaceTempView("df_cp_join_iab_join_ais_priority")

    df_traffic_get_missing_urls = spark.sql("""select
       a.mobile_no,
       a.event_partition_date,
       a.url,
       a.category_name,
       a.level_2,
       a.level_3,
       a.level_4,
       a.priority,
       a.total_visit_duration,
       a.total_visit_count
       from unmatched_urls a
       left join df_cp_join_iab_join_ais_priority b
       on a.site_id = b.siteid
       """)

    return [matched_urls, df_traffic_get_missing_urls]

def l1_digital_get_best_match_for_unmatched_urls(
    df_traffic_join_cp_missing: pyspark.sql.DataFrame,
    df_cp_join_iab: pyspark.sql.DataFrame,
):
    spark = get_spark_session()
    df_traffic_join_cp_missing.createOrReplaceTempView("df_traffic_join_cp_missing")
    df_cp_join_iab_join_ais_priority = get_cp_category_ais_priorities(df_cp_join_iab)
    df_cp_join_iab_join_ais_priority.createOrReplaceTempView("df_cp_join_iab_join_ais_priority")

    df_traffic_get_missing_urls = spark.sql("""select
    mobile_no,
    event_partition_date,
    url,
    category_name,
    level_2,
    level_3,
    level_4,
    priority,
    total_visit_duration,
    total_visit_count
    from df_traffic_join_cp_missing a
    left join df_cp_join_iab_join_ais_priority
    inner a.site_id = b.siteid
    """)

    # df_traffic_get_missing_urls = (
    #     df_traffic_join_cp_missing.drop(*df_cp_join_iab.columns)
    #     .join(
    #         df_cp_join_iab_join_ais_priority,
    #         on=[
    #             df_traffic_join_cp_missing.site_id
    #             == df_cp_join_iab_join_ais_priority.siteid
    #         ],
    #         how="inner",
    #     )
    #     .drop("siteid").select("mobile_no",
    #      "event_partition_date",
    #      "url",
    #      "category_name",
    #      "level_2",
    #      "level_3",
    #      "level_4",
    #      "priority",
    #      "total_visit_duration",
    #      "total_visit_count")
    # )
    return df_traffic_get_missing_urls

def l1_digital_union_matched_and_unmatched_urls(
    cxense_agg_daily: pyspark.sql.DataFrame,
    iab_content: pyspark.sql.DataFrame,
    customer_profile: pyspark.sql.DataFrame
):

    spark = get_spark_session()
    cxense_agg_daily.createOrReplaceTempView("df_traffic_agg")
    iab_content.createOrReplaceTempView("df_cp_join_iab")

    df_traffic_join_cp_join_iab = spark.sql("""
        select 
        mobile_no,
        event_partition_date,
        b.url0,
        b.siteid,
        a.site_id,
        a.url,
        category_name,
        level_2,
        level_3,
        level_4,
        priority,
        total_visit_duration,
        total_visit_count
        from df_traffic_agg a
        left join df_cp_join_iab b
        on a.site_id = b.siteid
        and a.url = b.url0""")

    df_traffic_join_cp_join_iab.createOrReplaceTempView("df_traffic_join_cp_join_iab")

    matched_urls = spark.sql("""select * from df_traffic_join_cp_join_iab where siteid is not null and url0 is not null""")

    unmatched_urls = spark.sql("""select * from df_traffic_join_cp_join_iab where siteid is null and url0 is null""")

    unmatched_urls.createOrReplaceTempView("unmatched_urls")
    df_cp_join_iab_join_ais_priority = get_cp_category_ais_priorities(iab_content)
    df_cp_join_iab_join_ais_priority.createOrReplaceTempView("df_cp_join_iab_join_ais_priority")

    df_traffic_get_missing_urls = spark.sql("""select
           a.mobile_no,
           a.event_partition_date,
           a.url,
           a.category_name,
           a.level_2,
           a.level_3,
           a.level_4,
           a.priority,
           a.total_visit_duration,
           a.total_visit_count
           from unmatched_urls a
           left join df_cp_join_iab_join_ais_priority b
           on a.site_id = b.siteid
           """)

    matched_urls = matched_urls.groupBy("mobile_no", "event_partition_date", "url",
                                                                      "category_name", "priority").agg(
            f.sum("total_visit_duration").alias("total_visit_duration"),
            f.sum("total_visit_count").alias("total_visit_count")
    )
    df_traffic_get_missing_urls = df_traffic_get_missing_urls.groupBy("mobile_no", "event_partition_date",
                                                                    "url", "category_name",
                                                                    "priority").agg(
            f.sum("total_visit_duration").alias("total_visit_duration"),
            f.sum("total_visit_count").alias("total_visit_count"))

    df_traffic_join_cp_union = matched_urls.unionAll(df_traffic_get_missing_urls).distinct()
    df_traffic_join_cp_union = df_traffic_join_cp_union.groupBy("mobile_no", "event_partition_date",
                                                                      "url", "category_name",
                                                                      "priority").agg(
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_visit_count").alias("total_visit_count"))
    df_traffic_join_cp_union = df_traffic_join_cp_union.join(customer_profile,
                                                                     on=[
                                                                         df_traffic_join_cp_union.mobile_no == customer_profile.access_method_num],
                                                                     how="inner").select(
            customer_profile.subscription_identifier,
            df_traffic_join_cp_union.mobile_no,
            df_traffic_join_cp_union.event_partition_date,
            df_traffic_join_cp_union.url,
            df_traffic_join_cp_union.category_name,
            df_traffic_join_cp_union.priority,
            df_traffic_join_cp_union.total_visit_duration,
            df_traffic_join_cp_union.total_visit_count)


    return df_traffic_join_cp_union

def l1_digital_union_matched_and_unmatched_urls_non_site_id(
    customer_profile: pyspark.sql.DataFrame,
    df_traffic_get_missing_urls: pyspark.sql.DataFrame,
):
    df_traffic_get_missing_urls = df_traffic_get_missing_urls.groupBy("mobile_no", "event_partition_date", "url",
                                                                      "category_name", "priority").agg(
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_visit_count").alias("total_visit_count")
    )

    df_traffic_get_missing_urls = df_traffic_get_missing_urls.join(customer_profile,
                                   on=[df_traffic_get_missing_urls.mobile_no == customer_profile.access_method_num],
                                   how="inner").select(customer_profile.subscription_identifier,
                                                       df_traffic_get_missing_urls.mobile_no,
                                                       df_traffic_get_missing_urls.event_partition_date,
                                                       df_traffic_get_missing_urls.url,
                                                       df_traffic_get_missing_urls.category_name,
                                                       df_traffic_get_missing_urls.priority,
                                                       df_traffic_get_missing_urls.total_visit_duration,
                                                       df_traffic_get_missing_urls.total_visit_count)
    return df_traffic_get_missing_urls

def l1_digital_union_matched_and_unmatched_urls_cat_level(
    cxense_agg_daily: pyspark.sql.DataFrame,
    iab_content: pyspark.sql.DataFrame,
    customer_profile: pyspark.sql.DataFrame,
    cat_level: dict
):
    spark = get_spark_session()
    cxense_agg_daily.createOrReplaceTempView("df_traffic_agg")
    iab_content.createOrReplaceTempView("df_cp_join_iab")

    df_traffic_join_cp_join_iab = spark.sql("""
                select 
                mobile_no,
                event_partition_date,
                b.url0,
                b.siteid,
                a.site_id,
                a.url,
                category_name,
                level_2,
                level_3,
                level_4,
                priority,
                total_visit_duration,
                total_visit_count
                from df_traffic_agg a
                left join df_cp_join_iab b
                on a.site_id = b.siteid
                and a.url = b.url0""")

    df_traffic_join_cp_join_iab.createOrReplaceTempView("df_traffic_join_cp_join_iab")

    matched_urls = spark.sql(
        """select * from df_traffic_join_cp_join_iab where siteid is not null and url0 is not null""")

    unmatched_urls = spark.sql("""select * from df_traffic_join_cp_join_iab where siteid is null and url0 is null""")

    unmatched_urls.createOrReplaceTempView("unmatched_urls")
    df_cp_join_iab_join_ais_priority = get_cp_category_ais_priorities(iab_content)
    df_cp_join_iab_join_ais_priority.createOrReplaceTempView("df_cp_join_iab_join_ais_priority")

    df_traffic_get_missing_urls = spark.sql("""select
                   a.mobile_no,
                   a.event_partition_date,
                   a.url,
                   a.category_name,
                   a.level_2,
                   a.level_3,
                   a.level_4,
                   a.priority,
                   a.total_visit_duration,
                   a.total_visit_count
                   from unmatched_urls a
                   left join df_cp_join_iab_join_ais_priority b
                   on a.site_id = b.siteid
                   """)

    matched_urls = matched_urls.groupBy("mobile_no", "event_partition_date", "url",
                                        cat_level, "priority").agg(
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_visit_count").alias("total_visit_count")
    )
    df_traffic_get_missing_urls = df_traffic_get_missing_urls.groupBy("mobile_no", "event_partition_date",
                                                                      "url", cat_level,
                                                                      "priority").agg(
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_visit_count").alias("total_visit_count"))

    df_traffic_join_cp_union = matched_urls.union(df_traffic_get_missing_urls).distinct()

    df_traffic_join_cp_union = df_traffic_join_cp_union.withColumnRenamed(cat_level, "category_name")

    df_traffic_join_cp_union = df_traffic_join_cp_union.join(customer_profile,
                                                             on=[
                                                                 df_traffic_join_cp_union.mobile_no == customer_profile.access_method_num],
                                                             how="inner").select(
        customer_profile.subscription_identifier,
        df_traffic_join_cp_union.mobile_no,
        df_traffic_join_cp_union.event_partition_date,
        df_traffic_join_cp_union.url,
        df_traffic_join_cp_union.category_name,
        df_traffic_join_cp_union.priority,
        df_traffic_join_cp_union.total_visit_duration,
        df_traffic_join_cp_union.total_visit_count)

        # df_traffic_get_missing_urls = df_traffic_get_missing_urls.groupBy("mobile_no", "event_partition_date", "url",
    #                                                                   cat_level, "priority").agg(
    #     f.sum("total_visit_duration").alias("total_visit_duration"),
    #     f.sum("total_visit_count").alias("total_visit_count")
    # )
    # df_traffic_join_cp_matched = df_traffic_join_cp_matched.groupBy("mobile_no", "event_partition_date",
    #                                                                 "url", cat_level,
    #                                                                 "priority").agg(
    #     f.sum("total_visit_duration").alias("total_visit_duration"),
    #     f.sum("total_visit_count").alias("total_visit_count")
    # )
    #
        # df_traffic_join_cp_matched = df_traffic_join_cp_matched.withColumnRenamed(cat_level, "category_name")
        #
        # df_traffic_join_cp_matched = df_traffic_join_cp_matched.union(df_traffic_get_missing_urls).distinct()
        #
        # df_traffic_join_cp_matched = df_traffic_join_cp_matched.join(customer_profile,
        #                            on=[df_traffic_join_cp_matched.mobile_no == customer_profile.access_method_num],
        #                            how="inner").select(customer_profile.subscription_identifier,
        #                                                df_traffic_join_cp_matched.mobile_no,
        #                                                df_traffic_join_cp_matched.event_partition_date,
        #                                                df_traffic_join_cp_matched.url,
        #                                                df_traffic_join_cp_matched.category_name,
        #                                                df_traffic_join_cp_matched.priority,
        #                                                df_traffic_join_cp_matched.total_visit_duration,
        #                                                df_traffic_join_cp_matched.total_visit_count)

    return df_traffic_join_cp_union

def l1_digital_union_matched_and_unmatched_urls_non_site_id_cat_level(
    customer_profile: pyspark.sql.DataFrame,
    df_traffic_get_missing_urls: pyspark.sql.DataFrame,
    cat_level: dict
):
    df_traffic_get_missing_urls = df_traffic_get_missing_urls.groupBy("mobile_no", "event_partition_date", "url",
                                                                      cat_level, "priority").agg(
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_visit_count").alias("total_visit_count")
    )

    df_traffic_get_missing_urls = df_traffic_get_missing_urls.withColumnRenamed(cat_level, "category_name")

    df_traffic_get_missing_urls = df_traffic_get_missing_urls.join(customer_profile,
                                   on=[df_traffic_get_missing_urls.mobile_no == customer_profile.access_method_num],
                                   how="inner").select(customer_profile.subscription_identifier,
                                                       df_traffic_get_missing_urls.mobile_no,
                                                       df_traffic_get_missing_urls.event_partition_date,
                                                       df_traffic_get_missing_urls.url,
                                                       df_traffic_get_missing_urls.category_name,
                                                       df_traffic_get_missing_urls.priority,
                                                       df_traffic_get_missing_urls.total_visit_duration,
                                                       df_traffic_get_missing_urls.total_visit_count)

    return df_traffic_get_missing_urls

def digital_cxense_traffic_mapping_subscription_identifier(
        traffic: DataFrame,customer_profile_key: DataFrame
):
    if check_empty_dfs([traffic, customer_profile_key]):
        return get_spark_empty_df()

    # traffic = traffic.withColumn(
    #     "event_partition_date",
    #     f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
    #              f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-"),
    #              f.substring(f.col("partition_date").cast("string"), 7, 2)
    #              ),
    # ).drop(*["partition_date"])

    traffic = traffic.join(customer_profile_key,
        on=[traffic.mobile_no == customer_profile_key.access_method_num, traffic.event_partition_date == customer_profile_key.event_partition_date],
        how="left",).select(customer_profile_key.subscription_identifier
                                                      ,traffic.mobile_no
                                                      ,traffic.hash_id
                                                      ,traffic.cx_id
                                                      ,traffic.site_id
                                                      ,traffic.activetime
                                                      ,traffic.adspace
                                                      ,traffic.browser
                                                      ,traffic.browsertimezone
                                                      ,traffic.browserversion
                                                      ,traffic.capabilities
                                                      ,traffic.city
                                                      ,traffic.colordepth
                                                      ,traffic.company
                                                      ,traffic.connectionspeed
                                                      ,traffic.country
                                                      ,traffic.devicetype
                                                      ,traffic.exitlinkhost
                                                      ,traffic.exitlinkurl
                                                      ,traffic.host
                                                      ,traffic.intents
                                                      ,traffic.isoregion
                                                      ,traffic.metrocode
                                                      ,traffic.mobilebrand
                                                      ,traffic.os
                                                      ,traffic.postalcode
                                                      ,traffic.query
                                                      ,traffic.referrerhost
                                                      ,traffic.referrerhostclass
                                                      ,traffic.referrerquery
                                                      ,traffic.referrersearchengine
                                                      ,traffic.referrersocialnetwork
                                                      ,traffic.referrerurl
                                                      ,traffic.region
                                                      ,traffic.resolution
                                                      ,traffic.retargetingparameters
                                                      ,traffic.scrolldepth
                                                      ,traffic.sessionbounce
                                                      ,traffic.sessionstart
                                                      ,traffic.sessionstop
                                                      ,traffic.site
                                                      ,traffic.start
                                                      ,traffic.stop
                                                      ,traffic.time
                                                      ,traffic.traffic_name
                                                      ,traffic.traffic_value
                                                      ,traffic.url
                                                      ,traffic.usercorrelationid
                                                      ,traffic.userparameters
                                                      ,traffic.event_partition_date)

    return traffic

def digital_customer_web_network_company_usage_hourly(
    df_traffic:pyspark.sql.DataFrame,
):
    df_traffic = df_traffic.select("subscription_identifier",
                                   "mobile_no",
                                   "time",
                                   "company",
                                   "connectionspeed",
                                   "event_partition_date").dropDuplicates()

    df_traffic = df_traffic.where("connectionspeed IN ('mobile','broadband')")
    df_traffic = df_traffic.filter((f.col("mobile_no").isNotNull()) & (f.col("subscription_identifier").isNotNull()))


    # rename/add column
    df_traffic = df_traffic.withColumnRenamed("company", "network_company")
    df_traffic = df_traffic.withColumnRenamed("connectionspeed", "network_type")
    df_traffic = df_traffic.withColumn("hour", f.substring(f.col("time").cast("string"), 12, 2))

    # timeband
    df_traffic = df_traffic.withColumn("timeband",when(f.col("hour").between(6, 11), "Morning").when(f.col("hour").between(12, 17),"Afternoon").when(f.col("hour").between(18, 23),"Evening").otherwise("Night"))

    # column flag
    df_traffic = df_traffic.withColumn("ais_sim_flag", when((f.col("network_type") == "mobile") & ((f.col("network_company") == "ais 3g4g") | (f.col("network_company") == "ais mobile")), 1).otherwise(0))
    df_traffic = df_traffic.withColumn("ais_broadband_flag", when((f.col("network_type") == "broadband") & (f.col("network_company") == "ais fibre"), 1).otherwise(0))

    df_traffic = df_traffic.withColumn("competitor_sim_flag", when((f.col("network_type") == "mobile") & (~((f.col("network_company") == "ais 3g4g") | (f.col("network_company") == "ais mobile")) | f.col("network_company").isNull()), 1).otherwise(0))
    df_traffic = df_traffic.withColumn("competitor_broadband_flag", when((f.col("network_type") == "broadband") & (~(f.col("network_company") == "ais fibre") | (f.col("network_company").isNull())), 1).otherwise(0))

    customer_web_network_company_usage_hourly = df_traffic.select("subscription_identifier","mobile_no","hour","timeband","network_company","network_type","ais_sim_flag","ais_broadband_flag","competitor_sim_flag","competitor_broadband_flag","event_partition_date")\
        .groupby("subscription_identifier","mobile_no","hour","timeband","network_company","network_type","ais_sim_flag","ais_broadband_flag","competitor_sim_flag","competitor_broadband_flag","event_partition_date").count()
    customer_web_network_company_usage_hourly = customer_web_network_company_usage_hourly.drop('count')

    return customer_web_network_company_usage_hourly

def digital_customer_multi_company_sim_daily(
    customer_web_network_company:pyspark.sql.DataFrame, sum_flag: Dict[str, Any],
):
    customer_multi_company_sim = node_from_config(customer_web_network_company,sum_flag)

    customer_multi_company_sim = customer_multi_company_sim.withColumn("multi_company_sim_flag", when((f.col("competitor_sim_flag") != 0), "Y").otherwise("N"))
    customer_multi_company_sim = customer_multi_company_sim.withColumn("multi_company_broadband_flag", when((f.col("competitor_broadband_flag") != 0), "Y").otherwise("N"))

    customer_multi_company_sim = customer_multi_company_sim.select("subscription_identifier","mobile_no", "multi_company_sim_flag", "multi_company_broadband_flag","event_partition_date")

    return customer_multi_company_sim
#################### CXEN daily ##############################
def digital_customer_cxense_master( cxense_content_profile_master:pyspark.sql.DataFrame,aib_categories:pyspark.sql.DataFrame,category_priority:pyspark.sql.DataFrame):
    spark = get_spark_session()
    #--------get max date IAB ----------------
    P_MAX_DATE = aib_categories.select(f.max(f.col("partition_date")).alias("max_date"))
    mobile_app_daily = mobile_app_daily.where(f.col("partition_date") == P_MAX_DATE)
    aib_categories.createOrReplaceTempView("l1_digital_aib_categories_clean")

    cxense_content_profile_master.createOrReplaceTempView("cxense_content_profile")
    category_priority.createOrReplaceTempView("aib_category_priority")
    master = spark.sql("""
        select site_url,content_value,category_level_1,category_level_2,category_level_3,category_level_4
        from(
        SELECT site_url,content_value,weight,category_level_1,category_level_2,category_level_3,category_level_4,ROW_NUMBER() OVER(PARTITION BY site_url ORDER BY  weight desc,priority asc) as CT
        from (
        select site_url,content_value,weight
        from cxense_content_profile
        where content_value is not null 
        group by site_url,content_value,weight
        ) Z
        left join (
        SELECT * 
        FROM l1_digital_aib_categories_clean iab
        LEFT JOIN aib_category_priority priority
        on iab.argument = priority.category
        ) B
        on Z.content_value = B.argument
        ) A
        where CT = 1
    """)
    return master

def digital_customer_cxense_agg_daily( cxen_traffic:pyspark.sql.DataFrame,cxen_master:pyspark.sql.DataFrame,customer_profile:pyspark.sql.DataFrame):
    if check_empty_dfs([cxen_traffic, cxen_master,customer_profile]):
        return get_spark_empty_df()
    #-------- Sum ---------#
    cxen_traffic = cxen_traffic.groupBy("subscription_identifier","mobile_no", "url", "event_partition_date").agg(f.sum("activetime").alias("duration"),f.count("*").alias("count_trans"))
    # cxen_traffic = cxen_traffic.withColumn("event_partition_date",
    #     f.concat(
    #         f.substring(f.col("partition_date").cast("string"), 1, 4),
    #         f.lit("-"),
    #         f.substring(f.col("partition_date").cast("string"), 5, 2),
    #         f.lit("-"),
    #         f.substring(f.col("partition_date").cast("string"), 7, 2)
    #         )).drop(*["partition_date"])
    #-------- Join Master ---------#
    cxen_traffic = cxen_traffic.join(cxen_master,on=[cxen_traffic.url == cxen_master.site_url],how="left")
    #-------- rename category ---------#
    cxen_traffic = cxen_traffic.withColumnRenamed("level_1", 'category_level_1')
    cxen_traffic = cxen_traffic.withColumnRenamed("level_2", 'category_level_2')
    cxen_traffic = cxen_traffic.withColumnRenamed("level_3", 'category_level_3')
    cxen_traffic = cxen_traffic.withColumnRenamed("level_4", 'category_level_4')
    cxen_traffic = cxen_traffic.select("subscription_identifier","mobile_no", "url", "category_level_1", "category_level_2", "category_level_3", "category_level_4", "count_trans","duration","event_partition_date")
    #-------- Join Profile ---------#
    # cxen_traffic = cxen_traffic.join(customer_profile,on=[cxen_traffic.mobile_no == customer_profile.access_method_num,cxen_traffic.event_partition_date == customer_profile.event_partition_date],how="left")
    #-------- select column ---------#
    cxen_traffic = cxen_traffic.withColumn("upload_byte", f.lit(0).cast(LongType()))
    cxen_traffic = cxen_traffic.withColumn("download_byte", f.lit(0).cast(LongType()))
    cxen_traffic = cxen_traffic.withColumn("total_byte", f.lit(0).cast(LongType()))
    cxen_traffic = cxen_traffic.select("subscription_identifier", "mobile_no", "url", "category_level_1",
                                       "category_level_2", "category_level_3", "category_level_4", "count_trans",
                                       "duration", "upload_byte", "download_byte", "total_byte", "event_partition_date")
    cxen_traffic = cxen_traffic.filter(f.col("mobile_no").isNotNull())
    cxen_traffic = cxen_traffic.filter(f.col("url").isNotNull())

    return cxen_traffic

def digital_cxense_traffic_json(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-08-27"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    """)

    return df_cxense_user_traffic

# def digital_cxense_traffic_json(
#     traffic_json: pyspark.sql.DataFrame
# ):
#     spark = get_spark_session()
#     #location run & path data
#     running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
#     if running_environment == "on_cloud":
#         path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
#         path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
#     else:
#         path_json = "hdfs://10.237.82.9:8020/C360/ONLINE/source_online_traffic/"
#         path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
#     ##### TEST on Cloud
#     path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/20210720/"
#     lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
#     #read meta data
#     metadata_table = spark.read.parquet(metadata_table_path)
#     metadata_table.createOrReplaceTempView("mdtl")
#     target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))
#
#     #read Json
#     df_json = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")
#     df_json.show()
#     return 0

def digital_cxense_hash_id_key_mapping(cxense_hash_id: pyspark.sql.DataFrame, key_mapping: pyspark.sql.DataFrame):
    if check_empty_dfs([cxense_hash_id, key_mapping]):
        return get_spark_empty_df()
    spark = get_spark_session()
    # -------- max date key_mapping---------#
    # key_mapping

     # -------- distinct(hash_id) ---------#
    # cxense_hash_id = cxense_hash_id.select(distinct(hash_id), cx_id)
    cxense_hash_id.createOrReplaceTempView('cxense_hash_id')
    cxense_hash_id = spark.sql("""select distinct(hash_id), cx_id from cxense_hash_id""")

    # -------- Join ---------#
    cxense_hash_id_key_mapping = cxense_hash_id.join(key_mapping, on=[cxense_hash_id.hash_id == key_mapping.id_2],
                                                     how="inner")
    cxense_hash_id_key_mapping = cxense_hash_id_key_mapping.select("hash_id", "cx_id", "id_1", "id_2")
    cxense_hash_id_key_mapping = cxense_hash_id_key_mapping.withColumnRenamed("id_1", "mobile_no")

    return cxense_hash_id_key_mapping

#####################################################################################
def digital_cxense_traffic_json_28(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-08-28"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    # customer_profile_key.createOrReplaceTempView('customer_profile_key')
    # master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    # df_cxense_user_traffic = spark.sql("""
    # select
    # b.subscription_identifier
    # , a.mobile_no
    # , a.hash_id
    # , a.cx_id
    # , a.site_id
    # , a.activetime
    # , a.adspace
    # , a.browser
    # , a.browsertimezone
    # , a.browserversion
    # , a.capabilities
    # , a.city
    # , a.colordepth
    # , a.company
    # , a.connectionspeed
    # , a.country
    # , a.devicetype
    # , a.exitlinkhost
    # , a.exitlinkurl
    # , a.host
    # , a.intents
    # , a.isoregion
    # , a.metrocode
    # , a.mobilebrand
    # , a.os
    # , a.postalcode
    # , a.query
    # , a.referrerhost
    # , a.referrerhostclass
    # , a.referrerquery
    # , a.referrersearchengine
    # , a.referrersocialnetwork
    # , a.referrerurl
    # , a.region
    # , a.resolution
    # , a.retargetingparameters
    # , a.scrolldepth
    # , a.sessionbounce
    # , a.sessionstart
    # , a.sessionstop
    # , a.site
    # , a.start
    # , a.stop
    # , a.time
    # , a.traffic_name
    # , a.traffic_value
    # , a.url
    # , c.level_1 as category_level_1
    # , c.level_2 as category_level_2
    # , c.level_3 as category_level_3
    # , c.level_4 as category_level_4
    # , a.usercorrelationid
    # , a.userparameters
    # , a.event_partition_date
    # from df_cxense_traffic_cast a
    # left join customer_profile_key b
    # on a.mobile_no = b.access_method_num
    # and a.event_partition_date = b.event_partition_date
    # left join master_cxense c
    # on a.url = c.site_url
    # """)

    return df_cxense_traffic_cast

def digital_cxense_traffic_json_29(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-08-29"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    left join customer_profile_key c
    on b.mobile_no = c.access_method_num
    and a.event_partition_date = c.event_partition_date
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    # df_cxense_user_traffic = spark.sql("""
    # select
    # b.subscription_identifier
    # , a.mobile_no
    # , a.hash_id
    # , a.cx_id
    # , a.site_id
    # , a.activetime
    # , a.adspace
    # , a.browser
    # , a.browsertimezone
    # , a.browserversion
    # , a.capabilities
    # , a.city
    # , a.colordepth
    # , a.company
    # , a.connectionspeed
    # , a.country
    # , a.devicetype
    # , a.exitlinkhost
    # , a.exitlinkurl
    # , a.host
    # , a.intents
    # , a.isoregion
    # , a.metrocode
    # , a.mobilebrand
    # , a.os
    # , a.postalcode
    # , a.query
    # , a.referrerhost
    # , a.referrerhostclass
    # , a.referrerquery
    # , a.referrersearchengine
    # , a.referrersocialnetwork
    # , a.referrerurl
    # , a.region
    # , a.resolution
    # , a.retargetingparameters
    # , a.scrolldepth
    # , a.sessionbounce
    # , a.sessionstart
    # , a.sessionstop
    # , a.site
    # , a.start
    # , a.stop
    # , a.time
    # , a.traffic_name
    # , a.traffic_value
    # , a.url
    # , c.level_1 as category_level_1
    # , c.level_2 as category_level_2
    # , c.level_3 as category_level_3
    # , c.level_4 as category_level_4
    # , a.usercorrelationid
    # , a.userparameters
    # , a.event_partition_date
    # from df_cxense_traffic_cast a
    # left join customer_profile_key b
    # on a.mobile_no = b.access_method_num
    # and a.event_partition_date = b.event_partition_date
    # left join master_cxense c
    # on a.url = c.site_url
    # """)

    return df_cxense_traffic_cast

def digital_cxense_traffic_json_30(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-10-12"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    # customer_profile_key.createOrReplaceTempView('customer_profile_key')
    # master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activetime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browsertimezone
    ,cast(a.browserVersion as string) as browserversion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colordepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionspeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as devicetype
    ,cast(a.exitLinkHost as string) as exitlinkhost
    ,cast(a.exitLinkUrl as string) as exitlinkurl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoregion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobilebrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalcode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerhost
    ,cast(a.referrerHostClass as string) as referrerhostclass
    ,cast(a.referrerQuery as string) as referrerquery
    ,cast(a.referrerSearchEngine as string) as referrersearchengine
    ,cast(a.referrerSocialNetwork as string) as referrersocialnetwork
    ,cast(a.referrerUrl as string) as referrerurl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingparameters
    ,cast(a.scrollDepth as double) as scrolldepth
    ,cast(a.sessionBounce as boolean) as sessionbounce
    ,cast(a.sessionStart as boolean) as sessionstart
    ,cast(a.sessionStop as boolean) as sessionstop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as usercorrelationid
    ,cast(a.userParameters as string) as userparameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    # df_cxense_user_traffic = spark.sql("""
    # select
    # b.subscription_identifier
    # , a.mobile_no
    # , a.hash_id
    # , a.cx_id
    # , a.site_id
    # , a.activetime
    # , a.adspace
    # , a.browser
    # , a.browsertimezone
    # , a.browserversion
    # , a.capabilities
    # , a.city
    # , a.colordepth
    # , a.company
    # , a.connectionspeed
    # , a.country
    # , a.devicetype
    # , a.exitlinkhost
    # , a.exitlinkurl
    # , a.host
    # , a.intents
    # , a.isoregion
    # , a.metrocode
    # , a.mobilebrand
    # , a.os
    # , a.postalcode
    # , a.query
    # , a.referrerhost
    # , a.referrerhostclass
    # , a.referrerquery
    # , a.referrersearchengine
    # , a.referrersocialnetwork
    # , a.referrerurl
    # , a.region
    # , a.resolution
    # , a.retargetingparameters
    # , a.scrolldepth
    # , a.sessionbounce
    # , a.sessionstart
    # , a.sessionstop
    # , a.site
    # , a.start
    # , a.stop
    # , a.time
    # , a.traffic_name
    # , a.traffic_value
    # , a.url
    # , c.level_1 as category_level_1
    # , c.level_2 as category_level_2
    # , c.level_3 as category_level_3
    # , c.level_4 as category_level_4
    # , a.usercorrelationid
    # , a.userparameters
    # , a.event_partition_date
    # from df_cxense_traffic_cast a
    # left join customer_profile_key b
    # on a.mobile_no = b.access_method_num
    # and a.event_partition_date = b.event_partition_date
    # left join master_cxense c
    # on a.url = c.site_url
    # """)

    return df_cxense_traffic_cast

def digital_cxense_traffic_json_1(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-01"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_2(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-02"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_3(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-03"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_4(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-04"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_5(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-05"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_6(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-06"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_7(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-07"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    , a.mobile_no
    , a.hash_id
    , a.cx_id
    , a.site_id
    , a.activetime
    , a.adspace
    , a.browser
    , a.browsertimezone
    , a.browserversion
    , a.capabilities
    , a.city
    , a.colordepth
    , a.company
    , a.connectionspeed
    , a.country
    , a.devicetype
    , a.exitlinkhost
    , a.exitlinkurl
    , a.host
    , a.intents
    , a.isoregion
    , a.metrocode
    , a.mobilebrand
    , a.os
    , a.postalcode
    , a.query
    , a.referrerhost
    , a.referrerhostclass
    , a.referrerquery
    , a.referrersearchengine
    , a.referrersocialnetwork
    , a.referrerurl
    , a.region
    , a.resolution
    , a.retargetingparameters
    , a.scrolldepth
    , a.sessionbounce
    , a.sessionstart
    , a.sessionstop
    , a.site
    , a.start
    , a.stop
    , a.time
    , a.traffic_name
    , a.traffic_value
    , a.url
    , c.level_1 as category_level_1
    , c.level_2 as category_level_2
    , c.level_3 as category_level_3
    , c.level_4 as category_level_4
    , a.usercorrelationid
    , a.userparameters
    , a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)

    return df_cxense_user_traffic

def digital_cxense_traffic_json_8(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date=2021-09-24"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    customer_profile_key.createOrReplaceTempView('customer_profile_key')
    master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activeTime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browserTimezone
    ,cast(a.browserVersion as string) as browserVersion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colorDepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionSpeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as deviceType
    ,cast(a.exitLinkHost as string) as exitLinkHost
    ,cast(a.exitLinkUrl as string) as exitLinkUrl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoRegion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobileBrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalCode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerHost
    ,cast(a.referrerHostClass as string) as referrerHostClass
    ,cast(a.referrerQuery as string) as referrerQuery
    ,cast(a.referrerSearchEngine as string) as referrerSearchEngine
    ,cast(a.referrerSocialNetwork as string) as referrerSocialNetwork
    ,cast(a.referrerUrl as string) as referrerUrl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingParameters
    ,cast(a.scrollDepth as double) as scrollDepth
    ,cast(a.sessionBounce as boolean) as sessionBounce
    ,cast(a.sessionStart as boolean) as sessionStart
    ,cast(a.sessionStop as boolean) as sessionStop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as userCorrelationId
    ,cast(a.userParameters as string) as userParameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    df_cxense_user_traffic = spark.sql("""
    select
    b.subscription_identifier
    ,a.mobile_no
    ,a.hash_id
    ,a.cx_id
    ,a.site_id
    ,a.activetime
    ,a.adspace
    ,a.browser
    ,a.browsertimezone
    ,a.browserversion
    ,a.capabilities
    ,a.city
    ,a.colordepth
    ,a.company
    ,a.connectionspeed
    ,a.country
    ,a.devicetype
    ,a.exitlinkhost
    ,a.exitlinkurl
    ,a.host
    ,a.intents
    ,a.isoregion
    ,a.metrocode
    ,a.mobilebrand
    ,a.os
    ,a.postalcode
    ,a.query
    ,a.referrerhost
    ,a.referrerhostclass
    ,a.referrerquery
    ,a.referrersearchengine
    ,a.referrersocialnetwork
    ,a.referrerurl
    ,a.region
    ,a.resolution
    ,a.retargetingparameters
    ,a.scrolldepth
    ,a.sessionbounce
    ,a.sessionstart
    ,a.sessionstop
    ,a.site
    ,a.start
    ,a.stop
    ,a.time
    ,a.traffic_name
    ,a.traffic_value
    ,a.url
    ,c.level_1 as category_level_1
    ,c.level_2 as category_level_2
    ,c.level_3 as category_level_3
    ,c.level_4 as category_level_4
    ,a.usercorrelationid
    ,a.userparameters
    ,a.event_partition_date
    from df_cxense_traffic_cast a
    left join customer_profile_key b
    on a.mobile_no = b.access_method_num
    and a.event_partition_date = b.event_partition_date
    left join master_cxense c
    on a.url = c.site_url
    """)
    return df_cxense_user_traffic

def digital_cxense_user_profile(
    user_profile: pyspark.sql.DataFrame, key_mapping: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_user_profile"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    ##### TEST on Cloud
    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_user_profile/partition_month=2021-09-01/"
    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    p_file = "user_profile_export_*.gz"

    df_sp = spark.read.csv(path_json + p_file, sep=',')

    conf = pyspark.SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)

    df_sp2 = df_sp.withColumnRenamed('_c0', 'hash_id').withColumnRenamed('_c1', 'cx_id').withColumnRenamed('_c2','profile')
    df_sp3 = df_sp2.select("hash_id", "cx_id", "profile")

    udf_hash_id = udf(lambda x: "{'hash_id': '" + str(x) + "'", StringType())
    udf_cx_id = udf(lambda x: "'cx_id': '" + str(x) + "'", StringType())

    df_all = df_sp3.withColumn("hash_id", udf_hash_id(df_sp3.hash_id)).withColumn("cx_id", udf_cx_id(
        df_sp3.cx_id))  # .withColumn("profile", udf_profile(df_sp3.profile))

    rdd1 = df_all.rdd.map(list)
    json = rdd1.map(lambda x: x[0] + "," + x[1] + "," + x[2][1:])

    df1 = sqlContext.read.json(json)
    df1 = df1.drop("_corrupt_record")
    df0 = df1.select("hash_id", "cx_id").distinct()
    df2 = df1.select("hash_id", "cx_id", "type", explode("profile").alias("profile"))
    df3 = df2.select("hash_id", "cx_id", "type", df2.profile.item.alias("item"),
                     explode("profile.groups").alias("groups"))
    df4 = df3.select("hash_id", "cx_id", "type", "item", "groups.group", "groups.weight")

    df4.registerTempTable("c360_online_cxense_user_profile")

    online_cxense_user_profile_temp = spark.sql("""
    SELECT
    cast(hash_id as string) as hash_id
    ,cast(cx_id as string) as cx_id
    ,cast(type as string) as type
    ,cast(item as string) as item
    ,cast(group as string) as groups
    ,cast(weight as double) as weight
    FROM c360_online_cxense_user_profile
    """)
    online_cxense_user_profile_temp.createOrReplaceTempView('online_cxense_user_profile_temp')

    key_mapping = key_mapping.withColumnRenamed("id_1", "mobile_no")

    df_cxense_join_key = online_cxense_user_profile_temp.join(
        key_mapping, on=[online_cxense_user_profile_temp.hash_id == key_mapping.id_2 ], how="inner"
    )

    df_cxense_join_key = df_cxense_join_key.withColumn("partition_month",  lit("202108"))

    df_cxense_join_key =df_cxense_join_key.select("mobile_no","hash_id","cx_id","type","item","groups","weight","partition_month")


    # df_cxense_join_key = spark.sql("""
    # select
    # b.mobile_number as mobile_no
    # ,a.hash_id
    # ,a.cx_id
    # ,a.type
    # ,a.item
    # ,a.groups
    # ,a.weight
    # ,'202108' as partition_month
    # from online_cxense_user_profile_temp a
    # inner join key_mapping b
    # on a.hash_id = b.private_id_v2
    # """)
    # df_cxense_join_key.createOrReplaceTempView('df_cxense_join_key')
    return df_cxense_join_key

def digital_cxense_user_traffic(
    traffic_json: pyspark.sql.DataFrame, cxense_hash_id_key_mapping: pyspark.sql.DataFrame, customer_profile_key: pyspark.sql.DataFrame, master_cxense: pyspark.sql.DataFrame
):
    spark = get_spark_session()
    #location run & path data
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if running_environment == "on_cloud":
        path_json = "/mnt/customer360-blob-data/C360/ONLINE/source_online_traffic/"
        path_metadata = "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table"
    else:
        path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic"
        path_metadata = "/projects/prod/c360/data/UTILITIES/metadata_table"
    #####on Prim
    today = date.today()
    data_date = today - timedelta(days=2)
    p_partition = data_date.strftime("%Y-%m-%d")

    path_json = "hdfs://10.237.82.9:8020/C360/DIGITAL/digital_cxense_traffic/partition_date="+p_partition

    # lookup_table_name = "l1_digital_customer_app_category_agg_daily_catlv_1"
    #read meta data
    # metadata_table = spark.read.parquet(metadata_table_path)
    # metadata_table.createOrReplaceTempView("mdtl")
    # target_max_data_load_date = spark.sql("""select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date from mdtl where table_name = '{0}'""".format(lookup_table_name))

    #read Json
    traffic = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(path_json,"json")

    traffic_drop = traffic.drop("_corrupt_record")
    # spilt_json
    traffic_json = traffic_drop.select(explode("events").alias("events"), "start", "stop")

    traffic_json_master = traffic_json.select("events.*", "start", "stop")

    traffic_json_temp = traffic_json_master.select("userId", "start", "stop",
                                                   explode("customParameters").alias("customParameters"))

    traffic_master = traffic_json_master.select("activeTime", "adspaces", "browser", "browserTimezone",
                                                "browserVersion", "capabilities", "city", "colorDepth", "company",
                                                "connectionSpeed", "country", "deviceType", "exitLinkHost",
                                                "exitLinkUrl", "host", "intents", "isoRegion", "metrocode",
                                                "mobileBrand", "os", "postalCode", "query", "referrerHost",
                                                "referrerHostClass", "referrerQuery", "referrerSearchEngine",
                                                "referrerSocialNetwork", "referrerUrl", "region", "resolution",
                                                "retargetingParameters", "scrollDepth", "sessionBounce", "sessionStart",
                                                "sessionStop", "site", "time", "url", "userCorrelationId", "userId",
                                                "userParameters", "start", "stop")

    traffic_temp = traffic_json_temp.select("userId", "start", "stop", "customParameters.*")

    traffic_master.createOrReplaceTempView('traffic_master')
    traffic_temp.createOrReplaceTempView('traffic_temp')

    df_cxense_traffic = spark.sql("""
    select a.*
    ,b.group as traffic_name
    ,b.item as traffic_value
    from traffic_master a
    left join traffic_temp b
    on a.userId = b.userId
    and a.start = b.start
    and a.stop = b.stop
    """)
    # df_cxense_traffic = df_cxense_traffic.withColumn("event_partition_date",'2021-08-29')
    df_cxense_traffic.createOrReplaceTempView('df_cxense_traffic')
    cxense_hash_id_key_mapping.createOrReplaceTempView('cxense_hash_id_key_mapping')
    # customer_profile_key.createOrReplaceTempView('customer_profile_key')
    # master_cxense.createOrReplaceTempView('master_cxense')

    df_cxense_traffic_cast = spark.sql("""
    select
    b.mobile_no
    ,b.hash_id
    ,b.cx_id
    ,cast(a.site as string) as site_id
    ,cast(a.activeTime as double) as activetime
    ,cast(a.adspaces as string) as adspace
    ,cast(a.browser as string) as browser
    ,cast(a.browserTimezone as string) as browsertimezone
    ,cast(a.browserVersion as string) as browserversion
    ,cast(a.capabilities as string) as capabilities
    ,cast(a.city as string) as city
    ,cast(a.colorDepth as string) as colordepth
    ,cast(a.company as string) as company
    ,cast(a.connectionSpeed as string) as connectionspeed
    ,cast(a.country as string) as country
    ,cast(a.deviceType as string) as devicetype
    ,cast(a.exitLinkHost as string) as exitlinkhost
    ,cast(a.exitLinkUrl as string) as exitlinkurl
    ,cast(a.host as string) as host
    ,cast(a.intents as string) as intents
    ,cast(a.isoRegion as string) as isoregion
    ,cast(a.metrocode as string) as metrocode
    ,cast(a.mobileBrand as string) as mobilebrand
    ,cast(a.os as string) as os
    ,cast(a.postalCode as string) as postalcode
    ,cast(a.query as string) as query
    ,cast(a.referrerHost as string) as referrerhost
    ,cast(a.referrerHostClass as string) as referrerhostclass
    ,cast(a.referrerQuery as string) as referrerquery
    ,cast(a.referrerSearchEngine as string) as referrersearchengine
    ,cast(a.referrerSocialNetwork as string) as referrersocialnetwork
    ,cast(a.referrerUrl as string) as referrerurl
    ,cast(a.region as string) as region
    ,cast(a.resolution as string) as resolution
    ,cast(a.retargetingParameters as string) as retargetingparameters
    ,cast(a.scrollDepth as double) as scrolldepth
    ,cast(a.sessionBounce as boolean) as sessionbounce
    ,cast(a.sessionStart as boolean) as sessionstart
    ,cast(a.sessionStop as boolean) as sessionstop
    ,cast(a.site as string) as site
    ,substr(cast((from_unixtime(cast(a.start as bigint)),7) as string),2,19) as start
    ,substr(cast((from_unixtime(cast(a.stop as bigint)),7) as string),2,19) as stop
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,19) as time
    ,cast(a.traffic_name as string) as traffic_name
    ,cast(a.traffic_value as string) as traffic_value
    ,cast(a.url as string) as url
    ,cast(a.userCorrelationId as string) as usercorrelationid
    ,cast(a.userParameters as string) as userparameters
    ,substr(cast((from_unixtime(cast(a.time as bigint)),7) as string),2,10) as event_partition_date
    from df_cxense_traffic a
    join cxense_hash_id_key_mapping b
    on cast(a.userId as string)=b.cx_id
    """)
    df_cxense_traffic_cast.createOrReplaceTempView('df_cxense_traffic_cast')

    traffic = df_cxense_traffic_cast.join(customer_profile_key,
                           on=[df_cxense_traffic_cast.mobile_no == customer_profile_key.access_method_num,
                               df_cxense_traffic_cast.event_partition_date == customer_profile_key.event_partition_date],
                           how="left", ).select(customer_profile_key.subscription_identifier
                                                , df_cxense_traffic_cast.mobile_no
                                                , df_cxense_traffic_cast.hash_id
                                                , df_cxense_traffic_cast.cx_id
                                                , df_cxense_traffic_cast.site_id
                                                , df_cxense_traffic_cast.activetime
                                                , df_cxense_traffic_cast.adspace
                                                , df_cxense_traffic_cast.browser
                                                , df_cxense_traffic_cast.browsertimezone
                                                , df_cxense_traffic_cast.browserversion
                                                , df_cxense_traffic_cast.capabilities
                                                , df_cxense_traffic_cast.city
                                                , df_cxense_traffic_cast.colordepth
                                                , df_cxense_traffic_cast.company
                                                , df_cxense_traffic_cast.connectionspeed
                                                , df_cxense_traffic_cast.country
                                                , df_cxense_traffic_cast.devicetype
                                                , df_cxense_traffic_cast.exitlinkhost
                                                , df_cxense_traffic_cast.exitlinkurl
                                                , df_cxense_traffic_cast.host
                                                , df_cxense_traffic_cast.intents
                                                , df_cxense_traffic_cast.isoregion
                                                , df_cxense_traffic_cast.metrocode
                                                , df_cxense_traffic_cast.mobilebrand
                                                , df_cxense_traffic_cast.os
                                                , df_cxense_traffic_cast.postalcode
                                                , df_cxense_traffic_cast.query
                                                , df_cxense_traffic_cast.referrerhost
                                                , df_cxense_traffic_cast.referrerhostclass
                                                , df_cxense_traffic_cast.referrerquery
                                                , df_cxense_traffic_cast.referrersearchengine
                                                , df_cxense_traffic_cast.referrersocialnetwork
                                                , df_cxense_traffic_cast.referrerurl
                                                , df_cxense_traffic_cast.region
                                                , df_cxense_traffic_cast.resolution
                                                , df_cxense_traffic_cast.retargetingparameters
                                                , df_cxense_traffic_cast.scrolldepth
                                                , df_cxense_traffic_cast.sessionbounce
                                                , df_cxense_traffic_cast.sessionstart
                                                , df_cxense_traffic_cast.sessionstop
                                                , df_cxense_traffic_cast.site
                                                , df_cxense_traffic_cast.start
                                                , df_cxense_traffic_cast.stop
                                                , df_cxense_traffic_cast.time
                                                , df_cxense_traffic_cast.traffic_name
                                                , df_cxense_traffic_cast.traffic_value
                                                , df_cxense_traffic_cast.url
                                                , df_cxense_traffic_cast.usercorrelationid
                                                , df_cxense_traffic_cast.userparameters
                                                , df_cxense_traffic_cast.event_partition_date)
    #-------- rename category ---------#
    master_cxense = master_cxense.withColumnRenamed("level_1", 'category_level_1')
    master_cxense = master_cxense.withColumnRenamed("level_2", 'category_level_2')
    master_cxense = master_cxense.withColumnRenamed("level_3", 'category_level_3')
    master_cxense = master_cxense.withColumnRenamed("level_4", 'category_level_4')

    online_cxense_traffic = traffic.join(master_cxense, on=[df_cxense_traffic_cast.url == master_cxense.site_url], how="left").select(traffic.subscription_identifier
                                                      ,traffic.mobile_no
                                                      ,traffic.hash_id
                                                      ,traffic.cx_id
                                                      ,traffic.site_id
                                                      ,traffic.activetime
                                                      ,traffic.adspace
                                                      ,traffic.browser
                                                      ,traffic.browsertimezone
                                                      ,traffic.browserversion
                                                      ,traffic.capabilities
                                                      ,traffic.city
                                                      ,traffic.colordepth
                                                      ,traffic.company
                                                      ,traffic.connectionspeed
                                                      ,traffic.country
                                                      ,traffic.devicetype
                                                      ,traffic.exitlinkhost
                                                      ,traffic.exitlinkurl
                                                      ,traffic.host
                                                      ,traffic.intents
                                                      ,traffic.isoregion
                                                      ,traffic.metrocode
                                                      ,traffic.mobilebrand
                                                      ,traffic.os
                                                      ,traffic.postalcode
                                                      ,traffic.query
                                                      ,traffic.referrerhost
                                                      ,traffic.referrerhostclass
                                                      ,traffic.referrerquery
                                                      ,traffic.referrersearchengine
                                                      ,traffic.referrersocialnetwork
                                                      ,traffic.referrerurl
                                                      ,traffic.region
                                                      ,traffic.resolution
                                                      ,traffic.retargetingparameters
                                                      ,traffic.scrolldepth
                                                      ,traffic.sessionbounce
                                                      ,traffic.sessionstart
                                                      ,traffic.sessionstop
                                                      ,traffic.site
                                                      ,traffic.start
                                                      ,traffic.stop
                                                      ,traffic.time
                                                      ,traffic.traffic_name
                                                      ,traffic.traffic_value
                                                      ,traffic.url
                                                      ,master_cxense.category_level_1
                                                      ,master_cxense.category_level_2
                                                      ,master_cxense.category_level_3
                                                      ,master_cxense.category_level_4
                                                      ,traffic.usercorrelationid
                                                      ,traffic.userparameters
                                                      ,traffic.event_partition_date)

    # online_cxense_traffic = df_cxense_traffic_cast.join(master_cxense, on=[df_cxense_traffic_cast.url == master_cxense.site_url], how="left").select(
    #                                                   df_cxense_traffic_cast.mobile_no
    #                                                   ,df_cxense_traffic_cast.hash_id
    #                                                   ,df_cxense_traffic_cast.cx_id
    #                                                   ,df_cxense_traffic_cast.site_id
    #                                                   ,df_cxense_traffic_cast.activetime
    #                                                   ,df_cxense_traffic_cast.adspace
    #                                                   ,df_cxense_traffic_cast.browser
    #                                                   ,df_cxense_traffic_cast.browsertimezone
    #                                                   ,df_cxense_traffic_cast.browserversion
    #                                                   ,df_cxense_traffic_cast.capabilities
    #                                                   ,df_cxense_traffic_cast.city
    #                                                   ,df_cxense_traffic_cast.colordepth
    #                                                   ,df_cxense_traffic_cast.company
    #                                                   ,df_cxense_traffic_cast.connectionspeed
    #                                                   ,df_cxense_traffic_cast.country
    #                                                   ,df_cxense_traffic_cast.devicetype
    #                                                   ,df_cxense_traffic_cast.exitlinkhost
    #                                                   ,df_cxense_traffic_cast.exitlinkurl
    #                                                   ,df_cxense_traffic_cast.host
    #                                                   ,df_cxense_traffic_cast.intents
    #                                                   ,df_cxense_traffic_cast.isoregion
    #                                                   ,df_cxense_traffic_cast.metrocode
    #                                                   ,df_cxense_traffic_cast.mobilebrand
    #                                                   ,df_cxense_traffic_cast.os
    #                                                   ,df_cxense_traffic_cast.postalcode
    #                                                   ,df_cxense_traffic_cast.query
    #                                                   ,df_cxense_traffic_cast.referrerhost
    #                                                   ,df_cxense_traffic_cast.referrerhostclass
    #                                                   ,df_cxense_traffic_cast.referrerquery
    #                                                   ,df_cxense_traffic_cast.referrersearchengine
    #                                                   ,df_cxense_traffic_cast.referrersocialnetwork
    #                                                   ,df_cxense_traffic_cast.referrerurl
    #                                                   ,df_cxense_traffic_cast.region
    #                                                   ,df_cxense_traffic_cast.resolution
    #                                                   ,df_cxense_traffic_cast.retargetingparameters
    #                                                   ,df_cxense_traffic_cast.scrolldepth
    #                                                   ,df_cxense_traffic_cast.sessionbounce
    #                                                   ,df_cxense_traffic_cast.sessionstart
    #                                                   ,df_cxense_traffic_cast.sessionstop
    #                                                   ,df_cxense_traffic_cast.site
    #                                                   ,df_cxense_traffic_cast.start
    #                                                   ,df_cxense_traffic_cast.stop
    #                                                   ,df_cxense_traffic_cast.time
    #                                                   ,df_cxense_traffic_cast.traffic_name
    #                                                   ,df_cxense_traffic_cast.traffic_value
    #                                                   ,df_cxense_traffic_cast.url
    #                                                   ,master_cxense.category_level_1
    #                                                   ,master_cxense.category_level_2
    #                                                   ,master_cxense.category_level_3
    #                                                   ,master_cxense.category_level_4
    #                                                   ,df_cxense_traffic_cast.usercorrelationid
    #                                                   ,df_cxense_traffic_cast.userparameters
    #                                                   ,df_cxense_traffic_cast.event_partition_date)

    return online_cxense_traffic

