import pyspark.sql.functions as f, logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
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
    ).select("argument", "level_1", "level_2", "level_3", "level_4", "priority").withColumnRenamed("level_1" , "category_name")

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

def digital_mobile_app_category_agg_timeband(Mobile_app_timeband: DataFrame,Mobile_app_daily: DataFrame,app_categories_master: DataFrame, category_level: dict,timeband: dict,mobile_app_timeband_sql: dict,mobile_app_timeband_sql_share: dict):
    import os,subprocess

    ##check missing data##
    if check_empty_dfs([Mobile_app_timeband]):
        return get_spark_empty_df()
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
def l1_digital_customer_web_category_agg_daily(mobile_web_daily_raw: DataFrame, aib_categories_clean: DataFrame) -> DataFrame:
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
    ).select("subscription_identifier", "mobile_no", "category_name", "priority", "upload_byte", "download_byte", "duration" , "total_byte", "count_trans", "partition_date")

    df_mobile_web_daily_category_agg = df_mobile_web_daily.groupBy("subscription_identifier", "mobile_no",
                                                                   "category_name", "priority", "partition_date").agg(

        f.sum("count_trans").cast("decimal(35,4)").alias("total_visit_count"),
        f.sum("duration").cast("decimal(35,4)").alias("total_visit_duration"),
        f.sum("total_byte").cast("decimal(35,4)").alias("total_volume_byte"),
        f.sum("download_byte").cast("decimal(35,4)").alias("total_download_byte"),
        f.sum("upload_byte").cast("decimal(35,4)").alias("total_upload_byte"),
        )

    df_mobile_web_daily_category_agg_partition = df_mobile_web_daily_category_agg.withColumnRenamed("partition_date", "event_partition_date")

    return df_mobile_web_daily_category_agg_partition

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
                                                 mobile_web_daily_raw: DataFrame,
                                                 aib_categories_clean: DataFrame,
                                                 df_mobile_web_hourly_agg_sql: dict,
                                                 df_timeband_web: dict,
                                                 mobile_web_timeband_sql_share: dict) -> DataFrame:

    if check_empty_dfs([mobile_web_hourly_raw]):
        return get_spark_empty_df()
    if check_empty_dfs([aib_categories_clean]):
        return get_spark_empty_df()

    # Filter Hour
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
            aib_categories_clean.argument == mobile_web_hourly_raw.url], how="inner", )).select("batchno", "mobile_no",
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

    # -------------------------------- share ----------------------------
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_visit_count", 'total_visit_count_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_visit_duration", 'total_visit_duration_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_volume_byte", 'total_volume_byte_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_download_byte", 'total_download_byte_daily')
    mobile_web_daily_raw = mobile_web_daily_raw.withColumnRenamed("total_upload_byte", 'total_upload_byte_daily')

    mobile_web_hourly_raw = mobile_web_hourly_raw.join(mobile_web_daily_raw, on=[mobile_web_hourly_raw.mobile_no == mobile_web_daily_raw.mobile_no], how="inner").select(mobile_web_daily_raw.subscription_identifier,
                                                                                                                                                                         mobile_web_daily_raw.mobile_no,
                                                                                                                                                                         mobile_web_hourly_raw.category_name,
                                                                                                                                                                         mobile_web_hourly_raw.priority,
                                                                                                                                                                        "total_visit_count",
                                                                                                                                                                        "total_visit_duration",
                                                                                                                                                                        "total_volume_byte",
                                                                                                                                                                        "total_download_byte",
                                                                                                                                                                        "total_upload_byte",
                                                                                                                                                                        "total_visit_count_daily",
                                                                                                                                                                        "total_visit_duration_daily",
                                                                                                                                                                        "total_volume_byte_daily",
                                                                                                                                                                        "total_download_byte_daily",
                                                                                                                                                                        "total_upload_byte_daily",
                                                                                                                                                                        mobile_web_hourly_raw.event_partition_date)
    df_return = node_from_config(mobile_web_hourly_raw, mobile_web_timeband_sql_share)
    return df_return

################## Timebrand join subscription identifier ###########################
def l1_digital_mobile_web_category_agg_timeband_features(union_profile_daily: DataFrame,
                                                 mobile_web_hourly_agg: DataFrame,) -> DataFrame:

    if check_empty_dfs([union_profile_daily]):
        return get_spark_empty_df()
    if check_empty_dfs([mobile_web_hourly_agg]):
        return get_spark_empty_df()

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


    combine = app_category_agg_daily.union(app_category_web_daily)
    logging.info("Union App & Web Complete")

    combine = combine.withColumnRenamed("category_name", "category_name_old")
    combine = combine.withColumn('category_name', f.lower(f.col("category_name_old")))
    combine = combine.drop('category_name_old')
    df_return = node_from_config(combine,combine_app_web_agg_daily)

    return df_return