import pyspark.sql.functions as f, logging
from pyspark.sql.functions import max
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , add_event_week_and_month_from_yyyymmdd, union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df
import subprocess


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
    mobile_app_daily.show()
    mobile_app_daily = mobile_app_daily.withColumn("priority", f.lit(None).cast(StringType()))
    mobile_app_daily = mobile_app_daily.withColumnRenamed('partition_date', 'event_partition_date')

    df_return = node_from_config(mobile_app_daily, mobile_app_daily_sql)
    return df_return

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
    
    ############################### Mobile_app_timeband ##############################
def digital_mobile_app_category_agg_timeband(Mobile_app_timeband: DataFrame,app_categories_master: DataFrame,key_c360: DataFrame, mobile_app_timeband_sql: dict, category_level: dict,timeband: dict):
    ##check missing data##
    if check_empty_dfs([Mobile_app_timeband]):
        return get_spark_empty_df()
    
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
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("dw_byte") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("ul_kbyte") > 0)

    #join master
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed("msisdn", "mobile_no").join(f.broadcast(app_categories_master),
        on=[app_categories_master.application_id == Mobile_app_timeband.application],
        how="inner",
    )
    #where max date key
    running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
    if (running_environment == "on_cloud"):
        load_path = "/mnt/customer360-blob-output/C360/PROFILE/l1_features/l1_customer_profile_union_daily_feature/"
        list_temp = subprocess.check_output(
        "ls -d /dbfs" + load_path + "*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20 |tail -1",
        shell=True).splitlines()
        max_date = str(list_temp[0])[2:-1].split('/')[-2].split('=')[1]
    else:
        max_date = key_c360.select(f.max(f.to_date((f.col("event_partition_date")).cast(StringType()), 'yyyy-MM-dd')).alias("max_date"))

    key_c360 = key_c360.where(f.to_date(f.col("event_partition_date").cast(StringType()), 'yyyy-MM-dd')) = max_date)
    #join key
    Mobile_app_timeband = Mobile_app_timeband.join(f.broadcast(key_c360),
        on=[key_c360.access_method_num == Mobile_app_timeband.mobile_no],
        how="inner",
    )

    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed(category_level, 'category_name')
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('ul_kbyte', 'ul_byte')
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('partition_date', 'event_partition_date')

    df_return = node_from_config(Mobile_app_timeband, mobile_app_timeband_sql)
    return df_return


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

def l1_digital_mobile_web_category_agg_daily(mobile_web_daily_raw: DataFrame, aib_categories_clean: DataFrame) -> DataFrame:
    ##check missing data##
    if check_empty_dfs([mobile_web_daily_raw]):
        return get_spark_empty_df()

    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_type"))) == "url")
    aib_categories_clean = aib_categories_clean.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")

    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("trans") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("duration") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("total_kb") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("download_kb") > 0)
    mobile_web_daily_raw = mobile_web_daily_raw.where(f.col("upload_kb") > 0)

    df_mobile_web_daily = mobile_web_daily_raw.join(
        f.broadcast(aib_categories_clean)
        , on=[aib_categories_clean.argument == mobile_web_daily_raw.domain]
        , how="inner",
    ).select("mobile_no", "subscription_identifier", "category_name", "priority","upload_kb", "download_kb", "duration" , "total_kb", "trans", "partition_date")

    df_mobile_web_daily_category_agg = df_mobile_web_daily.groupBy("mobile_no", "subscription_identifier",
                                                                   "category_name", "priority", "partition_date").agg(
        f.sum("duration").alias("total_visit_duration"),
        f.sum("upload_kb").alias("total_upload_byte"),
        f.sum("download_kb").alias("total_download_byte"),
        f.sum("total_kb").alias("total_volume_byte"),
        f.sum("trans").alias("total_visit_counts"), )

    df_mobile_web_daily_category_agg_partition = df_mobile_web_daily_category_agg.withColumnRenamed('partition_date', 'event_partition_date')

    return df_mobile_web_daily_category_agg_partition
