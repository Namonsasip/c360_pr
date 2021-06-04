import pyspark as pyspark

import pyspark.sql.functions as f, logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from typing import Dict, Any
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , add_event_week_and_month_from_yyyymmdd, union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session


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

def digital_mobile_app_category_agg_timeband(Mobile_app_timeband: DataFrame, mobile_app_timeband_sql: dict):
    ##check missing data##
    if check_empty_dfs([mobile_app_daily]):
        return get_spark_empty_df()

    # where this column more than 0
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("count_trans") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("duration") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("total_byte") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("download_byte") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("upload_byte") > 0)

    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('category_level_1', 'category_name')
    Mobile_app_timeband = Mobile_app_timeband.withColumn("priority", f.lit(None).cast(StringType()))
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


def relay_drop_nulls(df_relay: pyspark.sql.DataFrame):
    df_relay_cleaned = df_relay.filter(
        (f.col("mobile_no").isNotNull())
        & (f.col("mobile_no") != "")
        & (f.col("subscription_identifier") != "")
        & (f.col("subscription_identifier").isNotNull())
    ).dropDuplicates()
    return df_relay_cleaned


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

    spark = get_spark_session()
    df_conversion_and_package_visits = spark.sql("""
    select
    COALESCE(a.subscription_identifier,b.subscription_identifier) as subscription_identifier,
    COALESCE(a.mobile_no,b.mobile_no) as mobile_no,
    COALESCE(a.campaign_id,b.campaign_id) as campaign_id,
    a.total_conversion_product_count as total_conversion_product_count,
    b.total_conversion_package_count as total_conversion_package_count,
    COALESCE(a.event_partition_date,b.event_partition_date) as event_partition_date
    from df_engagement_conversion_visits a
    FULL JOIN df_engagement_conversion_package_visits b
    ON a.subscription_identifier = b.subscription_identifier
    and a.mobile_no = b.mobile_no
    and a.campaign_id = b.campaign_id
    and a.event_partition_date = b.event_partition_date       
    """)
    return df_conversion_and_package_visits

def remove_time_dupe_cxense_traffic(df_traffic: pyspark.sql.DataFrame):
    # first grouping by traffic_name, traffic value because they are
    # repeated at identical times with different activetime
    # getting max for the same traffic name and traffic value
    df_traffic_cleaned = (
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
        .withColumn(
            "is_afternoon",
            f.when(f.col("hour").between(12, 17), f.lit(1)).otherwise(f.lit(0)),
        )
    )
    return df_traffic_cleaned


def basic_clean_cxense_traffic(df_traffic_raw: pyspark.sql.DataFrame):
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
    df_traffic = basic_clean_cxense_traffic(df_traffic_raw)
    df_traffic = remove_time_dupe_cxense_traffic(df_traffic)
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
        .withColumn("url0", f.lower("url0"))
        .dropDuplicates()
    )
    return df_cp


def digital_cxense_clean(
    df_traffic_raw: pyspark.sql.DataFrame,
    df_cxense_cp_raw: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_traffic_raw]):
        return get_spark_empty_df()
    # if check_empty_dfs([df_cxense_cp_raw]):
    #     return get_spark_empty_df()

    df_traffic = clean_cxense_traffic(df_traffic_raw)
    df_cxense_traffic = df_traffic.withColumn(
        "event_partition_date",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 7, 2)
                 ),
    ).drop(*["partition_date"])

    df_cxense_cp = clean_cxense_content_profile(df_cxense_cp_raw)

    return [df_cxense_traffic, df_cxense_cp]


def create_content_profile_mapping(
    df_cxense_cp: pyspark.sql.DataFrame, df_aib_categories: pyspark.sql.DataFrame
):
    df_aib_categories = df_aib_categories.filter(f.lower(f.trim(f.col("source_platform"))) == "than")
    df_cxense_cp_rank_by_wt = (
        df_cxense_cp.filter("content_name = 'ais-categories'")
        .withColumn("category_length", f.size(f.split("content_value", "/")))
        .withColumn(
            "rn",
            f.rank().over(
                Window.partitionBy("siteid", "url0").orderBy(
                    f.desc("weight"),
                    f.desc("category_length"),
                    f.desc("partition_month"),
                    f.desc("lastfetched"),
                )
            ),
        )
        .filter("rn = 1")
    )

    df_cxense_cp_urls_with_multiple_weights = (
        df_cxense_cp_rank_by_wt.groupBy("siteid", "url0", "rn")
        .count()
        .filter("count > 1")
        .select("siteid", "url0")
        .distinct()
    )

    df_cxense_cp_cleaned = df_cxense_cp_rank_by_wt.join(
        df_cxense_cp_urls_with_multiple_weights, on=["siteid", "url0"], how="left_anti"
    )

    df_cxense_cp_join_iab = df_cxense_cp_cleaned.join(
        df_aib_categories, on=[df_cxense_cp_cleaned.content_value == df_aib_categories.argument]
    )
    return df_cxense_cp_join_iab


def digital_cxense_content_profile_mapping(
    df_cxense_cp: pyspark.sql.DataFrame, df_aib_categories: pyspark.sql.DataFrame
):
    df_cxense_cp_cleaned = create_content_profile_mapping(df_cxense_cp, df_aib_categories)
    return df_cxense_cp_cleaned

def digital_agg_cxense_traffic(
        df_traffic_cleaned: pyspark.sql.DataFrame
):
    # aggregating url visits activetime, visit counts
    if check_empty_dfs([df_traffic_cleaned]):
        return get_spark_empty_df()

    df_traffic_agg = df_traffic_cleaned.groupBy(
        "mobile_no", "site_id", "url", "event_partition_date"
    ).agg(
        f.sum("activetime").alias("total_visit_duration"),
        f.count("*").alias("total_visit_counts"),
        f.sum(
            f.when((f.col("is_afternoon") == 1), f.col("activetime")).otherwise(
                f.lit(0)
            )
        ).alias("total_afternoon_duration"),
        f.sum("is_afternoon").alias("total_afternoon_visit_counts"),
    )
    return df_traffic_agg

