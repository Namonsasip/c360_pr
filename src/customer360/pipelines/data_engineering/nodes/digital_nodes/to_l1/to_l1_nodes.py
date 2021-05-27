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

def l1_digital_mobile_web_category_agg_daily(mobile_web_daily_raw: DataFrame, aib_categories_clean: DataFrame) -> DataFrame:
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
        , how="inner",
    ).select("subscription_identifier", "mobile_no", "category_name", "priority","upload_byte", "download_byte", "duration" , "total_byte", "count_trans", "partition_date")

    df_mobile_web_daily_category_agg = df_mobile_web_daily.groupBy("mobile_no", "subscription_identifier",
                                                                   "category_name", "priority", "partition_date").agg(
        f.sum("count_trans").alias("total_visit_counts"),
        f.sum("duration").alias("total_visit_duration"),
        f.sum("total_byte").alias("total_volume_byte"),
        f.sum("download_byte").alias("total_download_byte"),
        f.sum("upload_byte").alias("total_upload_byte"),
        )

    df_mobile_web_daily_category_agg_partition = df_mobile_web_daily_category_agg.withColumnRenamed('partition_date', 'event_partition_date')

    return df_mobile_web_daily_category_agg_partition

def l1_digital_mobile_web_level_category(mobile_web_daily_category_agg: DataFrame):

    if check_empty_dfs([mobile_web_daily_category_agg]):
        return get_spark_empty_df()

    key = ["mobile_no", "event_partition_date"]
    df_soc_web_day_level_stats = mobile_web_daily_category_agg.groupBy(key).agg(
        f.sum("total_download_byte").alias("total_download_byte"),
        f.sum("total_upload_byte").alias("total_upload_byte"),
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_volume_byte").alias("total_volume_byte"),
        f.sum("total_visit_counts").alias("total_visit_counts"),
    )

    return df_soc_web_day_level_stats

def node_soc_web_daily_category_level_features_massive_processing(
    df_combined_web_app_daily_and_hourly_agg,
    df_soc_web_day_level_stats,
    df_cust,
    config_soc_web_daily_agg_features,
    config_soc_web_daily_ratio_based_features,
    config_soc_web_popular_domain_by_download_volume,
    config_soc_web_most_popular_domain_by_download_volume,
) -> DataFrame:
    if check_empty_dfs(
        [df_combined_web_app_daily_and_hourly_agg, df_soc_web_day_level_stats]
    ):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_combined_web_app_daily_and_hourly_agg = (
        df_combined_web_app_daily_and_hourly_agg.select(
            f.collect_set(source_partition_col).alias(source_partition_col)
        ).first()[source_partition_col]
    )

    list_soc_web_day_level_stats = df_soc_web_day_level_stats.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list(
        set((list_combined_web_app_daily_and_hourly_agg + list_soc_web_day_level_stats))
    )

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_soc_web_daily_category_level_features"

    df_cust = df_cust.withColumn(
        "partition_date", f.date_format(f.col("event_partition_date"), "yyyyMMdd")
    )
    df_cust = df_cust.withColumnRenamed("access_method_num", "mobile_no")
    df_cust = df_cust.select("mobile_no", "partition_date", "subscription_identifier")

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_combined_web_app_daily_and_hourly_agg_chunk = (
            df_combined_web_app_daily_and_hourly_agg.filter(
                f.col(source_partition_col).isin(*[curr_item])
            )
        )
        df_soc_web_day_level_stats_chunk = df_soc_web_day_level_stats.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[curr_item]))
        output_df = node_soc_web_daily_category_level_features(
            df_combined_web_app_daily_and_hourly_agg_chunk,
            df_soc_web_day_level_stats_chunk,
            df_cust_chunk,
            config_soc_web_daily_agg_features,
            config_soc_web_daily_ratio_based_features,
            config_soc_web_popular_domain_by_download_volume,
            config_soc_web_most_popular_domain_by_download_volume,
        )

        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_combined_web_app_daily_and_hourly_agg_chunk = (
        df_combined_web_app_daily_and_hourly_agg.filter(
            f.col(source_partition_col).isin(*[first_item])
        )
    )
    df_soc_web_day_level_stats_chunk = df_soc_web_day_level_stats.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[first_item]))
    return_df = node_soc_web_daily_category_level_features(
        df_combined_web_app_daily_and_hourly_agg_chunk,
        df_soc_web_day_level_stats_chunk,
        df_cust_chunk,
        config_soc_web_daily_agg_features,
        config_soc_web_daily_ratio_based_features,
        config_soc_web_popular_domain_by_download_volume,
        config_soc_web_most_popular_domain_by_download_volume,
    )
    CNTX.catalog.save(filepath, return_df)
    return return_df


def node_soc_web_daily_category_level_features(
    df_combined_soc_app_daily_and_hourly_agg: pyspark.sql.DataFrame,
    df_soc_web_day_level_stats: pyspark.sql.DataFrame,
    df_cust: DataFrame,
    config_soc_web_daily_agg_features: Dict[str, Any],
    config_soc_web_daily_ratio_based_features: Dict,
    config_soc_web_popular_domain_by_download_volume: Dict[str, Any],
    config_soc_web_most_popular_domain_by_download_volume: Dict[str, Any],
):

    if check_empty_dfs(
        [df_combined_soc_app_daily_and_hourly_agg, df_soc_web_day_level_stats]
    ):
        return get_spark_empty_df()
    join_on = ["mobile_no", "partition_date"]
    df_soc_web_daily_features = node_from_config(
        df_combined_soc_app_daily_and_hourly_agg, config_soc_web_daily_agg_features
    )

    df_soc_web_daily_features_with_day_level_stats = df_soc_web_daily_features.join(
        df_soc_web_day_level_stats, on=join_on, how="left"
    )

    df_soc_web_fea_non_fav = node_from_config(
        df_soc_web_daily_features_with_day_level_stats,
        config_soc_web_daily_ratio_based_features,
    )

    df_combined_soc_app_daily_and_hourly_agg = clean_favourite_category(
        df_combined_soc_app_daily_and_hourly_agg, "url"
    )
    df_popular_domain_by_download_volume = node_from_config(
        df_combined_soc_app_daily_and_hourly_agg,
        config_soc_web_popular_domain_by_download_volume,
    )

    df_most_popular_domain_by_download_volume = node_from_config(
        df_popular_domain_by_download_volume,
        config_soc_web_most_popular_domain_by_download_volume,
    )

    df_soc_web_fea_all = join_all(
        [df_soc_web_fea_non_fav, df_most_popular_domain_by_download_volume],
        on=["mobile_no", "partition_date", "level_1"],
        how="outer",
    )

    df_fea = df_cust.join(
        df_soc_web_fea_all, ["mobile_no", "partition_date"], how="inner"
    )
    return df_fea


##################################################################
# SOC WEB DAILY FEATURES
##################################################################


def node_soc_web_daily_features_massive_processing(
    df_combined_soc_app_daily_and_hourly_agg,
    df_cust,
    config_popular_category_by_download_volume,
    config_most_popular_category_by_download_volume,
) -> DataFrame:
    if check_empty_dfs([df_combined_soc_app_daily_and_hourly_agg]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_combined_soc_app_daily_and_hourly_agg = (
        df_combined_soc_app_daily_and_hourly_agg.select(
            f.collect_set(source_partition_col).alias(source_partition_col)
        ).first()[source_partition_col]
    )

    mvv_array = list_combined_soc_app_daily_and_hourly_agg

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 2
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_soc_web_daily_features"
    df_cust = df_cust.withColumn(
        "partition_date", f.date_format(f.col("event_partition_date"), "yyyyMMdd")
    )
    df_cust = df_cust.withColumnRenamed("access_method_num", "mobile_no")
    df_cust = df_cust.select("mobile_no", "partition_date", "subscription_identifier")
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_combined_soc_app_daily_and_hourly_agg_chunk = (
            df_combined_soc_app_daily_and_hourly_agg.filter(
                f.col(source_partition_col).isin(*[curr_item])
            )
        )
        df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[curr_item]))
        output_df = node_soc_web_daily_features(
            df_combined_soc_app_daily_and_hourly_agg_chunk,
            df_cust_chunk,
            config_popular_category_by_download_volume,
            config_most_popular_category_by_download_volume,
        )

        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_combined_soc_app_daily_and_hourly_agg_chunk = (
        df_combined_soc_app_daily_and_hourly_agg.filter(
            f.col(source_partition_col).isin(*[first_item])
        )
    )
    df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[first_item]))
    return_df = node_soc_web_daily_features(
        df_combined_soc_app_daily_and_hourly_agg_chunk,
        df_cust_chunk,
        config_popular_category_by_download_volume,
        config_most_popular_category_by_download_volume,
    )

    return return_df


def node_soc_web_daily_features(
    df_combined_soc_app_daily_and_hourly_agg: DataFrame,
    df_cust: DataFrame,
    config_popular_category_by_download_volume: Dict[str, Any],
    config_most_popular_category_by_download_volume: Dict[str, Any],
):

    df_popular_category_by_download_volume = node_from_config(
        df_combined_soc_app_daily_and_hourly_agg,
        config_popular_category_by_download_volume,
    )
    df_most_popular_category_by_download_volume = node_from_config(
        df_popular_category_by_download_volume,
        config_most_popular_category_by_download_volume,
    )
    df_fea = df_cust.join(
        df_most_popular_category_by_download_volume,
        ["mobile_no", "partition_date"],
        how="inner",
    )
    return df_fea