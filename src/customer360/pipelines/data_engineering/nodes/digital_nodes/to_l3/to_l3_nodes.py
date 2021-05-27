import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df


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

# DIGITAL WEB MONTHLY

def node_compute_int_soc_web_monthly_features(
        df_soc_web_daily: pyspark.sql.DataFrame,
        df_level_priority: pyspark.sql.DataFrame,
        config_soc_web_monthly_agg: Dict[str, Any],
        config_soc_web_monthly_stats: Dict[str, Any],
        config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk: Dict[
            str, Any
        ],
        config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk: Dict[
            str, Any
        ],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_soc_web_daily]):
        return get_spark_empty_df()
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    df_soc_web_daily = df_soc_web_daily.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    source_partition_col = "partition_date"
    data_frame = df_soc_web_daily
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    if len(mvv_array) == 0:
        return get_spark_empty_df()

    partition_num_per_job = 7
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    logging.info(f"mvv_new: {mvv_new}")

    add_list = mvv_new
    CNTX = load_context(Path.cwd(), env=conf)
    filepath = "l3_soc_web_monthly_features_int"
    first_item = add_list[-1]
    logging.info(f"first_item: {first_item}")
    add_list.remove(first_item)

    sno = 0
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_web_daily_small = data_frame.filter(
            F.col(source_partition_col).isin(*[curr_item])
        )
        sno += 1
        output_df = node_compute_chunk_soc_web_monthly_features(
            df_soc_web_daily_small,
            config_soc_web_monthly_agg,
            config_soc_web_monthly_stats,
            config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk,
            config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk,
        )

        output_df = output_df.withColumn("sno", F.lit(sno))

        CNTX.catalog.save(filepath, output_df)

    logging.info("begin running for dates {0}".format(str(first_item)))
    df_soc_web_daily_small = data_frame.filter(
        F.col(source_partition_col).isin(*[first_item])
    )
    sno += 1
    output_df = node_compute_chunk_soc_web_monthly_features(
        df_soc_web_daily_small,
        config_soc_web_monthly_agg,
        config_soc_web_monthly_stats,
        config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk,
        config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    output_df = output_df.withColumn("sno", F.lit(1))
    logging.info("__COMPLETED__")
    return output_df


def node_compute_chunk_soc_web_monthly_features(
        df_soc_web_daily: pyspark.sql.DataFrame,
        config_soc_web_monthly_agg: Dict[str, Any],
        config_soc_web_monthly_stats: Dict[str, Any],
        config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk: Dict[
            str, Any
        ],
        config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk: Dict[
            str, Any
        ],
) -> pyspark.sql.DataFrame:
    df_soc_web_monthly_sum_features = node_from_config(
        df_soc_web_daily, config_soc_web_monthly_agg
    )
    logging.info("1.completed: config_soc_web_monthly_agg")
    df_soc_web_monthly_stats = node_from_config(
        df_soc_web_daily, config_soc_web_monthly_stats
    )
    logging.info("2.completed: config_soc_web_monthly_stats")
    df_final_sum = df_soc_web_monthly_sum_features.join(
        df_soc_web_monthly_stats,
        on=["mobile_no", "start_of_month", "subscription_identifier"],
        how="left",
    )
    logging.info("3.completed: join sum features and daily stats")

    # -> Download Traffic
    df_soc_web_monthly_popular_app_rank_download_traffic = node_from_config(
        df_soc_web_daily,
        config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk,
    )

    df_soc_web_monthly_most_popular_app_by_download_traffic = node_from_config(
        df_soc_web_monthly_popular_app_rank_download_traffic,
        config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    logging.info(
        "4.completed: config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk"
    )

    pk = ["mobile_no", "start_of_month", "level_1", "subscription_identifier"]
    df_fea_all = df_final_sum.join(
        df_soc_web_monthly_most_popular_app_by_download_traffic,
        on=pk,
        how="left",
    )
    logging.info("5.completed: saving final output..")
    return df_fea_all


def node_compute_final_soc_web_monthly_features(
        df_level_priority: pyspark.sql.DataFrame,
        df_soc_web_monthly_features_int: pyspark.sql.DataFrame,
        config_soc_web_monthly_final_sum: Dict[str, Any],
        config_soc_web_monthly_ratio_features: Dict[str, Any],
        config_soc_web_monthly_final_popular_app_rank_download_traffic_merge_chunk: Dict[
            str, Any
        ],
        config_soc_web_monthly_final_most_popular_app_by_download_traffic_merge_chunk: Dict[
            str, Any
        ],
        config_soc_web_monthly_level_stats: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()

    df = df_soc_web_monthly_features_int.join(
        F.broadcast(df_level_priority), on=["level_1"], how="inner"
    )

    df_soc_web_monthly_agg = node_from_config(
        df.select(
            "mobile_no",
            "subscription_identifier",
            "start_of_month",
            "sno",
            "soc_web_monthly_all_download_traffic",
        ).distinct(),
        config_soc_web_monthly_level_stats,
    )

    df_final_sum = node_from_config(df, config_soc_web_monthly_final_sum).join(
        df_soc_web_monthly_agg,
        on=["mobile_no", "start_of_month", "subscription_identifier"],
        how="left",
    )

    # -> Ratio Features
    df_soc_web_monthly_ratio_features = node_from_config(
        df_final_sum, config_soc_web_monthly_ratio_features
    )

    # -> Download Traffic
    df_soc_web_monthly_popular_app_rank_download_traffic = node_from_config(
        df,
        config_soc_web_monthly_final_popular_app_rank_download_traffic_merge_chunk,
    )

    df_soc_web_monthly_most_popular_app_by_download_traffic = node_from_config(
        df_soc_web_monthly_popular_app_rank_download_traffic,
        config_soc_web_monthly_final_most_popular_app_by_download_traffic_merge_chunk,
    )

    pk = ["mobile_no", "start_of_month", "level_1", "subscription_identifier"]
    df_fea_all = df_soc_web_monthly_ratio_features.join(
        df_soc_web_monthly_most_popular_app_by_download_traffic,
        on=pk,
        how="left",
    )
    return df_fea_all


def node_soc_web_monthly_user_category_granularity_features(
        df_combined_soc_app_daily_and_hourly_agg: pyspark.sql.DataFrame,
        df_level_priority: pyspark.sql.DataFrame,
        config_soc_web_monthly_popular_category_by_download_volume: Dict[str, Any],
        config_soc_web_monthly_most_popular_category_by_download_volume: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_combined_soc_app_daily_and_hourly_agg]):
        return get_spark_empty_df()
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    df_combined_soc_app_daily_and_hourly_agg = (
        df_combined_soc_app_daily_and_hourly_agg.join(
            df_level_priority, on=["level_1"], how="inner"
        )
    )

    df_monthly_popular_category_by_download_volume = node_from_config(
        df_combined_soc_app_daily_and_hourly_agg,
        config_soc_web_monthly_popular_category_by_download_volume,
    )
    df_monthly_most_popular_category_by_download_volume = node_from_config(
        df_monthly_popular_category_by_download_volume,
        config_soc_web_monthly_most_popular_category_by_download_volume,
    )

    return df_monthly_most_popular_category_by_download_volume