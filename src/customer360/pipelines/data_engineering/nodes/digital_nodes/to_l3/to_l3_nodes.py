import pyspark as pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from typing import Dict, Any
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


def node_compute_int_soc_app_monthly_features(
    df_soc_app_daily: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_soc_app_monthly_sum_features: Dict[str, Any],
    config_soc_app_monthly_stats: Dict[str, Any],
    config_soc_app_monthly_popular_app_rank_visit_count_merge_chunk: Dict[str, Any],
    config_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk: Dict[str, Any],
    config_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk: Dict[str, Any],
    config_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk: Dict[
        str, Any
    ],
    config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk: Dict[
        str, Any
    ],
    config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk: Dict[
        str, Any
    ],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_soc_app_daily]):
        return get_spark_empty_df()
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()

    df_soc_app_daily = df_soc_app_daily.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    source_partition_col = "partition_date"
    data_frame = df_soc_app_daily
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
    filepath = "l3_soc_app_monthly_features_int"
    first_item = add_list[-1]
    logging.info(f"first_item: {first_item}")
    add_list.remove(first_item)

    sno = 0
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_app_daily_small = data_frame.filter(
            F.col(source_partition_col).isin(*[curr_item])
        )
        sno += 1
        output_df = node_compute_chunk_soc_app_monthly_features(
            df_soc_app_daily_small,
            config_soc_app_monthly_sum_features,
            config_soc_app_monthly_stats,
            config_soc_app_monthly_popular_app_rank_visit_count_merge_chunk,
            config_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk,
            config_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk,
            config_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk,
            config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk,
            config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk,
        )

        output_df = output_df.withColumn("sno", F.lit(sno))

        CNTX.catalog.save(filepath, output_df)

    logging.info("begin running for dates {0}".format(str(first_item)))
    df_soc_app_daily_small = data_frame.filter(
        F.col(source_partition_col).isin(*[first_item])
    )
    sno += 1
    output_df = node_compute_chunk_soc_app_monthly_features(
        df_soc_app_daily_small,
        config_soc_app_monthly_sum_features,
        config_soc_app_monthly_stats,
        config_soc_app_monthly_popular_app_rank_visit_count_merge_chunk,
        config_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk,
        config_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk,
        config_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk,
        config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk,
        config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    output_df = output_df.withColumn("sno", F.lit(1))
    logging.info("__COMPLETED__")
    return output_df


def digital_mobile_app_category_agg_timeband_monthly(Mobile_app_timeband: DataFrame, app_categories_master: DataFrame,
                                                     mobile_app_timeband_sql: dict):
    import os, subprocess
    ##check missing data##
    if check_empty_dfs([Mobile_app_timeband]):
        return get_spark_empty_df()
    # where data timeband
    p_partition = str(os.getenv("RUN_PARTITION", "no_input"))
    if (p_partition != 'no_input'):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["starttime"][0:8] == p_partition)

    # where timeband
    if (timeband == "Morning"):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 6).filter(
            Mobile_app_timeband["ld_hour"] <= 11)
    elif (timeband == "Afternoon"):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 12).filter(
            Mobile_app_timeband["ld_hour"] <= 17)
    elif (timeband == "Evening"):
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 18).filter(
            Mobile_app_timeband["ld_hour"] <= 23)
    else:
        Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 0).filter(
            Mobile_app_timeband["ld_hour"] <= 5)

    # where this column more than 0
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("dw_byte") > 0)
    Mobile_app_timeband = Mobile_app_timeband.where(f.col("ul_kbyte") > 0)

    # join master
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed("msisdn", "mobile_no").join(
        f.broadcast(app_categories_master),
        on=[app_categories_master.application_id == Mobile_app_timeband.application],
        how="inner",
    )

    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed(category_level, 'category_name')
    Mobile_app_timeband = Mobile_app_timeband.withColumnRenamed('ul_kbyte', 'ul_byte')
    Mobile_app_timeband = Mobile_app_timeband.withColumn('start_of_month',
                                                         f.concat(col("starttime")[0:4], f.lit('-'),
                                                         f.concat(col("starttime")[5:2]), f.lit('-01')))

    f.CONCAT(f.SUBSTRING('2021-05-01', 1, 7), '-01') as start_of_month

    df_return = node_from_config(Mobile_app_timeband, mobile_app_timeband_sql)
    return df_return

# def digital_mobile_app_category_agg_timeband_monthly (Mobile_app_timeband: DataFrame):
#     import os, subprocess
#     ##check missing data##
#     if check_empty_dfs([Mobile_app_timeband]):
#         return get_spark_empty_df()
#     # where data timeband
#     p_partition = str(os.getenv("RUN_PARTITION", "no_input"))
#     if (p_partition != 'no_input'):
#         Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["starttime"][0:8] == p_partition)
#
#     # where timeband
#     # if (timeband == "Morning"):
#     #     Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 6).filter(
#     #         Mobile_app_timeband["ld_hour"] <= 11)
#     # elif (timeband == "Afternoon"):
#     #     Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 12).filter(
#     #         Mobile_app_timeband["ld_hour"] <= 17)
#     # elif (timeband == "Evening"):
#     #     Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 18).filter(
#     #         Mobile_app_timeband["ld_hour"] <= 23)
#     # else:
#     #     Mobile_app_timeband = Mobile_app_timeband.filter(Mobile_app_timeband["ld_hour"] >= 0).filter(
#     #         Mobile_app_timeband["ld_hour"] <= 5)
#
#     # join master
#
#
#     Mobile_app_timeband = Mobile_app_timeband.groupBy("subscription_identifier", "mobile_no",
#                                                                        "category_name", "priority"
#                                                                        , "start_of_month").agg(
#         f.sum("total_visit_count").alias("total_visit_count"),
#         f.sum("total_visit_duration").alias("total_visit_duration"),
#         f.sum("total_download_byte").alias("total_download_byte"),
#         f.sum("total_upload_byte").alias("total_upload_byte"),
#         f.sum("total_volume_byte").alias("total_volume_byte")
#     )
#
#     Mobile_app_timeband = Mobile_app_timeband.withColumn("start_of_month",f.to_date(f.date_trunc('month', "event_partition_date")))
#     return Mobile_app_timeband


