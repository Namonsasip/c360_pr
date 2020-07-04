import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config, expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check,\
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)

# Defaulted date in DAC is  exception_partitions=["2020-04-01"] as we are reading from April Starting


def generate_l3_fav_streaming_day(input_df, app_list):
    if check_empty_dfs([input_df]):
        return input_df

    spark = get_spark_session()
    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext
    ctx = ProjectContext(str(Path.cwd()), env=conf)

    for each_app in app_list:
        df = spark.sql("""
            select
                access_method_num, 
                national_id_card, 
                subscription_identifier,
                start_of_month,
                day_of_week as fav_{each_app}_streaming_day_of_week,
                download_kb_traffic_{each_app}_sum 
            from input_df
            where {each_app}_by_download_rank = 1
            and download_kb_traffic_{each_app}_sum > 0
        """.format(each_app=each_app))

        ctx.catalog.save("l3_streaming_fav_{}_streaming_day_of_week_feature"
                         .format(each_app), df)

    return None


def dac_for_streaming_to_l3_pipeline_from_l1(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name=target_table_name,
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=["2020-04-01"])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


# def dac_for_l3_streaming_fav_tv_show_by_episode_watched(input_df: DataFrame, cust_df: DataFrame):
#     ################################# Start Implementing Data availability checks #############################
#     if check_empty_dfs([input_df, cust_df]):
#         return [get_spark_empty_df(), get_spark_empty_df]
#
#     cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="monthly", par_col="partition_month",
#                                                       target_table_name="l3_billing_and_payments_monthly_overdue_bills")
#
#     if check_empty_dfs([input_df, cust_df]):
#         return [get_spark_empty_df(), get_spark_empty_df()]
#
#     min_value = union_dataframes_with_missing_cols(
#         [
#             input_df.select(
#                 F.max(F.col("start_of_month")).alias("max_date")),
#             cust_df.select(
#                 F.max(F.col("partition_month")).alias("max_date")),
#         ]
#     ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date
#
#     input_df = input_df.filter(F.col("start_of_month") <= min_value)
#     cust_df = cust_df.filter(F.col("partition_month") <= min_value)
#
#     ################################# End Implementing Data availability checks ###############################
#
#     return [input_df, cust_df]


def streaming_to_l3_content_type_features(input_df: DataFrame,
                                          int_l3_streaming_content_type_features_dict: dict,
                                          l3_streaming_fav_content_group_by_volume_dict: dict,
                                          l3_streaming_fav_content_group_by_duration_dict: dict) -> [DataFrame,
                                                                                                     DataFrame,
                                                                                                     DataFrame]:
    """
    :param input_df:
    :param int_l3_streaming_content_type_features_dict:
    :param l3_streaming_fav_content_group_by_volume_dict:
    :param l3_streaming_fav_content_group_by_duration_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l3_streaming_fav_content_group_by_duration_dict["output_catalog"],
        exception_partitions=["2020-04-01"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_content_type_features = node_from_config(small_df, int_l3_streaming_content_type_features_dict)

        l3_streaming_fav_content_group_by_volume = node_from_config(int_l3_streaming_content_type_features,
                                                                    l3_streaming_fav_content_group_by_volume_dict)
        CNTX.catalog.save(l3_streaming_fav_content_group_by_volume_dict["output_catalog"],
                          l3_streaming_fav_content_group_by_volume)

        l3_streaming_fav_content_group_by_duration = node_from_config(int_l3_streaming_content_type_features,
                                                                      l3_streaming_fav_content_group_by_duration_dict)
        CNTX.catalog.save(l3_streaming_fav_content_group_by_duration_dict["output_catalog"],
                          l3_streaming_fav_content_group_by_duration)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_content_type_features = node_from_config(small_df, int_l3_streaming_content_type_features_dict)

    l3_streaming_fav_content_group_by_volume = node_from_config(int_l3_streaming_content_type_features,
                                                                l3_streaming_fav_content_group_by_volume_dict)

    l3_streaming_fav_content_group_by_duration = node_from_config(int_l3_streaming_content_type_features,
                                                                  l3_streaming_fav_content_group_by_duration_dict)

    return [l3_streaming_fav_content_group_by_volume, l3_streaming_fav_content_group_by_duration]


def streaming_to_l3_tv_channel_type_features(input_df: DataFrame,
                                             int_l3_streaming_tv_channel_features_dict: dict,
                                             l3_streaming_fav_tv_channel_by_volume_dict: dict,
                                             l3_streaming_fav_tv_channel_by_duration_dict: dict,
                                             ) -> [DataFrame, DataFrame]:
    """
    :param input_df:
    :param int_l3_streaming_tv_channel_features_dict:
    :param l3_streaming_fav_tv_channel_by_volume_dict:
    :param l3_streaming_fav_tv_channel_by_duration_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l3_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
        exception_partitions=["2020-04-01"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_tv_channel_features = node_from_config(small_df, int_l3_streaming_tv_channel_features_dict)
        CNTX.catalog.save(int_l3_streaming_tv_channel_features_dict["output_catalog"],
                          int_l3_streaming_tv_channel_features)

        l3_streaming_fav_tv_channel_by_volume = node_from_config(int_l3_streaming_tv_channel_features,
                                                                 l3_streaming_fav_tv_channel_by_volume_dict)
        CNTX.catalog.save(l3_streaming_fav_tv_channel_by_volume_dict["output_catalog"],
                          l3_streaming_fav_tv_channel_by_volume)

        l3_streaming_fav_tv_channel_by_duration = node_from_config(int_l3_streaming_tv_channel_features,
                                                                   l3_streaming_fav_tv_channel_by_duration_dict)
        CNTX.catalog.save(l3_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
                          l3_streaming_fav_tv_channel_by_duration)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_tv_channel_features = node_from_config(small_df, int_l3_streaming_tv_channel_features_dict)

    l3_streaming_fav_tv_channel_by_volume = node_from_config(int_l3_streaming_tv_channel_features,
                                                             l3_streaming_fav_tv_channel_by_volume_dict)

    l3_streaming_fav_tv_channel_by_duration = node_from_config(int_l3_streaming_tv_channel_features,
                                                               l3_streaming_fav_tv_channel_by_duration_dict)

    return [l3_streaming_fav_tv_channel_by_volume, l3_streaming_fav_tv_channel_by_duration]


def streaming_streaming_fav_tv_show_by_episode_watched_features(
        input_df: DataFrame,
        int_l3_streaming_tv_show_features_dict,
        l3_streaming_fav_tv_show_by_episode_watched_dict) -> DataFrame:
    """
    :param input_df:
    :param int_l3_streaming_tv_show_features_dict:
    :param l3_streaming_fav_tv_show_by_episode_watched_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l3_streaming_fav_tv_show_by_episode_watched_dict["output_catalog"],
        exception_partitions=["2020-04-01"])

    if check_empty_dfs([input_df]):
        return  get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_tv_show_features = node_from_config(small_df, int_l3_streaming_tv_show_features_dict)

        l3_streaming_fav_tv_show_by_episode_watched = node_from_config(int_l3_streaming_tv_show_features,
                                                                 l3_streaming_fav_tv_show_by_episode_watched_dict)
        CNTX.catalog.save(l3_streaming_fav_tv_show_by_episode_watched_dict["output_catalog"],
                          l3_streaming_fav_tv_show_by_episode_watched)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_tv_show_features = node_from_config(small_df, int_l3_streaming_tv_show_features_dict)

    l3_streaming_fav_tv_show_by_episode_watched = node_from_config(int_l3_streaming_tv_show_features,
                                                                   l3_streaming_fav_tv_show_by_episode_watched_dict)

    return l3_streaming_fav_tv_show_by_episode_watched


def streaming_fav_service_download_traffic_visit_count(
        input_df: DataFrame,
        int_l3_streaming_service_feature_dict,
        favourite_dict: dict,
        second_favourite_dict: dict,
        fav_count_dict: dict) -> [DataFrame, DataFrame, DataFrame]:

    """
    :param input_df:
    :param int_l3_streaming_service_feature_dict:
    :param favourite_dict:
    :param second_favourite_dict:
    :param fav_count_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=fav_count_dict["output_catalog"],
        exception_partitions=["2020-04-01"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_service_feature = node_from_config(small_df, int_l3_streaming_service_feature_dict)

        favourite_video = node_from_config(int_l3_streaming_service_feature, favourite_dict)
        CNTX.catalog.save(favourite_dict["output_catalog"],
                          favourite_video)

        second_favourite = node_from_config(int_l3_streaming_service_feature, second_favourite_dict)
        CNTX.catalog.save(second_favourite_dict["output_catalog"],
                          second_favourite)

        fav_count = node_from_config(int_l3_streaming_service_feature, fav_count_dict)
        CNTX.catalog.save(fav_count_dict["output_catalog"],
                          fav_count)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_service_feature = node_from_config(small_df, int_l3_streaming_service_feature_dict)

    favourite_video = node_from_config(int_l3_streaming_service_feature, favourite_dict)
    CNTX.catalog.save(favourite_dict["output_catalog"],
                      favourite_video)

    second_favourite = node_from_config(int_l3_streaming_service_feature, second_favourite_dict)
    CNTX.catalog.save(second_favourite_dict["output_catalog"],
                      second_favourite)

    fav_count = node_from_config(int_l3_streaming_service_feature, fav_count_dict)
    CNTX.catalog.save(fav_count_dict["output_catalog"],
                      fav_count)

    return [favourite_video, second_favourite, fav_count]
