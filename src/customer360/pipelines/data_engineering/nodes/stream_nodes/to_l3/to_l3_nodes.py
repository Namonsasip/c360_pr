import pyspark
import uuid
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from typing import Dict, Any
from customer360.utilities.re_usable_functions import (
    check_empty_dfs,
    data_non_availability_and_missing_check,
    union_dataframes_with_missing_cols,
    add_event_week_and_month_from_yyyymmdd,
    gen_max_sql,
    execute_sql,
    clean_favourite_category,
    execute_sql,
    join_all,
)
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)

# Defaulted date in DAC is  exception_partitions=["2020-04-01"] as we are reading from April Starting


def series_title_master(vimmi_usage: DataFrame) -> DataFrame:
    """

    :param vimmi_usage:
    :return:
    """
    master = (
        vimmi_usage.select("series_title", "title")
        .groupBy("series_title")
        .agg(F.countDistinct("title").alias("total_episode_count"))
    )

    return master


def generate_l3_fav_streaming_day(input_df, app_list):
    if check_empty_dfs([input_df]):
        return input_df

    spark = get_spark_session()
    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext

    ctx = ProjectContext(str(Path.cwd()), env=conf)

    for each_app in app_list:
        df = spark.sql(
            """
            select
                subscription_identifier,
                start_of_month,
                day_of_week as fav_{each_app}_streaming_day_of_week,
                download_kb_traffic_{each_app}_sum 
            from input_df
            where {each_app}_by_download_rank = 1
            and download_kb_traffic_{each_app}_sum > 0
        """.format(
                each_app=each_app
            )
        )

        ctx.catalog.save(
            "l3_streaming_fav_{}_streaming_day_of_week_feature".format(each_app), df
        )

    return None


def dac_for_streaming_to_l3_pipeline_from_l1(
    input_df: DataFrame, target_table_name: str
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        target_table_name=target_table_name,
        missing_data_check_flg="Y",
        exception_partitions=["2020-04-01"],
    )

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


def streaming_to_l3_content_type_features(
    input_df: DataFrame,
    int_l3_streaming_content_type_features_dict: dict,
    l3_streaming_fav_content_group_by_volume_dict: dict,
    l3_streaming_fav_content_group_by_duration_dict: dict,
) -> [DataFrame, DataFrame, DataFrame]:
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
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name=l3_streaming_fav_content_group_by_duration_dict[
            "output_catalog"
        ],
        exception_partitions=["2020-04-01"],
    )

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select("start_of_month").distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_content_type_features = node_from_config(
            small_df, int_l3_streaming_content_type_features_dict
        )

        l3_streaming_fav_content_group_by_volume = node_from_config(
            int_l3_streaming_content_type_features,
            l3_streaming_fav_content_group_by_volume_dict,
        )
        CNTX.catalog.save(
            l3_streaming_fav_content_group_by_volume_dict["output_catalog"],
            l3_streaming_fav_content_group_by_volume,
        )

        l3_streaming_fav_content_group_by_duration = node_from_config(
            int_l3_streaming_content_type_features,
            l3_streaming_fav_content_group_by_duration_dict,
        )
        CNTX.catalog.save(
            l3_streaming_fav_content_group_by_duration_dict["output_catalog"],
            l3_streaming_fav_content_group_by_duration,
        )

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_content_type_features = node_from_config(
        small_df, int_l3_streaming_content_type_features_dict
    )

    l3_streaming_fav_content_group_by_volume = node_from_config(
        int_l3_streaming_content_type_features,
        l3_streaming_fav_content_group_by_volume_dict,
    )

    l3_streaming_fav_content_group_by_duration = node_from_config(
        int_l3_streaming_content_type_features,
        l3_streaming_fav_content_group_by_duration_dict,
    )

    return [
        l3_streaming_fav_content_group_by_volume,
        l3_streaming_fav_content_group_by_duration,
    ]


def streaming_to_l3_tv_channel_type_features(
    input_df: DataFrame,
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
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name=l3_streaming_fav_tv_channel_by_duration_dict[
            "output_catalog"
        ],
        exception_partitions=["2020-04-01"],
    )

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select("start_of_month").distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_tv_channel_features = node_from_config(
            small_df, int_l3_streaming_tv_channel_features_dict
        )
        CNTX.catalog.save(
            int_l3_streaming_tv_channel_features_dict["output_catalog"],
            int_l3_streaming_tv_channel_features,
        )

        l3_streaming_fav_tv_channel_by_volume = node_from_config(
            int_l3_streaming_tv_channel_features,
            l3_streaming_fav_tv_channel_by_volume_dict,
        )
        CNTX.catalog.save(
            l3_streaming_fav_tv_channel_by_volume_dict["output_catalog"],
            l3_streaming_fav_tv_channel_by_volume,
        )

        l3_streaming_fav_tv_channel_by_duration = node_from_config(
            int_l3_streaming_tv_channel_features,
            l3_streaming_fav_tv_channel_by_duration_dict,
        )
        CNTX.catalog.save(
            l3_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
            l3_streaming_fav_tv_channel_by_duration,
        )

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_tv_channel_features = node_from_config(
        small_df, int_l3_streaming_tv_channel_features_dict
    )

    l3_streaming_fav_tv_channel_by_volume = node_from_config(
        int_l3_streaming_tv_channel_features, l3_streaming_fav_tv_channel_by_volume_dict
    )

    l3_streaming_fav_tv_channel_by_duration = node_from_config(
        int_l3_streaming_tv_channel_features,
        l3_streaming_fav_tv_channel_by_duration_dict,
    )

    return [
        l3_streaming_fav_tv_channel_by_volume,
        l3_streaming_fav_tv_channel_by_duration,
    ]


def streaming_streaming_fav_tv_show_by_episode_watched_features(
    input_df: DataFrame,
    int_l3_streaming_tv_show_features_dict: dict,
    l3_streaming_fav_tv_show_by_episode_watched_dict: dict,
    int_l3_streaming_genre_dict: dict,
    l3_streaming_genre_dict: dict,
) -> [DataFrame, DataFrame]:
    """
    :param input_df:
    :param int_l3_streaming_tv_show_features_dict:
    :param l3_streaming_fav_tv_show_by_episode_watched_dict:
    :param int_l3_streaming_genre_dict:
    :param l3_streaming_genre_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name=l3_streaming_fav_tv_show_by_episode_watched_dict[
            "output_catalog"
        ],
        exception_partitions=["2020-04-01"],
    )

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]
    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select("start_of_month").distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_tv_show_features = node_from_config(
            small_df, int_l3_streaming_tv_show_features_dict
        )

        l3_streaming_fav_tv_show_by_episode_watched = node_from_config(
            int_l3_streaming_tv_show_features,
            l3_streaming_fav_tv_show_by_episode_watched_dict,
        )
        CNTX.catalog.save(
            l3_streaming_fav_tv_show_by_episode_watched_dict["output_catalog"],
            l3_streaming_fav_tv_show_by_episode_watched,
        )

        int_l3_streaming_genre_features = node_from_config(
            small_df, int_l3_streaming_genre_dict
        )

        l3_streaming_genre = node_from_config(
            int_l3_streaming_genre_features, l3_streaming_genre_dict
        )
        CNTX.catalog.save(l3_streaming_genre_dict["output_catalog"], l3_streaming_genre)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_tv_show_features = node_from_config(
        small_df, int_l3_streaming_tv_show_features_dict
    )

    l3_streaming_fav_tv_show_by_episode_watched = node_from_config(
        int_l3_streaming_tv_show_features,
        l3_streaming_fav_tv_show_by_episode_watched_dict,
    )

    int_l3_streaming_genre_features = node_from_config(
        small_df, int_l3_streaming_genre_dict
    )

    l3_streaming_genre = node_from_config(
        int_l3_streaming_genre_features, l3_streaming_genre_dict
    )

    return [l3_streaming_fav_tv_show_by_episode_watched, l3_streaming_genre]


def streaming_fav_service_download_traffic_visit_count(
    input_df: DataFrame,
    int_l3_streaming_service_feature_dict,
    favourite_dict: dict,
    second_favourite_dict: dict,
    fav_count_dict: dict,
) -> [DataFrame, DataFrame, DataFrame]:
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
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name=fav_count_dict["output_catalog"],
        exception_partitions=["2020-04-01"],
    )

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select("start_of_month").distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        int_l3_streaming_service_feature = node_from_config(
            small_df, int_l3_streaming_service_feature_dict
        )

        favourite_video = node_from_config(
            int_l3_streaming_service_feature, favourite_dict
        )
        CNTX.catalog.save(favourite_dict["output_catalog"], favourite_video)

        second_favourite = node_from_config(
            int_l3_streaming_service_feature, second_favourite_dict
        )
        CNTX.catalog.save(second_favourite_dict["output_catalog"], second_favourite)

        fav_count = node_from_config(int_l3_streaming_service_feature, fav_count_dict)
        CNTX.catalog.save(fav_count_dict["output_catalog"], fav_count)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_service_feature = node_from_config(
        small_df, int_l3_streaming_service_feature_dict
    )

    favourite_video = node_from_config(int_l3_streaming_service_feature, favourite_dict)
    CNTX.catalog.save(favourite_dict["output_catalog"], favourite_video)

    second_favourite = node_from_config(
        int_l3_streaming_service_feature, second_favourite_dict
    )
    CNTX.catalog.save(second_favourite_dict["output_catalog"], second_favourite)

    fav_count = node_from_config(int_l3_streaming_service_feature, fav_count_dict)
    CNTX.catalog.save(fav_count_dict["output_catalog"], fav_count)

    return [favourite_video, second_favourite, fav_count]


def streaming_to_l3_fav_tv_show_by_share_of_completed_episodes(
    vimmi_usage_daily: DataFrame,
    streaming_series_title_master: DataFrame,
    int_l3_streaming_share_of_completed_episodes_features_dict: dict,
    int_l3_streaming_share_of_completed_episodes_ratio_features_dict: dict,
    l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict: dict,
) -> DataFrame:
    """

    :param vimmi_usage_daily:
    :param streaming_series_title_master:
    :param int_l3_streaming_share_of_completed_episodes_features_dict:
    :param int_l3_streaming_share_of_completed_episodes_ratio_features_dict:
    :param l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([vimmi_usage_daily]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=vimmi_usage_daily,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name=l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict[
            "output_catalog"
        ],
        exception_partitions=["2020-02-01"],
    )

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select("start_of_month").distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        selective_df = small_df.select(
            "subscription_identifier",
            "start_of_month",
            "content_group",
            "title",
            "series_title",
        )

        # share_of_completed_episodes feature
        int_l3_streaming_share_of_completed_episodes_features = node_from_config(
            selective_df, int_l3_streaming_share_of_completed_episodes_features_dict
        )

        int_l3_streaming_share_of_completed_episodes_ratio_features_temp = (
            int_l3_streaming_share_of_completed_episodes_features.join(
                streaming_series_title_master, on="series_title", how="left"
            )
        )

        int_l3_streaming_share_of_completed_episodes_ratio_features_temp = (
            int_l3_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
                "share_of_completed_episodes",
                (
                    F.col("episode_watched_count")
                    / F.coalesce(F.col("total_episode_count"), F.lit(1))
                ),
            )
        )

        int_l3_streaming_share_of_completed_episodes_ratio_features = node_from_config(
            int_l3_streaming_share_of_completed_episodes_ratio_features_temp,
            int_l3_streaming_share_of_completed_episodes_ratio_features_dict,
        )

        l3_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
            int_l3_streaming_share_of_completed_episodes_ratio_features,
            l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict,
        )

        CNTX.catalog.save(
            l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict[
                "output_catalog"
            ],
            l3_streaming_fav_tv_show_by_share_of_completed_episodes,
        )

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    selective_df = small_df.select(
        "subscription_identifier",
        "start_of_month",
        "content_group",
        "title",
        "series_title",
    )

    # share_of_completed_episodes feature
    int_l3_streaming_share_of_completed_episodes_features = node_from_config(
        selective_df, int_l3_streaming_share_of_completed_episodes_features_dict
    )

    int_l3_streaming_share_of_completed_episodes_ratio_features_temp = (
        int_l3_streaming_share_of_completed_episodes_features.join(
            streaming_series_title_master, on="series_title", how="left"
        )
    )
    int_l3_streaming_share_of_completed_episodes_ratio_features_temp = (
        int_l3_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
            "share_of_completed_episodes",
            (
                F.col("episode_watched_count")
                / F.coalesce(F.col("total_episode_count"), F.lit(1))
            ),
        )
    )

    int_l3_streaming_share_of_completed_episodes_ratio_features = node_from_config(
        int_l3_streaming_share_of_completed_episodes_ratio_features_temp,
        int_l3_streaming_share_of_completed_episodes_ratio_features_dict,
    )

    l3_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
        int_l3_streaming_share_of_completed_episodes_ratio_features,
        l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict,
    )

    return l3_streaming_fav_tv_show_by_share_of_completed_episodes


def streaming_favourite_start_hour_of_day_func(input_df: DataFrame) -> None:
    """
    :param input_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return None
    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name="l3_streaming_traffic_consumption_time_based_features",
    )
    if check_empty_dfs([input_df]):
        return None
    ################################# End Implementing Data availability checks ###############################
    def process_massive_processing_favourite_hour(data_frame: DataFrame):
        """
        :param data_frame:
        :return:
        """
        dictionary = [
            {
                "filter_condition": "youtube,youtube_go,youtubebyclick",
                "output_col": "fav_youtube_streaming_hour_of_day",
            },
            {
                "filter_condition": "trueid",
                "output_col": "fav_trueid_streaming_hour_of_day",
            },
            {
                "filter_condition": "truevisions",
                "output_col": "fav_truevisions_streaming_hour_of_day",
            },
            {
                "filter_condition": "monomaxx",
                "output_col": "fav_monomaxx_streaming_hour_of_day",
            },
            {
                "filter_condition": "qqlive",
                "output_col": "fav_qqlive_streaming_hour_of_day",
            },
            {
                "filter_condition": "facebook",
                "output_col": "fav_facebook_streaming_hour_of_day",
            },
            {
                "filter_condition": "linetv",
                "output_col": "fav_linetv_streaming_hour_of_day",
            },
            {
                "filter_condition": "ais_play",
                "output_col": "fav_ais_play_streaming_hour_of_day",
            },
            {
                "filter_condition": "netflix",
                "output_col": "fav_netflix_streaming_hour_of_day",
            },
            {
                "filter_condition": "viu,viutv",
                "output_col": "fav_viu_streaming_hour_of_day",
            },
            {
                "filter_condition": "iflix",
                "output_col": "fav_iflix_streaming_hour_of_day",
            },
            {
                "filter_condition": "spotify",
                "output_col": "fav_spotify_streaming_hour_of_day",
            },
            {
                "filter_condition": "jooxmusic",
                "output_col": "fav_jooxmusic_streaming_hour_of_day",
            },
            {
                "filter_condition": "twitchtv",
                "output_col": "fav_twitchtv_streaming_hour_of_day",
            },
            {
                "filter_condition": "bigo",
                "output_col": "fav_bigo_streaming_hour_of_day",
            },
            {
                "filter_condition": "valve_steam",
                "output_col": "fav_valve_steam_streaming_hour_of_day",
            },
        ]

        final_dfs = []
        win = Window.partitionBy(["subscription_identifier", "start_of_month"]).orderBy(
            F.col("download").desc()
        )
        for curr_dict in dictionary:
            filter_query = curr_dict["filter_condition"].split(",")
            output_col = curr_dict["output_col"]
            curr_item = data_frame.filter(
                F.lower(F.col("application_name")).isin(filter_query)
            )
            curr_item = curr_item.groupBy(
                ["subscription_identifier", "hour", "start_of_month"]
            ).agg(F.sum("dw_kbyte").alias("download"))
            curr_item = curr_item.withColumn("rnk", F.row_number().over(win)).where(
                "rnk = 1"
            )

            curr_item = curr_item.select(
                "subscription_identifier",
                F.col("hour").alias(output_col),
                "start_of_month",
            )

            final_dfs.append(curr_item)

        union_df = union_dataframes_with_missing_cols(final_dfs)
        group_cols = ["subscription_identifier", "start_of_month"]

        final_df_str = gen_max_sql(union_df, "tmp_table_name", group_cols)
        merged_df = execute_sql(union_df, "tmp_table_name", final_df_str)

        CNTX = load_context(Path.cwd(), env=conf)
        CNTX.catalog.save("l3_streaming_favourite_start_time_hour_of_day", merged_df)

        return None

    def process_massive_processing_application_group(data_frame: DataFrame):
        """
        :param data_frame:
        :return:
        """
        morning = [7, 8, 9, 10, 11, 12]
        afternoon = [13, 14, 15, 16, 17, 18]
        evening = [19, 20, 21, 22, 23, 0]

        input_with_application = data_frame.withColumn(
            "time_of_day",
            F.when(F.col("hour").isin(morning), F.lit("morning")).otherwise(
                F.when(F.col("hour").isin(afternoon), F.lit("afternoon")).otherwise(
                    F.when(F.col("hour").isin(evening), F.lit("evening")).otherwise(
                        F.lit("night")
                    )
                )
            ),
        )

        weekend_type = ["Saturday", "Sunday"]
        input_with_application = input_with_application.withColumn(
            "day_type",
            F.when(
                F.date_format("event_partition_date", "EEEE").isin(weekend_type),
                F.lit("weekend"),
            ).otherwise(F.lit("weekday")),
        )

        final_dfs = []
        for curr_time_type in ["morning", "afternoon", "evening", "night"]:
            for app_group_type in ["videoplayers_editors", "music_audio", "game"]:
                v_time_type = curr_time_type
                v_app_group = app_group_type
                final_col = "share_of_{}_streaming_usage_{}_by_total".format(
                    v_time_type, app_group_type
                )
                filtered = input_with_application.filter(
                    F.lower(F.col("application_group")) == v_app_group
                )
                filtered_agg = filtered.groupBy(
                    ["subscription_identifier", "start_of_month"]
                ).agg(F.sum("dw_kbyte").alias("main_download"))

                curr_time_type_agg = (
                    filtered.filter(F.col("time_of_day") == v_time_type)
                    .groupBy(["subscription_identifier", "start_of_month"])
                    .agg(F.sum("dw_kbyte").alias("download"))
                )

                final_df = (
                    filtered_agg.join(
                        curr_time_type_agg,
                        ["subscription_identifier", "start_of_month"],
                    )
                    .withColumn(final_col, F.col("download") / F.col("main_download"))
                    .drop("download", "main_download")
                )
                final_dfs.append(final_df)

        for curr_time_type in ["weekend", "weekday"]:
            for app_group_type in ["videoplayers_editors", "music_audio", "game"]:
                v_time_type = curr_time_type
                v_app_group = app_group_type
                final_col = "share_of_{}_streaming_usage_{}_by_total".format(
                    v_time_type, app_group_type
                )
                filtered = input_with_application.filter(
                    F.lower(F.col("application_group")) == v_app_group
                )
                filtered_agg = filtered.groupBy(
                    ["subscription_identifier", "start_of_month"]
                ).agg(F.sum("dw_kbyte").alias("main_download"))

                curr_time_type_agg = (
                    filtered.filter(F.col("day_type") == v_time_type)
                    .groupBy(["subscription_identifier", "start_of_month"])
                    .agg(F.sum("dw_kbyte").alias("download"))
                )

                final_df = (
                    filtered_agg.join(
                        curr_time_type_agg,
                        ["subscription_identifier", "start_of_month"],
                    )
                    .withColumn(final_col, F.col("download") / F.col("main_download"))
                    .drop("download", "main_download")
                )
                final_dfs.append(final_df)

        union_df = union_dataframes_with_missing_cols(final_dfs)

        group_cols = ["subscription_identifier", "start_of_month"]

        final_df_str = gen_max_sql(union_df, "tmp_table_name", group_cols)
        merged_df = execute_sql(union_df, "tmp_table_name", final_df_str)

        CNTX = load_context(Path.cwd(), env=conf)
        CNTX.catalog.save(
            "l3_streaming_traffic_consumption_time_based_features", merged_df
        )

        return None

    application_list = [
        "youtube",
        "youtube_go",
        "youtubebyclick",
        "trueid",
        "truevisions",
        "monomaxx",
        "qqlive",
        "facebook",
        "linetv",
        "ais_play",
        "netflix",
        "viu",
        "viutv",
        "iflix",
        "spotify",
        "jooxmusic",
        "twitchtv",
        "bigo",
        "valve_steam",
    ]
    input_df_hour_based = input_df.filter(
        F.lower(F.col("application_name")).isin(application_list)
    )
    process_massive_processing_favourite_hour(input_df_hour_based)

    application_group = ["videoplayers_editors", "music_audio", "game"]
    input_df_group = input_df.filter(
        F.lower(F.col("application_group")).isin(application_group)
    )
    process_massive_processing_application_group(input_df_group)

    return None


def streaming_favourite_location_features_func(input_df: DataFrame) -> DataFrame:
    """
    :param input_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name="l3_streaming_favourite_location_features",
    )

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    dictionary = [
        {
            "filter_condition": "youtube,youtube_go,youtubebyclick",
            "output_col": "fav_youtube_streaming_base_station_id",
        },
        {
            "filter_condition": "trueid",
            "output_col": "fav_trueid_streaming_base_station_id",
        },
        {
            "filter_condition": "truevisions",
            "output_col": "fav_truevisions_streaming_base_station_id",
        },
        {
            "filter_condition": "monomaxx",
            "output_col": "fav_monomaxx_streaming_base_station_id",
        },
        {
            "filter_condition": "qqlive",
            "output_col": "fav_qqlive_streaming_base_station_id",
        },
        {
            "filter_condition": "facebook",
            "output_col": "fav_facebook_streaming_base_station_id",
        },
        {
            "filter_condition": "linetv",
            "output_col": "fav_linetv_streaming_base_station_id",
        },
        {
            "filter_condition": "ais_play",
            "output_col": "fav_ais_play_streaming_base_station_id",
        },
        {
            "filter_condition": "netflix",
            "output_col": "fav_netflix_streaming_base_station_id",
        },
        {
            "filter_condition": "viu,viutv",
            "output_col": "fav_viu_streaming_base_station_id",
        },
        {
            "filter_condition": "iflix",
            "output_col": "fav_iflix_streaming_base_station_id",
        },
        {
            "filter_condition": "spotify",
            "output_col": "fav_spotify_streaming_base_station_id",
        },
        {
            "filter_condition": "jooxmusic",
            "output_col": "fav_jooxmusic_streaming_base_station_id",
        },
        {
            "filter_condition": "twitchtv",
            "output_col": "fav_twitchtv_streaming_base_station_id",
        },
        {
            "filter_condition": "bigo",
            "output_col": "fav_bigo_streaming_base_station_id",
        },
        {
            "filter_condition": "valve_steam",
            "output_col": "fav_valve_steam_streaming_base_station_id",
        },
    ]

    final_dfs = []
    win = Window.partitionBy(["subscription_identifier", "start_of_month"]).orderBy(
        F.col("download").desc()
    )
    for curr_dict in dictionary:
        filter_query = curr_dict["filter_condition"].split(",")
        output_col = curr_dict["output_col"]
        curr_item = input_df.filter(
            F.lower(F.col("application_name")).isin(filter_query)
        )
        curr_item = curr_item.groupBy(
            ["subscription_identifier", "location_id", "start_of_month"]
        ).agg(F.sum("download").alias("download"))
        curr_item = curr_item.withColumn("rnk", F.row_number().over(win)).where(
            "rnk = 1"
        )
        curr_item = curr_item.select(
            "subscription_identifier",
            F.col("location_id").alias(output_col),
            "start_of_month",
        )
        final_dfs.append(curr_item)

    union_df = union_dataframes_with_missing_cols(final_dfs)
    group_cols = ["subscription_identifier", "start_of_month"]

    final_df_str = gen_max_sql(union_df, "tmp_table_name", group_cols)
    merged_df = execute_sql(union_df, "tmp_table_name", final_df_str)

    return merged_df


def streaming_favourite_quality_features_func(input_df: DataFrame) -> DataFrame:
    """
    :param input_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="monthly",
        par_col="event_partition_date",
        missing_data_check_flg="Y",
        target_table_name="l3_streaming_app_quality_features",
    )

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    dictionary = [
        {
            "filter_condition": "youtube,youtube_go,youtubebyclick",
            "output_col": "avg_streaming_quality_youtube",
        },
        {"filter_condition": "trueid", "output_col": "avg_streaming_quality_trueid"},
        {
            "filter_condition": "truevisions",
            "output_col": "avg_streaming_quality_truevisions",
        },
        {
            "filter_condition": "monomaxx",
            "output_col": "avg_streaming_quality_monomaxx",
        },
        {"filter_condition": "qqlive", "output_col": "avg_streaming_quality_qqlive"},
        {
            "filter_condition": "ais_play",
            "output_col": "avg_streaming_quality_ais_play",
        },
        {"filter_condition": "netflix", "output_col": "avg_streaming_quality_netflix"},
        {"filter_condition": "viu,viutv", "output_col": "avg_streaming_quality_viu"},
        {"filter_condition": "iflix", "output_col": "avg_streaming_quality_iflix"},
        {"filter_condition": "spotify", "output_col": "avg_streaming_quality_spotify"},
        {
            "filter_condition": "jooxmusic",
            "output_col": "avg_streaming_quality_jooxmusic",
        },
        {
            "filter_condition": "twitchtv",
            "output_col": "avg_streaming_quality_twitchtv",
        },
        {"filter_condition": "bigo", "output_col": "avg_streaming_quality_bigo"},
        {
            "filter_condition": "valve_steam",
            "output_col": "avg_streaming_quality_valve_steam",
        },
    ]

    final_dfs = []
    for curr_dict in dictionary:
        filter_query = curr_dict["filter_condition"].split(",")
        output_col = curr_dict["output_col"]
        curr_item = input_df.filter(
            F.lower(F.col("application_name")).isin(filter_query)
        )

        curr_item = curr_item.groupBy(
            ["subscription_identifier", "start_of_month"]
        ).agg(F.avg("calc_column").alias(output_col))
        final_dfs.append(curr_item)

    union_df = union_dataframes_with_missing_cols(final_dfs)
    group_cols = ["subscription_identifier", "start_of_month"]

    final_df_str = gen_max_sql(union_df, "tmp_table_name", group_cols)
    merged_df = execute_sql(union_df, "tmp_table_name", final_df_str)

    return merged_df


def __divide_chunks(arr, n):
    for i in range(0, len(arr), n):
        yield arr[i : i + n]


def node_compute_chunk_soc_app_monthly_features(
    df_soc_app_daily: pyspark.sql.DataFrame,
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

    df_soc_app_monthly_sum_features = node_from_config(
        df_soc_app_daily, config_soc_app_monthly_sum_features
    )
    logging.info("1.completed: config_soc_app_monthly_sum_features")

    df_soc_app_monthly_stats = node_from_config(
        df_soc_app_daily, config_soc_app_monthly_stats
    )
    logging.info("2.completed: config_soc_app_monthly_stats")

    df_final_sum = df_soc_app_monthly_sum_features.join(
        df_soc_app_monthly_stats, on=["mobile_no", "start_of_month"], how="left"
    )
    logging.info("3.completed: join sum features and daily stats")

    # -> Visit Counts
    df_soc_app_monthly_popular_app_rank_visit_count = node_from_config(
        df_soc_app_daily,
        config_soc_app_monthly_popular_app_rank_visit_count_merge_chunk,
    )

    df_soc_app_monthly_most_popular_app_by_visit_count = node_from_config(
        df_soc_app_monthly_popular_app_rank_visit_count,
        config_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk,
    )
    logging.info(
        "4.completed: config_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk"
    )

    # -> Visit Duration
    df_soc_app_monthly_popular_app_rank_visit_duration = node_from_config(
        df_soc_app_daily,
        config_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk,
    )

    df_soc_app_monthly_most_popular_app_by_visit_duration = node_from_config(
        df_soc_app_monthly_popular_app_rank_visit_duration,
        config_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk,
    )
    logging.info(
        "5.completed: config_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk"
    )

    # -> Download Traffic
    df_soc_app_monthly_popular_app_rank_download_traffic = node_from_config(
        df_soc_app_daily,
        config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk,
    )

    df_soc_app_monthly_most_popular_app_by_download_traffic = node_from_config(
        df_soc_app_monthly_popular_app_rank_download_traffic,
        config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    logging.info(
        "6.completed: config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk"
    )

    pk = ["mobile_no", "start_of_month", "level_1"]
    df_fea_all = (
        df_final_sum.join(
            df_soc_app_monthly_most_popular_app_by_visit_count,
            on=pk,
            how="left",
        )
        .join(
            df_soc_app_monthly_most_popular_app_by_visit_duration,
            on=pk,
            how="left",
        )
        .join(
            df_soc_app_monthly_most_popular_app_by_download_traffic,
            on=pk,
            how="left",
        )
    )
    logging.info("7.completed: saving final output..")
    return df_fea_all


def node_compute_final_soc_app_monthly_features(
    df_level_priority: pyspark.sql.DataFrame,
    config_soc_app_monthly_final_sum: Dict[str, Any],
    config_soc_app_monthly_ratio_features: Dict[str, Any],
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
    config_soc_app_monthly_agg: Dict[str, Any],
):
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_soc_app_monthly_features/"
    spark = get_spark_session()
    df = spark.read.parquet(int_path)
    df = df.join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    df_soc_app_monthly_agg = node_from_config(
        df.select(
            "mobile_no",
            "start_of_month",
            "sno",
            "soc_app_monthly_all_download_traffic",
            "soc_app_monthly_all_visit_counts",
            "soc_app_monthly_all_duration",
        ).distinct(),
        config_soc_app_monthly_agg,
    )

    df_final_sum = node_from_config(df, config_soc_app_monthly_final_sum).join(
        df_soc_app_monthly_agg, on=["mobile_no", "start_of_month"], how="left"
    )

    # -> Ratio Features
    df_soc_app_monthly_ratio_features = node_from_config(
        df_final_sum, config_soc_app_monthly_ratio_features
    )

    # -> Visit Counts
    df_soc_app_monthly_popular_app_rank_visit_count = node_from_config(
        df,
        config_soc_app_monthly_popular_app_rank_visit_count_merge_chunk,
    )

    df_soc_app_monthly_most_popular_app_by_visit_count = node_from_config(
        df_soc_app_monthly_popular_app_rank_visit_count,
        config_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk,
    )

    # -> Visit Duration
    df_soc_app_monthly_popular_app_rank_visit_duration = node_from_config(
        df,
        config_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk,
    )

    df_soc_app_monthly_most_popular_app_by_visit_duration = node_from_config(
        df_soc_app_monthly_popular_app_rank_visit_duration,
        config_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk,
    )

    # -> Download Traffic
    df_soc_app_monthly_popular_app_rank_download_traffic = node_from_config(
        df,
        config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk,
    )

    df_soc_app_monthly_most_popular_app_by_download_traffic = node_from_config(
        df_soc_app_monthly_popular_app_rank_download_traffic,
        config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )

    pk = ["mobile_no", "start_of_month", "level_1"]
    df_fea_all = (
        df_soc_app_monthly_ratio_features.join(
            df_soc_app_monthly_most_popular_app_by_visit_count,
            on=pk,
            how="left",
        )
        .join(
            df_soc_app_monthly_most_popular_app_by_visit_duration,
            on=pk,
            how="left",
        )
        .join(
            df_soc_app_monthly_most_popular_app_by_download_traffic,
            on=pk,
            how="left",
        )
    )
    return df_fea_all


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

    partition_num_per_job = 7
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    logging.info(f"mvv_new: {mvv_new}")
    add_list = mvv_new
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_soc_app_monthly_features/"

    first_item = add_list[-1]
    logging.info(f"first_item: {first_item}")
    add_list.remove(first_item)

    logging.info("begin running for dates {0}".format(str(first_item)))
    df_soc_app_daily_small = data_frame.filter(
        F.col(source_partition_col).isin(*[first_item])
    )

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
    output_df.write.partitionBy("start_of_month", "sno").mode("overwrite").parquet(
        int_path
    )

    sno = 2
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_app_daily_small = data_frame.filter(
            F.col(source_partition_col).isin(*[curr_item])
        )

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
        output_df.write.partitionBy("start_of_month", "sno").mode("overwrite").parquet(
            int_path
        )
        sno += 1
    logging.info("__COMPLETED__")


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
        df_soc_web_monthly_stats, on=["mobile_no", "start_of_month"], how="left"
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

    pk = ["mobile_no", "start_of_month", "level_1"]
    df_fea_all = df_final_sum.join(
        df_soc_web_monthly_most_popular_app_by_download_traffic,
        on=pk,
        how="left",
    )
    logging.info("5.completed: saving final output..")
    return df_fea_all


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

    partition_num_per_job = 7
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    logging.info(f"mvv_new: {mvv_new}")
    add_list = mvv_new
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_soc_web_monthly_features/"

    first_item = add_list[-1]
    logging.info(f"first_item: {first_item}")
    add_list.remove(first_item)

    logging.info("begin running for dates {0}".format(str(first_item)))
    df_soc_web_daily_small = data_frame.filter(
        F.col(source_partition_col).isin(*[first_item])
    )

    output_df = node_compute_chunk_soc_web_monthly_features(
        df_soc_web_daily_small,
        config_soc_web_monthly_agg,
        config_soc_web_monthly_stats,
        config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk,
        config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    output_df = output_df.withColumn("sno", F.lit(1))
    output_df.write.partitionBy("start_of_month", "sno").mode("overwrite").parquet(
        int_path
    )

    sno = 2
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_web_daily_small = data_frame.filter(
            F.col(source_partition_col).isin(*[curr_item])
        )

        output_df = node_compute_chunk_soc_web_monthly_features(
            df_soc_web_daily_small,
            config_soc_web_monthly_agg,
            config_soc_web_monthly_stats,
            config_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk,
            config_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk,
        )

        output_df = output_df.withColumn("sno", F.lit(sno))
        output_df.write.partitionBy("start_of_month", "sno").mode("overwrite").parquet(
            int_path
        )
        sno += 1
    logging.info("__COMPLETED__")


def node_compute_final_soc_web_monthly_features(
    df_level_priority: pyspark.sql.DataFrame,
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
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_soc_web_monthly_features/"
    spark = get_spark_session()
    df = spark.read.parquet(int_path)
    df = df.join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    df_soc_web_monthly_agg = node_from_config(
        df.select(
            "mobile_no",
            "start_of_month",
            "sno",
            "soc_web_monthly_all_download_traffic",
        ).distinct(),
        config_soc_web_monthly_level_stats,
    )

    df_final_sum = node_from_config(df, config_soc_web_monthly_final_sum).join(
        df_soc_web_monthly_agg, on=["mobile_no", "start_of_month"], how="left"
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

    pk = ["mobile_no", "start_of_month", "level_1"]
    df_fea_all = df_soc_web_monthly_ratio_features.join(
        df_soc_web_monthly_most_popular_app_by_download_traffic,
        on=pk,
        how="left",
    )
    return df_fea_all


def node_comb_all_monthly_user_category_granularity_features(
    df_comb_all: pyspark.sql.DataFrame,
    config_comb_all_monthly_popular_category: Dict[str, Any],
    config_comb_all_monthly_most_popular_category_by_visit_counts: Dict[str, Any],
    config_comb_all_monthly_most_popular_category_by_visit_duration: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    df_comb_all = df_comb_all.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    )

    df_comb_all_monthly_popular_categories = node_from_config(
        df_comb_all, config_comb_all_monthly_popular_category
    )

    df_most_popular_monthly_category_by_visit_counts = node_from_config(
        df_comb_all_monthly_popular_categories,
        config_comb_all_monthly_most_popular_category_by_visit_counts,
    )

    df_most_popular_monthly_category_by_visit_duration = node_from_config(
        df_comb_all_monthly_popular_categories,
        config_comb_all_monthly_most_popular_category_by_visit_duration,
    )

    df_comb_all_monthly_popular_category_features = join_all(
        [
            df_most_popular_monthly_category_by_visit_counts,
            df_most_popular_monthly_category_by_visit_duration,
        ],
        on=["mobile_no", "partition_date"],
        how="outer",
    )
    return df_comb_all_monthly_popular_category_features


def node_compute_chunk_comb_soc_user_granularity_monthly_features(
    df_comb_soc_web_and_app: pyspark.sql.DataFrame,
    config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk: Dict[
        str, Any
    ],
    config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk: Dict[
        str, Any
    ],
) -> pyspark.sql.DataFrame:

    df_comb_soc_popular_app_or_url = node_from_config(
        df_comb_soc_web_and_app,
        config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk,
    )
    logging.info(
        "1.completed: config_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk"
    )

    df_comb_soc_most_popular_app_or_url = node_from_config(
        df_comb_soc_popular_app_or_url,
        config_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    logging.info("3.completed all features, saving..")
    return df_comb_soc_most_popular_app_or_url


def node_comb_soc_monthly_user_category_granularity_features(
    df_comb_soc_web_and_app: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_comb_soc_app_web_popular_category_by_download_traffic: Dict[str, Any],
    config_comb_soc_app_web_most_popular_category_by_download_traffic: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    df_comb_soc_web_and_app = df_comb_soc_web_and_app.join(
        df_level_priority, on=["level_1"], how="inner"
    )

    df_comb_soc_web_and_app_monthly_popular_category = node_from_config(
        df_comb_soc_web_and_app,
        config_comb_soc_app_web_popular_category_by_download_traffic,
    )

    df_monthly_most_popular_category_by_download_volume = node_from_config(
        df_comb_soc_web_and_app_monthly_popular_category,
        config_comb_soc_app_web_most_popular_category_by_download_traffic,
    )

    return df_monthly_most_popular_category_by_download_volume


def node_soc_app_monthly_user_category_granularity_features(
    df_soc: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_soc_app_monthly_popular_category_by_frequency_access: Dict[str, Any],
    config_soc_app_monthly_most_popular_category_by_frequency_access: Dict[str, Any],
    config_soc_app_monthly_popular_category_by_visit_duration: Dict[str, Any],
    config_soc_app_monthly_most_popular_category_by_visit_duration: Dict[str, Any],
    config_soc_app_monthly_popular_category_by_download_traffic: Dict[str, Any],
    config_soc_app_monthly_most_popular_category_by_download_traffic: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    df_soc = df_soc.join(df_level_priority, on=["level_1"], how="inner")

    df_popular_category_by_frequency_access = node_from_config(
        df_soc, config_soc_app_monthly_popular_category_by_frequency_access
    )
    df_most_popular_by_frequency_access = node_from_config(
        df_popular_category_by_frequency_access,
        config_soc_app_monthly_most_popular_category_by_frequency_access,
    )

    df_popular_category_by_visit_duration = node_from_config(
        df_soc, config_soc_app_monthly_popular_category_by_visit_duration
    )
    df_most_popular_category_by_visit_duration = node_from_config(
        df_popular_category_by_visit_duration,
        config_soc_app_monthly_most_popular_category_by_visit_duration,
    )

    df_popular_category_by_dw_traffic = node_from_config(
        df_soc, config_soc_app_monthly_popular_category_by_download_traffic
    )
    df_most_popular_category_by_dw_traffic = node_from_config(
        df_popular_category_by_dw_traffic,
        config_soc_app_monthly_most_popular_category_by_download_traffic,
    )

    df_soc_app_monthly_fav_features = join_all(
        [
            df_most_popular_by_frequency_access,
            df_most_popular_category_by_visit_duration,
            df_most_popular_category_by_dw_traffic,
        ],
        on=["mobile_no", "start_of_month"],
        how="outer",
    )
    return df_soc_app_monthly_fav_features


def node_soc_web_monthly_user_category_granularity_features(
    df_combined_soc_app_daily_and_hourly_agg: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_soc_web_monthly_popular_category_by_download_volume: Dict[str, Any],
    config_soc_web_monthly_most_popular_category_by_download_volume: Dict[str, Any],
) -> pyspark.sql.DataFrame:

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


def __divide_chunks(arr, n):
    for i in range(0, len(arr), n):
        yield arr[i : i + n]


def node_compute_chunk_comb_soc_monthly_features(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_soc_monthly_sum_features: Dict[str, Any],
    config_comb_soc_monthly_stats: Dict[str, Any],
    config_comb_soc_monthly_popular_app_rank_download_traffic_merge_chunk: Dict[
        str, Any
    ],
    config_comb_soc_monthly_most_popular_app_by_download_traffic_merge_chunk: Dict[
        str, Any
    ],
) -> pyspark.sql.DataFrame:

    df_comb_web_sum_features = node_from_config(
        df_comb_web, config_comb_soc_monthly_sum_features
    )
    logging.info("1.completed features for: config_comb_soc_sum_features")

    df_comb_web_sum_daily_stats = node_from_config(
        df_comb_web, config_comb_soc_monthly_stats
    )
    logging.info("2.completed features for: comb_web_sum_daily_stats")

    df_join_sum_features_with_monthly_stats = df_comb_web_sum_features.join(
        df_comb_web_sum_daily_stats, on=["mobile_no", "start_of_month"], how="left"
    )
    logging.info("3.completed: join_sum_features_with_monthly_stats")

    df_comb_web_popular_app_or_url = node_from_config(
        df_comb_web,
        config_comb_soc_monthly_popular_app_rank_download_traffic_merge_chunk,
    )
    logging.info("4.completed features for: comb_web_popular_app_or_url")

    df_comb_web_most_popular_app_or_url = node_from_config(
        df_comb_web_popular_app_or_url,
        config_comb_soc_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    logging.info("5.completed features for: comb_web_most_popular_app_or_url")

    df_comb_web_one_chunk = df_join_sum_features_with_monthly_stats.join(
        df_comb_web_most_popular_app_or_url,
        on=["mobile_no", "start_of_month", "level_1"],
        how="left",
    )

    logging.info("6.completed all features, saving..")
    return df_comb_web_one_chunk


def node_compute_int_comb_soc_monthly_features(
    df_comb_web: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_comb_soc_monthly_sum_features: Dict[str, Any],
    config_comb_soc_monthly_stats: Dict[str, Any],
    config_comb_soc_monthly_popular_app_rank_download_traffic_merge_chunk: Dict[
        str, Any
    ],
    config_comb_soc_monthly_most_popular_app_by_download_traffic_merge_chunk: Dict[
        str, Any
    ],
) -> pyspark.sql.DataFrame:

    spark = get_spark_session()
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()

    df_comb_web = df_comb_web.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    source_partition_col = "partition_date"
    data_frame = df_comb_web
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = 7
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    logging.info(f"mvv_new: {mvv_new}")
    add_list = mvv_new
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/intrm_comb_soc_monthly_features/"

    first_item = add_list[-1]
    logging.info(f"first_item: {first_item}")
    add_list.remove(first_item)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    logging.info("begin running for dates {0}".format(str(first_item)))
    df_comb_web_small = data_frame.filter(
        F.col(source_partition_col).isin(*[first_item])
    )

    output_df = node_compute_chunk_comb_soc_monthly_features(
        df_comb_web_small,
        config_comb_soc_monthly_sum_features,
        config_comb_soc_monthly_stats,
        config_comb_soc_monthly_popular_app_rank_download_traffic_merge_chunk,
        config_comb_soc_monthly_most_popular_app_by_download_traffic_merge_chunk,
    )
    output_df = output_df.withColumn("sno", F.lit(1))
    output_df.write.partitionBy("start_of_month", "sno").mode("overwrite").parquet(
        int_path
    )

    sno = 2
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_web_small = data_frame.filter(
            F.col(source_partition_col).isin(*[curr_item])
        )

        output_df = node_compute_chunk_comb_soc_monthly_features(
            df_comb_web_small,
            config_comb_soc_monthly_sum_features,
            config_comb_soc_monthly_stats,
            config_comb_soc_monthly_popular_app_rank_download_traffic_merge_chunk,
            config_comb_soc_monthly_most_popular_app_by_download_traffic_merge_chunk,
        )
        output_df = output_df.withColumn("sno", F.lit(sno))
        output_df.write.partitionBy("start_of_month", "sno").mode("overwrite").parquet(
            int_path
        )
        sno += 1
    logging.info("__COMPLETED__")


def node_compute_final_comb_soc_monthly_features(
    df_level_priority: pyspark.sql.DataFrame,
    config_comb_soc_final_monthly_agg: Dict[str, Any],
    config_comb_soc_monthly_final_sum: Dict[str, Any],
    config_comb_soc_ratio_based_features: Dict[str, Any],
    config_comb_soc_monthly_final_popular_app_rank_download_traffic_merge_chunk: Dict[
        str, Any
    ],
    config_comb_soc_monthly_final_most_popular_app_by_download_traffic_merge_chunk: Dict[
        str, Any
    ],
) -> pyspark.sql.DataFrame:
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/intrm_comb_soc_monthly_features/"
    spark = get_spark_session()
    df = spark.read.parquet(int_path)
    df = df.join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    df_comb_soc_monthly_agg = node_from_config(
        df.select(
            "mobile_no",
            "start_of_month",
            "sno",
            "total_comb_soc_monthly_download_traffic",
        ).distinct(),
        config_comb_soc_final_monthly_agg,
    )

    df_final_sum = node_from_config(df, config_comb_soc_monthly_final_sum).join(
        df_comb_soc_monthly_agg, on=["mobile_no", "start_of_month"], how="left"
    )

    # -> Ratio Features
    df_soc_app_monthly_ratio_features = node_from_config(
        df_final_sum, config_comb_soc_ratio_based_features
    )

    # -> Download Traffic
    df_comb_soc_monthly_popular_app_rank_download_traffic = node_from_config(
        df,
        config_comb_soc_monthly_final_popular_app_rank_download_traffic_merge_chunk,
    )

    df_comb_soc_monthly_most_popular_app_by_download_traffic = node_from_config(
        df_comb_soc_monthly_popular_app_rank_download_traffic,
        config_comb_soc_monthly_final_most_popular_app_by_download_traffic_merge_chunk,
    )

    pk = ["mobile_no", "start_of_month", "level_1"]
    df_fea_all = df_soc_app_monthly_ratio_features.join(
        df_comb_soc_monthly_most_popular_app_by_download_traffic,
        on=pk,
        how="left",
    )
    return df_fea_all


#######################################
# COMB ALL
#######################################


def node_compute_int_comb_all_monthly_features(
    df_comb_all: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_comb_all_monthly_sum_features: pyspark.sql.DataFrame,
    config_comb_all_monthly_stats: pyspark.sql.DataFrame,
    config_comb_all_monthly_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
    config_comb_all_monthly_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
    config_comb_all_monthly_most_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
    config_comb_all_monthly_most_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
):

    spark = get_spark_session()
    df_level_priority = df_level_priority.select("level_1", "priority").distinct()

    df_comb_all = df_comb_all.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).join(df_level_priority, on=["level_1"], how="inner")

    source_partition_col = "partition_date"
    data_frame = df_comb_all
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = 4
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    logging.info(f"mvv_new: {mvv_new}")
    add_list = mvv_new
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_all_monthly_features/"

    first_item = add_list[-1]
    logging.info(f"first_item: {first_item}")
    add_list.remove(first_item)

    sno = 0
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_all_small = data_frame.filter(
            F.col(source_partition_col).isin(*[curr_item])
        )
        sno = sno + 1
        output_df = node_compute_chunk_comb_all_monthly_features(
            df_comb_all_small,
            config_comb_all_monthly_sum_features,
            config_comb_all_monthly_stats,
            config_comb_all_monthly_popular_url_by_visit_counts_merge_chunk,
            config_comb_all_monthly_popular_url_by_visit_duration_merge_chunk,
            config_comb_all_monthly_most_popular_url_by_visit_counts_merge_chunk,
            config_comb_all_monthly_most_popular_url_by_visit_duration_merge_chunk,
        )
        output_df = output_df.withColumn("sno", F.lit(sno))
        output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
            int_path
        )

    sno = sno + 1
    logging.info("begin running for dates {0}".format(str(first_item)))
    df_comb_all_small = data_frame.filter(
        F.col(source_partition_col).isin(*[first_item])
    )
    output_df = node_compute_chunk_comb_all_monthly_features(
        df_comb_all_small,
        config_comb_all_monthly_sum_features,
        config_comb_all_monthly_stats,
        config_comb_all_monthly_popular_url_by_visit_counts_merge_chunk,
        config_comb_all_monthly_popular_url_by_visit_duration_merge_chunk,
        config_comb_all_monthly_most_popular_url_by_visit_counts_merge_chunk,
        config_comb_all_monthly_most_popular_url_by_visit_duration_merge_chunk,
    )
    output_df = output_df.withColumn("sno", F.lit(sno))
    output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
        int_path
    )
    logging.info("__COMPLETED__")


def node_compute_chunk_comb_all_monthly_features(
    df_comb_all,
    config_comb_all_monthly_sum_features,
    config_comb_all_monthly_stats,
    config_comb_all_monthly_popular_url_by_visit_counts_merge_chunk,
    config_comb_all_monthly_popular_url_by_visit_duration_merge_chunk,
    config_comb_all_monthly_most_popular_url_by_visit_counts_merge_chunk,
    config_comb_all_monthly_most_popular_url_by_visit_duration_merge_chunk,
) -> pyspark.sql.DataFrame:

    df_comb_all = df_comb_all.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    )

    df_comb_all_sum_features = node_from_config(
        df_comb_all, config_comb_all_monthly_sum_features
    )
    logging.info("1.completed features for: config_comb_soc_sum_features")

    df_comb_all_sum_monthly_stats = node_from_config(
        df_comb_all, config_comb_all_monthly_stats
    )
    logging.info("2.completed features for: comb_all_sum_daily_stats")

    df_join_sum_features_with_monthly_stats = df_comb_all_sum_features.join(
        df_comb_all_sum_monthly_stats, on=["mobile_no", "start_of_month"], how="left"
    )
    logging.info("3.completed: join_sum_features_with_monthly_stats")

    df_comb_all_popular_url_by_visit_counts = node_from_config(
        df_comb_all, config_comb_all_monthly_popular_url_by_visit_counts_merge_chunk
    )
    logging.info("4.completed: popular url by visit counts")

    df_comb_all_most_popular_url_by_visit_counts = node_from_config(
        df_comb_all_popular_url_by_visit_counts,
        config_comb_all_monthly_most_popular_url_by_visit_counts_merge_chunk,
    )
    logging.info("5.completed: most popular url by visit counts")

    df_comb_all_popular_url_by_visit_duration = node_from_config(
        df_comb_all, config_comb_all_monthly_popular_url_by_visit_duration_merge_chunk
    )
    logging.info("6.completed: popular url by visit duration")

    df_comb_all_most_popular_url_by_visit_duration = node_from_config(
        df_comb_all_popular_url_by_visit_duration,
        config_comb_all_monthly_most_popular_url_by_visit_duration_merge_chunk,
    )
    logging.info("7.completed: most popular url by visit duration")

    df_comb_all_one_chunk = join_all(
        [
            df_join_sum_features_with_monthly_stats,
            df_comb_all_most_popular_url_by_visit_counts,
            df_comb_all_most_popular_url_by_visit_duration,
        ],
        on=["mobile_no", "start_of_month", "level_1"],
        how="outer",
    )

    logging.info("8.completed all features, saving..")
    return df_comb_all_one_chunk


def node_compute_final_comb_all_monthly_features(
    df_level_priority: pyspark.sql.DataFrame,
    config_comb_all_monthly_agg_visit_counts_final: Dict[str, Any],
    config_comb_all_monthly_agg_visit_duration_final: Dict[str, Any],
    config_comb_all_monthly_sum_final: Dict[str, Any],
    config_comb_all_monthly_ratio_features_visit_counts: Dict[str, Any],
    config_comb_all_monthly_ratio_features_visit_duration: Dict[str, Any],
    config_comb_all_monthly_popular_url_by_visit_counts_final: Dict[str, Any],
    config_comb_all_monthly_most_popular_url_by_visit_counts_final: Dict[str, Any],
    config_comb_all_monthly_popular_url_by_visit_duration_final: Dict[str, Any],
    config_comb_all_monthly_most_popular_url_by_visit_duration_final: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_all_monthly_features/"
    spark = get_spark_session()
    df = spark.read.parquet(int_path)
    df = df.join(F.broadcast(df_level_priority), on=["level_1"], how="inner")

    ########################
    df_comb_all_monthly_agg_visit_counts = node_from_config(
        df.select(
            "mobile_no",
            "start_of_month",
            "sno",
            "comb_all_total_monthly_visit_counts",
        ).distinct(),
        config_comb_all_monthly_agg_visit_counts_final,
    )

    df_final_sum_total_visit_counts = node_from_config(
        df, config_comb_all_monthly_sum_final
    ).join(
        df_comb_all_monthly_agg_visit_counts,
        on=["mobile_no", "start_of_month"],
        how="left",
    )

    # -> Ratio Features
    df_soc_app_monthly_ratio_features_visit_counts = node_from_config(
        df_final_sum_total_visit_counts,
        config_comb_all_monthly_ratio_features_visit_counts,
    )

    df_comb_all_monthly_agg_visit_duration = node_from_config(
        df.select(
            "mobile_no",
            "start_of_month",
            "sno",
            "comb_all_total_monthly_visit_duration",
        ).distinct(),
        config_comb_all_monthly_agg_visit_duration_final,
    )

    df_final_sum_total_visit_duration = node_from_config(
        df, config_comb_all_monthly_sum_final
    ).join(
        df_comb_all_monthly_agg_visit_duration,
        on=["mobile_no", "start_of_month"],
        how="left",
    )

    # -> Ratio Features
    df_soc_app_monthly_ratio_features_visit_duration = node_from_config(
        df_final_sum_total_visit_duration,
        config_comb_all_monthly_ratio_features_visit_duration,
    )

    # Favourite url by visit counts
    df_comb_all_monthly_popular_url_by_visit_counts = node_from_config(
        df,
        config_comb_all_monthly_popular_url_by_visit_counts_final,
    )

    df_comb_all_monthly_most_popular_url_by_visit_counts = node_from_config(
        df_comb_all_monthly_popular_url_by_visit_counts,
        config_comb_all_monthly_most_popular_url_by_visit_counts_final,
    )

    # Favourite url by visit duration
    df_comb_all_monthly_popular_url_by_visit_duration = node_from_config(
        df,
        config_comb_all_monthly_popular_url_by_visit_duration_final,
    )

    df_comb_all_monthly_most_popular_url_by_visit_duration = node_from_config(
        df_comb_all_monthly_popular_url_by_visit_duration,
        config_comb_all_monthly_most_popular_url_by_visit_duration_final,
    )

    fea_all = join_all(
        [
            df_soc_app_monthly_ratio_features_visit_counts,
            df_soc_app_monthly_ratio_features_visit_duration,
            df_comb_all_monthly_most_popular_url_by_visit_counts,
            df_comb_all_monthly_most_popular_url_by_visit_duration,
        ],
        on=["mobile_no", "start_of_month", "level_1"],
        how="outer",
    )

    return fea_all


def node_comb_all_monthly_user_category_granularity_features(
    df_comb_all: pyspark.sql.DataFrame,
    df_level_priority: pyspark.sql.DataFrame,
    config_comb_all_popular_category_by_visit_counts: Dict[str, Any],
    config_comb_all_most_popular_category_by_visit_counts: Dict[str, Any],
    config_comb_all_popular_category_by_visit_duration: Dict[str, Any],
    config_comb_all_most_popular_category_by_visit_duration: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    df_level_priority = df_level_priority.select("level_1", "priority").distinct()
    df_comb_all = df_comb_all.join(df_level_priority, on=["level_1"], how="inner")

    df_popular_category_by_visit_counts = node_from_config(
        df_comb_all, config_comb_all_popular_category_by_visit_counts
    )
    df_most_popular_category_by_visit_counts = node_from_config(
        df_popular_category_by_visit_counts,
        config_comb_all_most_popular_category_by_visit_counts,
    )
    df_popular_category_by_visit_duration = node_from_config(
        df_comb_all,
        config_comb_all_popular_category_by_visit_duration,
    )

    df_most_popular_category_by_visit_duration = node_from_config(
        df_popular_category_by_visit_duration,
        config_comb_all_most_popular_category_by_visit_duration,
    )

    fea_all = join_all(
        [
            df_most_popular_category_by_visit_duration,
            df_most_popular_category_by_visit_counts,
        ],
        on=["mobile_no", "start_of_month"],
        how="outer",
    )
    return fea_all


# def node_compute_int_comb_web_monthly_features(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_sum_features: pyspark.sql.DataFrame,
#     config_comb_web_monthly_stats: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
# ):
#
#     spark = get_spark_session()
#     df_level_priority = df_level_priority.select("level_1", "priority").distinct()
#
#     df_comb_web = df_comb_web.withColumn(
#         "start_of_month",
#         F.concat(
#             F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
#         ).cast("int"),
#     ).join(df_level_priority, on=["level_1"], how="inner")
#
#     source_partition_col = "partition_date"
#     data_frame = df_comb_web
#     dates_list = data_frame.select(source_partition_col).distinct().collect()
#     mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
#     mvv_array = sorted(mvv_array)
#     logging.info("Dates to run for {0}".format(str(mvv_array)))
#
#     partition_num_per_job = 7
#     mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
#     logging.info(f"mvv_new: {mvv_new}")
#     add_list = mvv_new
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#
#     first_item = add_list[-1]
#     logging.info(f"first_item: {first_item}")
#     add_list.remove(first_item)
#
#     sno = 0
#     for curr_item in add_list:
#         logging.info("running for dates {0}".format(str(curr_item)))
#         df_comb_web_small = data_frame.filter(
#             F.col(source_partition_col).isin(*[curr_item])
#         )
#         sno = sno + 1
#         output_df = node_compute_chunk_comb_web_monthly_features(
#             df_comb_web_small,
#             config_comb_web_monthly_sum_features,
#             config_comb_web_monthly_stats,
#             config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#         )
#
#         output_df = output_df.withColumn("sno", F.lit(sno))
#         output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#             int_path
#         )
#
#     sno = sno + 1
#     logging.info("Last run for dates {0}".format(str(first_item)))
#     df_comb_web_small = data_frame.filter(
#         F.col(source_partition_col).isin(*[first_item])
#     )
#     output_df = node_compute_chunk_comb_web_monthly_features(
#         df_comb_web_small,
#         config_comb_web_monthly_sum_features,
#         config_comb_web_monthly_stats,
#         config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#     )
#     output_df = output_df.withColumn("sno", F.lit(sno))
#     output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#         int_path
#     )
#     logging.info("__COMPLETED__")
#
#
# def node_compute_int_comb_web_monthly_features_catlv2(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_sum_features: pyspark.sql.DataFrame,
#     config_comb_web_monthly_stats: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
# ):
#
#     spark = get_spark_session()
#     df_level_priority = df_level_priority.select("level_2", "priority").distinct()
#
#     df_comb_web = df_comb_web.withColumn(
#         "start_of_month",
#         F.concat(
#             F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
#         ).cast("int"),
#     ).join(df_level_priority, on=["level_2"], how="inner")
#
#     source_partition_col = "partition_date"
#     data_frame = df_comb_web
#     dates_list = data_frame.select(source_partition_col).distinct().collect()
#     mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
#     mvv_array = sorted(mvv_array)
#     logging.info("Dates to run for {0}".format(str(mvv_array)))
#
#     partition_num_per_job = 7
#     mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
#     logging.info(f"mvv_new: {mvv_new}")
#     add_list = mvv_new
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#
#     first_item = add_list[-1]
#     logging.info(f"first_item: {first_item}")
#     add_list.remove(first_item)
#
#     sno = 0
#     for curr_item in add_list:
#         logging.info("running for dates {0}".format(str(curr_item)))
#         df_comb_web_small = data_frame.filter(
#             F.col(source_partition_col).isin(*[curr_item])
#         )
#         sno = sno + 1
#         output_df = node_compute_chunk_comb_web_monthly_features_catlv2(
#             df_comb_web_small,
#             config_comb_web_monthly_sum_features,
#             config_comb_web_monthly_stats,
#             config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#         )
#
#         output_df = output_df.withColumn("sno", F.lit(sno))
#         output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#             int_path
#         )
#
#     sno = sno + 1
#     logging.info("Last run for dates {0}".format(str(first_item)))
#     df_comb_web_small = data_frame.filter(
#         F.col(source_partition_col).isin(*[first_item])
#     )
#     output_df = node_compute_chunk_comb_web_monthly_features_catlv2(
#         df_comb_web_small,
#         config_comb_web_monthly_sum_features,
#         config_comb_web_monthly_stats,
#         config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#     )
#     output_df = output_df.withColumn("sno", F.lit(sno))
#     output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#         int_path
#     )
#     logging.info("__COMPLETED__")
#
#
# def node_compute_int_comb_web_monthly_features_catlv3(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_sum_features: pyspark.sql.DataFrame,
#     config_comb_web_monthly_stats: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
# ):
#
#     spark = get_spark_session()
#     df_level_priority = df_level_priority.select("level_3", "priority").distinct()
#
#     df_comb_web = df_comb_web.withColumn(
#         "start_of_month",
#         F.concat(
#             F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
#         ).cast("int"),
#     ).join(df_level_priority, on=["level_3"], how="inner")
#
#     source_partition_col = "partition_date"
#     data_frame = df_comb_web
#     dates_list = data_frame.select(source_partition_col).distinct().collect()
#     mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
#     mvv_array = sorted(mvv_array)
#     logging.info("Dates to run for {0}".format(str(mvv_array)))
#
#     partition_num_per_job = 7
#     mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
#     logging.info(f"mvv_new: {mvv_new}")
#     add_list = mvv_new
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#
#     first_item = add_list[-1]
#     logging.info(f"first_item: {first_item}")
#     add_list.remove(first_item)
#
#     sno = 0
#     for curr_item in add_list:
#         logging.info("running for dates {0}".format(str(curr_item)))
#         df_comb_web_small = data_frame.filter(
#             F.col(source_partition_col).isin(*[curr_item])
#         )
#         sno = sno + 1
#         output_df = node_compute_chunk_comb_web_monthly_features_catlv3(
#             df_comb_web_small,
#             config_comb_web_monthly_sum_features,
#             config_comb_web_monthly_stats,
#             config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#         )
#
#         output_df = output_df.withColumn("sno", F.lit(sno))
#         output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#             int_path
#         )
#
#     sno = sno + 1
#     logging.info("Last run for dates {0}".format(str(first_item)))
#     df_comb_web_small = data_frame.filter(
#         F.col(source_partition_col).isin(*[first_item])
#     )
#     output_df = node_compute_chunk_comb_web_monthly_features_catlv3(
#         df_comb_web_small,
#         config_comb_web_monthly_sum_features,
#         config_comb_web_monthly_stats,
#         config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#     )
#     output_df = output_df.withColumn("sno", F.lit(sno))
#     output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#         int_path
#     )
#     logging.info("__COMPLETED__")
#
#
# def node_compute_int_comb_web_monthly_features_catlv4(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_sum_features: pyspark.sql.DataFrame,
#     config_comb_web_monthly_stats: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk: pyspark.sql.DataFrame,
#     config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk: pyspark.sql.DataFrame,
# ):
#
#     spark = get_spark_session()
#     df_level_priority = df_level_priority.select("level_4", "priority").distinct()
#
#     df_comb_web = df_comb_web.withColumn(
#         "start_of_month",
#         F.concat(
#             F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
#         ).cast("int"),
#     ).join(df_level_priority, on=["level_4"], how="inner")
#
#     source_partition_col = "partition_date"
#     data_frame = df_comb_web
#     dates_list = data_frame.select(source_partition_col).distinct().collect()
#     mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
#     mvv_array = sorted(mvv_array)
#     logging.info("Dates to run for {0}".format(str(mvv_array)))
#
#     partition_num_per_job = 7
#     mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
#     logging.info(f"mvv_new: {mvv_new}")
#     add_list = mvv_new
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#
#     first_item = add_list[-1]
#     logging.info(f"first_item: {first_item}")
#     add_list.remove(first_item)
#
#     sno = 0
#     for curr_item in add_list:
#         logging.info("running for dates {0}".format(str(curr_item)))
#         df_comb_web_small = data_frame.filter(
#             F.col(source_partition_col).isin(*[curr_item])
#         )
#         sno = sno + 1
#         output_df = node_compute_chunk_comb_web_monthly_features_catlv4(
#             df_comb_web_small,
#             config_comb_web_monthly_sum_features,
#             config_comb_web_monthly_stats,
#             config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#             config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#         )
#
#         output_df = output_df.withColumn("sno", F.lit(sno))
#         output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#             int_path
#         )
#
#     sno = sno + 1
#     logging.info("Last run for dates {0}".format(str(first_item)))
#     df_comb_web_small = data_frame.filter(
#         F.col(source_partition_col).isin(*[first_item])
#     )
#     output_df = node_compute_chunk_comb_web_monthly_features_catlv4(
#         df_comb_web_small,
#         config_comb_web_monthly_sum_features,
#         config_comb_web_monthly_stats,
#         config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#     )
#     output_df = output_df.withColumn("sno", F.lit(sno))
#     output_df.write.mode("overwrite").partitionBy("start_of_month", "sno").parquet(
#         int_path
#     )
#     logging.info("__COMPLETED__")
#
# def node_compute_chunk_comb_web_monthly_features(
#     df_comb_web,
#     config_comb_web_monthly_sum_features,
#     config_comb_web_monthly_stats,
#     config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk,
#     config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk,
#     config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#     config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
# ) -> pyspark.sql.DataFrame:
#
#     df_comb_web = df_comb_web.withColumn(
#         "start_of_month",
#         F.concat(
#             F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
#         ).cast("int"),
#     )
#
#     df_comb_web_sum_features = node_from_config(
#         df_comb_web, config_comb_web_monthly_sum_features
#     )
#     logging.info("1.completed features for: config_comb_soc_sum_features")
#
#     df_comb_web_sum_monthly_stats = node_from_config(
#         df_comb_web, config_comb_web_monthly_stats
#     )
#     logging.info("2.completed features for: comb_web_sum_daily_stats")
#
#     df_join_sum_features_with_monthly_stats = df_comb_web_sum_features.join(
#         df_comb_web_sum_monthly_stats, on=["mobile_no", "start_of_month"], how="left"
#     )
#     logging.info("3.completed: join_sum_features_with_monthly_stats")
#
#     df_comb_web_popular_url_by_visit_counts = node_from_config(
#         df_comb_web, config_comb_web_monthly_popular_url_by_visit_counts_merge_chunk
#     )
#     logging.info("4.completed: popular url by visit counts")
#
#     df_comb_web_most_popular_url_by_visit_counts = node_from_config(
#         df_comb_web_popular_url_by_visit_counts,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk,
#     )
#     logging.info("5.completed: most popular url by visit counts")
#
#     df_comb_web_popular_url_by_visit_duration = node_from_config(
#         df_comb_web, config_comb_web_monthly_popular_url_by_visit_duration_merge_chunk
#     )
#     logging.info("6.completed: popular url by visit duration")
#
#     df_comb_web_most_popular_url_by_visit_duration = node_from_config(
#         df_comb_web_popular_url_by_visit_duration,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk,
#     )
#     logging.info("7.completed: most popular url by visit duration")
#
#     df_comb_web_one_chunk = join_all(
#         [
#             df_join_sum_features_with_monthly_stats,
#             df_comb_web_most_popular_url_by_visit_counts,
#             df_comb_web_most_popular_url_by_visit_duration,
#         ],
#         on=["mobile_no", "start_of_month", "level_1"],
#         how="outer",
#     )
#
#     logging.info("8.completed all features, saving..")
#     return df_comb_web_one_chunk
#
#
# def node_compute_final_comb_web_monthly_features(
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_agg_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_agg_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_sum_final: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_duration_final: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_1", "priority").distinct()
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#     spark = get_spark_session()
#     df = spark.read.parquet(int_path)
#     df = df.join(F.broadcast(df_level_priority), on=["level_1"], how="inner")
#
#     df_comb_web_monthly_agg_visit_counts = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_counts",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_counts_final,
#     )
#
#     df_final_sum_total_visit_counts = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_counts,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_counts = node_from_config(
#         df_final_sum_total_visit_counts,
#         config_comb_web_monthly_ratio_features_visit_counts,
#     )
#     df_comb_web_monthly_agg_visit_duration = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_duration",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_duration_final,
#     )
#
#     df_final_sum_total_visit_duration = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_duration,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_duration = node_from_config(
#         df_final_sum_total_visit_duration,
#         config_comb_web_monthly_ratio_features_visit_duration,
#     )
#
#     # Favourite url by visit counts
#     df_comb_web_monthly_popular_url_by_visit_counts = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_counts_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_counts = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_counts,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_final,
#     )
#
#     # Favourite url by visit duration
#     df_comb_web_monthly_popular_url_by_visit_duration = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_duration_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_duration = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_duration,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_final,
#     )
#
#     fea_all = join_all(
#         [
#             df_soc_app_monthly_ratio_features_visit_counts,
#             df_soc_app_monthly_ratio_features_visit_duration,
#             df_comb_web_monthly_most_popular_url_by_visit_counts,
#             df_comb_web_monthly_most_popular_url_by_visit_duration,
#         ],
#         on=["mobile_no", "start_of_month", "level_1"],
#         how="outer",
#     )
#
#     return fea_all
#
#
# def node_compute_final_comb_web_monthly_features_catlv2(
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_agg_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_agg_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_sum_final: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_duration_final: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_2", "priority").distinct()
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#     spark = get_spark_session()
#     df = spark.read.parquet(int_path)
#     df = df.join(F.broadcast(df_level_priority), on=["level_2"], how="inner")
#
#     df_comb_web_monthly_agg_visit_counts = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_counts",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_counts_final,
#     )
#
#     df_final_sum_total_visit_counts = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_counts,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_counts = node_from_config(
#         df_final_sum_total_visit_counts,
#         config_comb_web_monthly_ratio_features_visit_counts,
#     )
#     df_comb_web_monthly_agg_visit_duration = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_duration",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_duration_final,
#     )
#
#     df_final_sum_total_visit_duration = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_duration,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_duration = node_from_config(
#         df_final_sum_total_visit_duration,
#         config_comb_web_monthly_ratio_features_visit_duration,
#     )
#
#     # Favourite url by visit counts
#     df_comb_web_monthly_popular_url_by_visit_counts = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_counts_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_counts = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_counts,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_final,
#     )
#
#     # Favourite url by visit duration
#     df_comb_web_monthly_popular_url_by_visit_duration = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_duration_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_duration = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_duration,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_final,
#     )
#
#     fea_all = join_all(
#         [
#             df_soc_app_monthly_ratio_features_visit_counts,
#             df_soc_app_monthly_ratio_features_visit_duration,
#             df_comb_web_monthly_most_popular_url_by_visit_counts,
#             df_comb_web_monthly_most_popular_url_by_visit_duration,
#         ],
#         on=["mobile_no", "start_of_month", "level_2"],
#         how="outer",
#     )
#
#     return fea_all
#
#
# def node_compute_final_comb_web_monthly_features_catlv3(
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_agg_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_agg_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_sum_final: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_duration_final: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_3", "priority").distinct()
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#     spark = get_spark_session()
#     df = spark.read.parquet(int_path)
#     df = df.join(F.broadcast(df_level_priority), on=["level_3"], how="inner")
#
#     df_comb_web_monthly_agg_visit_counts = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_counts",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_counts_final,
#     )
#
#     df_final_sum_total_visit_counts = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_counts,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_counts = node_from_config(
#         df_final_sum_total_visit_counts,
#         config_comb_web_monthly_ratio_features_visit_counts,
#     )
#     df_comb_web_monthly_agg_visit_duration = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_duration",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_duration_final,
#     )
#
#     df_final_sum_total_visit_duration = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_duration,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_duration = node_from_config(
#         df_final_sum_total_visit_duration,
#         config_comb_web_monthly_ratio_features_visit_duration,
#     )
#
#     # Favourite url by visit counts
#     df_comb_web_monthly_popular_url_by_visit_counts = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_counts_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_counts = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_counts,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_final,
#     )
#
#     # Favourite url by visit duration
#     df_comb_web_monthly_popular_url_by_visit_duration = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_duration_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_duration = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_duration,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_final,
#     )
#
#     fea_all = join_all(
#         [
#             df_soc_app_monthly_ratio_features_visit_counts,
#             df_soc_app_monthly_ratio_features_visit_duration,
#             df_comb_web_monthly_most_popular_url_by_visit_counts,
#             df_comb_web_monthly_most_popular_url_by_visit_duration,
#         ],
#         on=["mobile_no", "start_of_month", "level_3"],
#         how="outer",
#     )
#
#     return fea_all
#
#
# def node_compute_final_comb_web_monthly_features_catlv4(
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_monthly_agg_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_agg_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_sum_final: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_ratio_features_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_counts_final: Dict[str, Any],
#     config_comb_web_monthly_popular_url_by_visit_duration_final: Dict[str, Any],
#     config_comb_web_monthly_most_popular_url_by_visit_duration_final: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_4", "priority").distinct()
#     int_path = "/mnt/mck-test-customer360-blob-output/C360/STREAM/l3_features/int_comb_web_monthly_features/"
#     spark = get_spark_session()
#     df = spark.read.parquet(int_path)
#     df = df.join(F.broadcast(df_level_priority), on=["level_4"], how="inner")
#
#     df_comb_web_monthly_agg_visit_counts = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_counts",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_counts_final,
#     )
#
#     df_final_sum_total_visit_counts = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_counts,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_counts = node_from_config(
#         df_final_sum_total_visit_counts,
#         config_comb_web_monthly_ratio_features_visit_counts,
#     )
#     df_comb_web_monthly_agg_visit_duration = node_from_config(
#         df.select(
#             "mobile_no",
#             "start_of_month",
#             "sno",
#             "comb_web_total_monthly_visit_duration",
#         ).distinct(),
#         config_comb_web_monthly_agg_visit_duration_final,
#     )
#
#     df_final_sum_total_visit_duration = node_from_config(
#         df, config_comb_web_monthly_sum_final
#     ).join(
#         df_comb_web_monthly_agg_visit_duration,
#         on=["mobile_no", "start_of_month"],
#         how="left",
#     )
#
#     # -> Ratio Features
#     df_soc_app_monthly_ratio_features_visit_duration = node_from_config(
#         df_final_sum_total_visit_duration,
#         config_comb_web_monthly_ratio_features_visit_duration,
#     )
#
#     # Favourite url by visit counts
#     df_comb_web_monthly_popular_url_by_visit_counts = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_counts_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_counts = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_counts,
#         config_comb_web_monthly_most_popular_url_by_visit_counts_final,
#     )
#
#     # Favourite url by visit duration
#     df_comb_web_monthly_popular_url_by_visit_duration = node_from_config(
#         df,
#         config_comb_web_monthly_popular_url_by_visit_duration_final,
#     )
#
#     df_comb_web_monthly_most_popular_url_by_visit_duration = node_from_config(
#         df_comb_web_monthly_popular_url_by_visit_duration,
#         config_comb_web_monthly_most_popular_url_by_visit_duration_final,
#     )
#
#     fea_all = join_all(
#         [
#             df_soc_app_monthly_ratio_features_visit_counts,
#             df_soc_app_monthly_ratio_features_visit_duration,
#             df_comb_web_monthly_most_popular_url_by_visit_counts,
#             df_comb_web_monthly_most_popular_url_by_visit_duration,
#         ],
#         on=["mobile_no", "start_of_month", "level_4"],
#         how="outer",
#     )
#
#     return fea_all
#
#
# def node_comb_web_monthly_user_category_granularity_features(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_most_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_popular_category_by_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_most_popular_category_by_visit_duration: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_1", "priority").distinct()
#     df_comb_web = df_comb_web.join(df_level_priority, on=["level_1"], how="inner")
#
#     df_popular_category_by_visit_counts = node_from_config(
#         df_comb_web, config_comb_web_popular_category_by_visit_counts
#     )
#
#     df_most_popular_category_by_visit_counts = node_from_config(
#         df_popular_category_by_visit_counts,
#         config_comb_web_most_popular_category_by_visit_counts,
#     )
#
#     df_popular_category_by_visit_duration = node_from_config(
#         df_comb_web,
#         config_comb_web_monthly_popular_category_by_visit_duration,
#     )
#
#     df_most_popular_category_by_visit_duration = node_from_config(
#         df_popular_category_by_visit_duration,
#         config_comb_web_monthly_most_popular_category_by_visit_duration,
#     )
#
#     fea_all = join_all(
#         [
#             df_most_popular_category_by_visit_duration,
#             df_most_popular_category_by_visit_counts,
#         ],
#         on=["mobile_no", "start_of_month"],
#         how="outer",
#     )
#     return fea_all
#
#
# def node_comb_web_monthly_user_category_granularity_features_catlv2(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_most_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_popular_category_by_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_most_popular_category_by_visit_duration: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_2", "priority").distinct()
#     df_comb_web = df_comb_web.join(df_level_priority, on=["level_2"], how="inner")
#
#     df_popular_category_by_visit_counts = node_from_config(
#         df_comb_web, config_comb_web_popular_category_by_visit_counts
#     )
#
#     df_most_popular_category_by_visit_counts = node_from_config(
#         df_popular_category_by_visit_counts,
#         config_comb_web_most_popular_category_by_visit_counts,
#     )
#
#     df_popular_category_by_visit_duration = node_from_config(
#         df_comb_web,
#         config_comb_web_monthly_popular_category_by_visit_duration,
#     )
#
#     df_most_popular_category_by_visit_duration = node_from_config(
#         df_popular_category_by_visit_duration,
#         config_comb_web_monthly_most_popular_category_by_visit_duration,
#     )
#
#     fea_all = join_all(
#         [
#             df_most_popular_category_by_visit_duration,
#             df_most_popular_category_by_visit_counts,
#         ],
#         on=["mobile_no", "start_of_month"],
#         how="outer",
#     )
#     return fea_all
#
#
# def node_comb_web_monthly_user_category_granularity_features_catlv3(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_most_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_popular_category_by_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_most_popular_category_by_visit_duration: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_3", "priority").distinct()
#     df_comb_web = df_comb_web.join(df_level_priority, on=["level_3"], how="inner")
#
#     df_popular_category_by_visit_counts = node_from_config(
#         df_comb_web, config_comb_web_popular_category_by_visit_counts
#     )
#
#     df_most_popular_category_by_visit_counts = node_from_config(
#         df_popular_category_by_visit_counts,
#         config_comb_web_most_popular_category_by_visit_counts,
#     )
#
#     df_popular_category_by_visit_duration = node_from_config(
#         df_comb_web,
#         config_comb_web_monthly_popular_category_by_visit_duration,
#     )
#
#     df_most_popular_category_by_visit_duration = node_from_config(
#         df_popular_category_by_visit_duration,
#         config_comb_web_monthly_most_popular_category_by_visit_duration,
#     )
#
#     fea_all = join_all(
#         [
#             df_most_popular_category_by_visit_duration,
#             df_most_popular_category_by_visit_counts,
#         ],
#         on=["mobile_no", "start_of_month"],
#         how="outer",
#     )
#     return fea_all
#
#
# def node_comb_web_monthly_user_category_granularity_features_catlv4(
#     df_comb_web: pyspark.sql.DataFrame,
#     df_level_priority: pyspark.sql.DataFrame,
#     config_comb_web_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_most_popular_category_by_visit_counts: Dict[str, Any],
#     config_comb_web_monthly_popular_category_by_visit_duration: Dict[str, Any],
#     config_comb_web_monthly_most_popular_category_by_visit_duration: Dict[str, Any],
# ) -> pyspark.sql.DataFrame:
#
#     df_level_priority = df_level_priority.select("level_4", "priority").distinct()
#     df_comb_web = df_comb_web.join(df_level_priority, on=["level_4"], how="inner")
#
#     df_popular_category_by_visit_counts = node_from_config(
#         df_comb_web, config_comb_web_popular_category_by_visit_counts
#     )
#
#     df_most_popular_category_by_visit_counts = node_from_config(
#         df_popular_category_by_visit_counts,
#         config_comb_web_most_popular_category_by_visit_counts,
#     )
#
#     df_popular_category_by_visit_duration = node_from_config(
#         df_comb_web,
#         config_comb_web_monthly_popular_category_by_visit_duration,
#     )
#
#     df_most_popular_category_by_visit_duration = node_from_config(
#         df_popular_category_by_visit_duration,
#         config_comb_web_monthly_most_popular_category_by_visit_duration,
#     )
#
#     fea_all = join_all(
#         [
#             df_most_popular_category_by_visit_duration,
#             df_most_popular_category_by_visit_counts,
#         ],
#         on=["mobile_no", "start_of_month"],
#         how="outer",
#     )
#     return fea_all


def _relay_drop_nulls(df_relay: pyspark.sql.DataFrame)- > pyspark.sql.DataFrame:
    df_relay_cleaned = df_relay.filter(
        (f.col("mobile_no").isNotNull())
        & (f.col("mobile_no") != "")
        & (f.col("subscription_identifier") != "")
        & (f.col("subscription_identifier").isNotNull())
    ).dropDuplicates()
    return df_relay_cleaned


def node_pageviews_monthly_features(
    df_pageviews: pyspark.sql.DataFrame,
    config_total_visits: Dict[str, Any],
    config_popular_url: Dict[str, Any],
    config_popular_subcategory1: Dict[str, Any],
    config_popular_subcategory2: Dict[str, Any],
    config_popular_cid: Dict[str, Any],
    config_popular_productname: Dict[str, Any],
    config_most_popular_url: Dict[str, Any],
    config_most_popular_subcategory1: Dict[str, Any],
    config_most_popular_subcategory2: Dict[str, Any],
    config_most_popular_cid: Dict[str, Any],
    config_most_popular_productname: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    df_pageviews = df_pageviews.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).drop(*["partition_date"])

    df_pageviews_clean = _relay_drop_nulls(df_pageviews).dropDuplicates()

    # total visits
    df_total_visits = node_from_config(df_pageviews_clean, config_total_visits)

    # most_popular_subcategory1
    df_pageviews_subcat1 = clean_favourite_category(df_pageviews_clean, "subCategory1")
    popular_subcategory1_df = node_from_config(
        df_pageviews_subcat1, config_popular_subcategory1
    )
    df_most_popular_subcategory1 = node_from_config(
        popular_subcategory1_df, config_most_popular_subcategory1
    )

    # most_popular_subcategory2
    df_pageviews_subcat2 = clean_favourite_category(df_pageviews_clean, "subCategory2")
    popular_subcategory2_df = node_from_config(
        df_pageviews_subcat2, config_popular_subcategory2
    )
    df_most_popular_subcategory2 = node_from_config(
        popular_subcategory2_df, config_most_popular_subcategory2
    )

    # most_popular_url
    df_pageviews_url = clean_favourite_category(df_pageviews_clean, "url")
    popular_url_df = node_from_config(df_pageviews_url, config_popular_url)
    df_most_popular_url = node_from_config(popular_url_df, config_most_popular_url)

    # most_popular_productname
    # df_pageviews_productname = clean_favourite_category(
    #     df_pageviews_clean, "R42productName"
    # )
    # popular_productname_df = node_from_config(
    #     df_pageviews_productname, config_popular_productname
    # )
    # df_most_popular_productname = node_from_config(
    #     popular_productname_df, config_most_popular_productname
    # )
    # print('df_most_popular_productname', df_most_popular_productname.columns)

    # most_popular_cid
    df_pageviews_cid = clean_favourite_category(df_pageviews_clean, "cid")
    df_popular_cid = node_from_config(df_pageviews_cid, config_popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, config_most_popular_cid)

    # TODO: handle null feature
    pageviews_monthly_features = join_all(
        [
            df_total_visits,
            df_most_popular_subcategory1,
            df_most_popular_subcategory2,
            df_most_popular_url,
            # df_most_popular_productname,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "start_of_month"],
        how="outer",
    )

    return pageviews_monthly_features


def node_engagement_conversion_monthly_features(
    df_engagement: pyspark.sql.DataFrame,
    config_popular_product: Dict[str, Any],
    config_popular_cid: Dict[str, Any],
    config_most_popular_product: Dict[str, Any],
    config_most_popular_cid: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    df_engagement = df_engagement.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).drop(*["partition_date"])

    df_engagement_clean = _relay_drop_nulls(df_engagement)
    df_engagement_conversion = df_engagement_clean.filter(
        f.lower(f.trim(f.col("R42paymentStatus"))) == "successful"
    )

    # favourite product
    df_engagement_conversion_product = df_engagement_conversion.withColumn(
        "R42productLists", f.split("R42productLists", ",")
    ).withColumn("product", f.explode("R42productLists"))
    df_engagement_conversion_product_clean = clean_favourite_category(
        df_engagement_conversion_product, "product"
    )
    df_popular_product = node_from_config(
        df_engagement_conversion_product_clean, config_popular_product
    )
    df_most_popular_product = node_from_config(
        df_popular_product, config_most_popular_product
    )

    # favourite cid
    df_engagement_cid = clean_favourite_category(df_engagement_conversion, "cid")
    df_popular_cid = node_from_config(df_engagement_cid, config_popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, config_most_popular_cid)

    engagement_conversion_monthly_features = join_all(
        [df_most_popular_product, df_most_popular_cid],
        on=["subscription_identifier", "start_of_month"],
        how="outer",
    )

    return engagement_conversion_monthly_features


def node_engagement_conversion_cid_level_monthly_features(
    df_engagement: pyspark.sql.DataFrame, config_total_visits: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    df_engagement = df_engagement.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).drop(*["partition_date"])
    df_engagement_clean = _relay_drop_nulls(df_engagement)
    df_engagement_clean = clean_favourite_category(df_engagement_clean, "cid")
    df_engagement_conversion = df_engagement_clean.filter(
        f.lower(f.trim(f.col("R42paymentStatus"))) == "successful"
    )
    df_engagement_conversion_visits = node_from_config(
        df_engagement_conversion, config_total_visits
    )
    return df_engagement_conversion_visits


def node_engagement_conversion_package_monthly_features(
    df_engagement: pyspark.sql.DataFrame,
    config_popular_product: Dict[str, Any],
    config_popular_cid: Dict[str, Any],
    config_most_popular_product: Dict[str, Any],
    config_most_popular_cid: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    df_engagement = df_engagement.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).drop(*["partition_date"])

    df_engagement_clean = _relay_drop_nulls(df_engagement)
    df_engagement_conversion_package = df_engagement_clean.filter(
        f.lower(f.trim(f.col("R42Product_status"))) == "successful"
    ).withColumnRenamed("R42Product_name", "product")

    # favourite product
    df_engagement_conversion_package_product_clean = clean_favourite_category(
        df_engagement_conversion_package, "product"
    )
    df_popular_product = node_from_config(
        df_engagement_conversion_package_product_clean, config_popular_product
    )
    df_most_popular_product = node_from_config(
        df_popular_product, config_most_popular_product
    )

    # favourite cid
    df_engagement_cid = clean_favourite_category(
        df_engagement_conversion_package, "cid"
    )
    df_popular_cid = node_from_config(df_engagement_cid, config_popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, config_most_popular_cid)

    engagement_conversion_package_monthly_features = join_all(
        [
            df_most_popular_product,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "start_of_month"],
        how="outer",
    )

    return engagement_conversion_package_monthly_features


def node_engagement_conversion_package_cid_level_monthly_features(
    df_engagement: pyspark.sql.DataFrame, config_total_visits: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    df_engagement = df_engagement.withColumn(
        "start_of_month",
        F.concat(
            F.substring(F.col("partition_date").cast("string"), 1, 6), F.lit("01")
        ).cast("int"),
    ).drop(*["partition_date"])

    df_engagement_clean = _relay_drop_nulls(df_engagement)
    df_engagement_clean = clean_favourite_category(df_engagement_clean, "cid")
    df_engagement_conversion_package = df_engagement_clean.filter(
        f.lower(f.trim(f.col("R42Product_status"))) == "successful"
    ).withColumnRenamed("R42Product_name", "product")
    df_engagement_conversion_visits = node_from_config(
        df_engagement_conversion_package, config_total_visits
    )
    return df_engagement_conversion_visits
