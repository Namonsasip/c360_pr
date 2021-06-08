import logging
import os
from pathlib import Path

import pyspark
from kedro.context.context import load_context
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *
from typing import Dict, Any

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import (
    add_event_week_and_month_from_yyyymmdd,
    check_empty_dfs,
    data_non_availability_and_missing_check,
    union_dataframes_with_missing_cols,
    join_all,
    clean_favourite_category,
    check_empty_dfs,
)
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)


def divide_chunks(arr: list, chunk_size: int):
    for i in range(0, len(arr), chunk_size):
        yield arr[i : i + chunk_size]


def dac_for_streaming_to_l1_intermediate_pipeline(
    input_df: DataFrame, cust_df: DataFrame, target_table_name: str
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="partition_date",
        target_table_name=target_table_name,
    )

    cust_df = data_non_availability_and_missing_check(
        df=cust_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name=target_table_name,
    )

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = (
        union_dataframes_with_missing_cols(
            [
                input_df.select(
                    f.max(
                        f.to_date(
                            (f.col("partition_date")).cast(StringType()), "yyyyMMdd"
                        )
                    ).alias("max_date")
                ),
                cust_df.select(f.max(f.col("event_partition_date")).alias("max_date")),
            ]
        )
        .select(f.min(f.col("max_date")).alias("min_date"))
        .collect()[0]
        .min_date
    )

    input_df = input_df.filter(
        f.to_date((f.col("partition_date")).cast(StringType()), "yyyyMMdd") <= min_value
    )
    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]


def dac_for_streaming_to_l1_pipeline(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name=target_table_name,
    )

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def dac_for_streaming_to_l1_pipeline_from_l0(
    input_df: DataFrame, target_table_name: str
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="partition_date",
        target_table_name=target_table_name,
    )

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def application_duration(
    streaming_df: DataFrame, application_df: DataFrame
) -> DataFrame:
    """
    :param streaming_df:
    :param application_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([streaming_df, application_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    w_recent_partition = Window.partitionBy("application_id").orderBy(
        f.col("partition_month").desc()
    )
    w_lag_stream = Window.partitionBy("msisdn", "partition_date").orderBy(
        f.col("begin_time")
    )

    application_df = (
        application_df.withColumn("rank", f.row_number().over(w_recent_partition))
        .where(f.col("rank") == 1)
        .withColumnRenamed("application_name", "application")
    ).alias("application_df")

    joined_df = (
        streaming_df.alias("streaming_df")
        .join(
            application_df,
            f.col("streaming_df.app_id") == f.col("application_df.application_id"),
            "left_outer",
        )
        .withColumn("lead_begin_time", f.lead(f.col("begin_time")).over(w_lag_stream))
        .withColumn(
            "duration", f.col("lead_begin_time") - f.col("begin_time")
        )  # duration in seconds
        .select(streaming_df.columns + ["application", "duration"])
    )

    return joined_df


def stream_process_ru_a_onair_vimmi(
    vimmi_usage_daily: DataFrame,
    customer_df: DataFrame,
    streaming_series_title_master: DataFrame,
    int_l1_streaming_content_type_features_dict: dict,
    l1_streaming_fav_content_group_by_volume_dict: dict,
    l1_streaming_fav_content_group_by_duration_dict: dict,
    int_l1_streaming_tv_channel_features_tbl_dict: dict,
    l1_streaming_fav_tv_channel_by_volume_dict: dict,
    l1_streaming_fav_tv_channel_by_duration_dict: dict,
    int_l1_streaming_tv_show_features_dict: dict,
    l1_streaming_fav_tv_show_by_episode_watched_dict: dict,
    int_l1_streaming_share_of_completed_episodes_features_dict: dict,
    int_l1_streaming_share_of_completed_episodes_ratio_features_dict: dict,
    l1_streaming_fav_tv_show_by_share_of_completed_episodes_dict: dict,
) -> [
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
]:
    """

    :param vimmi_usage_daily:
    :param customer_df:
    :param streaming_series_title_master:
    :param int_l1_streaming_content_type_features_dict:
    :param l1_streaming_fav_content_group_by_volume_dict:
    :param l1_streaming_fav_content_group_by_duration_dict:
    :param int_l1_streaming_tv_channel_features_tbl_dict:
    :param l1_streaming_fav_tv_channel_by_volume_dict:
    :param l1_streaming_fav_tv_channel_by_duration_dict:
    :param int_l1_streaming_tv_show_features_dict:
    :param l1_streaming_fav_tv_show_by_episode_watched_dict:
    :param int_l1_streaming_share_of_completed_episodes_features_dict:
    :param int_l1_streaming_share_of_completed_episodes_ratio_features_dict:
    :param l1_streaming_fav_tv_show_by_share_of_completed_episodes_dict:
    :return:
    """
    # ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([vimmi_usage_daily, customer_df]):
        return [
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
        ]

    input_df = data_non_availability_and_missing_check(
        df=vimmi_usage_daily,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_streaming_fav_tv_show_by_episode_watched",
    )

    customer_df = data_non_availability_and_missing_check(
        df=customer_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_streaming_fav_tv_show_by_episode_watched",
    )

    if check_empty_dfs([input_df, customer_df]):
        return [
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
        ]

    # ################################# End Implementing Data availability checks ###############################
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    sel_cols = [
        "access_method_num",
        "event_partition_date",
        "subscription_identifier",
        "start_of_week",
        "start_of_month",
    ]
    join_cols = [
        "access_method_num",
        "event_partition_date",
        "start_of_week",
        "start_of_month",
    ]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")
    dates_list = data_frame.select("event_partition_date").distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 5))
    add_list = mvv_array

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("event_partition_date").isin(*[curr_item]))
        cust_df = customer_df.filter(
            (f.col("event_partition_date").isin(*[curr_item]))
        ).select(sel_cols)
        joined_data_with_cust = small_df.join(cust_df, join_cols, "left")

        # section for int_l1_streaming_content_type_features
        int_l1_streaming_content_type_features = node_from_config(
            joined_data_with_cust, int_l1_streaming_content_type_features_dict
        )
        CNTX.catalog.save(
            int_l1_streaming_content_type_features_dict["output_catalog"],
            int_l1_streaming_content_type_features,
        )

        l1_streaming_fav_content_group_by_volume_df = node_from_config(
            int_l1_streaming_content_type_features,
            l1_streaming_fav_content_group_by_volume_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_content_group_by_volume_dict["output_catalog"],
            l1_streaming_fav_content_group_by_volume_df,
        )

        l1_streaming_fav_content_group_by_duration_df = node_from_config(
            int_l1_streaming_content_type_features,
            l1_streaming_fav_content_group_by_duration_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_content_group_by_duration_dict["output_catalog"],
            l1_streaming_fav_content_group_by_duration_df,
        )

        # section for int_l1_streaming_tv_channel_features
        int_l1_streaming_tv_channel_features = node_from_config(
            joined_data_with_cust, int_l1_streaming_tv_channel_features_tbl_dict
        )
        CNTX.catalog.save(
            int_l1_streaming_tv_channel_features_tbl_dict["output_catalog"],
            int_l1_streaming_tv_channel_features,
        )

        l1_streaming_fav_tv_channel_by_volume_df = node_from_config(
            int_l1_streaming_tv_channel_features,
            l1_streaming_fav_tv_channel_by_volume_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_tv_channel_by_volume_dict["output_catalog"],
            l1_streaming_fav_tv_channel_by_volume_df,
        )

        l1_streaming_fav_tv_channel_by_duration_df = node_from_config(
            int_l1_streaming_tv_channel_features,
            l1_streaming_fav_tv_channel_by_duration_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
            l1_streaming_fav_tv_channel_by_duration_df,
        )

        # TV Show features
        selective_df = joined_data_with_cust.select(
            "subscription_identifier",
            "event_partition_date",
            "start_of_week",
            "start_of_month",
            "access_method_num",
            "content_group",
            "title",
            "series_title",
            "genre",
        )
        CNTX.catalog.save("int_l0_streaming_vimmi_table", selective_df)

        # share_of_completed_episodes feature
        int_l1_streaming_share_of_completed_episodes_features = node_from_config(
            selective_df, int_l1_streaming_share_of_completed_episodes_features_dict
        )

        int_l1_streaming_share_of_completed_episodes_ratio_features_temp = (
            int_l1_streaming_share_of_completed_episodes_features.join(
                streaming_series_title_master, on="series_title", how="left"
            )
        )

        int_l1_streaming_share_of_completed_episodes_ratio_features_temp = (
            int_l1_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
                "share_of_completed_episodes",
                (
                    f.col("episode_watched_count")
                    / f.coalesce(f.col("total_episode_count"), f.lit(1))
                ),
            )
        )

        int_l1_streaming_share_of_completed_episodes_ratio_features = node_from_config(
            int_l1_streaming_share_of_completed_episodes_ratio_features_temp,
            int_l1_streaming_share_of_completed_episodes_ratio_features_dict,
        )

        l1_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
            int_l1_streaming_share_of_completed_episodes_ratio_features,
            l1_streaming_fav_tv_show_by_share_of_completed_episodes_dict,
        )

        CNTX.catalog.save(
            l1_streaming_fav_tv_show_by_share_of_completed_episodes_dict[
                "output_catalog"
            ],
            l1_streaming_fav_tv_show_by_share_of_completed_episodes,
        )

        # section for favorites episodes
        int_l1_streaming_tv_show_features = node_from_config(
            joined_data_with_cust, int_l1_streaming_tv_show_features_dict
        )
        l1_streaming_fav_tv_show_by_episode_watched = node_from_config(
            int_l1_streaming_tv_show_features,
            l1_streaming_fav_tv_show_by_episode_watched_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_tv_show_by_episode_watched_dict["output_catalog"],
            l1_streaming_fav_tv_show_by_episode_watched,
        )

    small_df = data_frame.filter(f.col("event_partition_date").isin(*[first_item]))
    cust_df = customer_df.filter(
        (f.col("event_partition_date").isin(*[first_item]))
    ).select(sel_cols)
    joined_data_with_cust = small_df.join(cust_df, join_cols, "left")

    # section for int_l1_streaming_content_type_features
    int_l1_streaming_content_type_features = node_from_config(
        joined_data_with_cust, int_l1_streaming_content_type_features_dict
    )

    l1_streaming_fav_content_group_by_volume_df = node_from_config(
        int_l1_streaming_content_type_features,
        l1_streaming_fav_content_group_by_volume_dict,
    )

    l1_streaming_fav_content_group_by_duration_df = node_from_config(
        int_l1_streaming_content_type_features,
        l1_streaming_fav_content_group_by_duration_dict,
    )

    # section for int_l1_streaming_tv_channel_features
    int_l1_streaming_tv_channel_features = node_from_config(
        joined_data_with_cust, int_l1_streaming_tv_channel_features_tbl_dict
    )

    l1_streaming_fav_tv_channel_by_volume_df = node_from_config(
        int_l1_streaming_tv_channel_features, l1_streaming_fav_tv_channel_by_volume_dict
    )

    l1_streaming_fav_tv_channel_by_duration_df = node_from_config(
        int_l1_streaming_tv_channel_features,
        l1_streaming_fav_tv_channel_by_duration_dict,
    )

    # section for favorites episodes
    int_l1_streaming_tv_show_features = node_from_config(
        joined_data_with_cust, int_l1_streaming_tv_show_features_dict
    )
    l1_streaming_fav_tv_show_by_episode_watched = node_from_config(
        int_l1_streaming_tv_show_features,
        l1_streaming_fav_tv_show_by_episode_watched_dict,
    )

    # TV Show features
    selective_df = joined_data_with_cust.select(
        "subscription_identifier",
        "event_partition_date",
        "start_of_week",
        "start_of_month",
        "access_method_num",
        "content_group",
        "title",
        "series_title",
        "genre",
    )

    # share_of_completed_episodes feature
    int_l1_streaming_share_of_completed_episodes_features = node_from_config(
        selective_df, int_l1_streaming_share_of_completed_episodes_features_dict
    )

    int_l1_streaming_share_of_completed_episodes_ratio_features_temp = (
        int_l1_streaming_share_of_completed_episodes_features.join(
            streaming_series_title_master, on="series_title", how="left"
        )
    )
    int_l1_streaming_share_of_completed_episodes_ratio_features_temp = (
        int_l1_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
            "share_of_completed_episodes",
            (
                f.col("episode_watched_count")
                / f.coalesce(f.col("total_episode_count"), f.lit(1))
            ),
        )
    )

    int_l1_streaming_share_of_completed_episodes_ratio_features = node_from_config(
        int_l1_streaming_share_of_completed_episodes_ratio_features_temp,
        int_l1_streaming_share_of_completed_episodes_ratio_features_dict,
    )

    l1_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
        int_l1_streaming_share_of_completed_episodes_ratio_features,
        l1_streaming_fav_tv_show_by_share_of_completed_episodes_dict,
    )

    return [
        int_l1_streaming_content_type_features,
        l1_streaming_fav_content_group_by_volume_df,
        l1_streaming_fav_content_group_by_duration_df,
        int_l1_streaming_tv_channel_features,
        l1_streaming_fav_tv_channel_by_volume_df,
        l1_streaming_fav_tv_channel_by_duration_df,
        selective_df,
        l1_streaming_fav_tv_show_by_share_of_completed_episodes,
        l1_streaming_fav_tv_show_by_episode_watched,
    ]


def stream_process_soc_mobile_data(
    input_data: DataFrame,
    cust_profile_df: DataFrame,
    int_l1_streaming_video_service_feature_dict: dict,
    l1_streaming_fav_video_service_by_download_feature_dict: dict,
    l1_streaming_2nd_fav_video_service_by_download_feature_dict: dict,
    int_l1_streaming_music_service_feature_dict: dict,
    l1_streaming_fav_music_service_by_download_feature_dict: dict,
    l1_streaming_2nd_fav_music_service_by_download_feature_dict: dict,
    int_l1_streaming_esport_service_feature_dict: dict,
    l1_streaming_fav_esport_service_by_download_feature_dict: dict,
    l1_streaming_2nd_fav_esport_service_by_download_feature_dict: dict,
    l1_streaming_visit_count_and_download_traffic_feature_dict: dict,
) -> [
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
]:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_data, cust_profile_df]):
        return [
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
        ]

    input_df = data_non_availability_and_missing_check(
        df=input_data,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_streaming_visit_count_and_download_traffic_feature",
    )

    customer_df = data_non_availability_and_missing_check(
        df=cust_profile_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_streaming_visit_count_and_download_traffic_feature",
    )

    if check_empty_dfs([input_df, customer_df]):
        return [
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
            get_spark_empty_df(),
        ]

    # ################################# End Implementing Data availability checks ###############################
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    sel_cols = [
        "access_method_num",
        "event_partition_date",
        "subscription_identifier",
        "start_of_week",
        "start_of_month",
    ]
    join_cols = [
        "access_method_num",
        "event_partition_date",
        "start_of_week",
        "start_of_month",
    ]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    data_frame = data_frame.withColumnRenamed("mobile_no", "access_method_num")
    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")
    dates_list = data_frame.select("event_partition_date").distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 5))
    add_list = mvv_array

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("event_partition_date").isin(*[curr_item]))
        cust_df = customer_df.filter(
            (f.col("event_partition_date").isin(*[curr_item]))
        ).select(sel_cols)
        joined_data_with_cust = small_df.join(cust_df, join_cols, "left")

        int_l1_streaming_video_service_feature = node_from_config(
            joined_data_with_cust, int_l1_streaming_video_service_feature_dict
        )
        CNTX.catalog.save(
            int_l1_streaming_video_service_feature_dict["output_catalog"],
            int_l1_streaming_video_service_feature,
        )
        l1_streaming_fav_video_service_by_download_feature = node_from_config(
            int_l1_streaming_video_service_feature,
            l1_streaming_fav_video_service_by_download_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_video_service_by_download_feature_dict["output_catalog"],
            l1_streaming_fav_video_service_by_download_feature,
        )
        l1_streaming_2nd_fav_video_service_by_download_feature = node_from_config(
            int_l1_streaming_video_service_feature,
            l1_streaming_2nd_fav_video_service_by_download_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_2nd_fav_video_service_by_download_feature_dict[
                "output_catalog"
            ],
            l1_streaming_2nd_fav_video_service_by_download_feature,
        )

        int_l1_streaming_music_service_feature = node_from_config(
            joined_data_with_cust, int_l1_streaming_music_service_feature_dict
        )
        CNTX.catalog.save(
            int_l1_streaming_music_service_feature_dict["output_catalog"],
            int_l1_streaming_music_service_feature,
        )
        l1_streaming_fav_music_service_by_download_feature = node_from_config(
            int_l1_streaming_music_service_feature,
            l1_streaming_fav_music_service_by_download_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_music_service_by_download_feature_dict["output_catalog"],
            l1_streaming_fav_music_service_by_download_feature,
        )
        l1_streaming_2nd_fav_music_service_by_download_feature = node_from_config(
            int_l1_streaming_music_service_feature,
            l1_streaming_2nd_fav_music_service_by_download_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_2nd_fav_music_service_by_download_feature_dict[
                "output_catalog"
            ],
            l1_streaming_2nd_fav_music_service_by_download_feature,
        )

        int_l1_streaming_esport_service_feature = node_from_config(
            joined_data_with_cust, int_l1_streaming_esport_service_feature_dict
        )
        CNTX.catalog.save(
            int_l1_streaming_esport_service_feature_dict["output_catalog"],
            int_l1_streaming_esport_service_feature,
        )
        l1_streaming_fav_esport_service_by_download_feature = node_from_config(
            int_l1_streaming_esport_service_feature,
            l1_streaming_fav_esport_service_by_download_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_fav_esport_service_by_download_feature_dict["output_catalog"],
            l1_streaming_fav_esport_service_by_download_feature,
        )
        l1_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(
            int_l1_streaming_esport_service_feature,
            l1_streaming_2nd_fav_esport_service_by_download_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_2nd_fav_esport_service_by_download_feature_dict[
                "output_catalog"
            ],
            l1_streaming_2nd_fav_esport_service_by_download_feature,
        )

        l1_streaming_visit_count_and_download_traffic_feature = node_from_config(
            joined_data_with_cust,
            l1_streaming_visit_count_and_download_traffic_feature_dict,
        )
        CNTX.catalog.save(
            l1_streaming_visit_count_and_download_traffic_feature_dict[
                "output_catalog"
            ],
            l1_streaming_visit_count_and_download_traffic_feature,
        )

    small_df = data_frame.filter(f.col("event_partition_date").isin(*[first_item]))
    cust_df = customer_df.filter(
        (f.col("event_partition_date").isin(*[first_item]))
    ).select(sel_cols)
    joined_data_with_cust = small_df.join(cust_df, join_cols, "left")

    int_l1_streaming_video_service_feature = node_from_config(
        joined_data_with_cust, int_l1_streaming_video_service_feature_dict
    )

    l1_streaming_fav_video_service_by_download_feature = node_from_config(
        int_l1_streaming_video_service_feature,
        l1_streaming_fav_video_service_by_download_feature_dict,
    )
    l1_streaming_2nd_fav_video_service_by_download_feature = node_from_config(
        int_l1_streaming_video_service_feature,
        l1_streaming_2nd_fav_video_service_by_download_feature_dict,
    )

    int_l1_streaming_music_service_feature = node_from_config(
        joined_data_with_cust, int_l1_streaming_music_service_feature_dict
    )
    l1_streaming_fav_music_service_by_download_feature = node_from_config(
        int_l1_streaming_music_service_feature,
        l1_streaming_fav_music_service_by_download_feature_dict,
    )
    l1_streaming_2nd_fav_music_service_by_download_feature = node_from_config(
        int_l1_streaming_music_service_feature,
        l1_streaming_2nd_fav_music_service_by_download_feature_dict,
    )

    int_l1_streaming_esport_service_feature = node_from_config(
        joined_data_with_cust, int_l1_streaming_esport_service_feature_dict
    )
    l1_streaming_fav_esport_service_by_download_feature = node_from_config(
        int_l1_streaming_esport_service_feature,
        l1_streaming_fav_esport_service_by_download_feature_dict,
    )

    l1_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(
        int_l1_streaming_esport_service_feature,
        l1_streaming_2nd_fav_esport_service_by_download_feature_dict,
    )

    l1_streaming_visit_count_and_download_traffic_feature = node_from_config(
        joined_data_with_cust,
        l1_streaming_visit_count_and_download_traffic_feature_dict,
    )

    return [
        int_l1_streaming_video_service_feature,
        l1_streaming_fav_video_service_by_download_feature,
        l1_streaming_2nd_fav_video_service_by_download_feature,
        int_l1_streaming_music_service_feature,
        l1_streaming_fav_music_service_by_download_feature,
        l1_streaming_2nd_fav_music_service_by_download_feature,
        int_l1_streaming_esport_service_feature,
        l1_streaming_fav_esport_service_by_download_feature,
        l1_streaming_2nd_fav_esport_service_by_download_feature,
        l1_streaming_visit_count_and_download_traffic_feature,
    ]


def build_streaming_sdr_sub_app_hourly_for_l3_monthly(
    input_df: DataFrame, master_df: DataFrame, cust_profile_df: DataFrame
) -> DataFrame:
    """
    :param input_df:
    :param master_df:
    :param cust_profile_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_profile_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_streaming_sdr_sub_app_hourly",
    )

    cust_profile_df = data_non_availability_and_missing_check(
        df=cust_profile_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_streaming_sdr_sub_app_hourly",
    )

    if check_empty_dfs([input_df, cust_profile_df]):
        return get_spark_empty_df()

    # ################################# End Implementing Data availability checks ###############################

    w_recent_partition = Window.partitionBy("application_id").orderBy(
        f.col("partition_month").desc()
    )
    max_master_application = master_df.select("partition_month").agg(
        f.max("partition_month").alias("partition_month")
    )

    master_application = (
        master_df.join(max_master_application, ["partition_month"])
        .withColumn("rank", f.row_number().over(w_recent_partition))
        .where(f.col("rank") == 1)
        .withColumnRenamed("application_id", "application")
    )

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

    application_group = ["videoplayers_editors", "music_audio", "game"]

    master_application = master_application.filter(
        f.lower(f.col("application_name")).isin(application_list)
        | f.lower(f.col("application_group")).isin(application_group)
    )

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    sel_cols = [
        "access_method_num",
        "event_partition_date",
        "subscription_identifier",
        "start_of_week",
        "start_of_month",
    ]
    join_cols = [
        "access_method_num",
        "event_partition_date",
        "start_of_week",
        "start_of_month",
    ]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    data_frame = data_frame.withColumnRenamed("msisdn", "access_method_num")
    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")
    dates_list = data_frame.select("event_partition_date").distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 3))
    add_list = mvv_array

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        filtered_input_df = data_frame.filter(
            f.col("event_partition_date").isin(*[curr_item])
        )
        customer_filtered_df = cust_profile_df.filter(
            f.col("event_partition_date").isin(*[curr_item])
        )

        return_df = filtered_input_df.join(master_application, ["application"])
        return_df = return_df.join(customer_filtered_df.select(sel_cols), join_cols)
        return_df = return_df.groupBy(
            [
                "subscription_identifier",
                "event_partition_date",
                "start_of_month",
                "hour",
                "application_name",
                "application_group",
            ]
        ).agg(f.sum(f.col("dw_kbyte")).alias("dw_kbyte"))

        CNTX.catalog.save("l1_streaming_sdr_sub_app_hourly", return_df)

    logging.info("running for dates {0}".format(str(first_item)))

    filtered_input_df = data_frame.filter(
        f.col("event_partition_date").isin(*[first_item])
    )
    customer_filtered_df = cust_profile_df.filter(
        f.col("event_partition_date").isin(*[first_item])
    )

    return_df = filtered_input_df.join(master_application, ["application"])
    return_df = return_df.join(customer_filtered_df.select(sel_cols), join_cols)
    return_df = return_df.groupBy(
        [
            "subscription_identifier",
            "event_partition_date",
            "start_of_month",
            "hour",
            "application_name",
            "application_group",
        ]
    ).agg(f.sum(f.col("dw_kbyte")).alias("dw_kbyte"))

    return return_df


def build_streaming_ufdr_streaming_quality_for_l3_monthly(
    input_df: DataFrame, master_df: DataFrame, cust_profile_df: DataFrame
) -> DataFrame:
    """
    :param input_df:
    :param master_df:
    :param cust_profile_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_profile_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_streaming_app_quality_features",
    )

    cust_profile_df = data_non_availability_and_missing_check(
        df=cust_profile_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_streaming_app_quality_features",
    )

    if check_empty_dfs([input_df, cust_profile_df]):
        return get_spark_empty_df()

    # ################################# End Implementing Data availability checks ###############################

    w_recent_partition = Window.partitionBy("application_id").orderBy(
        f.col("partition_month").desc()
    )
    max_master_application = master_df.select("partition_month").agg(
        f.max("partition_month").alias("partition_month")
    )

    master_application = (
        master_df.join(max_master_application, ["partition_month"])
        .withColumn("rank", f.row_number().over(w_recent_partition))
        .where(f.col("rank") == 1)
        .withColumnRenamed("application_id", "application")
    )

    application_list = [
        "youtube",
        "youtube_go",
        "youtubebyclick",
        "trueid",
        "truevisions",
        "monomaxx",
        "qqlive",
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

    master_application = master_application.filter(
        f.lower(f.col("application_name")).isin(application_list)
    )

    sel_cols = [
        "access_method_num",
        "event_partition_date",
        "subscription_identifier",
        "start_of_week",
        "start_of_month",
    ]
    join_cols = [
        "access_method_num",
        "event_partition_date",
        "start_of_week",
        "start_of_month",
    ]

    data_frame = input_df
    data_frame = data_frame.withColumnRenamed(
        "msisdn", "access_method_num"
    ).withColumnRenamed("app_id", "application")

    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")

    return_df = data_frame.join(master_application, ["application"])

    return_df = return_df.join(cust_profile_df.select(sel_cols), join_cols)
    return_df = return_df.withColumn(
        "calc_column",
        f.col("streaming_dw_packets")
        / (
            ((f.col("STREAMING_Download_DELAY") * f.lit(1000) * f.lit(8)) / f.lit(1024))
            / f.lit(1024)
        ),
    )

    return_df = return_df.select(
        "subscription_identifier",
        "access_method_num",
        "event_partition_date",
        "start_of_month",
        "application_name",
        "application_group",
        "calc_column",
    )

    return return_df


def build_streaming_ufdr_streaming_favourite_base_station_for_l3_monthly(
    input_df: DataFrame,
    master_df: DataFrame,
    geo_master_plan: DataFrame,
    cust_profile_df: DataFrame,
) -> DataFrame:
    """
    :param input_df:
    :param master_df:
    :param cust_profile_df:
    :param geo_master_plan:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_profile_df, geo_master_plan]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_streaming_base_station_features",
    )

    cust_profile_df = data_non_availability_and_missing_check(
        df=cust_profile_df,
        grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_streaming_base_station_features",
    )

    if check_empty_dfs([input_df, cust_profile_df, geo_master_plan]):
        return get_spark_empty_df()

    # ################################# End Implementing Data availability checks ###############################

    w_recent_partition = Window.partitionBy("application_id").orderBy(
        f.col("partition_month").desc()
    )
    max_master_application = master_df.select("partition_month").agg(
        f.max("partition_month").alias("partition_month")
    )

    master_application = (
        master_df.join(max_master_application, ["partition_month"])
        .withColumn("rank", f.row_number().over(w_recent_partition))
        .where(f.col("rank") == 1)
        .withColumnRenamed("application_id", "application")
    )

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

    master_application = master_application.filter(
        f.lower(f.col("application_name")).isin(application_list)
    )

    geo_master_plan_max = geo_master_plan.agg(
        f.max("partition_date").alias("partition_date")
    )
    geo_master_plan = geo_master_plan.join(
        geo_master_plan_max, ["partition_date"]
    ).select("soc_cgi_hex", "location_id")

    sel_cols = [
        "access_method_num",
        "event_partition_date",
        "subscription_identifier",
        "start_of_week",
        "start_of_month",
    ]
    join_cols = [
        "access_method_num",
        "event_partition_date",
        "start_of_week",
        "start_of_month",
    ]

    data_frame = input_df
    data_frame = data_frame.withColumnRenamed(
        "msisdn", "access_method_num"
    ).withColumnRenamed("app_id", "application")

    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")

    return_df = data_frame.join(master_application, ["application"])

    return_df = return_df.join(cust_profile_df.select(sel_cols), join_cols)

    return_df = return_df.groupBy(
        [
            "subscription_identifier",
            "access_method_num",
            "event_partition_date",
            "start_of_month",
            "application_name",
            "LAST_SAI_CGI_ECGI",
        ]
    ).agg(f.sum("L4_DW_THROUGHPUT").alias("download"))

    return_df = return_df.withColumnRenamed("LAST_SAI_CGI_ECGI", "soc_cgi_hex")

    return_df = return_df.join(geo_master_plan, ["soc_cgi_hex"])

    return return_df


def build_iab_category_table(
    aib_raw: DataFrame, aib_priority_mapping: DataFrame
) -> DataFrame:
    """
    Purpose: To remove categories that dont match iab standards, add category prioritisation
    :param metadata_table:
    :return:
    """
    aib_clean = (
        aib_raw.withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
        .filter(f.col("argument").isNotNull())
        .filter(f.col("argument") != "")
    ).drop_duplicates()
    total_rows_in_aib = aib_clean.count()
    unique_rows_in_aib = aib_clean.dropDuplicates(["argument"]).count()
    if total_rows_in_aib != unique_rows_in_aib:
        raise Exception(
            "IAB has duplicates!!! Please make sure to have unique rows at argument level."
        )

    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_1"
    ).withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
    iab_category_table = aib_clean.join(
        aib_priority_mapping, on=["level_1"], how="inner"
    )
    return iab_category_table


def build_stream_mobile_app_categories_master_table(
    df_app_categories_master_raw: DataFrame, aib_priority_mapping: DataFrame
) -> DataFrame:

    df_app_categories_master_clean = (
        (
            df_app_categories_master_raw.withColumn(
                "level_1", f.trim(f.lower(f.col("level_1")))
            )
            .filter(f.col("application_id").isNotNull())
            .filter(f.col("application_id") != "")
            .filter(f.col("application_name").isNotNull())
            .filter(f.col("application_name") != "")
        )
        .drop(*["group_name"])
        .drop_duplicates()
    )

    cols_to_check = ["application_name", "level_1", "level_2", "level_3", "level_4"]

    df_app_categories_master_clean_filtered = df_app_categories_master_clean.select(
        cols_to_check
    ).drop_duplicates()
    total_rows_in_master = df_app_categories_master_clean_filtered.count()
    unique_rows_in_master = df_app_categories_master_clean_filtered.dropDuplicates(
        subset=["application_name"]
    ).count()

    if total_rows_in_master != unique_rows_in_master:
        raise Exception(
            "IAB master has duplicates!!! Please make sure to have unique rows at application name level."
        )

    aib_priority_mapping = aib_priority_mapping.withColumnRenamed(
        "category", "level_1"
    ).withColumn("level_1", f.trim(f.lower(f.col("level_1"))))
    iab_category_table = df_app_categories_master_clean.join(
        aib_priority_mapping, on=["level_1"], how="inner"
    )
    return iab_category_table


###########################################
# CXENSE AGGREGATION
###########################################


def _remove_time_dupe_cxense_traffic(df_traffic: pyspark.sql.DataFrame):
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
        .withColumn("url0", f.lower("url0"))
        .dropDuplicates()
    )
    return df_cp


def node_clean_datasets(
    df_traffic_raw: pyspark.sql.DataFrame,
    df_cxense_cp_raw: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_traffic_raw]):
        return get_spark_empty_df()
    df_traffic = clean_cxense_traffic(df_traffic_raw)
    df_cp = clean_cxense_content_profile(df_cxense_cp_raw)
    return [df_traffic, df_cp]


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
                    f.desc("partition_month"),
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
        df_cat, on=[df_cp_cleaned.content_value == df_cat.argument]
    )
    return df_cp_join_iab


def node_create_content_profile_mapping(
    df_cp: pyspark.sql.DataFrame, df_cat: pyspark.sql.DataFrame
):
    df_cp_cleaned = create_content_profile_mapping(df_cp, df_cat)
    return df_cp_cleaned


def node_agg_cxense_traffic(df_traffic_cleaned: pyspark.sql.DataFrame):
    # aggregating url visits activetime, visit counts
    if check_empty_dfs([df_traffic_cleaned]):
        return get_spark_empty_df()
    df_traffic_agg = df_traffic_cleaned.groupBy(
        "mobile_no", "site_id", "url", "partition_date"
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


def get_matched_urls(df_traffic_join_cp_join_iab: pyspark.sql.DataFrame):
    if check_empty_dfs([df_traffic_join_cp_join_iab]):
        return get_spark_empty_df()
    df_traffic_join_cp_matched = df_traffic_join_cp_join_iab.filter(
        (f.col("siteid").isNotNull()) & (f.col("url0").isNotNull())
    )
    return df_traffic_join_cp_matched


def get_unmatched_urls(df_traffic_join_cp_join_iab: pyspark.sql.DataFrame):
    if check_empty_dfs([df_traffic_join_cp_join_iab]):
        return get_spark_empty_df()
    df_traffic_join_cp_missing = df_traffic_join_cp_join_iab.filter(
        (f.col("siteid").isNull()) | (f.col("url0").isNull())
    )
    return df_traffic_join_cp_missing


def node_get_matched_and_unmatched_urls(
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
                f.desc("partition_month"),
                f.desc("lastfetched"),
                f.desc("priority"),
            )
        ),
    ).filter("cat_rank = 1")
    return df_cp_join_iab_join_ais_priority


def node_get_best_match_for_unmatched_urls(
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
        .drop("siteid")
    )
    return df_traffic_get_missing_urls


def node_union_matched_and_unmatched_urls(
    df_traffic_join_cp_matched: pyspark.sql.DataFrame,
    df_traffic_get_missing_urls: pyspark.sql.DataFrame,
):
    pk = ["mobile_no", "partition_date", "url", "level_1", "priority"]
    columns_of_interest = pk + [
        "total_visit_duration",
        "total_visit_counts",
        "total_afternoon_duration",
        "total_afternoon_visit_counts",
    ]
    df_traffic_join_cp_matched = df_traffic_join_cp_matched.select(columns_of_interest)

    df_cxense_agg = (
        df_traffic_join_cp_matched.union(
            df_traffic_get_missing_urls.select(columns_of_interest)
        )
        .groupBy(pk)
        .agg(
            f.sum("total_visit_duration").alias("total_visit_duration"),
            f.sum("total_visit_counts").alias("total_visit_counts"),
            f.sum("total_afternoon_duration").alias("total_afternoon_duration"),
            f.sum("total_afternoon_visit_counts").alias("total_afternoon_visit_counts"),
        )
    )
    return df_cxense_agg


###########################################
# SOC APP AGGREGATION
###########################################


def node_join_soc_hourly_with_aib_agg(
    df_soc_app_hourly: pyspark.sql.DataFrame,
    df_app_categories_master: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_soc_app_hourly]):
        return get_spark_empty_df()

    df_soc_app_hourly = df_soc_app_hourly.filter(
        f.col("partition_date") >= "20210315"
    )  # TODO: revert this after hourly data issue is fixed

    df_soc_app_hourly_with_iab_raw = df_soc_app_hourly.withColumnRenamed(
        "msisdn", "mobile_no"
    ).join(
        f.broadcast(df_app_categories_master),
        on=[df_app_categories_master.application_id == df_soc_app_hourly.application],
        how="inner",
    )

    df_soc_app_hourly_with_iab_raw = df_soc_app_hourly_with_iab_raw.drop(
        *["application"]
    )
    df_soc_app_hourly_with_iab_raw = df_soc_app_hourly_with_iab_raw.withColumnRenamed(
        "application_name", "application"
    )

    group_by = ["mobile_no", "partition_date", "application", "level_1", "priority"]
    columns_of_interest = group_by + ["duration_sec", "dw_byte", "ld_hour"]
    df_soc_app_hourly_with_iab_agg = (
        df_soc_app_hourly_with_iab_raw.select(columns_of_interest)
        .withColumn(
            "is_afternoon",
            f.when(f.col("ld_hour").between(12, 17), f.lit(1)).otherwise(f.lit(0)),
        )
        .groupBy(group_by)
        .agg(
            f.sum(
                f.when((f.col("is_afternoon") == 1), f.col("dw_byte")).otherwise(
                    f.lit(0)
                )
            ).alias("total_soc_app_download_traffic_afternoon"),
            f.sum(
                f.when((f.col("is_afternoon") == 1), f.col("duration_sec")).otherwise(
                    f.lit(0)
                )
            ).alias("total_soc_app_visit_duration_afternoon"),
            f.sum("is_afternoon").alias("total_soc_app_visit_counts_afternoon"),
        )
        .withColumn(
            "total_soc_app_download_traffic_afternoon",
            f.expr("total_soc_app_download_traffic_afternoon/1000"),
        )
    )

    return df_soc_app_hourly_with_iab_agg


def node_join_soc_daily_with_aib_agg(
    df_soc_app_daily: pyspark.sql.DataFrame, df_iab: pyspark.sql.DataFrame
):
    if check_empty_dfs([df_soc_app_daily]):
        return get_spark_empty_df()
    df_iab = df_iab.filter(f.lower(f.trim(f.col("source_type"))) == "application")
    df_iab = df_iab.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")
    df_soc_app_daily_with_iab_raw = df_soc_app_daily.join(
        f.broadcast(df_iab),
        on=[df_iab.argument == df_soc_app_daily.application],
        how="inner",
    )

    group_by = ["mobile_no", "partition_date", "application", "level_1", "priority"]
    columns_of_interest = group_by + ["duration", "download_kb"]
    df_soc_app_daily_with_iab_agg = (
        df_soc_app_daily_with_iab_raw.select(columns_of_interest)
        .groupBy(group_by)
        .agg(
            f.sum("download_kb").alias("total_download_kb"),
            f.sum("duration").alias("total_duration"),
            f.count("*").alias("total_visit_counts"),
        )
    )
    return df_soc_app_daily_with_iab_agg


def combine_soc_app_daily_and_hourly_agg(
    df_soc_app_daily_with_iab_agg: pyspark.sql.DataFrame,
    df_soc_app_hourly_with_iab_agg: pyspark.sql.DataFrame,
):

    if check_empty_dfs([df_soc_app_daily_with_iab_agg, df_soc_app_hourly_with_iab_agg]):
        return get_spark_empty_df()

    join_keys = ["mobile_no", "partition_date", "application", "level_1", "priority"]
    df_combined_soc_app_daily_and_hourly_agg = df_soc_app_daily_with_iab_agg.join(
        df_soc_app_hourly_with_iab_agg, on=join_keys, how="full"
    )

    return df_combined_soc_app_daily_and_hourly_agg


def node_generate_soc_app_day_level_stats(
    df_soc_app_daily_with_iab_agg: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_soc_app_daily_with_iab_agg]):
        return get_spark_empty_df()
    key = ["mobile_no", "partition_date"]
    df_soc_app_day_level_stats = df_soc_app_daily_with_iab_agg.groupBy(key).agg(
        f.sum("total_download_kb").alias("total_soc_app_daily_download_traffic"),
        f.sum("total_visit_counts").alias("total_soc_app_daily_visit_count"),
        f.sum("total_duration").alias("total_soc_app_daily_visit_duration"),
    )
    return df_soc_app_day_level_stats


def node_join_soc_web_daily_with_with_aib_agg(
    df_soc_web_daily: pyspark.sql.DataFrame, df_iab: pyspark.sql.DataFrame
):
    if check_empty_dfs([df_soc_web_daily]):
        return get_spark_empty_df()

    df_soc_web_daily = df_soc_web_daily.repartition(1000)
    df_iab = df_iab.filter(f.lower(f.trim(f.col("source_type"))) == "url")
    df_iab = df_iab.filter(f.lower(f.trim(f.col("source_platform"))) == "soc")
    group_by = ["mobile_no", "partition_date", "domain", "level_1", "priority"]
    columns_of_interest = group_by + ["download_kb", "duration"]

    df_soc_web_daily_with_iab_raw = df_soc_web_daily.join(
        f.broadcast(df_iab),
        on=[df_iab.argument == df_soc_web_daily.domain],
        how="inner",
    ).select(columns_of_interest)

    df_soc_web_daily_with_iab_agg = df_soc_web_daily_with_iab_raw.groupBy(group_by).agg(
        f.sum("duration").alias("total_duration"),
        f.sum("download_kb").alias("total_download_kb"),
        f.count("*").alias("total_visit_counts"),
    )

    return df_soc_web_daily_with_iab_agg


def node_join_soc_web_daily_with_with_aib_agg_massive_processing(
    df_soc_web_daily: pyspark.sql.DataFrame, df_iab: pyspark.sql.DataFrame
):
    if check_empty_dfs([df_soc_web_daily]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"

    list_soc_web_daily = df_soc_web_daily.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_soc_web_daily

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 2
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_soc_web_daily_with_iab"

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_web_daily_chunk = df_soc_web_daily.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )

        output_df = node_join_soc_web_daily_with_with_aib_agg(
            df_soc_web_daily_chunk, df_iab
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_soc_web_daily_chunk = df_soc_web_daily.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    df_soc_web_daily_chunk = df_soc_web_daily.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    return_df = node_join_soc_web_daily_with_with_aib_agg(
        df_soc_web_daily_chunk, df_iab
    )
    CNTX.catalog.save(filepath, return_df)
    return return_df


def node_join_soc_web_hourly_with_with_aib_agg(
    df_soc_web_hourly: pyspark.sql.DataFrame, df_iab: pyspark.sql.DataFrame
):
    if check_empty_dfs([df_soc_web_hourly]):
        return get_spark_empty_df()
    group_by = ["mobile_no", "partition_date", "url", "level_1", "priority"]
    columns_of_interest = group_by + ["dw_kbyte", "air_port_duration", "ld_hour"]

    df_soc_web_hourly_with_iab_raw = (
        df_soc_web_hourly.withColumnRenamed("msisdn", "mobile_no").join(
            f.broadcast(df_iab),
            on=[df_iab.argument == df_soc_web_hourly.url],
            how="inner",
        )
    ).select(columns_of_interest)

    df_soc_web_hourly_with_iab_agg = (
        df_soc_web_hourly_with_iab_raw.withColumn(
            "is_afternoon",
            f.when(f.col("ld_hour").cast("int").between(12, 17), f.lit(1)).otherwise(
                f.lit(0)
            ),
        )
        .groupBy(group_by)
        .agg(
            f.sum(
                f.when((f.col("is_afternoon") == 1), f.col("dw_kbyte")).otherwise(
                    f.lit(0)
                )
            ).alias("total_soc_web_download_traffic_afternoon"),
            f.sum("is_afternoon").alias("total_visit_counts_afternoon"),
            f.sum(
                f.when(
                    (f.col("is_afternoon") == 1), f.col("air_port_duration")
                ).otherwise(f.lit(0))
            ).alias("total_visit_duration_afternoon"),
        )
    )

    return df_soc_web_hourly_with_iab_agg


def combine_soc_web_daily_and_hourly_agg(
    df_soc_web_daily_with_iab_agg: pyspark.sql.DataFrame,
    df_soc_web_hourly_with_iab_agg: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_soc_web_daily_with_iab_agg, df_soc_web_hourly_with_iab_agg]):
        return get_spark_empty_df()
    join_keys = ["mobile_no", "partition_date", "url", "level_1", "priority"]
    df_soc_web_daily_with_iab_agg = df_soc_web_daily_with_iab_agg.withColumnRenamed(
        "domain", "url"
    )

    df_combined_soc_app_daily_and_hourly_agg = df_soc_web_daily_with_iab_agg.join(
        df_soc_web_hourly_with_iab_agg, on=join_keys, how="full"
    )

    return df_combined_soc_app_daily_and_hourly_agg


def node_generate_soc_web_day_level_stats(
    df_soc_web_daily_with_iab_raw: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_soc_web_daily_with_iab_raw]):
        return get_spark_empty_df()
    key = ["mobile_no", "partition_date"]
    df_soc_web_day_level_stats = df_soc_web_daily_with_iab_raw.groupBy(key).agg(
        f.sum("total_download_kb").alias("total_soc_web_daily_download_traffic"),
        f.sum("total_visit_counts").alias("total_soc_web_daily_visit_count"),
        f.sum("total_duration").alias("total_soc_web_daily_visit_duration"),
    )
    return df_soc_web_day_level_stats


#######################################################
# WEB DAILY CATEGORY LEVEL FEATURES
#######################################################


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


#############################################
# SOC APP DAILY CATEGORY LEVEL FEATURES
#############################################


def node_soc_app_daily_category_level_features_massive_processing(
    df_combined_soc_app_daily_and_hourly_agg: pyspark.sql.DataFrame,
    df_soc_app_day_level_stats: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
    config_daily_level_features,
    config_ratio_based_features,
    config_popular_app_by_download_volume,
    config_popular_app_by_frequency_access,
    config_popular_app_by_visit_duration,
    config_most_popular_app_by_download_volume,
    config_most_popular_app_by_frequency_access,
    config_most_popular_app_by_visit_duration,
) -> DataFrame:
    if check_empty_dfs(
        [df_combined_soc_app_daily_and_hourly_agg, df_soc_app_day_level_stats]
    ):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"

    date_list_combined_soc_app_daily_and_hourly = (
        df_combined_soc_app_daily_and_hourly_agg.select(
            f.collect_set(source_partition_col).alias(source_partition_col)
        ).first()[source_partition_col]
    )

    date_list_soc_app_day_level_stats = df_soc_app_day_level_stats.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list(
        set(
            date_list_combined_soc_app_daily_and_hourly
            + date_list_soc_app_day_level_stats
        )
    )

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    df_cust = df_cust.withColumn(
        "partition_date", f.date_format(f.col("event_partition_date"), "yyyyMMdd")
    )
    df_cust = df_cust.withColumnRenamed("access_method_num", "mobile_no")
    df_cust = df_cust.select("mobile_no", "partition_date", "subscription_identifier")
    filepath = "l1_soc_app_daily_category_level_features"
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_combined_soc_app_daily_and_hourly_agg_chunk = (
            df_combined_soc_app_daily_and_hourly_agg.filter(
                f.col(source_partition_col).isin(*[curr_item])
            )
        )

        df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[curr_item]))
        df_soc_app_day_level_stats_chunk = df_soc_app_day_level_stats.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_soc_app_daily_category_level_features(
            df_combined_soc_app_daily_and_hourly_agg_chunk,
            df_soc_app_day_level_stats_chunk,
            df_cust_chunk,
            config_daily_level_features,
            config_ratio_based_features,
            config_popular_app_by_download_volume,
            config_popular_app_by_frequency_access,
            config_popular_app_by_visit_duration,
            config_most_popular_app_by_download_volume,
            config_most_popular_app_by_frequency_access,
            config_most_popular_app_by_visit_duration,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_combined_soc_app_daily_and_hourly_agg_chunk = (
        df_combined_soc_app_daily_and_hourly_agg.filter(
            f.col(source_partition_col).isin(*[first_item])
        )
    )
    df_soc_app_day_level_stats_chunk = df_soc_app_day_level_stats.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[first_item]))
    return_df = node_soc_app_daily_category_level_features(
        df_combined_soc_app_daily_and_hourly_agg_chunk,
        df_soc_app_day_level_stats_chunk,
        df_cust_chunk,
        config_daily_level_features,
        config_ratio_based_features,
        config_popular_app_by_download_volume,
        config_popular_app_by_frequency_access,
        config_popular_app_by_visit_duration,
        config_most_popular_app_by_download_volume,
        config_most_popular_app_by_frequency_access,
        config_most_popular_app_by_visit_duration,
    )
    return return_df


def node_soc_app_daily_category_level_features(
    df_combined_soc_app_daily_and_hourly_agg: pyspark.sql.DataFrame,
    df_soc_app_day_level_stats: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
    config_daily_level_features: Dict[str, Any],
    config_ratio_based_features: Dict[str, Any],
    config_popular_app_by_download_volume: Dict[str, Any],
    config_popular_app_by_frequency_access: Dict[str, Any],
    config_popular_app_by_visit_duration: Dict[str, Any],
    config_most_popular_app_by_download_volume: Dict[str, Any],
    config_most_popular_app_by_frequency_access: Dict[str, Any],
    config_most_popular_app_by_visit_duration: Dict[str, Any],
):
    if check_empty_dfs(
        [df_combined_soc_app_daily_and_hourly_agg, df_soc_app_day_level_stats]
    ):
        return get_spark_empty_df()
    df_soc_app_daily_features = node_from_config(
        df_combined_soc_app_daily_and_hourly_agg, config_daily_level_features
    )

    df_soc_app_daily_features_with_daily_stats = df_soc_app_daily_features.join(
        df_soc_app_day_level_stats, on=["mobile_no", "partition_date"], how="left"
    )

    df_fea_soc_non_fav_all = node_from_config(
        df_soc_app_daily_features_with_daily_stats, config_ratio_based_features
    )

    df_soc_app_clean = clean_favourite_category(
        df_combined_soc_app_daily_and_hourly_agg, "application"
    )

    # favourite app by download volume
    df_popular_app_by_download_volume = node_from_config(
        df_soc_app_clean, config_popular_app_by_download_volume
    )

    df_most_popular_app_by_download_volume = node_from_config(
        df_popular_app_by_download_volume, config_most_popular_app_by_download_volume
    )

    # favourite app by frequency access
    df_popular_app_by_frequency_access = node_from_config(
        df_soc_app_clean, config_popular_app_by_frequency_access
    )

    df_most_popular_app_by_frequency_access = node_from_config(
        df_popular_app_by_frequency_access, config_most_popular_app_by_frequency_access
    )

    # favourite app by visit duration
    df_popular_app_by_visit_duration = node_from_config(
        df_soc_app_clean, config_popular_app_by_visit_duration
    )
    df_most_popular_app_by_visit_duration = node_from_config(
        df_popular_app_by_visit_duration, config_most_popular_app_by_visit_duration
    )

    df_fea_soc_app_all = join_all(
        [
            df_fea_soc_non_fav_all,
            df_most_popular_app_by_download_volume,
            df_most_popular_app_by_frequency_access,
            df_most_popular_app_by_visit_duration,
        ],
        on=["mobile_no", "partition_date", "level_1"],
        how="outer",
    )
    df_fea = df_cust.join(
        df_fea_soc_app_all, ["mobile_no", "partition_date"], how="inner"
    )
    return df_fea


#############################################
# SOC APP DAILY FEATURES
#############################################


def node_soc_app_daily_features_massive_processing(
    df_soc,
    df_cust,
    config_popular_category_by_frequency_access,
    config_popular_category_by_visit_duration,
    config_most_popular_category_by_frequency_access,
    config_most_popular_category_by_visit_duration,
) -> DataFrame:
    if check_empty_dfs([df_soc]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_soc = df_soc.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_soc

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_soc_app_daily_features"

    df_cust = df_cust.withColumn(
        "partition_date", f.date_format(f.col("event_partition_date"), "yyyyMMdd")
    )
    df_cust = df_cust.withColumnRenamed("access_method_num", "mobile_no")
    df_cust = df_cust.select("mobile_no", "partition_date", "subscription_identifier")
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_chunk = df_soc.filter(f.col(source_partition_col).isin(*[curr_item]))
        df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[curr_item]))
        output_df = node_soc_app_daily_features(
            df_soc_chunk,
            df_cust_chunk,
            config_popular_category_by_frequency_access,
            config_popular_category_by_visit_duration,
            config_most_popular_category_by_frequency_access,
            config_most_popular_category_by_visit_duration,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_soc_chunk = df_soc.filter(f.col(source_partition_col).isin(*[first_item]))
    df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[first_item]))
    return_df = node_soc_app_daily_features(
        df_soc_chunk,
        df_cust_chunk,
        config_popular_category_by_frequency_access,
        config_popular_category_by_visit_duration,
        config_most_popular_category_by_frequency_access,
        config_most_popular_category_by_visit_duration,
    )

    return return_df


def node_soc_app_daily_features(
    df_soc: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
    config_popular_category_by_frequency_access: Dict[str, Any],
    config_popular_category_by_visit_duration: Dict[str, Any],
    config_most_popular_category_by_frequency_access: Dict[str, Any],
    config_most_popular_category_by_visit_duration: Dict[str, Any],
):
    if check_empty_dfs([df_soc]):
        return get_spark_empty_df()
    # favourite category by freuency access
    df_popular_category_by_frequency_access = node_from_config(
        df_soc, config_popular_category_by_frequency_access
    )
    df_most_popular_by_frequency_access = node_from_config(
        df_popular_category_by_frequency_access,
        config_most_popular_category_by_frequency_access,
    )

    # favourite category by visit duration
    df_popular_category_by_visit_duration = node_from_config(
        df_soc, config_popular_category_by_visit_duration
    )
    df_most_popular_category_by_visit_duration = node_from_config(
        df_popular_category_by_visit_duration,
        config_most_popular_category_by_visit_duration,
    )

    soc_app_daily_features = join_all(
        [
            df_most_popular_by_frequency_access,
            df_most_popular_category_by_visit_duration,
        ],
        on=["mobile_no", "partition_date"],
        how="outer",
    )
    df_fea = df_cust.join(
        soc_app_daily_features, ["mobile_no", "partition_date"], how="inner"
    )
    return df_fea


#############################################
# RELAY FEATURES
#############################################


def _relay_drop_nulls(df_relay: pyspark.sql.DataFrame):
    df_relay_cleaned = df_relay.filter(
        (f.col("mobile_no").isNotNull())
        & (f.col("mobile_no") != "")
        & (f.col("subscription_identifier") != "")
        & (f.col("subscription_identifier").isNotNull())
    ).dropDuplicates()
    return df_relay_cleaned


def node_pageviews_daily_features(
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
):
    if check_empty_dfs([df_pageviews]):
        return get_spark_empty_df()
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

    pageviews_daily_features = join_all(
        [
            df_total_visits,
            df_most_popular_subcategory1,
            df_most_popular_subcategory2,
            df_most_popular_url,
            # df_most_popular_productname,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "partition_date", "mobile_no"],
        how="outer",
    )

    return pageviews_daily_features

def node_pageviews_cid_level_daily_features(df_pageviews: pyspark.sql.DataFrame, config_total_visits: Dict[str, Any]):
    if check_empty_dfs([df_pageviews]):
        return get_spark_empty_df()
    df_pageviews_clean = _relay_drop_nulls(df_pageviews)
    df_pageviews_clean = clean_favourite_category(df_pageviews_clean, "cid")
    df_pageviews_visits = node_from_config(df_pageviews_clean, config_total_visits)
    return df_pageviews_visits

def node_engagement_conversion_daily_features(
    df_engagement: pyspark.sql.DataFrame,
    config_popular_product: Dict[str, Any],
    config_popular_cid: Dict[str, Any],
    config_most_popular_product: Dict[str, Any],
    config_most_popular_cid: Dict[str, Any],
):
    if check_empty_dfs([df_engagement]):
        return get_spark_empty_df()
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

    engagement_conversion_daily_features = join_all(
        [df_most_popular_product, df_most_popular_cid],
        on=["subscription_identifier", "partition_date", "mobile_no"],
        how="outer",
    )

    return engagement_conversion_daily_features


def node_engagement_conversion_cid_level_daily_features(
    df_engagement: pyspark.sql.DataFrame, config_total_visits: Dict[str, Any]
):
    if check_empty_dfs([df_engagement]):
        return get_spark_empty_df()
    df_engagement_clean = _relay_drop_nulls(df_engagement)
    df_engagement_clean = clean_favourite_category(df_engagement_clean, "cid")
    df_engagement_conversion = df_engagement_clean.filter(
        f.lower(f.trim(f.col("R42paymentStatus"))) == "successful"
    )
    df_engagement_conversion_visits = node_from_config(
        df_engagement_conversion, config_total_visits
    )
    return df_engagement_conversion_visits


def node_engagement_conversion_package_daily_features(
    df_engagement: pyspark.sql.DataFrame,
    config_popular_product: Dict[str, Any],
    config_popular_cid: Dict[str, Any],
    config_most_popular_product: Dict[str, Any],
    config_most_popular_cid: Dict[str, Any],
):
    if check_empty_dfs([df_engagement]):
        return get_spark_empty_df()
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

    engagement_conversion_package_daily_features = join_all(
        [
            df_most_popular_product,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "partition_date", "mobile_no"],
        how="outer",
    )

    return engagement_conversion_package_daily_features


def node_engagement_conversion_package_cid_level_daily_features(
    df_engagement: pyspark.sql.DataFrame, config_total_visits: Dict[str, Any]
):
    if check_empty_dfs([df_engagement]):
        return get_spark_empty_df()
    df_engagement_clean = _relay_drop_nulls(df_engagement)
    df_engagement_clean = clean_favourite_category(df_engagement_clean, "cid")
    df_engagement_conversion_package = df_engagement_clean.filter(
        f.lower(f.trim(f.col("R42Product_status"))) == "successful"
    ).withColumnRenamed("R42Product_name", "product")
    df_engagement_conversion_visits = node_from_config(
        df_engagement_conversion_package, config_total_visits
    )
    return df_engagement_conversion_visits


def node_combine_soc_all_and_cxense(
    df_cxense: pyspark.sql.DataFrame, df_comb_soc: pyspark.sql.DataFrame
):
    if check_empty_dfs([df_cxense, df_comb_soc]):
        return get_spark_empty_df()
    df_cxense = (
        df_cxense.withColumnRenamed("url", "app_or_url")
        .withColumnRenamed(
            "total_afternoon_duration", "total_cxense_afternoon_duration"
        )
        .withColumnRenamed("total_visit_duration", "total_cxense_visit_duration")
        .withColumnRenamed("total_visit_counts", "total_cxense_visit_counts")
        .withColumnRenamed(
            "total_afternoon_visit_counts", "total_cxense_afternoon_visit_counts"
        )
    )
    pk = ["mobile_no", "partition_date", "app_or_url", "level_1", "priority"]
    df_comb_all = df_cxense.join(df_comb_soc, on=pk, how="full")
    return df_comb_all


#############################################
# COMB ALL CATEGORY FEATURES
#############################################


def node_comb_all_features_massive_processing(
    df_comb_all: pyspark.sql.DataFrame,
    config_comb_all_create_single_view,
    config_com_all_day_level_stats,
    config_comb_all_sum_features,
    config_comb_all_sum_and_ratio_based_features,
    config_comb_all_popular_app_or_url,
    config_comb_all_most_popular_app_or_url_by_visit_count,
    config_comb_all_most_popular_app_or_url_by_visit_duration,
) -> DataFrame:
    if check_empty_dfs([df_comb_all]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_comb_all = df_comb_all.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_comb_all

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_all_features"

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_all_chunk = df_comb_all.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_comb_all_features(
            df_comb_all_chunk,
            config_comb_all_create_single_view,
            config_com_all_day_level_stats,
            config_comb_all_sum_features,
            config_comb_all_sum_and_ratio_based_features,
            config_comb_all_popular_app_or_url,
            config_comb_all_most_popular_app_or_url_by_visit_count,
            config_comb_all_most_popular_app_or_url_by_visit_duration,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_comb_all_chunk = df_comb_all.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    fea_comb_all = node_comb_all_features(
        df_comb_all_chunk,
        config_comb_all_create_single_view,
        config_com_all_day_level_stats,
        config_comb_all_sum_features,
        config_comb_all_sum_and_ratio_based_features,
        config_comb_all_popular_app_or_url,
        config_comb_all_most_popular_app_or_url_by_visit_count,
        config_comb_all_most_popular_app_or_url_by_visit_duration,
    )
    return fea_comb_all


def node_comb_all_features(
    df_comb_all,
    config_comb_all_create_single_view: Dict[str, Any],
    config_com_all_day_level_stats: Dict[str, Any],
    config_comb_all_sum_features: Dict[str, Any],
    config_comb_all_sum_and_ratio_based_features: Dict[str, Any],
    config_comb_all_popular_app_or_url: Dict[str, Any],
    config_comb_all_most_popular_app_or_url_by_visit_count: Dict[str, Any],
    config_comb_all_most_popular_app_or_url_by_visit_duration: Dict[str, Any],
):
    if check_empty_dfs([df_comb_all]):
        return get_spark_empty_df()
    df_comb_all_single_view = node_from_config(
        df_comb_all, config_comb_all_create_single_view
    )
    logging.info("1.completed features for config: config_comb_all_create_single_view")

    df_comb_all_day_level_stats = node_from_config(
        df_comb_all_single_view, config_com_all_day_level_stats
    )
    logging.info("2.completed features for config: config_com_all_day_level_stats")

    df_comb_all_sum_features = node_from_config(
        df_comb_all_single_view, config_comb_all_sum_features
    )
    logging.info("3.completed features for config: config_comb_all_sum_features")

    df_join_comb_all_sum_features_with_daily_stats = df_comb_all_sum_features.join(
        df_comb_all_day_level_stats,
        on=["mobile_no", "partition_date", "subscription_identifier"],
        how="left",
    )
    logging.info("4.joining sum features with daily stats")

    df_comb_all_sum_with_ratio_features = node_from_config(
        df_join_comb_all_sum_features_with_daily_stats,
        config_comb_all_sum_and_ratio_based_features,
    )
    logging.info(
        "5.completed features for config: config_comb_all_sum_and_ratio_based_features"
    )

    df_comb_all_single_view_cleaned = clean_favourite_category(
        df_comb_all_single_view, "app_or_url"
    )

    df_get_popular_app_or_url_ranks = node_from_config(
        df_comb_all_single_view_cleaned, config_comb_all_popular_app_or_url
    )
    logging.info("6.completed features for config: config_comb_all_popular_app_or_url")

    df_most_popular_app_or_url_by_visit_count = node_from_config(
        df_get_popular_app_or_url_ranks,
        config_comb_all_most_popular_app_or_url_by_visit_count,
    )
    logging.info(
        "7.completed features for config: config_comb_all_most_popular_app_or_url_by_visit_count"
    )

    df_most_popular_app_or_url_by_visit_duration = node_from_config(
        df_get_popular_app_or_url_ranks,
        config_comb_all_most_popular_app_or_url_by_visit_duration,
    )
    logging.info(
        "8.completed features for config: config_comb_all_most_popular_app_or_url_by_visit_duration"
    )

    pk = ["mobile_no", "partition_date", "level_1", "subscription_identifier"]
    df_comb_all = df_comb_all_sum_with_ratio_features.join(
        df_most_popular_app_or_url_by_visit_count, on=pk, how="left"
    ).join(df_most_popular_app_or_url_by_visit_duration, on=pk, how="left")
    logging.info("9.completed all features, saving..")
    return df_comb_all


#############################################
# COMP ALL DAILY FEATURES
#############################################


def node_comb_all_daily_features_massive_processing(
    df_comb_all,
    config_comb_all_popular_category,
    config_comb_all_most_popular_category_by_visit_counts,
    config_comb_all_most_popular_category_by_visit_duration,
) -> DataFrame:
    if check_empty_dfs([df_comb_all]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_comb_all = df_comb_all.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_comb_all
    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_all_features"

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_all_chunk = df_comb_all.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_comb_all_daily_features(
            df_comb_all_chunk,
            config_comb_all_popular_category,
            config_comb_all_most_popular_category_by_visit_counts,
            config_comb_all_most_popular_category_by_visit_duration,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_comb_all_chunk = df_comb_all.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    fea_comb_all = node_comb_all_daily_features(
        df_comb_all_chunk,
        config_comb_all_popular_category,
        config_comb_all_most_popular_category_by_visit_counts,
        config_comb_all_most_popular_category_by_visit_duration,
    )
    return fea_comb_all


def node_comb_all_daily_features(
    df_comb_all: pyspark.sql.DataFrame,
    config_comb_all_popular_category: Dict[str, Any],
    config_comb_all_most_popular_category_by_visit_counts: Dict[str, Any],
    config_comb_all_most_popular_category_by_visit_duration: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_comb_all]):
        return get_spark_empty_df()
    df_comb_all_popular_categories = node_from_config(
        df_comb_all, config_comb_all_popular_category
    )

    df_most_popular_category_by_visit_counts = node_from_config(
        df_comb_all_popular_categories,
        config_comb_all_most_popular_category_by_visit_counts,
    )

    df_most_popular_category_by_visit_duration = node_from_config(
        df_comb_all_popular_categories,
        config_comb_all_most_popular_category_by_visit_duration,
    )

    df_comb_all_daily_features = join_all(
        [
            df_most_popular_category_by_visit_counts,
            df_most_popular_category_by_visit_duration,
        ],
        on=["mobile_no", "partition_date", "subscription_identifier"],
        how="outer",
    )
    return df_comb_all_daily_features


def node_combine_soc_app_and_web_massive_processing(
    df_soc_app: pyspark.sql.DataFrame,
    df_soc_web: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
) -> DataFrame:
    if check_empty_dfs([df_soc_app, df_soc_web]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_soc_app = df_soc_app.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]
    list_soc_web = df_soc_web.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list(set(list_soc_app + list_soc_web))
    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_soc_web_and_app"
    df_cust = df_cust.withColumn(
        "partition_date", f.date_format(f.col("event_partition_date"), "yyyyMMdd")
    )
    df_cust = df_cust.withColumnRenamed("access_method_num", "mobile_no")
    df_cust = df_cust.select("mobile_no", "partition_date", "subscription_identifier")

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_soc_app_chunk = df_soc_app.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        df_soc_web_chunk = df_soc_web.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[curr_item]))
        output_df = node_combine_soc_app_and_web(
            df_soc_app_chunk, df_soc_web_chunk, df_cust_chunk
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_soc_app_chunk = df_soc_app.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[first_item]))
    df_soc_web_chunk = df_soc_web.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    return_df = node_combine_soc_app_and_web(
        df_soc_app_chunk, df_soc_web_chunk, df_cust_chunk
    )

    return return_df


def node_combine_soc_app_and_web(
    df_soc_app: pyspark.sql.DataFrame,
    df_soc_web: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
):
    if check_empty_dfs([df_soc_app, df_soc_web]):
        return get_spark_empty_df()
    df_soc_app = (
        df_soc_app.withColumnRenamed("application", "app_or_url")
        .withColumnRenamed("total_download_kb", "total_soc_app_download_kb")
        .withColumnRenamed("total_duration", "total_soc_app_duration")
        .withColumnRenamed("total_visit_counts", "total_soc_app_visit_counts")
    )

    df_soc_web = (
        df_soc_web.withColumnRenamed("url", "app_or_url")
        .withColumnRenamed("total_download_kb", "total_soc_web_download_kb")
        .withColumnRenamed("total_duration", "total_soc_web_duration")
        .withColumnRenamed("total_visit_counts", "total_soc_web_visit_counts")
        .withColumnRenamed(
            "total_visit_counts_afternoon", "total_soc_web_visit_counts_afternoon"
        )
        .withColumnRenamed(
            "total_visit_duration_afternoon", "total_soc_web_visit_duration_afternoon"
        )
    )
    pk = ["mobile_no", "partition_date", "app_or_url", "level_1", "priority"]
    df_soc_app_and_web = df_soc_web.join(df_soc_app, on=pk, how="full")
    df_soc_app_and_web = df_cust.join(
        df_soc_app_and_web, ["mobile_no", "partition_date"], how="inner"
    )
    return df_soc_app_and_web


def node_comb_soc_app_web_features_massive_processing(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_soc_sum_features: Dict[str, Any],
    config_comb_soc_daily_stats: Dict[str, Any],
    config_comb_soc_popular_app_or_url: Dict[str, Any],
    config_comb_soc_most_popular_app_or_url: Dict[str, Any],
    config_comb_soc_web_fea_all: Dict[str, Any],
) -> DataFrame:
    if check_empty_dfs([df_comb_web]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_comb_web = df_comb_web.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_comb_web

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_soc_features"
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_web_chunk = df_comb_web.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_comb_soc_app_web_features(
            df_comb_web_chunk,
            config_comb_soc_sum_features,
            config_comb_soc_daily_stats,
            config_comb_soc_popular_app_or_url,
            config_comb_soc_most_popular_app_or_url,
            config_comb_soc_web_fea_all,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_comb_web_chunk = df_comb_web.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    df_fea = node_comb_soc_app_web_features(
        df_comb_web_chunk,
        config_comb_soc_sum_features,
        config_comb_soc_daily_stats,
        config_comb_soc_popular_app_or_url,
        config_comb_soc_most_popular_app_or_url,
        config_comb_soc_web_fea_all,
    )
    return df_fea


def node_comb_soc_app_web_features(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_soc_sum_features: Dict[str, Any],
    config_comb_soc_daily_stats: Dict[str, Any],
    config_comb_soc_popular_app_or_url: Dict[str, Any],
    config_comb_soc_most_popular_app_or_url: Dict[str, Any],
    config_comb_soc_web_fea_all: Dict[str, Any],
):
    if check_empty_dfs([df_comb_web]):
        return get_spark_empty_df()
    df_comb_web = df_comb_web.repartition(1000)
    df_comb_web.cache()

    df_comb_web_sum_features = node_from_config(
        df_comb_web, config_comb_soc_sum_features
    )
    logging.info("1.completed features for: config_comb_soc_sum_features")

    df_comb_web_sum_daily_stats = node_from_config(
        df_comb_web, config_comb_soc_daily_stats
    )
    logging.info("2.completed features for: comb_web_sum_daily_stats")

    df_comb_web_popular_app_or_url = node_from_config(
        df_comb_web, config_comb_soc_popular_app_or_url
    )
    logging.info("3.completed features for: comb_web_popular_app_or_url")

    df_comb_web_most_popular_app_or_url = node_from_config(
        df_comb_web_popular_app_or_url, config_comb_soc_most_popular_app_or_url
    )
    logging.info("4.completed features for: comb_web_most_popular_app_or_url")

    df_comb_web_join_sum_stats_popular_features = df_comb_web_sum_features.join(
        df_comb_web_sum_daily_stats,
        on=["mobile_no", "partition_date", "subscription_identifier"],
        how="left",
    ).join(
        df_comb_web_most_popular_app_or_url,
        on=["mobile_no", "partition_date", "level_1", "subscription_identifier"],
        how="left",
    )
    logging.info("5.completed features for: comb_web_join_sum_stats_popular_features")

    df_comb_web_fea_all = node_from_config(
        df_comb_web_join_sum_stats_popular_features, config_comb_soc_web_fea_all
    )
    logging.info("6.completed all features, saving..")
    return df_comb_web_fea_all


#############################################
# COMB SOC DAILY FEATURES
#############################################


def node_comb_soc_app_web_daily_features_massive_processing(
    df_comb_soc_web_and_app: pyspark.sql.DataFrame,
    config_comb_soc_app_web_popular_category_by_download_traffic: Dict[str, Any],
    config_comb_soc_app_web_most_popular_category_by_download_traffic: Dict[str, Any],
) -> DataFrame:
    if check_empty_dfs([df_comb_soc_web_and_app]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_comb_soc_web_and_app = df_comb_soc_web_and_app.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_comb_soc_web_and_app

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_soc_daily_features"
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_soc_web_and_app_chunk = df_comb_soc_web_and_app.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_comb_soc_app_web_daily_features(
            df_comb_soc_web_and_app_chunk,
            config_comb_soc_app_web_popular_category_by_download_traffic,
            config_comb_soc_app_web_most_popular_category_by_download_traffic,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_comb_soc_web_and_app_chunk = df_comb_soc_web_and_app.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    fea_comb_soc = node_comb_soc_app_web_daily_features(
        df_comb_soc_web_and_app_chunk,
        config_comb_soc_app_web_popular_category_by_download_traffic,
        config_comb_soc_app_web_most_popular_category_by_download_traffic,
    )
    return fea_comb_soc


def node_comb_soc_app_web_daily_features(
    df_comb_soc_web_and_app: pyspark.sql.DataFrame,
    config_comb_soc_app_web_popular_category_by_download_traffic: Dict[str, Any],
    config_comb_soc_app_web_most_popular_category_by_download_traffic: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_comb_soc_web_and_app]):
        return get_spark_empty_df()
    df_comb_soc_web_and_app_popular_category = node_from_config(
        df_comb_soc_web_and_app,
        config_comb_soc_app_web_popular_category_by_download_traffic,
    )

    df_most_popular_category_by_download_volume = node_from_config(
        df_comb_soc_web_and_app_popular_category,
        config_comb_soc_app_web_most_popular_category_by_download_traffic,
    )

    return df_most_popular_category_by_download_volume


#############################################
# COMB WEB DAILY AGGREGATION
#############################################


def node_comb_web_daily_agg_massive_processing(
    df_cxense: pyspark.sql.DataFrame,
    df_soc_web: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
    config_comb_web_agg: Dict[str, Any],
) -> DataFrame:
    if check_empty_dfs([df_cxense, df_soc_web]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_cxense = df_cxense.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]
    list_soc_web = df_soc_web.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]
    mvv_array = list(set(list_cxense + list_soc_web))
    mvv_array = sorted(mvv_array)
    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_web_agg"
    df_cust = df_cust.withColumn(
        "partition_date", f.date_format(f.col("event_partition_date"), "yyyyMMdd")
    )
    df_cust = df_cust.withColumnRenamed("access_method_num", "mobile_no")
    df_cust = df_cust.select("mobile_no", "partition_date", "subscription_identifier")

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_cxense_chunk = df_cxense.filter(
            f.col(source_partition_col).isin(*[first_item])
        )
        df_soc_chunk = df_soc_web.filter(f.col(source_partition_col).isin(*[curr_item]))
        df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[curr_item]))
        output_df = node_comb_web_daily_agg(
            df_cxense_chunk,
            df_soc_chunk,
            df_cust_chunk,
            config_comb_web_agg,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_cxense_chunk = df_cxense.filter(f.col(source_partition_col).isin(*[first_item]))
    df_soc_chunk = df_soc_web.filter(f.col(source_partition_col).isin(*[first_item]))
    df_cust_chunk = df_cust.filter(f.col(source_partition_col).isin(*[first_item]))
    return_df = node_comb_web_daily_agg(
        df_cxense_chunk,
        df_soc_chunk,
        df_cust_chunk,
        config_comb_web_agg,
    )

    return return_df


def node_comb_web_daily_agg(
    df_cxense: pyspark.sql.DataFrame,
    df_soc_web: pyspark.sql.DataFrame,
    df_cust: pyspark.sql.DataFrame,
    config_comb_web_agg: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_cxense, df_soc_web]):
        return get_spark_empty_df()
    df_cxense = (
        df_cxense.withColumnRenamed(
            "total_visit_duration", "total_cxense_visit_duration"
        )
        .withColumnRenamed("total_visit_counts", "total_cxense_visit_counts")
        .withColumnRenamed(
            "total_afternoon_visit_counts", "total_cxense_afternoon_visit_counts"
        )
        .withColumnRenamed(
            "total_afternoon_duration", "total_cxense_afternoon_duration"
        )
    )

    df_soc_web = (
        df_soc_web.withColumnRenamed("total_duration", "total_soc_web_duration")
        .withColumnRenamed("total_visit_counts", "total_soc_web_visit_counts")
        .withColumnRenamed(
            "total_visit_counts_afternoon", "total_soc_web_visit_counts_afternoon"
        )
        .withColumnRenamed(
            "total_visit_duration_afternoon", "total_soc_web_visit_duration_afternoon"
        )
    )

    pk = ["mobile_no", "partition_date", "url", "level_1", "priority"]
    df_combine_web = df_soc_web.join(df_cxense, on=pk, how="outer")
    df_combine_total_web = node_from_config(df_combine_web, config_comb_web_agg)
    df_combine_total_web = df_cust.join(
        df_combine_total_web, ["mobile_no", "partition_date"], how="inner"
    )
    return df_combine_total_web


#############################################
# COMB WEB CATEGORY FEATURES
#############################################


def node_comb_web_daily_category_level_features_massive_processing(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_web_daily_stats: Dict[str, Any],
    config_comb_web_total_category_sum_features: Dict[str, Any],
    config_comb_web_total_sum_and_ratio_features: Dict[str, Any],
    config_comb_web_popular_url: Dict[str, Any],
    config_comb_web_most_popular_url_by_visit_duration: Dict[str, Any],
    config_comb_web_most_popular_url_by_visit_counts: Dict[str, Any],
) -> DataFrame:
    if check_empty_dfs([df_comb_web]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_comb_web = df_comb_web.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]

    mvv_array = list_comb_web

    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_web_category_level_features"

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_web_chunk = df_comb_web.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_comb_web_daily_category_level_features(
            df_comb_web_chunk,
            config_comb_web_daily_stats,
            config_comb_web_total_category_sum_features,
            config_comb_web_total_sum_and_ratio_features,
            config_comb_web_popular_url,
            config_comb_web_most_popular_url_by_visit_duration,
            config_comb_web_most_popular_url_by_visit_counts,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_comb_web_chunk = df_comb_web.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    fea_comb_web = node_comb_web_daily_category_level_features(
        df_comb_web_chunk,
        config_comb_web_daily_stats,
        config_comb_web_total_category_sum_features,
        config_comb_web_total_sum_and_ratio_features,
        config_comb_web_popular_url,
        config_comb_web_most_popular_url_by_visit_duration,
        config_comb_web_most_popular_url_by_visit_counts,
    )
    return fea_comb_web


def node_comb_web_daily_category_level_features(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_web_daily_stats: Dict[str, Any],
    config_comb_web_total_category_sum_features: Dict[str, Any],
    config_comb_web_total_sum_and_ratio_features: Dict[str, Any],
    config_comb_web_popular_url: Dict[str, Any],
    config_comb_web_most_popular_url_by_visit_duration: Dict[str, Any],
    config_comb_web_most_popular_url_by_visit_counts: Dict[str, Any],
):
    if check_empty_dfs([df_comb_web]):
        return get_spark_empty_df()
    df_comb_web_sum_features = node_from_config(
        df_comb_web, config_comb_web_total_category_sum_features
    )

    df_comb_web_sum_daily_stats = node_from_config(
        df_comb_web, config_comb_web_daily_stats
    )

    df_comb_web_sum_features_with_daily_stats = df_comb_web_sum_features.join(
        df_comb_web_sum_daily_stats,
        on=["mobile_no", "partition_date", "subscription_identifier"],
        how="left",
    )

    df_comb_web_sum_with_ratio_features = node_from_config(
        df_comb_web_sum_features_with_daily_stats,
        config_comb_web_total_sum_and_ratio_features,
    )

    df_comb_web = clean_favourite_category(df_comb_web, "url")

    df_comb_web_popular_url = node_from_config(df_comb_web, config_comb_web_popular_url)

    df_comb_web_most_popular_url_by_visit_duration = node_from_config(
        df_comb_web_popular_url, config_comb_web_most_popular_url_by_visit_duration
    )

    df_comb_web_most_popular_url_by_visit_counts = node_from_config(
        df_comb_web_popular_url, config_comb_web_most_popular_url_by_visit_counts
    )

    df_comb_web = join_all(
        [
            df_comb_web_sum_with_ratio_features,
            df_comb_web_most_popular_url_by_visit_duration,
            df_comb_web_most_popular_url_by_visit_counts,
        ],
        on=["mobile_no", "partition_date", "level_1", "subscription_identifier"],
        how="outer",
    )
    return df_comb_web


#############################################
# COMB WEB DAILY FEATURES
#############################################


def node_comb_web_daily_features_massive_processing(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_web_popular_category: Dict[str, Any],
    config_comb_web_most_popular_category_by_visit_duration: Dict[str, Any],
    config_comb_web_most_popular_category_by_visit_counts: Dict[str, Any],
) -> DataFrame:
    if check_empty_dfs([df_comb_web]):
        return get_spark_empty_df()
    CNTX = load_context(Path.cwd(), env=conf)
    source_partition_col = "partition_date"
    list_comb_web = df_comb_web.select(
        f.collect_set(source_partition_col).alias(source_partition_col)
    ).first()[source_partition_col]
    mvv_array = list_comb_web
    mvv_array = sorted(mvv_array)

    partition_num_per_job = 3
    mvv_new = list(divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)

    filepath = "l1_comb_web_daily_features"

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        df_comb_web_chunk = df_comb_web.filter(
            f.col(source_partition_col).isin(*[curr_item])
        )
        output_df = node_comb_web_daily_features(
            df_comb_web_chunk,
            config_comb_web_popular_category,
            config_comb_web_most_popular_category_by_visit_duration,
            config_comb_web_most_popular_category_by_visit_counts,
        )
        CNTX.catalog.save(filepath, output_df)

    logging.info("Final date to run {0}".format(str(first_item)))
    df_comb_web_chunk = df_comb_web.filter(
        f.col(source_partition_col).isin(*[first_item])
    )
    fea_comb_web = node_comb_web_daily_features(
        df_comb_web_chunk,
        config_comb_web_popular_category,
        config_comb_web_most_popular_category_by_visit_duration,
        config_comb_web_most_popular_category_by_visit_counts,
    )
    return fea_comb_web


def node_comb_web_daily_features(
    df_comb_web: pyspark.sql.DataFrame,
    config_comb_web_popular_category: Dict[str, Any],
    config_comb_web_most_popular_category_by_visit_duration: Dict[str, Any],
    config_comb_web_most_popular_category_by_visit_counts: Dict[str, Any],
):
    if check_empty_dfs([df_comb_web]):
        return get_spark_empty_df()
    df_comb_web = clean_favourite_category(df_comb_web, "url")

    df_comb_web_popular_category = node_from_config(
        df_comb_web, config_comb_web_popular_category
    )

    df_comb_web_most_popular_category_by_visit_duration = node_from_config(
        df_comb_web_popular_category,
        config_comb_web_most_popular_category_by_visit_duration,
    )

    df_comb_web_most_popular_category_by_visit_counts = node_from_config(
        df_comb_web_popular_category,
        config_comb_web_most_popular_category_by_visit_counts,
    )

    df_comb_web_daily_features = join_all(
        [
            df_comb_web_most_popular_category_by_visit_duration,
            df_comb_web_most_popular_category_by_visit_counts,
        ],
        ["mobile_no", "partition_date", "subscription_identifier"],
        how="outer",
    )
    return df_comb_web_daily_features
