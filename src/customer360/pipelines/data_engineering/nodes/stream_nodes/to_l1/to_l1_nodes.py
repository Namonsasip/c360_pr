import logging
import os
from pathlib import Path

import pyspark
from kedro.context.context import load_context
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import (
    add_event_week_and_month_from_yyyymmdd,
    check_empty_dfs,
    data_non_availability_and_missing_check,
    union_dataframes_with_missing_cols,
)
from src.customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


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

        int_l1_streaming_share_of_completed_episodes_ratio_features_temp = int_l1_streaming_share_of_completed_episodes_features.join(
            streaming_series_title_master, on="series_title", how="left"
        )

        int_l1_streaming_share_of_completed_episodes_ratio_features_temp = int_l1_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
            "share_of_completed_episodes",
            (
                f.col("episode_watched_count")
                / f.coalesce(f.col("total_episode_count"), f.lit(1))
            ),
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

    int_l1_streaming_share_of_completed_episodes_ratio_features_temp = int_l1_streaming_share_of_completed_episodes_features.join(
        streaming_series_title_master, on="series_title", how="left"
    )
    int_l1_streaming_share_of_completed_episodes_ratio_features_temp = int_l1_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
        "share_of_completed_episodes",
        (
            f.col("episode_watched_count")
            / f.coalesce(f.col("total_episode_count"), f.lit(1))
        ),
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


# ############ CXENSE AGGREGATION ################# #


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
    df_traffic_raw: pyspark.sql.DataFrame, df_cxense_cp_raw: pyspark.sql.DataFrame,
):
    df_traffic = clean_cxense_traffic(df_traffic_raw)
    df_cp = clean_cxense_content_profile(df_cxense_cp_raw)
    return [df_traffic, df_cp]


def create_content_profile_mapping(
    df_cp: pyspark.sql.DataFrame, df_cat: pyspark.sql.DataFrame
):
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
    df_traffic_join_cp_matched = df_traffic_join_cp_join_iab.filter(
        (f.col("siteid").isNotNull()) & (f.col("url0").isNotNull())
    )
    return df_traffic_join_cp_matched


def get_unmatched_urls(df_traffic_join_cp_join_iab: pyspark.sql.DataFrame):
    df_traffic_join_cp_missing = df_traffic_join_cp_join_iab.filter(
        (f.col("siteid").isNull()) | (f.col("url0").isNull())
    )
    return df_traffic_join_cp_missing


def node_get_matched_and_unmatched_urls(
    df_traffic_agg: pyspark.sql.DataFrame, df_cp_join_iab: pyspark.sql.DataFrame
):
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
    pk = ["mobile_no", "partition_date", "url", "level_1"]
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


# ############ SOC APP AGGREGATION ################# #


def node_join_soc_hourly_with_aib_agg(
    df_soc_app_hourly: pyspark.sql.DataFrame, df_iab: pyspark.sql.DataFrame
):
    df_soc_app_hourly_with_iab_raw = df_soc_app_hourly.withColumnRenamed(
        "msisdn", "mobile_no"
    ).join(
        f.broadcast(df_iab),
        on=[df_iab.argument == df_soc_app_hourly.application],
        how="inner",
    )

    group_by = ["mobile_no", "partition_date", "application", "level_1"]
    columns_of_interest = group_by + ["duration_sec", "dw_byte", "ld_hour"]
    df_soc_app_hourly_with_iab_agg = (
        df_soc_app_hourly_with_iab_raw.select(columns_of_interest)
        .withColumn(
            "is_afternoon",
            f.when(f.col("ld_hour").between(12, 17), f.lit(1)).otherwise(f.lit(0)),
        )
        .groupBy(group_by)
        .agg(
            f.sum("duration_sec").alias("total_duration_sec"),
            f.sum("dw_byte").alias("total_dw_byte"),
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
        .withColumn("total_dw_kbyte", f.expr("total_dw_byte/1000"))
    )
    return df_soc_app_hourly_with_iab_agg


def node_join_soc_daily_with_aib_agg(
    df_soc_app_daily: pyspark.sql.DataFrame, df_iab: pyspark.sql.DataFrame
):
    df_soc_app_daily_with_iab_raw = df_soc_app_daily.join(
        f.broadcast(df_iab),
        on=[df_iab.argument == df_soc_app_daily.application],
        how="inner",
    )

    group_by = ["mobile_no", "partition_date", "application", "level_1"]
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


def node_generate_soc_app_day_level_stats(
    df_soc_app_daily_with_iab_agg: pyspark.sql.DataFrame,
):
    key = ["mobile_no", "partition_date"]
    df_soc_app_day_level_stats = df_soc_app_daily_with_iab_agg.groupBy(key).agg(
        f.sum("total_download_kb").alias("total_soc_app_daily_download_traffic"),
        f.count("*").alias("total_soc_app_daily_visit_count"),
        f.sum("total_duration").alias("total_soc_app_daily_visit_duration"),
    )
    return df_soc_app_day_level_stats
