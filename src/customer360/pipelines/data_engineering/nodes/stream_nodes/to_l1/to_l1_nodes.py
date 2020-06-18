from pyspark.sql import functions as f, DataFrame, Window
from pyspark.sql.types import *
import logging
import os
from pathlib import Path

from kedro.context.context import load_context

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, add_start_of_week_and_month, add_event_week_and_month_from_yyyymmdd
from src.customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def dac_for_streaming_to_l1_intermediate_pipeline(input_df: DataFrame, cust_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                      target_table_name=target_table_name)

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]


def dac_for_streaming_to_l1_pipeline(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="event_partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def dac_for_streaming_to_l1_pipeline_from_l0(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def application_duration(streaming_df: DataFrame, application_df: DataFrame) -> DataFrame:
    w_recent_partition = Window.partitionBy("application_id").orderBy(f.col("partition_month").desc())
    w_lag_stream = Window.partitionBy("msisdn", "partition_date").orderBy(f.col("begin_time"))

    application_df = (application_df
                      .withColumn("rank", f.row_number().over(w_recent_partition))
                      .where(f.col("rank") == 1)
                      .withColumnRenamed("application_name", "application")
                      ).alias("application_df")

    joined_df = (streaming_df.alias("streaming_df")
                 .join(application_df, f.col("streaming_df.app_id") == f.col("application_df.application_id"),
                       "left_outer")
                 .withColumn("lead_begin_time", f.lead(f.col("begin_time")).over(w_lag_stream))
                 .withColumn("duration", f.col("lead_begin_time") - f.col("begin_time"))  # duration in seconds
                 .select(streaming_df.columns + ["application", "duration"])
                 )

    return joined_df


def stream_process_ru_a_onair_vimmi(vimmi_usage_daily: DataFrame,
                                    customer_df: DataFrame,
                                    int_l1_streaming_content_type_features_dict: dict,
                                    l1_streaming_fav_content_group_by_volume_dict: dict,
                                    l1_streaming_fav_content_group_by_duration_dict: dict,

                                    int_l1_streaming_tv_channel_features_tbl_dict: dict,
                                    l1_streaming_fav_tv_channel_by_volume_dict: dict,
                                    l1_streaming_fav_tv_channel_by_duration_dict: dict,

                                    int_l1_streaming_tv_show_features_dict: dict,
                                    l1_streaming_fav_tv_show_by_episode_watched_dict: dict,
                                    ) -> [DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame
    , DataFrame]:
    """
    :param vimmi_usage_daily:
    :param customer_df:
    :param int_l1_streaming_content_type_features_dict:
    :param l1_streaming_fav_content_group_by_volume_dict:
    :param l1_streaming_fav_content_group_by_duration_dict:
    :param int_l1_streaming_tv_channel_features_tbl_dict:
    :param l1_streaming_fav_tv_channel_by_volume_dict:
    :param l1_streaming_fav_tv_channel_by_duration_dict:
    :param int_l1_streaming_tv_show_features_dict:
    :param l1_streaming_fav_tv_show_by_episode_watched_dict:
    :return:
    """
    input_df = vimmi_usage_daily
    # ################################# Start Implementing Data availability checks #############################
    # if check_empty_dfs([vimmi_usage_daily, customer_df]):
    #     return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
    #             get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]
    #
    # input_df = data_non_availability_and_missing_check(
    #     df=vimmi_usage_daily,
    #     grouping="daily",
    #     par_col="partition_date",
    #     target_table_name="int_l1_streaming_tv_channel_features")
    #
    # customer_df = data_non_availability_and_missing_check(
    #     df=customer_df,
    #     grouping="daily",
    #     par_col="event_partition_date",
    #     target_table_name="int_l1_streaming_tv_channel_features")
    #
    # if check_empty_dfs([input_df, customer_df]):
    #     return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
    #             get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]
    #
    # ################################# End Implementing Data availability checks ###############################
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    sel_cols = ['access_method_num',
                'event_partition_date',
                "subscription_identifier",
                "register_date",
                "national_id_card",
                "start_of_week",
                "start_of_month",
                ]
    join_cols = ['access_method_num', 'event_partition_date', "start_of_week", "start_of_month", "register_date"]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")
    dates_list = data_frame.select('event_partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 1))
    add_list = mvv_array

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("event_partition_date").isin(*[curr_item]))
        cust_df = customer_df.filter((f.col("event_partition_date").isin(*[curr_item]))).select(sel_cols)
        joined_data_with_cust = small_df.join(cust_df, join_cols, 'left')

        # section for int_l1_streaming_content_type_features
        int_l1_streaming_content_type_features = node_from_config(joined_data_with_cust,
                                                                  int_l1_streaming_content_type_features_dict)
        CNTX.catalog.save(int_l1_streaming_content_type_features_dict['output_catalog'],
                          int_l1_streaming_content_type_features)

        l1_streaming_fav_content_group_by_volume_df = node_from_config(int_l1_streaming_content_type_features,
                                                                       l1_streaming_fav_content_group_by_volume_dict)
        CNTX.catalog.save(l1_streaming_fav_content_group_by_volume_dict['output_catalog'],
                          l1_streaming_fav_content_group_by_volume_df)

        l1_streaming_fav_content_group_by_duration_df = node_from_config(int_l1_streaming_content_type_features,
                                                                         l1_streaming_fav_content_group_by_duration_dict)
        CNTX.catalog.save(l1_streaming_fav_content_group_by_duration_dict["output_catalog"],
                          l1_streaming_fav_content_group_by_duration_df)

        # section for int_l1_streaming_tv_channel_features
        int_l1_streaming_tv_channel_features = node_from_config(joined_data_with_cust,
                                                                int_l1_streaming_tv_channel_features_tbl_dict)
        CNTX.catalog.save(int_l1_streaming_tv_channel_features_tbl_dict["output_catalog"],
                          int_l1_streaming_tv_channel_features)

        l1_streaming_fav_tv_channel_by_volume_df = node_from_config(int_l1_streaming_tv_channel_features,
                                                                    l1_streaming_fav_tv_channel_by_volume_dict)
        CNTX.catalog.save(l1_streaming_fav_tv_channel_by_volume_dict["output_catalog"],
                          l1_streaming_fav_tv_channel_by_volume_df)

        l1_streaming_fav_tv_channel_by_duration_df = node_from_config(int_l1_streaming_tv_channel_features,
                                                                      l1_streaming_fav_tv_channel_by_duration_dict)
        CNTX.catalog.save(l1_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
                          l1_streaming_fav_tv_channel_by_duration_df)

        # TV Show features
        selective_df = joined_data_with_cust.\
            select("subscription_identifier", "event_partition_date", "start_of_week", "start_of_month",
                   "access_method_num", "register_date", "national_id_card", "content_group", "title", "series_title")
        CNTX.catalog.save("int_l0_streaming_vimmi_table", selective_df)

        # section for favorites episodes
        int_l1_streaming_tv_show_features = node_from_config(joined_data_with_cust,
                                                             int_l1_streaming_tv_show_features_dict)
        l1_streaming_fav_tv_show_by_episode_watched = node_from_config(int_l1_streaming_tv_show_features,
                                                                       l1_streaming_fav_tv_show_by_episode_watched_dict)
        CNTX.catalog.save(l1_streaming_fav_tv_show_by_episode_watched_dict['output_catalog'],
                          l1_streaming_fav_tv_show_by_episode_watched)

    small_df = data_frame.filter(f.col("partition_date").isin(*[first_item]))
    cust_df = customer_df.filter((f.col("partition_date").isin(*[first_item]))).select(sel_cols)
    joined_data_with_cust = small_df.join(cust_df, join_cols, 'left')

    # section for int_l1_streaming_content_type_features
    int_l1_streaming_content_type_features = node_from_config(joined_data_with_cust,
                                                              int_l1_streaming_content_type_features_dict)

    l1_streaming_fav_content_group_by_volume_df = node_from_config(int_l1_streaming_content_type_features,
                                                                   l1_streaming_fav_content_group_by_volume_dict)

    l1_streaming_fav_content_group_by_duration_df = node_from_config(int_l1_streaming_content_type_features,
                                                                     l1_streaming_fav_content_group_by_duration_dict)

    # section for int_l1_streaming_tv_channel_features
    int_l1_streaming_tv_channel_features = node_from_config(joined_data_with_cust,
                                                            int_l1_streaming_tv_channel_features_tbl_dict)

    l1_streaming_fav_tv_channel_by_volume_df = node_from_config(int_l1_streaming_tv_channel_features,
                                                                l1_streaming_fav_tv_channel_by_volume_dict)

    l1_streaming_fav_tv_channel_by_duration_df = node_from_config(int_l1_streaming_tv_channel_features,
                                                                  l1_streaming_fav_tv_channel_by_duration_dict)

    # section for favorites episodes
    int_l1_streaming_tv_show_features = node_from_config(joined_data_with_cust,
                                                         int_l1_streaming_tv_show_features_dict)
    l1_streaming_fav_tv_show_by_episode_watched = node_from_config(int_l1_streaming_tv_show_features,
                                                                   l1_streaming_fav_tv_show_by_episode_watched_dict)

    # TV Show features
    selective_df = joined_data_with_cust. \
        select("subscription_identifier", "event_partition_date", "start_of_week", "start_of_month",
               "access_method_num", "register_date", "national_id_card", "content_group", "title", "series_title")

    return [int_l1_streaming_content_type_features, l1_streaming_fav_content_group_by_volume_df,
            l1_streaming_fav_content_group_by_duration_df,
            int_l1_streaming_tv_channel_features,
            l1_streaming_fav_tv_channel_by_volume_df, l1_streaming_fav_tv_channel_by_duration_df,
            selective_df,
            l1_streaming_fav_tv_show_by_episode_watched
            ]


def stream_process_soc_mobile_data(input_data: DataFrame,
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

                                   l1_streaming_visit_count_and_download_traffic_feature_dict: dict
                                   ) -> [DataFrame, DataFrame, DataFrame, DataFrame, DataFrame,
                                         DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:

    input_df = input_data
    customer_df = cust_profile_df
    ################################# Start Implementing Data availability checks #############################
    # if check_empty_dfs([input_data, cust_profile_df]):
    #     return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
    #             get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
    #             get_spark_empty_df(), get_spark_empty_df()]
    #
    # input_df = data_non_availability_and_missing_check(
    #     df=input_data,
    #     grouping="daily",
    #     par_col="partition_date",
    #     target_table_name="int_l1_streaming_video_service_feature")
    #
    # customer_df = data_non_availability_and_missing_check(
    #     df=cust_profile_df,
    #     grouping="daily",
    #     par_col="event_partition_date",
    #     target_table_name="int_l1_streaming_video_service_feature")
    #
    # if check_empty_dfs([input_df, customer_df]):
    #     return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
    #             get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
    #             get_spark_empty_df(), get_spark_empty_df()]
    #
    # ################################# End Implementing Data availability checks ###############################
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    sel_cols = ['access_method_num',
                'event_partition_date',
                "subscription_identifier",
                "register_date",
                "national_id_card",
                "start_of_week",
                "start_of_month",
                ]
    join_cols = ['access_method_num', 'event_partition_date', "start_of_week", "start_of_month", "register_date"]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    data_frame = add_event_week_and_month_from_yyyymmdd(data_frame, "partition_date")
    dates_list = data_frame.select('event_partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 1))
    add_list = mvv_array

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("event_partition_date").isin(*[curr_item]))
        cust_df = customer_df.filter((f.col("event_partition_date").isin(*[curr_item]))).select(sel_cols)
        joined_data_with_cust = small_df.join(cust_df, join_cols, 'left')

        int_l1_streaming_video_service_feature = node_from_config(joined_data_with_cust,
                                                                  int_l1_streaming_video_service_feature_dict)
        CNTX.catalog.save(int_l1_streaming_video_service_feature_dict['output_catalog'],
                          int_l1_streaming_video_service_feature)
        l1_streaming_fav_video_service_by_download_feature = node_from_config(
            int_l1_streaming_video_service_feature,
            l1_streaming_fav_video_service_by_download_feature_dict)
        CNTX.catalog.save(l1_streaming_fav_video_service_by_download_feature_dict['output_catalog'],
                          l1_streaming_fav_video_service_by_download_feature)
        l1_streaming_2nd_fav_video_service_by_download_feature = node_from_config(
            int_l1_streaming_video_service_feature,
            l1_streaming_2nd_fav_video_service_by_download_feature_dict)
        CNTX.catalog.save(l1_streaming_2nd_fav_video_service_by_download_feature_dict['output_catalog'],
                          l1_streaming_2nd_fav_video_service_by_download_feature)

        int_l1_streaming_music_service_feature = node_from_config(joined_data_with_cust,
                                                                  int_l1_streaming_music_service_feature_dict)
        CNTX.catalog.save(int_l1_streaming_music_service_feature_dict['output_catalog'],
                          int_l1_streaming_music_service_feature)
        l1_streaming_fav_music_service_by_download_feature = node_from_config(
            int_l1_streaming_music_service_feature,
            l1_streaming_fav_music_service_by_download_feature_dict)
        CNTX.catalog.save(l1_streaming_fav_music_service_by_download_feature_dict['output_catalog'],
                          l1_streaming_fav_music_service_by_download_feature)
        l1_streaming_2nd_fav_music_service_by_download_feature = node_from_config(
            int_l1_streaming_music_service_feature,
            l1_streaming_2nd_fav_music_service_by_download_feature_dict)
        CNTX.catalog.save(l1_streaming_2nd_fav_music_service_by_download_feature_dict['output_catalog'],
                          l1_streaming_2nd_fav_music_service_by_download_feature)

        int_l1_streaming_esport_service_feature = node_from_config(joined_data_with_cust,
                                                                   int_l1_streaming_esport_service_feature_dict)
        CNTX.catalog.save(int_l1_streaming_esport_service_feature_dict['output_catalog'],
                          int_l1_streaming_esport_service_feature)
        l1_streaming_fav_esport_service_by_download_feature = node_from_config(
            int_l1_streaming_esport_service_feature,
            l1_streaming_fav_esport_service_by_download_feature_dict)
        CNTX.catalog.save(l1_streaming_fav_esport_service_by_download_feature_dict['output_catalog'],
                          l1_streaming_fav_esport_service_by_download_feature)
        l1_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(
            int_l1_streaming_esport_service_feature,
            l1_streaming_2nd_fav_esport_service_by_download_feature_dict)
        CNTX.catalog.save(l1_streaming_2nd_fav_esport_service_by_download_feature_dict['output_catalog'],
                          l1_streaming_2nd_fav_esport_service_by_download_feature)

        l1_streaming_visit_count_and_download_traffic_feature = node_from_config(
            joined_data_with_cust,
            l1_streaming_visit_count_and_download_traffic_feature_dict)
        CNTX.catalog.save(l1_streaming_visit_count_and_download_traffic_feature_dict['output_catalog'],
                          l1_streaming_visit_count_and_download_traffic_feature)

    int_l1_streaming_video_service_feature = node_from_config(joined_data_with_cust,
                                                              int_l1_streaming_video_service_feature_dict)

    l1_streaming_fav_video_service_by_download_feature = node_from_config(
        int_l1_streaming_video_service_feature,
        l1_streaming_fav_video_service_by_download_feature_dict)
    l1_streaming_2nd_fav_video_service_by_download_feature = node_from_config(
        int_l1_streaming_video_service_feature,
        l1_streaming_2nd_fav_video_service_by_download_feature_dict)

    int_l1_streaming_music_service_feature = node_from_config(joined_data_with_cust,
                                                              int_l1_streaming_music_service_feature_dict)
    l1_streaming_fav_music_service_by_download_feature = node_from_config(
        int_l1_streaming_music_service_feature,
        l1_streaming_fav_music_service_by_download_feature_dict)
    l1_streaming_2nd_fav_music_service_by_download_feature = node_from_config(
        int_l1_streaming_music_service_feature,
        l1_streaming_2nd_fav_music_service_by_download_feature_dict)

    int_l1_streaming_esport_service_feature = node_from_config(joined_data_with_cust,
                                                               int_l1_streaming_esport_service_feature_dict)
    l1_streaming_fav_esport_service_by_download_feature = node_from_config(
        int_l1_streaming_esport_service_feature,
        l1_streaming_fav_esport_service_by_download_feature_dict)

    l1_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(
        int_l1_streaming_esport_service_feature,
        l1_streaming_2nd_fav_esport_service_by_download_feature_dict)

    l1_streaming_visit_count_and_download_traffic_feature = node_from_config(
        joined_data_with_cust,
        l1_streaming_visit_count_and_download_traffic_feature_dict)

    return [int_l1_streaming_video_service_feature, l1_streaming_fav_video_service_by_download_feature,
            l1_streaming_2nd_fav_video_service_by_download_feature,
            int_l1_streaming_music_service_feature, l1_streaming_fav_music_service_by_download_feature,
            l1_streaming_2nd_fav_music_service_by_download_feature,
            int_l1_streaming_esport_service_feature, l1_streaming_fav_esport_service_by_download_feature,
            l1_streaming_2nd_fav_esport_service_by_download_feature,
            l1_streaming_visit_count_and_download_traffic_feature]
