from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols, add_event_week_and_month_from_yyyymmdd, gen_max_sql, execute_sql
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)


# Defaulted date in DAC is  exception_partitions=["2020-04-01"] as we are reading from April Starting


def series_title_master(vimmi_usage: DataFrame) -> DataFrame:
    """

    :param vimmi_usage:
    :return:
    """
    master = vimmi_usage.select("series_title", "title").groupBy("series_title").agg(
        F.countDistinct("title").alias("total_episode_count"))

    return master


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

    mvv_new = list(divide_chunks(mvv_array, 3))
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

    mvv_new = list(divide_chunks(mvv_array, 3))
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
        int_l3_streaming_tv_show_features_dict: dict,
        l3_streaming_fav_tv_show_by_episode_watched_dict: dict,
        int_l3_streaming_genre_dict: dict,
        l3_streaming_genre_dict: dict) -> [DataFrame, DataFrame]:
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
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l3_streaming_fav_tv_show_by_episode_watched_dict["output_catalog"],
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

    mvv_new = list(divide_chunks(mvv_array, 3))
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

        int_l3_streaming_genre_features = node_from_config(small_df, int_l3_streaming_genre_dict)

        l3_streaming_genre = node_from_config(int_l3_streaming_genre_features, l3_streaming_genre_dict)
        CNTX.catalog.save(l3_streaming_genre_dict["output_catalog"],
                          l3_streaming_genre)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    int_l3_streaming_tv_show_features = node_from_config(small_df, int_l3_streaming_tv_show_features_dict)

    l3_streaming_fav_tv_show_by_episode_watched = node_from_config(int_l3_streaming_tv_show_features,
                                                                   l3_streaming_fav_tv_show_by_episode_watched_dict)

    int_l3_streaming_genre_features = node_from_config(small_df, int_l3_streaming_genre_dict)

    l3_streaming_genre = node_from_config(int_l3_streaming_genre_features, l3_streaming_genre_dict)

    return [l3_streaming_fav_tv_show_by_episode_watched, l3_streaming_genre]


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

    mvv_new = list(divide_chunks(mvv_array, 3))
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


def streaming_to_l3_fav_tv_show_by_share_of_completed_episodes(
        vimmi_usage_daily: DataFrame,
        streaming_series_title_master: DataFrame,
        int_l3_streaming_share_of_completed_episodes_features_dict: dict,
        int_l3_streaming_share_of_completed_episodes_ratio_features_dict: dict,
        l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict: dict) -> DataFrame:
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
        df=vimmi_usage_daily, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict["output_catalog"],
        exception_partitions=["2020-02-01"])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
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

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        selective_df = small_df. \
            select("subscription_identifier", "start_of_month", "content_group", "title", "series_title")

        # share_of_completed_episodes feature
        int_l3_streaming_share_of_completed_episodes_features = node_from_config(
            selective_df, int_l3_streaming_share_of_completed_episodes_features_dict)

        int_l3_streaming_share_of_completed_episodes_ratio_features_temp = \
            int_l3_streaming_share_of_completed_episodes_features.join(
                streaming_series_title_master, on="series_title", how="left")

        int_l3_streaming_share_of_completed_episodes_ratio_features_temp = \
            int_l3_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
                "share_of_completed_episodes",
                (F.col("episode_watched_count") / F.coalesce(F.col("total_episode_count"), F.lit(1))))

        int_l3_streaming_share_of_completed_episodes_ratio_features = node_from_config(
            int_l3_streaming_share_of_completed_episodes_ratio_features_temp,
            int_l3_streaming_share_of_completed_episodes_ratio_features_dict)

        l3_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
            int_l3_streaming_share_of_completed_episodes_ratio_features,
            l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict)

        CNTX.catalog.save(l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict["output_catalog"],
                          l3_streaming_fav_tv_show_by_share_of_completed_episodes)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    selective_df = small_df. \
        select("subscription_identifier", "start_of_month", "content_group", "title", "series_title")

    # share_of_completed_episodes feature
    int_l3_streaming_share_of_completed_episodes_features = node_from_config(
        selective_df, int_l3_streaming_share_of_completed_episodes_features_dict)

    int_l3_streaming_share_of_completed_episodes_ratio_features_temp = \
        int_l3_streaming_share_of_completed_episodes_features.join(streaming_series_title_master,
                                                                   on="series_title", how="left")
    int_l3_streaming_share_of_completed_episodes_ratio_features_temp = \
        int_l3_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
            "share_of_completed_episodes",
            (F.col("episode_watched_count") / F.coalesce(F.col("total_episode_count"), F.lit(1))))

    int_l3_streaming_share_of_completed_episodes_ratio_features = node_from_config(
        int_l3_streaming_share_of_completed_episodes_ratio_features_temp,
        int_l3_streaming_share_of_completed_episodes_ratio_features_dict)

    l3_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
        int_l3_streaming_share_of_completed_episodes_ratio_features,
        l3_streaming_fav_tv_show_by_share_of_completed_episodes_dict)

    return l3_streaming_fav_tv_show_by_share_of_completed_episodes


def streaming_favourite_start_hour_of_day_func(
        input_df: DataFrame) -> None:
    """
    :param input_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return None
    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l3_streaming_traffic_consumption_time_based_features")
    if check_empty_dfs([input_df]):
        return None
    ################################# End Implementing Data availability checks ###############################
    def process_massive_processing_favourite_hour(data_frame: DataFrame):
        """
        :param data_frame:
        :return:
        """
        dictionary = [{'filter_condition': "youtube,youtube_go,youtubebyclick",
                       'output_col': 'fav_youtube_streaming_hour_of_day'},
                      {'filter_condition': "trueid",
                       'output_col': 'fav_trueid_streaming_hour_of_day'},
                      {'filter_condition': "truevisions",
                       'output_col': 'fav_truevisions_streaming_hour_of_day'},
                      {'filter_condition': "monomaxx",
                       'output_col': 'fav_monomaxx_streaming_hour_of_day'},
                      {'filter_condition': "qqlive",
                       'output_col': 'fav_qqlive_streaming_hour_of_day'},
                      {'filter_condition': "facebook",
                       'output_col': 'fav_facebook_streaming_hour_of_day'},
                      {'filter_condition': "linetv",
                       'output_col': 'fav_linetv_streaming_hour_of_day'},
                      {'filter_condition': "ais_play",
                       'output_col': 'fav_ais_play_streaming_hour_of_day'},
                      {'filter_condition': "netflix",
                       'output_col': 'fav_netflix_streaming_hour_of_day'},
                      {'filter_condition': "viu,viutv",
                       'output_col': 'fav_viu_streaming_hour_of_day'},
                      {'filter_condition': "iflix",
                       'output_col': 'fav_iflix_streaming_hour_of_day'},
                      {'filter_condition': "spotify",
                       'output_col': 'fav_spotify_streaming_hour_of_day'},
                      {'filter_condition': "jooxmusic",
                       'output_col': 'fav_jooxmusic_streaming_hour_of_day'},
                      {'filter_condition': "twitchtv",
                       'output_col': 'fav_twitchtv_streaming_hour_of_day'},
                      {'filter_condition': "bigo",
                       'output_col': 'fav_bigo_streaming_hour_of_day'},
                      {'filter_condition': "valve_steam",
                       'output_col': 'fav_valve_steam_streaming_hour_of_day'}]

        final_dfs = []
        win = Window.partitionBy(["subscription_identifier", "start_of_month"]).orderBy(F.col("download").desc())
        for curr_dict in dictionary:
            filter_query = curr_dict["filter_condition"].split(",")
            output_col = curr_dict["output_col"]
            curr_item = data_frame. \
                filter(F.lower(F.col("application_name")).isin(filter_query))
            curr_item = curr_item.groupBy(["subscription_identifier", "hour", "start_of_month"])\
                .agg(F.sum("dw_kbyte").alias("download"))
            curr_item = curr_item.withColumn("rnk", F.row_number().over(win)).where("rnk = 1")

            curr_item = curr_item.select("subscription_identifier",
                                         F.col("hour").alias(output_col),
                                         "start_of_month")

            final_dfs.append(curr_item)

        union_df = union_dataframes_with_missing_cols(final_dfs)
        group_cols = ["subscription_identifier", "start_of_month"]

        final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
        merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

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

        input_with_application = data_frame.\
            withColumn("time_of_day", F.when(F.col("hour").isin(morning), F.lit("morning")).
                       otherwise(
            F.when(F.col("hour").isin(afternoon), F.lit("afternoon")).
                otherwise(F.when(F.col("hour").isin(evening), F.lit("evening")).
                          otherwise(F.lit("night"))
                          )
        )
                       )

        weekend_type = ['Saturday', 'Sunday']
        input_with_application = input_with_application.\
            withColumn("day_type", F.when(F.date_format("event_partition_date", 'EEEE').isin(weekend_type),
                       F.lit("weekend")).otherwise(F.lit('weekday')))

        final_dfs = []
        for curr_time_type in ["morning", "afternoon", "evening", "night"]:
            for app_group_type in ["videoplayers_editors", "music_audio", "game"]:
                v_time_type = curr_time_type
                v_app_group = app_group_type
                final_col = "share_of_{}_streaming_usage_{}_by_total".format(v_time_type, app_group_type)
                filtered = input_with_application.filter(F.lower(F.col("application_group")) == v_app_group)
                filtered_agg = filtered.groupBy(["subscription_identifier", "start_of_month"])\
                    .agg(F.sum("dw_kbyte").alias("main_download"))

                curr_time_type_agg = filtered.filter(F.col("time_of_day") == v_time_type) \
                    .groupBy(["subscription_identifier", "start_of_month"]).agg(F.sum("dw_kbyte").alias("download"))

                final_df = filtered_agg.join(curr_time_type_agg, ["subscription_identifier", "start_of_month"])\
                    .withColumn(final_col, F.col("download") / F.col("main_download"))\
                    .drop("download", "main_download")
                final_dfs.append(final_df)

        for curr_time_type in ["weekend", "weekday"]:
            for app_group_type in ["videoplayers_editors", "music_audio", "game"]:
                v_time_type = curr_time_type
                v_app_group = app_group_type
                final_col = "share_of_{}_streaming_usage_{}_by_total".format(v_time_type, app_group_type)
                filtered = input_with_application.filter(F.lower(F.col("application_group")) == v_app_group)
                filtered_agg = filtered.groupBy(["subscription_identifier", "start_of_month"])\
                    .agg(F.sum("dw_kbyte").alias("main_download"))

                curr_time_type_agg = filtered.filter(F.col("day_type") == v_time_type)\
                    .groupBy(["subscription_identifier", "start_of_month"]).agg(F.sum("dw_kbyte").alias("download"))

                final_df = filtered_agg.join(curr_time_type_agg, ["subscription_identifier", "start_of_month"])\
                    .withColumn(final_col, F.col("download") / F.col("main_download"))\
                    .drop("download", "main_download")
                final_dfs.append(final_df)

        union_df = union_dataframes_with_missing_cols(final_dfs)

        group_cols = ["subscription_identifier", "start_of_month"]

        final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
        merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

        CNTX = load_context(Path.cwd(), env=conf)
        CNTX.catalog.save("l3_streaming_traffic_consumption_time_based_features", merged_df)

        return None

    application_list = ["youtube", "youtube_go", "youtubebyclick", "trueid", "truevisions", "monomaxx","qqlive",
                        "facebook", "linetv", "ais_play", "netflix", "viu", "viutv", "iflix", "spotify", "jooxmusic",
                        "twitchtv", "bigo", "valve_steam"]
    input_df_hour_based = input_df.filter(F.lower(F.col("application_name")).isin(application_list))
    process_massive_processing_favourite_hour(input_df_hour_based)

    application_group = ["videoplayers_editors", "music_audio", "game"]
    input_df_group = input_df.filter(F.lower(F.col("application_group")).isin(application_group))
    process_massive_processing_application_group(input_df_group)

    return None


def streaming_favourite_location_features_func(
        input_df: DataFrame) -> DataFrame:
    """
    :param input_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l3_streaming_favourite_location_features")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    dictionary = [{'filter_condition': "youtube,youtube_go,youtubebyclick",
                   'output_col': 'fav_youtube_streaming_base_station_id'},
                  {'filter_condition': "trueid",
                   'output_col': 'fav_trueid_streaming_base_station_id'},
                  {'filter_condition': "truevisions",
                   'output_col': 'fav_truevisions_streaming_base_station_id'},
                  {'filter_condition': "monomaxx",
                   'output_col': 'fav_monomaxx_streaming_base_station_id'},
                  {'filter_condition': "qqlive",
                   'output_col': 'fav_qqlive_streaming_base_station_id'},
                  {'filter_condition': "facebook",
                   'output_col': 'fav_facebook_streaming_base_station_id'},
                  {'filter_condition': "linetv",
                   'output_col': 'fav_linetv_streaming_base_station_id'},
                  {'filter_condition': "ais_play",
                   'output_col': 'fav_ais_play_streaming_base_station_id'},
                  {'filter_condition': "netflix",
                   'output_col': 'fav_netflix_streaming_base_station_id'},
                  {'filter_condition': "viu,viutv",
                   'output_col': 'fav_viu_streaming_base_station_id'},
                  {'filter_condition': "iflix",
                   'output_col': 'fav_iflix_streaming_base_station_id'},
                  {'filter_condition': "spotify",
                   'output_col': 'fav_spotify_streaming_base_station_id'},
                  {'filter_condition': "jooxmusic",
                   'output_col': 'fav_jooxmusic_streaming_base_station_id'},
                  {'filter_condition': "twitchtv",
                   'output_col': 'fav_twitchtv_streaming_base_station_id'},
                  {'filter_condition': "bigo",
                   'output_col': 'fav_bigo_streaming_base_station_id'},
                  {'filter_condition': "valve_steam",
                   'output_col': 'fav_valve_steam_streaming_base_station_id'}]

    final_dfs = []
    win = Window.partitionBy(["subscription_identifier", "start_of_month"]).orderBy(F.col("download").desc())
    for curr_dict in dictionary:
        filter_query = curr_dict["filter_condition"].split(",")
        output_col = curr_dict["output_col"]
        curr_item = input_df. \
            filter(F.lower(F.col("application_name")).isin(filter_query))
        curr_item = curr_item.groupBy(["subscription_identifier", "location_id", "start_of_month"])\
            .agg(F.sum("download").alias("download"))
        curr_item = curr_item.withColumn("rnk", F.row_number().over(win)).where("rnk = 1")
        curr_item = curr_item.select("subscription_identifier",
                                     F.col("location_id").alias(output_col),
                                     "start_of_month")
        final_dfs.append(curr_item)

    union_df = union_dataframes_with_missing_cols(final_dfs)
    group_cols = ["subscription_identifier", "start_of_month"]

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df


def streaming_favourite_quality_features_func(
        input_df: DataFrame) -> DataFrame:
    """
    :param input_df:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l3_streaming_app_quality_features")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    dictionary = [{'filter_condition': "youtube,youtube_go,youtubebyclick",
                   'output_col': 'avg_streaming_quality_youtube'},
                  {'filter_condition': "trueid",
                   'output_col': 'avg_streaming_quality_trueid'},
                  {'filter_condition': "truevisions",
                   'output_col': 'avg_streaming_quality_truevisions'},
                  {'filter_condition': "monomaxx",
                   'output_col': 'avg_streaming_quality_monomaxx'},
                  {'filter_condition': "qqlive",
                   'output_col': 'avg_streaming_quality_qqlive'},
                  {'filter_condition': "ais_play",
                   'output_col': 'avg_streaming_quality_ais_play'},
                  {'filter_condition': "netflix",
                   'output_col': 'avg_streaming_quality_netflix'},
                  {'filter_condition': "viu,viutv",
                   'output_col': 'avg_streaming_quality_viu'},
                  {'filter_condition': "iflix",
                   'output_col': 'avg_streaming_quality_iflix'},
                  {'filter_condition': "spotify",
                   'output_col': 'avg_streaming_quality_spotify'},
                  {'filter_condition': "jooxmusic",
                   'output_col': 'avg_streaming_quality_jooxmusic'},
                  {'filter_condition': "twitchtv",
                   'output_col': 'avg_streaming_quality_twitchtv'},
                  {'filter_condition': "bigo",
                   'output_col': 'avg_streaming_quality_bigo'},
                  {'filter_condition': "valve_steam",
                   'output_col': 'avg_streaming_quality_valve_steam'}]

    final_dfs = []
    for curr_dict in dictionary:
        filter_query = curr_dict["filter_condition"].split(",")
        output_col = curr_dict["output_col"]
        curr_item = input_df.\
            filter(F.lower(F.col("application_name")).isin(filter_query))

        curr_item = curr_item.groupBy(["subscription_identifier", "start_of_month"]) \
            .agg(F.avg("calc_column").alias(output_col))
        final_dfs.append(curr_item)

    union_df = union_dataframes_with_missing_cols(final_dfs)
    group_cols = ["subscription_identifier", "start_of_month"]

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df















