from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config, expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)


def generate_l2_fav_streaming_day(input_df: DataFrame, app_list: list):
    """
    :param input_df:
    :param app_list:
    :return:
    """
    spark = get_spark_session()
    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext
    ctx = ProjectContext(str(Path.cwd()), env=conf)

    for each_app in app_list:
        df = spark.sql("""
            select
                subscription_identifier,
                start_of_week,
                day_of_week as fav_{each_app}_streaming_day_of_week,
                download_kb_traffic_{each_app}_sum 
            from input_df
            where {each_app}_by_download_rank = 1
            and download_kb_traffic_{each_app}_sum > 0
        """.format(each_app=each_app))

        ctx.catalog.save("l2_streaming_fav_{}_streaming_day_of_week_feature"
                         .format(each_app), df)

    return None


def dac_for_streaming_to_l2_pipeline_from_l1(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name=target_table_name,
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def dac_for_streaming_to_l2_pipeline_from_l2(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="start_of_week",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def streaming_to_l2_content_type_features(input_df: DataFrame,
                                          int_l2_streaming_content_type_features_dict: dict,
                                          l2_streaming_fav_content_group_by_volume_dict: dict,
                                          l2_streaming_fav_content_group_by_duration_dict: dict) -> [DataFrame,
                                                                                                     DataFrame,
                                                                                                     DataFrame]:
    """
    :param input_df:
    :param int_l2_streaming_content_type_features_dict:
    :param l2_streaming_fav_content_group_by_volume_dict:
    :param l2_streaming_fav_content_group_by_duration_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l2_streaming_fav_content_group_by_duration_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        int_l2_streaming_content_type_features = node_from_config(small_df, int_l2_streaming_content_type_features_dict)
        CNTX.catalog.save(int_l2_streaming_content_type_features_dict["output_catalog"],
                          int_l2_streaming_content_type_features)

        l2_streaming_fav_content_group_by_volume = node_from_config(int_l2_streaming_content_type_features,
                                                                    l2_streaming_fav_content_group_by_volume_dict)
        CNTX.catalog.save(l2_streaming_fav_content_group_by_volume_dict["output_catalog"],
                          l2_streaming_fav_content_group_by_volume)

        l2_streaming_fav_content_group_by_duration = node_from_config(int_l2_streaming_content_type_features,
                                                                      l2_streaming_fav_content_group_by_duration_dict)
        CNTX.catalog.save(l2_streaming_fav_content_group_by_duration_dict["output_catalog"],
                          l2_streaming_fav_content_group_by_duration)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    int_l2_streaming_content_type_features = node_from_config(small_df, int_l2_streaming_content_type_features_dict)

    l2_streaming_fav_content_group_by_volume = node_from_config(int_l2_streaming_content_type_features,
                                                                l2_streaming_fav_content_group_by_volume_dict)

    l2_streaming_fav_content_group_by_duration = node_from_config(int_l2_streaming_content_type_features,
                                                                  l2_streaming_fav_content_group_by_duration_dict)

    return [int_l2_streaming_content_type_features, l2_streaming_fav_content_group_by_volume,
            l2_streaming_fav_content_group_by_duration]


def streaming_to_l2_tv_channel_type_features(input_df: DataFrame,
                                             int_l2_streaming_tv_channel_features_dict: dict,
                                             l2_streaming_fav_tv_channel_by_volume_dict: dict,
                                             l2_streaming_fav_tv_channel_by_duration_dict: dict,
                                             ) -> [DataFrame, DataFrame, DataFrame]:
    """

    :param input_df:
    :param int_l2_streaming_tv_channel_features_dict:
    :param l2_streaming_fav_tv_channel_by_volume_dict:
    :param l2_streaming_fav_tv_channel_by_duration_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l2_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        int_l2_streaming_tv_channel_features = node_from_config(small_df, int_l2_streaming_tv_channel_features_dict)
        CNTX.catalog.save(int_l2_streaming_tv_channel_features_dict["output_catalog"],
                          int_l2_streaming_tv_channel_features)

        l2_streaming_fav_tv_channel_by_volume = node_from_config(int_l2_streaming_tv_channel_features,
                                                                 l2_streaming_fav_tv_channel_by_volume_dict)
        CNTX.catalog.save(l2_streaming_fav_tv_channel_by_volume_dict["output_catalog"],
                          l2_streaming_fav_tv_channel_by_volume)

        l2_streaming_fav_tv_channel_by_duration = node_from_config(int_l2_streaming_tv_channel_features,
                                                                   l2_streaming_fav_tv_channel_by_duration_dict)
        CNTX.catalog.save(l2_streaming_fav_tv_channel_by_duration_dict["output_catalog"],
                          l2_streaming_fav_tv_channel_by_duration)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    int_l2_streaming_tv_channel_features = node_from_config(small_df, int_l2_streaming_tv_channel_features_dict)

    l2_streaming_fav_tv_channel_by_volume = node_from_config(int_l2_streaming_tv_channel_features,
                                                             l2_streaming_fav_tv_channel_by_volume_dict)

    l2_streaming_fav_tv_channel_by_duration = node_from_config(int_l2_streaming_tv_channel_features,
                                                               l2_streaming_fav_tv_channel_by_duration_dict)

    return [int_l2_streaming_tv_channel_features, l2_streaming_fav_tv_channel_by_volume,
            l2_streaming_fav_tv_channel_by_duration]


def streaming_to_l2_tv_channel_features(input_df: DataFrame,
                                        int_l2_streaming_video_service_feature_dict: dict,
                                        l2_streaming_fav_video_service_by_download_feature_dict: dict,
                                        l2_streaming_2nd_fav_video_service_by_download_feature_dict: dict,
                                        l2_streaming_fav_video_service_by_visit_count_feature_dict: dict) -> [DataFrame,
                                                                                                              DataFrame,
                                                                                                              DataFrame,
                                                                                                              DataFrame]:
    """
    :param input_df:
    :param int_l2_streaming_video_service_feature_dict:
    :param l2_streaming_fav_video_service_by_download_feature_dict:
    :param l2_streaming_2nd_fav_video_service_by_download_feature_dict:
    :param l2_streaming_fav_video_service_by_visit_count_feature_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l2_streaming_fav_video_service_by_visit_count_feature_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        int_l2_streaming_video_service_feature = node_from_config(small_df, int_l2_streaming_video_service_feature_dict)
        CNTX.catalog.save(int_l2_streaming_video_service_feature_dict["output_catalog"],
                          int_l2_streaming_video_service_feature)

        l2_streaming_fav_video_service_by_download_feature = node_from_config(
            int_l2_streaming_video_service_feature,
            l2_streaming_fav_video_service_by_download_feature_dict)
        CNTX.catalog.save(l2_streaming_fav_video_service_by_download_feature_dict["output_catalog"],
                          l2_streaming_fav_video_service_by_download_feature)

        l2_streaming_fav_tv_channel_by_duration = node_from_config(
            int_l2_streaming_video_service_feature,
            l2_streaming_2nd_fav_video_service_by_download_feature_dict)
        CNTX.catalog.save(l2_streaming_2nd_fav_video_service_by_download_feature_dict["output_catalog"],
                          l2_streaming_fav_tv_channel_by_duration)

        l2_streaming_fav_video_service_by_visit_count_feature = node_from_config(
            int_l2_streaming_video_service_feature,
            l2_streaming_fav_video_service_by_visit_count_feature_dict)
        CNTX.catalog.save(l2_streaming_fav_video_service_by_visit_count_feature_dict["output_catalog"],
                          l2_streaming_fav_video_service_by_visit_count_feature)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    int_l2_streaming_video_service_feature = node_from_config(small_df, int_l2_streaming_video_service_feature_dict)

    l2_streaming_fav_video_service_by_download_feature = node_from_config(
        int_l2_streaming_video_service_feature,
        l2_streaming_fav_video_service_by_download_feature_dict)

    l2_streaming_fav_tv_channel_by_duration = node_from_config(
        int_l2_streaming_video_service_feature,
        l2_streaming_2nd_fav_video_service_by_download_feature_dict)

    l2_streaming_fav_video_service_by_visit_count_feature = node_from_config(
        int_l2_streaming_video_service_feature,
        l2_streaming_fav_video_service_by_visit_count_feature_dict)

    return [int_l2_streaming_video_service_feature, l2_streaming_fav_video_service_by_download_feature,
            l2_streaming_fav_tv_channel_by_duration, l2_streaming_fav_video_service_by_visit_count_feature]


def streaming_to_l2_music_service_by_download(input_df: DataFrame,
                                              int_l2_streaming_music_service_feature_dict: dict,
                                              l2_streaming_fav_music_service_by_download_feature_dict: dict,
                                              l2_streaming_2nd_fav_music_service_by_download_feature_dict: dict,
                                              l2_streaming_fav_music_service_by_visit_count_feature_dict: dict) -> [
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame]:
    """
    :param input_df:
    :param int_l2_streaming_music_service_feature_dict:
    :param l2_streaming_fav_music_service_by_download_feature_dict:
    :param l2_streaming_2nd_fav_music_service_by_download_feature_dict:
    :param l2_streaming_fav_music_service_by_visit_count_feature_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l2_streaming_fav_music_service_by_visit_count_feature_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        int_l2_streaming_music_service_feature = node_from_config(small_df, int_l2_streaming_music_service_feature_dict)
        CNTX.catalog.save(int_l2_streaming_music_service_feature_dict["output_catalog"],
                          int_l2_streaming_music_service_feature)

        l2_streaming_fav_music_service_by_download_feature = node_from_config(
            int_l2_streaming_music_service_feature,
            l2_streaming_fav_music_service_by_download_feature_dict)
        CNTX.catalog.save(l2_streaming_fav_music_service_by_download_feature_dict["output_catalog"],
                          l2_streaming_fav_music_service_by_download_feature)

        l2_streaming_2nd_fav_music_service_by_download_feature = node_from_config(
            int_l2_streaming_music_service_feature,
            l2_streaming_2nd_fav_music_service_by_download_feature_dict)
        CNTX.catalog.save(l2_streaming_2nd_fav_music_service_by_download_feature_dict["output_catalog"],
                          l2_streaming_2nd_fav_music_service_by_download_feature)

        l2_streaming_fav_music_service_by_visit_count_feature = node_from_config(
            int_l2_streaming_music_service_feature,
            l2_streaming_fav_music_service_by_visit_count_feature_dict)
        CNTX.catalog.save(l2_streaming_fav_music_service_by_visit_count_feature_dict["output_catalog"],
                          l2_streaming_fav_music_service_by_visit_count_feature)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    int_l2_streaming_music_service_feature = node_from_config(small_df, int_l2_streaming_music_service_feature_dict)

    l2_streaming_fav_music_service_by_download_feature = node_from_config(
        int_l2_streaming_music_service_feature,
        l2_streaming_fav_music_service_by_download_feature_dict)

    l2_streaming_2nd_fav_music_service_by_download_feature = node_from_config(
        int_l2_streaming_music_service_feature,
        l2_streaming_2nd_fav_music_service_by_download_feature_dict)

    l2_streaming_fav_music_service_by_visit_count_feature = node_from_config(
        int_l2_streaming_music_service_feature,
        l2_streaming_fav_music_service_by_visit_count_feature_dict)

    return [int_l2_streaming_music_service_feature, l2_streaming_fav_music_service_by_download_feature,
            l2_streaming_2nd_fav_music_service_by_download_feature,
            l2_streaming_fav_music_service_by_visit_count_feature]


def streaming_to_l2_esoprt_service_by_download(input_df: DataFrame,
                                               int_l2_streaming_esport_service_feature_dict: dict,
                                               l2_streaming_fav_esport_service_by_download_feature_dict: dict,
                                               l2_streaming_2nd_fav_esport_service_by_download_feature_dict: dict,
                                               l2_streaming_fav_esport_service_by_visit_count_feature_dict: dict) -> [
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame]:
    """
    :param input_df:
    :param int_l2_streaming_esport_service_feature_dict:
    :param l2_streaming_fav_esport_service_by_download_feature_dict:
    :param l2_streaming_2nd_fav_esport_service_by_download_feature_dict:
    :param l2_streaming_fav_esport_service_by_visit_count_feature_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l2_streaming_fav_esport_service_by_visit_count_feature_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        int_l2_streaming_esport_service_feature = node_from_config(
            small_df, int_l2_streaming_esport_service_feature_dict)
        CNTX.catalog.save(int_l2_streaming_esport_service_feature_dict["output_catalog"],
                          int_l2_streaming_esport_service_feature)

        l2_streaming_fav_esport_service_by_download_feature = node_from_config(
            int_l2_streaming_esport_service_feature,
            l2_streaming_fav_esport_service_by_download_feature_dict)
        CNTX.catalog.save(l2_streaming_fav_esport_service_by_download_feature_dict["output_catalog"],
                          l2_streaming_fav_esport_service_by_download_feature)

        l2_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(
            int_l2_streaming_esport_service_feature,
            l2_streaming_2nd_fav_esport_service_by_download_feature_dict)
        CNTX.catalog.save(l2_streaming_2nd_fav_esport_service_by_download_feature_dict["output_catalog"],
                          l2_streaming_2nd_fav_esport_service_by_download_feature)

        l2_streaming_fav_esport_service_by_visit_count_feature = node_from_config(
            int_l2_streaming_esport_service_feature,
            l2_streaming_fav_esport_service_by_visit_count_feature_dict)
        CNTX.catalog.save(l2_streaming_fav_esport_service_by_visit_count_feature_dict["output_catalog"],
                          l2_streaming_fav_esport_service_by_visit_count_feature)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    int_l2_streaming_esport_service_feature = node_from_config(small_df, int_l2_streaming_esport_service_feature_dict)

    l2_streaming_fav_esport_service_by_download_feature = node_from_config(
        int_l2_streaming_esport_service_feature,
        l2_streaming_fav_esport_service_by_download_feature_dict)

    l2_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(
        int_l2_streaming_esport_service_feature,
        l2_streaming_2nd_fav_esport_service_by_download_feature_dict)

    l2_streaming_fav_esport_service_by_visit_count_feature = node_from_config(
        int_l2_streaming_esport_service_feature,
        l2_streaming_fav_esport_service_by_visit_count_feature_dict)

    return [int_l2_streaming_esport_service_feature, l2_streaming_fav_esport_service_by_download_feature,
            l2_streaming_2nd_fav_esport_service_by_download_feature,
            l2_streaming_fav_esport_service_by_visit_count_feature]


def streaming_streaming_ranked_of_day_per_week(input_df: DataFrame,
                                               int_l2_streaming_sum_per_day_dict: dict,
                                               int_l2_streaming_ranked_of_day_per_week_dict: dict,
                                               streaming_app_list: list) -> DataFrame:
    """
    :param input_df:
    :param int_l2_streaming_sum_per_day_dict:
    :param int_l2_streaming_ranked_of_day_per_week_dict:
    :param streaming_app_list:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=int_l2_streaming_sum_per_day_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        int_l2_streaming_sum_per_day = expansion(small_df, int_l2_streaming_sum_per_day_dict)
        CNTX.catalog.save(int_l2_streaming_sum_per_day_dict["output_catalog"],
                          int_l2_streaming_sum_per_day)

        int_l2_streaming_ranked_of_day_per_week = node_from_config(int_l2_streaming_sum_per_day,
                                                                   int_l2_streaming_ranked_of_day_per_week_dict)

        generate_l2_fav_streaming_day(int_l2_streaming_ranked_of_day_per_week, streaming_app_list)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))

    int_l2_streaming_sum_per_day = expansion(small_df, int_l2_streaming_sum_per_day_dict)

    int_l2_streaming_ranked_of_day_per_week = node_from_config(int_l2_streaming_sum_per_day,
                                                               int_l2_streaming_ranked_of_day_per_week_dict)

    generate_l2_fav_streaming_day(int_l2_streaming_ranked_of_day_per_week, streaming_app_list)

    return int_l2_streaming_sum_per_day


def streaming_to_l2_fav_tv_show_by_share_of_completed_episodes(
        vimmi_usage_daily: DataFrame,
        streaming_series_title_master: DataFrame,
        int_l2_streaming_share_of_completed_episodes_features_dict: dict,
        int_l2_streaming_share_of_completed_episodes_ratio_features_dict: dict,
        l2_streaming_fav_tv_show_by_share_of_completed_episodes_dict: dict) -> DataFrame:
    """

    :param vimmi_usage_daily:
    :param streaming_series_title_master:
    :param int_l2_streaming_share_of_completed_episodes_features_dict:
    :param int_l2_streaming_share_of_completed_episodes_ratio_features_dict:
    :param l2_streaming_fav_tv_show_by_share_of_completed_episodes_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([vimmi_usage_daily]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=vimmi_usage_daily, grouping="weekly", par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name=l2_streaming_fav_tv_show_by_share_of_completed_episodes_dict["output_catalog"],
        exception_partitions=["2020-04-27"])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        selective_df = small_df. \
            select("subscription_identifier",  "start_of_week", "content_group", "title", "series_title")

        # share_of_completed_episodes feature
        int_l2_streaming_share_of_completed_episodes_features = node_from_config(
            selective_df, int_l2_streaming_share_of_completed_episodes_features_dict)

        int_l2_streaming_share_of_completed_episodes_ratio_features_temp = \
            int_l2_streaming_share_of_completed_episodes_features.join(
                streaming_series_title_master, on="series_title", how="left")
        int_l2_streaming_share_of_completed_episodes_ratio_features_temp = \
            int_l2_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
                "share_of_completed_episodes",
                (F.col("episode_watched_count") / F.coalesce(F.col("total_episode_count"), F.lit(1))))

        int_l2_streaming_share_of_completed_episodes_ratio_features = node_from_config(
            int_l2_streaming_share_of_completed_episodes_ratio_features_temp,
            int_l2_streaming_share_of_completed_episodes_ratio_features_dict)

        l2_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
            int_l2_streaming_share_of_completed_episodes_ratio_features,
            l2_streaming_fav_tv_show_by_share_of_completed_episodes_dict)

        CNTX.catalog.save(l2_streaming_fav_tv_show_by_share_of_completed_episodes_dict["output_catalog"],
                          l2_streaming_fav_tv_show_by_share_of_completed_episodes)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    selective_df = small_df. \
        select("subscription_identifier", "start_of_week", "content_group", "title", "series_title")

    # share_of_completed_episodes feature
    int_l2_streaming_share_of_completed_episodes_features = node_from_config(
        selective_df, int_l2_streaming_share_of_completed_episodes_features_dict)

    int_l2_streaming_share_of_completed_episodes_ratio_features_temp = \
        int_l2_streaming_share_of_completed_episodes_features.join(
            streaming_series_title_master, on="series_title", how="left")
    int_l2_streaming_share_of_completed_episodes_ratio_features_temp = \
        int_l2_streaming_share_of_completed_episodes_ratio_features_temp.withColumn(
            "share_of_completed_episodes",
            (F.col("episode_watched_count") / F.coalesce(F.col("total_episode_count"), F.lit(1))))

    int_l2_streaming_share_of_completed_episodes_ratio_features = node_from_config(
        int_l2_streaming_share_of_completed_episodes_ratio_features_temp,
        int_l2_streaming_share_of_completed_episodes_ratio_features_dict)

    l2_streaming_fav_tv_show_by_share_of_completed_episodes = node_from_config(
        int_l2_streaming_share_of_completed_episodes_ratio_features,
        l2_streaming_fav_tv_show_by_share_of_completed_episodes_dict)

    return l2_streaming_fav_tv_show_by_share_of_completed_episodes
