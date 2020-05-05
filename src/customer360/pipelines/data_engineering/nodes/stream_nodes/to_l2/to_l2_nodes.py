import os

from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from pathlib import Path
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from pyspark.sql import DataFrame

conf = os.getenv("CONF", None)


def generate_l2_fav_streaming_day(input_df, app_list):
    if len(input_df.head(1)) == 0:
        return input_df

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
                                                       missing_data_check_flg='Y')

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
