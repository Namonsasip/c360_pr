from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from pathlib import Path
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import os

conf = os.getenv("CONF", "local")


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
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def dac_for_l3_streaming_fav_tv_show_by_episode_watched(input_df: DataFrame, cust_df: DataFrame):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df]

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="monthly", par_col="partition_month",
                                                      target_table_name="l3_billing_and_payments_monthly_overdue_bills")

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                F.max(F.col("start_of_month")).alias("max_date")),
            cust_df.select(
                F.max(F.col("partition_month")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(F.col("start_of_month") <= min_value)
    cust_df = cust_df.filter(F.col("partition_month") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]
